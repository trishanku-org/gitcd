// TODO test
package backend

import (
	"context"
	"path"
	"reflect"

	"github.com/go-logr/logr"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type InitOpts struct {
	Repo                               git.Repository
	Errors                             git.Errors
	DataRefName, MetadataRefNamePrefix git.ReferenceName
	StartRevision                      int64
	Version                            string
	Force                              bool
	CommitterName, CommitterEmail      string
}

func InitMetadata(ctx context.Context, opts *InitOpts, log logr.Logger) error {
	var b = &backend{
		repo:                  opts.Repo,
		errors:                opts.Errors,
		refName:               opts.DataRefName,
		metadataRefNamePrefix: opts.MetadataRefNamePrefix,
		commitConfig: commitConfig{
			committerName:  opts.CommitterName,
			committerEmail: opts.CommitterEmail,
		},
		log: log,
	}

	return b.initMetadata(ctx, opts.StartRevision, opts.Version, opts.Force)
}

func (b *backend) getHead(ctx context.Context, refName git.ReferenceName) (c git.Commit, err error) {
	var ref git.Reference

	if ref, err = b.getReference(ctx, refName); err != nil {
		return
	}

	defer ref.Close()

	c, err = b.repo.Peeler().PeelToCommit(ctx, ref)
	return
}

func (b *backend) getDataHead(ctx context.Context) (c git.Commit, err error) {
	var refName git.ReferenceName

	if refName, err = b.getDataRefName(); err != nil {
		return
	}

	c, err = b.getHead(ctx, refName)
	return
}

func (b *backend) getMetadataHead(ctx context.Context) (c git.Commit, err error) {
	var refName git.ReferenceName

	if refName, err = b.getMetadataRefName(); err != nil {
		return
	}

	c, err = b.getHead(ctx, refName)
	return
}

func (b *backend) loadDataMetadataMapping(ctx context.Context, metaHead git.Commit, dmMapping map[git.ObjectID]git.ObjectID) (err error) {
	var cw git.CommitWalker

	if metaHead == nil {
		return
	}

	cw = b.repo.CommitWalker()
	defer cw.Close()

	err = cw.ForEachCommit(ctx, metaHead, func(ctx context.Context, c git.Commit) (done, skip bool, err error) {
		var (
			t   git.Tree
			dID git.ObjectID
		)

		if t, err = b.repo.Peeler().PeelToTree(ctx, c); err != nil {
			return
		}

		defer t.Close()

		if dID, err = b.readOjectID(ctx, t, metadataPathData); err != nil {
			return
		}

		if _, skip = dmMapping[dID]; skip {
			return
		}

		dmMapping[dID] = c.ID()
		return
	})

	return
}

func (b *backend) createEmptyCommit(ctx context.Context, revision int64) (id git.ObjectID, err error) {
	var (
		tb  git.TreeBuilder
		tID git.ObjectID
	)

	if tb, err = b.repo.TreeBuilder(ctx); err != nil {
		return
	}

	defer tb.Close()

	if tID, err = tb.Build(ctx); err != nil {
		return
	}

	id, err = b.createCommit(ctx, revisionToString(revision), tID, nil)
	return
}

func (b *backend) createStartingMetadata(ctx context.Context, revision int64, version string) (mID git.ObjectID, err error) {
	var (
		mtb       git.TreeBuilder
		dID, mtID git.ObjectID
	)

	// defer func() {
	// 	b.log.V(-1).Info("Initialized", "revision", revision, "data commit", dID, "metadata commit", mID, "error", err)
	// }()

	if dID, err = b.createEmptyCommit(ctx, revision); err != nil {
		return
	}

	if mtb, err = b.repo.TreeBuilder(ctx); err != nil {
		return
	}

	defer mtb.Close()

	for entryName, content := range map[string][]byte{
		metadataPathRevision: []byte(revisionToString(revision)),
		metadataPathVersion:  []byte(version),
		metadataPathData:     []byte(dID.String()),
	} {
		if _, err = b.addOrReplaceTreeEntry(ctx, mtb, entryName, content, nil); err != nil {
			return
		}
	}

	if mtID, err = mtb.Build(ctx); err != nil {
		return
	}

	mID, err = b.createCommit(ctx, revisionToString(revision), mtID, nil)
	return
}

func (b *backend) initMetadataForDataParents(
	ctx context.Context,
	data git.Commit,
	startRevision int64,
	version string,
	dmMapping map[git.ObjectID]git.ObjectID,
) (metaParentIDs []git.ObjectID, err error) {
	err = data.ForEachParent(ctx, func(ctx context.Context, c git.Commit) (done bool, err error) {
		var mID git.ObjectID

		// Initialize metadata for all parent commits of data.
		if mID, err = b.initMetadataForData(ctx, c, startRevision, version, dmMapping); err != nil {
			return
		}

		metaParentIDs = append(metaParentIDs, mID)
		return
	})
	return
}

func (b *backend) mutateMetadataForDataChange(
	ctx context.Context,
	metaT, oldDataT, newDataT git.Tree,
	newDataHeadID git.ObjectID,
	newRevision int64,
) (mutated bool, newMetaTID git.ObjectID, err error) {
	var (
		diff                   git.Diff
		deleteFn               = mutation.DeleteEntry
		revisionMutateFn       = b.mutateRevisionTo(newRevision)
		createRevisionMutateFn = b.mutateRevisionIfNotExistsTo(newRevision)
		versionMutateFn        = b.incrementRevision()
		mtm                    *mutation.TreeMutation
	)

	if diff, err = b.repo.TreeDiff(ctx, oldDataT, newDataT); err != nil {
		return
	}

	defer diff.Close()

	if err = diff.ForEachDiffChange(ctx, func(ctx context.Context, change git.DiffChange) (done bool, err error) {
		var (
			mutations = make(map[string]mutation.MutateTreeEntryFunc, 3)
			p         = change.Path()
		)

		switch change.Type() {
		case git.DiffChangeTypeDeleted:
			mutations[p] = deleteFn
		default:
			mutations[path.Join(p, etcdserverpb.Compare_CREATE.String())] = createRevisionMutateFn
			mutations[path.Join(p, etcdserverpb.Compare_MOD.String())] = revisionMutateFn
			if !reflect.DeepEqual(change.OldBlobID(), change.NewBlobID()) {
				mutations[path.Join(p, etcdserverpb.Compare_VERSION.String())] = versionMutateFn
			}
		}

		for p, mutateFn := range mutations {
			if mtm, err = mutation.AddMutation(mtm, p, mutateFn); err != nil {
				return
			}
		}

		return
	}); err != nil {
		return
	}

	for p, mutateFn := range map[string]mutation.MutateTreeEntryFunc{
		metadataPathRevision: revisionMutateFn,
		metadataPathData: func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
			mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(newDataHeadID.String()), te)
			return
		},
	} {
		if mtm, err = mutation.AddMutation(mtm, p, mutateFn); err != nil {
			return
		}
	}

	mutated, newMetaTID, err = mutation.MutateTree(ctx, b.repo, metaT, mtm, true)
	return
}

func (b *backend) initMetadataForData(
	ctx context.Context,
	data git.Commit,
	startRevision int64,
	version string,
	dmMapping map[git.ObjectID]git.ObjectID,
) (mID git.ObjectID, err error) {
	var (
		ok, mutated                    bool
		metaParentIDs                  []git.ObjectID
		dataID, prevMetaID, newMetaTID git.ObjectID
		prevMeta, prevData             git.Commit
		dataT, prevDataT, prevMetaT    git.Tree
		revision                       int64
	)

	defer func() {
		if err != nil {
			return
		}

		if _, ok := dmMapping[dataID]; !ok {
			dmMapping[dataID] = mID
		}
	}()

	if data != nil {
		dataID = data.ID()
	}

	if mID, ok = dmMapping[dataID]; ok {
		return
	}

	if data == nil {
		mID, err = b.createStartingMetadata(ctx, startRevision-1, version)
		return
	} else if dataT, err = b.repo.Peeler().PeelToTree(ctx, data); err != nil {
		return
	}

	dataT.Close()

	if metaParentIDs, err = b.initMetadataForDataParents(ctx, data, startRevision, version, dmMapping); err != nil {
		return
	}

	if len(metaParentIDs) == 0 {
		var id git.ObjectID

		if id, err = b.initMetadataForData(ctx, nil, startRevision, version, dmMapping); err != nil {
			return
		}

		metaParentIDs = append(metaParentIDs, id)
	}

	prevMetaID = metaParentIDs[0]

	if prevMeta, err = b.repo.ObjectGetter().GetCommit(ctx, prevMetaID); err != nil {
		return
	}

	defer prevMeta.Close()

	if prevMetaT, err = b.repo.Peeler().PeelToTree(ctx, prevMeta); err != nil {
		return
	}

	defer prevMetaT.Close()

	if prevData, err = b.getDataCommitForMetadata(ctx, prevMetaT); b.errors.IgnoreNotFound(err) != nil {
		return
	}

	if prevData != nil {
		defer prevData.Close()

		if prevDataT, err = b.repo.Peeler().PeelToTree(ctx, prevData); err != nil {
			return
		}

		defer prevDataT.Close()
	}

	if revision, err = b.readRevision(ctx, prevMetaT, metadataPathRevision); err != nil {
		revision = startRevision - 1
	}

	revision++

	if mutated, newMetaTID, err = b.mutateMetadataForDataChange(ctx, prevMetaT, prevDataT, dataT, dataID, revision); err != nil {
		return
	}

	if !mutated {
		newMetaTID = prevMetaT.ID()
	}

	if mID, err = b.createCommit(ctx, revisionToString(revision), newMetaTID, func(ctx context.Context, cb git.CommitBuilder) error {
		return cb.AddParentIDs(metaParentIDs...)
	}); err != nil {
		return
	}

	return
}

func (b *backend) initMetadata(ctx context.Context, startRevision int64, version string, force bool) (err error) {
	var (
		metaHead, dataHead git.Commit
		dmMapping          map[git.ObjectID]git.ObjectID
		mID, dID           git.ObjectID
		metaT              git.Tree
		revision           int64
	)

	if len(version) == 0 {
		version = Version
	}

	defer func() {
		if metaHead != nil {
			metaHead.Close()
		}
	}()

	if metaHead, err = b.getMetadataHead(ctx); b.errors.IgnoreNotFound(err) != nil {
		return
	} else if err == nil && !force {
		return // Skip if we find existing metadata and init is not forced.
	}

	if dataHead, err = b.getDataHead(ctx); b.errors.IgnoreNotFound(err) != nil {
		return
	}

	if dataHead != nil {
		defer dataHead.Close()
	}

	dmMapping = make(map[git.ObjectID]git.ObjectID)

	if err = b.loadDataMetadataMapping(ctx, metaHead, dmMapping); b.errors.IgnoreNotFound(err) != nil {
		return
	}

	if metaHead != nil {
		metaHead.Close()
		metaHead = nil
	}

	if mID, err = b.initMetadataForData(ctx, dataHead, startRevision, version, dmMapping); err != nil {
		return
	}

	if metaHead, err = b.repo.ObjectGetter().GetCommit(ctx, mID); err != nil {
		return
	}

	if metaT, err = b.repo.Peeler().PeelToTree(ctx, metaHead); err != nil {
		return
	}

	if revision, err = b.readRevision(ctx, metaT, metadataPathRevision); err != nil {
		return
	}

	if dID, err = b.readOjectID(ctx, metaT, metadataPathData); err != nil {
		return
	}

	err = b.advanceReferences(ctx, true, mID, dataHead == nil || !reflect.DeepEqual(dataHead.ID(), dID), dID, revision)
	return
}
