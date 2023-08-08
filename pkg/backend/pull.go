package backend

import (
	"context"
	"path"
	"time"

	"github.com/go-logr/logr"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
	"github.com/trishanku/gitcd/pkg/util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TODO Test
type PullOptionFunc func(*puller) error

func NewPull(optFns ...PullOptionFunc) (err error) {
	var p = &puller{}

	for _, fn := range optFns {
		if err = fn(p); err != nil {
			return
		}
	}

	return
}

type pullOpts struct{}

var PullOptions = pullOpts{}

func (pullOpts) WithBackend(kvs etcdserverpb.KVServer) PullOptionFunc {
	return func(p *puller) (err error) {
		var b *backend

		if b, err = checkIsBackend(kvs); err != nil {
			return
		}

		p.backend = b
		return
	}
}

func (opts pullOpts) WithMergeConfictResolutions(conflictResolutions []git.MergeConfictResolution) PullOptionFunc {
	return func(p *puller) error {
		p.ensureRemoteSpecs(len(conflictResolutions))
		for i, conflictResolution := range conflictResolutions {
			p.remoteSpecs[i].conflictResolution = conflictResolution
		}
		return nil
	}
}

func (opts pullOpts) WithMergeRetentionPolicies(retentionPolicies []git.MergeRetentionPolicy) PullOptionFunc {
	return func(p *puller) error {
		p.ensureRemoteSpecs(len(retentionPolicies))
		for i, retentionPolicy := range retentionPolicies {
			p.remoteSpecs[i].retentionPolicy = retentionPolicy
		}
		return nil
	}
}

func (pullOpts) WithRemoteNames(remoteNames []git.RemoteName) PullOptionFunc {
	return func(p *puller) error {
		p.ensureRemoteSpecs(len(remoteNames))

		for i, remoteName := range remoteNames {
			p.remoteSpecs[i].name = remoteName
		}

		return nil
	}
}

func (pullOpts) WithRemoteDataRefNames(dataRefNames []git.ReferenceName) PullOptionFunc {
	return func(p *puller) error {
		p.ensureRemoteSpecs(len(dataRefNames))

		for i, dataRefName := range dataRefNames {
			p.remoteSpecs[i].dataRefName = dataRefName
		}

		return nil
	}
}

func (pullOpts) WithRemoteMetadataRefNames(metadataRefNames []git.ReferenceName) PullOptionFunc {
	return func(p *puller) error {
		p.ensureRemoteSpecs(len(metadataRefNames))

		for i, metadataRefName := range metadataRefNames {
			p.remoteSpecs[i].metadataRefName = metadataRefName
		}

		return nil
	}
}

func (pullOpts) WithNoFastForward(noFastForward bool) PullOptionFunc {
	return func(p *puller) error {
		p.noFastForward = noFastForward
		return nil
	}
}

func (pullOpts) WithNoFetch(noFetch bool) PullOptionFunc {
	return func(p *puller) error {
		p.noFetch = noFetch
		return nil
	}
}

func (pullOpts) WithPushAfterMerge(pushAfterMerge bool) PullOptionFunc {
	return func(p *puller) error {
		p.pushAfterMerge = pushAfterMerge
		return nil
	}
}

func (pullOpts) WithDataPushRefSpec(dataPushRefSpec git.RefSpec) PullOptionFunc {
	return func(p *puller) error {
		p.dataPushRefSpec = dataPushRefSpec
		return nil
	}
}

func (pullOpts) WithMetadataPushRefSpec(metadataPushRefSpec git.RefSpec) PullOptionFunc {
	return func(p *puller) error {
		p.metadataPushRefSpec = metadataPushRefSpec
		return nil
	}
}

func (pullOpts) WithTicker(ticker <-chan time.Time) PullOptionFunc {
	return func(p *puller) error {
		p.ticker = ticker
		return nil
	}
}

func (pullOpts) WithLogger(log logr.Logger) PullOptionFunc {
	return func(p *puller) error {
		p.log = log
		return nil
	}
}

func (pullOpts) WithContext(ctx context.Context) PullOptionFunc {
	return func(p *puller) error {
		go func() {
			if err := p.Run(ctx); err != nil {
				p.log.Error(err, "Error running. Terminating...")
			}
		}()
		return nil
	}
}

func (pullOpts) WithOnce(ctx context.Context) PullOptionFunc {
	return func(p *puller) (err error) {
		for _, rs := range p.remoteSpecs {
			if err = p.pullRemote(ctx, rs); err != nil {
				return
			}
		}

		return
	}
}

const (
	DefaultRemoteName            = "origin"
	DefaultRemoteDataRefName     = "refs/remotes/origin/main"
	DefaultRemoteMetadataRefName = "refs/remotes/origin/gitcd/metadata/refs/heads/main"
)

type remoteSpec struct {
	name               git.RemoteName
	dataRefName        git.ReferenceName
	metadataRefName    git.ReferenceName
	conflictResolution git.MergeConfictResolution
	retentionPolicy    git.MergeRetentionPolicy
}

func (rs *remoteSpec) getName() git.RemoteName {
	if len(rs.name) <= 0 {
		rs.name = DefaultRemoteName
	}

	return rs.name
}

func (rs *remoteSpec) getDataRefName() git.ReferenceName {
	if len(rs.dataRefName) <= 0 {
		rs.dataRefName = DefaultRemoteDataRefName
	}

	return rs.dataRefName
}

func (rs *remoteSpec) getMetadataRefName() git.ReferenceName {
	if len(rs.metadataRefName) <= 0 {
		rs.metadataRefName = DefaultRemoteMetadataRefName
	}

	return rs.metadataRefName
}

func (rs *remoteSpec) getConflictResolution() git.MergeConfictResolution {
	if rs.conflictResolution <= 0 {
		rs.conflictResolution = git.DefaultConflictResolution
	}

	return rs.conflictResolution
}

func (rs *remoteSpec) getRetentionPolicy() git.MergeRetentionPolicy {
	if rs.retentionPolicy == nil {
		rs.retentionPolicy = git.DefaultMergeRetentionPolicy
	}

	return rs.retentionPolicy
}

// puller helps pull changes from a configured remote Git repository into the backend.
type puller struct {
	backend *backend

	remoteSpecs []*remoteSpec

	noFastForward  bool
	noFetch        bool
	pushAfterMerge bool

	dataPushRefSpec     git.RefSpec
	metadataPushRefSpec git.RefSpec

	ticker <-chan time.Time
	log    logr.Logger
}

func (p *puller) ensureRemoteSpecs(n int) {
	if n > len(p.remoteSpecs) {
		p.remoteSpecs = append(p.remoteSpecs, make([]*remoteSpec, n-len(p.remoteSpecs))...)

		for i := range p.remoteSpecs {
			if p.remoteSpecs[i] == nil {
				p.remoteSpecs[i] = &remoteSpec{}
			}
		}
	}
}

func (p *puller) getRemote(ctx context.Context, rs *remoteSpec) (r git.Remote, err error) {
	var (
		b  = p.backend
		rc git.RemoteCollection
	)

	if rc, err = b.repo.Remotes(); err != nil {
		return
	}

	defer rc.Close()

	r, err = rc.Get(ctx, rs.getName())
	return
}

func (p *puller) fetchRemote(ctx context.Context, rs *remoteSpec) (err error) {
	var remote git.Remote

	p.backend.RLock()
	defer p.backend.RUnlock()

	if remote, err = p.getRemote(ctx, rs); err != nil {
		return
	}

	defer remote.Close()

	err = remote.Fetch(ctx, nil)
	return
}

func (p *puller) pushUnsafe(ctx context.Context, rs *remoteSpec, lock bool) (err error) {
	var remote git.Remote

	if lock {
		p.backend.Lock()
		defer p.backend.Unlock()
	}

	if remote, err = p.getRemote(ctx, rs); err != nil {
		return
	}

	defer remote.Close()

	err = remote.Push(ctx, nil)

	return
}

func (p *puller) getRemoteDataHead(ctx context.Context, rs *remoteSpec) (c git.Commit, err error) {
	return p.backend.getHead(ctx, rs.getDataRefName())
}

func (p *puller) getRemoteMetadataHead(ctx context.Context, rs *remoteSpec) (c git.Commit, err error) {
	return p.backend.getHead(ctx, rs.getMetadataRefName())
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

func (p *puller) createMergeCommit(ctx context.Context, message string, mergeTreeID git.ObjectID, parents ...git.Commit) (newHeadID git.ObjectID, err error) {
	newHeadID, err = p.backend.createCommit(
		ctx,
		message,
		mergeTreeID,
		func(ctx context.Context, cb git.CommitBuilder) (err error) {
			for _, p := range parents {
				if p == nil {
					continue
				}

				if err = cb.AddParents(p); err != nil {
					return
				}
			}

			return
		},
	)
	return
}

// Here be hairy monsters! TODO find a better/simpler/more efficient way to deal with this.
//
// Non-fast-forward merge could result in many inconsistencies in metadata.
// 1. If merged value includes changes from both sides, the MOD revision and VERSION will still,
// at best, be the largest of the revisions on either side.
// Ideally, MOD revision should be the new revision corresponding to the merge commit and the
// VERSION should be incremented.
//
// 2. CREATE revision and LEASE metadata could be deleted if modify/delete conflicts are resolved
// in favour of the modification, because those metadata may not be part of the modification and
// hence, not conflicting.
// Ideally, the CREATE revision and LEASE should be retained from the modification.
// TODO: This issue can be completely avoided by consolidating metatata into a single YAML file per key.
//
// 3. The overall new revision (as well as data HEAD commit ID, it data was mutated) should be updated.
type metadataFixerAfterNonFastForwardMerge struct {
	backend                                         *backend
	newRevision                                     int64
	oursDataC, theirsDataC, oursMetaC, theirsMetaC  git.Commit
	dataMutated                                     bool
	dataMergeTreeID, metaMergeTreeID, newDataHeadID git.ObjectID
	message                                         string
}

func (f *metadataFixerAfterNonFastForwardMerge) fix(
	ctx context.Context,
) (
	mutated bool,
	newTreeID git.ObjectID,
	err error,
) {
	var (
		b                                                                      = f.backend
		oursDataT, theirsDataT, oursMetaT, theirsMetaT, dataMergeT, metaMergeT git.Tree
		tm                                                                     *mutation.TreeMutation
	)

	for _, s := range []struct {
		tPtr     *git.Tree
		peelable git.Peelable
		tID      git.ObjectID
		skip     bool
	}{
		{tPtr: &oursDataT, peelable: f.oursDataC},
		{tPtr: &theirsDataT, peelable: f.theirsDataC},
		{tPtr: &oursMetaT, peelable: f.oursMetaC},
		{tPtr: &theirsMetaT, peelable: f.theirsMetaC},
		{tPtr: &dataMergeT, tID: f.dataMergeTreeID, skip: !f.dataMutated},
		{tPtr: &metaMergeT, tID: f.metaMergeTreeID},
	} {
		if s.skip {
			continue
		}

		if s.peelable != nil {
			if *s.tPtr, err = b.repo.Peeler().PeelToTree(ctx, s.peelable); err != nil {
				return
			}
		} else {
			if *s.tPtr, err = b.repo.ObjectGetter().GetTree(ctx, s.tID); err != nil {
				return
			}
		}

		defer (*s.tPtr).Close()
	}

	if f.dataMutated {
		// TODO special case handling where oursDataT or theirsDataT is nil
		var (
			dataDiff, metaDiff git.Diff
			revisionMutateFn   = b.mutateRevisionTo(f.newRevision)
			versionMutateFn    = b.incrementRevision()
		)

		if dataDiff, err = b.repo.TreeDiff(ctx, oursDataT, dataMergeT); err != nil {
			return
		}

		defer dataDiff.Close()

		if err = dataDiff.ForEachDiffChange(ctx, func(ctx context.Context, change git.DiffChange) (done bool, err error) {
			var (
				te git.TreeEntry
				p  = change.Path()
			)

			if change.Type() != git.DiffChangeTypeModified {
				// Only value modifications could have changes from both sides.
				return
			}

			// theirsDataT cannot be nil because dataMutated.
			if te, err = theirsDataT.GetEntryByPath(ctx, change.Path()); b.errors.IgnoreNotFound(err) != nil {
				return
			}

			if err != nil {
				// Entry not found in theirs. Should not happen. Nothing to do.
				return
			}

			if te.EntryID() == change.NewBlobID() {
				// Change came from theirs. Nothing do to.
				return
			}

			// Merged entry is different from both ours and theirs. Update MOD revision and VERSION.
			for p, mutateFn := range map[string]mutation.MutateTreeEntryFunc{
				path.Join(p, etcdserverpb.Compare_MOD.String()):     revisionMutateFn,
				path.Join(p, etcdserverpb.Compare_VERSION.String()): versionMutateFn,
			} {
				func(p string, mutateFn mutation.MutateTreeEntryFunc) {
					if tm, err = mutation.AddMutation(tm, p, mutateFn); err != nil {
						return
					}
				}(p, mutateFn)
			}

			return
		}); err != nil {
			return
		}

		if metaDiff, err = b.repo.TreeDiff(ctx, oursMetaT, metaMergeT); err != nil {
			return
		}

		defer metaDiff.Close()

		if err = metaDiff.ForEachDiffChange(ctx, func(ctx context.Context, change git.DiffChange) (done bool, err error) {
			var (
				p    = change.Path()
				base = util.ToCanonicalPath(path.Base(p))
				dir  = util.ToCanonicalPath(path.Dir(p))
				te   git.TreeEntry
			)

			switch change.Type() {
			case git.DiffChangeTypeAdded:
				if base != etcdserverpb.Compare_MOD.String() {
					// Only addition of MOD revision needs to be checked to see if other metadata are also added.
					return
				}

				// VERSION should have been already handled similar to MOD revision because they change together.
				for mBase, skipNotFound := range map[string]bool{
					etcdserverpb.Compare_CREATE.String(): false,
					etcdserverpb.Compare_LEASE.String():  true,
				} {
					var mPath = path.Join(dir, mBase)

					if _, err = metaMergeT.GetEntryByPath(ctx, mPath); b.errors.IgnoreNotFound(err) != nil {
						return
					}

					if err == nil {
						// Metadata exists in the merge tree. Nothing to do.
						continue
					}

					// Metadata not found. Restore it from theirs which is the only possible place MOD revision would have come from.
					if te, err = theirsMetaT.GetEntryByPath(ctx, mPath); b.errors.IgnoreNotFound(err) != nil {
						return
					} else if err != nil { // Not found.
						if skipNotFound {
							err = nil
							continue // Skip.
						}

						return // Error out.
					}

					if tm, err = mutation.AddMutation(
						tm,
						mPath,
						func(ctx context.Context, tb git.TreeBuilder, entryName string, _ git.TreeEntry) (mutated bool, err error) {
							err = tb.AddEntry(entryName, te.EntryID(), te.EntryMode())
							mutated = err == nil
							return
						},
					); err != nil {
						return
					}

				}
			case git.DiffChangeTypeDeleted:
				var skipNotFound bool

				if base != etcdserverpb.Compare_CREATE.String() && base != etcdserverpb.Compare_LEASE.String() {
					// Only deletion of CREATE revision or LEASE needs to be checked to see if other metadata are also deleted.
					return
				}

				if _, err = metaMergeT.GetEntryByPath(
					ctx,
					path.Join(dir, etcdserverpb.Compare_MOD.String()),
				); b.errors.IgnoreNotFound(err) != nil {
					return
				}

				if err != nil {
					// Not found. MOD revision has also been deleted. Nothing to do.
					err = nil
					return
				}

				// Partial metadata found. Undo the deletion from ours because the partial deletion must have come from theirs.
				// VERSION should have been already handled like MOD revision because they change together.
				skipNotFound = base == etcdserverpb.Compare_LEASE.String()

				if te, err = oursMetaT.GetEntryByPath(ctx, p); b.errors.IgnoreNotFound(err) != nil {
					return
				} else if err != nil { // Not found.
					if skipNotFound {
						err = nil // Skip.
					}

					return // Error out.
				}

				if tm, err = mutation.AddMutation(
					tm,
					p,
					func(ctx context.Context, tb git.TreeBuilder, entryName string, _ git.TreeEntry) (mutated bool, err error) {
						err = tb.AddEntry(entryName, te.EntryID(), te.EntryMode())
						mutated = err == nil
						return
					},
				); err != nil {
					return
				}
			}

			return
		}); err != nil {
			return
		}
	}

	if tm, err = mutation.AddMutation(
		tm,
		metadataPathData,
		f.backend.addOrReplaceTreeEntryMutateFn([]byte(f.newDataHeadID.String())),
	); err != nil {
		return
	}

	if tm, err = mutation.AddMutation(
		tm,
		metadataPathRevision,
		f.backend.addOrReplaceTreeEntryMutateFn([]byte(revisionToString(f.newRevision))),
	); err != nil {
		return
	}

	if mutation.IsMutationNOP(tm) {
		return
	}

	mutated, newTreeID, err = mutation.MutateTree(ctx, b.repo, metaMergeT, tm, true)
	return
}

func (p *puller) newMerger(_ context.Context, rs *remoteSpec) (merger git.Merger) {
	merger = p.backend.repo.Merger(p.backend.errors)
	merger.SetConflictResolution(rs.getConflictResolution())
	merger.SetRetentionPolicy(rs.getRetentionPolicy())

	return
}

func (p *puller) mergeRemote(ctx context.Context, rs *remoteSpec) (err error) {
	var (
		b      = p.backend
		merger = p.newMerger(ctx, rs)
		log    = p.log.WithValues(
			"our-data-reference", b.refName,
			"their-data-reference", rs.getDataRefName(),
		)

		oursDataC, theirsDataC, oursMetaC, theirsMetaC git.Commit
		revision, oursRevision, theirsRevision         int64
		dataMutated, metaMutated                       bool
		newDataHeadID, newMetaHeadID                   git.ObjectID
		message                                        string

		noFastForward = p.noFastForward

		dataHeadFn = func(headFn func(context.Context) (git.Commit, error)) func(ctx context.Context, _ *remoteSpec) (git.Commit, error) {
			return func(ctx context.Context, _ *remoteSpec) (git.Commit, error) {
				return headFn(ctx)
			}
		}
	)

	defer merger.Close()

	log.V(-1).Info("Merging", "conflictResolution", merger.GetConfictResolution(), "retentionPolicy", merger.GetRetentionPolicy().String())
	defer func() {
		log.V(-1).Info("Merged", "error", err, "dataMutated", dataMutated, "metaMutated", metaMutated, "pushAfterMerge", p.pushAfterMerge)
	}()

	p.backend.Lock()
	defer p.backend.Unlock()

	defer func() {
		if err == nil && p.pushAfterMerge {
			err = p.pushUnsafe(ctx, rs, false)
		}
	}()

	for _, s := range []struct {
		ptrC           *git.Commit
		headFn         func(context.Context, *remoteSpec) (git.Commit, error)
		ignoreNotFound bool
	}{
		{ptrC: &theirsDataC, headFn: p.getRemoteDataHead, ignoreNotFound: false},
		{ptrC: &theirsMetaC, headFn: p.getRemoteMetadataHead, ignoreNotFound: false},
		{ptrC: &oursDataC, headFn: dataHeadFn(b.getDataHead), ignoreNotFound: true},
		{ptrC: &oursMetaC, headFn: dataHeadFn(b.getMetadataHead), ignoreNotFound: true},
	} {
		if *s.ptrC, err = s.headFn(ctx, rs); err != nil {
			if !b.errors.IsNotFound(err) {
				return
			} else if !s.ignoreNotFound {
				return
			}
		}

		defer (*s.ptrC).Close()
	}

	for _, s := range []struct {
		ptrRevision *int64
		metaP       git.Peelable
	}{
		{ptrRevision: &oursRevision, metaP: oursMetaC},
		{ptrRevision: &theirsRevision, metaP: theirsMetaC},
	} {
		if s.metaP == nil {
			continue
		}

		if *s.ptrRevision, err = b.readRevisionFromMetaPeelable(ctx, s.metaP); err != nil {
			return
		}
	}

	revision = maxInt64(oursRevision, theirsRevision)

	for i, s := range []struct {
		oursC, theirsC git.Commit
		mutated        *bool
		headID         *git.ObjectID
		createCommitFn git.CreateCommitFunc
	}{
		{
			oursC:   oursDataC,
			theirsC: theirsDataC,
			mutated: &dataMutated,
			headID:  &newDataHeadID,
			createCommitFn: func(ctx context.Context, dataTreeID git.ObjectID, parents ...git.Commit) (commitID git.ObjectID, err error) {
				revision++
				message = revisionToString(revision)

				commitID, err = p.createMergeCommit(ctx, message, dataTreeID, parents...)
				return
			},
		},
		{
			oursC:   oursMetaC,
			theirsC: theirsMetaC,
			mutated: &metaMutated,
			headID:  &newMetaHeadID,
			createCommitFn: func(ctx context.Context, metaTreeID git.ObjectID, parents ...git.Commit) (commitID git.ObjectID, err error) {
				var (
					newMetaMutated     bool
					newMetaMergeTreeID git.ObjectID
					dataC              git.Commit
				)

				if dataC, err = p.backend.repo.ObjectGetter().GetCommit(ctx, newDataHeadID); err != nil {
					log.V(-1).Error(err, "GetCommit")
					return
				}

				defer dataC.Close()

				// revision and message would have already been updated during data merge.

				p.log.Info("Before metadataFixerAfterNonFastForwardMerge", "dataMergeTreeID", dataC.TreeID(), "metaMergeTreeID", metaTreeID, "newDataHeadID", newDataHeadID)

				if newMetaMutated, newMetaMergeTreeID, err = (&metadataFixerAfterNonFastForwardMerge{
					backend:         p.backend,
					newRevision:     revision,
					oursDataC:       oursDataC,
					theirsDataC:     theirsDataC,
					oursMetaC:       oursMetaC,
					theirsMetaC:     theirsMetaC,
					dataMutated:     dataMutated,
					dataMergeTreeID: dataC.TreeID(),
					metaMergeTreeID: metaTreeID,
					newDataHeadID:   newDataHeadID,
					message:         message,
				}).fix(ctx); err != nil {
					log.V(-1).Error(err, "metadataFixerAfterNonFastForwardMerge")
					return
				} else if !newMetaMutated {
					newMetaMergeTreeID = metaTreeID
				}

				commitID, err = p.createMergeCommit(ctx, message, newMetaMergeTreeID, parents...)
				return
			},
		},
	} {
		if *s.mutated, *s.headID, err = merger.MergeCommits(ctx, s.oursC, s.theirsC, noFastForward, s.createCommitFn); err != nil {
			return
		}

		if *s.mutated {
			noFastForward = true // If data mutated, metadata should not be fast-forwarded because at least the data ID needs to change.
		} else {
			*s.headID = s.oursC.ID() // To be safe.
		}

		{
			var (
				spec           = []string{"data", "metadata"}
				ourID, theirID git.ObjectID
			)

			if s.oursC != nil {
				ourID = s.oursC.ID()
			}

			if s.theirsC != nil {
				theirID = s.theirsC.ID()
			}

			log.V(-1).Info(
				"merge info 1",
				"spec", spec[i],
				"ourID", ourID,
				"theirID", theirID,
				"mutated", s.mutated,
				"headID", s.headID,
				"noFastForward", noFastForward,
			)
		}
	}

	log.V(-1).Info(
		"merge advanceReferences",
		"metaMutated", metaMutated,
		"newMetaHeadID", newMetaHeadID,
		"dataMutated", dataMutated,
		"newDataHeadID", newDataHeadID,
		"revision", revision,
	)

	return b.advanceReferences(ctx, metaMutated, newMetaHeadID, dataMutated, newDataHeadID, revision)
}

func (p *puller) pullRemote(ctx context.Context, rs *remoteSpec) (err error) {
	var fns []func(context.Context, *remoteSpec) error

	if !p.noFetch {
		fns = append(fns, p.fetchRemote)
	}

	fns = append(fns, p.mergeRemote)

	for _, fn := range fns {
		if err = fn(ctx, rs); err != nil {
			return
		}
	}

	return
}

func (p *puller) Run(ctx context.Context) (err error) {
	p.log.V(-1).Info("Running")
	defer func() { p.log.V(-1).Info("Stopping", "error", err) }()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-p.ticker:
			if !ok {
				return
			}

			for _, rs := range p.remoteSpecs {
				if err = p.pullRemote(ctx, rs); err != nil {
					p.log.Error(err, "Error pulling watch queue", "remoteName", rs.getName())
				}
			}
		}
	}
}
