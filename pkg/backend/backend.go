package backend

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"

	"github.com/go-logr/logr"
)

// Version of the backend. Set during go build.
var Version string

// committerConfig defines the committer configuration to be used when creating Git commits.
type commitConfig struct {
	committerName, committerEmail string
}

// backend implements an ETCD server backed by a Git repository.
type backend struct {
	sync.RWMutex
	keyPrefix
	repo                     git.Repository
	errors                   git.Errors
	refName, metadataRefName git.ReferenceName
	clusterID, memberID      uint64
	commitConfig             commitConfig
	log                      logr.Logger
	watchDispatchTicker      chan<- time.Time
}

var _ etcdserverpb.KVServer = (*backend)(nil)

const (
	DefaultDataReferenceName     = "refs/heads/main"
	DefaultMetadataReferenceName = "refs/gitcd/metadata/main"
	metadataPathRevision         = ".revision"
	metadataPathData             = ".data"
	metadataPathVersion          = ".version"
)

func (b *backend) getDataRefName() (refName git.ReferenceName, err error) {
	return validateReferenceName(b.refName)
}

func (b *backend) getMetadataRefName() (refName git.ReferenceName, err error) {
	return validateReferenceName(b.metadataRefName)
}

func validateReferenceName(refName git.ReferenceName) (git.ReferenceName, error) {
	if len(refName) == 0 {
		return "", rpctypes.ErrGRPCCorrupt
	}

	return refName, nil
}

func (b *backend) getReference(ctx context.Context, refName git.ReferenceName) (ref git.Reference, err error) {
	var rc git.ReferenceCollection

	if rc, err = b.repo.References(); err != nil {
		return nil, err
	}

	defer rc.Close()

	ref, err = rc.Get(ctx, refName)
	return
}

func (b *backend) getMetadataReference(ctx context.Context) (ref git.Reference, err error) {
	var refName git.ReferenceName

	if refName, err = b.getMetadataRefName(); err != nil {
		return
	}

	ref, err = b.getReference(ctx, refName)
	return
}

func (b *backend) Start(ctx context.Context) error { return nil }

func NewUnsupportedObjectType(typ git.ObjectType) error {
	return fmt.Errorf("unsupported ObjectType %v", typ)
}

func (b *backend) getContent(ctx context.Context, t git.Tree, path string) (v []byte, err error) {
	var te git.TreeEntry

	if te, err = t.GetEntryByPath(ctx, path); err != nil {
		return
	}

	return b.getContentForTreeEntry(ctx, te)
}

func (b *backend) getContentForTreeEntry(ctx context.Context, te git.TreeEntry) (v []byte, err error) {
	var (
		typ  git.ObjectType
		blob git.Blob
	)

	if typ = te.EntryType(); typ != git.ObjectTypeBlob {
		err = NewUnsupportedObjectType(typ)
		return
	}

	if blob, err = b.repo.ObjectGetter().GetBlob(ctx, te.EntryID()); err != nil {
		return
	}

	defer blob.Close()

	return blob.Content()
}

func (b *backend) readRevision(ctx context.Context, t git.Tree, path string) (revision int64, err error) {
	var v []byte

	if v, err = b.getContent(ctx, t, path); err != nil {
		return
	}

	return strconv.ParseInt(string(v), 10, 64)
}

func (b *backend) readRevisionFromTreeEntry(ctx context.Context, te git.TreeEntry) (revision int64, err error) {
	var v []byte

	if v, err = b.getContentForTreeEntry(ctx, te); err != nil {
		return
	}

	return strconv.ParseInt(string(v), 10, 64)
}

func (b *backend) readRevisionFromMetaPeelable(ctx context.Context, metaP git.Peelable) (revision int64, err error) {
	var t git.Tree

	if t, err = b.repo.Peeler().PeelToTree(ctx, metaP); err != nil {
		return
	}

	defer t.Close()

	return b.readRevision(ctx, t, metadataPathRevision)
}

func revisionToString(revision int64) string {
	return strconv.FormatInt(revision, 10)
}

func (b *backend) readOjectID(ctx context.Context, t git.Tree, path string) (git.ObjectID, error) {
	var (
		v   []byte
		err error
	)

	if v, err = b.getContent(ctx, t, path); err != nil {
		return git.ObjectID{}, err
	}

	return git.NewObjectID(string(v))
}

func (b *backend) getMetadataFor(ctx context.Context, metaRoot git.Tree, k string) (kv *mvccpb.KeyValue, err error) {
	var (
		t   git.Tree
		te  git.TreeEntry
		typ git.ObjectType
	)

	if te, err = metaRoot.GetEntryByPath(ctx, b.getPathForKey(k)); err != nil {
		return
	}

	if typ = te.EntryType(); typ != git.ObjectTypeTree {
		err = NewUnsupportedObjectType(typ)
		return
	}

	if t, err = b.repo.ObjectGetter().GetTree(ctx, te.EntryID()); err != nil {
		return
	}

	defer t.Close()

	kv, err = b.loadKeyValue(ctx, t, []byte(k), nil)
	return
}

func (b *backend) loadKeyValue(ctx context.Context, t git.Tree, k, v []byte) (kv *mvccpb.KeyValue, err error) {
	var createRevision, lease, modRevision, version int64

	if createRevision, err = b.readRevision(ctx, t, etcdserverpb.Compare_CREATE.String()); err != nil {
		return
	}

	if lease, err = b.readRevision(ctx, t, etcdserverpb.Compare_LEASE.String()); b.errors.IgnoreNotFound(err) != nil {
		return
	}

	if modRevision, err = b.readRevision(ctx, t, etcdserverpb.Compare_MOD.String()); err != nil {
		return
	}

	if version, err = b.readRevision(ctx, t, etcdserverpb.Compare_VERSION.String()); err != nil {
		return
	}

	kv = &mvccpb.KeyValue{
		Key:            k,
		CreateRevision: createRevision,
		Lease:          lease,
		ModRevision:    modRevision,
		Version:        version,
		Value:          v,
	}
	return
}

// getPeelablesForRevision returns the metadata Peelable that corresponds to the given revision.
// The metadata commits are searched in pre-order starting from the given metaHead.
func (b *backend) getMetadataPeelableForRevision(ctx context.Context, metaHead git.Commit, revision int64) (metaP git.Peelable, err error) {
	var (
		cw      = b.repo.CommitWalker()
		headRev int64
	)

	defer cw.Close()

	if err = cw.ForEachCommit(ctx, metaHead, func(ctx context.Context, c git.Commit) (done, skip bool, err error) {
		var (
			t   git.Tree
			rev int64
		)

		if t, err = b.repo.Peeler().PeelToTree(ctx, c); err != nil {
			return
		}

		defer t.Close()

		if rev, err = b.readRevision(ctx, t, metadataPathRevision); err != nil {
			return
		}

		if headRev == 0 {
			headRev = rev
		}

		if rev < revision {
			skip = true
			return
		}

		if rev == revision {
			done = true

			// Reload the commit to avoid it being closed by the commit walker.
			if metaP, err = b.repo.ObjectGetter().GetCommit(ctx, c.ID()); err != nil {
				return
			}

			return
		}

		return
	}); err != nil {
		return
	}

	if metaP == nil {
		switch {
		case headRev < revision:
			err = rpctypes.ErrGRPCFutureRev
		default:
			err = rpctypes.ErrGRPCCompacted
		}
	}

	return
}

func (b *backend) getDataCommitForMetadata(ctx context.Context, metaRoot git.Tree) (dataP git.Commit, err error) {
	var id git.ObjectID

	if id, err = b.readOjectID(ctx, metaRoot, metadataPathData); err != nil {
		return
	}

	return b.repo.ObjectGetter().GetCommit(ctx, id)
}

func (b *backend) newResponseHeaderWithRevision(revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		ClusterId: b.clusterID,
		MemberId:  b.memberID,
		Revision:  revision,
		RaftTerm:  uint64(revision),
	}
}

func (b *backend) newResponseHeaderFromMetaTree(ctx context.Context, metaRoot git.Tree) *etcdserverpb.ResponseHeader {
	var revision int64

	if metaRoot != nil {
		revision, _ = b.readRevision(ctx, metaRoot, metadataPathRevision)
	}

	return b.newResponseHeaderWithRevision(revision)
}

func (b *backend) newResponseHeaderFromMetaPeelable(ctx context.Context, metaP git.Peelable) *etcdserverpb.ResponseHeader {
	var metaRoot git.Tree

	if metaP != nil {
		if metaRoot, _ = b.repo.Peeler().PeelToTree(ctx, metaP); metaRoot != nil {
			defer metaRoot.Close()
		}
	}

	return b.newResponseHeaderFromMetaTree(ctx, metaRoot)
}

func (b *backend) newResponseHeader(ctx context.Context) *etcdserverpb.ResponseHeader {
	var metaP git.Peelable

	if metaP, _ = b.getMetadataReference(ctx); metaP != nil {
		defer metaP.Close()
	}

	return b.newResponseHeaderFromMetaPeelable(ctx, metaP)
}

func newClosedOpenInterval(start, end []byte) interval {
	var i = &closedOpenInterval{start: key(start)}

	if end != nil {
		i.end = key(end)
	}

	return i
}

// commitTreeFunc defines the contract to create a new commit based on a new Tree and an existing commit.
// The currentCommit might be nil if this is the first commit.
type commitTreeFunc func(ctx context.Context, message string, newTreeID git.ObjectID, currentCommit git.Commit) (newCommitID git.ObjectID, err error)

func (b *backend) newCommitBuilder(ctx context.Context) (cb git.CommitBuilder, err error) {
	if cb, err = b.repo.CommitBuilder(ctx); err != nil {
		return
	}

	defer func() {
		if err != nil { // close the builder and return nil builder on error
			cb.Close()
			cb = nil
		}
	}()

	if err = cb.SetCommitterName(b.commitConfig.committerName); err != nil {
		return
	}

	err = cb.SetCommitterEmail(b.commitConfig.committerEmail)
	return
}

func (b *backend) createCommit(
	ctx context.Context,
	message string,
	newTreeID git.ObjectID,
	decorateFn func(context.Context, git.CommitBuilder) error,
) (newCommitID git.ObjectID, err error) {
	var cb git.CommitBuilder

	if cb, err = b.newCommitBuilder(ctx); err != nil {
		return
	}

	defer cb.Close()

	if err = cb.SetMessage(message); err != nil {
		return
	}

	if err = cb.SetTreeID(newTreeID); err != nil {
		return
	}

	if decorateFn != nil {
		if err = decorateFn(ctx, cb); err != nil {
			return
		}
	}

	newCommitID, err = cb.Build(ctx)
	return
}

func (b *backend) replaceCurrentCommit(ctx context.Context, message string, newTreeID git.ObjectID, currentCommit git.Commit) (newCommitID git.ObjectID, err error) {
	return b.createCommit(ctx, message, newTreeID, func(ctx context.Context, cb git.CommitBuilder) (err error) {
		if currentCommit == nil {
			return
		}

		err = currentCommit.ForEachParentID(ctx, func(_ context.Context, id git.ObjectID) (done bool, err error) {
			err = cb.AddParentIDs(id)
			return
		})

		return
	})
}

func (b *backend) inheritCurrentCommit(ctx context.Context, message string, newTreeID git.ObjectID, currentCommit git.Commit) (newCommitID git.ObjectID, err error) {
	return b.createCommit(ctx, message, newTreeID, func(ctx context.Context, cb git.CommitBuilder) (err error) {
		if currentCommit == nil {
			return
		}

		err = cb.AddParentIDs(currentCommit.ID())
		return
	})
}

func (b *backend) mutateRevisionConditionallyTo(
	conditionFn func(context.Context, git.TreeEntry) (check bool, revision int64, err error),
) mutation.MutateTreeEntryFunc {
	return func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
		var (
			check    bool
			revision int64
		)

		if check, revision, err = conditionFn(ctx, te); err != nil || !check {
			return
		}

		mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(revision)), te)
		return
	}
}

func (b *backend) mutateRevisionTo(newRevision int64) mutation.MutateTreeEntryFunc {
	return b.mutateRevisionConditionallyTo(func(ctx context.Context, te git.TreeEntry) (check bool, revision int64, err error) {
		var currentRevision int64

		revision = newRevision

		if check = te == nil || te.EntryType() != git.ObjectTypeBlob; check {
			return
		}

		if currentRevision, err = b.readRevisionFromTreeEntry(ctx, te); b.errors.IgnoreNotFound(err) != nil {
			return
		}

		check, err = currentRevision != newRevision, nil
		return
	})
}

func (b *backend) mutateRevisionIfNotExistsTo(newRevision int64) mutation.MutateTreeEntryFunc {
	return b.mutateRevisionConditionallyTo(func(_ context.Context, te git.TreeEntry) (bool, int64, error) {
		return te == nil || te.EntryType() != git.ObjectTypeBlob, newRevision, nil
	})
}

func (b *backend) incrementRevision() mutation.MutateTreeEntryFunc {
	return b.mutateRevisionConditionallyTo(func(ctx context.Context, te git.TreeEntry) (check bool, revision int64, err error) {
		defer func() { revision++ }()

		if check = te == nil || te.EntryType() != git.ObjectTypeBlob; check {
			return
		}

		if revision, err = b.readRevisionFromTreeEntry(ctx, te); b.errors.IgnoreNotFound(err) != nil {
			return
		}

		check, err = true, nil
		return
	})
}

func (b *backend) createBlob(ctx context.Context, content []byte) (blobID git.ObjectID, err error) {
	var bb git.BlobBuilder

	if bb, err = b.repo.BlobBuilder(ctx); err != nil {
		return
	}

	defer bb.Close()

	if err = bb.SetContent(content); err != nil {
		return
	}

	blobID, err = bb.Build(ctx)
	return
}

func (b *backend) addOrReplaceTreeEntryMutateFn(newContent []byte) mutation.MutateTreeEntryFunc {
	return func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
		mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, newContent, te)
		return
	}
}

func (b *backend) addOrReplaceTreeEntry(ctx context.Context, tb git.TreeBuilder, entryName string, newContent []byte, te git.TreeEntry) (mutated bool, err error) {
	var entryID git.ObjectID

	if entryID, err = b.createBlob(ctx, newContent); err != nil {
		return
	}

	if te != nil {
		if err = tb.RemoveEntry(entryName); err != nil {
			return
		}
	}

	err = tb.AddEntry(entryName, entryID, git.FilemodeBlob)
	mutated = true

	return
}

func (b *backend) advanceReferences(
	ctx context.Context,
	metaMutated bool,
	newMetaHeadID git.ObjectID,
	dataMutated bool,
	newDataHeadID git.ObjectID,
	revision int64,
) (err error) {
	var metaRefName, dataRefName git.ReferenceName

	if metaRefName, err = b.getMetadataRefName(); err != nil {
		return
	}

	if dataRefName, err = b.getDataRefName(); err != nil {
		return
	}

	for _, e := range []struct {
		mutated bool
		refName git.ReferenceName
		headID  git.ObjectID
	}{
		{mutated: metaMutated, refName: metaRefName, headID: newMetaHeadID},
		{mutated: dataMutated, refName: dataRefName, headID: newDataHeadID},
	} {
		var rc git.ReferenceCollection

		if !e.mutated {
			continue
		}

		if rc, err = b.repo.References(); err != nil {
			return
		}

		defer rc.Close()

		if err = rc.Create(ctx, e.refName, e.headID, true, revisionToString(revision)); err != nil {
			return
		}
	}

	if err = b.tickWatchDispatchTicker(ctx); err != nil {
		b.log.Error(err, "Error ticking watch dispatch ticker")
	}

	return
}

const watchDispatchTickTimeout = 10 * time.Millisecond

func (b *backend) tickWatchDispatchTicker(ctx context.Context) error {
	var (
		now      time.Time
		cancelFn context.CancelFunc
	)

	if b.watchDispatchTicker == nil {
		return nil
	}

	now = time.Now()
	ctx, cancelFn = context.WithTimeout(ctx, watchDispatchTickTimeout)

	defer cancelFn()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b.watchDispatchTicker <- now:
			b.log.V(-1).Info("Ticked watch dispatcher ticker", "time", now)
			return nil
		}
	}
}

func setHeaderRevision(h *etcdserverpb.ResponseHeader, mutated bool, newRevision int64) {
	if mutated {
		h.Revision = newRevision
		h.RaftTerm = uint64(newRevision)
	}
}

func perfCounter() func() time.Duration {
	var t = time.Now()

	return func() time.Duration { return time.Since(t) }
}
