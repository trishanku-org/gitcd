package backend

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

// backend implements an ETCD server backed by a Git repository.
type backend struct {
	repo                git.Repository
	refName             git.ReferenceName
	keyPrefix           string
	clusterID, memberID uint64
}

var _ etcdserverpb.KVServer = &backend{}

const (
	metadataReferencePrefix       = "refs/gitcd/metadata/"
	metadataPathRevision          = ".revision"
	metadataPathData              = ".data"
	metadataPathKeyCreateRevision = ".createRevision"
	metadataPathKeyLease          = ".lease"
	metadataPathKeyModRevision    = ".modRevision"
	metadataPathKeyVersion        = ".version"
)

func (b *backend) getMetadataRefName() git.ReferenceName {
	return git.ReferenceName(metadataReferencePrefix + b.refName)
}

func (b *backend) getReference(ctx context.Context, refName git.ReferenceName) (git.Reference, error) {
	if refs, err := b.repo.References(); err != nil {
		return nil, err
	} else {
		return refs.Get(ctx, refName)
	}
}

func (b *backend) getDataReference(ctx context.Context) (git.Reference, error) {
	return b.getReference(ctx, b.refName)
}

func (b *backend) getMetadataReference(ctx context.Context) (git.Reference, error) {
	return b.getReference(ctx, b.getMetadataRefName())
}

func (b *backend) Start(ctx context.Context) error { return nil }

func (b *backend) getPathForKey(key string) string {
	const defaultPrefix = "/"
	var prefix = b.keyPrefix

	if len(prefix) == 0 {
		prefix = defaultPrefix
	}

	if strings.HasPrefix(key, prefix) {
		return key[len(prefix):]
	} else {
		return ""
	}
}

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

func appendRevision(content []byte, revision int64) ([]byte, error) {
	return strconv.AppendInt(content, revision, 10), nil
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

func checkMinConstraint(constraint, value int64) bool {
	return constraint <= 0 || constraint <= value
}

func checkMaxConstraint(constraint, value int64) bool {
	return constraint <= 0 || value <= constraint
}

func (b *backend) getMetadataFor(ctx context.Context, metaRoot git.Tree, k string) (kv *mvccpb.KeyValue, err error) {
	var (
		t   git.Tree
		te  git.TreeEntry
		typ git.ObjectType
	)

	if te, err = t.GetEntryByPath(ctx, b.getPathForKey(k)); err != nil {
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

	kv = &mvccpb.KeyValue{Key: []byte(k)}

	if kv.CreateRevision, err = b.readRevision(ctx, t, metadataPathKeyCreateRevision); err != nil {
		return
	}

	if kv.Lease, err = b.readRevision(ctx, t, metadataPathKeyLease); err != nil {
		return
	}

	if kv.ModRevision, err = b.readRevision(ctx, t, metadataPathKeyModRevision); err != nil {
		return
	}

	if kv.Version, err = b.readRevision(ctx, t, metadataPathKeyVersion); err != nil {
		return
	}

	return
}

// getPeelablesForRevision returns the metadata Peelable that corresponds to the given revision.
// The metadata commits are searched in pre-order starting from the given metaHead.
func (b *backend) getMetadataPeelableForRevision(ctx context.Context, metaHead git.Commit, revision int64) (metaP git.Peelable, _ error) {
	if err := b.repo.CommitWalker().ForEachCommit(ctx, metaHead, func(ctx context.Context, c git.Commit) (done, skip bool, err error) {
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

		if rev < revision {
			skip, err = true, rpctypes.ErrGRPCFutureRev
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
		return metaP, err
	}

	if metaP == nil {
		return metaP, rpctypes.ErrGRPCCompacted
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
		var err error

		if metaRoot, err = b.repo.Peeler().PeelToTree(ctx, metaP); err == nil {
			defer metaRoot.Close()
		}
	}

	return b.newResponseHeaderFromMetaTree(ctx, metaRoot)
}

func (b *backend) newResponseHeader(ctx context.Context) *etcdserverpb.ResponseHeader {
	var (
		metaP git.Peelable
		err   error
	)

	if metaP, err = b.getMetadataReference(ctx); err == nil {
		defer metaP.Close()
	}

	return b.newResponseHeaderFromMetaPeelable(ctx, metaP)
}

func (b *backend) doRange(ctx context.Context, metaHead git.Commit, req *etcdserverpb.RangeRequest) (res *etcdserverpb.RangeResponse, err error) {
	var (
		dataP, metaP       git.Peelable
		metaRoot, dataRoot git.Tree
		revision           int64
	)

	if req.GetRevision() == 0 {
		metaP = metaHead
	} else if metaP, err = b.getMetadataPeelableForRevision(ctx, metaHead, req.GetRevision()); err != nil {
		return
	} else {
		defer metaP.Close()
	}

	if metaRoot, err = b.repo.Peeler().PeelToTree(ctx, metaP); err != nil {
		return
	}

	defer metaRoot.Close()

	if dataP, err = b.getDataCommitForMetadata(ctx, metaRoot); err != nil {
		return
	}

	defer dataP.Close()

	if dataRoot, err = b.repo.Peeler().PeelToTree(ctx, dataP); err != nil {
		return
	}

	defer dataRoot.Close()

	if revision, err = b.readRevision(ctx, metaRoot, metadataPathRevision); err != nil {
		return
	}

	res = &etcdserverpb.RangeResponse{
		Header: b.newResponseHeaderWithRevision(revision),
	}

	err = (&intervalExplorer{
		keyPrefix: b.keyPrefix,
		repo:      b.repo,
		tree:      dataRoot,
		interval:  &closedOpenInterval{start: key(req.GetKey()), end: key(req.GetRangeEnd())},
	}).forEachMatchingKey(
		ctx,
		func(ctx context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
			var kv *mvccpb.KeyValue

			if len(res.Kvs) >= int(req.GetLimit()) {
				res.More = true
				done = true
				return
			}

			if kv, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
				return
			}

			for _, satisfied := range []bool{
				checkMaxConstraint(req.MaxCreateRevision, kv.CreateRevision),
				checkMinConstraint(req.MinCreateRevision, kv.CreateRevision),
				checkMaxConstraint(req.MaxModRevision, kv.ModRevision),
				checkMinConstraint(req.MinModRevision, kv.ModRevision),
			} {
				if !satisfied {
					return
				}
			}

			if req.CountOnly {
				res.Count++
				return
			}

			if req.KeysOnly {
				return
			}

			kv.Value, err = b.getContentForTreeEntry(ctx, te)

			return
		},
		newObjectTypeFilter(git.ObjectTypeBlob),
	)

	return
}

func (b *backend) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (res *etcdserverpb.RangeResponse, err error) {
	var (
		metaRef  git.Reference
		metaHead git.Commit
	)

	if metaRef, err = b.getMetadataReference(ctx); err != nil {
		return
	}

	defer metaRef.Close()

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err != nil {
		return
	}

	defer metaHead.Close()

	return b.doRange(ctx, metaHead, req)
}

// commitTreeFunc defines the contract to create a new commit based on a new Tree and an existing commit.
// The currentCommit might be nil if this is the first commit.
type commitTreeFunc func(ctx context.Context, message string, newTreeID git.ObjectID, currentCommit git.Commit) (newCommitID git.ObjectID, err error)

func (b *backend) replaceCurrentComit(ctx context.Context, message string, newTreeID git.ObjectID, currentCommit git.Commit) (newCommitID git.ObjectID, err error) {
	var cb git.CommitBuilder

	if cb, err = b.repo.CommitBuilder(ctx); err != nil {
		return
	}

	defer cb.Close()

	if err = cb.SetMessage(message); err != nil {
		return
	}

	if err = cb.SetTreeID(newTreeID); err != nil {
		return
	}

	if currentCommit != nil {
		if err = currentCommit.ForEachParentID(ctx, func(_ context.Context, id git.ObjectID) (done bool, err error) {
			err = cb.AddParentIDs(id)
			return
		}); err != nil {
			return
		}
	}

	newCommitID, err = cb.Build(ctx)

	return
}

func (b *backend) inheritCurrentCommit(ctx context.Context, message string, newTreeID git.ObjectID, currentCommit git.Commit) (newCommitID git.ObjectID, err error) {
	var cb git.CommitBuilder

	if cb, err = b.repo.CommitBuilder(ctx); err != nil {
		return
	}

	defer cb.Close()

	if err = cb.SetMessage(message); err != nil {
		return
	}

	if err = cb.SetTreeID(newTreeID); err != nil {
		return
	}

	if currentCommit != nil {
		if err = cb.AddParentIDs(currentCommit.ID()); err != nil {
			return
		}
	}

	newCommitID, err = cb.Build(ctx)

	return
}

// contentFunc return the new content for a tree entry.
// The current tree entry which is passed as an argument might be nil if such an entry does not exist yet.
// This function can also be used as a callback to simultaneously retrieve previous content as well as set new content.
type contentFunc func(context.Context, git.TreeEntry) (skip bool, content []byte, error error)

// contentTree defines the contract to combile multiple changes to a tree.
type contentTree struct {
	parentPathSlice pathSlice
	entries         map[string]contentFunc
	subtrees        []contentTree
}

func (b *backend) addOrReplaceTreeEntry(ctx context.Context, t git.Tree, tb git.TreeBuilder, ct contentTree) (err error) {
	if len(ct.parentPathSlice) > 0 {
		var (
			entryName string
			entryID   git.ObjectID
			et        git.Tree
			etb       git.TreeBuilder
		)

		entryName = ct.parentPathSlice[0]

		if t != nil {
			var te git.TreeEntry
			if te, err = t.GetEntryByPath(ctx, entryName); err == nil {
				if te.EntryType() == git.ObjectTypeTree {
					if et, err = b.repo.ObjectGetter().GetTree(ctx, te.EntryID()); err != nil {
						return
					}

					defer et.Close()

					if etb, err = b.repo.TreeBuilderFromTree(ctx, et); err != nil {
						return
					}
				}
			} else {
				err = nil // Just to be sure
			}
		}

		if etb == nil {
			if etb, err = b.repo.TreeBuilder(ctx); err != nil {
				return
			}
		}

		defer etb.Close()

		if err = b.addOrReplaceTreeEntry(ctx, et, etb, contentTree{
			parentPathSlice: ct.parentPathSlice[1:],
			entries:         ct.entries,
			subtrees:        ct.subtrees,
		}); err != nil {
			return
		}

		if entryID, err = etb.Build(ctx); err != nil {
			return
		}

		if et != nil {
			if !reflect.DeepEqual(entryID, et.ID()) {
				if err = tb.RemoveEntry(entryName); err != nil {
					return
				}
			} else {
				return // The entry didn't change.
			}
		}

		err = tb.AddEntry(entryName, entryID, git.FilemodeTree)

		return
	}

	for entryName, entryContentFn := range ct.entries {
		var (
			te      git.TreeEntry
			bb      git.BlobBuilder
			entryID git.ObjectID
			skip    bool
			content []byte
		)

		if t != nil {
			if te, err = t.GetEntryByPath(ctx, entryName); err == nil {
				if te.EntryType() != git.ObjectTypeBlob {
					te = nil // Just to be sure
				}
			} else {
				err = nil // Just to be sure
			}
		}

		if bb, err = b.repo.BlobBuilder(ctx); err != nil {
			return
		}

		defer bb.Close()

		if skip, content, err = entryContentFn(ctx, te); err != nil {
			return
		} else if skip {
			continue
		}

		if err = bb.SetContent(content); err != nil {
			return
		}

		if entryID, err = bb.Build(ctx); err != nil {
			return
		}

		if te != nil {
			if !reflect.DeepEqual(entryID, te.EntryID()) {
				if err = tb.RemoveEntry(entryName); err != nil {
					return
				}
			} else {
				continue // The entry didn't change.
			}
		}

		if err = tb.AddEntry(entryName, entryID, git.FilemodeBlob); err != nil {
			return
		}
	}

	for _, subtree := range ct.subtrees {
		if err = b.addOrReplaceTreeEntry(ctx, t, tb, subtree); err != nil {
			return
		}
	}

	return
}

func (b *backend) createOrUpdateTree(ctx context.Context, currentT git.Tree, contentTree contentTree) (newTreeID git.ObjectID, err error) {
	var tb git.TreeBuilder

	if currentT != nil {
		tb, err = b.repo.TreeBuilderFromTree(ctx, currentT)
	} else {
		tb, err = b.repo.TreeBuilder(ctx)
	}

	if err != nil {
		return
	}

	defer tb.Close()

	if err = b.addOrReplaceTreeEntry(ctx, currentT, tb, contentTree); err != nil {
		return
	}

	newTreeID, err = tb.Build(ctx)

	return
}

func (b *backend) doPut(ctx context.Context, metaHead git.Commit, req *etcdserverpb.PutRequest, res *etcdserverpb.PutResponse, commitTreeFn commitTreeFunc) (newMetaHead, newDataHead git.Commit, err error) {
	var (
		metaRoot, dataRoot           git.Tree
		dataHead                     git.Commit
		ie                           *intervalExplorer
		k                            = string(req.GetKey())
		p                            string
		ps                           pathSlice
		prevKV                       *mvccpb.KeyValue
		newMetaContentTree           = contentTree{entries: make(map[string]contentFunc)}
		newMetaKeyContentTree        = contentTree{entries: make(map[string]contentFunc)}
		newRevision                  = res.GetHeader().GetRevision()
		newMetaRootID, newMetaHeadID git.ObjectID
	)

	if metaHead != nil {
		if metaRoot, err = b.repo.Peeler().PeelToTree(ctx, metaHead); err != nil {
			return
		}

		defer metaRoot.Close()

		if dataHead, err = b.getDataCommitForMetadata(ctx, metaRoot); err != nil {
			return
		}

		defer dataHead.Close()

		if dataRoot, err = b.repo.Peeler().PeelToTree(ctx, dataHead); err != nil {
			return
		}

		defer dataRoot.Close()
	}

	ie = &intervalExplorer{
		keyPrefix: b.keyPrefix,
		repo:      b.repo,
	}

	if p = ie.getPathForKey(k); len(p) == 0 {
		err = rpctypes.ErrEmptyKey
		return

	}

	if ps = splitPath(p); len(ps) == 0 {
		err = rpctypes.ErrEmptyKey
		return
	}

	newMetaKeyContentTree.parentPathSlice = ps

	if !req.IgnoreValue {
		var newDataRootID git.ObjectID

		if newDataRootID, err = b.createOrUpdateTree(ctx, dataRoot, contentTree{
			parentPathSlice: ps[1:],
			entries: map[string]contentFunc{
				ps[0]: func(ctx context.Context, te git.TreeEntry) (skip bool, content []byte, err error) {
					content = req.GetValue()

					if te != nil {
						if prevKV, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
							return
						}

						if prevKV.Value, err = b.getContentForTreeEntry(ctx, te); err != nil {
							return
						}

						if req.GetPrevKv() {
							res.PrevKv = prevKV
						}

						skip = reflect.DeepEqual(prevKV.Value, content)
					}

					return
				},
			},
		}); err != nil {
			return
		}

		if dataRoot == nil || !reflect.DeepEqual(newDataRootID, dataRoot.ID()) {
			newRevision = res.GetHeader().GetRevision() + 1

			newMetaContentTree.entries[metadataPathData] = func(ctx context.Context, _ git.TreeEntry) (skip bool, content []byte, err error) {
				var newDataHeadID git.ObjectID

				if newDataHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newDataRootID, dataHead); err == nil {
					newDataHead, err = b.repo.ObjectGetter().GetCommit(ctx, newDataHeadID)
					content = []byte(newDataHeadID.String())
				}

				return
			}

			newMetaKeyContentTree.entries[metadataPathKeyVersion] = func(ctx context.Context, te git.TreeEntry) (skip bool, content []byte, err error) {
				var currentVersion int64

				if te != nil {
					if currentVersion, err = b.readRevisionFromTreeEntry(ctx, te); err != nil {
						return
					}
				}

				content, err = appendRevision(content, currentVersion+1)
				return
			}
		}
	}

	if !req.GetIgnoreLease() {
		newMetaKeyContentTree.entries[metadataPathKeyLease] = func(ctx context.Context, te git.TreeEntry) (skip bool, content []byte, err error) {
			var (
				currentLease int64
				newLease     = req.GetLease()
			)

			if te != nil {
				if currentLease, err = b.readRevisionFromTreeEntry(ctx, te); err != nil {
					return
				}
			}

			if skip = currentLease == newLease; skip {
				return
			}

			content, err = appendRevision(content, newLease)
			return
		}
	}

	if len(newMetaKeyContentTree.entries) == 0 {
		newMetaHead = metaHead
		return // nop
	}

	newMetaContentTree.entries[metadataPathRevision] = func(ctx context.Context, _ git.TreeEntry) (skip bool, content []byte, err error) {
		if skip = newRevision == res.GetHeader().GetRevision(); skip {
			return
		}

		content, err = appendRevision(content, newRevision)
		return
	}

	newMetaKeyContentTree.entries[metadataPathKeyCreateRevision] = func(ctx context.Context, te git.TreeEntry) (skip bool, content []byte, err error) {
		if skip = te == nil; skip {
			return
		}

		content, err = appendRevision(content, newRevision)
		return
	}

	newMetaKeyContentTree.entries[metadataPathKeyModRevision] = func(ctx context.Context, _ git.TreeEntry) (skip bool, content []byte, err error) {
		if skip = newRevision == res.GetHeader().GetRevision(); skip {
			return
		}

		content, err = appendRevision(content, newRevision)
		return
	}

	newMetaContentTree.subtrees = []contentTree{newMetaKeyContentTree}

	if newMetaRootID, err = b.createOrUpdateTree(ctx, metaRoot, newMetaContentTree); err != nil {
		return
	}

	if metaRoot != nil && reflect.DeepEqual(newMetaRootID, metaRoot.ID()) {
		newMetaHead = metaHead
		return // nop
	}

	if newMetaHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newMetaRootID, metaHead); err != nil {
		return
	}

	if newMetaHead, err = b.repo.ObjectGetter().GetCommit(ctx, newMetaHeadID); err != nil {
		return
	}

	res.Header.Revision = newRevision
	res.Header.RaftTerm = uint64(newRevision)

	return
}

func (b *backend) Put(ctx context.Context, req *etcdserverpb.PutRequest) (res *etcdserverpb.PutResponse, err error) {
	var (
		metaRef                            git.Reference
		metaHead, newMetaHead, newDataHead git.Commit
	)

	res = &etcdserverpb.PutResponse{}

	if metaRef, err = b.getMetadataReference(ctx); err == nil {
		defer metaRef.Close()
	}

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err == nil {
		defer metaHead.Close()
	}

	res.Header = b.newResponseHeaderFromMetaPeelable(ctx, metaHead)

	if newMetaHead, newDataHead, err = b.doPut(ctx, metaHead, req, res, b.inheritCurrentCommit); err != nil {
		return
	}

	for _, e := range []struct {
		refName git.ReferenceName
		head    git.Commit
	}{
		{refName: b.getMetadataRefName(), head: newMetaHead},
		{refName: b.refName, head: newDataHead},
	} {
		var rc git.ReferenceCollection

		if e.head == nil {
			continue
		}

		defer e.head.Close()

		if rc, err = b.repo.References(); err != nil {
			return
		}

		if err = rc.Create(ctx, e.refName, e.head.ID(), true, revisionToString(res.GetHeader().GetRevision())); err != nil {
			return
		}
	}

	return
}

func (b *backend) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (res *etcdserverpb.DeleteRangeResponse, err error) {
	return //TODO
}

func (b *backend) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (res *etcdserverpb.TxnResponse, err error) {
	return //TODO
}

func (b *backend) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (res *etcdserverpb.CompactionResponse, err error) {
	res = &etcdserverpb.CompactionResponse{}
	res.Header = b.newResponseHeader(ctx)
	return
}
