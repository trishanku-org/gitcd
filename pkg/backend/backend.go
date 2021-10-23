package backend

import (
	"context"
	"fmt"
	"path"
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
	metadataReferencePrefix = "refs/gitcd/metadata/"
	metadataPathRevision    = ".revision"
	metadataPathData        = ".data"
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

	if kv.CreateRevision, err = b.readRevision(ctx, t, etcdserverpb.Compare_VERSION.String()); err != nil {
		return
	}

	if kv.Lease, err = b.readRevision(ctx, t, etcdserverpb.Compare_LEASE.String()); err != nil {
		return
	}

	if kv.ModRevision, err = b.readRevision(ctx, t, etcdserverpb.Compare_MOD.String()); err != nil {
		return
	}

	if kv.Version, err = b.readRevision(ctx, t, etcdserverpb.Compare_VERSION.String()); err != nil {
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

func newClosedOpenInterval(start, end []byte) interval {
	var i = &closedOpenInterval{start: key(start)}

	if end != nil {
		i.end = key(end)
	}

	return i
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
		interval:  newClosedOpenInterval(req.GetKey(), req.GetRangeEnd()),
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

func (b *backend) replaceCurrentCommit(ctx context.Context, message string, newTreeID git.ObjectID, currentCommit git.Commit) (newCommitID git.ObjectID, err error) {
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

// mutateFunc defines the contract to mutate a tree entry.
// The current tree entry which is passed as an argument might be nil if such an entry does not exist yet.
// This function can be used as a callback to simultaneously retrieve previous content while mutating the tree entry.
type mutateTreeEntryFunc func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error)

// treeMutation defines the contract to combile multiple changes to a tree.
type treeMutation struct {
	entries  map[string]mutateTreeEntryFunc
	subtrees map[string]*treeMutation
}

func (b *backend) isTreeEmpty(ctx context.Context, treeID git.ObjectID) (empty bool, err error) {
	var t git.Tree

	if t, err = b.repo.ObjectGetter().GetTree(ctx, treeID); err != nil {
		return
	}

	defer t.Close()

	err = t.ForEachEntry(ctx, func(_ context.Context, te git.TreeEntry) (done bool, err error) {
		empty = false
		done = true
		return
	})

	return
}

func (b *backend) mutateTreeBuilder(ctx context.Context, t git.Tree, tb git.TreeBuilder, tm *treeMutation, cleanupEmptySubtrees bool) (mutated bool, err error) {
	// Process subtrees first to ensure depth first mutation.

	for entryName, stm := range tm.subtrees {
		var (
			te             git.TreeEntry
			st             git.Tree
			stb            git.TreeBuilder
			subtreeMutated bool
			entryID        git.ObjectID
		)

		if t != nil {
			if te, err = t.GetEntryByPath(ctx, entryName); err == nil {
				if te.EntryType() == git.ObjectTypeTree {
					if st, err = b.repo.ObjectGetter().GetTree(ctx, te.EntryID()); err != nil {
						return
					}

					defer st.Close()

					if stb, err = b.repo.TreeBuilderFromTree(ctx, st); err != nil {
						return
					}
				} else {
					te = nil // Just to be sure
				}
			} else {
				err = nil // Just to be sure
			}
		}

		if stb == nil {
			if stb, err = b.repo.TreeBuilder(ctx); err != nil {
				return
			}
		}

		defer stb.Close()

		if subtreeMutated, err = b.mutateTreeBuilder(ctx, st, stb, stm, cleanupEmptySubtrees); err != nil {
			return
		}

		if !subtreeMutated {
			continue
		}

		if entryID, err = stb.Build(ctx); err != nil {
			return
		}

		if st != nil {
			if subtreeMutated = !reflect.DeepEqual(entryID, st.ID()); subtreeMutated {
				if err = tb.RemoveEntry(entryName); err != nil {
					return
				}
			}
		}

		if !subtreeMutated {
			continue
		}

		mutated = mutated || subtreeMutated

		if cleanupEmptySubtrees {
			var empty bool

			if empty, err = b.isTreeEmpty(ctx, entryID); err != nil {
				return
			}

			if empty {
				continue // Skip adding the entry. It would already have been removed above.
			}
		}

		if err = tb.AddEntry(entryName, entryID, git.FilemodeTree); err != nil {
			return
		}
	}

	for entryName, entryMutateFn := range tm.entries {
		var (
			te           git.TreeEntry
			entryMutated bool
		)

		if t != nil {
			te, _ = t.GetEntryByPath(ctx, entryName)
		}

		if entryMutated, err = entryMutateFn(ctx, tb, entryName, te); err != nil {
			return
		}

		mutated = mutated || entryMutated
	}

	return
}

func (b *backend) mutateTree(ctx context.Context, currentT git.Tree, tm *treeMutation, cleanupEmptySubtrees bool) (mutated bool, newTreeID git.ObjectID, err error) {
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

	if mutated, err = b.mutateTreeBuilder(ctx, currentT, tb, tm, cleanupEmptySubtrees); err != nil {
		return
	}

	if !mutated {
		return
	}

	if newTreeID, err = tb.Build(ctx); err != nil {
		return
	}

	if currentT != nil {
		mutated = !reflect.DeepEqual(newTreeID, currentT.ID())
	}

	return
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

func appendMutationPathSlice(tm *treeMutation, ps pathSlice, entryName string, entryMutateFn mutateTreeEntryFunc) (newTM *treeMutation, err error) {
	var (
		stName string
		stm    *treeMutation
		stOK   bool
	)

	if tm == nil {
		tm = &treeMutation{}
	}

	if len(ps) == 0 {
		if tm.entries == nil {
			tm.entries = make(map[string]mutateTreeEntryFunc)
		}

		tm.entries[entryName] = entryMutateFn // TODO error if already exists

		newTM = tm
		return
	}

	if tm.subtrees == nil {
		tm.subtrees = make(map[string]*treeMutation)
	}

	stName = ps[0]

	if stm, stOK = tm.subtrees[stName]; !stOK {
		stm = &treeMutation{}
	}

	tm.subtrees[stName], err = appendMutationPathSlice(stm, ps[1:], entryName, entryMutateFn)

	newTM = tm
	return
}

func appendMutation(tm *treeMutation, p string, mutateFn mutateTreeEntryFunc) (newTM *treeMutation, err error) {
	var ps = splitPath(p)

	if len(ps) == 0 {
		newTM = tm
		err = rpctypes.ErrEmptyKey
		return
	}

	newTM, err = appendMutationPathSlice(tm, ps[:len(ps)-1], ps[len(ps)-1], mutateFn)
	return
}

func isMutationNOP(tm *treeMutation) bool {
	if tm == nil {
		return true
	}

	if len(tm.entries) > 0 {
		return false
	}

	for _, stm := range tm.subtrees {
		if !isMutationNOP(stm) {
			return false
		}
	}

	return true
}

func (b *backend) doPut(
	ctx context.Context,
	metaHead git.Commit,
	req *etcdserverpb.PutRequest,
	res *etcdserverpb.PutResponse,
	newRevision int64,
	commitTreeFn commitTreeFunc,
) (
	metaMutated bool,
	newMetaHeadID git.ObjectID,
	dataMutated bool,
	newDataHeadID git.ObjectID,
	err error,
) {
	var (
		metaRoot, dataRoot git.Tree
		dataHead           git.Commit
		ie                 *intervalExplorer
		k                  = string(req.GetKey())
		p                  string
		prevKV             *mvccpb.KeyValue
		metaTM             *treeMutation
		bumpRevision       bool
		revisionMutateFn   mutateTreeEntryFunc
		newMetaRootID      git.ObjectID
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

	if !req.IgnoreValue {
		var (
			dataTM        *treeMutation
			newDataRootID git.ObjectID
		)

		if dataTM, err = appendMutation(
			nil,
			p,
			func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
				var newContent = req.GetValue()

				if te != nil && te.EntryType() == git.ObjectTypeBlob {
					if prevKV, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
						return
					}

					if prevKV.Value, err = b.getContentForTreeEntry(ctx, te); err != nil {
						return
					}

					if req.GetPrevKv() {
						res.PrevKv = prevKV
					}

					if reflect.DeepEqual(prevKV.Value, newContent) {
						return
					}
				}

				mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, newContent, te)
				return
			},
		); err != nil {
			return
		}

		if dataMutated, newDataRootID, err = b.mutateTree(ctx, dataRoot, dataTM, false); err != nil {
			return
		}

		if dataMutated {
			bumpRevision = true

			if metaTM, err = appendMutation(
				metaTM,
				metadataPathData,
				func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
					if newDataHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newDataRootID, dataHead); err != nil {
						return
					}

					mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(newDataHeadID.String()), te)
					return
				},
			); err != nil {
				return
			}

			if metaTM, err = appendMutation(
				metaTM,
				path.Join(p, etcdserverpb.Compare_VERSION.String()),
				func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
					var currentVersion int64

					if te != nil && te.EntryType() == git.ObjectTypeBlob {
						if currentVersion, err = b.readRevisionFromTreeEntry(ctx, te); err != nil {
							return
						}
					}

					mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(currentVersion+1)), te)
					return
				},
			); err != nil {
				return
			}
		}
	}

	if !req.GetIgnoreLease() {
		if metaTM, err = appendMutation(
			metaTM,
			path.Join(p, etcdserverpb.Compare_LEASE.String()),
			func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
				var (
					currentLease int64
					newLease     = req.GetLease()
				)

				if te != nil && te.EntryType() == git.ObjectTypeBlob {
					if currentLease, err = b.readRevisionFromTreeEntry(ctx, te); err != nil {
						return
					}
				}

				if currentLease == newLease {
					return
				}

				if mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(newLease)), te); err != nil {
					return
				}

				if mutated {
					bumpRevision = true
				}

				return
			},
		); err != nil {
			return
		}
	}

	if isMutationNOP(metaTM) {
		dataMutated = false // Just to be sure
		return              // nop
	}

	revisionMutateFn = func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
		if bumpRevision {
			return
		}

		mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(newRevision)), te)
		return
	}

	if metaTM, err = appendMutation(metaTM, metadataPathRevision, revisionMutateFn); err != nil {
		return
	}

	if metaTM, err = appendMutation(
		metaTM,
		path.Join(p, etcdserverpb.Compare_CREATE.String()),
		func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
			if te != nil && te.EntryType() == git.ObjectTypeBlob {
				return
			}

			mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(newRevision)), te)
			return
		},
	); err != nil {
		return
	}

	if metaTM, err = appendMutation(metaTM, path.Join(p, etcdserverpb.Compare_MOD.String()), revisionMutateFn); err != nil {
		return
	}

	if metaMutated, newMetaRootID, err = b.mutateTree(ctx, metaRoot, metaTM, false); err != nil {
		return
	}

	if !metaMutated {
		dataMutated = false // Just to be sure
		return              // nop
	}

	if newMetaHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newMetaRootID, metaHead); err != nil {
		return
	}

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
	for _, e := range []struct {
		mutated bool
		refName git.ReferenceName
		headID  git.ObjectID
	}{
		{mutated: metaMutated, refName: b.getMetadataRefName(), headID: newMetaHeadID},
		{mutated: dataMutated, refName: b.refName, headID: newDataHeadID},
	} {
		var rc git.ReferenceCollection

		if !e.mutated {
			continue
		}

		if rc, err = b.repo.References(); err != nil {
			return
		}

		if err = rc.Create(ctx, e.refName, e.headID, true, revisionToString(revision)); err != nil {
			return
		}
	}

	return
}

func setHeaderRevision(h *etcdserverpb.ResponseHeader, mutated bool, newRevision int64) {
	if mutated {
		h.Revision = newRevision
		h.RaftTerm = uint64(newRevision)
	}
}

func (b *backend) Put(ctx context.Context, req *etcdserverpb.PutRequest) (res *etcdserverpb.PutResponse, err error) {
	var (
		metaRef                      git.Reference
		metaHead                     git.Commit
		metaMutated, dataMutated     bool
		newMetaHeadID, newDataHeadID git.ObjectID
		newRevision                  int64
	)

	if metaRef, err = b.getMetadataReference(ctx); err == nil {
		defer metaRef.Close()
	}

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err == nil {
		defer metaHead.Close()
	}

	res = &etcdserverpb.PutResponse{
		Header: b.newResponseHeaderFromMetaPeelable(ctx, metaHead),
	}

	newRevision = res.GetHeader().GetRevision() + 1

	if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doPut(
		ctx,
		metaHead,
		req,
		res,
		newRevision,
		b.inheritCurrentCommit,
	); err != nil {
		return
	}

	setHeaderRevision(res.Header, metaMutated, newRevision)

	err = b.advanceReferences(ctx, metaMutated, newMetaHeadID, dataMutated, newDataHeadID, res.GetHeader().GetRevision())
	return
}

func (b *backend) doDeleteRange(
	ctx context.Context,
	metaHead git.Commit,
	req *etcdserverpb.DeleteRangeRequest,
	res *etcdserverpb.DeleteRangeResponse,
	newRevision int64,
	commitTreeFn commitTreeFunc,
) (
	metaMutated bool,
	newMetaHeadID git.ObjectID,
	dataMutated bool,
	newDataHeadID git.ObjectID,
	err error,
) {
	var (
		metaRoot, dataRoot           git.Tree
		dataHead                     git.Commit
		ie                           *intervalExplorer
		metaTM, dataTM               *treeMutation
		newMetaRootID, newDataRootID git.ObjectID

		deleteEntryFn = func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
			err = tb.RemoveEntry(entryName)
			mutated = err == nil
			return
		}
	)

	if metaHead == nil {
		return
	}

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

	if dataRoot == nil {
		return
	}

	defer dataRoot.Close()

	ie = &intervalExplorer{
		keyPrefix: b.keyPrefix,
		repo:      b.repo,
		tree:      dataRoot,
		interval:  newClosedOpenInterval(req.GetKey(), req.GetRangeEnd()),
	}

	if err = ie.forEachMatchingKey(
		ctx,
		func(ctx context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
			var p = ie.getPathForKey(k)

			if te.EntryType() == git.ObjectTypeBlob && req.PrevKv {
				var kv *mvccpb.KeyValue

				if kv, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
					return
				}

				if kv.Value, err = b.getContentForTreeEntry(ctx, te); err != nil {
					return
				}

				res.PrevKvs = append(res.PrevKvs, kv)
			}

			if dataTM, err = appendMutation(dataTM, p, deleteEntryFn); err != nil {
				return
			}

			if metaTM, err = appendMutation(metaTM, p, deleteEntryFn); err != nil {
				return
			}

			res.Deleted++

			return
		},
		newObjectTypeFilter(git.ObjectTypeBlob),
	); err != nil {
		return
	}

	if isMutationNOP(dataTM) {
		return
	}

	if dataMutated, newDataRootID, err = b.mutateTree(ctx, dataRoot, dataTM, true); err != nil {
		return
	}

	if !dataMutated {
		return
	}

	if metaTM, err = appendMutation(
		metaTM,
		metadataPathData,
		func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
			if newDataHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newDataRootID, dataHead); err != nil {
				return
			}

			mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(newDataHeadID.String()), te)
			return
		},
	); err != nil {
		return
	}

	if metaTM, err = appendMutation(
		metaTM,
		metadataPathRevision,
		func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
			mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(newRevision)), te)
			return
		},
	); err != nil {
		return
	}

	if isMutationNOP(metaTM) {
		return
	}

	if metaMutated, newMetaRootID, err = b.mutateTree(ctx, metaRoot, metaTM, true); err != nil {
		return
	}

	if !metaMutated {
		dataMutated = false // Just to be sure
		return              // nop
	}

	if newMetaHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newMetaRootID, metaHead); err != nil {
		return
	}

	return
}

func (b *backend) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (res *etcdserverpb.DeleteRangeResponse, err error) {
	var (
		metaRef                      git.Reference
		metaHead                     git.Commit
		metaMutated, dataMutated     bool
		newMetaHeadID, newDataHeadID git.ObjectID
		newRevision                  int64
	)

	if metaRef, err = b.getMetadataReference(ctx); err == nil {
		defer metaRef.Close()
	}

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err == nil {
		defer metaHead.Close()
	}

	res = &etcdserverpb.DeleteRangeResponse{
		Header: b.newResponseHeaderFromMetaPeelable(ctx, metaHead),
	}

	newRevision = res.GetHeader().GetRevision() + 1

	if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doDeleteRange(
		ctx,
		metaHead,
		req,
		res,
		newRevision,
		b.inheritCurrentCommit,
	); err != nil {
		return
	}

	setHeaderRevision(res.Header, metaMutated, newRevision)

	err = b.advanceReferences(ctx, metaMutated, newMetaHeadID, dataMutated, newDataHeadID, res.GetHeader().GetRevision())
	return
}

type int64Cmp int64

func (a int64Cmp) Cmp(b int64) cmpResult {
	switch {
	case int64(a) < b:
		return cmpResultLess
	case int64(a) == b:
		return cmpResultEqual
	default:
		return cmpResultMore
	}
}

type int64ReaderFunc func(*etcdserverpb.Compare) int64

var compareTarget_int64Reader = map[etcdserverpb.Compare_CompareTarget]int64ReaderFunc{
	etcdserverpb.Compare_CREATE:  func(c *etcdserverpb.Compare) int64 { return c.GetCreateRevision() },
	etcdserverpb.Compare_LEASE:   func(c *etcdserverpb.Compare) int64 { return c.GetLease() },
	etcdserverpb.Compare_MOD:     func(c *etcdserverpb.Compare) int64 { return c.GetModRevision() },
	etcdserverpb.Compare_VERSION: func(c *etcdserverpb.Compare) int64 { return c.GetVersion() },
}

func getCompareTargetInt64(c *etcdserverpb.Compare) (int64, error) {
	if readerFn, ok := compareTarget_int64Reader[c.GetTarget()]; ok {
		return readerFn(c), nil
	}

	return 0, fmt.Errorf("unsupported compare target %s", c.GetTarget().String())
}

type checkCompareResultFunc func(cmpResult) bool

var compareResult_checker = map[etcdserverpb.Compare_CompareResult]checkCompareResultFunc{
	etcdserverpb.Compare_EQUAL:     func(r cmpResult) bool { return r == cmpResultEqual },
	etcdserverpb.Compare_NOT_EQUAL: func(r cmpResult) bool { return r != cmpResultEqual },
	etcdserverpb.Compare_GREATER:   func(r cmpResult) bool { return r == cmpResultMore },
	etcdserverpb.Compare_LESS:      func(r cmpResult) bool { return r == cmpResultLess },
}

func checkCompareResult(c *etcdserverpb.Compare, r cmpResult) (bool, error) {
	if checkerFn, ok := compareResult_checker[c.GetResult()]; ok {
		return checkerFn(r), nil
	}

	return false, fmt.Errorf("unsupported compare result %s", c.GetResult().String())
}

func (b *backend) readInt64(ctx context.Context, te git.TreeEntry, path string) (ignore, skip bool, i int64, err error) {
	var (
		t git.Tree
		v []byte
	)

	if t, err = b.repo.ObjectGetter().GetTree(ctx, te.EntryID()); err != nil {
		ignore, err = true, nil
		return
	}

	defer t.Close()

	if te, err = t.GetEntryByPath(ctx, path); err != nil {
		ignore, err = true, nil
		return
	}

	if te.EntryType() != git.ObjectTypeBlob {
		ignore, skip = true, true
		return
	}

	if v, err = b.getContentForTreeEntry(ctx, te); err != nil {
		return
	}

	i, err = strconv.ParseInt(string(v), 10, 64)
	return
}

func copyResponseHeaderFrom(h *etcdserverpb.ResponseHeader) *etcdserverpb.ResponseHeader {
	var r = *h
	return &r
}

func (b *backend) doTxnRequestOps(
	ctx context.Context,
	metaHead git.Commit,
	requestOps []*etcdserverpb.RequestOp,
	res *etcdserverpb.TxnResponse,
	newRevision int64,
) (
	metaMutated bool,
	newMetaHeadID git.ObjectID,
	dataMutated bool,
	newDataHeadID git.ObjectID,
	err error,
) {
	var (
		rangeRes                           *etcdserverpb.RangeResponse
		putRes                             *etcdserverpb.PutResponse
		deleteRangeRes                     *etcdserverpb.DeleteRangeResponse
		txnRes                             *etcdserverpb.TxnResponse
		subMetaMutated, subDataMutated     bool
		subNewMetaHeadID, subNewDataHeadID git.ObjectID
		subHeader                          *etcdserverpb.ResponseHeader
	)

	if len(requestOps) == 0 {
		return
	}

	subHeader = copyResponseHeaderFrom(res.Header)

	switch sop := requestOps[0].GetRequest().(type) {
	case *etcdserverpb.RequestOp_RequestRange:
		if rangeRes, err = b.doRange(ctx, metaHead, sop.RequestRange); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseRange{
				ResponseRange: rangeRes,
			},
		})

	case *etcdserverpb.RequestOp_RequestPut:
		putRes = &etcdserverpb.PutResponse{Header: subHeader}

		if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doPut(
			ctx,
			metaHead,
			sop.RequestPut,
			putRes,
			newRevision,
			b.replaceCurrentCommit,
		); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponsePut{
				ResponsePut: putRes,
			},
		})

	case *etcdserverpb.RequestOp_RequestDeleteRange:
		deleteRangeRes = &etcdserverpb.DeleteRangeResponse{Header: subHeader}

		if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doDeleteRange(
			ctx,
			metaHead,
			sop.RequestDeleteRange,
			deleteRangeRes,
			newRevision,
			b.replaceCurrentCommit,
		); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
				ResponseDeleteRange: deleteRangeRes,
			},
		})

	case *etcdserverpb.RequestOp_RequestTxn:
		txnRes = &etcdserverpb.TxnResponse{Header: subHeader}

		if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doTxn(
			ctx,
			metaHead,
			sop.RequestTxn,
			txnRes,
			newRevision,
			b.replaceCurrentCommit,
		); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseTxn{
				ResponseTxn: txnRes,
			},
		})

	default:
		err = fmt.Errorf("unsupported request type %T", sop)
	}

	if metaMutated {
		if metaHead, err = b.repo.ObjectGetter().GetCommit(ctx, newMetaHeadID); err != nil {
			return
		}

		defer metaHead.Close()
	}

	if subMetaMutated, subNewMetaHeadID, subDataMutated, subNewDataHeadID, err = b.doTxnRequestOps(
		ctx,
		metaHead,
		requestOps[1:],
		res,
		newRevision,
	); err != nil {
		return
	}

	if subMetaMutated {
		metaMutated, newMetaHeadID = true, subNewMetaHeadID
	}

	if subDataMutated {
		dataMutated, newDataHeadID = true, subNewDataHeadID
	}

	return
}

func (b *backend) doTxn(
	ctx context.Context,
	metaHead git.Commit,
	req *etcdserverpb.TxnRequest,
	res *etcdserverpb.TxnResponse,
	newRevision int64,
	commitTreeFn commitTreeFunc,
) (
	metaMutated bool,
	newMetaHeadID git.ObjectID,
	dataMutated bool,
	newDataHeadID git.ObjectID,
	err error,
) {
	var (
		metaRoot, dataRoot git.Tree
		compare            = true
		ie                 *intervalExplorer
		requestOps         []*etcdserverpb.RequestOp
	)

	if metaHead != nil {
		var dataHead git.Commit

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

	for _, c := range req.GetCompare() {
		var (
			receiverFn intervalExplorerReceiverFunc
			filterFns  []intervalExplorerFilterFunc
		)

		if !compare {
			break
		}

		if c == nil {
			continue
		}

		if c.GetTarget() == etcdserverpb.Compare_VALUE {
			if dataRoot == nil {
				compare = false
				continue
			}

			ie.tree = dataRoot

			receiverFn = func(ctx context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
				var (
					value, target []byte
					entryCompare  bool
				)

				if value, err = b.getContentForTreeEntry(ctx, te); err != nil {
					return
				}

				if entryCompare, err = checkCompareResult(c, key(value).Cmp(key(target))); err != nil {
					return
				}

				if !entryCompare {
					compare = false
					done = true
				}

				return
			}

			filterFns = append(filterFns, newObjectTypeFilter(git.ObjectTypeBlob))
		} else {
			if metaRoot == nil {
				compare = false
				continue
			}

			ie.tree = metaRoot

			receiverFn = func(ctx context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
				var (
					value, target        int64
					ignore, entryCompare bool
				)

				if ignore, skip, value, err = b.readInt64(ctx, te, c.GetTarget().String()); err != nil {
					return
				}

				if ignore || skip {
					return
				}

				if target, err = getCompareTargetInt64(c); err != nil {
					return
				}

				if entryCompare, err = checkCompareResult(c, int64Cmp(value).Cmp(target)); err != nil {
					return
				}

				if !entryCompare {
					compare = false
					done = true
				}

				return
			}

			filterFns = append(filterFns, newObjectTypeFilter(git.ObjectTypeTree))
		}

		ie.interval = newClosedOpenInterval(c.GetKey(), c.GetRangeEnd())

		if err = ie.forEachMatchingKey(ctx, receiverFn, filterFns...); err != nil {
			return
		}
	}

	if compare {
		requestOps = req.Success
	} else {
		requestOps = req.Failure
	}

	metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doTxnRequestOps(ctx, metaHead, requestOps, res, newRevision)
	return
}

func (b *backend) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (res *etcdserverpb.TxnResponse, err error) {
	var (
		metaRef                      git.Reference
		metaHead                     git.Commit
		metaMutated, dataMutated     bool
		newMetaHeadID, newDataHeadID git.ObjectID
		newRevision                  int64
	)

	if metaRef, err = b.getMetadataReference(ctx); err == nil {
		defer metaRef.Close()
	}

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err == nil {
		defer metaHead.Close()
	}

	res = &etcdserverpb.TxnResponse{
		Header: b.newResponseHeaderFromMetaPeelable(ctx, metaHead),
	}

	newRevision = res.GetHeader().GetRevision() + 1

	if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doTxn(
		ctx,
		metaHead,
		req,
		res,
		newRevision,
		b.inheritCurrentCommit,
	); err != nil {
		return
	}

	setHeaderRevision(res.Header, metaMutated, newRevision)

	err = b.advanceReferences(ctx, metaMutated, newMetaHeadID, dataMutated, newDataHeadID, res.GetHeader().GetRevision())
	return
}

func (b *backend) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (res *etcdserverpb.CompactionResponse, err error) {
	res = &etcdserverpb.CompactionResponse{
		Header: b.newResponseHeader(ctx),
	}

	return
}
