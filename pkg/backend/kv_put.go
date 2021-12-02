package backend

import (
	"context"
	"path"
	"reflect"

	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

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
		metaRoot, dataRoot     git.Tree
		dataHead               git.Commit
		ie                     *intervalExplorer
		k                      = string(req.GetKey())
		p                      string
		metaTM                 *treeMutation
		revisionMutateFn       mutateTreeEntryFunc
		newMetaRootID, newDHID git.ObjectID
		mMutated, dMutated     bool
		mte                    git.TreeEntry
	)

	if metaHead != nil {
		if metaRoot, err = b.repo.Peeler().PeelToTree(ctx, metaHead); err != nil {
			return
		}

		defer func() {
			if metaRoot != nil {
				metaRoot.Close()
			}
		}()

		if dataHead, err = b.getDataCommitForMetadata(ctx, metaRoot); err != nil {
			return
		}

		defer dataHead.Close()

		if dataRoot, err = b.repo.Peeler().PeelToTree(ctx, dataHead); err != nil {
			return
		}

		defer func() {
			if dataRoot != nil {
				dataRoot.Close()
			}
		}()
	}

	ie = &intervalExplorer{
		keyPrefix: b.keyPrefix,
		repo:      b.repo,
	}

	if p = ie.getPathForKey(k); len(p) == 0 {
		err = rpctypes.ErrGRPCEmptyKey
		return

	}

	if !req.IgnoreValue || req.PrevKv {
		var (
			dataTM        *treeMutation
			newDataRootID git.ObjectID
		)

		if dataTM, err = addMutation(
			nil,
			p,
			func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
				var newContent = req.GetValue()

				if te != nil && te.EntryType() == git.ObjectTypeBlob {
					var prevV []byte

					if prevV, err = b.getContentForTreeEntry(ctx, te); err != nil {
						return
					}

					if req.PrevKv {
						var prevKV *mvccpb.KeyValue
						if prevKV, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
							return
						}

						prevKV.Value = prevV

						if req.GetPrevKv() {
							res.PrevKv = prevKV
						}
					}

					if reflect.DeepEqual(prevV, newContent) {
						return
					}
				}

				if req.IgnoreValue {
					return
				}

				mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, newContent, te)
				return
			},
		); err != nil {
			return
		}

		if dMutated, newDataRootID, err = b.mutateTree(ctx, dataRoot, dataTM, false); err != nil {
			return
		}

		if dMutated {
			if metaTM, err = addMutation(
				metaTM,
				metadataPathData,
				func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
					if newDHID, err = commitTreeFn(ctx, revisionToString(newRevision), newDataRootID, dataHead); err != nil {
						return
					}

					mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(newDHID.String()), te)
					return
				},
			); err != nil {
				return
			}

			if metaTM, err = addMutation(
				metaTM,
				path.Join(p, etcdserverpb.Compare_VERSION.String()),
				func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
					var currentVersion int64

					if te != nil && te.EntryType() == git.ObjectTypeBlob {
						if currentVersion, err = b.readRevisionFromTreeEntry(ctx, te); b.errors.IgnoreNotFound(err) != nil {
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
		if metaTM, err = addMutation(
			metaTM,
			path.Join(p, etcdserverpb.Compare_LEASE.String()),
			func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
				var (
					currentLease int64
					newLease     = req.GetLease()
				)

				if te != nil && te.EntryType() == git.ObjectTypeBlob {
					if currentLease, err = b.readRevisionFromTreeEntry(ctx, te); b.errors.IgnoreNotFound(err) != nil {
						return
					}
				}

				if currentLease == newLease {
					return
				}

				mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(newLease)), te)
				return
			},
		); err != nil {
			return
		}
	}

	if isMutationNOP(metaTM) {
		return // nop
	}

	// Apply non-conditional mutations on metadata first to see if the conditional mutations need to be applied.
	if mMutated, newMetaRootID, err = b.mutateTree(ctx, metaRoot, metaTM, false); err != nil || !mMutated {
		return
	}

	// Apply conditional mutations on metadata on the reloaded mutated metaRoot
	metaTM = nil

	if metaRoot != nil {
		metaRoot.Close()
	}

	if metaRoot, err = b.repo.ObjectGetter().GetTree(ctx, newMetaRootID); err != nil {
		return
	}

	// metaRoot is already closed in defer at the beginning of the function.

	// Add mutations to cleanup any non-metadata entries to handle a subtree being replaced by a value.
	if mte, err = metaRoot.GetEntryByPath(ctx, p); b.errors.IgnoreNotFound(err) != nil {
		return
	} else if !b.errors.IsNotFound(err) && mte.EntryType() == git.ObjectTypeTree {
		var t git.Tree

		if t, err = b.repo.ObjectGetter().GetTree(ctx, mte.EntryID()); err != nil {
			return
		}

		defer t.Close()

		if err = t.ForEachEntry(ctx, func(_ context.Context, te git.TreeEntry) (done bool, err error) {
			var entryName = te.EntryName()

			if _, ok := etcdserverpb.Compare_CompareTarget_value[entryName]; ok {
				return
			}

			metaTM, err = addMutation(metaTM, path.Join(p, entryName), deleteEntry)
			return
		}); err != nil {
			return
		}
	}

	revisionMutateFn = func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
		mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, []byte(revisionToString(newRevision)), te)
		return
	}

	if metaTM, err = addMutation(metaTM, metadataPathRevision, revisionMutateFn); err != nil {
		return
	}

	if metaTM, err = addMutation(
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

	if metaTM, err = addMutation(metaTM, path.Join(p, etcdserverpb.Compare_MOD.String()), revisionMutateFn); err != nil {
		return
	}

	if metaMutated, newMetaRootID, err = b.mutateTree(ctx, metaRoot, metaTM, false); err != nil {
		return
	}

	if !metaMutated {
		return // nop
	}

	if newMetaHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newMetaRootID, metaHead); err != nil {
		return
	}

	dataMutated, newDataHeadID = dMutated, newDHID
	return
}

func (b *backend) Put(ctx context.Context, req *etcdserverpb.PutRequest) (res *etcdserverpb.PutResponse, err error) {
	var (
		metaRef                      git.Reference
		metaHead                     git.Commit
		metaMutated, dataMutated     bool
		newMetaHeadID, newDataHeadID git.ObjectID
		newRevision                  int64
	)

	if metaRef, err = b.getMetadataReference(ctx); err != nil && err == ctx.Err() {
		return
	}

	if metaRef != nil {
		defer metaRef.Close()

		if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err != nil && err == ctx.Err() {
			return
		}

		if metaHead != nil {
			defer metaHead.Close()
		}
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
