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
		err = rpctypes.ErrGRPCEmptyKey
		return

	}

	if !req.IgnoreValue {
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

			if metaTM, err = addMutation(
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

			if metaTM, err = addMutation(
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
		if metaTM, err = addMutation(
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
		dataMutated = false // Just to be sure
		return              // nop
	}

	if newMetaHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newMetaRootID, metaHead); err != nil {
		return
	}

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
