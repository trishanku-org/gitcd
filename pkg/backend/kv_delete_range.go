package backend

import (
	"context"

	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

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
			if te != nil {
				err = tb.RemoveEntry(entryName)
				mutated = err == nil
			}
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

			if dataTM, err = addMutation(dataTM, p, deleteEntryFn); err != nil {
				return
			}

			if metaTM, err = addMutation(metaTM, p, deleteEntryFn); err != nil {
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