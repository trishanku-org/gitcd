package backend

import (
	"context"

	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
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
		metaRoot, dataRoot                    git.Tree
		dataHead                              git.Commit
		ie                                    *intervalExplorer
		metaTM, dataTM                        *mutation.TreeMutation
		dMutated                              bool
		newMetaRootID, newDataRootID, newDHID git.ObjectID
		ndeleted                              int64
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
		errors:    b.errors,
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

			if dataTM, err = mutation.AddMutation(dataTM, p, mutation.DeleteEntry); err != nil {
				return
			}

			if metaTM, err = mutation.AddMutation(metaTM, p, mutation.DeleteEntry); err != nil {
				return
			}

			ndeleted++

			return
		},
		newObjectTypeFilter(git.ObjectTypeBlob),
	); err != nil {
		return
	}

	if mutation.IsMutationNOP(dataTM) {
		return
	}

	if dMutated, newDataRootID, err = mutation.MutateTree(ctx, b.repo, dataRoot, dataTM, true); err != nil {
		return
	}

	if !dMutated {
		return
	}

	if metaTM, err = mutation.AddMutation(
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

	if metaTM, err = mutation.AddMutation(
		metaTM,
		metadataPathRevision,
		b.mutateRevisionTo(newRevision),
	); err != nil {
		return
	}

	if mutation.IsMutationNOP(metaTM) {
		return
	}

	if metaMutated, newMetaRootID, err = mutation.MutateTree(ctx, b.repo, metaRoot, metaTM, true); err != nil {
		return
	}

	if !metaMutated {
		return // nop
	}

	if newMetaHeadID, err = commitTreeFn(ctx, revisionToString(newRevision), newMetaRootID, metaHead); err != nil {
		return
	}

	dataMutated, newDataHeadID, res.Deleted = dMutated, newDHID, ndeleted
	return
}

func (b *backend) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (res *etcdserverpb.DeleteRangeResponse, err error) {
	var (
		log                          = b.log.WithName("DeleteRange")
		metaRef                      git.Reference
		metaHead                     git.Commit
		metaMutated, dataMutated     bool
		newMetaHeadID, newDataHeadID git.ObjectID
		newRevision                  int64
		perf                         = perfCounter()
	)

	log.V(-1).Info("delete received", "request", req)
	defer func() { log.V(-1).Info("delete returned", "response", res, "error", err, "duration", perf().String()) }()

	b.Lock()
	defer b.Unlock()

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
