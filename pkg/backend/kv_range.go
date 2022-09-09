package backend

import (
	"context"

	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func checkMinConstraint(constraint, value int64) bool {
	return constraint <= 0 || constraint <= value
}

func checkMaxConstraint(constraint, value int64) bool {
	return constraint <= 0 || value <= constraint
}

func (b *backend) doRange(ctx context.Context, metaHead git.Commit, req *etcdserverpb.RangeRequest) (res *etcdserverpb.RangeResponse, err error) {
	var (
		dataP, metaP       git.Peelable
		metaRoot, dataRoot git.Tree
		revision           int64
	)

	if metaHead == nil {
		return
	}

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
		errors:    b.errors,
		tree:      dataRoot,
		interval:  newClosedOpenInterval(req.GetKey(), req.GetRangeEnd()),
	}).forEachMatchingKey(
		ctx,
		func(ctx context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
			var kv *mvccpb.KeyValue

			if done = !checkMaxConstraint(req.GetLimit(), int64(len(res.Kvs)+1)); done {
				res.More = true
				return
			}

			if req.GetMaxCreateRevision()+req.GetMinCreateRevision()+req.GetMaxModRevision()+req.GetMinModRevision() > 0 {
				if kv, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
					return
				}

				if !checkMaxConstraint(req.MaxCreateRevision, kv.CreateRevision) ||
					!checkMinConstraint(req.MinCreateRevision, kv.CreateRevision) ||
					!checkMaxConstraint(req.MaxModRevision, kv.ModRevision) ||
					!checkMinConstraint(req.MinModRevision, kv.ModRevision) {
					return
				}
			}

			if req.GetCountOnly() {
				res.Count++
				return
			}

			if req.GetKeysOnly() {
				res.Kvs = append(res.Kvs, &mvccpb.KeyValue{Key: []byte(k)})
				return
			}

			if kv == nil {
				if kv, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
					return
				}
			}

			if kv.Value, err = b.getContentForTreeEntry(ctx, te); err != nil {
				return
			}

			res.Kvs = append(res.Kvs, kv)
			return
		},
		newObjectTypeFilter(git.ObjectTypeBlob),
	)

	if !req.GetCountOnly() {
		res.Count = int64(len(res.Kvs))
	}

	return
}

func (b *backend) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (res *etcdserverpb.RangeResponse, err error) {
	var (
		log      = b.log.WithName("Range")
		metaRef  git.Reference
		metaHead git.Commit
		perf     = perfCounter()
	)

	log.V(-1).Info("range received", "request", req)
	defer func() { log.V(-1).Info("range returned", "response", res, "error", err, "duration", perf().String()) }()

	b.RLock()
	defer b.RUnlock()

	if metaRef, err = b.getMetadataReference(ctx); err != nil {
		return
	}

	defer metaRef.Close()

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err != nil {
		return
	}

	defer metaHead.Close()

	if res, err = b.doRange(ctx, metaHead, req); err != nil {
		return
	}

	if res == nil {
		res = &etcdserverpb.RangeResponse{Header: b.newResponseHeader(ctx)}
	}

	return
}
