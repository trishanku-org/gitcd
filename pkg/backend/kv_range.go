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
