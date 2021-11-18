package backend

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (b *backend) Compact(ctx context.Context, _ *etcdserverpb.CompactionRequest) (res *etcdserverpb.CompactionResponse, err error) {
	res = &etcdserverpb.CompactionResponse{
		Header: b.newResponseHeader(ctx),
	}

	return
}
