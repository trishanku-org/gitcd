package backend

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (b *backend) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (res *etcdserverpb.CompactionResponse, err error) {
	var log = b.log.WithName("Compact")

	log.V(-1).Info("received", "request", req)
	defer func() { log.V(-1).Info("returned", "response", res, "error", err) }()

	res = &etcdserverpb.CompactionResponse{
		Header: b.newResponseHeader(ctx),
	}

	return
}
