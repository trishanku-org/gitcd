package backend

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (b *backend) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (res *etcdserverpb.CompactionResponse, err error) {
	var (
		log  = b.log.WithName("Compact")
		perf = perfCounter()
	)

	log.V(-1).Info("received", "request", req)
	defer func() { log.V(-1).Info("returned", "response", res, "error", err, "duration", perf().String()) }()

	b.RLock()
	defer b.RUnlock()

	res = &etcdserverpb.CompactionResponse{
		Header: b.newResponseHeader(ctx),
	}

	return
}
