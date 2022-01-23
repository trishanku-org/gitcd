package backend

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func NewLeaseServer(kvs etcdserverpb.KVServer) (ls etcdserverpb.LeaseServer, err error) {
	var b *backend

	if b, err = checkIsBackend(kvs); err != nil {
		return
	}

	ls = &leaseImpl{backend: b}
	return
}

type leaseImpl struct {
	etcdserverpb.UnimplementedLeaseServer

	backend *backend
}

var _ etcdserverpb.LeaseServer = (*leaseImpl)(nil)

func (l *leaseImpl) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (res *etcdserverpb.LeaseGrantResponse, err error) {
	var log = l.backend.log.WithName("LeaseGrant")

	log.V(-1).Info("received", "request", req)
	defer func() { log.V(-1).Info("returned", "response", res, "error", err) }()

	res = &etcdserverpb.LeaseGrantResponse{
		Header: l.backend.newResponseHeader(ctx),
		ID:     req.ID,
		TTL:    req.TTL,
	}

	return
}
