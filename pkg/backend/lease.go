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

func (l *leaseImpl) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return &etcdserverpb.LeaseGrantResponse{
		Header: l.backend.newResponseHeader(ctx),
		ID:     req.ID,
		TTL:    req.TTL,
	}, nil
}
