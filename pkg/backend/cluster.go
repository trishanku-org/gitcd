package backend

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func NewClusterServer(kvs etcdserverpb.KVServer, clientURLs []string) (cs etcdserverpb.ClusterServer, err error) {
	var b *backend

	if b, err = checkIsBackend(kvs); err != nil {
		return
	}

	cs = &clusterImpl{backend: b, clientURLs: clientURLs}
	return
}

const memberName = "trishanku"

type clusterImpl struct {
	etcdserverpb.UnimplementedClusterServer

	backend *backend

	clientURLs []string
}

var _ etcdserverpb.ClusterServer = (*clusterImpl)(nil)

func (c *clusterImpl) MemberList(ctx context.Context, req *etcdserverpb.MemberListRequest) (res *etcdserverpb.MemberListResponse, err error) {
	var (
		b   = c.backend
		log = b.log.WithName("MemberList")
		h   = b.newResponseHeader(ctx)
	)

	log.V(-1).Info("received", "request", req)
	defer func() { log.V(-1).Info("returned", "response", res, "error", err) }()

	res = &etcdserverpb.MemberListResponse{
		Header: h,
		Members: []*etcdserverpb.Member{{
			ID:         h.MemberId,
			Name:       memberName,
			ClientURLs: c.clientURLs,
		}},
	}

	return
}
