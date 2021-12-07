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

func (c *clusterImpl) MemberList(ctx context.Context, req *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	var (
		b = c.backend
		h = b.newResponseHeader(ctx)
	)

	return &etcdserverpb.MemberListResponse{
		Header: h,
		Members: []*etcdserverpb.Member{{
			ID:         h.MemberId,
			Name:       memberName,
			ClientURLs: c.clientURLs,
		}},
	}, nil
}
