package backend

import (
	"context"

	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func NewMaintenanceServer(kvs etcdserverpb.KVServer) (ms etcdserverpb.MaintenanceServer, err error) {
	var b *backend

	if b, err = checkIsBackend(kvs); err != nil {
		return
	}

	ms = &maintenanceImpl{backend: b}
	return
}

type maintenanceImpl struct {
	*etcdserverpb.UnimplementedMaintenanceServer

	backend *backend
}

var _ etcdserverpb.MaintenanceServer = (*maintenanceImpl)(nil)

func (m *maintenanceImpl) Status(ctx context.Context, req *etcdserverpb.StatusRequest) (res *etcdserverpb.StatusResponse, err error) {
	var (
		b       = m.backend
		version []byte
		size    int64
		h       *etcdserverpb.ResponseHeader

		metaRef git.Reference
		metaT   git.Tree
	)

	if metaRef, err = b.getMetadataReference(ctx); err != nil {
		return
	}

	defer metaRef.Close()

	if metaT, err = b.repo.Peeler().PeelToTree(ctx, metaRef); err != nil {
		return
	}

	defer metaT.Close()

	if version, err = b.getContent(ctx, metaT, metadataPathVersion); err != nil {
		return
	}

	if size, err = b.repo.Size(); err != nil {
		return
	}

	h = b.newResponseHeaderFromMetaTree(ctx, metaT)

	res = &etcdserverpb.StatusResponse{
		Header:           h,
		Version:          string(version),
		DbSize:           size,
		Leader:           h.MemberId,
		RaftIndex:        h.RaftTerm,
		RaftTerm:         h.RaftTerm,
		RaftAppliedIndex: h.RaftTerm,
	}
	return
}
