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

func (m *maintenanceImpl) Alarm(ctx context.Context, req *etcdserverpb.AlarmRequest) (res *etcdserverpb.AlarmResponse, err error) {
	if req.Action != etcdserverpb.AlarmRequest_GET {
		res, err = m.UnimplementedMaintenanceServer.Alarm(ctx, req)
		return
	}

	res = &etcdserverpb.AlarmResponse{Header: m.backend.newResponseHeader(ctx)}
	return
}

func (m *maintenanceImpl) Status(ctx context.Context, req *etcdserverpb.StatusRequest) (res *etcdserverpb.StatusResponse, err error) {
	var (
		b       = m.backend
		log     = b.log.WithName("Status")
		version []byte
		size    int64
		h       *etcdserverpb.ResponseHeader

		metaRef git.Reference
		metaT   git.Tree

		perf = perfCounter()
	)

	log.V(-1).Info("status received", "request", req)
	defer func() { log.V(-1).Info("status returned", "response", res, "error", err, "duration", perf().String()) }()

	b.RLock()
	defer b.RUnlock()

	if metaRef, err = b.getMetadataReference(ctx); err != nil {
		return
	}

	defer metaRef.Close()

	if metaT, err = b.repo.Peeler().PeelToTree(ctx, metaRef); err != nil {
		return
	}

	defer metaT.Close()

	if version, _ = b.getContent(ctx, metaT, metadataPathVersion); err != nil {
		log.V(1).Info("Error reading backend version", "error", err)
		err = nil
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
