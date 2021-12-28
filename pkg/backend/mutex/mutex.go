package mutex

import (
	"context"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func NewKVServer(kvs etcdserverpb.KVServer) etcdserverpb.KVServer {
	return &mutexKVServer{KVServer: kvs}
}

type mutexKVServer struct {
	sync.RWMutex
	etcdserverpb.KVServer
}

func (m *mutexKVServer) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	m.RLock()
	defer m.RUnlock()

	return m.KVServer.Range(ctx, req)
}

func (m *mutexKVServer) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	m.Lock()
	defer m.Unlock()

	return m.KVServer.Put(ctx, req)
}

func (m *mutexKVServer) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	m.Lock()
	defer m.Unlock()

	return m.KVServer.DeleteRange(ctx, req)
}

func (m *mutexKVServer) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	m.Lock()
	defer m.Unlock()

	return m.KVServer.Txn(ctx, req)
}

func (m *mutexKVServer) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	m.Lock()
	defer m.Unlock()

	return m.KVServer.Compact(ctx, req)
}
