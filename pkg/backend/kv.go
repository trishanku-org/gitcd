package backend

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type KVOptionFunc func(etcdserverpb.KVServer) error

func NewKVServer(optFns ...KVOptionFunc) (kvs etcdserverpb.KVServer, err error) {
	var b = &backend{}

	for _, optFn := range optFns {
		if err = optFn(b); err != nil {
			return
		}
	}

	kvs = b
	return
}

type backendOptionFunc func(*backend) error

func checkIsBackend(i interface{}) (*backend, error) {
	if b, ok := i.(*backend); ok {
		return b, nil
	}

	return nil, fmt.Errorf("unsupported backend type %T", i)
}

type kvOpts struct{}

var KVOptions = kvOpts{}

func (kvOpts) doIt(fn backendOptionFunc) KVOptionFunc {
	return func(kvs etcdserverpb.KVServer) (err error) {
		var b *backend

		if b, err = checkIsBackend(kvs); err != nil {
			return
		}

		return fn(b)
	}
}

func (opts kvOpts) WithKeyPrefix(prefix string) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.keyPrefix.prefix = prefix
		return nil
	})
}

func (opts kvOpts) WithRepoAndErrors(repo git.Repository, errs git.Errors) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.repo, b.errors = repo, errs
		return nil
	})
}

func (opts kvOpts) WithRefName(refName git.ReferenceName) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.refName = refName
		return nil
	})
}

func (opts kvOpts) WithMetadataRefName(metaRefName git.ReferenceName) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.metadataRefName = metaRefName
		return nil
	})
}

func (opts kvOpts) WithClusterId(id uint64) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.clusterID = id
		return nil
	})
}

func (opts kvOpts) WithMemberId(id uint64) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.memberID = id
		return nil
	})
}

func (opts kvOpts) WithCommitterName(name string) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.commitConfig.committerName = name
		return nil
	})
}

func (opts kvOpts) WithCommitterEmail(email string) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.commitConfig.committerEmail = email
		return nil
	})
}

func (opts kvOpts) WithLogger(log logr.Logger) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.log = log
		return nil
	})
}

func (opts kvOpts) WithWatchDispatchTicker(ch chan<- time.Time) KVOptionFunc {
	return opts.doIt(func(b *backend) error {
		b.watchDispatchTicker = ch
		return nil
	})
}
