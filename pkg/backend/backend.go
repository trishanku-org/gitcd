package backend

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

// backend implements an ETCD server backed by a Git repository.
type backend struct {
	repo      git.Repository
	refName   git.ReferenceName
	keyPrefix string
}

var _ etcdserverpb.KVServer = &backend{}

const (
	metadataReferencePrefix       = "refs/gitcd/metadata/"
	metadataPathRevision          = ".revision"
	metadataPathData              = ".data"
	metadataPathKeyCreateRevision = ".createRevision"
	metadataPathKeyLease          = ".lease"
	metadataPathKeyModRevision    = ".modRevision"
	metadataPathKeyVersion        = ".version"
)

func (b *backend) getMetadataRefName() git.ReferenceName {
	return git.ReferenceName(metadataReferencePrefix + b.refName)
}

func (b *backend) getReference(ctx context.Context, refName git.ReferenceName) (git.Reference, error) {
	if refs, err := b.repo.References(); err != nil {
		return nil, err
	} else {
		return refs.Get(ctx, refName)
	}
}

func (b *backend) getDataReference(ctx context.Context) (git.Reference, error) {
	return b.getReference(ctx, b.refName)
}

func (b *backend) getMetadataReference(ctx context.Context) (git.Reference, error) {
	return b.getReference(ctx, b.getMetadataRefName())
}

func (b *backend) Start(ctx context.Context) error { return nil }

func (b *backend) getPathForKey(key string) string {
	const defaultPrefix = "/"
	var prefix = b.keyPrefix

	if len(prefix) == 0 {
		prefix = defaultPrefix
	}

	if strings.HasPrefix(key, prefix) {
		return key[len(prefix):]
	} else {
		return ""
	}
}

func NewUnsupportedObjectType(typ git.ObjectType) error {
	return fmt.Errorf("unsupported ObjectType %v", typ)
}

func (b *backend) getContent(ctx context.Context, t git.Tree, path string) (v []byte, err error) {
	var (
		te   git.TreeEntry
		typ  git.ObjectType
		blob git.Blob
	)

	if te, err = t.GetEntryByPath(ctx, path); err != nil {
		return
	}

	if typ = te.EntryType(); typ != git.ObjectTypeBlob {
		err = NewUnsupportedObjectType(typ)
		return
	}

	if blob, err = b.repo.ObjectGetter().GetBlob(ctx, te.EntryID()); err != nil {
		return
	}

	defer blob.Close()

	return blob.Content()
}

func (b *backend) readRevision(ctx context.Context, t git.Tree, path string) (revision int64, err error) {
	var v []byte

	if v, err = b.getContent(ctx, t, path); err != nil {
		return
	}

	return strconv.ParseInt(string(v), 10, 64)
}

func (b *backend) readOjectID(ctx context.Context, t git.Tree, path string) (git.ObjectID, error) {
	var (
		v   []byte
		err error
	)

	if v, err = b.getContent(ctx, t, path); err != nil {
		return git.ObjectID{}, err
	}

	return git.NewObjectID(string(v))
}

/*
// kvMetadata holds the metadata associated with a particular key and value.
type kvMetadata struct {
	revision, createRevision, lease, modRevision, version int64
}

func (b *backend) getMetadataFor(ctx context.Context, metaP git.Peelable, k string) (kv *kvMetadata, err error) {
	var (
		root, t git.Tree
		te      git.TreeEntry
		typ     git.ObjectType
	)

	if root, err = b.repo.Peeler().PeelToTree(ctx, metaP); err != nil {
		return
	}

	defer root.Close()

	kv = &kvMetadata{}

	if kv.revision, err = b.readRevision(ctx, root, metadataPathRevision); err != nil {
		return
	}

	if te, err = t.GetEntryByPath(ctx, b.getPathForKey(k)); err != nil {
		return
	}

	if typ = te.EntryType(); typ != git.ObjectTypeTree {
		err = NewUnsupportedObjectType(typ)
		return
	}

	if t, err = b.repo.ObjectGetter().GetTree(ctx, te.EntryID()); err != nil {
		return
	}

	defer t.Close()

	if kv.createRevision, err = b.readRevision(ctx, t, metadataPathKeyCreateRevision); err != nil {
		return
	}

	if kv.lease, err = b.readRevision(ctx, t, metadataPathKeyLease); err != nil {
		return
	}

	if kv.modRevision, err = b.readRevision(ctx, t, metadataPathKeyModRevision); err != nil {
		return
	}

	if kv.version, err = b.readRevision(ctx, t, metadataPathKeyVersion); err != nil {
		return
	}

	return
}
*/

// getPeelablesForRevision returns the metadata Peelable that corresponds to the given revision.
// The metadata commits are searched in pre-order starting from the given metaHead.
// It might return metaHead itself as the metadata Peelable it it matches the given revision.
// So, the caller should check this possibility before closing metaHead and the returned metaP.
func (b *backend) getMetadataPeelableForRevision(ctx context.Context, metaHead git.Commit, revision int64) (metaP git.Peelable, _ error) {
	if err := b.repo.CommitWalker().ForEachCommit(ctx, metaHead, func(ctx context.Context, c git.Commit) (done, skip bool, err error) {
		var (
			t   git.Tree
			rev int64
		)

		if t, err = b.repo.Peeler().PeelToTree(ctx, c); err != nil {
			return
		}

		defer t.Close()

		if rev, err = b.readRevision(ctx, t, metadataPathRevision); err != nil {
			return
		}

		if rev < revision {
			skip, err = true, rpctypes.ErrGRPCFutureRev
			return
		}

		if rev == revision {
			done = true

			// Reload the commit to avoid it being closed by the commit walker.
			if metaP, err = b.repo.ObjectGetter().GetCommit(ctx, c.ID()); err != nil {
				return
			}

			return
		}

		return
	}); err != nil {
		return metaP, err
	}

	if metaP == nil {
		return metaP, rpctypes.ErrGRPCCompacted
	}

	return
}

func (b *backend) getDataCommitForMetadata(ctx context.Context, metaP git.Peelable) (dataP git.Peelable, err error) {
	var (
		t  git.Tree
		id git.ObjectID
	)

	if t, err = b.repo.Peeler().PeelToTree(ctx, metaP); err != nil {
		return
	}

	defer t.Close()

	if id, err = b.readOjectID(ctx, t, metadataPathData); err != nil {
		return
	}

	return b.repo.ObjectGetter().GetCommit(ctx, id)
}

func (b *backend) doRange(ctx context.Context, metaHead git.Commit, req *etcdserverpb.RangeRequest) (res *etcdserverpb.RangeResponse, err error) {
	var (
		dataP, metaP git.Peelable
		root         git.Tree
	)

	if req.Revision <= 0 {
		metaP = metaHead
	} else if metaP, err = b.getMetadataPeelableForRevision(ctx, metaHead, req.GetRevision()); err != nil {
		return
	} else {
		defer metaP.Close()
	}

	if dataP, err = b.getDataCommitForMetadata(ctx, metaP); err != nil {
		return
	}

	defer dataP.Close()

	if root, err = b.repo.Peeler().PeelToTree(ctx, dataP); err != nil {
		return
	}

	defer root.Close()

	return //TODO
}

func (b *backend) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (res *etcdserverpb.RangeResponse, err error) {
	var (
		metaRef  git.Reference
		metaHead git.Commit
	)

	if metaRef, err = b.getMetadataReference(ctx); err != nil {
		return
	}

	defer metaRef.Close()

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err != nil {
		return
	}

	defer metaHead.Close()

	return b.doRange(ctx, metaHead, req)
}

func (b *backend) Put(ctx context.Context, req *etcdserverpb.PutRequest) (res *etcdserverpb.PutResponse, err error) {
	return //TODO
}

func (b *backend) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (res *etcdserverpb.DeleteRangeResponse, err error) {
	return //TODO
}

func (b *backend) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (res *etcdserverpb.TxnResponse, err error) {
	return //TODO
}

func (b *backend) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (res *etcdserverpb.CompactionResponse, err error) {
	return //TODO
}
