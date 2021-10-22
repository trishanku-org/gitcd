package git2go

import (
	"context"
	"fmt"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

// repository implements the Repository interface defined in the parent git package.
type repository struct {
	impl *impl.Repository
}

var _ git.Repository = &repository{}

func (repo *repository) Close() error { repo.impl.Free(); return nil }

func (repo *repository) References() (git.ReferenceCollection, error) {
	return (*referenceCollection)(&repo.impl.References), nil
}

func (repo *repository) ObjectGetter() git.ObjectGetter { return repo }

func (repo *repository) GetObject(ctx context.Context, id git.ObjectID) (git.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var oid = impl.Oid(id)
	if implO, err := repo.impl.Lookup(&oid); err != nil {
		return nil, err
	} else {
		return (*object)(implO), nil
	}
}

func (repo *repository) GetBlob(ctx context.Context, id git.ObjectID) (git.Blob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var oid = impl.Oid(id)
	if implB, err := repo.impl.LookupBlob(&oid); err != nil {
		return nil, err
	} else {
		return (*blob)(implB), nil
	}
}

func (repo *repository) GetTree(ctx context.Context, id git.ObjectID) (git.Tree, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var oid = impl.Oid(id)
	if implT, err := repo.impl.LookupTree(&oid); err != nil {
		return nil, err
	} else {
		return (*tree)(implT), nil
	}
}

func (repo *repository) GetCommit(ctx context.Context, id git.ObjectID) (git.Commit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var oid = impl.Oid(id)
	if implC, err := repo.impl.LookupCommit(&oid); err != nil {
		return nil, err
	} else {
		return (*commit)(implC), nil
	}
}

func (repo *repository) ObjectConverter() git.ObjectConverter { return repo }

func NewUnsupportedImplementationError(i interface{}) error {
	return fmt.Errorf("unsuppported implementation %T", i)
}

func (repo *repository) ToBlob(ctx context.Context, o git.Object) (git.Blob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if oo, ok := o.(*object); !ok {
		return nil, NewUnsupportedImplementationError(o)
	} else if implB, err := oo.impl().AsBlob(); err != nil {
		return nil, err
	} else {
		return (*blob)(implB), nil
	}
}

func (repo *repository) ToTree(ctx context.Context, o git.Object) (git.Tree, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if oo, ok := o.(*object); !ok {
		return nil, NewUnsupportedImplementationError(o)
	} else if implT, err := oo.impl().AsTree(); err != nil {
		return nil, err
	} else {
		return (*tree)(implT), nil
	}
}

func (repo *repository) ToCommit(ctx context.Context, o git.Object) (git.Commit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if oo, ok := o.(*object); !ok {
		return nil, NewUnsupportedImplementationError(o)
	} else if implC, err := oo.impl().AsCommit(); err != nil {
		return nil, err
	} else {
		return (*commit)(implC), nil
	}
}

func (repo *repository) Peeler() git.Peeler { return repo }

func (repo *repository) peelToObjectType(ctx context.Context, p git.Peelable, typ git.ObjectType) (git.Object, error) {
	var peeledO git.Object

	if err := p.Peel(ctx, git.NewObjectReceiver(typ, func(ctx context.Context, o git.Object) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		peeledO = o
		return nil
	})); err != nil {
		return nil, err
	}

	return peeledO, nil
}

func (repo *repository) PeelToBlob(ctx context.Context, p git.Peelable) (git.Blob, error) {
	if o, err := repo.peelToObjectType(ctx, p, git.ObjectTypeBlob); err != nil {
		return nil, err
	} else {
		return repo.ObjectConverter().ToBlob(ctx, o)
	}
}

func (repo *repository) PeelToTree(ctx context.Context, p git.Peelable) (git.Tree, error) {
	if o, err := repo.peelToObjectType(ctx, p, git.ObjectTypeTree); err != nil {
		return nil, err
	} else {
		return repo.ObjectConverter().ToTree(ctx, o)
	}
}

func (repo *repository) PeelToCommit(ctx context.Context, p git.Peelable) (git.Commit, error) {
	if o, err := repo.peelToObjectType(ctx, p, git.ObjectTypeCommit); err != nil {
		return nil, err
	} else {
		return repo.ObjectConverter().ToCommit(ctx, o)
	}
}

func (repo *repository) TreeWalker() git.TreeWalker { return repo }

const (
	implTreeWalkDone = iota - 1
	implTreeWalkContinue
	implTreeWalkSkip
)

func (repo *repository) ForEachTreeEntry(ctx context.Context, t git.Tree, fn git.TreeWalkerReceiverFunc) (err error) {
	if err = ctx.Err(); err != nil {
		return
	}

	if tt, ok := t.(*tree); !ok {
		err = NewUnsupportedImplementationError(t)
		return
	} else {
		tt.impl().Walk(func(parentPath string, implTE *impl.TreeEntry) int {
			var done, skip bool

			if err = ctx.Err(); err != nil {
				return implTreeWalkDone
			}

			if done, skip, err = fn(ctx, parentPath, (*treeEntry)(implTE)); err != nil || done {
				return implTreeWalkDone
			} else if skip {
				return implTreeWalkSkip
			}

			return implTreeWalkContinue
		})

		return err
	}
}

func (repo *repository) CommitWalker() git.CommitWalker { return repo }

func (repo *repository) ForEachCommit(ctx context.Context, c git.Commit, fn git.CommitWalkerReceiverFunc) (err error) {
	_, err = repo.forEachCommit(ctx, c, fn)
	return
}

func (repo *repository) forEachCommit(ctx context.Context, c git.Commit, fn git.CommitWalkerReceiverFunc) (done bool, err error) {
	var skip bool
	// TODO Improve this naive recursive implementation to avoid stack overflow.

	if err = ctx.Err(); err != nil {
		return
	}

	if done, skip, err = fn(ctx, c); err != nil || done || skip {
		return
	}

	err = c.ForEachParent(ctx, func(ctx context.Context, c git.Commit) (done bool, err error) {
		if err = ctx.Err(); err != nil {
			return
		}

		return repo.forEachCommit(ctx, c, fn)
	})

	return
}

func (repo *repository) BlobBuilder(ctx context.Context) (b git.BlobBuilder, err error) {
	if err = ctx.Err(); err == nil {
		b = &blobBuilder{repo: repo}
	}

	return
}

func (repo *repository) TreeBuilder(ctx context.Context) (b git.TreeBuilder, err error) {
	var implB *impl.TreeBuilder

	if err = ctx.Err(); err != nil {
		return
	}

	if implB, err = repo.impl.TreeBuilder(); err == nil {
		b = (*treeBuilder)(implB)
	}

	return
}

func (repo *repository) TreeBuilderFromTree(ctx context.Context, t git.Tree) (b git.TreeBuilder, err error) {
	var implB *impl.TreeBuilder

	if err = ctx.Err(); err != nil {
		return
	}

	if it, ok := t.(*tree); ok {
		if implB, err = repo.impl.TreeBuilderFromTree((*impl.Tree)(it)); err == nil {
			b = (*treeBuilder)(implB)
		}
		return
	}

	err = NewUnsupportedImplementationError(t)
	return
}

func (repo *repository) CommitBuilder(ctx context.Context) (b git.CommitBuilder, err error) {
	if err = ctx.Err(); err == nil {
		b = &commitBuilder{repo: repo}
	}

	return
}
