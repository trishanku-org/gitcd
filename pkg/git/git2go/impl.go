package git2go

import (
	"context"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

// gitImpl implements the Git Interface defined in the parent git package.
type gitImpl struct{}

var _ git.Interface = &gitImpl{}

func (gitImpl) OpenOrInitBareRepository(ctx context.Context, path string) (git.Repository, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if implR, err := impl.OpenRepository(path); err != nil {
		var firstErr = err
		if implR, err = impl.InitRepository(path, true); err != nil {
			return nil, firstErr
		}

		return &repository{impl: implR}, nil
	} else {
		return &repository{impl: implR}, nil
	}
}
