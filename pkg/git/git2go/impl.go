package git2go

import (
	"context"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

// gitImpl implements the Git Interface defined in the parent git package.
type gitImpl struct{}

// New returns an interface to a Git implementation.
func New() git.Interface {
	return &gitImpl{}
}

func (gitImpl) OpenOrInitBareRepository(ctx context.Context, path string) (git.Repository, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if implR, err := impl.OpenRepository(path); err != nil {
		var firstErr = err
		if implR, err = impl.InitRepository(path, true); err != nil {
			return nil, firstErr
		}

		return &repository{impl: implR, path: path}, nil
	} else {
		return &repository{impl: implR, path: path}, nil
	}
}

func (i gitImpl) Errors() git.Errors { return i }

// freeable defines the contract to free implementation objects.
type freeable interface {
	Free()
}

// free safely frees a freeable object.
func free(f freeable) error {
	if f != nil {
		f.Free()
	}

	return nil
}
