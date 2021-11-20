package git2go

import (
	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

var _ git.Errors = gitImpl{}

func (gitImpl) isErrCode(err error, c impl.ErrorCode) bool {
	if gErr, ok := err.(*impl.GitError); ok {
		return gErr.Code == c
	}

	return false
}

func (gitImpl) ignoreErrCode(err error, c impl.ErrorCode) error {
	if gErr, ok := err.(*impl.GitError); ok && gErr.Code == c {
		return nil
	}

	return err
}

func (i gitImpl) IsNotFound(err error) bool {
	return i.isErrCode(err, impl.ErrorCodeNotFound)
}

func (i gitImpl) IgnoreNotFound(err error) error {
	return i.ignoreErrCode(err, impl.ErrorCodeNotFound)
}
