package git2go

import (
	"context"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

// remote implements the Remote interface defined in the parent git package.
type remote impl.Remote

var _ git.Remote = (*remote)(nil)

func (r *remote) impl() *impl.Remote   { return (*impl.Remote)(r) }
func (r *remote) Close() error         { return free(r.impl()) }
func (r *remote) Name() git.RemoteName { return git.RemoteName(r.impl().Name()) }

func (r *remote) Fetch(ctx context.Context, refSpecs []git.RefSpec) (err error) {
	var rs []string

	if err = ctx.Err(); err != nil {
		return
	}

	rs = make([]string, len(refSpecs))

	for i, refSpec := range refSpecs {
		rs[i] = string(refSpec)
	}

	return r.impl().Fetch(
		rs,
		&impl.FetchOptions{
			RemoteCallbacks: impl.RemoteCallbacks{
				CredentialsCallback: NewCredentialUserpass,
			},
		},
		"",
	)
}

// remoteCollection implements the RemoteCollection interface defined in the parent git package.
type remoteCollection impl.RemoteCollection

var _ git.RemoteCollection = (*remoteCollection)(nil)

func (rc *remoteCollection) impl() *impl.RemoteCollection { return (*impl.RemoteCollection)(rc) }
func (rc *remoteCollection) Close() error                 { return free(rc.impl()) }

func (rc *remoteCollection) Get(ctx context.Context, remoteName git.RemoteName) (git.Remote, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if implR, err := rc.impl().Lookup(string(remoteName)); err != nil {
		return nil, err
	} else {
		return (*remote)(implR), nil
	}
}
