package git2go

import (
	"context"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

// reference implements the Reference interface defined in the parent git package.
type reference impl.Reference

var _ git.Reference = &reference{}

func (ref *reference) impl() *impl.Reference   { return (*impl.Reference)(ref) }
func (ref *reference) Close() error            { ref.impl().Free(); return nil }
func (ref *reference) Name() git.ReferenceName { return git.ReferenceName(ref.impl().Name()) }
func (ref *reference) IsBranch() bool          { return ref.impl().IsBranch() }
func (ref *reference) IsRemote() bool          { return ref.impl().IsRemote() }
func (ref *reference) IsSymbolic() bool        { return ref.impl().Type() == impl.ReferenceSymbolic }

func (ref *reference) Peel(ctx context.Context, r git.ObjectReceiver) error {
	if implO, err := ref.impl().Peel(toImplObjectType(r.Type())); err != nil {
		return err
	} else {
		return r.Receive(ctx, (*object)(implO))
	}
}

// referenceCollection implements the ReferenceCollection interface defined in the parent git package.
type referenceCollection impl.ReferenceCollection

var _ git.ReferenceCollection = &referenceCollection{}

func (rc *referenceCollection) impl() *impl.ReferenceCollection {
	return (*impl.ReferenceCollection)(rc)
}

func (rc *referenceCollection) Get(_ context.Context, refName git.ReferenceName) (git.Reference, error) {
	if implR, err := rc.impl().Lookup(string(refName)); err != nil {
		return nil, err
	} else {
		return (*reference)(implR), nil
	}
}
