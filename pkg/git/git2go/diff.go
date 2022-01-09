package git2go

import (
	"context"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

// diffChange implements the DiffChange interface in the parent git package.
type diffChange impl.DiffDelta

var _ git.DiffChange = (*diffChange)(nil)

func (d *diffChange) impl() *impl.DiffDelta   { return (*impl.DiffDelta)(d) }
func (d *diffChange) OldBlobID() git.ObjectID { return git.ObjectID(*d.impl().OldFile.Oid) }
func (d *diffChange) NewBlobID() git.ObjectID { return git.ObjectID(*d.impl().NewFile.Oid) }

func (d *diffChange) Type() git.DiffChangeType {
	switch d.impl().Status {
	case impl.DeltaAdded:
		return git.DiffChangeTypeAdded
	case impl.DeltaDeleted:
		return git.DiffChangeTypeDeleted
	default:
		return git.DiffChangeTypeModified // Map all other types to Modified.
	}
}

func (d *diffChange) Path() string {
	switch d.Type() {
	case git.DiffChangeTypeDeleted:
		return d.impl().OldFile.Path
	default:
		return d.impl().NewFile.Path
	}
}

// diff implements the Diff interface defined in the parent git package.
type diff impl.Diff

var _ git.Diff = (*diff)(nil)

func (d *diff) impl() *impl.Diff { return (*impl.Diff)(d) }
func (d *diff) Close() error     { return d.impl().Free() }

func (d *diff) ForEachDiffChange(ctx context.Context, receiverFn git.DiffChangeReceiverFunc) (err error) {
	var ndeltas int

	if err = ctx.Err(); err != nil {
		return
	}

	if ndeltas, err = d.impl().NumDeltas(); err != nil {
		return
	}

	for i := 0; i < ndeltas; i++ {
		var (
			implDelta impl.DiffDelta
			done      bool
		)

		if err = ctx.Err(); err != nil {
			return
		}

		if implDelta, err = d.impl().Delta(i); err != nil {
			return
		}

		if done, err = receiverFn(ctx, (*diffChange)(&implDelta)); err != nil || done {
			return
		}
	}

	return
}
