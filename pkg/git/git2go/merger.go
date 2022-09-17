package git2go

import (
	"context"
	"fmt"
	"reflect"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
)

// TODO test

// mergerImpl implements the Merger Interface defined in the parent git package.
type mergerImpl struct {
	repo   *repository
	errors git.Errors

	conflictResolution git.MergeConfictResolution
	retentionPolicy    git.MergeRetentionPolicy
}

var _ git.Merger = (*mergerImpl)(nil)

func (*mergerImpl) Close() error                                          { return nil }
func (m *mergerImpl) GetConfictResolution() git.MergeConfictResolution    { return m.conflictResolution }
func (m *mergerImpl) GetRetentionPolicy() git.MergeRetentionPolicy        { return m.retentionPolicy }
func (m *mergerImpl) SetConflictResolution(cr git.MergeConfictResolution) { m.conflictResolution = cr }
func (m *mergerImpl) SetRetentionPolicy(rp git.MergeRetentionPolicy)      { m.retentionPolicy = rp }

func (m *mergerImpl) retainFromMergeTree(
	ctx context.Context,
	ours, merged git.Tree,
) (
	mutated bool,
	treeID git.ObjectID,
	err error,
) {
	var (
		retentionPolicy = m.GetRetentionPolicy()
		diff            git.Diff
		tm              *mutation.TreeMutation
	)

	defer func() {
		if err == nil {
			mutated = ours == nil || !reflect.DeepEqual(treeID, ours.ID())
		}
	}()

	switch retentionPolicy {
	case git.AllMergeRetentionPolicy():
		treeID = merged.ID()
		return
	case git.NoneMergeRetentionPolicy():
		if ours != nil {
			treeID = ours.ID()
		}
		return
	}

	if diff, err = m.repo.TreeDiff(ctx, ours, merged); err != nil {
		return
	}

	defer diff.Close()

	if err = diff.ForEachDiffChange(ctx, func(ctx context.Context, change git.DiffChange) (done bool, err error) {
		var (
			p      = change.Path()
			retain bool
		)

		if retain, err = retentionPolicy.Retain(ctx, change.Path()); err != nil || !retain {
			return
		}

		switch change.Type() {
		case git.DiffChangeTypeAdded:
			fallthrough
		case git.DiffChangeTypeModified:
			tm, err = mutation.AddMutation(
				tm,
				p,
				func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
					err = tb.AddEntry(entryName, change.NewBlobID(), git.FilemodeBlob)
					mutated = err == nil
					return
				},
			)
		case git.DiffChangeTypeDeleted:
			tm, err = mutation.AddMutation(tm, p, mutation.DeleteEntry)
		}

		return
	}); err != nil {
		return
	}

	if mutation.IsMutationNOP(tm) {
		if ours != nil {
			treeID = ours.ID()
		}
		return
	}

	mutated, treeID, err = mutation.MutateTree(ctx, m.repo, ours, tm, false)
	return
}

func (m *mergerImpl) mergeOptions() (*impl.MergeOptions, error) {
	var opts, err = impl.DefaultMergeOptions()

	opts.FileFavor = impl.MergeFileFavor(m.GetConfictResolution())
	opts.TreeFlags = opts.TreeFlags & ^impl.MergeTreeFindRenames // Renames will be treated as remove+add.

	return &opts, err
}

func (m *mergerImpl) indexToTree(ctx context.Context, index *impl.Index) (t git.Tree, err error) {
	var tOid *impl.Oid

	if err = ctx.Err(); err != nil {
		return
	}

	if tOid, err = index.WriteTreeTo(m.repo.impl); err != nil {
		return
	}

	if tOid == nil {
		return
	}

	t, err = m.repo.GetTree(ctx, git.ObjectID(*tOid))
	return
}

type indexActionFunc func() error

func indexRemoveByPath(index *impl.Index, p string) indexActionFunc {
	return func() error { return index.RemoveByPath(p) }
}

func indexAddEntry(index *impl.Index, ie *impl.IndexEntry) indexActionFunc {
	return func() error { return index.Add(ie) }
}

func indexRemoveConflict(index *impl.Index, p string) indexActionFunc {
	return func() error { return index.RemoveConflict(p) }
}

func (m *mergerImpl) resolveModifyDeleteConflicts(ctx context.Context, index *impl.Index) (err error) {
	var (
		i         *impl.IndexConflictIterator
		actionFns []func() error
	)

	if i, err = index.ConflictIterator(); err != nil {
		return
	}

	defer i.Free()

	for {
		var (
			ic impl.IndexConflict
			ie *impl.IndexEntry
			p  string
		)

		if ic, err = i.Next(); m.errors.IgnoreIterOver(err) != nil {
			return
		} else if err != nil { // IterOver
			for _, actionFn := range actionFns {
				if err = actionFn(); m.errors.IgnoreNotFound(err) != nil {
					return
				}
			}

			err = nil
			return
		}

		if ic.Our != nil && ic.Their != nil {
			// Should not happen. Modify/Modify conflict should be resolved using impl.FileFlavor.
			continue
		}

		switch {
		case ic.Ancestor != nil:
			p = ic.Ancestor.Path
		case ic.Our != nil:
			p = ic.Our.Path
		case ic.Their != nil:
			p = ic.Their.Path
		}

		switch m.conflictResolution {
		case git.MergeConfictResolutionFavorOurs:
			ie = ic.Our
		case git.MergeConfictResolutionFavorTheirs:
			ie = ic.Their
		default:
			err = fmt.Errorf("invalid conflict resolution %d", m.conflictResolution)
			return
		}

		actionFns = append(actionFns, indexRemoveConflict(index, p))

		if ie == nil {
			actionFns = append(actionFns, indexRemoveByPath(index, p))
		} else {
			actionFns = append(actionFns, indexAddEntry(index, ie))
		}
	}
}

func (m *mergerImpl) retainFromMergeIndex(
	ctx context.Context,
	ours git.Peelable,
	index *impl.Index,
) (
	mutated bool,
	treeID git.ObjectID,
	err error,
) {
	var oursT, mergeT git.Tree

	if ours != nil {
		if oursT, err = m.repo.PeelToTree(ctx, ours); err != nil {
			return
		}

		defer oursT.Close()
	}

	if mergeT, err = m.indexToTree(ctx, index); err != nil {
		return
	}

	defer mergeT.Close()

	mutated, treeID, err = m.retainFromMergeTree(ctx, oursT, mergeT)
	return
}

func (m *mergerImpl) retainFromMergePeelable(
	ctx context.Context,
	ours, merge git.Peelable,
) (
	mutated bool,
	treeID git.ObjectID,
	sameAsMergeTree bool,
	err error,
) {
	var oursT, mergeT git.Tree

	if ours != nil {
		if oursT, err = m.repo.PeelToTree(ctx, ours); err != nil {
			return
		}

		defer oursT.Close()
	}

	if merge != nil {
		if mergeT, err = m.repo.PeelToTree(ctx, merge); err != nil {
			return
		}

		defer mergeT.Close()
	}

	if mutated, treeID, err = m.retainFromMergeTree(ctx, oursT, mergeT); err != nil {
		return
	}

	sameAsMergeTree = reflect.DeepEqual(treeID, mergeT.ID())
	return
}

func (m *mergerImpl) MergeBase(ctx context.Context, ours, theirs git.Commit) (baseCommitID git.ObjectID, err error) {
	var (
		iOurs, iTheirs *commit
		ok             bool
	)

	if iOurs, ok = ours.(*commit); !ok {
		err = NewUnsupportedImplementationError(ours)
		return
	}

	if iTheirs, ok = theirs.(*commit); !ok {
		err = NewUnsupportedImplementationError(theirs)
		return
	}

	return m.mergeBase(ctx, iOurs, iTheirs)
}

func (m *mergerImpl) mergeBase(ctx context.Context, ours, theirs *commit) (baseCommitID git.ObjectID, err error) {
	var baseOid *impl.Oid

	if err = ctx.Err(); err != nil {
		return
	}

	if ours == nil {
		baseCommitID = theirs.ID()
		return
	}

	if baseOid, err = m.repo.impl.MergeBase(ours.Id(), theirs.Id()); err != nil {
		return
	}

	baseCommitID = git.ObjectID(*baseOid)
	return
}

func (m *mergerImpl) MergeCommits(
	ctx context.Context,
	ours, theirs git.Commit,
	noFastForward bool,
	createCommitFn git.CreateCommitFunc,
) (
	mutated bool,
	headID git.ObjectID,
	err error,
) {
	var (
		iOurs, iTheirs       *commit
		ok, treeMutated      bool
		baseCommitID, treeID git.ObjectID
		opts                 *impl.MergeOptions
		index                *impl.Index
	)

	if err = ctx.Err(); err != nil {
		return
	}

	if theirs == nil {
		// Nothing to merge. Ours remains unmodified.
		mutated = false

		if ours != nil {
			headID = ours.ID()
		}

		return
	} else if iTheirs, ok = theirs.(*commit); !ok {
		err = NewUnsupportedImplementationError(theirs)
		return
	}

	if ours == nil {
		// Simple merge. Theirs is the merge candidate.
		var sameAsTheirTree bool

		if _, treeID, sameAsTheirTree, err = m.retainFromMergePeelable(ctx, nil, theirs); err != nil {
			return
		}

		if sameAsTheirTree && !noFastForward {
			mutated, headID = true, theirs.ID() // Fast-forward
			return
		}

		if headID, err = createCommitFn(ctx, treeID, theirs); err != nil {
			return
		}

		mutated = true
		return
	} else if iOurs, ok = ours.(*commit); !ok {
		err = NewUnsupportedImplementationError(ours)
		return
	}

	if reflect.DeepEqual(ours.ID(), theirs.ID()) {
		// Nothing to do. Both sides are identical.
		mutated, headID = false, ours.ID()
		return
	}

	if baseCommitID, err = m.mergeBase(ctx, iOurs, iTheirs); m.errors.IgnoreNotFound(err) != nil {
		return
	} else if err == nil { // Merge base found.
		if reflect.DeepEqual(baseCommitID, theirs.ID()) {
			// Theirs is already merged to ours. Ours remains unmodified.
			mutated, headID = false, ours.ID()
			return
		}

		if reflect.DeepEqual(baseCommitID, ours.ID()) {
			// Simple merge. Theirs is the merge candidate because it has already merged ours but has gone ahead.
			var sameAsTheirTree bool

			if treeMutated, treeID, sameAsTheirTree, err = m.retainFromMergePeelable(ctx, ours, theirs); err != nil {
				return
			}

			if sameAsTheirTree && !noFastForward {
				mutated, headID = true, theirs.ID() // Fast-forward
				return
			}

			if !treeMutated {
				treeID = ours.TreeID() // To be safe.
			}

			if headID, err = createCommitFn(ctx, treeID, ours, theirs); err != nil {
				return
			}

			mutated = true
			return
		}
	}

	// Merge base not found or fast-forward not possible. Try merging either way.

	if opts, err = m.mergeOptions(); err != nil {
		return
	}

	if index, err = m.repo.impl.MergeCommits(iOurs.impl(), iTheirs.impl(), opts); err != nil {
		return
	}

	defer index.Free()

	if err = m.resolveModifyDeleteConflicts(ctx, index); err != nil {
		return
	}

	treeMutated, treeID, err = m.retainFromMergeIndex(ctx, ours, index)

	if !treeMutated {
		treeID = ours.TreeID() // To be safe.
	}

	if headID, err = createCommitFn(ctx, treeID, ours, theirs); err != nil {
		return
	}

	mutated = true
	return
}
