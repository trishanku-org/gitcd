package git2go

import (
	"context"
	"reflect"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
)

// TODO test

// mergerImpl implements the Merger Interface defined in the parent git package.
type mergerImpl struct {
	repo *repository

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

	if tOid, err = index.WriteTree(); err != nil {
		return
	}

	if tOid == nil {
		return
	}

	t, err = m.repo.GetTree(ctx, git.ObjectID(*tOid))
	return
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

func (m *mergerImpl) MergeTrees(ctx context.Context, ancestor, ours, theirs git.Tree) (mutated bool, treeID git.ObjectID, err error) {
	var (
		iAncestor, iOurs, iTheirs *tree
		ok                        bool
		opts                      *impl.MergeOptions
		index                     *impl.Index
	)

	if err = ctx.Err(); err != nil {
		return
	}

	if theirs == nil {
		// Nothing to merge. Ours remains unmodified.
		return
	} else if iTheirs, ok = theirs.(*tree); !ok {
		err = NewUnsupportedImplementationError(theirs)
		return
	}

	if ours == nil {
		// Simple merge. Theirs is the merge candidate.
		mutated, treeID, err = m.retainFromMergeTree(ctx, nil, theirs)
		return
	} else if iOurs, ok = ours.(*tree); !ok {
		err = NewUnsupportedImplementationError(ours)
		return
	}

	if reflect.DeepEqual(ours.ID(), theirs.ID()) {
		// Nothing to merge. Ours and theirs are identical.
		return
	}

	if ancestor != nil {
		if iAncestor, ok = ancestor.(*tree); !ok {
			err = NewUnsupportedImplementationError(ancestor)
			return
		}
	}

	if opts, err = m.mergeOptions(); err != nil {
		return
	}

	if index, err = m.repo.impl.MergeTrees(iAncestor.impl(), iOurs.impl(), iTheirs.impl(), opts); err != nil {
		return
	}

	mutated, treeID, err = m.retainFromMergeIndex(ctx, ours, index)
	return
}

func (m *mergerImpl) retainFromMergePeelable(
	ctx context.Context,
	ours, merge git.Peelable,
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

	if merge != nil {
		if mergeT, err = m.repo.PeelToTree(ctx, merge); err != nil {
			return
		}

		defer mergeT.Close()
	}

	mutated, treeID, err = m.retainFromMergeTree(ctx, oursT, mergeT)
	return
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

func (m *mergerImpl) MergeTreesFromCommits(
	ctx context.Context,
	ours, theirs git.Commit,
) (
	mutated bool,
	treeID git.ObjectID,
	fastForward bool,
	err error,
) {
	var (
		iOurs, iTheirs *commit
		ok             bool
		baseCommitID   git.ObjectID
		opts           *impl.MergeOptions
		index          *impl.Index
	)

	if err = ctx.Err(); err != nil {
		return
	}

	if theirs == nil {
		// Nothing to merge. Ours remains unmodified.
		return
	} else if iTheirs, ok = theirs.(*commit); !ok {
		err = NewUnsupportedImplementationError(theirs)
		return
	}

	if ours == nil {
		// Simple merge. Theirs is the merge candidate.
		fastForward = true
		mutated, treeID, err = m.retainFromMergePeelable(ctx, nil, theirs)
		return
	} else if iOurs, ok = ours.(*commit); !ok {
		err = NewUnsupportedImplementationError(ours)
		return
	}

	if reflect.DeepEqual(ours.ID(), theirs.ID()) {
		// Nothing to do both sides are identical.
		return
	}

	if baseCommitID, err = m.mergeBase(ctx, iOurs, iTheirs); err != nil {
		return
	}

	if reflect.DeepEqual(baseCommitID, theirs.ID()) {
		// Theirs is already merged to ours. Ours remains unmodified.
		return
	}

	if reflect.DeepEqual(baseCommitID, ours.ID()) {
		// Simple merge. Theirs is the merge candidate because it has already merged ours but has gone ahead.
		fastForward = true
		mutated, treeID, err = m.retainFromMergePeelable(ctx, ours, theirs)
		return
	}

	if opts, err = m.mergeOptions(); err != nil {
		return
	}

	if index, err = m.repo.impl.MergeCommits(iOurs.impl(), iTheirs.impl(), opts); err != nil {
		return
	}

	mutated, treeID, err = m.retainFromMergeIndex(ctx, ours, index)
	return
}
