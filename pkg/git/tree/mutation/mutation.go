package mutation

import (
	"context"
	"reflect"

	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/util"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// MutateFunc defines the contract to mutate a tree entry.
// The current tree entry which is passed as an argument might be nil if such an entry does not exist yet.
// This function can be used as a callback to simultaneously retrieve previous content while mutating the tree entry.
type MutateTreeEntryFunc func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error)

// TreeMutation defines the contract to combile multiple changes to a tree.
type TreeMutation struct {
	Entries  map[string]MutateTreeEntryFunc
	Subtrees map[string]*TreeMutation
}

func IsTreeEmpty(ctx context.Context, repo git.Repository, treeID git.ObjectID) (empty bool, err error) {
	var (
		t          git.Tree
		hasEntries = false
	)

	if t, err = repo.ObjectGetter().GetTree(ctx, treeID); err != nil {
		return
	}

	defer t.Close()

	if err = t.ForEachEntry(ctx, func(_ context.Context, te git.TreeEntry) (done bool, err error) {
		hasEntries = true
		done = true
		return
	}); err != nil {
		return
	}

	empty = !hasEntries
	return
}

func DeleteEntry(_ context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
	if te != nil {
		err = tb.RemoveEntry(entryName)
		mutated = err == nil
	}
	return
}

var _ MutateTreeEntryFunc = DeleteEntry

func MutateTreeBuilder(
	ctx context.Context,
	repo git.Repository,
	t git.Tree,
	tb git.TreeBuilder,
	tm *TreeMutation,
	cleanupEmptySubtrees bool,
) (
	mutated bool,
	err error,
) {
	// Process subtrees first to ensure depth first mutation.

	for entryName, stm := range tm.Subtrees {
		var (
			te             git.TreeEntry
			st             git.Tree
			stb            git.TreeBuilder
			subtreeMutated bool
			entryID        git.ObjectID
		)

		if t != nil {
			if te, err = t.GetEntryByPath(ctx, entryName); err == nil {
				if te.EntryType() == git.ObjectTypeTree {
					if st, err = repo.ObjectGetter().GetTree(ctx, te.EntryID()); err != nil {
						return
					}

					defer st.Close()

					if stb, err = repo.TreeBuilderFromTree(ctx, st); err != nil {
						return
					}
				} else {
					te = nil // Just to be sure
				}
			} else if err == ctx.Err() {
				return // Return if context error
			}
		}

		if stb == nil {
			if stb, err = repo.TreeBuilder(ctx); err != nil {
				return
			}
		}

		defer stb.Close()

		if subtreeMutated, err = MutateTreeBuilder(ctx, repo, st, stb, stm, cleanupEmptySubtrees); err != nil {
			return
		}

		if !subtreeMutated {
			continue
		}

		if entryID, err = stb.Build(ctx); err != nil {
			return
		}

		if st != nil {
			if subtreeMutated = !reflect.DeepEqual(entryID, st.ID()); subtreeMutated {
				if err = tb.RemoveEntry(entryName); err != nil {
					return
				}
			}
		}

		if !subtreeMutated {
			continue
		}

		mutated = mutated || subtreeMutated

		if cleanupEmptySubtrees {
			var empty bool

			if empty, err = IsTreeEmpty(ctx, repo, entryID); err != nil {
				return
			}

			if empty {
				continue // Skip adding the entry. It would already have been removed above.
			}
		}

		if err = tb.AddEntry(entryName, entryID, git.FilemodeTree); err != nil {
			return
		}
	}

	for entryName, entryMutateFn := range tm.Entries {
		var (
			te           git.TreeEntry
			entryMutated bool
		)

		if t != nil {
			if te, err = t.GetEntryByPath(ctx, entryName); err != nil && err == ctx.Err() {
				return // Return if context error
			}
		}

		if entryMutated, err = entryMutateFn(ctx, tb, entryName, te); err != nil {
			return
		}

		mutated = mutated || entryMutated
	}

	return
}

func MutateTree(
	ctx context.Context,
	repo git.Repository,
	currentT git.Tree,
	tm *TreeMutation,
	cleanupEmptySubtrees bool,
) (
	mutated bool,
	newTreeID git.ObjectID,
	err error,
) {
	var tb git.TreeBuilder

	if currentT != nil {
		tb, err = repo.TreeBuilderFromTree(ctx, currentT)
	} else {
		tb, err = repo.TreeBuilder(ctx)
	}

	if err != nil {
		return
	}

	defer tb.Close()

	if mutated, err = MutateTreeBuilder(ctx, repo, currentT, tb, tm, cleanupEmptySubtrees); err != nil {
		return
	}

	if !mutated {
		return
	}

	if newTreeID, err = tb.Build(ctx); err != nil {
		return
	}

	if currentT != nil {
		mutated = !reflect.DeepEqual(newTreeID, currentT.ID())
	}

	return
}

func AddMutationPathSlice(tm *TreeMutation, ps util.PathSlice, entryName string, entryMutateFn MutateTreeEntryFunc) (newTM *TreeMutation, err error) {
	var (
		stName string
		stm    *TreeMutation
		stOK   bool
	)

	if tm == nil {
		tm = &TreeMutation{}
	}

	if len(ps) == 0 {
		if tm.Entries == nil {
			tm.Entries = make(map[string]MutateTreeEntryFunc)
		}

		tm.Entries[entryName] = entryMutateFn // TODO error if already exists

		newTM = tm
		return
	}

	if tm.Subtrees == nil {
		tm.Subtrees = make(map[string]*TreeMutation)
	}

	stName = ps[0]

	if stm, stOK = tm.Subtrees[stName]; !stOK {
		stm = &TreeMutation{}
	}

	tm.Subtrees[stName], err = AddMutationPathSlice(stm, ps[1:], entryName, entryMutateFn)

	newTM = tm
	return
}

func AddMutation(tm *TreeMutation, p string, mutateFn MutateTreeEntryFunc) (newTM *TreeMutation, err error) {
	var ps = util.SplitPath(p)

	if len(ps) == 0 {
		newTM = tm
		err = rpctypes.ErrGRPCEmptyKey
		return
	}

	newTM, err = AddMutationPathSlice(tm, ps[:len(ps)-1], ps[len(ps)-1], mutateFn)
	return
}

func IsMutationNOP(tm *TreeMutation) bool {
	if tm == nil {
		return true
	}

	if len(tm.Entries) > 0 {
		return false
	}

	for _, stm := range tm.Subtrees {
		if !IsMutationNOP(stm) {
			return false
		}
	}

	return true
}
