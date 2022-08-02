package backend

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/util"
)

type cmpResult int

const (
	cmpResultLess = cmpResult(iota - 1)
	cmpResultEqual
	cmpResultMore
)

const (
	zero = "\x00"
)

type key []byte

func (k key) String() string { return string(k) }

func (k key) Cmp(t key) cmpResult {
	var sk, st = k.String(), t.String()
	switch {
	case sk < st:
		return cmpResultLess
	case sk == st:
		return cmpResultEqual
	default:
		return cmpResultMore
	}
}

func (k key) IsZero() bool {
	return k.String() == zero
}

type checkResult int

const (
	checkResultOutOfRangeLeft = checkResult(iota - 1)
	checkResultInRange
	checkResultOutOfRangeRight
)

// interval defines the interface to interact with an interval of key strings.
type interval interface {
	// IsSingleton returns true if the interval selects a single key string at best.
	IsSingleton() bool
	// GetStartInclusive returns the starting key (inclusive) of the interval.
	GetStartInclusive() key
	// Check returns 0 if the string is part of the interval, -1 if it is out of the interval on the left (start) side
	// and 1 if it is out of the interval on the right (end) side.
	Check(key) checkResult
	// String returns the string representation of the interval.
	String() string
}

type closedOpenInterval struct {
	start, end key
}

var _ interval = &closedOpenInterval{}

func (i *closedOpenInterval) IsSingleton() bool {
	return len(i.end) == 0
}

func (i *closedOpenInterval) GetStartInclusive() (s key) {
	return i.start
}

func (i *closedOpenInterval) Check(s key) checkResult {
	if i.IsSingleton() {
		return checkResult(s.Cmp(i.start))
	}

	if !i.start.IsZero() && s.Cmp(i.start) == cmpResultLess {
		return checkResultOutOfRangeLeft
	}

	if !i.end.IsZero() && s.Cmp(i.end) != cmpResultLess {
		return checkResultOutOfRangeRight
	}

	return checkResultInRange
}

func (i *closedOpenInterval) String() string {
	return fmt.Sprintf("[%s, %s)", i.start.String(), i.end.String())
}

// Merge expands the interval to include the supplied interval and returnes it.
func (i *closedOpenInterval) merge(s *closedOpenInterval) *closedOpenInterval {
	if i.GetStartInclusive().Cmp(s.GetStartInclusive()) == cmpResultMore {
		i.start = s.start
	}

	if i.end.Cmp(s.end) == cmpResultLess {
		i.end = s.end
	}

	return i
}

func joinSafe(parent, child string) string {
	if len(parent) == 0 {
		return child
	}

	return path.Join(parent, child)
}

type intervalExplorerFilterFunc func(context.Context, git.TreeEntry) bool

func newObjectTypeFilter(typ git.ObjectType) intervalExplorerFilterFunc {
	return func(_ context.Context, te git.TreeEntry) bool {
		return te.EntryType() == typ
	}
}

type keyPrefix struct {
	prefix string
}

func (kp *keyPrefix) getPathForKey(key string) string {
	var prefix = kp.prefix

	if len(prefix) == 0 {
		return util.ToCanonicalPath(key)
	}

	prefix = strings.TrimSuffix(prefix, util.PathSeparator)

	if strings.HasPrefix(key, prefix) {
		// Only consider if the prefix is followed by a '/' in the key.
		if key = key[len(prefix):]; len(key) > 0 && key[:1] == util.PathSeparator {
			return util.ToCanonicalRelativePath(key)
		}
	}

	return ""
}

func (kp *keyPrefix) getKeyForPath(p string) string {
	return path.Clean(joinSafe(kp.prefix, p))
}

type intervalExplorerReceiverFunc func(ctx context.Context, key string, te git.TreeEntry) (done, skip bool, err error)

type intervalExplorer struct {
	keyPrefix
	repo     git.Repository
	errors   git.Errors
	tree     git.Tree
	interval interval
}

func (ie *intervalExplorer) doFilterAndReceive(ctx context.Context, key string, te git.TreeEntry, receiverFn intervalExplorerReceiverFunc, filterFns ...intervalExplorerFilterFunc) (done, skip bool, err error) {
	for _, fn := range filterFns {
		if !fn(ctx, te) {
			return
		}
	}

	return receiverFn(ctx, key, te)
}

func (ie *intervalExplorer) forEachMatchingKey(ctx context.Context, receiverFn intervalExplorerReceiverFunc, filterFns ...intervalExplorerFilterFunc) (err error) {
	var (
		startPathSlice util.PathSlice
		tw             git.TreeWalker
	)

	if ie.interval.IsSingleton() {
		var (
			k  = ie.interval.GetStartInclusive().String()
			te git.TreeEntry
		)

		if te, err = ie.tree.GetEntryByPath(ctx, ie.getPathForKey(k)); err != nil {
			err = ie.errors.IgnoreNotFound(err)
			return
		}

		_, _, err = ie.doFilterAndReceive(ctx, k, te, receiverFn, filterFns...)
		return
	}

	startPathSlice = util.PathSlice(util.SplitPath(ie.getPathForKey(ie.interval.GetStartInclusive().String())))

	tw = ie.repo.TreeWalker()

	defer tw.Close()

	err = tw.ForEachTreeEntry(
		ctx,
		ie.tree,
		func(ctx context.Context, parentPath string, te git.TreeEntry) (done, skip bool, err error) {
			var (
				p               = joinSafe(parentPath, te.EntryName())
				k               = key(ie.getKeyForPath(p))
				startKeyForPath = key(ie.getKeyForPath(startPathSlice.GetRelevantPathForDepthOf(p)))
			)

			if k.Cmp(startKeyForPath) == cmpResultLess {
				skip = true
				return
			}

			switch ie.interval.Check(k) {
			case checkResultOutOfRangeLeft:
				return
			case checkResultOutOfRangeRight:
				done = true
				return
			}

			return ie.doFilterAndReceive(ctx, k.String(), te, receiverFn, filterFns...)
		},
	)

	return
}
