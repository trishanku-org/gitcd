package backend

import (
	"context"
	"path"
	"strings"

	"github.com/trishanku/gitcd/pkg/git"
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

type intervalExplorerFilterFunc func(context.Context, git.TreeEntry) bool

type intervalExplorerReceiverFunc func(ctx context.Context, key string, te git.TreeEntry) (done, skip bool, err error)

type intervalExplorer struct {
	repo      git.Repository
	interval  interval
	tree      git.Tree
	keyPrefix string
}

func toCanonicalRelativePath(p string) string {
	if p = path.Clean(p); path.IsAbs(p) || p == "." {
		return p[1:]
	}

	return p
}

func (ie *intervalExplorer) getPathForKey(key string) string {
	var prefix = ie.keyPrefix

	if len(prefix) == 0 {
		return toCanonicalRelativePath(key)
	}

	// Ignore the trailing '/' if any.
	if prefix[len(prefix)-1] == '/' {
		prefix = prefix[:len(prefix)-1]
	}

	if strings.HasPrefix(key, prefix) {
		// Only consider if the prefix is followed by a '/' in the key.
		if key = key[len(prefix):]; len(key) > 0 && key[0] == '/' {
			return toCanonicalRelativePath(key)
		}
	}

	return ""
}

func (ie *intervalExplorer) getKeyForPath(p string) string {

	return path.Clean(path.Join(ie.keyPrefix, p))
}

func (ie *intervalExplorer) doFilterAndReceive(ctx context.Context, key string, te git.TreeEntry, receiverFn intervalExplorerReceiverFunc, filterFns ...intervalExplorerFilterFunc) (done, skip bool, err error) {
	for _, fn := range filterFns {
		if !fn(ctx, te) {
			return
		}
	}

	return receiverFn(ctx, key, te)
}

func (ie *intervalExplorer) ForEachMatchingKey(ctx context.Context, receiverFn intervalExplorerReceiverFunc, filterFns ...intervalExplorerFilterFunc) (err error) {
	var startPathSlice pathSlice

	if ie.interval.IsSingleton() {
		var (
			k  = ie.interval.GetStartInclusive().String()
			te git.TreeEntry
		)

		if te, err = ie.tree.GetEntryByPath(ctx, ie.getPathForKey(k)); err != nil {
			return
		}

		_, _, err = ie.doFilterAndReceive(ctx, k, te, receiverFn, filterFns...)
		return
	}

	startPathSlice = pathSlice(splitPath(ie.getPathForKey(ie.interval.GetStartInclusive().String())))

	return ie.repo.TreeWalker().ForEachTreeEntry(
		ctx,
		ie.tree,
		func(ctx context.Context, parentPath string, te git.TreeEntry) (done, skip bool, err error) {
			var (
				p               = path.Join(parentPath, te.EntryName())
				k               = key(ie.getKeyForPath(p))
				startKeyForPath = key(ie.getKeyForPath(startPathSlice.getRelevantPathForDepthOf(p)))
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
}

func splitPath(p string) (ps []string) {
	ps = strings.Split(p, "/")

	if ps[0] == "" {
		ps = ps[1:]
	}

	if ps[len(ps)-1] == "" {
		ps = ps[:len(ps)-1]
	}

	return
}

type pathSlice []string

func (ps pathSlice) boundedIndex(i int) int {
	switch {
	case i <= len(ps):
		return i
	default:
		return len(ps)
	}
}

func (ps pathSlice) getRelevantPathForDepthOf(p string) string {
	return path.Join(ps[:ps.boundedIndex(len(splitPath(p)))]...)
}
