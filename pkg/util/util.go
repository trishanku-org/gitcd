package util

import (
	"path"
	"strings"
)

func ToCanonicalPath(p string) string {
	if p = path.Clean(p); p == "." {
		p = p[1:]
	}

	return p
}

func ToCanonicalRelativePath(p string) string {
	if p = ToCanonicalPath(p); path.IsAbs(p) {
		p = p[1:]
	}

	return p
}

func SplitPath(p string) (ps []string) {
	p = ToCanonicalRelativePath(p)

	if len(p) > 0 {
		ps = strings.Split(p, PathSeparator)
	}

	return
}

// TODO document and test below this line

const (
	PathSeparator = "/"
)

type PathSlice []string

func (ps PathSlice) BoundedIndex(i int) int {
	switch {
	case i < 0:
		return 0
	case i <= len(ps):
		return i
	default:
		return len(ps)
	}
}

func (ps PathSlice) GetRelevantPathForDepthOf(p string) string {
	return path.Join(ps[:ps.BoundedIndex(len(SplitPath(p)))]...)
}
