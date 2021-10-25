package util

import "path"

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
