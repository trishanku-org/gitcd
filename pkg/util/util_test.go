package util

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ToCanonicalRelativePath", func() {
	for p, cp := range map[string]string{
		"":         "",
		".":        "",
		"a":        "a",
		"/":        "",
		"//":       "",
		"///":      "",
		"a/":       "a",
		"a/a":      "a/a",
		"a//a":     "a/a",
		"a///a/":   "a/a",
		"///a//a/": "a/a",
	} {
		func(p, cp string) {
			It(fmt.Sprintf("of %q, should be %q", p, cp), func() {
				Expect(ToCanonicalRelativePath(p)).To(Equal(cp))
			})
		}(p, cp)
	}
})

var _ = Describe("ToCanonicalPath", func() {
	for p, cp := range map[string]string{
		"":         "",
		".":        "",
		"a":        "a",
		"/":        "/",
		"//":       "/",
		"///":      "/",
		"a/":       "a",
		"a/a":      "a/a",
		"a//a":     "a/a",
		"a///a/":   "a/a",
		"///a//a/": "/a/a",
	} {
		func(p, cp string) {
			It(fmt.Sprintf("of %q, should be %q", p, cp), func() {
				Expect(ToCanonicalPath(p)).To(Equal(cp))
			})
		}(p, cp)
	}
})

var _ = Describe("PathSlice", func() {
	var ps PathSlice
	Describe("BoundedIndex", func() {
		for _, t := range []struct {
			path   string
			checks map[int]int
		}{
			{
				checks: map[int]int{
					-1: 0,
					0:  0,
					1:  0,
				},
			},
			{
				path: "/",
				checks: map[int]int{
					-1: 0,
					0:  0,
					1:  0,
				},
			},
			{
				path: "a",
				checks: map[int]int{
					-1: 0,
					0:  0,
					1:  1,
					2:  1,
				},
			},
			{
				path: "/a",
				checks: map[int]int{
					-1: 0,
					0:  0,
					1:  1,
					2:  1,
				},
			},
			{
				path: "a/a",
				checks: map[int]int{
					-1: 0,
					0:  0,
					1:  1,
					2:  2,
					3:  2,
				},
			},
		} {
			func(p string, checks map[int]int) {
				Describe(fmt.Sprintf("with path %q", p), func() {
					BeforeEach(func() {
						ps = SplitPath(p)
					})

					for i, r := range checks {
						func(i, r int) {
							It(fmt.Sprintf("of %d should be %d", i, r), func() {
								Expect(ps.BoundedIndex(i)).To(Equal(r))
							})
						}(i, r)
					}
				})
			}(t.path, t.checks)
		}
	})

	Describe("GetRelevantPathForDepthOf", func() {
		for _, t := range []struct {
			path   string
			checks map[string]string
		}{
			{
				checks: map[string]string{
					"":       "",
					"/":      "",
					"b":      "",
					"/b":     "",
					"b/b":    "",
					"/b//b/": "",
				},
			},
			{
				path: "/",
				checks: map[string]string{
					"":       "",
					"/":      "",
					"b":      "",
					"/b":     "",
					"b/b":    "",
					"/b//b/": "",
				},
			},
			{
				path: "a",
				checks: map[string]string{
					"":       "",
					"/":      "",
					"b":      "a",
					"/b":     "a",
					"b/b":    "a",
					"/b//b/": "a",
				},
			},
			{
				path: "//a/",
				checks: map[string]string{
					"":       "",
					"/":      "",
					"b":      "a",
					"/b":     "a",
					"b/b":    "a",
					"/b//b/": "a",
				},
			},
			{
				path: "a/a/a",
				checks: map[string]string{
					"":             "",
					"/":            "",
					"b":            "a",
					"/b":           "a",
					"b/b":          "a/a",
					"/b//b/":       "a/a",
					"b/b/b":        "a/a/a",
					"//b/b///b/":   "a/a/a",
					"/b//b///b/b/": "a/a/a",
				},
			},
			{
				path: "//a/a///a/",
				checks: map[string]string{
					"":             "",
					"/":            "",
					"b":            "a",
					"/b":           "a",
					"b/b":          "a/a",
					"/b//b/":       "a/a",
					"b/b/b":        "a/a/a",
					"//b/b///b/":   "a/a/a",
					"/b//b///b/b/": "a/a/a",
				},
			},
		} {
			func(p string, checks map[string]string) {
				Describe(fmt.Sprintf("with path %q", p), func() {
					BeforeEach(func() {
						ps = SplitPath(p)
					})

					for i, r := range checks {
						func(i, r string) {
							It(fmt.Sprintf("of %q should be %q", i, r), func() {
								Expect(ps.GetRelevantPathForDepthOf(i)).To(Equal(r))
							})
						}(i, r)
					}
				})
			}(t.path, t.checks)
		}
	})
})
