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
