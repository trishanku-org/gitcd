package backend

import (
	"context"
	"errors"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/trishanku/gitcd/pkg/git"

	"k8s.io/utils/pointer"
)

var _ = Describe("key", func() {
	for _, t := range []struct {
		k, t   key
		result cmpResult
	}{
		{result: cmpResultEqual},
		{t: key("/"), result: cmpResultLess},
		{k: key("/"), result: cmpResultMore},
		{k: key("/"), t: key("/"), result: cmpResultEqual},
		{k: key("/"), t: key("/a"), result: cmpResultLess},
		{k: key("/a/a"), t: key("/b"), result: cmpResultLess},
	} {
		func(k, t key, result cmpResult) {
			It(fmt.Sprintf("%#v.Cmp(%#v)", k.String(), t.String()), func() {
				Expect(k.Cmp(t)).To(Equal(result))
			})
		}(t.k, t.t, t.result)
	}
})

var _ = Describe("closedOpenInterval", func() {
	for _, t := range []struct {
		spec       string
		start, end key
		singleton  bool
		checks     map[string]checkResult
	}{
		{
			spec:      `/a`,
			start:     key("/a"),
			singleton: true,
			checks: map[string]checkResult{
				"":     checkResultOutOfRangeLeft,
				"\x00": checkResultOutOfRangeLeft,
				"/":    checkResultOutOfRangeLeft,
				"/a":   checkResultInRange,
				"/a/":  checkResultOutOfRangeRight,
				"/a/a": checkResultOutOfRangeRight,
				"/b":   checkResultOutOfRangeRight,
				"/b/b": checkResultOutOfRangeRight,
			},
		},
		{
			spec:      `[\0, /b)`,
			start:     key("\x00"),
			end:       key("/b"),
			singleton: false,
			checks: map[string]checkResult{
				"":     checkResultInRange,
				"\x00": checkResultInRange,
				"/":    checkResultInRange,
				"/a":   checkResultInRange,
				"/a/":  checkResultInRange,
				"/a/a": checkResultInRange,
				"/b":   checkResultOutOfRangeRight,
				"/b/b": checkResultOutOfRangeRight,
			},
		},
		{
			spec:      `[/a, /b)`,
			start:     key("/a"),
			end:       key("/b"),
			singleton: false,
			checks: map[string]checkResult{
				"":     checkResultOutOfRangeLeft,
				"\x00": checkResultOutOfRangeLeft,
				"/":    checkResultOutOfRangeLeft,
				"/a":   checkResultInRange,
				"/a/":  checkResultInRange,
				"/a/a": checkResultInRange,
				"/b":   checkResultOutOfRangeRight,
				"/b/b": checkResultOutOfRangeRight,
			},
		},
		{
			spec:      `[/a, \0)`,
			start:     key("/a"),
			end:       key("\x00"),
			singleton: false,
			checks: map[string]checkResult{
				"":     checkResultOutOfRangeLeft,
				"\x00": checkResultOutOfRangeLeft,
				"/":    checkResultOutOfRangeLeft,
				"/a":   checkResultInRange,
				"/a/":  checkResultInRange,
				"/a/a": checkResultInRange,
				"/b":   checkResultInRange,
				"/b/b": checkResultInRange,
			},
		},
		{
			spec:      `[\0, \0)`,
			start:     key("\x00"),
			end:       key("\x00"),
			singleton: false,
			checks: map[string]checkResult{
				"":     checkResultInRange,
				"\x00": checkResultInRange,
				"/":    checkResultInRange,
				"/a":   checkResultInRange,
				"/a/":  checkResultInRange,
				"/a/a": checkResultInRange,
				"/b":   checkResultInRange,
				"/b/b": checkResultInRange,
			},
		},
	} {
		func(spec string, start, end key, singleton bool, checks map[string]checkResult) {
			Describe(spec, func() {
				var i interval

				BeforeEach(func() {
					i = &closedOpenInterval{start: start, end: end}
				})

				if singleton {
					It("should be singleton", func() { Expect(i.IsSingleton()).To(BeTrue()) })
				} else {
					It("should not be singleton", func() { Expect(i.IsSingleton()).To(BeFalse()) })
				}

				It(fmt.Sprintf("should start from %q", start.String()), func() {
					Expect(i.GetStartInclusive()).To(Equal(start))
				})

				Describe("Check", func() {
					for s, result := range checks {
						func(s string, result checkResult) {
							It(fmt.Sprintf("%#v should be %v", s, result), func() {
								var k key

								if len(s) > 0 {
									k = key(s)
								}

								Expect(i.Check(k)).To(Equal(result))
							})
						}(s, result)
					}
				})
			})
		}(t.spec, t.start, t.end, t.singleton, t.checks)
	}
})

var _ = Describe("toCanonicalRelativePath", func() {
	for p, cp := range map[string]string{
		"":         "",
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
				Expect(toCanonicalRelativePath(p)).To(Equal(cp))
			})
		}(p, cp)
	}
})

var _ = Describe("intervalExplorer", func() {
	var ie *intervalExplorer

	BeforeEach(func() {
		ie = &intervalExplorer{}
	})

	Describe("getPathForKey", func() {
		for _, t := range []struct {
			prefix string
			checks map[string]string
		}{
			{
				checks: map[string]string{
					"a":     "a",
					"a/":    "a",
					"/a":    "a",
					"/a/":   "a",
					"a/a":   "a/a",
					"/a/a/": "a/a",
				},
			},
			{
				prefix: "p",
				checks: map[string]string{
					"a":      "",
					"a/":     "",
					"/a":     "",
					"/a/":    "",
					"a/a":    "",
					"/a/a/":  "",
					"/p":     "",
					"/p/":    "",
					"/pa":    "",
					"/p/a":   "",
					"p":      "",
					"pa":     "",
					"p/":     "",
					"p/a":    "a",
					"p/a/":   "a",
					"p/a/a":  "a/a",
					"p/a/a/": "a/a",
				},
			},
			{
				prefix: "/p",
				checks: map[string]string{
					"a":       "",
					"a/":      "",
					"/a":      "",
					"/a/":     "",
					"a/a":     "",
					"/a/a/":   "",
					"/p":      "",
					"/p/":     "",
					"/p/a":    "a",
					"/p/a/":   "a",
					"/p/a/a":  "a/a",
					"/p/a/a/": "a/a",
					"/pa":     "",
					"p/a":     "",
					"pa":      "",
				},
			},
			{
				prefix: "/p/",
				checks: map[string]string{
					"a":       "",
					"a/":      "",
					"/a":      "",
					"/a/":     "",
					"a/a":     "",
					"/a/a/":   "",
					"/p":      "",
					"/p/":     "",
					"/p/a":    "a",
					"/p/a/":   "a",
					"/p/a/a":  "a/a",
					"/p/a/a/": "a/a",
					"/pa":     "",
					"p/a":     "",
					"pa":      "",
				},
			},
		} {
			func(prefix string, checks map[string]string) {
				Describe(fmt.Sprintf("with key prefix %q", prefix), func() {
					BeforeEach(func() {
						ie.keyPrefix = prefix
					})

					for k, p := range checks {
						func(k, p string) {
							It(fmt.Sprintf("%q should return %q", k, p), func() {
								Expect(ie.getPathForKey(k)).To(Equal(p))
							})
						}(k, p)
					}
				})
			}(t.prefix, t.checks)
		}
	})

	Describe("getKeyForPath", func() {
		for _, t := range []struct {
			prefix string
			checks map[string]string
		}{
			{
				checks: map[string]string{
					"a":     "a",
					"a/":    "a",
					"/a":    "/a",
					"/a/":   "/a",
					"a/a":   "a/a",
					"/a/a/": "/a/a",
				},
			},
			{
				prefix: "p",
				checks: map[string]string{
					"a":     "p/a",
					"a/":    "p/a",
					"/a":    "p/a",
					"/a/":   "p/a",
					"a/a":   "p/a/a",
					"/a/a/": "p/a/a",
					"/p":    "p/p",
					"/p/":   "p/p",
					"/pa":   "p/pa",
					"p":     "p/p",
					"p/":    "p/p",
					"pa":    "p/pa",
				},
			},
			{
				prefix: "/p",
				checks: map[string]string{
					"a":     "/p/a",
					"a/":    "/p/a",
					"/a":    "/p/a",
					"/a/":   "/p/a",
					"a/a":   "/p/a/a",
					"/a/a/": "/p/a/a",
					"/p":    "/p/p",
					"/p/":   "/p/p",
					"/pa":   "/p/pa",
					"p":     "/p/p",
					"p/":    "/p/p",
					"pa":    "/p/pa",
				},
			},
			{
				prefix: "/p/",
				checks: map[string]string{
					"a":     "/p/a",
					"a/":    "/p/a",
					"/a":    "/p/a",
					"/a/":   "/p/a",
					"a/a":   "/p/a/a",
					"/a/a/": "/p/a/a",
					"/p":    "/p/p",
					"/p/":   "/p/p",
					"/pa":   "/p/pa",
					"p":     "/p/p",
					"p/":    "/p/p",
					"pa":    "/p/pa",
				},
			},
		} {
			func(prefix string, checks map[string]string) {
				Describe(fmt.Sprintf("with key prefix %q", prefix), func() {
					BeforeEach(func() {
						ie.keyPrefix = prefix
					})

					for p, k := range checks {
						func(p, k string) {
							It(fmt.Sprintf("%q should return %q", p, k), func() {
								Expect(ie.getKeyForPath(p)).To(Equal(k))
							})
						}(p, k)
					}
				})
			}(t.prefix, t.checks)
		}
	})

	Describe("doFilterAndReceive", func() {
		for _, t := range []struct {
			filters []*bool
			result  bool
		}{
			{result: true},
			{filters: []*bool{pointer.BoolPtr(true)}, result: true},
			{filters: []*bool{pointer.BoolPtr(true), pointer.BoolPtr(true)}, result: true},
			{filters: []*bool{pointer.BoolPtr(true), pointer.BoolPtr(false)}, result: false},
			{filters: []*bool{pointer.BoolPtr(false)}, result: false},
			{filters: []*bool{pointer.BoolPtr(false), nil}, result: false},
		} {
			func(filters []*bool, result bool) {
				Describe(
					func() string {
						var s = "with filters ["

						for _, r := range filters {
							if r == nil {
								s = s + "nil, "
							} else {
								s = fmt.Sprintf("%s%t, ", s, *r)
							}
						}

						return strings.TrimSuffix(s, ", ") + "]"
					}(),
					func() {
						It(
							func() string {
								if result {
									return "should filter"
								}

								return "should not filter"
							}(),
							func() {
								var (
									fns                []intervalExplorerFilterFunc
									key                = "key"
									received           bool
									expectedReceiveErr = errors.New("error")
									done, skip         bool
									err                error
								)

								for _, r := range filters {
									func(r *bool) {
										fns = append(fns, intervalExplorerFilterFunc(func(_ context.Context, _ git.TreeEntry) bool {
											return *r
										}))
									}(r)
								}

								done, skip, err = ie.doFilterAndReceive(
									context.Background(),
									key,
									nil,
									func(_ context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
										Expect(k).To(Equal(key))
										Expect(te).To(BeNil())

										received = true

										done = true
										err = expectedReceiveErr
										return
									},
									fns...,
								)

								Expect(skip).To(BeFalse())
								Expect(received).To(Equal(result))

								if result {
									Expect(done).To(BeTrue())
									Expect(err).To(MatchError(expectedReceiveErr))
								} else {
									Expect(done).To(BeFalse())
									Expect(err).ToNot(HaveOccurred())
								}
							},
						)
					})
			}(t.filters, t.result)
		}
	})
})
