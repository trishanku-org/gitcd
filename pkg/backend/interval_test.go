package backend

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	mockgit "github.com/trishanku/gitcd/pkg/mocks/git"

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

var _ = Describe("closedOpenInterval#merge", func() {
	for _, s := range []struct {
		o, n, e *closedOpenInterval
	}{
		{o: &closedOpenInterval{}, n: &closedOpenInterval{}, e: &closedOpenInterval{}},
		{o: &closedOpenInterval{}, n: &closedOpenInterval{start: key("\x00")}, e: &closedOpenInterval{}},
		{o: &closedOpenInterval{}, n: &closedOpenInterval{start: key("a")}, e: &closedOpenInterval{}},
		{o: &closedOpenInterval{}, n: &closedOpenInterval{end: key("\x00")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{}, n: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{}, n: &closedOpenInterval{start: key("\x00"), end: key("x")}, e: &closedOpenInterval{end: key("x")}},
		{o: &closedOpenInterval{}, n: &closedOpenInterval{start: key("a"), end: key("b")}, e: &closedOpenInterval{end: key("b")}},
		{o: &closedOpenInterval{start: key("\x00")}, n: &closedOpenInterval{}, e: &closedOpenInterval{}},
		{o: &closedOpenInterval{start: key("\x00")}, n: &closedOpenInterval{start: key("\x00")}, e: &closedOpenInterval{start: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00")}, n: &closedOpenInterval{start: key("a")}, e: &closedOpenInterval{start: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00")}, n: &closedOpenInterval{end: key("\x00")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00")}, n: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, e: &closedOpenInterval{start: key("\x00"), end: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00")}, n: &closedOpenInterval{start: key("\x00"), end: key("x")}, e: &closedOpenInterval{start: key("\x00"), end: key("x")}},
		{o: &closedOpenInterval{start: key("\x00")}, n: &closedOpenInterval{start: key("a"), end: key("b")}, e: &closedOpenInterval{start: key("\x00"), end: key("b")}},
		{o: &closedOpenInterval{end: key("\x00")}, n: &closedOpenInterval{}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{end: key("\x00")}, n: &closedOpenInterval{start: key("\x00")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{end: key("\x00")}, n: &closedOpenInterval{start: key("a")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{end: key("\x00")}, n: &closedOpenInterval{end: key("\x00")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{end: key("\x00")}, n: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{end: key("\x00")}, n: &closedOpenInterval{start: key("\x00"), end: key("x")}, e: &closedOpenInterval{end: key("x")}},
		{o: &closedOpenInterval{end: key("\x00")}, n: &closedOpenInterval{start: key("a"), end: key("b")}, e: &closedOpenInterval{end: key("b")}},
		{o: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, n: &closedOpenInterval{}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, n: &closedOpenInterval{start: key("\x00")}, e: &closedOpenInterval{start: key("\x00"), end: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, n: &closedOpenInterval{start: key("a")}, e: &closedOpenInterval{start: key("\x00"), end: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, n: &closedOpenInterval{end: key("\x00")}, e: &closedOpenInterval{end: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, n: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, e: &closedOpenInterval{start: key("\x00"), end: key("\x00")}},
		{o: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, n: &closedOpenInterval{start: key("\x00"), end: key("x")}, e: &closedOpenInterval{start: key("\x00"), end: key("x")}},
		{o: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, n: &closedOpenInterval{start: key("a"), end: key("b")}, e: &closedOpenInterval{start: key("\x00"), end: key("b")}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{}, e: &closedOpenInterval{}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{start: key("\x00")}, e: &closedOpenInterval{start: key("\x00")}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{start: key("a")}, e: &closedOpenInterval{start: key("a")}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{end: key("c")}, e: &closedOpenInterval{end: key("c")}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, e: &closedOpenInterval{start: key("\x00"), end: key("\x00")}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{start: key("\x00"), end: key("x")}, e: &closedOpenInterval{start: key("\x00"), end: key("x")}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{start: key("a"), end: key("b")}, e: &closedOpenInterval{start: key("a"), end: key("b")}},
		{o: &closedOpenInterval{start: key("p")}, n: &closedOpenInterval{start: key("q"), end: key("r")}, e: &closedOpenInterval{start: key("p"), end: key("r")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{start: key("\x00")}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{start: key("a")}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{end: key("\x00")}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{end: key("r")}, e: &closedOpenInterval{end: key("r")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{start: key("\x00"), end: key("x")}, e: &closedOpenInterval{end: key("x")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{start: key("a"), end: key("b")}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{end: key("q")}, n: &closedOpenInterval{start: key("a"), end: key("q/1")}, e: &closedOpenInterval{end: key("q/1")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("\x00")}, e: &closedOpenInterval{start: key("\x00"), end: key("q")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("q")}, e: &closedOpenInterval{start: key("p"), end: key("q")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("a")}, e: &closedOpenInterval{start: key("a"), end: key("q")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{end: key("\x00")}, e: &closedOpenInterval{end: key("q")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, e: &closedOpenInterval{start: key("\x00"), end: key("q")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("\x00"), end: key("x")}, e: &closedOpenInterval{start: key("\x00"), end: key("x")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("a"), end: key("b")}, e: &closedOpenInterval{start: key("a"), end: key("q")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("o/1"), end: key("q/1")}, e: &closedOpenInterval{start: key("o/1"), end: key("q/1")}},
		{o: &closedOpenInterval{start: key("p"), end: key("q")}, n: &closedOpenInterval{start: key("p/1"), end: key("p/2")}, e: &closedOpenInterval{start: key("p"), end: key("q")}},
	} {
		func(o, n, e *closedOpenInterval) {
			It(fmt.Sprintf("old=%#v, new=%#v, expected=%#v", o, n, e), func() {
				var a = o.merge(n)

				Expect(a).To(BeIdenticalTo(o))
				Expect(a).To(Equal(e))
			})
		}(s.o, s.n, s.e)
	}
})

var _ = Describe("keyPrefix", func() {
	var kp *keyPrefix

	BeforeEach(func() {
		kp = &keyPrefix{}
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
					"/a":    "/a",
					"/a/":   "/a",
					"a/a":   "a/a",
					"/a/a/": "/a/a",
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
						kp.prefix = prefix
					})

					for k, p := range checks {
						func(k, p string) {
							It(fmt.Sprintf("%q should return %q", k, p), func() {
								Expect(kp.getPathForKey(k)).To(Equal(p))
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
						kp.prefix = prefix
					})

					for p, k := range checks {
						func(p, k string) {
							It(fmt.Sprintf("%q should return %q", p, k), func() {
								Expect(kp.getKeyForPath(p)).To(Equal(k))
							})
						}(p, k)
					}
				})
			}(t.prefix, t.checks)
		}
	})
})

var _ = Describe("intervalExplorer", func() {
	var ie *intervalExplorer

	BeforeEach(func() {
		ie = &intervalExplorer{}
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

	Describe("forEachMatchingKey", func() {
		var (
			ctx        context.Context
			ctrl       *gomock.Controller
			repo       *mockgit.MockRepository
			errs       *mockgit.MockErrors
			isNotFound bool
			tree       *mockgit.MockTree
		)

		BeforeEach(func() {
			ctx = context.Background()

			ctrl = gomock.NewController(GinkgoT())

			repo = mockgit.NewMockRepository(ctrl)

			errs = mockgit.NewMockErrors(ctrl)
			errs.EXPECT().IgnoreNotFound(gomock.Any()).DoAndReturn(func(err error) error {
				if isNotFound {
					return nil
				}

				return err
			})

			tree = mockgit.NewMockTree(ctrl)

			ie.repo = repo
			ie.errors = errs
			ie.tree = tree
		})

		type entry struct {
			name    string
			entries []entry
		}

		type check struct {
			interval  interval
			filterFns []intervalExplorerFilterFunc
			error     bool
			match     []string
		}

		type spec struct {
			keyPrefix string
			entries   []entry
			checks    []check
		}

		var repoRoot = entry{entries: []entry{
			{name: "1"},
			{name: "2"},
			{name: "3", entries: []entry{
				{name: "1", entries: []entry{
					{name: "1"},
					{name: "2"},
				}},
				{name: "2"},
				{name: "3"},
			}},
			{name: "4", entries: []entry{
				{name: "1"},
				{name: "2"},
				{name: "3", entries: []entry{
					{name: "1"},
					{name: "2"},
				}},
			}},
			{name: "5"},
		}}

		for _, notFound := range []bool{false, true} {
			func(notFound bool) {
				Describe(fmt.Sprintf("notFound=%t", notFound), func() {
					BeforeEach(func() {
						isNotFound = notFound
					})

					for _, s := range []spec{
						{
							entries: repoRoot.entries,
							checks: []check{
								{interval: &closedOpenInterval{start: key("")}, error: true},
								{interval: &closedOpenInterval{start: key("1")}, match: []string{"1"}},
								{interval: &closedOpenInterval{start: key("2/1")}, error: true},
								{interval: &closedOpenInterval{start: key("3")}, match: []string{"3"}},
								{interval: &closedOpenInterval{start: key("3/1")}, match: []string{"3/1"}},
								{interval: &closedOpenInterval{start: key("3/1/1")}, match: []string{"3/1/1"}},
								{interval: &closedOpenInterval{start: key("3/2")}, match: []string{"3/2"}},
								{interval: &closedOpenInterval{start: key("3/3/1")}, error: true},
								{interval: &closedOpenInterval{start: key("4/3")}, match: []string{"4/3"}},
								{interval: &closedOpenInterval{start: key("4/3/2")}, match: []string{"4/3/2"}},
								{interval: &closedOpenInterval{start: key("4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"4/3/2"}},
								{interval: &closedOpenInterval{start: key("4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}},
								{interval: &closedOpenInterval{start: key("4/5")}, error: true},
								{interval: &closedOpenInterval{start: key("6")}, error: true},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("2")}, match: []string{"1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("3")}, match: []string{"1", "2"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("3/1/2")}, match: []string{"1", "2", "3", "3/1", "3/1/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("3/2")}, match: []string{"1", "2", "3", "3/1", "3/1/1", "3/1/2"}},
								{interval: &closedOpenInterval{start: key("3"), end: key("4/3/2")}, match: []string{"3", "3/1", "3/1/1", "3/1/2", "3/2", "3/3", "4", "4/1", "4/2", "4/3", "4/3/1"}},
								{interval: &closedOpenInterval{start: key("3"), end: key("5")}, match: []string{"3", "3/1", "3/1/1", "3/1/2", "3/2", "3/3", "4", "4/1", "4/2", "4/3", "4/3/1", "4/3/2"}},
								{interval: &closedOpenInterval{start: key("4/3/1"), end: key("6")}, match: []string{"4/3/1", "4/3/2", "5"}},
								{interval: &closedOpenInterval{start: key("4/3/1"), end: key("\x00")}, match: []string{"4/3/1", "4/3/2", "5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, match: []string{"1", "2", "3", "3/1", "3/1/1", "3/1/2", "3/2", "3/3", "4", "4/1", "4/2", "4/3", "4/3/1", "4/3/2", "5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"1", "2", "3/1/1", "3/1/2", "3/2", "3/3", "4/1", "4/2", "4/3/1", "4/3/2", "5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}, match: []string{"3", "3/1", "4", "4/3"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeCommit)}},
								{interval: &closedOpenInterval{start: key("6"), end: key("7")}},
							},
						},
						{
							keyPrefix: "/",
							entries:   repoRoot.entries,
							checks: []check{
								{interval: &closedOpenInterval{start: key("/")}, error: true},
								{interval: &closedOpenInterval{start: key("/1")}, match: []string{"/1"}},
								{interval: &closedOpenInterval{start: key("/2/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/3")}, match: []string{"/3"}},
								{interval: &closedOpenInterval{start: key("/3/1")}, match: []string{"/3/1"}},
								{interval: &closedOpenInterval{start: key("/3/1/1")}, match: []string{"/3/1/1"}},
								{interval: &closedOpenInterval{start: key("/3/2")}, match: []string{"/3/2"}},
								{interval: &closedOpenInterval{start: key("/3/3/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/4/3")}, match: []string{"/4/3"}},
								{interval: &closedOpenInterval{start: key("/4/3/2")}, match: []string{"/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}},
								{interval: &closedOpenInterval{start: key("/4/5")}, error: true},
								{interval: &closedOpenInterval{start: key("/6")}, error: true},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/2")}, match: []string{"/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/3")}, match: []string{"/1", "/2"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/3/1/2")}, match: []string{"/1", "/2", "/3", "/3/1", "/3/1/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/3/2")}, match: []string{"/1", "/2", "/3", "/3/1", "/3/1/1", "/3/1/2"}},
								{interval: &closedOpenInterval{start: key("/3"), end: key("/4/3/2")}, match: []string{"/3", "/3/1", "/3/1/1", "/3/1/2", "/3/2", "/3/3", "/4", "/4/1", "/4/2", "/4/3", "/4/3/1"}},
								{interval: &closedOpenInterval{start: key("/3"), end: key("/5")}, match: []string{"/3", "/3/1", "/3/1/1", "/3/1/2", "/3/2", "/3/3", "/4", "/4/1", "/4/2", "/4/3", "/4/3/1", "/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/4/3/1"), end: key("/6")}, match: []string{"/4/3/1", "/4/3/2", "/5"}},
								{interval: &closedOpenInterval{start: key("/4/3/1"), end: key("\x00")}, match: []string{"/4/3/1", "/4/3/2", "/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, match: []string{"/1", "/2", "/3", "/3/1", "/3/1/1", "/3/1/2", "/3/2", "/3/3", "/4", "/4/1", "/4/2", "/4/3", "/4/3/1", "/4/3/2", "/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/1", "/2", "/3/1/1", "/3/1/2", "/3/2", "/3/3", "/4/1", "/4/2", "/4/3/1", "/4/3/2", "/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}, match: []string{"/3", "/3/1", "/4", "/4/3"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeCommit)}},
								{interval: &closedOpenInterval{start: key("/6"), end: key("/7")}},
							},
						},
						{
							keyPrefix: "a",
							entries:   repoRoot.entries,
							checks: []check{
								{interval: &closedOpenInterval{start: key("/")}, error: true},
								{interval: &closedOpenInterval{start: key("a")}, error: true},
								{interval: &closedOpenInterval{start: key("a/")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/1")}, error: true},
								{interval: &closedOpenInterval{start: key("a/1")}, match: []string{"a/1"}},
								{interval: &closedOpenInterval{start: key("a/2/1")}, error: true},
								{interval: &closedOpenInterval{start: key("a/3")}, match: []string{"a/3"}},
								{interval: &closedOpenInterval{start: key("a/3/1")}, match: []string{"a/3/1"}},
								{interval: &closedOpenInterval{start: key("a/3/1/1")}, match: []string{"a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("a/3/2")}, match: []string{"a/3/2"}},
								{interval: &closedOpenInterval{start: key("a/3/3/1")}, error: true},
								{interval: &closedOpenInterval{start: key("a/4/3")}, match: []string{"a/4/3"}},
								{interval: &closedOpenInterval{start: key("a/4/3/2")}, match: []string{"a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}},
								{interval: &closedOpenInterval{start: key("a/4/5")}, error: true},
								{interval: &closedOpenInterval{start: key("a/6")}, error: true},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/2")}, match: []string{"a/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/3")}, match: []string{"a/1", "a/2"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/3/1/2")}, match: []string{"a/1", "a/2", "a/3", "a/3/1", "a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/3/2")}, match: []string{"a/1", "a/2", "a/3", "a/3/1", "a/3/1/1", "a/3/1/2"}},
								{interval: &closedOpenInterval{start: key("a/3"), end: key("a/4/3/2")}, match: []string{"a/3", "a/3/1", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4", "a/4/1", "a/4/2", "a/4/3", "a/4/3/1"}},
								{interval: &closedOpenInterval{start: key("a/3"), end: key("a/5")}, match: []string{"a/3", "a/3/1", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4", "a/4/1", "a/4/2", "a/4/3", "a/4/3/1", "a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("a/4/3/1"), end: key("a/6")}, match: []string{"a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("a/4/3/1"), end: key("\x00")}, match: []string{"a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, match: []string{"a/1", "a/2", "a/3", "a/3/1", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4", "a/4/1", "a/4/2", "a/4/3", "a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"a/1", "a/2", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4/1", "a/4/2", "a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}, match: []string{"a/3", "a/3/1", "a/4", "a/4/3"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeCommit)}},
								{interval: &closedOpenInterval{start: key("a/6"), end: key("a/7")}},
							},
						},
						{
							keyPrefix: "a/",
							entries:   repoRoot.entries,
							checks: []check{
								{interval: &closedOpenInterval{start: key("/")}, error: true},
								{interval: &closedOpenInterval{start: key("a")}, error: true},
								{interval: &closedOpenInterval{start: key("a/")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/1")}, error: true},
								{interval: &closedOpenInterval{start: key("a/1")}, match: []string{"a/1"}},
								{interval: &closedOpenInterval{start: key("a/2/1")}, error: true},
								{interval: &closedOpenInterval{start: key("a/3")}, match: []string{"a/3"}},
								{interval: &closedOpenInterval{start: key("a/3/1")}, match: []string{"a/3/1"}},
								{interval: &closedOpenInterval{start: key("a/3/1/1")}, match: []string{"a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("a/3/2")}, match: []string{"a/3/2"}},
								{interval: &closedOpenInterval{start: key("a/3/3/1")}, error: true},
								{interval: &closedOpenInterval{start: key("a/4/3")}, match: []string{"a/4/3"}},
								{interval: &closedOpenInterval{start: key("a/4/3/2")}, match: []string{"a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}},
								{interval: &closedOpenInterval{start: key("a/4/5")}, error: true},
								{interval: &closedOpenInterval{start: key("a/6")}, error: true},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/2")}, match: []string{"a/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/3")}, match: []string{"a/1", "a/2"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/3/1/2")}, match: []string{"a/1", "a/2", "a/3", "a/3/1", "a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("a/3/2")}, match: []string{"a/1", "a/2", "a/3", "a/3/1", "a/3/1/1", "a/3/1/2"}},
								{interval: &closedOpenInterval{start: key("a/3"), end: key("a/4/3/2")}, match: []string{"a/3", "a/3/1", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4", "a/4/1", "a/4/2", "a/4/3", "a/4/3/1"}},
								{interval: &closedOpenInterval{start: key("a/3"), end: key("a/5")}, match: []string{"a/3", "a/3/1", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4", "a/4/1", "a/4/2", "a/4/3", "a/4/3/1", "a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("a/4/3/1"), end: key("a/6")}, match: []string{"a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("a/4/3/1"), end: key("\x00")}, match: []string{"a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, match: []string{"a/1", "a/2", "a/3", "a/3/1", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4", "a/4/1", "a/4/2", "a/4/3", "a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"a/1", "a/2", "a/3/1/1", "a/3/1/2", "a/3/2", "a/3/3", "a/4/1", "a/4/2", "a/4/3/1", "a/4/3/2", "a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}, match: []string{"a/3", "a/3/1", "a/4", "a/4/3"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeCommit)}},
								{interval: &closedOpenInterval{start: key("a/6"), end: key("a/7")}},
							},
						},
						{
							keyPrefix: "/a",
							entries:   repoRoot.entries,
							checks: []check{
								{interval: &closedOpenInterval{start: key("/")}, error: true},
								{interval: &closedOpenInterval{start: key("a")}, error: true},
								{interval: &closedOpenInterval{start: key("a/")}, error: true},
								{interval: &closedOpenInterval{start: key("a/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/1")}, match: []string{"/a/1"}},
								{interval: &closedOpenInterval{start: key("/a/2/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/3")}, match: []string{"/a/3"}},
								{interval: &closedOpenInterval{start: key("/a/3/1")}, match: []string{"/a/3/1"}},
								{interval: &closedOpenInterval{start: key("/a/3/1/1")}, match: []string{"/a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("/a/3/2")}, match: []string{"/a/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/3/3/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/4/3")}, match: []string{"/a/4/3"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/2")}, match: []string{"/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}},
								{interval: &closedOpenInterval{start: key("/a/4/5")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/6")}, error: true},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/2")}, match: []string{"/a/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/3")}, match: []string{"/a/1", "/a/2"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/3/1/2")}, match: []string{"/a/1", "/a/2", "/a/3", "/a/3/1", "/a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/3/2")}, match: []string{"/a/1", "/a/2", "/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2"}},
								{interval: &closedOpenInterval{start: key("/a/3"), end: key("/a/4/3/2")}, match: []string{"/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4", "/a/4/1", "/a/4/2", "/a/4/3", "/a/4/3/1"}},
								{interval: &closedOpenInterval{start: key("/a/3"), end: key("/a/5")}, match: []string{"/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4", "/a/4/1", "/a/4/2", "/a/4/3", "/a/4/3/1", "/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/1"), end: key("/a/6")}, match: []string{"/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/1"), end: key("\x00")}, match: []string{"/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, match: []string{"/a/1", "/a/2", "/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4", "/a/4/1", "/a/4/2", "/a/4/3", "/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/a/1", "/a/2", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4/1", "/a/4/2", "/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}, match: []string{"/a/3", "/a/3/1", "/a/4", "/a/4/3"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeCommit)}},
								{interval: &closedOpenInterval{start: key("/a/6"), end: key("/a/7")}},
							},
						},
						{
							keyPrefix: "/a/",
							entries:   repoRoot.entries,
							checks: []check{
								{interval: &closedOpenInterval{start: key("/")}, error: true},
								{interval: &closedOpenInterval{start: key("a")}, error: true},
								{interval: &closedOpenInterval{start: key("a/")}, error: true},
								{interval: &closedOpenInterval{start: key("a/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/1")}, match: []string{"/a/1"}},
								{interval: &closedOpenInterval{start: key("/a/2/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/3")}, match: []string{"/a/3"}},
								{interval: &closedOpenInterval{start: key("/a/3/1")}, match: []string{"/a/3/1"}},
								{interval: &closedOpenInterval{start: key("/a/3/1/1")}, match: []string{"/a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("/a/3/2")}, match: []string{"/a/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/3/3/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/4/3")}, match: []string{"/a/4/3"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/2")}, match: []string{"/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}},
								{interval: &closedOpenInterval{start: key("/a/4/5")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/6")}, error: true},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/2")}, match: []string{"/a/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/3")}, match: []string{"/a/1", "/a/2"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/3/1/2")}, match: []string{"/a/1", "/a/2", "/a/3", "/a/3/1", "/a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/3/2")}, match: []string{"/a/1", "/a/2", "/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2"}},
								{interval: &closedOpenInterval{start: key("/a/3"), end: key("/a/4/3/2")}, match: []string{"/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4", "/a/4/1", "/a/4/2", "/a/4/3", "/a/4/3/1"}},
								{interval: &closedOpenInterval{start: key("/a/3"), end: key("/a/5")}, match: []string{"/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4", "/a/4/1", "/a/4/2", "/a/4/3", "/a/4/3/1", "/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/1"), end: key("/a/6")}, match: []string{"/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("/a/4/3/1"), end: key("\x00")}, match: []string{"/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, match: []string{"/a/1", "/a/2", "/a/3", "/a/3/1", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4", "/a/4/1", "/a/4/2", "/a/4/3", "/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/a/1", "/a/2", "/a/3/1/1", "/a/3/1/2", "/a/3/2", "/a/3/3", "/a/4/1", "/a/4/2", "/a/4/3/1", "/a/4/3/2", "/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}, match: []string{"/a/3", "/a/3/1", "/a/4", "/a/4/3"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeCommit)}},
								{interval: &closedOpenInterval{start: key("/a/6"), end: key("/a/7")}},
							},
						},
						{
							keyPrefix: "/a/a/a",
							entries:   repoRoot.entries,
							checks: []check{
								{interval: &closedOpenInterval{start: key("/")}, error: true},
								{interval: &closedOpenInterval{start: key("a")}, error: true},
								{interval: &closedOpenInterval{start: key("a/")}, error: true},
								{interval: &closedOpenInterval{start: key("a/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/a/a/1")}, match: []string{"/a/a/a/1"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/2/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/a/a/3")}, match: []string{"/a/a/a/3"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/3/1")}, match: []string{"/a/a/a/3/1"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/3/1/1")}, match: []string{"/a/a/a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/3/2")}, match: []string{"/a/a/a/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/3/3/1")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/a/a/4/3")}, match: []string{"/a/a/a/4/3"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/4/3/2")}, match: []string{"/a/a/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/a/a/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/4/3/2")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}},
								{interval: &closedOpenInterval{start: key("/a/a/a/4/5")}, error: true},
								{interval: &closedOpenInterval{start: key("/a/a/a/6")}, error: true},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/a/a/2")}, match: []string{"/a/a/a/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/a/a/3")}, match: []string{"/a/a/a/1", "/a/a/a/2"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/a/a/3/1/2")}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("/a/a/a/3/2")}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/3"), end: key("/a/a/a/4/3/2")}, match: []string{"/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3", "/a/a/a/4", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3", "/a/a/a/4/3/1"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/3"), end: key("/a/a/a/5")}, match: []string{"/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3", "/a/a/a/4", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3", "/a/a/a/4/3/1", "/a/a/a/4/3/2"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/4/3/1"), end: key("/a/a/a/6")}, match: []string{"/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/4/3/1"), end: key("\x00")}, match: []string{"/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3", "/a/a/a/4", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3", "/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("/a"), end: key("/b")}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3", "/a/a/a/4", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3", "/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("/a"), end: key("/a/b")}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3", "/a/a/a/4", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3", "/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("/a"), end: key("/a/a/b")}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3", "/a/a/a/4", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3", "/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("/a"), end: key("/a/a/a/3")}, match: []string{"/a/a/a/1", "/a/a/a/2"}},
								{interval: &closedOpenInterval{start: key("/a"), end: key("/a/a/a/4")}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3", "/a/a/a/3/1", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3"}},
								{interval: &closedOpenInterval{start: key("/a/a/a/4"), end: key("/b")}, match: []string{"/a/a/a/4", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3", "/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeBlob)}, match: []string{"/a/a/a/1", "/a/a/a/2", "/a/a/a/3/1/1", "/a/a/a/3/1/2", "/a/a/a/3/2", "/a/a/a/3/3", "/a/a/a/4/1", "/a/a/a/4/2", "/a/a/a/4/3/1", "/a/a/a/4/3/2", "/a/a/a/5"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeTree)}, match: []string{"/a/a/a/3", "/a/a/a/3/1", "/a/a/a/4", "/a/a/a/4/3"}},
								{interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")}, filterFns: []intervalExplorerFilterFunc{newObjectTypeFilter(git.ObjectTypeCommit)}},
								{interval: &closedOpenInterval{start: key("/a/a/a/6"), end: key("/a/a/a/7")}},
							},
						},
					} {
						func(s spec) {
							Describe(fmt.Sprintf("with keyPrefix %q and entries %v", s.keyPrefix, s.entries), func() {
								BeforeEach(func() {
									var (
										tw           = mockgit.NewMockTreeWalker(ctrl)
										entryMap     = make(map[string]git.TreeEntry, len(s.entries))
										fillEntryMap func(string, []entry)
									)

									ie.keyPrefix.prefix = s.keyPrefix

									fillEntryMap = func(parentPath string, entries []entry) {
										for _, e := range entries {
											var (
												p  = joinSafe(parentPath, e.name)
												te = mockgit.NewMockTreeEntry(ctrl)
											)

											te.EXPECT().EntryName().Return(e.name).Times(2)

											if len(e.entries) > 0 {
												te.EXPECT().EntryType().Return(git.ObjectTypeTree)
											} else {
												te.EXPECT().EntryType().Return(git.ObjectTypeBlob)
											}

											entryMap[ie.getKeyForPath(p)] = te

											fillEntryMap(p, e.entries)
										}
									}

									fillEntryMap("", s.entries)

									tree.EXPECT().GetEntryByPath(gomock.Any(), gomock.Any()).DoAndReturn(
										func(_ context.Context, p string) (git.TreeEntry, error) {
											var k = ie.getKeyForPath(p)

											if te, ok := entryMap[k]; ok {
												return te, nil
											}

											return nil, errors.New("error")
										},
									)

									repo.EXPECT().TreeWalker().Return(tw)
									tw.EXPECT().Close().Return(nil)
									tw.EXPECT().ForEachTreeEntry(gomock.Any(), tree, gomock.Any()).DoAndReturn(
										func(ctx context.Context, t git.Tree, fn git.TreeWalkerReceiverFunc) error {
											var callForEntries func(parentPath string, entries []entry) error

											callForEntries = func(parentPath string, entries []entry) error {
												for _, e := range entries {
													var (
														p = joinSafe(parentPath, e.name)
														k = ie.getKeyForPath(p)
													)

													if done, skip, err := fn(ctx, parentPath, entryMap[k]); done || err != nil {
														return err
													} else if skip {
														continue
													}

													if err := callForEntries(p, e.entries); err != nil {
														return err
													}
												}

												return nil
											}

											return callForEntries("", s.entries)
										},
									)
								})

								for _, c := range s.checks {
									func(c check) {
										Describe(fmt.Sprintf("interval %s with filterFns %v", c.interval, c.filterFns), func() {
											BeforeEach(func() {
												ie.interval = c.interval
											})

											It(fmt.Sprintf("should match %v", c.match), func() {
												var (
													entries []string
													err     = ie.forEachMatchingKey(
														ctx,
														func(_ context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
															Expect(path.Base(k)).To(Equal(te.EntryName()))
															entries = append(entries, k)
															return
														},
														c.filterFns...,
													)
												)

												Expect(entries).To(Equal(c.match))

												Expect(err).To(func() types.GomegaMatcher {
													if c.error && !isNotFound {
														return HaveOccurred()
													}

													return Succeed()
												}())
											})
										})
									}(c)
								}
							})
						}(s)
					}
				})
			}(notFound)
		}
	})
})
