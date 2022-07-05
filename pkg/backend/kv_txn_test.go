package backend

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	. "github.com/onsi/gomega/gstruct"

	impl "github.com/libgit2/git2go/v31"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	mockgit "github.com/trishanku/gitcd/pkg/mocks/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
)

var _ = Describe("int64Cmp", func() {
	for _, s := range []struct {
		a, b  int64
		match types.GomegaMatcher
	}{
		{a: -10, b: 0, match: Equal(cmpResultLess)},
		{a: -2, b: -1, match: Equal(cmpResultLess)},
		{a: -1, b: -1, match: Equal(cmpResultEqual)},
		{a: -10, b: -11, match: Equal(cmpResultMore)},
		{a: 0, b: 0, match: Equal(cmpResultEqual)},
		{a: 0, b: 1, match: Equal(cmpResultLess)},
		{a: 8, b: 10, match: Equal(cmpResultLess)},
		{a: 9, b: 10, match: Equal(cmpResultLess)},
		{a: 10, b: 10, match: Equal(cmpResultEqual)},
		{a: 11, b: 10, match: Equal(cmpResultMore)},
		{a: 12, b: 10, match: Equal(cmpResultMore)},
	} {
		func(a, b int64, match types.GomegaMatcher) {
			It(fmt.Sprintf("compare %d with %d", a, b), func() {
				Expect(int64Cmp(a).Cmp(b)).To(match)
			})
		}(s.a, s.b, s.match)
	}
})

var _ = Describe("getCompareTargetInt64", func() {
	for _, s := range []struct {
		c                    *etcdserverpb.Compare
		matchErr, matchInt64 types.GomegaMatcher
	}{
		{matchErr: Succeed(), matchInt64: BeZero()},
		{c: &etcdserverpb.Compare{}, matchErr: Succeed(), matchInt64: BeZero()},
		{c: &etcdserverpb.Compare{TargetUnion: &etcdserverpb.Compare_Lease{Lease: 2}}, matchErr: Succeed(), matchInt64: BeZero()},
		{c: &etcdserverpb.Compare{TargetUnion: &etcdserverpb.Compare_Version{Version: 10}}, matchErr: Succeed(), matchInt64: Equal(int64(10))},
		{
			c:          &etcdserverpb.Compare{Target: etcdserverpb.Compare_CompareTarget(-1)},
			matchErr:   MatchError(ContainSubstring("unsupported compare target")),
			matchInt64: BeZero(),
		},
		{
			c:          &etcdserverpb.Compare{Target: etcdserverpb.Compare_VALUE},
			matchErr:   MatchError(ContainSubstring("unsupported compare target")),
			matchInt64: BeZero(),
		},
		{
			c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_VALUE, TargetUnion: &etcdserverpb.Compare_Value{
				Value: []byte("1"),
			}},
			matchErr:   MatchError(ContainSubstring("unsupported compare target")),
			matchInt64: BeZero(),
		},
		{
			c:          &etcdserverpb.Compare{Target: etcdserverpb.Compare_CompareTarget(5)},
			matchErr:   MatchError(ContainSubstring("unsupported compare target")),
			matchInt64: BeZero(),
		},
		{c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_CREATE}, matchErr: Succeed(), matchInt64: BeZero()},
		{
			c:          &etcdserverpb.Compare{Target: etcdserverpb.Compare_CREATE, TargetUnion: &etcdserverpb.Compare_CreateRevision{}},
			matchErr:   Succeed(),
			matchInt64: BeZero(),
		},
		{
			c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_CREATE, TargetUnion: &etcdserverpb.Compare_CreateRevision{
				CreateRevision: 2,
			}},
			matchErr:   Succeed(),
			matchInt64: Equal(int64(2)),
		},
		{c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_LEASE}, matchErr: Succeed(), matchInt64: BeZero()},
		{
			c:          &etcdserverpb.Compare{Target: etcdserverpb.Compare_LEASE, TargetUnion: &etcdserverpb.Compare_Lease{}},
			matchErr:   Succeed(),
			matchInt64: BeZero(),
		},
		{
			c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_LEASE, TargetUnion: &etcdserverpb.Compare_Lease{
				Lease: 4,
			}},
			matchErr:   Succeed(),
			matchInt64: Equal(int64(4)),
		},
		{c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_MOD}, matchErr: Succeed(), matchInt64: BeZero()},
		{
			c:          &etcdserverpb.Compare{Target: etcdserverpb.Compare_MOD, TargetUnion: &etcdserverpb.Compare_ModRevision{}},
			matchErr:   Succeed(),
			matchInt64: BeZero(),
		},
		{
			c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_MOD, TargetUnion: &etcdserverpb.Compare_ModRevision{
				ModRevision: 6,
			}},
			matchErr:   Succeed(),
			matchInt64: Equal(int64(6)),
		},
		{c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_VERSION}, matchErr: Succeed(), matchInt64: BeZero()},
		{
			c:          &etcdserverpb.Compare{Target: etcdserverpb.Compare_VERSION, TargetUnion: &etcdserverpb.Compare_Version{}},
			matchErr:   Succeed(),
			matchInt64: BeZero(),
		},
		{
			c: &etcdserverpb.Compare{Target: etcdserverpb.Compare_VERSION, TargetUnion: &etcdserverpb.Compare_Version{
				Version: 8,
			}},
			matchErr:   Succeed(),
			matchInt64: Equal(int64(8)),
		},
	} {
		func(c *etcdserverpb.Compare, matchErr, matchInt64 types.GomegaMatcher) {
			It(fmt.Sprintf("compare=%v %s", c, ItSpecForMatchError(matchErr)), func() {
				var i, err = getCompareTargetInt64(c)
				Expect(err).To(matchErr)
				Expect(i).To(matchInt64)
			})
		}(s.c, s.matchErr, s.matchInt64)
	}
})

var _ = Describe("checkCompareResult", func() {
	for _, s := range []struct {
		c                    *etcdserverpb.Compare
		r                    cmpResult
		matchErr, matchCheck types.GomegaMatcher
	}{
		{matchErr: Succeed(), matchCheck: BeTrue()},
		{r: cmpResultEqual, matchErr: Succeed(), matchCheck: BeTrue()},
		{r: cmpResultLess, matchErr: Succeed(), matchCheck: BeFalse()},
		{r: cmpResult(-2), matchErr: Succeed(), matchCheck: BeFalse()},
		{r: cmpResultMore, matchErr: Succeed(), matchCheck: BeFalse()},
		{r: cmpResult(2), matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{}, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{}, r: cmpResultEqual, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{}, r: cmpResultLess, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{}, r: cmpResult(-2), matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{}, r: cmpResultMore, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{}, r: cmpResult(2), matchErr: Succeed(), matchCheck: BeFalse()},
		{
			c:          &etcdserverpb.Compare{Result: etcdserverpb.Compare_CompareResult(-1)},
			matchErr:   MatchError(ContainSubstring("unsupported compare result")),
			matchCheck: BeFalse(),
		},
		{
			c:          &etcdserverpb.Compare{Result: etcdserverpb.Compare_CompareResult(4)},
			matchErr:   MatchError(ContainSubstring("unsupported compare result")),
			matchCheck: BeFalse(),
		},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_EQUAL}, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_EQUAL}, r: cmpResultEqual, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_EQUAL}, r: cmpResultLess, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_EQUAL}, r: cmpResult(-2), matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_EQUAL}, r: cmpResultMore, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_EQUAL}, r: cmpResult(2), matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_NOT_EQUAL}, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_NOT_EQUAL}, r: cmpResultEqual, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_NOT_EQUAL}, r: cmpResultLess, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_NOT_EQUAL}, r: cmpResult(-2), matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_NOT_EQUAL}, r: cmpResultMore, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_NOT_EQUAL}, r: cmpResult(2), matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_GREATER}, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_GREATER}, r: cmpResultEqual, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_GREATER}, r: cmpResultLess, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_GREATER}, r: cmpResult(-2), matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_GREATER}, r: cmpResultMore, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_GREATER}, r: cmpResult(2), matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_LESS}, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_LESS}, r: cmpResultEqual, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_LESS}, r: cmpResultLess, matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_LESS}, r: cmpResult(-2), matchErr: Succeed(), matchCheck: BeTrue()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_LESS}, r: cmpResultMore, matchErr: Succeed(), matchCheck: BeFalse()},
		{c: &etcdserverpb.Compare{Result: etcdserverpb.Compare_LESS}, r: cmpResult(2), matchErr: Succeed(), matchCheck: BeFalse()},
	} {
		func(c *etcdserverpb.Compare, r cmpResult, matchErr, matchCheck types.GomegaMatcher) {
			It(fmt.Sprintf("compare=%v result=%d %s", c, r, ItSpecForMatchError(matchErr)), func() {
				var check, err = checkCompareResult(c, r)
				Expect(err).To(matchErr)
				Expect(check).To(matchCheck)
			})
		}(s.c, s.r, s.matchErr, s.matchCheck)
	}
})

var _ = Describe("copyResponseHeaderFrom", func() {
	for _, h := range []*etcdserverpb.ResponseHeader{
		{},
		{ClusterId: 1, MemberId: 2},
		{ClusterId: 3, MemberId: 1, Revision: 10, RaftTerm: 11},
	} {
		func(h *etcdserverpb.ResponseHeader) {
			It(fmt.Sprintf("%v", h), func() {
				Expect(copyResponseHeaderFrom(h)).To(Equal(h))
				Expect(copyResponseHeaderFrom(h)).ToNot(BeIdenticalTo(h))
			})
		}(h)
	}
})

var _ = Describe("backend", func() {
	var (
		b   *backend
		ctx context.Context
		dir string
	)

	BeforeEach(func() {
		var gitImpl = git2go.New()

		b = &backend{
			log: getTestLogger(),
			commitConfig: commitConfig{
				committerName:  "trishanku",
				committerEmail: "trishanku@heaven.com",
			},
		}

		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { b.repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(b.repo).ToNot(BeNil())

		b.errors = gitImpl.Errors()
	})

	AfterEach(func() {
		if b.repo != nil {
			Expect(b.repo.Close()).To(Succeed())
		}

		if len(dir) > 0 {
			Expect(os.RemoveAll(dir))
		}
	})

	Describe("readInt64", func() {
		type treeEntryFunc func() git.TreeEntry

		type check struct {
			spec                 string
			ctxFn                ContextFunc
			treeEntryFn          treeEntryFunc
			path                 string
			matchErr, matchInt64 types.GomegaMatcher
		}

		var (
			mockTreeEntry = func(id git.ObjectID, typ git.ObjectType) treeEntryFunc {
				return func() git.TreeEntry {
					var te = mockgit.NewMockTreeEntry(gomock.NewController(GinkgoT()))

					te.EXPECT().EntryID().Return(id)
					te.EXPECT().EntryType().Return(typ).MaxTimes(2)
					return te
				}
			}

			treeEntryFrom = func(td *TreeDef) treeEntryFunc {
				return func() git.TreeEntry {
					var id git.ObjectID

					Expect(func() (err error) { id, err = CreateTreeFromDef(ctx, b.repo, td); return }()).To(Succeed())

					return mockTreeEntry(id, git.ObjectTypeTree)()
				}
			}
		)

		for _, s := range []check{
			{
				spec:        "expired context, non-tree",
				ctxFn:       CanceledContext,
				treeEntryFn: mockTreeEntry(git.ObjectID{}, git.ObjectTypeBlob),
				matchErr:    MatchError(ContainSubstring("unsupported ObjectType")),
				matchInt64:  BeZero(),
			},
			{
				spec:        "expired context, non-existent tree",
				ctxFn:       CanceledContext,
				treeEntryFn: mockTreeEntry(git.ObjectID{}, git.ObjectTypeTree),
				matchErr:    MatchError("context canceled"),
				matchInt64:  BeZero(),
			},
			{
				spec:        "expired context, existing tree",
				ctxFn:       CanceledContext,
				treeEntryFn: treeEntryFrom(&TreeDef{}),
				matchErr:    MatchError("context canceled"),
				matchInt64:  BeZero(),
			},
			{
				spec:        "non-existent tree",
				treeEntryFn: mockTreeEntry(git.ObjectID{}, git.ObjectTypeTree),
				matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchInt64:  BeZero(),
			},
			{
				spec:        "non-existent non-tree",
				treeEntryFn: mockTreeEntry(git.ObjectID{}, git.ObjectTypeBlob),
				matchErr: And(
					MatchError(ContainSubstring("unsupported ObjectType")),
					Not(PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))),
				),
				matchInt64: BeZero(),
			},
			{
				spec: "existent non-tree",
				treeEntryFn: func() treeEntryFunc {
					return func() git.TreeEntry {
						var id git.ObjectID

						Expect(func() (err error) { id, err = CreateBlob(ctx, b.repo, []byte("1")); return }()).To(Succeed())

						return mockTreeEntry(id, git.ObjectTypeBlob)()
					}
				}(),
				matchErr:   MatchError(ContainSubstring("unsupported ObjectType")),
				matchInt64: BeZero(),
			},
			{
				spec:        "tree without the path",
				treeEntryFn: treeEntryFrom(&TreeDef{}),
				matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchInt64:  BeZero(),
			},
			{
				spec:        "tree with non-blob path",
				treeEntryFn: treeEntryFrom(&TreeDef{Subtrees: map[string]TreeDef{"p": {}}}),
				path:        "p",
				matchErr: And(
					MatchError(ContainSubstring("unsupported ObjectType")),
					Not(PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))),
				),
				matchInt64: BeZero(),
			},
			{
				spec:        "empty value",
				treeEntryFn: treeEntryFrom(&TreeDef{Blobs: map[string][]byte{"p": {}}}),
				path:        "p",
				matchErr: And(
					MatchError(ContainSubstring("ParseInt")),
					Not(PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))),
				),
				matchInt64: BeZero(),
			},
			{
				spec:        "non-integer value",
				treeEntryFn: treeEntryFrom(&TreeDef{Blobs: map[string][]byte{"p": []byte("a")}}),
				path:        "p",
				matchErr: And(
					MatchError(ContainSubstring("ParseInt")),
					Not(PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))),
				),
				matchInt64: BeZero(),
			},
			{
				spec:        "integer value",
				treeEntryFn: treeEntryFrom(&TreeDef{Blobs: map[string][]byte{"p": []byte("1")}}),
				path:        "p",
				matchErr:    Succeed(),
				matchInt64:  Equal(int64(1)),
			},
			{
				spec:        "integer value",
				treeEntryFn: treeEntryFrom(&TreeDef{Blobs: map[string][]byte{"p": []byte(strconv.FormatInt(math.MinInt64, 10))}}),
				path:        "p",
				matchErr:    Succeed(),
				matchInt64:  Equal(int64(math.MinInt64)),
			},
			{
				spec:        "integer value",
				treeEntryFn: treeEntryFrom(&TreeDef{Blobs: map[string][]byte{"p": []byte(strconv.FormatInt(math.MaxInt64, 10))}}),
				path:        "p",
				matchErr:    Succeed(),
				matchInt64:  Equal(int64(math.MaxInt64)),
			},
		} {
			func(s check) {
				Describe(fmt.Sprintf("%s path=%q", s.spec, s.path), func() {
					var te git.TreeEntry

					BeforeEach(func() {
						te = s.treeEntryFn()

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}
					})

					It(ItSpecForMatchError(s.matchErr), func() {
						var i, err = b.readInt64(ctx, te, s.path)
						Expect(err).To(s.matchErr)
						Expect(i).To(s.matchInt64)
					})
				})
			}(s)
		}
	})

	Describe("checkTxnCompare", func() {
		type check struct {
			spec                   string
			ctxFn                  ContextFunc
			metaRoot, dataRoot     *TreeDef
			compares               []*etcdserverpb.Compare
			matchErr, matchCompare types.GomegaMatcher
		}

		for _, prefix := range []string{"", "/", "registry", "/registry"} {
			func(prefix string) {
				Describe(fmt.Sprintf("prefix=%q", prefix), func() {
					BeforeEach(func() {
						b.keyPrefix.prefix = prefix
					})

					var checks = []check{
						{
							spec:         "expired context",
							ctxFn:        CanceledContext,
							matchErr:     Succeed(),
							matchCompare: BeTrue(),
						},
						{
							spec:         "expired context",
							ctxFn:        CanceledContext,
							compares:     []*etcdserverpb.Compare{},
							matchErr:     Succeed(),
							matchCompare: BeTrue(),
						},
						{
							spec:         "expired context",
							ctxFn:        CanceledContext,
							compares:     []*etcdserverpb.Compare{nil, nil},
							matchErr:     Succeed(),
							matchCompare: BeFalse(),
						},
						{
							spec:         "expired context",
							ctxFn:        CanceledContext,
							metaRoot:     &TreeDef{},
							compares:     []*etcdserverpb.Compare{nil, nil},
							matchErr:     MatchError("context canceled"),
							matchCompare: BeFalse(),
						},
						{
							spec:         "expired context",
							ctxFn:        CanceledContext,
							metaRoot:     &TreeDef{},
							compares:     []*etcdserverpb.Compare{{Target: etcdserverpb.Compare_LEASE, TargetUnion: &etcdserverpb.Compare_Lease{Lease: 2}}},
							matchErr:     MatchError("context canceled"),
							matchCompare: BeFalse(),
						},
						{
							spec:  "expired context",
							ctxFn: CanceledContext,
							compares: []*etcdserverpb.Compare{{
								Target:      etcdserverpb.Compare_VALUE,
								TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
							}},
							matchErr:     Succeed(),
							matchCompare: BeFalse(),
						},
						{
							spec:     "expired context",
							ctxFn:    CanceledContext,
							dataRoot: &TreeDef{},
							compares: []*etcdserverpb.Compare{{
								Target:      etcdserverpb.Compare_VALUE,
								TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
							}},
							matchErr:     MatchError("context canceled"),
							matchCompare: BeFalse(),
						},
						{
							spec:     "expired context",
							ctxFn:    CanceledContext,
							metaRoot: &TreeDef{},
							compares: []*etcdserverpb.Compare{
								nil,
								{Target: etcdserverpb.Compare_VALUE, TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")}},
							},
							matchErr:     MatchError("context canceled"),
							matchCompare: BeFalse(),
						},
						{
							spec:     "expired context",
							ctxFn:    CanceledContext,
							dataRoot: &TreeDef{},
							compares: []*etcdserverpb.Compare{
								{Target: etcdserverpb.Compare_VALUE, TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")}},
								nil,
							},
							matchErr:     MatchError("context canceled"),
							matchCompare: BeFalse(),
						},
					}

					{
						var spec = "value not found"

						checks = append(
							checks,
							check{
								spec:     spec,
								dataRoot: &TreeDef{},
								compares: []*etcdserverpb.Compare{{
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec:     spec,
								dataRoot: &TreeDef{},
								compares: []*etcdserverpb.Compare{{
									Key:         []byte("z"),
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec:     spec,
								dataRoot: &TreeDef{Blobs: map[string][]byte{"a": []byte("2")}},
								compares: []*etcdserverpb.Compare{{
									Key:         []byte("z"),
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
						)
					}

					{
						var (
							spec     = "value does not match"
							interval = &closedOpenInterval{start: []byte(path.Join(prefix, "a")), end: []byte(path.Join(prefix, "c"))}
							td       = &TreeDef{Blobs: map[string][]byte{"a": []byte("1")}}
						)

						checks = append(
							checks,
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_CompareResult(-1),
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     MatchError(ContainSubstring("unsupported compare result")),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_CompareResult(4),
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     MatchError(ContainSubstring("unsupported compare result")),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_EQUAL,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_NOT_EQUAL,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_GREATER,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeFalse(),
							},
							check{
								spec: spec,
								metaRoot: &TreeDef{
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{etcdserverpb.Compare_LEASE.String(): []byte("a")}},
									},
								},
								dataRoot: td,
								compares: []*etcdserverpb.Compare{
									{
										Key:         interval.start,
										RangeEnd:    interval.end,
										Result:      etcdserverpb.Compare_LESS,
										Target:      etcdserverpb.Compare_VALUE,
										TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
									},
									{
										Key:         interval.start,
										RangeEnd:    interval.end,
										Result:      etcdserverpb.Compare_EQUAL,
										Target:      etcdserverpb.Compare_LEASE,
										TargetUnion: &etcdserverpb.Compare_Lease{Lease: 1},
									},
								},
								matchErr:     Succeed(),
								matchCompare: BeFalse(),
							},
							check{
								spec: spec,
								dataRoot: &TreeDef{
									Blobs: map[string][]byte{"a": []byte("1")},
									Subtrees: map[string]TreeDef{
										"b": {
											Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("1")},
											Subtrees: map[string]TreeDef{
												"3": {
													Blobs: map[string][]byte{"4": []byte("1")},
													Subtrees: map[string]TreeDef{
														"5": {Blobs: map[string][]byte{"6": []byte("1"), "7": []byte("2")}},
													},
												},
											},
										},
									},
								},
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeFalse(),
							},
						)
					}

					{
						var (
							spec     = "value matches"
							interval = &closedOpenInterval{start: []byte(path.Join(prefix, "a")), end: []byte(path.Join(prefix, "c"))}
							td       = &TreeDef{Blobs: map[string][]byte{"a": []byte("3")}}
						)

						checks = append(
							checks,
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_CompareResult(-1),
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("3")},
								}},
								matchErr:     MatchError(ContainSubstring("unsupported compare result")),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_CompareResult(4),
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("3")},
								}},
								matchErr:     MatchError(ContainSubstring("unsupported compare result")),
								matchCompare: BeFalse(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("3")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_EQUAL,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("3")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_NOT_EQUAL,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_GREATER,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec:     spec,
								dataRoot: td,
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("4")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec: spec,
								dataRoot: &TreeDef{
									Blobs: map[string][]byte{"a": []byte("1")},
									Subtrees: map[string]TreeDef{
										"b": {
											Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("1")},
											Subtrees: map[string]TreeDef{
												"3": {
													Blobs: map[string][]byte{"4": []byte("1")},
													Subtrees: map[string]TreeDef{
														"5": {Blobs: map[string][]byte{"6": []byte("1"), "7": []byte("1")}},
													},
												},
											},
										},
									},
								},
								compares: []*etcdserverpb.Compare{{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("2")},
								}},
								matchErr:     Succeed(),
								matchCompare: BeTrue(),
							},
							check{
								spec: spec,
								metaRoot: &TreeDef{
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{etcdserverpb.Compare_LEASE.String(): []byte("a")}},
									},
								},
								dataRoot: &TreeDef{
									Blobs: map[string][]byte{"a": []byte("1")},
									Subtrees: map[string]TreeDef{
										"b": {
											Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("1")},
											Subtrees: map[string]TreeDef{
												"3": {
													Blobs: map[string][]byte{"4": []byte("1")},
													Subtrees: map[string]TreeDef{
														"5": {Blobs: map[string][]byte{"6": []byte("1"), "7": []byte("1")}},
													},
												},
											},
										},
									},
								},
								compares: []*etcdserverpb.Compare{
									{
										Key:         interval.start,
										RangeEnd:    interval.end,
										Result:      etcdserverpb.Compare_LESS,
										Target:      etcdserverpb.Compare_VALUE,
										TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("4")},
									},
									{
										Key:         interval.start,
										RangeEnd:    interval.end,
										Result:      etcdserverpb.Compare_EQUAL,
										Target:      etcdserverpb.Compare_LEASE,
										TargetUnion: &etcdserverpb.Compare_Lease{Lease: 1},
									},
								},
								matchErr:     MatchError(ContainSubstring("ParseInt")),
								matchCompare: BeFalse(),
							},
						)
					}

					for _, s := range []struct {
						spec, targetName string
						fillTargetFn     func(*etcdserverpb.Compare, int64) *etcdserverpb.Compare
					}{
						{
							spec:       "default",
							targetName: etcdserverpb.Compare_VERSION.String(),
							fillTargetFn: func(c *etcdserverpb.Compare, i int64) *etcdserverpb.Compare {
								c.TargetUnion = &etcdserverpb.Compare_Version{Version: i}
								return c
							},
						},
						{
							targetName: etcdserverpb.Compare_VERSION.String(),
							fillTargetFn: func(c *etcdserverpb.Compare, i int64) *etcdserverpb.Compare {
								c.Target = etcdserverpb.Compare_VERSION
								c.TargetUnion = &etcdserverpb.Compare_Version{Version: i}
								return c
							},
						},
						{
							targetName: etcdserverpb.Compare_CREATE.String(),
							fillTargetFn: func(c *etcdserverpb.Compare, i int64) *etcdserverpb.Compare {
								c.Target = etcdserverpb.Compare_CREATE
								c.TargetUnion = &etcdserverpb.Compare_CreateRevision{CreateRevision: i}
								return c
							},
						},
						{
							targetName: etcdserverpb.Compare_LEASE.String(),
							fillTargetFn: func(c *etcdserverpb.Compare, i int64) *etcdserverpb.Compare {
								c.Target = etcdserverpb.Compare_LEASE
								c.TargetUnion = &etcdserverpb.Compare_Lease{Lease: i}
								return c
							},
						},
						{
							targetName: etcdserverpb.Compare_MOD.String(),
							fillTargetFn: func(c *etcdserverpb.Compare, i int64) *etcdserverpb.Compare {
								c.Target = etcdserverpb.Compare_MOD
								c.TargetUnion = &etcdserverpb.Compare_ModRevision{ModRevision: i}
								return c
							},
						},
					} {
						var (
							interval = &closedOpenInterval{start: []byte(path.Join(prefix, "a")), end: []byte(path.Join(prefix, "c"))}
							spec     = s.spec
						)

						if len(spec) <= 0 {
							spec = s.targetName
						}

						{
							var (
								spec = fmt.Sprintf("%s does not match", spec)
								td   = &TreeDef{Subtrees: map[string]TreeDef{
									"a": {
										Blobs: map[string][]byte{s.targetName: []byte("1")},
									},
								}}
							)

							checks = append(
								checks,
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_CompareResult(-1),
										},
										int64(2),
									)},
									matchErr:     MatchError(ContainSubstring("unsupported compare result")),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_CompareResult(4),
										},
										int64(2),
									)},
									matchErr:     MatchError(ContainSubstring("unsupported compare result")),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
										},
										int64(2),
									)},
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_EQUAL,
										},
										int64(2),
									)},
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_NOT_EQUAL,
										},
										int64(1),
									)},
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_GREATER,
										},
										int64(1),
									)},
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_LESS,
										},
										int64(1),
									)},
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									dataRoot: &TreeDef{Blobs: map[string][]byte{"a": []byte("1")}},
									compares: []*etcdserverpb.Compare{
										s.fillTargetFn(
											&etcdserverpb.Compare{
												Key:      interval.start,
												RangeEnd: interval.end,
												Result:   etcdserverpb.Compare_LESS,
											},
											int64(1),
										),
										{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_CompareResult(-1),
											Target:      etcdserverpb.Compare_VALUE,
											TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
										},
									},
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec: spec,
									metaRoot: &TreeDef{
										Subtrees: map[string]TreeDef{
											"a": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
											"b": {
												Subtrees: map[string]TreeDef{
													"1": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
													"2": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
													"3": {
														Subtrees: map[string]TreeDef{
															"4": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
															"5": {
																Subtrees: map[string]TreeDef{
																	"6": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
																	"7": {Blobs: map[string][]byte{s.targetName: []byte("3")}},
																},
															},
														},
													},
												},
											},
										},
									},
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_LESS,
										},
										int64(2),
									)},
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
							)
						}

						{
							var (
								spec = fmt.Sprintf("%s does not match", spec)
								td   = &TreeDef{Subtrees: map[string]TreeDef{
									"a": {
										Blobs: map[string][]byte{s.targetName: []byte("3")},
									},
								}}
							)

							checks = append(
								checks,
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_CompareResult(-1),
										},
										int64(3),
									)},
									matchErr:     MatchError(ContainSubstring("unsupported compare result")),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_CompareResult(4),
										},
										int64(3),
									)},
									matchErr:     MatchError(ContainSubstring("unsupported compare result")),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
										},
										int64(3),
									)},
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_EQUAL,
										},
										int64(3),
									)},
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_NOT_EQUAL,
										},
										int64(2),
									)},
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_GREATER,
										},
										int64(2),
									)},
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec:     spec,
									metaRoot: td,
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_LESS,
										},
										int64(4),
									)},
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec: spec,
									metaRoot: &TreeDef{
										Subtrees: map[string]TreeDef{
											"a": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
											"b": {
												Subtrees: map[string]TreeDef{
													"1": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
													"2": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
													"3": {
														Subtrees: map[string]TreeDef{
															"4": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
															"5": {
																Subtrees: map[string]TreeDef{
																	"6": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
																	"7": {Blobs: map[string][]byte{s.targetName: []byte("3")}},
																},
															},
														},
													},
												},
											},
										},
									},
									compares: []*etcdserverpb.Compare{s.fillTargetFn(
										&etcdserverpb.Compare{
											Key:      interval.start,
											RangeEnd: interval.end,
											Result:   etcdserverpb.Compare_LESS,
										},
										int64(4),
									)},
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec: spec,
									metaRoot: &TreeDef{
										Subtrees: map[string]TreeDef{
											"a": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
											"b": {
												Subtrees: map[string]TreeDef{
													"1": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
													"2": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
													"3": {
														Subtrees: map[string]TreeDef{
															"4": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
															"5": {
																Subtrees: map[string]TreeDef{
																	"6": {Blobs: td.DeepCopy().Subtrees["a"].Blobs},
																	"7": {Blobs: map[string][]byte{s.targetName: []byte("3")}},
																},
															},
														},
													},
												},
											},
										},
									},
									dataRoot: &TreeDef{Blobs: map[string][]byte{"a": []byte("a")}},
									compares: []*etcdserverpb.Compare{
										s.fillTargetFn(
											&etcdserverpb.Compare{
												Key:      interval.start,
												RangeEnd: interval.end,
												Result:   etcdserverpb.Compare_LESS,
											},
											int64(4),
										),
										{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_CompareResult(-1),
											Target:      etcdserverpb.Compare_VALUE,
											TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("4")},
										},
									},
									matchErr:     MatchError(ContainSubstring("unsupported compare result")),
									matchCompare: BeFalse(),
								},
							)
						}
					}

					{
						var (
							spec     = "multiple mixed comparisions"
							interval = &closedOpenInterval{start: []byte(path.Join(prefix, "a")), end: []byte(path.Join(prefix, "c"))}

							metaRoot = &TreeDef{
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte("0"),
										etcdserverpb.Compare_LEASE.String():   []byte("1"),
										etcdserverpb.Compare_MOD.String():     []byte("2"),
										etcdserverpb.Compare_VERSION.String(): []byte("3"),
									}},
									"a": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte("1"),
										etcdserverpb.Compare_LEASE.String():   []byte("2"),
										etcdserverpb.Compare_MOD.String():     []byte("3"),
										etcdserverpb.Compare_VERSION.String(): []byte("4"),
									}},
									"b": {
										Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte("1"),
												etcdserverpb.Compare_LEASE.String():   []byte("2"),
												etcdserverpb.Compare_MOD.String():     []byte("3"),
												etcdserverpb.Compare_VERSION.String(): []byte("4"),
											}},
											"2": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte("2"),
												etcdserverpb.Compare_LEASE.String():   []byte("3"),
												etcdserverpb.Compare_MOD.String():     []byte("4"),
												etcdserverpb.Compare_VERSION.String(): []byte("5"),
											}},
											"3": {
												Subtrees: map[string]TreeDef{
													"4": {Blobs: map[string][]byte{
														etcdserverpb.Compare_CREATE.String():  []byte("4"),
														etcdserverpb.Compare_LEASE.String():   []byte("5"),
														etcdserverpb.Compare_MOD.String():     []byte("6"),
														etcdserverpb.Compare_VERSION.String(): []byte("7"),
													}},
													"5": {
														Subtrees: map[string]TreeDef{
															"6": {Blobs: map[string][]byte{
																etcdserverpb.Compare_CREATE.String():  []byte("6"),
																etcdserverpb.Compare_LEASE.String():   []byte("7"),
																etcdserverpb.Compare_MOD.String():     []byte("8"),
																etcdserverpb.Compare_VERSION.String(): []byte("9"),
															}},
															"7": {Blobs: map[string][]byte{
																etcdserverpb.Compare_CREATE.String():  []byte("7"),
																etcdserverpb.Compare_LEASE.String():   []byte("8"),
																etcdserverpb.Compare_MOD.String():     []byte("9"),
																etcdserverpb.Compare_VERSION.String(): []byte("10"),
															}},
														},
													},
												},
											},
										},
									},
									"c": {
										Subtrees: map[string]TreeDef{
											"8": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte("8"),
												etcdserverpb.Compare_LEASE.String():   []byte("9"),
												etcdserverpb.Compare_MOD.String():     []byte("10"),
												etcdserverpb.Compare_VERSION.String(): []byte("11"),
											}},
										},
									},
								},
							}

							dataRoot = &TreeDef{
								Blobs: map[string][]byte{"1": []byte("0"), "a": []byte("1")},
								Subtrees: map[string]TreeDef{
									"b": {
										Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")},
										Subtrees: map[string]TreeDef{
											"3": {
												Blobs: map[string][]byte{"4": []byte("4")},
												Subtrees: map[string]TreeDef{
													"5": {Blobs: map[string][]byte{"6": []byte("6"), "7": []byte("7")}},
												},
											},
										},
									},
									"c": {Blobs: map[string][]byte{"8": []byte("8")}},
								},
							}

							copyAndAppend = func(template []*etcdserverpb.Compare, more ...*etcdserverpb.Compare) (compares []*etcdserverpb.Compare) {
								compares = append(compares, template...)
								compares = append(compares, more...)
								return
							}
						)

						{
							var compares = []*etcdserverpb.Compare{
								{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("8")},
								},
								{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_NOT_EQUAL,
									Target:      etcdserverpb.Compare_CREATE,
									TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 8},
								},
								{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_GREATER,
									Target:      etcdserverpb.Compare_LEASE,
									TargetUnion: &etcdserverpb.Compare_Lease{Lease: 1},
								},
								{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_MOD,
									TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 10},
								},
								{
									Key:         interval.start,
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_VERSION,
									TargetUnion: &etcdserverpb.Compare_Version{Version: 11},
								},
							}

							checks = append(
								checks,
								check{
									spec:         spec,
									metaRoot:     metaRoot,
									dataRoot:     dataRoot,
									compares:     compares,
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec:     spec,
									metaRoot: metaRoot,
									dataRoot: dataRoot,
									compares: copyAndAppend(
										compares,
										&etcdserverpb.Compare{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_LESS,
											Target:      etcdserverpb.Compare_VERSION,
											TargetUnion: &etcdserverpb.Compare_Version{Version: 9},
										},
										&etcdserverpb.Compare{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_CompareResult(-1),
											Target:      etcdserverpb.Compare_VERSION,
											TargetUnion: &etcdserverpb.Compare_Version{Version: 9},
										},
									),
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: metaRoot,
									dataRoot: dataRoot,
									compares: copyAndAppend(
										compares,
										&etcdserverpb.Compare{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_LESS,
											Target:      etcdserverpb.Compare_CompareTarget(-1),
											TargetUnion: &etcdserverpb.Compare_Version{Version: 11},
										},
										&etcdserverpb.Compare{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_CompareResult(-1),
											Target:      etcdserverpb.Compare_VERSION,
											TargetUnion: &etcdserverpb.Compare_Version{Version: 11},
										},
									),
									matchErr:     MatchError(ContainSubstring("unsupported compare target")),
									matchCompare: BeFalse(),
								},
							)
						}

						{
							var compares = []*etcdserverpb.Compare{
								{
									Key:         interval.start,
									RangeEnd:    []byte(path.Join(prefix, "b")),
									Result:      etcdserverpb.Compare_EQUAL,
									Target:      etcdserverpb.Compare_VALUE,
									TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
								},
								{
									Key:         []byte(path.Join(prefix, "b/3/5/6")),
									RangeEnd:    interval.end,
									Result:      etcdserverpb.Compare_GREATER,
									Target:      etcdserverpb.Compare_CREATE,
									TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 4},
								},
								{
									Key:         []byte(path.Join(prefix, "b/3/5/6")),
									RangeEnd:    []byte(path.Join(prefix, "b/3/5/7")),
									Result:      etcdserverpb.Compare_EQUAL,
									Target:      etcdserverpb.Compare_LEASE,
									TargetUnion: &etcdserverpb.Compare_Lease{Lease: 7},
								},
								{
									Key:         []byte(path.Join(prefix, "c/8")),
									Result:      etcdserverpb.Compare_NOT_EQUAL,
									Target:      etcdserverpb.Compare_MOD,
									TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 11},
								},
								{
									Key:         []byte(path.Join(prefix, "1")),
									Result:      etcdserverpb.Compare_LESS,
									Target:      etcdserverpb.Compare_VERSION,
									TargetUnion: &etcdserverpb.Compare_Version{Version: 4},
								},
							}

							checks = append(
								checks,
								check{
									spec:         spec,
									metaRoot:     metaRoot,
									dataRoot:     dataRoot,
									compares:     compares,
									matchErr:     Succeed(),
									matchCompare: BeTrue(),
								},
								check{
									spec:     spec,
									metaRoot: metaRoot,
									dataRoot: dataRoot,
									compares: copyAndAppend(
										compares,
										&etcdserverpb.Compare{
											Key:         []byte(path.Join(prefix, "1")),
											Result:      etcdserverpb.Compare_LESS,
											Target:      etcdserverpb.Compare_VERSION,
											TargetUnion: &etcdserverpb.Compare_Version{Version: 3},
										},
										&etcdserverpb.Compare{
											Key:         []byte(path.Join(prefix, "1")),
											Result:      etcdserverpb.Compare_CompareResult(-1),
											Target:      etcdserverpb.Compare_VERSION,
											TargetUnion: &etcdserverpb.Compare_Version{Version: 3},
										},
									),
									matchErr:     Succeed(),
									matchCompare: BeFalse(),
								},
								check{
									spec:     spec,
									metaRoot: metaRoot,
									dataRoot: dataRoot,
									compares: copyAndAppend(
										compares,
										&etcdserverpb.Compare{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_LESS,
											Target:      etcdserverpb.Compare_CompareTarget(-1),
											TargetUnion: &etcdserverpb.Compare_Version{Version: 11},
										},
										&etcdserverpb.Compare{
											Key:         interval.start,
											RangeEnd:    interval.end,
											Result:      etcdserverpb.Compare_CompareResult(-1),
											Target:      etcdserverpb.Compare_VERSION,
											TargetUnion: &etcdserverpb.Compare_Version{Version: 11},
										},
									),
									matchErr:     MatchError(ContainSubstring("unsupported compare target")),
									matchCompare: BeFalse(),
								},
							)
						}
					}

					for _, s := range checks {
						func(s check) {
							Describe(
								fmt.Sprintf("%s hasMetaRoot=%t hasDataRoot=%t compares=%v", s.spec, s.metaRoot != nil, s.dataRoot != nil, s.compares),
								func() {
									var metaRoot, dataRoot git.Tree

									BeforeEach(func() {
										if s.metaRoot == nil {
											metaRoot = nil
										} else {
											Expect(func() (err error) {
												metaRoot, err = CreateAndLoadTreeFromDef(ctx, b.repo, s.metaRoot)
												return
											}()).To(Succeed())
										}

										if s.dataRoot == nil {
											dataRoot = nil
										} else {
											Expect(func() (err error) {
												dataRoot, err = CreateAndLoadTreeFromDef(ctx, b.repo, s.dataRoot)
												return
											}()).To(Succeed())
										}

										if s.ctxFn != nil {
											ctx = s.ctxFn(ctx)
										}
									})

									It(ItSpecForMatchError(s.matchErr), func() {
										var compare, err = b.checkTxnCompare(ctx, metaRoot, dataRoot, s.compares)
										Expect(err).To(s.matchErr)
										Expect(compare).To(s.matchCompare)
									})
								},
							)
						}(s)
					}
				})
			}(prefix)
		}
	})

	Describe("doTxnRequestOps", func() {
		type check struct {
			spec                                        string
			ctxFn                                       ContextFunc
			metaHeadFn                                  metaHeadFunc
			reqOps                                      []*etcdserverpb.RequestOp
			res                                         *etcdserverpb.TxnResponse
			newRevision                                 int64
			commitTreeFn                                func() commitTreeFunc
			matchErr                                    types.GomegaMatcher
			matchMetaMutated                            types.GomegaMatcher
			expectMetaHead                              expectMetaHeadFunc
			matchDataMutated, matchDataHeadCommitDefPtr types.GomegaMatcher
			matchResponse                               types.GomegaMatcher
			expectedResponse                            *etcdserverpb.TxnResponse
		}

		var (
			replace = func() commitTreeFunc { return b.replaceCurrentCommit }
			inherit = func() commitTreeFunc { return b.inheritCurrentCommit }

			appendChecks = func(
				checks []check,
				spec string,
				cmh, cdh, nmh, ndh *CommitDef,
				reqOps []*etcdserverpb.RequestOp,
				res *etcdserverpb.TxnResponse,
				newRevision int64,
				matchErr, matchMetaMutated, matchDataMutated, matchResponse types.GomegaMatcher,
				expectedResponse *etcdserverpb.TxnResponse,
			) []check {
				var (
					safeCopy = func(cd *CommitDef) *CommitDef {
						if cd == nil {
							return cd
						}

						return cd.DeepCopy()
					}
				)

				for _, commitStrategy := range []struct {
					spec             string
					adjustParentsFn  func(ch, nh *CommitDef)
					commitTreeFn     func() commitTreeFunc
					expectMetaHeadFn func(nmh, ndh *CommitDef, dataMutated bool) expectMetaHeadFunc
				}{
					{
						spec: "replace",
						adjustParentsFn: func(ch, nh *CommitDef) {
							if ch != nil {
								nh.Parents = ch.Parents
							}
						},
						commitTreeFn:     replace,
						expectMetaHeadFn: func(nmh, ndh *CommitDef, dataMutated bool) expectMetaHeadFunc { return expectMetaHead(nmh, ndh) },
					},
					{
						spec: "inherit",
						adjustParentsFn: func(ch, nh *CommitDef) {
							if ch != nil {
								nh.Parents = []CommitDef{*safeCopy(ch)}
							}
						},
						commitTreeFn:     inherit,
						expectMetaHeadFn: expectMetaHeadInherit,
					},
				} {
					var txnRes = &etcdserverpb.TxnResponse{
						Header:    res.Header,
						Succeeded: res.Succeeded,
						Responses: append([]*etcdserverpb.ResponseOp(nil), res.Responses...),
					}

					nmh, ndh = nmh.DeepCopy(), ndh.DeepCopy()
					commitStrategy.adjustParentsFn(cmh, nmh)
					commitStrategy.adjustParentsFn(cdh, ndh)

					checks = append(checks, check{
						spec:                      fmt.Sprintf("%s %s commit", spec, commitStrategy.spec),
						metaHeadFn:                metaHeadFrom(cmh, cdh),
						reqOps:                    reqOps,
						res:                       txnRes,
						newRevision:               newRevision,
						commitTreeFn:              commitStrategy.commitTreeFn,
						matchErr:                  matchErr,
						matchMetaMutated:          matchMetaMutated,
						expectMetaHead:            commitStrategy.expectMetaHeadFn(nmh, ndh, reflect.DeepEqual(matchDataMutated, BeTrue())),
						matchDataMutated:          matchDataMutated,
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(ndh)),
						matchResponse:             matchResponse,
						expectedResponse:          expectedResponse,
					})
				}

				return checks
			}
		)

		for clusterID, memberID := range map[uint64]uint64{
			0: 0,
			// 10: 2, TODO
		} {
			func(clusterID, memberID uint64) {
				Describe(fmt.Sprintf("clusterID=%d, memberID=%d", clusterID, memberID), func() {
					BeforeEach(func() {
						b.clusterID = clusterID
						b.memberID = memberID
					})

					var (
						checks = []check{
							{
								spec:                      "expired context",
								ctxFn:                     CanceledContext,
								matchErr:                  Succeed(),
								matchMetaMutated:          BeFalse(),
								expectMetaHead:            delegateToMatcher(BeNil()),
								matchDataMutated:          BeFalse(),
								matchDataHeadCommitDefPtr: BeNil(),
								matchResponse:             BeNil(),
							},
							{
								spec:                      "expired context",
								ctxFn:                     CanceledContext,
								reqOps:                    []*etcdserverpb.RequestOp{},
								res:                       &etcdserverpb.TxnResponse{},
								matchErr:                  Succeed(),
								matchMetaMutated:          BeFalse(),
								expectMetaHead:            delegateToMatcher(BeNil()),
								matchDataMutated:          BeFalse(),
								matchDataHeadCommitDefPtr: BeNil(),
								matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
									"Header":    BeNil(),
									"Succeeded": BeFalse(),
									"Responses": BeEmpty(),
								})),
							},
							func() check {
								var res = &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
									ClusterId: clusterID,
									MemberId:  memberID,
								}}

								return check{
									spec:  "expired context",
									ctxFn: CanceledContext,
									reqOps: []*etcdserverpb.RequestOp{
										{},
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:   []byte("1"),
											Value: []byte("1"),
										}}},
									},
									res:                       res,
									matchErr:                  MatchError(ContainSubstring("unsupported request type")),
									matchMetaMutated:          BeFalse(),
									expectMetaHead:            delegateToMatcher(BeNil()),
									matchDataMutated:          BeFalse(),
									matchDataHeadCommitDefPtr: BeNil(),
									matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
										"Header":    Equal(res.Header),
										"Succeeded": BeFalse(),
										"Responses": BeEmpty(),
									})),
								}
							}(),
							func() check {
								var res = &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
									ClusterId: clusterID,
									MemberId:  memberID,
								}}

								return check{
									spec:  "expired context",
									ctxFn: CanceledContext,
									reqOps: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:   []byte("1"),
											Value: []byte("1"),
										}}},
										{},
									},
									res:                       res,
									matchErr:                  MatchError("context canceled"),
									matchMetaMutated:          BeFalse(),
									expectMetaHead:            delegateToMatcher(BeNil()),
									matchDataMutated:          BeFalse(),
									matchDataHeadCommitDefPtr: BeNil(),
									matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
										"Header":    Equal(res.Header),
										"Succeeded": BeFalse(),
										"Responses": BeEmpty(),
									})),
								}
							}(),
							func() check {
								var res = &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
									ClusterId: clusterID,
									MemberId:  memberID,
								}}

								return check{
									spec:  "expired context",
									ctxFn: CanceledContext,
									reqOps: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key: []byte("1"),
										}}},
										{},
									},
									res:                       res,
									matchErr:                  MatchError(ContainSubstring("unsupported request type")),
									matchMetaMutated:          BeFalse(),
									expectMetaHead:            delegateToMatcher(BeNil()),
									matchDataMutated:          BeFalse(),
									matchDataHeadCommitDefPtr: BeNil(),
									matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
										"Header":    Equal(res.Header),
										"Succeeded": BeFalse(),
										"Responses": ConsistOf(&etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{}}),
									})),
								}
							}(),
							func() check {
								var res = &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
									ClusterId: clusterID,
									MemberId:  memberID,
								}}

								return check{
									spec:       "expired context",
									ctxFn:      CanceledContext,
									metaHeadFn: metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "1"}),
									reqOps: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key: []byte("1"),
										}}},
										{},
									},
									res:                       res,
									matchErr:                  MatchError("context canceled"),
									matchMetaMutated:          BeFalse(),
									expectMetaHead:            delegateToMatcher(BeNil()),
									matchDataMutated:          BeFalse(),
									matchDataHeadCommitDefPtr: BeNil(),
									matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
										"Header":    Equal(res.Header),
										"Succeeded": BeFalse(),
										"Responses": BeEmpty(),
									})),
								}
							}(),
							func() check {
								var res = &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
									ClusterId: clusterID,
									MemberId:  memberID,
								}}

								return check{
									spec:  "expired context",
									ctxFn: CanceledContext,
									reqOps: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
											Key: []byte("1"),
										}}},
										{},
									},
									res:                       res,
									matchErr:                  MatchError(ContainSubstring("unsupported request type")),
									matchMetaMutated:          BeFalse(),
									expectMetaHead:            delegateToMatcher(BeNil()),
									matchDataMutated:          BeFalse(),
									matchDataHeadCommitDefPtr: BeNil(),
									matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
										"Header":    Equal(res.Header),
										"Succeeded": BeFalse(),
										"Responses": ConsistOf(&etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
											ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: res.Header},
										}}),
									})),
								}
							}(),
							func() check {
								var res = &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
									ClusterId: clusterID,
									MemberId:  memberID,
								}}

								return check{
									spec:       "expired context",
									ctxFn:      CanceledContext,
									metaHeadFn: metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "1"}),
									reqOps: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
											Key: []byte("1"),
										}}},
										{},
									},
									res:                       res,
									matchErr:                  MatchError("context canceled"),
									matchMetaMutated:          BeFalse(),
									expectMetaHead:            delegateToMatcher(BeNil()),
									matchDataMutated:          BeFalse(),
									matchDataHeadCommitDefPtr: BeNil(),
									matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
										"Header":    Equal(res.Header),
										"Succeeded": BeFalse(),
										"Responses": BeEmpty(),
									})),
								}
							}(),
							func() check {
								var res = &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
									ClusterId: clusterID,
									MemberId:  memberID,
								}}

								return check{
									spec:  "expired context",
									ctxFn: CanceledContext,
									reqOps: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key: []byte("1"),
										}}},
										{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
											Success: []*etcdserverpb.RequestOp{
												{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
													Key: []byte("1"),
												}}},
											},
											Failure: []*etcdserverpb.RequestOp{{}},
										}}},
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:   []byte("1"),
											Value: []byte("1"),
										}}},
										{},
									},
									res:                       res,
									matchErr:                  MatchError("context canceled"),
									matchMetaMutated:          BeFalse(),
									expectMetaHead:            delegateToMatcher(BeNil()),
									matchDataMutated:          BeFalse(),
									matchDataHeadCommitDefPtr: BeNil(),
									matchResponse: PointTo(MatchFields(IgnoreExtras, Fields{
										"Header":    Equal(res.Header),
										"Succeeded": BeFalse(),
										"Responses": ConsistOf(
											&etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{}},
											PointTo(MatchFields(IgnoreExtras, Fields{
												"Response": PointTo(MatchFields(IgnoreExtras, Fields{
													"ResponseTxn": PointTo(MatchFields(IgnoreExtras, Fields{
														"Header":    Equal(res.Header),
														"Succeeded": BeTrue(),
														"Responses": ConsistOf(
															&etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
																ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: res.Header},
															}},
														),
													})),
												})),
											})),
										),
									})),
								}
							}(),
						}
					)

					{
						var (
							newRevision = int64(3)
							newHeader   = &etcdserverpb.ResponseHeader{
								ClusterId: clusterID,
								MemberId:  memberID,
								Revision:  newRevision,
								RaftTerm:  uint64(newRevision),
							}
						)

						checks = appendChecks(
							checks,
							"no metadata, no data",
							nil,
							nil,
							&CommitDef{Message: revisionToString(newRevision), Tree: TreeDef{
								Blobs: map[string][]byte{metadataPathRevision: []byte(revisionToString(newRevision))},
								Subtrees: map[string]TreeDef{
									"1": {
										Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(newRevision)),
											etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(4)),
											etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
											etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(2)),
										},
									},
									"3": {
										Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(newRevision)),
											etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
											etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(1)),
										},
									},
								},
							}},
							&CommitDef{Message: revisionToString(newRevision), Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("10"), "3": []byte("3")},
							}},
							[]*etcdserverpb.RequestOp{
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key: []byte("1"),
								}}},
								{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
									Key:    []byte("1"),
									Value:  []byte("1"),
									PrevKv: true,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key: []byte("1"),
								}}},
								{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
									Key:    []byte("2"),
									PrevKv: true,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
									Key:    []byte("2"),
									Value:  []byte("2"),
									PrevKv: true,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:            []byte("\x00"),
									RangeEnd:       []byte("\x00"),
									MinModRevision: 4,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:               []byte("\x00"),
									RangeEnd:          []byte("\x00"),
									MinCreateRevision: 3,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
									Key:    []byte("1"),
									Value:  []byte("10"),
									Lease:  4,
									PrevKv: true,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
									Key:    []byte("3"),
									Value:  []byte("3"),
									PrevKv: true,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:      []byte("\x00"),
									RangeEnd: []byte("\x00"),
								}}},
								{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
									Key:      []byte("2"),
									RangeEnd: []byte("3"),
									PrevKv:   true,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:      []byte("\x00"),
									RangeEnd: []byte("\x00"),
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:      []byte("\x00"),
									RangeEnd: []byte("\x00"),
									Limit:    1,
								}}},
							},
							&etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
								ClusterId: clusterID,
								MemberId:  memberID,
							}},
							int64(3),
							Succeed(),
							BeTrue(),
							BeTrue(),
							nil,
							&etcdserverpb.TxnResponse{
								Header: &etcdserverpb.ResponseHeader{
									ClusterId: newHeader.ClusterId,
									MemberId:  newHeader.MemberId,
								},
								Responses: []*etcdserverpb.ResponseOp{
									{Response: &etcdserverpb.ResponseOp_ResponseRange{}},
									{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: newHeader}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{Key: []byte("1"), Value: []byte("1"), CreateRevision: 3, ModRevision: 3, Version: 1},
										},
										Count: 1,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{
										Header: &etcdserverpb.ResponseHeader{
											ClusterId: newHeader.ClusterId,
											MemberId:  newHeader.MemberId,
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: newHeader}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: newHeader}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{Key: []byte("1"), Value: []byte("1"), CreateRevision: 3, ModRevision: 3, Version: 1},
											{Key: []byte("2"), Value: []byte("2"), CreateRevision: 3, ModRevision: 3, Version: 1},
										},
										Count: 2,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
										Header: newHeader,
										PrevKv: &mvccpb.KeyValue{Key: []byte("1"), Value: []byte("1"), CreateRevision: 3, ModRevision: 3, Version: 1},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: newHeader}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{Key: []byte("1"), Value: []byte("10"), CreateRevision: 3, Lease: 4, ModRevision: 3, Version: 2},
											{Key: []byte("2"), Value: []byte("2"), CreateRevision: 3, ModRevision: 3, Version: 1},
											{Key: []byte("3"), Value: []byte("3"), CreateRevision: 3, ModRevision: 3, Version: 1},
										},
										Count: 3,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{
										Header:  newHeader,
										Deleted: 1,
										PrevKvs: []*mvccpb.KeyValue{
											{Key: []byte("2"), Value: []byte("2"), CreateRevision: 3, ModRevision: 3, Version: 1},
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{Key: []byte("1"), Value: []byte("10"), CreateRevision: 3, Lease: 4, ModRevision: 3, Version: 2},
											{Key: []byte("3"), Value: []byte("3"), CreateRevision: 3, ModRevision: 3, Version: 1},
										},
										Count: 2,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{Key: []byte("1"), Value: []byte("10"), CreateRevision: 3, Lease: 4, ModRevision: 3, Version: 2},
										},
										Count: 1,
										More:  true,
									}}},
								},
							},
						)

						checks = appendChecks(
							checks,
							"no metadata, no data",
							nil,
							nil,
							&CommitDef{Message: revisionToString(newRevision), Tree: TreeDef{
								Blobs: map[string][]byte{metadataPathRevision: []byte(revisionToString(newRevision))},
								Subtrees: map[string]TreeDef{
									"a": {
										Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(newRevision)),
											etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(22)),
											etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
											etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(1)),
										},
									},
									"b": {
										Subtrees: map[string]TreeDef{
											"3": {
												Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(newRevision)),
													etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(33)),
													etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
													etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(1)),
												},
											},
											"4": {
												Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(newRevision)),
													etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(44)),
													etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
													etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(2)),
												},
											},
										},
									},
								},
							}},
							&CommitDef{Message: revisionToString(newRevision), Tree: TreeDef{
								Blobs: map[string][]byte{"a": []byte("20")},
								Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{"3": []byte("3"), "4": []byte("40")}},
								},
							}},
							[]*etcdserverpb.RequestOp{
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:      []byte("\x00"),
									RangeEnd: []byte("\x00"),
								}}},
								{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
									Success: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("1"),
											Value:  []byte("1"),
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key: []byte("1"),
										}}},
									},
									Failure: []*etcdserverpb.RequestOp{{}},
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:               []byte("\x00"),
									RangeEnd:          []byte("\x00"),
									MinCreateRevision: newRevision - 1,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
									Compare: []*etcdserverpb.Compare{{
										Key:         []byte("a/2"),
										Target:      etcdserverpb.Compare_VALUE,
										TargetUnion: &etcdserverpb.Compare_Value{},
									}},
									Success: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("a/2"),
											Value:  []byte("2"),
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key: []byte("a/2"),
										}}},
									},
									Failure: []*etcdserverpb.RequestOp{{}}, // Compare now succeeds for missing keys.
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:               []byte("\x00"),
									RangeEnd:          []byte("\x00"),
									MaxCreateRevision: newRevision + 1,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
									Compare: []*etcdserverpb.Compare{{
										Key:         []byte("a/2"),
										Result:      etcdserverpb.Compare_NOT_EQUAL,
										Target:      etcdserverpb.Compare_LEASE,
										TargetUnion: &etcdserverpb.Compare_Lease{Lease: 100},
									}},
									Success: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("b/3"),
											Value:  []byte("3"),
											Lease:  30,
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key: []byte("b/3"),
										}}},
									},
									Failure: []*etcdserverpb.RequestOp{{}},
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:            []byte("\x00"),
									RangeEnd:       []byte("\x00"),
									MinModRevision: newRevision,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
									Compare: []*etcdserverpb.Compare{{
										Key:         []byte("b/3"),
										Result:      etcdserverpb.Compare_LESS,
										Target:      etcdserverpb.Compare_LEASE,
										TargetUnion: &etcdserverpb.Compare_Lease{Lease: 10},
									}},
									Success: []*etcdserverpb.RequestOp{{}},
									Failure: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("b/4"),
											Value:  []byte("4"),
											Lease:  40,
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key: []byte("b/4"),
										}}},
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:         []byte("b/3"),
											Value:       []byte("30"),
											Lease:       33,
											IgnoreValue: true,
											PrevKv:      true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("b/4"),
											Value:  []byte("40"),
											Lease:  44,
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key:      []byte("b"),
											RangeEnd: []byte("c"),
										}}},
									},
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:            []byte("\x00"),
									RangeEnd:       []byte("\x00"),
									MaxModRevision: newRevision,
								}}},
								{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
									Compare: []*etcdserverpb.Compare{
										{
											Key:         []byte("1"),
											Target:      etcdserverpb.Compare_VALUE,
											TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
										},
										{
											Key:         []byte("b/3"),
											Target:      etcdserverpb.Compare_VERSION,
											TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
										},
									},
									Success: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
											Key:    []byte("1/1"),
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
											Key:    []byte("b"),
											PrevKv: true,
										}}},
									},
									Failure: []*etcdserverpb.RequestOp{{}},
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:      []byte("\x00"),
									RangeEnd: []byte("\x00"),
								}}},
								{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
									Compare: []*etcdserverpb.Compare{
										{
											Key:         []byte("b/3"),
											Result:      etcdserverpb.Compare_NOT_EQUAL,
											Target:      etcdserverpb.Compare_VALUE,
											TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("1")},
										},
									},
									Success: []*etcdserverpb.RequestOp{
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("a"),
											Value:  []byte("20"),
											Lease:  22,
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("2"),
											Value:  []byte("2"),
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
											Key:    []byte("3"),
											Value:  []byte("3"),
											PrevKv: true,
										}}},
										{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
											Key:      []byte("\x00"),
											RangeEnd: []byte("\x00"),
										}}},
										{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
											Key:      []byte("1"),
											RangeEnd: []byte("a"),
											PrevKv:   true,
										}}},
									},
									Failure: []*etcdserverpb.RequestOp{{}},
								}}},
								{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
									Key:      []byte("\x00"),
									RangeEnd: []byte("\x00"),
								}}},
							},
							&etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{
								ClusterId: clusterID,
								MemberId:  memberID,
							}},
							int64(3),
							Succeed(),
							BeTrue(),
							BeTrue(),
							nil,
							&etcdserverpb.TxnResponse{
								Header: &etcdserverpb.ResponseHeader{
									ClusterId: newHeader.ClusterId,
									MemberId:  newHeader.MemberId,
								},
								Responses: []*etcdserverpb.ResponseOp{
									{Response: &etcdserverpb.ResponseOp_ResponseRange{}},
									{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{
										Header:    newHeader,
										Succeeded: true,
										Responses: []*etcdserverpb.ResponseOp{
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
												Header: newHeader,
												Kvs: []*mvccpb.KeyValue{
													{
														Key:            []byte("1"),
														Value:          []byte("1"),
														CreateRevision: newRevision,
														ModRevision:    newRevision,
														Version:        1,
													},
												},
												Count: 1,
											}}},
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{
												Key:            []byte("1"),
												Value:          []byte("1"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
										},
										Count: 1,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{
										Header:    newHeader,
										Succeeded: true,
										Responses: []*etcdserverpb.ResponseOp{
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
												Header: newHeader,
												Kvs: []*mvccpb.KeyValue{
													{
														Key:            []byte("a/2"),
														Value:          []byte("2"),
														CreateRevision: newRevision,
														ModRevision:    newRevision,
														Version:        1,
													},
												},
												Count: 1,
											}}},
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{
												Key:            []byte("1"),
												Value:          []byte("1"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("a/2"),
												Value:          []byte("2"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
										},
										Count: 2,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{
										Header:    newHeader,
										Succeeded: true,
										Responses: []*etcdserverpb.ResponseOp{
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
												Header: newHeader,
												Kvs: []*mvccpb.KeyValue{
													{
														Key:            []byte("b/3"),
														Value:          []byte("3"),
														CreateRevision: newRevision,
														Lease:          30,
														ModRevision:    newRevision,
														Version:        1,
													},
												},
												Count: 1,
											}}},
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{
												Key:            []byte("1"),
												Value:          []byte("1"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("a/2"),
												Value:          []byte("2"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("b/3"),
												Value:          []byte("3"),
												CreateRevision: newRevision,
												Lease:          30,
												ModRevision:    newRevision,
												Version:        1,
											},
										},
										Count: 3,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{
										Header:    newHeader,
										Succeeded: false,
										Responses: []*etcdserverpb.ResponseOp{
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
												Header: newHeader,
												Kvs: []*mvccpb.KeyValue{
													{
														Key:            []byte("b/4"),
														Value:          []byte("4"),
														CreateRevision: newRevision,
														Lease:          40,
														ModRevision:    newRevision,
														Version:        1,
													},
												},
												Count: 1,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
												PrevKv: &mvccpb.KeyValue{
													Key:            []byte("b/3"),
													Value:          []byte("3"),
													CreateRevision: newRevision,
													Lease:          30,
													ModRevision:    newRevision,
													Version:        1,
												},
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
												PrevKv: &mvccpb.KeyValue{
													Key:            []byte("b/4"),
													Value:          []byte("4"),
													CreateRevision: newRevision,
													Lease:          40,
													ModRevision:    newRevision,
													Version:        1,
												},
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
												Header: newHeader,
												Kvs: []*mvccpb.KeyValue{
													{
														Key:            []byte("b/3"),
														Value:          []byte("3"),
														CreateRevision: newRevision,
														Lease:          33,
														ModRevision:    newRevision,
														Version:        1,
													},
													{
														Key:            []byte("b/4"),
														Value:          []byte("40"),
														CreateRevision: newRevision,
														Lease:          44,
														ModRevision:    newRevision,
														Version:        2,
													},
												},
												Count: 2,
											}}},
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{
												Key:            []byte("1"),
												Value:          []byte("1"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("a/2"),
												Value:          []byte("2"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("b/3"),
												Value:          []byte("3"),
												CreateRevision: newRevision,
												Lease:          33,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("b/4"),
												Value:          []byte("40"),
												CreateRevision: newRevision,
												Lease:          44,
												ModRevision:    newRevision,
												Version:        2,
											},
										},
										Count: 4,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{
										Header: &etcdserverpb.ResponseHeader{
											ClusterId: newHeader.ClusterId,
											MemberId:  newHeader.MemberId,
										},
										Succeeded: true,
										Responses: []*etcdserverpb.ResponseOp{
											{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
												ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: &etcdserverpb.ResponseHeader{
													ClusterId: newHeader.ClusterId,
													MemberId:  newHeader.MemberId,
												}},
											}},
											{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
												ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: &etcdserverpb.ResponseHeader{
													ClusterId: newHeader.ClusterId,
													MemberId:  newHeader.MemberId,
												}},
											}},
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{
												Key:            []byte("1"),
												Value:          []byte("1"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("a/2"),
												Value:          []byte("2"),
												CreateRevision: newRevision,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("b/3"),
												Value:          []byte("3"),
												CreateRevision: newRevision,
												Lease:          33,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("b/4"),
												Value:          []byte("40"),
												CreateRevision: newRevision,
												Lease:          44,
												ModRevision:    newRevision,
												Version:        2,
											},
										},
										Count: 4,
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{
										Header:    newHeader,
										Succeeded: true,
										Responses: []*etcdserverpb.ResponseOp{
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
												Header: newHeader,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
												Header: newHeader,
												Kvs: []*mvccpb.KeyValue{
													{
														Key:            []byte("1"),
														Value:          []byte("1"),
														CreateRevision: newRevision,
														ModRevision:    newRevision,
														Version:        1,
													},
													{
														Key:            []byte("2"),
														Value:          []byte("2"),
														CreateRevision: newRevision,
														ModRevision:    newRevision,
														Version:        1,
													},
													{
														Key:            []byte("3"),
														Value:          []byte("3"),
														CreateRevision: newRevision,
														ModRevision:    newRevision,
														Version:        1,
													},
													{
														Key:            []byte("a"),
														Value:          []byte("20"), // TODO
														CreateRevision: newRevision,
														Lease:          22,
														ModRevision:    newRevision,
														Version:        1,
													},
													{
														Key:            []byte("b/3"),
														Value:          []byte("3"),
														CreateRevision: newRevision,
														Lease:          33,
														ModRevision:    newRevision,
														Version:        1,
													},
													{
														Key:            []byte("b/4"),
														Value:          []byte("40"),
														CreateRevision: newRevision,
														Lease:          44,
														ModRevision:    newRevision,
														Version:        2,
													},
												},
												Count: 6,
											}}},
											{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
												ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{
													Header:  newHeader,
													Deleted: 3,
													PrevKvs: []*mvccpb.KeyValue{
														{
															Key:            []byte("1"),
															Value:          []byte("1"),
															CreateRevision: newRevision,
															ModRevision:    newRevision,
															Version:        1,
														},
														{
															Key:            []byte("2"),
															Value:          []byte("2"),
															CreateRevision: newRevision,
															ModRevision:    newRevision,
															Version:        1,
														},
														{
															Key:            []byte("3"),
															Value:          []byte("3"),
															CreateRevision: newRevision,
															ModRevision:    newRevision,
															Version:        1,
														},
													},
												}},
											},
										},
									}}},
									{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
										Header: newHeader,
										Kvs: []*mvccpb.KeyValue{
											{
												Key:            []byte("a"),
												Value:          []byte("20"),
												CreateRevision: newRevision,
												Lease:          22,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("b/3"),
												Value:          []byte("3"),
												CreateRevision: newRevision,
												Lease:          33,
												ModRevision:    newRevision,
												Version:        1,
											},
											{
												Key:            []byte("b/4"),
												Value:          []byte("40"),
												CreateRevision: newRevision,
												Lease:          44,
												ModRevision:    newRevision,
												Version:        2,
											},
										},
										Count: 3,
									}}},
								},
							},
						)

						{
							var (
								pm = &TreeDef{
									Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte("1"),
										etcdserverpb.Compare_LEASE.String():   []byte("1"),
										etcdserverpb.Compare_MOD.String():     []byte("1"),
										etcdserverpb.Compare_VERSION.String(): []byte("1"),
									},
								}

								cm = &TreeDef{
									Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte("2"),
										etcdserverpb.Compare_LEASE.String():   []byte("2"),
										etcdserverpb.Compare_MOD.String():     []byte("2"),
										etcdserverpb.Compare_VERSION.String(): []byte("2"),
									},
								}

								nm1 = &TreeDef{
									Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte("2"),
										etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(newRevision)),
										etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
										etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(newRevision)),
									},
								}

								nm2 = &TreeDef{
									Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(newRevision)),
										etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(newRevision)),
										etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
										etcdserverpb.Compare_VERSION.String(): []byte("1"),
									},
								}
							)

							checks = appendChecks(
								checks,
								"full metadata and data",
								&CommitDef{
									Message: "2",
									Tree: TreeDef{
										Blobs: map[string][]byte{metadataPathRevision: []byte("2")},
										Subtrees: map[string]TreeDef{
											"1": *cm.DeepCopy(),
											"a": *cm.DeepCopy(),
											"b": {Subtrees: map[string]TreeDef{"2": *cm.DeepCopy()}},
											"c": {Subtrees: map[string]TreeDef{"3": *cm.DeepCopy(), "4": *cm.DeepCopy()}},
										},
									},
									Parents: []CommitDef{{
										Message: "1",
										Tree: TreeDef{
											Blobs: map[string][]byte{metadataPathRevision: []byte("1")},
											Subtrees: map[string]TreeDef{
												"1": *pm.DeepCopy(),
											},
										},
									}},
								},
								&CommitDef{
									Message: "2",
									Tree: TreeDef{
										Blobs: map[string][]byte{"1": []byte("1"), "a": []byte("a")},
										Subtrees: map[string]TreeDef{
											"b": {Blobs: map[string][]byte{"2": []byte("2")}},
											"c": {Blobs: map[string][]byte{"3": []byte("3"), "4": []byte("4")}},
										},
									},
									Parents: []CommitDef{{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}},
								},
								&CommitDef{
									Message: "3",
									Tree: TreeDef{
										Blobs: map[string][]byte{metadataPathRevision: []byte(revisionToString(newRevision))},
										Subtrees: map[string]TreeDef{
											"1": *nm1.DeepCopy(),
											"a": *cm.DeepCopy(),
											"b": *nm2.DeepCopy(),
											"c": {Subtrees: map[string]TreeDef{"3": *cm.DeepCopy(), "4": *cm.DeepCopy()}},
										},
									},
								},
								&CommitDef{
									Message: "3",
									Tree: TreeDef{
										Blobs: map[string][]byte{"1": []byte("10"), "a": []byte("a"), "b": []byte("b")},
										Subtrees: map[string]TreeDef{
											"c": {Blobs: map[string][]byte{"3": []byte("3"), "4": []byte("4")}},
										},
									},
								},
								[]*etcdserverpb.RequestOp{
									{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
										Key:      []byte("\x00"),
										RangeEnd: []byte("\x00"),
									}}},
									{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
										Key:      []byte("\x00"),
										RangeEnd: []byte("\x00"),
										Revision: 1,
									}}},
									{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
										Key:   []byte("1"),
										Value: []byte("10"),
										Lease: newRevision,
									}}},
									{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
										Key:    []byte("b"),
										Value:  []byte("b"),
										Lease:  newRevision,
										PrevKv: true,
									}}},
									{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
										Key:      []byte("\x00"),
										RangeEnd: []byte("\x00"),
									}}},
								},
								&etcdserverpb.TxnResponse{},
								newRevision,
								Succeed(),
								BeTrue(),
								BeTrue(),
								nil,
								&etcdserverpb.TxnResponse{
									Responses: []*etcdserverpb.ResponseOp{
										{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  2,
												RaftTerm:  2,
											},
											Count: 5,
											Kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte("1"),
													Value:          []byte("1"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
												{
													Key:            []byte("a"),
													Value:          []byte("a"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
												{
													Key:            []byte("b/2"),
													Value:          []byte("2"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
												{
													Key:            []byte("c/3"),
													Value:          []byte("3"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
												{
													Key:            []byte("c/4"),
													Value:          []byte("4"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
											},
										}}},
										{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  1,
												RaftTerm:  1,
											},
											Count: 1,
											Kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte("1"),
													Value:          []byte("1"),
													CreateRevision: 1,
													ModRevision:    1,
													Version:        1,
													Lease:          1,
												},
											},
										}}},
										{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
											Header: newHeader,
										}}},
										{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
											Header: newHeader,
										}}},
										{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
											Header: newHeader,
											Count:  5,
											Kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte("1"),
													Value:          []byte("10"),
													CreateRevision: 2,
													ModRevision:    newRevision,
													Version:        newRevision,
													Lease:          newRevision,
												},
												{
													Key:            []byte("a"),
													Value:          []byte("a"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
												{
													Key:            []byte("b"),
													Value:          []byte("b"),
													CreateRevision: newRevision,
													ModRevision:    newRevision,
													Version:        1,
													Lease:          newRevision,
												},
												{
													Key:            []byte("c/3"),
													Value:          []byte("3"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
												{
													Key:            []byte("c/4"),
													Value:          []byte("4"),
													CreateRevision: 2,
													ModRevision:    2,
													Version:        2,
													Lease:          2,
												},
											},
										}}},
									},
								},
							)
						}
					}

					for _, s := range checks {
						func(s check) {
							Describe(fmt.Sprintf("%s hasMetaHead=%t, reqOps=%v, res=%v", s.spec, s.metaHeadFn != nil, s.reqOps, s.res), func() {
								var (
									metaHead  git.Commit
									parentCtx context.Context
								)

								BeforeEach(func() {
									if s.metaHeadFn != nil {
										Expect(func() (err error) { metaHead, err = s.metaHeadFn(ctx, b.repo); return }()).To(Succeed())
									}

									parentCtx = ctx

									if s.ctxFn != nil {
										ctx = s.ctxFn(ctx)
									}
								})

								AfterEach(func() {
									if metaHead != nil {
										Expect(metaHead.Close()).To(Succeed())
									}
								})

								It(ItSpecForMatchError(s.matchErr), func() {
									var (
										commitTreeFn                 commitTreeFunc
										metaMutated, dataMutated     bool
										newMetaHeadID, newDataHeadID git.ObjectID
										err                          error
									)

									if s.commitTreeFn != nil {
										commitTreeFn = s.commitTreeFn()
									}

									metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doTxnRequestOps(
										ctx,
										metaHead,
										s.reqOps,
										s.res,
										s.newRevision,
										commitTreeFn,
									)

									Expect(err).To(s.matchErr)

									Expect(s.res).To(func() types.GomegaMatcher {
										for _, s := range []struct {
											check interface{}
											match types.GomegaMatcher
										}{
											{check: s.matchResponse, match: s.matchResponse},
											{check: s.expectedResponse, match: Equal(s.expectedResponse)},
										} {
											if s.check != nil {
												return s.match
											}
										}

										return BeNil()
									}())

									for _, s := range []struct {
										spec            string
										mutated         bool
										id              git.ObjectID
										matchMutated    types.GomegaMatcher
										expectCommitDef expectMetaHeadFunc
									}{
										{
											spec:            "data", // Check data first to see the details of the difference on error
											mutated:         dataMutated,
											id:              newDataHeadID,
											matchMutated:    s.matchDataMutated,
											expectCommitDef: delegateToMatcher(s.matchDataHeadCommitDefPtr),
										},
										{
											spec:            "metadata",
											mutated:         metaMutated,
											id:              newMetaHeadID,
											matchMutated:    s.matchMetaMutated,
											expectCommitDef: s.expectMetaHead,
										},
									} {
										Expect(s.mutated).To(s.matchMutated, s.spec)

										s.expectCommitDef(
											parentCtx,
											b.repo,
											func() *CommitDef {
												if reflect.DeepEqual(s.id, git.ObjectID{}) {
													return nil
												}

												return GetCommitDefByID(parentCtx, b.repo, s.id)
											}(),
											s.spec,
										)
									}
								})
							})
						}(s)
					}
				})
			}(clusterID, memberID)
		}
	})
})
