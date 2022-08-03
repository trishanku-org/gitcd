package backend

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/mock/gomock"
	impl "github.com/libgit2/git2go/v31"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"

	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

type assertCancelFunc func(*etcdserverpb.ResponseHeader, error)
type assertFilterSendFunc func(*etcdserverpb.ResponseHeader, []*mvccpb.Event) error
type watchMakerFunc func(*gomock.Controller, int64, assertCancelFunc, *bool, assertFilterSendFunc) watch

func newMockWatch(
	ctrl *gomock.Controller,
	watchID int64,
	assertCancelFn assertCancelFunc,
	prevKv *bool,
	assertFilterSendFn assertFilterSendFunc,
) watch {
	var (
		w             = NewMockwatch(ctrl)
		ctx, cancelFn = context.WithCancel(context.Background())
	)

	w.EXPECT().WatchId().Return(watchID).AnyTimes()
	w.EXPECT().Context().Return(ctx).AnyTimes()
	w.EXPECT().Cancel(gomock.Any(), gomock.Any()).DoAndReturn(
		func(header *etcdserverpb.ResponseHeader, err error) {
			defer cancelFn()
			if assertCancelFn != nil {
				assertCancelFn(header, err)
			}
		},
	)

	if prevKv != nil {
		w.EXPECT().PrevKv().Return(*prevKv)
	}

	if assertFilterSendFn != nil {
		w.EXPECT().FilterAndSend(gomock.Any(), gomock.Any()).DoAndReturn(
			func(header *etcdserverpb.ResponseHeader, events []*mvccpb.Event) error {
				return assertFilterSendFn(header, events)
			},
		)
	}

	return w
}

func getExpectCancelFn(spec string, matchHeader, matchErr types.GomegaMatcher) assertCancelFunc {
	return func(rh *etcdserverpb.ResponseHeader, err error) {
		Expect(rh).To(matchHeader, spec)
		Expect(err).To(matchErr, spec)
	}
}

func getExpectFilterAndSendFn(spec string, matchHeader, matchEvents types.GomegaMatcher, err error) assertFilterSendFunc {
	return func(rh *etcdserverpb.ResponseHeader, ev []*mvccpb.Event) error {
		Expect(rh).To(matchHeader, spec)
		Expect(ev).To(matchEvents, spec)
		return err
	}
}

type watchSpec struct {
	makerFn               watchMakerFunc
	cancel, prevKv        bool
	expectFilterAndSendFn assertFilterSendFunc
	expectCancelFn        assertCancelFunc
}

func getEventsWithoutPrevKv(events []*mvccpb.Event) (eventsNoPrevKv []*mvccpb.Event) {
	for _, ev := range events {
		eventsNoPrevKv = append(eventsNoPrevKv, &mvccpb.Event{Type: ev.Type, Kv: ev.Kv})
	}

	return
}

func prepareWatches(ctrl *gomock.Controller, ctxFn ContextFunc, watchSpecs []watchSpec) (watches []watch) {
	for i, ws := range watchSpecs {
		var (
			prevKvPtr             *bool
			expectFilterAndSendFn assertFilterSendFunc
			w                     watch
		)

		if ctxFn == nil && !ws.cancel {
			prevKvPtr = &ws.prevKv
			expectFilterAndSendFn = ws.expectFilterAndSendFn
		}

		w = ws.makerFn(ctrl, int64(i), ws.expectCancelFn, prevKvPtr, expectFilterAndSendFn)

		if ws.cancel {
			w.Cancel(nil, nil)
		}

		watches = append(watches, w)
	}

	return
}

func cancelOpenWatches(watches []watch) {
	for _, w := range watches {
		if w.Context().Err() == nil {
			w.Cancel(nil, nil)
		}
	}
}

var _ = Describe("getActiveWatches", func() {
	type spec struct {
		watches []watchMakerFunc
		cancels []int
	}

	var ctrl *gomock.Controller

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
	})

	for _, s := range []spec{
		{},
		{watches: []watchMakerFunc{}},
		{watches: []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch}},
		{watches: []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch}, cancels: []int{1}},
		{watches: []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch}, cancels: []int{0, 3, 5}},
		{watches: []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch}, cancels: []int{0, 1, 2, 3, 4, 5}},
		{watches: []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch}, cancels: []int{1, 2, 4, 5}},
		{watches: []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch}, cancels: []int{0, 1, 2}},
	} {
		func(s spec) {
			Describe(
				fmt.Sprintf("watches=%v, cancels=%v", s.watches, s.cancels),
				func() {
					var (
						watches, activeWatches []watch
						expectedIdenticalTos   []interface{}
					)

					BeforeEach(func() {
						for i, fn := range s.watches {
							watches = append(watches, fn(ctrl, int64(i), nil, nil, nil))
						}

						for _, i := range s.cancels {
							watches[i].Cancel(nil, nil)
						}

						for i, w := range watches {
							var canceled bool

							for _, j := range s.cancels {
								if canceled = i == j; canceled {
									break
								}
							}

							if !canceled {
								expectedIdenticalTos = append(expectedIdenticalTos, BeIdenticalTo(w))
							}
						}
					})

					AfterEach(func() {
						for _, w := range activeWatches {
							w.Cancel(nil, nil)
						}
					})

					It("should succeed", func() {
						activeWatches = getActiveWatches(watches)
						Expect(activeWatches).To(ConsistOf(expectedIdenticalTos...))
					})
				},
			)
		}(s)
	}
})

var _ = Describe("revisionWatcher", func() {
	var (
		ctrl *gomock.Controller
		rw   *revisionWatcher
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		rw = &revisionWatcher{}
	})

	Describe("next", func() {
		type spec struct {
			backend     *backend
			revision    int64
			changesOnly bool
			interval    *closedOpenInterval
			watches     []watchMakerFunc
			nrun        int
			events      []*mvccpb.Event
			cancels     []int
		}

		for _, s := range []spec{
			{},
			{
				backend:     &backend{},
				revision:    2,
				changesOnly: false,
				watches:     []watchMakerFunc{newMockWatch, newMockWatch},
				cancels:     []int{0, 1},
			},
			{
				backend:     &backend{},
				revision:    10,
				changesOnly: true,
				interval:    &closedOpenInterval{start: key("i"), end: key("k")},
				watches:     []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch},
				nrun:        5,
				events:      []*mvccpb.Event{{}},
				cancels:     []int{1},
			},
			{
				backend:     &backend{},
				revision:    20,
				changesOnly: false,
				interval:    &closedOpenInterval{start: key("i"), end: key("k")},
				watches:     []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch, newMockWatch, newMockWatch},
				nrun:        5,
				events:      []*mvccpb.Event{{}, {}},
				cancels:     []int{0, 2, 4},
			},
		} {
			func(s spec) {
				Describe(
					fmt.Sprintf(
						"backend=%v, revision=%d, changesOnly=%t, interval=%v, watches=%v, nrun=%d, events=%v cancels=%v",
						s.backend,
						s.revision,
						s.changesOnly,
						s.interval,
						s.watches,
						s.nrun,
						s.events,
						s.cancels,
					),
					func() {
						var (
							expectedIdenticalTos []interface{}
							arw                  *revisionWatcher
						)

						BeforeEach(func() {
							rw.backend = s.backend
							rw.revision = s.revision
							rw.changesOnly = s.changesOnly
							rw.interval = s.interval
							rw.watches = nil
							rw.nrun = s.nrun
							rw.events = s.events

							for i, fn := range s.watches {
								rw.watches = append(rw.watches, fn(ctrl, int64(i), nil, nil, nil))
							}

							for _, i := range s.cancels {
								rw.watches[i].Cancel(nil, nil)
							}

							for i, w := range rw.watches {
								var canceled bool

								for _, j := range s.cancels {
									if canceled = i == j; canceled {
										break
									}
								}

								if !canceled {
									expectedIdenticalTos = append(expectedIdenticalTos, BeIdenticalTo(w))
								}
							}
						})

						AfterEach(func() {
							for _, w := range arw.watches {
								w.Cancel(nil, nil)
							}
						})

						It("should succeed", func() {
							arw = rw.next()

							Expect(arw).ToNot(BeNil(), "revisionWatcher")
							Expect(arw.backend).To(BeIdenticalTo(rw.backend), "backend")
							Expect(arw.revision).To(Equal(rw.revision+1), "revision")
							Expect(arw.changesOnly).To(BeTrue(), "changesOnly")
							Expect(arw.interval).To(BeIdenticalTo(rw.interval), "interval")
							Expect(arw.watches).To(ConsistOf(expectedIdenticalTos...), "watches")
							Expect(arw.nrun).To(BeZero(), "nrun")
							Expect(arw.events).To(BeNil(), "events")
						})
					},
				)
			}(s)
		}
	})

	Describe("cancelAllWatches", func() {
		type spec struct {
			nilRW                        bool
			backend                      *backend
			ctxFn                        ContextFunc
			prepareRepo, prepareRevision bool
			watches                      []watchMakerFunc
			err                          error
			expectCancelFn               assertCancelFunc
		}

		var ctx context.Context

		const preparedRevision = int64(5)

		BeforeEach(func() {
			ctx = context.Background()
		})

		for _, s := range []spec{
			{nilRW: true},
			func() spec {
				var err = errors.New("error for cancel")
				return spec{
					backend:        &backend{},
					watches:        []watchMakerFunc{newMockWatch},
					err:            err,
					expectCancelFn: getExpectCancelFn("Cancel", Equal(&etcdserverpb.ResponseHeader{}), MatchError(err)),
				}
			}(),
			func() spec {
				var err = errors.New("error for cancel")
				return spec{
					backend:        &backend{metadataRefName: "refs/gitcd/metadata/main"},
					ctxFn:          CanceledContext,
					prepareRepo:    true,
					watches:        []watchMakerFunc{newMockWatch},
					err:            err,
					expectCancelFn: getExpectCancelFn("Cancel", Equal(&etcdserverpb.ResponseHeader{}), MatchError(err)),
				}
			}(),
			func() spec {
				var err = errors.New("error for cancel")
				return spec{
					backend:        &backend{metadataRefName: "refs/gitcd/metadata/main"},
					prepareRepo:    true,
					watches:        []watchMakerFunc{newMockWatch, newMockWatch},
					err:            err,
					expectCancelFn: getExpectCancelFn("Cancel", Equal(&etcdserverpb.ResponseHeader{}), MatchError(err)),
				}
			}(),
			func() spec {
				var err = errors.New("error for cancel")
				return spec{
					backend:         &backend{metadataRefName: "refs/gitcd/metadata/main"},
					prepareRepo:     true,
					prepareRevision: true,
					watches:         []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch},
					err:             err,
					expectCancelFn: getExpectCancelFn(
						"Cancel",
						Equal(&etcdserverpb.ResponseHeader{
							Revision: preparedRevision,
							RaftTerm: uint64(preparedRevision),
						}),
						MatchError(err),
					),
				}
			}(),
			func() spec {
				var err = errors.New("error for cancel")
				return spec{
					backend:         &backend{metadataRefName: "refs/gitcd/metadata/main"},
					ctxFn:           CanceledContext,
					prepareRepo:     true,
					prepareRevision: true,
					watches:         []watchMakerFunc{newMockWatch, newMockWatch},
					err:             err,
					expectCancelFn:  getExpectCancelFn("Cancel", Equal(&etcdserverpb.ResponseHeader{}), MatchError(err)),
				}
			}(),
			func() spec {
				var err = errors.New("error for cancel")
				return spec{
					backend: &backend{
						metadataRefName: "refs/gitcd/metadata/main",
						clusterID:       2,
						memberID:        10,
					},
					prepareRepo:     true,
					prepareRevision: true,
					watches:         []watchMakerFunc{newMockWatch, newMockWatch, newMockWatch, newMockWatch},
					err:             err,
					expectCancelFn: getExpectCancelFn(
						"Cancel",
						Equal(&etcdserverpb.ResponseHeader{
							ClusterId: 2,
							MemberId:  10,
							Revision:  preparedRevision,
							RaftTerm:  uint64(preparedRevision),
						}),
						MatchError(err),
					),
				}
			}(),
		} {
			func(s spec) {
				Describe(
					fmt.Sprintf(
						"nilWR=%t, backend= %v, context expired=%t, prepareRepo=%t, prepareRevision=%t, watches=%v, err=%v",
						s.nilRW, s.backend, s.ctxFn != nil, s.prepareRepo, s.prepareRevision, s.watches, s.err,
					),
					func() {
						var dir string

						BeforeEach(func() {
							rw.backend = s.backend

							if s.prepareRepo {
								var (
									gitImpl     = git2go.New()
									strRevision string
								)

								strRevision = revisionToString(preparedRevision)

								Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
								Expect(dir).ToNot(BeEmpty())

								Expect(func() (err error) { rw.backend.repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
								Expect(rw.backend.repo).ToNot(BeNil())

								if s.prepareRevision {
									Expect(CreateReferenceFromDef(ctx, rw.backend.repo, rw.backend.metadataRefName, &CommitDef{
										Message: strRevision,
										Tree: TreeDef{
											Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision)},
										},
									})).To(Succeed())
								}
							}

							for i, fn := range s.watches {
								rw.watches = append(rw.watches, fn(ctrl, int64(i), s.expectCancelFn, nil, nil))
							}

							if s.nilRW {
								rw = nil
							}

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}
						})

						AfterEach(func() {
							if rw != nil && rw.backend != nil && rw.backend.repo != nil {
								Expect(rw.backend.repo.Close()).To(Succeed())
							}

							if len(dir) > 0 {
								Expect(os.RemoveAll(dir))
							}
						})

						It("should succeed", func() {
							Expect(rw.cancelAllWatches(ctx, s.err)).To(BeIdenticalTo(rw), "revisionWatcher")
							if rw != nil {
								Expect(getActiveWatches(rw.watches)).To(BeNil(), "watches")
							}
						})
					},
				)
			}(s)
		}
	})

	Describe("backend", func() {
		var (
			ctx context.Context
			dir string
		)

		BeforeEach(func() {
			var gitImpl = git2go.New()

			rw.backend = &backend{}
			ctx = context.Background()

			Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
			Expect(dir).ToNot(BeEmpty())

			Expect(func() (err error) { rw.backend.repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
			Expect(rw.backend.repo).ToNot(BeNil())

			rw.backend.errors = gitImpl.Errors()
		})

		AfterEach(func() {
			if rw.backend.repo != nil {
				Expect(rw.backend.repo.Close()).To(Succeed())
			}

			if len(dir) > 0 {
				Expect(os.RemoveAll(dir))
			}
		})

		Describe("getRevisionAndPredecessorMetadata", func() {
			type spec struct {
				ctxFn                                                         ContextFunc
				metaRefName                                                   git.ReferenceName
				revision                                                      int64
				changesOnly                                                   bool
				metaHead                                                      *CommitDef
				matchErr, matchMetaRootDefPtr, matchMetaPredecessorRootDefPtr types.GomegaMatcher
			}

			var (
				mh = &CommitDef{
					Message: "5",
					Tree: TreeDef{
						Blobs: map[string][]byte{
							metadataPathRevision: []byte("5"),
							"5":                  []byte("5"),
						},
						Subtrees: map[string]TreeDef{
							"a": {Blobs: map[string][]byte{"5": []byte("5")}},
						},
					},
					Parents: []CommitDef{
						{
							Message: "1",
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte("1"),
									"51":                 []byte("51"),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{"51": []byte("51")}},
								},
							},
						},
						{
							Message: "2",
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte("2"),
									"52":                 []byte("52"),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{"52": []byte("52")}},
								},
							},
							Parents: []CommitDef{{
								Message: "1",
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte("1"),
										"521":                []byte("521"),
									},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{"521": []byte("521")}},
									},
								},
							}},
						},
						{
							Message: "3",
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte("3"),
									"53":                 []byte("53"),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{"53": []byte("53")}},
								},
							},
							Parents: []CommitDef{
								{
									Message: "1",
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte("1"),
											"531":                []byte("531"),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"531": []byte("531")}},
										},
									},
								},
								{
									Message: "2",
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte("2"),
											"532":                []byte("532"),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"532": []byte("532")}},
										},
									},
									Parents: []CommitDef{{
										Message: "1",
										Tree: TreeDef{
											Blobs: map[string][]byte{
												metadataPathRevision: []byte("1"),
												"5321":               []byte("5321"),
											},
											Subtrees: map[string]TreeDef{
												"a": {Blobs: map[string][]byte{"5321": []byte("5321")}},
											},
										},
									}},
								},
							},
						},
						{
							Message: "4",
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte("4"),
									"54":                 []byte("54"),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{"54": []byte("54")}},
								},
							},
							Parents: []CommitDef{
								{
									Message: "1",
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte("1"),
											"541":                []byte("541"),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"541": []byte("541")}},
										},
									},
								},
								{
									Message: "2",
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte("2"),
											"542":                []byte("542"),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"542": []byte("542")}},
										},
									},
									Parents: []CommitDef{{
										Message: "1",
										Tree: TreeDef{
											Blobs: map[string][]byte{
												metadataPathRevision: []byte("1"),
												"5421":               []byte("5421"),
											},
											Subtrees: map[string]TreeDef{
												"a": {Blobs: map[string][]byte{"5421": []byte("5421")}},
											},
										},
									}},
								},
								{
									Message: "3",
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte("3"),
											"543":                []byte("543"),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"543": []byte("543")}},
										},
									},
									Parents: []CommitDef{
										{
											Message: "1",
											Tree: TreeDef{
												Blobs: map[string][]byte{
													metadataPathRevision: []byte("1"),
													"5431":               []byte("5431"),
												},
												Subtrees: map[string]TreeDef{
													"a": {Blobs: map[string][]byte{"5431": []byte("5431")}},
												},
											},
										},
										{
											Message: "2",
											Tree: TreeDef{
												Blobs: map[string][]byte{
													metadataPathRevision: []byte("2"),
													"5432":               []byte("5432"),
												},
												Subtrees: map[string]TreeDef{
													"a": {Blobs: map[string][]byte{"5432": []byte("5432")}},
												},
											},
											Parents: []CommitDef{{
												Message: "1",
												Tree: TreeDef{
													Blobs: map[string][]byte{
														metadataPathRevision: []byte("1"),
														"54321":              []byte("54321"),
													},
													Subtrees: map[string]TreeDef{
														"a": {Blobs: map[string][]byte{"54321": []byte("54321")}},
													},
												},
											}},
										},
									},
								},
							},
						},
					},
				}
			)

			for _, s := range []spec{
				{
					matchErr:                       MatchError(rpctypes.ErrGRPCCorrupt),
					matchMetaRootDefPtr:            BeNil(),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					ctxFn:                          CanceledContext,
					metaRefName:                    "refs/gitcd/metadata/main",
					matchErr:                       MatchError(context.Canceled),
					matchMetaRootDefPtr:            BeNil(),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					metaRefName:                    "refs/gitcd/metadata/main",
					matchErr:                       PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
					matchMetaRootDefPtr:            BeNil(),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					metaRefName:                    "refs/gitcd/metadata/main",
					metaHead:                       &CommitDef{Message: "1"},
					matchErr:                       PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
					matchMetaRootDefPtr:            BeNil(),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					metaRefName: "refs/gitcd/metadata/main",
					revision:    4,
					metaHead: &CommitDef{
						Message: "3",
						Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
					},
					matchErr:                       MatchError(rpctypes.ErrGRPCFutureRev),
					matchMetaRootDefPtr:            BeNil(),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					metaRefName: "refs/gitcd/metadata/main",
					revision:    1,
					metaHead: &CommitDef{
						Message: "3",
						Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
						Parents: []CommitDef{{
							Message: "2",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}},
						}},
					},
					matchErr:                       MatchError(rpctypes.ErrGRPCCompacted),
					matchMetaRootDefPtr:            BeNil(),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				func() spec {
					var (
						revision    = int64(0)
						strRevision = revisionToString(revision)
						mh          = &CommitDef{
							Message: strRevision,
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision),
									strRevision:          []byte(strRevision),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision: []byte(strRevision)}},
								},
							},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Tree)),
						matchMetaPredecessorRootDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						revision    = int64(-1)
						strRevision = revisionToString(revision)
						mh          = &CommitDef{
							Message: strRevision,
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision),
									strRevision:          []byte(strRevision),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision: []byte(strRevision)}},
								},
							},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Tree)),
						matchMetaPredecessorRootDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						revision    = int64(3)
						strRevision = revisionToString(revision)
						mh          = &CommitDef{
							Message: strRevision,
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision),
									strRevision:          []byte(strRevision),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision: []byte(strRevision)}},
								},
							},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Tree)),
						matchMetaPredecessorRootDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						revision3    = int64(3)
						revision2    = revision3 - 1
						revision1    = revision2 - 1
						strRevision3 = revisionToString(revision3)
						strRevision2 = revisionToString(revision2)
						strRevision1 = revisionToString(revision1)

						mh = &CommitDef{
							Message: strRevision3,
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision3),
									strRevision3:         []byte(strRevision3),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision3: []byte(strRevision3)}},
								},
							},
							Parents: []CommitDef{{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										strRevision2:         []byte(strRevision2),
									},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{strRevision2: []byte(strRevision2)}},
									},
								},
								Parents: []CommitDef{{Message: strRevision1}},
							}},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision2,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[0].Tree)),
						matchMetaPredecessorRootDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						revision3    = int64(3)
						revision2    = revision3 - 1
						revision1    = revision2 - 1
						strRevision3 = revisionToString(revision3)
						strRevision2 = revisionToString(revision2)
						strRevision1 = revisionToString(revision1)

						mh = &CommitDef{
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision3),
									strRevision3:         []byte(strRevision3),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision3: []byte(strRevision3)}},
								},
							},
							Parents: []CommitDef{{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										strRevision2:         []byte(strRevision2),
									},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{strRevision2: []byte(strRevision2)}},
									},
								},
								Parents: []CommitDef{{Message: strRevision1}},
							}},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision2,
						changesOnly:                    true,
						metaHead:                       mh,
						matchErr:                       PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[0].Tree)),
						matchMetaPredecessorRootDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						revision3    = int64(3)
						revision2    = revision3 - 1
						revision1    = revision2 - 1
						strRevision3 = revisionToString(revision3)
						strRevision2 = revisionToString(revision2)
						strRevision1 = revisionToString(revision1)

						mh = &CommitDef{
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision3),
									strRevision3:         []byte(strRevision3),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision3: []byte(strRevision3)}},
								},
							},
							Parents: []CommitDef{{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										strRevision2:         []byte(strRevision2),
									},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{strRevision2: []byte(strRevision2)}},
									},
								},
								Parents: []CommitDef{{
									Message: strRevision1,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strRevision1),
											strRevision1:         []byte(strRevision1),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{strRevision1: []byte(strRevision1)}},
										},
									},
								}},
							}},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision2,
						changesOnly:                    true,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[0].Tree)),
						matchMetaPredecessorRootDefPtr: PointTo(GetTreeDefMatcher(&mh.Parents[0].Parents[0].Tree)),
					}
				}(),
				func() spec {
					var (
						revision3    = int64(1)
						revision2    = revision3 - 1
						revision1    = revision2 - 1
						strRevision3 = revisionToString(revision3)
						strRevision2 = revisionToString(revision2)
						strRevision1 = revisionToString(revision1)

						mh = &CommitDef{
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision3),
									strRevision3:         []byte(strRevision3),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision3: []byte(strRevision3)}},
								},
							},
							Parents: []CommitDef{{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										strRevision2:         []byte(strRevision2),
									},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{strRevision2: []byte(strRevision2)}},
									},
								},
								Parents: []CommitDef{{
									Message: strRevision1,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strRevision1),
											strRevision1:         []byte(strRevision1),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{strRevision1: []byte(strRevision1)}},
										},
									},
								}},
							}},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision2,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[0].Tree)),
						matchMetaPredecessorRootDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						revision3    = int64(1)
						revision2    = revision3 - 1
						revision1    = revision2 - 1
						strRevision3 = revisionToString(revision3)
						strRevision2 = revisionToString(revision2)
						strRevision1 = revisionToString(revision1)

						mh = &CommitDef{
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision3),
									strRevision3:         []byte(strRevision3),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision3: []byte(strRevision3)}},
								},
							},
							Parents: []CommitDef{{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										strRevision2:         []byte(strRevision2),
									},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{strRevision2: []byte(strRevision2)}},
									},
								},
								Parents: []CommitDef{{
									Message: strRevision1,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strRevision1),
											strRevision1:         []byte(strRevision1),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{strRevision1: []byte(strRevision1)}},
										},
									},
								}},
							}},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision2,
						changesOnly:                    true,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[0].Tree)),
						matchMetaPredecessorRootDefPtr: BeNil(),
					}
				}(),
				{
					metaRefName:                    "refs/gitcd/metadata/main",
					revision:                       1,
					metaHead:                       mh,
					matchErr:                       Succeed(),
					matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[0].Tree)),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					metaRefName:                    "refs/gitcd/metadata/main",
					revision:                       1,
					changesOnly:                    true,
					metaHead:                       mh,
					matchErr:                       Succeed(),
					matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[0].Tree)),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					metaRefName:                    "refs/gitcd/metadata/main",
					revision:                       3,
					metaHead:                       mh,
					matchErr:                       Succeed(),
					matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[2].Tree)),
					matchMetaPredecessorRootDefPtr: BeNil(),
				},
				{
					metaRefName:                    "refs/gitcd/metadata/main",
					revision:                       3,
					changesOnly:                    true,
					metaHead:                       mh,
					matchErr:                       Succeed(),
					matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[2].Tree)),
					matchMetaPredecessorRootDefPtr: PointTo(GetTreeDefMatcher(&mh.Parents[2].Parents[1].Tree)),
				},
				{
					metaRefName:                    "refs/gitcd/metadata/main",
					revision:                       4,
					changesOnly:                    true,
					metaHead:                       mh,
					matchErr:                       Succeed(),
					matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Parents[3].Tree)),
					matchMetaPredecessorRootDefPtr: PointTo(GetTreeDefMatcher(&mh.Parents[3].Parents[2].Tree)),
				},
				func() spec {
					var (
						revision4    = int64(4)
						revision3    = revision4 - 1
						revision2    = revision3 - 1
						revision1    = revision2 - 1
						strRevision4 = revisionToString(revision4)
						strRevision2 = revisionToString(revision2)
						strRevision1 = revisionToString(revision1)

						mh = &CommitDef{
							Message: strRevision4,
							Tree: TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision4),
									strRevision4:         []byte(strRevision4),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{strRevision4: []byte(strRevision4)}},
								},
							},
							Parents: []CommitDef{
								{
									Message: strRevision1,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strRevision1),
											strRevision1:         []byte(strRevision1),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{strRevision1: []byte(strRevision1)}},
										},
									},
								},
								{
									Message: strRevision2,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strRevision2),
											strRevision2:         []byte(strRevision2),
										},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{strRevision2: []byte(strRevision2)}},
										},
									},
								},
							},
						}
					)

					return spec{
						metaRefName:                    "refs/gitcd/metadata/main",
						revision:                       revision4,
						changesOnly:                    true,
						metaHead:                       mh,
						matchErr:                       Succeed(),
						matchMetaRootDefPtr:            PointTo(GetTreeDefMatcher(&mh.Tree)),
						matchMetaPredecessorRootDefPtr: PointTo(GetTreeDefMatcher(&mh.Parents[1].Tree)),
					}
				}(),
			} {
				func(s spec) {
					Describe(
						fmt.Sprintf(
							"expired context=%t, metaRefName=%s, revision=%d, changesOnly=%t, metaHead=%v",
							s.ctxFn != nil, s.metaRefName, s.revision, s.changesOnly, s.metaHead,
						),
						func() {
							var (
								parentCtx context.Context

								treeDefIfExists = func(ctx context.Context, repo git.Repository, t git.Tree) *TreeDef {
									if t == nil {
										return nil
									}

									return GetTreeDefByTree(ctx, repo, t)
								}
							)

							BeforeEach(func() {
								rw.backend.metadataRefName = s.metaRefName
								rw.revision = s.revision
								rw.changesOnly = s.changesOnly

								if s.metaHead != nil {
									Expect(CreateReferenceFromDef(ctx, rw.backend.repo, s.metaRefName, s.metaHead)).To(Succeed())
								}

								parentCtx = ctx
								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var metaRoot, metaPredecessorRoot, err = rw.getRevisionAndPredecessorMetadata(ctx)

								Expect(err).To(s.matchErr, "err")
								Expect(treeDefIfExists(parentCtx, rw.backend.repo, metaRoot)).To(s.matchMetaRootDefPtr, "metaRoot")
								Expect(treeDefIfExists(parentCtx, rw.backend.repo, metaPredecessorRoot)).To(
									s.matchMetaPredecessorRootDefPtr, "metaPredecessorRoot",
								)
							})
						},
					)
				}(s)
			}
		})

		Describe("getDataRootForMetadata", func() {
			type spec struct {
				ctxFn                             ContextFunc
				metaHead, dataHead                *CommitDef
				matchErr, matchDataRootTreeDefPtr types.GomegaMatcher
			}

			for _, s := range []spec{
				{
					ctxFn:                   CanceledContext,
					metaHead:                &CommitDef{},
					matchErr:                MatchError(context.Canceled),
					matchDataRootTreeDefPtr: BeNil(),
				},
				{
					ctxFn:                   CanceledContext,
					metaHead:                &CommitDef{},
					dataHead:                &CommitDef{},
					matchErr:                MatchError(context.Canceled),
					matchDataRootTreeDefPtr: BeNil(),
				},
				{
					metaHead:                &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}},
					matchErr:                PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
					matchDataRootTreeDefPtr: BeNil(),
				},
				func() spec {
					var dcd = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}

					return spec{
						metaHead:                &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}},
						dataHead:                dcd,
						matchErr:                Succeed(),
						matchDataRootTreeDefPtr: PointTo(GetTreeDefMatcher(&dcd.Tree)),
					}
				}(),
			} {
				func(s spec) {
					Describe(
						fmt.Sprintf(
							"expired context=%t, metaHead=%t, dataHead=%t",
							s.ctxFn != nil, s.metaHead != nil, s.dataHead != nil,
						),
						func() {
							var (
								metaRoot  git.Tree
								parentCtx context.Context
							)

							BeforeEach(func() {
								var metaHead git.Commit

								Expect(func() (err error) {
									metaHead, err = metaHeadFrom(s.metaHead, s.dataHead)(ctx, rw.backend.repo)
									return
								}()).To(Succeed())

								defer metaHead.Close()

								Expect(func() (err error) {
									metaRoot, err = rw.backend.repo.Peeler().PeelToTree(ctx, metaHead)
									return
								}()).To(Succeed())

								parentCtx = ctx
								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							AfterEach(func() {
								if metaRoot != nil {
									Expect(metaRoot.Close()).To(Succeed())
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var dataRoot, err = rw.getDataRootForMetadata(ctx, metaRoot)

								if dataRoot != nil {
									defer dataRoot.Close()
								}

								Expect(err).To(s.matchErr, "err")
								Expect(func() *TreeDef {
									if dataRoot == nil {
										return nil
									}

									return GetTreeDefByTree(parentCtx, rw.backend.repo, dataRoot)
								}()).To(s.matchDataRootTreeDefPtr, "dataRoot")
							})
						},
					)
				}(s)
			}
		})

		Describe("getDeletionKeyValueLoader", func() {
			var (
				err = errors.New("prevKV error")
				kv  = &mvccpb.KeyValue{
					Key:            []byte("some/key"),
					CreateRevision: 10,
					ModRevision:    11,
					Lease:          12,
					Version:        13,
					Value:          []byte("some value"),
				}
			)

			for _, s := range []struct {
				revision          int64
				prevKV            *mvccpb.KeyValue
				prevKVErr         error
				matchErr, matchKV types.GomegaMatcher
			}{
				{matchErr: Succeed(), matchKV: BeNil()},
				{prevKVErr: err, matchErr: MatchError(err), matchKV: BeNil()},
				{
					prevKV:   kv,
					matchErr: Succeed(),
					matchKV:  Equal(&mvccpb.KeyValue{Key: kv.Key, CreateRevision: kv.CreateRevision}),
				},
				{
					prevKV:    kv,
					prevKVErr: err,
					matchErr:  MatchError(err),
					matchKV:   Equal(&mvccpb.KeyValue{Key: kv.Key, CreateRevision: kv.CreateRevision}),
				},
				{
					revision: 100,
					prevKV:   kv,
					matchErr: Succeed(),
					matchKV:  Equal(&mvccpb.KeyValue{Key: kv.Key, CreateRevision: kv.CreateRevision, ModRevision: 100}),
				},
				{
					revision:  100,
					prevKV:    kv,
					prevKVErr: err,
					matchErr:  MatchError(err),
					matchKV:   Equal(&mvccpb.KeyValue{Key: kv.Key, CreateRevision: kv.CreateRevision, ModRevision: 100}),
				},
			} {
				func(revision int64, prevKV *mvccpb.KeyValue, prevKVErr error, matchErr, matchKV types.GomegaMatcher) {
					Describe(fmt.Sprintf("prevKV=%v, prevKVErr=%v", s.prevKV, s.prevKVErr), func() {
						BeforeEach(func() {
							rw.revision = revision
						})

						It(ItSpecForMatchError(matchErr), func() {
							var kv, err = rw.getDeletionKeyValueLoader(func() (*mvccpb.KeyValue, error) {
								return prevKV, prevKVErr
							})()

							Expect(err).To(matchErr, "err")
							Expect(kv).To(matchKV, "kv")
						})
					})
				}(s.revision, s.prevKV, s.prevKVErr, s.matchErr, s.matchKV)
			}
		})

		Describe("mixed diff", func() {
			var (
				revision2    = int64(3)
				revision1    = revision2 - 1
				revision0    = revision1 - 1
				strRevision2 = []byte(revisionToString(revision2))
				strRevision1 = []byte(revisionToString(revision1))
				strRevision0 = []byte(revisionToString(revision0))

				mhAll = &CommitDef{
					Message: string(strRevision2),
					Tree: TreeDef{
						Blobs: map[string][]byte{metadataPathRevision: strRevision2},
						Subtrees: map[string]TreeDef{
							"shallow-add": {Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  strRevision2,
								etcdserverpb.Compare_MOD.String():     strRevision2,
								etcdserverpb.Compare_VERSION.String(): strRevision2,
							}},
							"shallow-delete": {Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  strRevision2,
								etcdserverpb.Compare_LEASE.String():   strRevision2,
								etcdserverpb.Compare_VERSION.String(): strRevision2,
							}},
							"shallow-update": {Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  strRevision1,
								etcdserverpb.Compare_MOD.String():     strRevision2,
								etcdserverpb.Compare_LEASE.String():   strRevision2,
								etcdserverpb.Compare_VERSION.String(): strRevision2,
							}},
							"shallow-update-mod-unmodified": {Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  strRevision2,
								etcdserverpb.Compare_MOD.String():     strRevision1,
								etcdserverpb.Compare_LEASE.String():   strRevision2,
								etcdserverpb.Compare_VERSION.String(): strRevision2,
							}},
							"shallow-blob-to-tree": {Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
								}},
								"b": {Subtrees: map[string]TreeDef{
									"2": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision2,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
								}},
							}},
							"shallow-tree-to-blob": {Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  strRevision2,
								etcdserverpb.Compare_MOD.String():     strRevision2,
								etcdserverpb.Compare_LEASE.String():   strRevision2,
								etcdserverpb.Compare_VERSION.String(): strRevision2,
							}},
							"z": {Subtrees: map[string]TreeDef{
								"deep": {Subtrees: map[string]TreeDef{
									"add": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
									"delete": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
									"update": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision1,
										etcdserverpb.Compare_MOD.String():     strRevision2,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
									"update-mod-unmodified": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision1,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
									"blob-to-tree": {Subtrees: map[string]TreeDef{
										"a": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision2,
												etcdserverpb.Compare_MOD.String():     strRevision2,
												etcdserverpb.Compare_VERSION.String(): strRevision2,
											}},
										}},
										"b": {Subtrees: map[string]TreeDef{
											"2": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision2,
												etcdserverpb.Compare_MOD.String():     strRevision2,
												etcdserverpb.Compare_LEASE.String():   strRevision2,
												etcdserverpb.Compare_VERSION.String(): strRevision2,
											}},
										}},
									}},
									"tree-to-blob": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision2,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
								}},
							}},
						},
					},
					Parents: []CommitDef{{
						Message: string(strRevision1),
						Tree: TreeDef{
							Blobs: map[string][]byte{metadataPathRevision: strRevision2},
							Subtrees: map[string]TreeDef{
								"shallow-delete": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_LEASE.String():   strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
								"shallow-update": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_LEASE.String():   strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
								"shallow-update-mod-unmodified": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_LEASE.String():   strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
								"shallow-blob-to-tree": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
								"shallow-tree-to-blob": {Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision1,
											etcdserverpb.Compare_MOD.String():     strRevision1,
											etcdserverpb.Compare_LEASE.String():   strRevision1,
											etcdserverpb.Compare_VERSION.String(): strRevision1,
										}},
									}},
									"b": {Subtrees: map[string]TreeDef{
										"2": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision1,
											etcdserverpb.Compare_MOD.String():     strRevision1,
											etcdserverpb.Compare_VERSION.String(): strRevision1,
										}},
									}},
								}},
								"z": {Subtrees: map[string]TreeDef{
									"deep": {Subtrees: map[string]TreeDef{
										"delete": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision1,
											etcdserverpb.Compare_MOD.String():     strRevision1,
											etcdserverpb.Compare_LEASE.String():   strRevision1,
											etcdserverpb.Compare_VERSION.String(): strRevision1,
										}},
										"update": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision1,
											etcdserverpb.Compare_MOD.String():     strRevision1,
											etcdserverpb.Compare_LEASE.String():   strRevision1,
											etcdserverpb.Compare_VERSION.String(): strRevision1,
										}},
										"update-mod-unmodified": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision1,
											etcdserverpb.Compare_MOD.String():     strRevision1,
											etcdserverpb.Compare_LEASE.String():   strRevision1,
											etcdserverpb.Compare_VERSION.String(): strRevision1,
										}},
										"blob-to-tree": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision1,
											etcdserverpb.Compare_MOD.String():     strRevision1,
											etcdserverpb.Compare_VERSION.String(): strRevision1,
										}},
										"tree-to-blob": {Subtrees: map[string]TreeDef{
											"a": {Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  strRevision1,
													etcdserverpb.Compare_MOD.String():     strRevision1,
													etcdserverpb.Compare_LEASE.String():   strRevision1,
													etcdserverpb.Compare_VERSION.String(): strRevision1,
												}},
											}},
											"b": {Subtrees: map[string]TreeDef{
												"2": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  strRevision1,
													etcdserverpb.Compare_MOD.String():     strRevision1,
													etcdserverpb.Compare_VERSION.String(): strRevision1,
												}},
											}},
										}},
									}},
								}},
							},
						},
						Parents: []CommitDef{{Message: string(strRevision0)}},
					}},
				}

				dhAll = &CommitDef{
					Message: string(strRevision2),
					Tree: TreeDef{
						Blobs: map[string][]byte{
							"shallow-add":                   []byte("shallow-add"),
							"shallow-delete":                []byte("shallow-delete-1"),
							"shallow-update":                []byte("shallow-update-1"),
							"shallow-update-mod-unmodified": []byte("shallow-update-mod-unmodified-1"),
							"shallow-tree-to-blob":          []byte("shallow-tree-to-blob"),
						},
						Subtrees: map[string]TreeDef{
							"shallow-blob-to-tree": {Subtrees: map[string]TreeDef{
								"a": {Blobs: map[string][]byte{"1": []byte("1")}},
								"b": {Blobs: map[string][]byte{"2": []byte("2")}},
							}},
							"z": {Subtrees: map[string]TreeDef{
								"deep": {
									Blobs: map[string][]byte{
										"add":                   []byte("add"),
										"delete":                []byte("delete-1"),
										"update":                []byte("update-1"),
										"update-mod-unmodified": []byte("update-mod-unmodified-1"),
										"tree-to-blob":          []byte("tree-to-blob"),
									},
									Subtrees: map[string]TreeDef{
										"blob-to-tree": {Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"1": []byte("1")}},
											"b": {Blobs: map[string][]byte{"2": []byte("2")}},
										}},
									},
								},
							}},
						},
					},
					Parents: []CommitDef{{
						Message: string(strRevision1),
						Tree: TreeDef{
							Blobs: map[string][]byte{
								"shallow-delete":                []byte("shallow-delete"),
								"shallow-update":                []byte("shallow-update"),
								"shallow-update-mod-unmodified": []byte("shallow-update-mod-unmodified"),
								"shallow-blob-to-tree":          []byte("shallow-blob-to-tree"),
							},
							Subtrees: map[string]TreeDef{
								"shallow-tree-to-blob": {Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{"1": []byte("1")}},
									"b": {Blobs: map[string][]byte{"2": []byte("2")}},
								}},
								"z": {Subtrees: map[string]TreeDef{
									"deep": {
										Blobs: map[string][]byte{
											"delete":                []byte("delete"),
											"update":                []byte("update"),
											"update-mod-unmodified": []byte("update-mod-unmodified"),
											"blob-to-tree":          []byte("blob-to-tree"),
										},
										Subtrees: map[string]TreeDef{
											"tree-to-blob": {Subtrees: map[string]TreeDef{
												"a": {Blobs: map[string][]byte{"1": []byte("1")}},
												"b": {Blobs: map[string][]byte{"2": []byte("2")}},
											}},
										},
									},
								}},
							},
						},
						Parents: []CommitDef{{Message: string(strRevision0)}},
					}},
				}

				eventsChangesOnlyAll = []*mvccpb.Event{
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-blob-to-tree"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("shallow-blob-to-tree"),
							CreateRevision: revision1,
							ModRevision:    revision1,
							Value:          []byte("shallow-blob-to-tree"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-delete"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("shallow-delete"),
							CreateRevision: revision1,
							Lease:          revision1,
							ModRevision:    revision1,
							Value:          []byte("shallow-delete"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-tree-to-blob/a/1"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("shallow-tree-to-blob/a/1"),
							CreateRevision: revision1,
							Lease:          revision1,
							ModRevision:    revision1,
							Value:          []byte("1"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-tree-to-blob/b/2"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("shallow-tree-to-blob/b/2"),
							CreateRevision: revision1,
							ModRevision:    revision1,
							Value:          []byte("2"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/blob-to-tree"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/blob-to-tree"),
							CreateRevision: revision1,
							ModRevision:    revision1,
							Value:          []byte("blob-to-tree"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/delete"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/delete"),
							CreateRevision: revision1,
							Lease:          revision1,
							ModRevision:    revision1,
							Value:          []byte("delete"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/tree-to-blob/a/1"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/tree-to-blob/a/1"),
							CreateRevision: revision1,
							Lease:          revision1,
							ModRevision:    revision1,
							Value:          []byte("1"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.DELETE,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/tree-to-blob/b/2"),
							CreateRevision: revision1,
							ModRevision:    revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/tree-to-blob/b/2"),
							CreateRevision: revision1,
							ModRevision:    revision1,
							Value:          []byte("2"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-add"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("shallow-add"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-blob-to-tree/a/1"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("1"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-blob-to-tree/b/2"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("2"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-tree-to-blob"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("shallow-tree-to-blob"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-update"),
							CreateRevision: revision1,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("shallow-update-1"),
							Version:        revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("shallow-update"),
							CreateRevision: revision1,
							Lease:          revision1,
							ModRevision:    revision1,
							Value:          []byte("shallow-update"),
							Version:        revision1,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/add"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("add"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/blob-to-tree/a/1"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("1"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/blob-to-tree/b/2"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("2"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/tree-to-blob"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("tree-to-blob"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/update"),
							CreateRevision: revision1,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("update-1"),
							Version:        revision2,
						},
						PrevKv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/update"),
							CreateRevision: revision1,
							Lease:          revision1,
							ModRevision:    revision1,
							Value:          []byte("update"),
							Version:        revision1,
						},
					},
				}

				eventsNoChangesOnlyAll = []*mvccpb.Event{
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-add"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("shallow-add"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-blob-to-tree/a/1"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("1"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-blob-to-tree/b/2"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("2"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-tree-to-blob"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("shallow-tree-to-blob"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-update-mod-unmodified"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision1,
							Value:          []byte("shallow-update-mod-unmodified-1"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("shallow-update"),
							CreateRevision: revision1,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("shallow-update-1"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/add"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("add"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/blob-to-tree/a/1"),
							CreateRevision: revision2,
							ModRevision:    revision2,
							Value:          []byte("1"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/blob-to-tree/b/2"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("2"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/tree-to-blob"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("tree-to-blob"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/update-mod-unmodified"),
							CreateRevision: revision2,
							Lease:          revision2,
							ModRevision:    revision1,
							Value:          []byte("update-mod-unmodified-1"),
							Version:        revision2,
						},
					},
					{
						Type: mvccpb.PUT,
						Kv: &mvccpb.KeyValue{
							Key:            []byte("z/deep/update"),
							CreateRevision: revision1,
							Lease:          revision2,
							ModRevision:    revision2,
							Value:          []byte("update-1"),
							Version:        revision2,
						},
					},
				}
			)

			Describe("loadEvents", func() {
				type spec struct {
					spec                  string
					ctxFn                 ContextFunc
					keyPrefix             string
					nmt, ndt, omt, odt    *TreeDef
					revision              int64
					events                []*mvccpb.Event
					matchErr, matchEvents types.GomegaMatcher
				}

				for _, s := range []spec{
					{
						spec:        "nil trees",
						ctxFn:       CanceledContext,
						matchErr:    Succeed(),
						matchEvents: BeNil(),
					},
					func() spec {
						var events = []*mvccpb.Event{{}}

						return spec{
							spec:        "nil trees",
							ctxFn:       CanceledContext,
							events:      events,
							matchErr:    Succeed(),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var td = &TreeDef{}

						return spec{
							spec:        "nil metadata trees, empty data trees",
							ctxFn:       CanceledContext,
							ndt:         td,
							odt:         td,
							matchErr:    Succeed(),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var td = &TreeDef{}

						return spec{
							spec:        "only empty new metadata",
							ctxFn:       CanceledContext,
							nmt:         td,
							matchErr:    MatchError(context.Canceled),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							td     = &TreeDef{}
							events = []*mvccpb.Event{{}}
						)

						return spec{
							spec:        "only empty new metadata",
							ctxFn:       CanceledContext,
							omt:         td,
							events:      events,
							matchErr:    MatchError(context.Canceled),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							td     = &TreeDef{}
							events = []*mvccpb.Event{{}}
						)

						return spec{
							spec:        "empty metadata trees",
							ctxFn:       CanceledContext,
							nmt:         td,
							omt:         td,
							events:      events,
							matchErr:    MatchError(context.Canceled),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							strRevision1 = []byte("1")
							strRevision2 = []byte("2")
						)

						return spec{
							spec: "ModVersion unmodified",
							nmt: &TreeDef{
								Blobs: map[string][]byte{
									"1": []byte("11"),
									"2": []byte("21"),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision1,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
									"b": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision1,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
								},
							},
							omt: &TreeDef{
								Blobs: map[string][]byte{
									"1": []byte("1"),
									"2": []byte("2"),
								},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision1,
										etcdserverpb.Compare_MOD.String():     strRevision1,
										etcdserverpb.Compare_LEASE.String():   strRevision1,
										etcdserverpb.Compare_VERSION.String(): strRevision1,
									}},
									"b": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision1,
										etcdserverpb.Compare_MOD.String():     strRevision1,
										etcdserverpb.Compare_LEASE.String():   strRevision1,
										etcdserverpb.Compare_VERSION.String(): strRevision1,
									}},
								},
							},
							matchErr:    Succeed(),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							strRevision1 = []byte("1")
							events       = []*mvccpb.Event{{}}
						)

						return spec{
							spec: "shallow add, missing value",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							},
							}, ndt: &TreeDef{},
							events:      events,
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var strRevision1 = []byte("1")

						return spec{
							spec: "shallow add, missing CreateVersion",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							ndt:         &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var strRevision1 = []byte("1")

						return spec{
							spec: "shallow add, missing Version",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String(): strRevision1,
									etcdserverpb.Compare_MOD.String():    strRevision1,
								}},
							}},
							ndt:         &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
						)

						return spec{
							spec: "shallow add, missing Lease",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision,
									etcdserverpb.Compare_MOD.String():     strRevision,
									etcdserverpb.Compare_VERSION.String(): strRevision,
								}},
							}},
							ndt:      &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{{
								Type: mvccpb.PUT,
								Kv: &mvccpb.KeyValue{
									Key:            []byte("1"),
									CreateRevision: revision,
									ModRevision:    revision,
									Value:          []byte("1"),
									Version:        revision,
								},
							}}),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
						)

						return spec{
							spec:      "shallow add, full metadata",
							keyPrefix: "/registry/custom",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision,
									etcdserverpb.Compare_MOD.String():     strRevision,
									etcdserverpb.Compare_LEASE.String():   strRevision,
									etcdserverpb.Compare_VERSION.String(): strRevision,
								}},
							}},
							ndt:      &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							events:   []*mvccpb.Event{{}},
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{
								{},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("/registry/custom/1"),
										CreateRevision: revision,
										Lease:          revision,
										ModRevision:    revision,
										Value:          []byte("1"),
										Version:        revision,
									},
								},
							}),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
						)

						return spec{
							spec: "deep add, missing Version",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String(): strRevision,
											etcdserverpb.Compare_MOD.String():    strRevision,
											etcdserverpb.Compare_LEASE.String():  strRevision,
										}},
									}},
								}},
							}},
							ndt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
							events      = []*mvccpb.Event{{}}
						)

						return spec{
							spec: "deep add, missing value",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision,
											etcdserverpb.Compare_MOD.String():     strRevision,
											etcdserverpb.Compare_LEASE.String():   strRevision,
											etcdserverpb.Compare_VERSION.String(): strRevision,
										}},
									}},
								}},
							}},
							ndt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"c": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}},
							events:      events,
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							revision    = int64(4)
							strRevision = []byte(revisionToString(revision))
						)

						return spec{
							spec:      "deep add, full metadata",
							keyPrefix: "registry",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision,
											etcdserverpb.Compare_MOD.String():     strRevision,
											etcdserverpb.Compare_LEASE.String():   strRevision,
											etcdserverpb.Compare_VERSION.String(): strRevision,
										}},
									}},
								}},
							}},
							ndt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}},
							revision: 10,
							events:   []*mvccpb.Event{{}},
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{
								{},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("registry/a/b/1"),
										CreateRevision: revision,
										Lease:          revision,
										ModRevision:    revision,
										Value:          []byte("1"),
										Version:        revision,
									},
								},
							}),
						}
					}(),
					func() spec {
						var strRevision1 = []byte("1")

						return spec{
							spec: "shallow delete, missing value",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							ndt: &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							odt:         &TreeDef{},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							strRevision1 = []byte("1")
							emptyTreeDef = &TreeDef{}
						)

						return spec{
							spec: "shallow delete, missing CreateVersion",
							nmt:  emptyTreeDef,
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							odt:         emptyTreeDef,
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var strRevision1 = []byte("1")

						return spec{
							spec: "shallow delete, missing Version",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String(): strRevision1,
									etcdserverpb.Compare_MOD.String():    strRevision1,
								}},
							}},
							ndt:         &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
							events      = []*mvccpb.Event{{}}
						)

						return spec{
							spec: "shallow delete, ModVersion unmoidifed",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_MOD.String(): strRevision,
								}},
							}},
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision,
									etcdserverpb.Compare_MOD.String():     strRevision,
									etcdserverpb.Compare_VERSION.String(): strRevision,
								}},
							}},
							events:      events,
							matchErr:    Succeed(),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
						)

						return spec{
							spec: "shallow delete, missing Lease",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision,
									etcdserverpb.Compare_VERSION.String(): strRevision,
								}},
							}},
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision,
									etcdserverpb.Compare_MOD.String():     strRevision,
									etcdserverpb.Compare_VERSION.String(): strRevision,
								}},
							}},
							odt:      &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{{
								Type: mvccpb.DELETE,
								Kv: &mvccpb.KeyValue{
									Key:            []byte("1"),
									CreateRevision: revision,
								},
								PrevKv: &mvccpb.KeyValue{
									Key:            []byte("1"),
									CreateRevision: revision,
									ModRevision:    revision,
									Value:          []byte("1"),
									Version:        revision,
								},
							}}),
						}
					}(),
					func() spec {
						var (
							revision     = int64(3)
							dRevision    = revision + 10
							strRevision  = []byte(revisionToString(revision))
							emptyTreeDef = &TreeDef{}
						)

						return spec{
							spec:      "shallow delete, full metadata",
							keyPrefix: "/registry/custom",
							nmt:       emptyTreeDef,
							ndt:       emptyTreeDef,
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision,
									etcdserverpb.Compare_MOD.String():     strRevision,
									etcdserverpb.Compare_LEASE.String():   strRevision,
									etcdserverpb.Compare_VERSION.String(): strRevision,
								}},
							}},
							odt:      &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							revision: dRevision,
							events:   []*mvccpb.Event{{}},
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{
								{},
								{
									Type: mvccpb.DELETE,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("/registry/custom/1"),
										CreateRevision: revision,
										ModRevision:    dRevision,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("/registry/custom/1"),
										CreateRevision: revision,
										Lease:          revision,
										ModRevision:    revision,
										Value:          []byte("1"),
										Version:        revision,
									},
								},
							}),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
							events      = []*mvccpb.Event{{}}
							dt          = &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}}
						)

						return spec{
							spec: "deep delete, missing Version",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision,
											etcdserverpb.Compare_LEASE.String():   strRevision,
											etcdserverpb.Compare_VERSION.String(): strRevision,
										}},
									}},
								}},
							}},
							ndt: dt,
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String(): strRevision,
											etcdserverpb.Compare_MOD.String():    strRevision,
											etcdserverpb.Compare_LEASE.String():  strRevision,
										}},
									}},
								}},
							}},
							odt:         dt,
							events:      events,
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							revision    = int64(3)
							strRevision = []byte(revisionToString(revision))
						)

						return spec{
							spec: "deep delete, missing value",
							nmt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"2": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision,
												etcdserverpb.Compare_MOD.String():     strRevision,
												etcdserverpb.Compare_LEASE.String():   strRevision,
												etcdserverpb.Compare_VERSION.String(): strRevision,
											}},
										}},
									}},
								},
							},
							ndt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"2": []byte("2"),
									}},
								}},
							}},
							omt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision,
												etcdserverpb.Compare_MOD.String():     strRevision,
												etcdserverpb.Compare_LEASE.String():   strRevision,
												etcdserverpb.Compare_VERSION.String(): strRevision,
											}},
										}},
									}},
								},
							},
							odt:         &TreeDef{},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							revision    = int64(4)
							dRevision   = revision - 2
							strRevision = []byte(revisionToString(revision))
						)

						return spec{
							spec:      "deep delete, full metadata",
							keyPrefix: "registry",
							omt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision,
												etcdserverpb.Compare_MOD.String():     strRevision,
												etcdserverpb.Compare_LEASE.String():   strRevision,
												etcdserverpb.Compare_VERSION.String(): strRevision,
											}},
										}},
									}},
								},
							},
							odt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}},
							revision: dRevision,
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{{
								Type: mvccpb.DELETE,
								Kv: &mvccpb.KeyValue{
									Key:            []byte("registry/a/b/1"),
									CreateRevision: revision,
									ModRevision:    dRevision,
								},
								PrevKv: &mvccpb.KeyValue{
									Key:            []byte("registry/a/b/1"),
									CreateRevision: revision,
									Lease:          revision,
									ModRevision:    revision,
									Value:          []byte("1"),
									Version:        revision,
								},
							}}),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 3
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
						)

						return spec{
							spec: "shallow update, missing value",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision2,
									etcdserverpb.Compare_VERSION.String(): strRevision2,
								}},
							}},
							ndt: &TreeDef{Blobs: map[string][]byte{"2": []byte("2")}},
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							odt:         &TreeDef{Blobs: map[string][]byte{"1": []byte("11")}},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 3
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
							emptyTreeDef = &TreeDef{}
							events       = []*mvccpb.Event{{}}
						)

						return spec{
							spec: "shallow update, missing CreateVersion",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision2,
									etcdserverpb.Compare_MOD.String():     strRevision2,
									etcdserverpb.Compare_VERSION.String(): strRevision2,
								}},
							}},
							ndt: emptyTreeDef,
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							events:      events,
							odt:         emptyTreeDef,
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 3
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
						)

						return spec{
							spec: "shallow update, missing Version",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String(): strRevision1,
									etcdserverpb.Compare_MOD.String():    strRevision2,
								}},
							}},
							ndt: &TreeDef{Blobs: map[string][]byte{"1": []byte("11")}},
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							odt:         &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 3
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
							events       = []*mvccpb.Event{{}}
						)

						return spec{
							spec: "shallow update, ModVersion unmoidifed",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision2,
									etcdserverpb.Compare_LEASE.String():   strRevision2,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision2,
								}},
							}},
							ndt: &TreeDef{Blobs: map[string][]byte{"1": []byte("11")}},
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_LEASE.String():   strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							odt:         &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							events:      events,
							matchErr:    Succeed(),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 2
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
						)

						return spec{
							spec: "shallow update, missing Lease",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision2,
									etcdserverpb.Compare_VERSION.String(): strRevision2,
								}},
							}},
							ndt: &TreeDef{Blobs: map[string][]byte{"1": []byte("11")}},
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							odt:      &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{{
								Type: mvccpb.PUT,
								Kv: &mvccpb.KeyValue{
									Key:            []byte("1"),
									CreateRevision: revision1,
									ModRevision:    revision2,
									Value:          []byte("11"),
									Version:        revision2,
								},
								PrevKv: &mvccpb.KeyValue{
									Key:            []byte("1"),
									CreateRevision: revision1,
									ModRevision:    revision1,
									Value:          []byte("1"),
									Version:        revision1,
								},
							}}),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 2
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
							dt           = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
						)

						return spec{
							spec:      "shallow update, full metadata",
							keyPrefix: "/registry/custom",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision2,
									etcdserverpb.Compare_MOD.String():     strRevision2,
									etcdserverpb.Compare_LEASE.String():   strRevision2,
									etcdserverpb.Compare_VERSION.String(): strRevision2,
								}},
							}},
							ndt: dt,
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"1": {Blobs: map[string][]byte{
									etcdserverpb.Compare_CREATE.String():  strRevision1,
									etcdserverpb.Compare_MOD.String():     strRevision1,
									etcdserverpb.Compare_LEASE.String():   strRevision1,
									etcdserverpb.Compare_VERSION.String(): strRevision1,
								}},
							}},
							odt:      dt,
							events:   []*mvccpb.Event{{}},
							revision: revision1,
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{
								{},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("/registry/custom/1"),
										CreateRevision: revision2,
										Lease:          revision2,
										ModRevision:    revision2,
										Value:          []byte("1"),
										Version:        revision2,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("/registry/custom/1"),
										CreateRevision: revision1,
										Lease:          revision1,
										ModRevision:    revision1,
										Value:          []byte("1"),
										Version:        revision1,
									},
								},
							}),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 1
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
							events       = []*mvccpb.Event{{}}
							dt           = &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}}
						)

						return spec{
							spec: "deep update, missing Version",
							nmt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  strRevision2,
											etcdserverpb.Compare_MOD.String():     strRevision2,
											etcdserverpb.Compare_LEASE.String():   strRevision2,
											etcdserverpb.Compare_VERSION.String(): strRevision2,
										}},
									}},
								}},
							}},
							ndt: dt,
							omt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String(): strRevision1,
											etcdserverpb.Compare_MOD.String():    strRevision1,
											etcdserverpb.Compare_LEASE.String():  strRevision1,
										}},
									}},
								}},
							}},
							odt:         dt,
							events:      events,
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: Equal(events),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 + 1
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
						)

						return spec{
							spec: "deep update, missing value",
							nmt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision2,
												etcdserverpb.Compare_MOD.String():     strRevision2,
												etcdserverpb.Compare_LEASE.String():   strRevision2,
												etcdserverpb.Compare_VERSION.String(): strRevision2,
											}},
										}},
									}},
								},
							},
							ndt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}},
							omt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision1,
												etcdserverpb.Compare_MOD.String():     strRevision1,
												etcdserverpb.Compare_LEASE.String():   strRevision1,
												etcdserverpb.Compare_VERSION.String(): strRevision1,
											}},
										}},
									}},
								},
							},
							odt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"2": []byte("2"),
									}},
								}},
							}},
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: BeNil(),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(5)
							revision2    = revision1 - 3
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
						)

						return spec{
							spec:      "deep update, full metadata",
							keyPrefix: "registry",
							nmt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision2,
												etcdserverpb.Compare_MOD.String():     strRevision2,
												etcdserverpb.Compare_LEASE.String():   strRevision2,
												etcdserverpb.Compare_VERSION.String(): strRevision2,
											}},
										}},
									}},
								},
							},
							ndt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("11"),
									}},
								}},
							}},
							omt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  strRevision1,
												etcdserverpb.Compare_MOD.String():     strRevision1,
												etcdserverpb.Compare_LEASE.String():   strRevision1,
												etcdserverpb.Compare_VERSION.String(): strRevision1,
											}},
										}},
									}},
								},
							},
							odt: &TreeDef{Subtrees: map[string]TreeDef{
								"a": {Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{
										"1": []byte("1"),
									}},
								}},
							}},
							revision: revision1,
							matchErr: Succeed(),
							matchEvents: Equal([]*mvccpb.Event{{
								Type: mvccpb.PUT,
								Kv: &mvccpb.KeyValue{
									Key:            []byte("registry/a/b/1"),
									CreateRevision: revision2,
									Lease:          revision2,
									ModRevision:    revision2,
									Value:          []byte("11"),
									Version:        revision2,
								},
								PrevKv: &mvccpb.KeyValue{
									Key:            []byte("registry/a/b/1"),
									CreateRevision: revision1,
									Lease:          revision1,
									ModRevision:    revision1,
									Value:          []byte("1"),
									Version:        revision1,
								},
							}}),
						}
					}(),
					func() spec {
						var (
							revision1    = int64(2)
							revision2    = revision1 + 1
							strRevision1 = []byte(revisionToString(revision1))
							strRevision2 = []byte(revisionToString(revision2))
							events       = []*mvccpb.Event{{}}
						)

						return spec{
							spec: "multiple updates, partial metadata",
							nmt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision2,
										etcdserverpb.Compare_MOD.String():     strRevision2,
										etcdserverpb.Compare_LEASE.String():   strRevision2,
										etcdserverpb.Compare_VERSION.String(): strRevision2,
									}},
									"2": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String(): strRevision1,
										etcdserverpb.Compare_MOD.String():    strRevision2,
										etcdserverpb.Compare_LEASE.String():  strRevision2,
									}},
								},
							},
							ndt: &TreeDef{Blobs: map[string][]byte{
								"1": []byte("11"),
								"2": []byte("22"),
							}},
							omt: &TreeDef{
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision1,
										etcdserverpb.Compare_MOD.String():     strRevision1,
										etcdserverpb.Compare_LEASE.String():   strRevision1,
										etcdserverpb.Compare_VERSION.String(): strRevision1,
									}},
									"2": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  strRevision1,
										etcdserverpb.Compare_MOD.String():     strRevision1,
										etcdserverpb.Compare_LEASE.String():   strRevision1,
										etcdserverpb.Compare_VERSION.String(): strRevision1,
									}},
								},
							},
							odt: &TreeDef{Blobs: map[string][]byte{
								"1": []byte("1"),
								"2": []byte("2"),
							}},
							events:      events,
							matchErr:    PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
							matchEvents: Equal(events),
						}
					}(),
					{
						spec:        "different kinds of changes, full metadata",
						nmt:         &mhAll.Tree,
						ndt:         &dhAll.Tree,
						omt:         &mhAll.Parents[0].Tree,
						odt:         &dhAll.Parents[0].Tree,
						revision:    revision2,
						matchErr:    Succeed(),
						matchEvents: Equal(eventsChangesOnlyAll),
					},
				} {
					func(s spec) {
						Describe(
							fmt.Sprintf(
								"%s: expired context=%t, keyPrefix=%s, revision=%d, nmt=%v, ndt=%v, omt=%v, odt=%v, events=%v",
								s.spec, s.ctxFn != nil, s.keyPrefix, s.revision, s.nmt, s.ndt, s.omt, s.odt, s.events,
							),
							func() {
								var nmt, ndt, omt, odt git.Tree

								BeforeEach(func() {
									rw.backend.keyPrefix.prefix = s.keyPrefix
									rw.revision = s.revision
									rw.events = s.events

									for _, s := range []struct {
										tPtr *git.Tree
										td   *TreeDef
									}{
										{tPtr: &nmt, td: s.nmt},
										{tPtr: &ndt, td: s.ndt},
										{tPtr: &omt, td: s.omt},
										{tPtr: &odt, td: s.odt},
									} {
										if s.td == nil {
											continue
										}

										Expect(func() (err error) {
											*s.tPtr, err = CreateAndLoadTreeFromDef(ctx, rw.backend.repo, s.td)
											return
										}()).To(Succeed())
									}

									if s.ctxFn != nil {
										ctx = s.ctxFn(ctx)
									}
								})

								AfterEach(func() {
									for _, t := range []git.Tree{nmt, ndt, omt, odt} {
										if t != nil {
											Expect(t.Close()).To(Succeed())
										}
									}
								})

								It(ItSpecForMatchError(s.matchErr), func() {
									Expect(rw.loadEvents(ctx, nmt, ndt, omt, odt)).To(s.matchErr, "err")
									Expect(rw.events).To(s.matchEvents, "events")
								})
							},
						)
					}(s)
				}
			})

			Describe("dispatchEvents", func() {
				type spec struct {
					spec                string
					ctxFn               ContextFunc
					clusterID, memberID uint64
					revision            int64
					watches             []watchSpec
					events              []*mvccpb.Event
					matchErr            types.GomegaMatcher
				}

				BeforeEach(func() {
					// Git repo access is not required to dispatch events.
					rw.backend.repo, rw.backend.errors = nil, nil
				})

				for _, s := range []spec{
					{
						spec:     "no watches, no events",
						ctxFn:    CanceledContext,
						matchErr: Succeed(),
					},
					{
						spec:     "no watches",
						ctxFn:    CanceledContext,
						matchErr: Succeed(),
					},
					{
						spec:     "no events",
						ctxFn:    CanceledContext,
						matchErr: Succeed(),
					},
					{
						spec:     "main context expired",
						ctxFn:    CanceledContext,
						revision: 1,
						watches:  []watchSpec{{makerFn: newMockWatch}, {makerFn: newMockWatch}},
						events:   []*mvccpb.Event{{}, {}},
						matchErr: MatchError(context.Canceled),
					},
					{
						spec:     "watch contexts expired",
						watches:  []watchSpec{{makerFn: newMockWatch, cancel: true}, {makerFn: newMockWatch, cancel: true}},
						events:   []*mvccpb.Event{{}, {}},
						matchErr: MatchError(utilerrors.NewAggregate([]error{context.Canceled, context.Canceled})),
					},
					func() spec {
						var (
							header = &etcdserverpb.ResponseHeader{}
							events = []*mvccpb.Event{
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
								},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
								},
							}

							filterAndSendErr = context.DeadlineExceeded
						)

						return spec{
							spec:      "prevKv, FilterAndSendFn fails",
							clusterID: header.ClusterId,
							memberID:  header.MemberId,
							revision:  header.Revision,
							watches: []watchSpec{{
								makerFn:               newMockWatch,
								prevKv:                true,
								expectFilterAndSendFn: getExpectFilterAndSendFn("0", Equal(header), Equal(events), filterAndSendErr),
								expectCancelFn:        getExpectCancelFn("0", Equal(header), MatchError(filterAndSendErr)),
							}},
							events:   events,
							matchErr: MatchError(filterAndSendErr),
						}
					}(),
					func() spec {
						var (
							header = &etcdserverpb.ResponseHeader{ClusterId: 2, MemberId: 10, Revision: 14, RaftTerm: 14}
							events = []*mvccpb.Event{
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
								},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
								},
							}
						)

						return spec{
							spec:      "prevKv, FilterAndSendFn succeeds",
							clusterID: header.ClusterId,
							memberID:  header.MemberId,
							revision:  header.Revision,
							watches: []watchSpec{{
								makerFn:               newMockWatch,
								prevKv:                true,
								expectFilterAndSendFn: getExpectFilterAndSendFn("0", Equal(header), Equal(events), nil),
								expectCancelFn:        getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
							}},
							events:   events,
							matchErr: Succeed(),
						}
					}(),
					func() spec {
						var (
							header = &etcdserverpb.ResponseHeader{}
							events = []*mvccpb.Event{
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
								},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
								},
							}

							filterAndSendErr = context.DeadlineExceeded
						)

						return spec{
							spec:      "no prevKv, FilterAndSendFn fails",
							clusterID: header.ClusterId,
							memberID:  header.MemberId,
							revision:  header.Revision,
							watches: []watchSpec{{
								makerFn: newMockWatch,
								expectFilterAndSendFn: getExpectFilterAndSendFn(
									"0", Equal(header), Equal(getEventsWithoutPrevKv(events)), filterAndSendErr,
								),
								expectCancelFn: getExpectCancelFn("0", Equal(header), MatchError(filterAndSendErr)),
							}},
							events:   events,
							matchErr: MatchError(filterAndSendErr),
						}
					}(),
					func() spec {
						var (
							header = &etcdserverpb.ResponseHeader{ClusterId: 2, MemberId: 10, Revision: 14, RaftTerm: 14}
							events = []*mvccpb.Event{
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
								},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
								},
							}
						)

						return spec{
							spec:      "no prevKv, FilterAndSendFn succeeds",
							clusterID: header.ClusterId,
							memberID:  header.MemberId,
							revision:  header.Revision,
							watches: []watchSpec{{
								makerFn: newMockWatch,
								expectFilterAndSendFn: getExpectFilterAndSendFn(
									"0", Equal(header), Equal(getEventsWithoutPrevKv(events)), nil,
								),
								expectCancelFn: getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
							}},
							events:   events,
							matchErr: Succeed(),
						}
					}(),
					func() spec {
						var (
							header = &etcdserverpb.ResponseHeader{ClusterId: 2, MemberId: 10, Revision: 14, RaftTerm: 14}
							events = []*mvccpb.Event{
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
								},
								{
									Type: mvccpb.PUT,
									Kv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 1,
										Lease:          2,
										ModRevision:    3,
										Version:        4,
									},
									PrevKv: &mvccpb.KeyValue{
										Key:            []byte("some/other/key"),
										CreateRevision: 10,
										Lease:          11,
										ModRevision:    12,
										Version:        13,
									},
								},
							}

							eventsNoPrevKv = getEventsWithoutPrevKv(events)

							errs = []error{
								nil,
								errors.New("1"),
								context.Canceled,
								nil,
								context.DeadlineExceeded,
								context.Canceled,
							}
						)

						return spec{
							spec:      "mixed case",
							clusterID: header.ClusterId,
							memberID:  header.MemberId,
							revision:  header.Revision,
							watches: []watchSpec{
								{
									makerFn:               newMockWatch,
									prevKv:                true,
									expectFilterAndSendFn: getExpectFilterAndSendFn("0", Equal(header), Equal(events), nil),
									expectCancelFn:        getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
								},
								{
									makerFn: newMockWatch,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"1", Equal(header), Equal(eventsNoPrevKv), errs[1],
									),
									expectCancelFn: getExpectCancelFn("1", Equal(header), MatchError(errs[1])),
								},
								{
									makerFn:        newMockWatch,
									cancel:         true,
									prevKv:         true,
									expectCancelFn: getExpectCancelFn("2", BeNil(), Not(HaveOccurred())),
								},
								{
									makerFn: newMockWatch,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"3", Equal(header), Equal(eventsNoPrevKv), nil,
									),
									expectCancelFn: getExpectCancelFn("3", BeNil(), Not(HaveOccurred())),
								},
								{
									makerFn: newMockWatch,
									prevKv:  true,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"4", Equal(header), Equal(events), errs[4],
									),
									expectCancelFn: getExpectCancelFn("4", Equal(header), MatchError(errs[4])),
								},
								{
									makerFn:        newMockWatch,
									cancel:         true,
									expectCancelFn: getExpectCancelFn("5", BeNil(), Not(HaveOccurred())),
								},
							},
							events:   events,
							matchErr: MatchError(utilerrors.NewAggregate(errs)),
						}
					}(),
				} {
					func(s spec) {
						Describe(
							fmt.Sprintf(
								"%s: expired context=%t, clusterID=%d, memberID=%d, revision=%d, watches=%v, events=%v",
								s.spec, s.ctxFn != nil, s.clusterID, s.memberID, s.revision, s.watches, s.events,
							),
							func() {
								BeforeEach(func() {
									rw.revision = s.revision
									rw.events = s.events
									rw.backend.clusterID = s.clusterID
									rw.backend.memberID = s.memberID

									rw.watches = append(rw.watches, prepareWatches(ctrl, s.ctxFn, s.watches)...)

									if s.ctxFn != nil {
										ctx = s.ctxFn(ctx)
									}
								})

								AfterEach(func() {
									cancelOpenWatches(rw.watches)
								})

								It(ItSpecForMatchError(s.matchErr), func() {
									Expect(rw.dispatchEvents(ctx)).To(s.matchErr)
								})
							},
						)
					}(s)
				}
			})

			Describe("Run", func() {
				type expectWatchContextsFunc func()

				type spec struct {
					spec                             string
					ctxFn                            ContextFunc
					metaRefName                      git.ReferenceName
					metaHead                         metaHeadFunc
					clusterID, memberID              uint64
					revision                         int64
					changesOnly                      bool
					nrun                             int
					watches                          []watchSpec
					events                           []*mvccpb.Event
					matchErr, matchEvents, matchNrun types.GomegaMatcher
					expectWatchContexts              expectWatchContextsFunc
					expectMetaHead                   expectMetaHeadFunc
				}

				var (
					expectAllWatchContextErrorsTo = func(match types.GomegaMatcher) func() {
						return func() {
							for i, w := range rw.watches {
								var spec = fmt.Sprintf("watch context: %d", i)
								Expect(w.Context().Err()).To(match, spec)
							}
						}
					}

					expectAllWatchContextErrorsToEach = func(matchErrs []types.GomegaMatcher) func() {
						return func() {
							Expect(rw.watches).To(HaveLen(len(matchErrs)))

							for i, w := range rw.watches {
								var (
									spec     = fmt.Sprintf("watch context: %d", i)
									matchErr = matchErrs[i]
								)

								Expect(w.Context().Err()).To(matchErr, spec)
							}
						}
					}

					errorToMatcher = func(err error) types.GomegaMatcher {
						if err != nil {
							return MatchError(err)
						}

						return Not(HaveOccurred())
					}

					errorsToMatchers = func(errs []error) (matchers []types.GomegaMatcher) {
						for _, err := range errs {
							matchers = append(matchers, errorToMatcher(err))
						}

						return matchers
					}
				)

				for _, s := range []spec{
					{
						spec:  "watch contexts expired",
						ctxFn: CanceledContext,
						watches: []watchSpec{
							{
								makerFn:        newMockWatch,
								cancel:         true,
								expectCancelFn: getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
							},
							{
								makerFn:        newMockWatch,
								cancel:         true,
								expectCancelFn: getExpectCancelFn("1", BeNil(), Not(HaveOccurred())),
							},
						},
						matchErr:            Succeed(),
						matchEvents:         BeNil(),
						matchNrun:           BeZero(),
						expectWatchContexts: func() { Expect(rw.watches).To(BeEmpty(), "watches") },
					},
					func() spec {
						var (
							header      = &etcdserverpb.ResponseHeader{}
							matchRunErr = MatchError(rpctypes.ErrGRPCCorrupt)

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									expectCancelFn: getExpectCancelFn("0", Equal(header), matchRunErr),
								},
							}
						)

						return spec{
							spec:                "no metadata reference name",
							ctxFn:               CanceledContext,
							watches:             watches,
							matchErr:            matchRunErr,
							matchEvents:         BeNil(),
							matchNrun:           Equal(1),
							expectWatchContexts: expectAllWatchContextErrorsTo(MatchError(context.Canceled)),
						}
					}(),
					func() spec {
						var (
							header = &etcdserverpb.ResponseHeader{}
							runErr = context.Canceled

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									expectCancelFn: getExpectCancelFn("0", Equal(header), MatchError(runErr)),
								},
							}

							events = []*mvccpb.Event{{}}
						)

						return spec{
							spec:                "no metadata",
							ctxFn:               CanceledContext,
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							watches:             watches,
							events:              events,
							matchErr:            MatchError(runErr),
							matchEvents:         Equal(events),
							matchNrun:           Equal(1),
							expectWatchContexts: expectAllWatchContextErrorsTo(MatchError(context.Canceled)),
						}
					}(),
					func() spec {
						var (
							revision    = int64(1)
							strRevision = revisionToString(revision)
							header      = &etcdserverpb.ResponseHeader{Revision: revision, RaftTerm: uint64(revision)}
							matchRunErr = PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))
							nrun        = 3

							mh = &CommitDef{Message: strRevision, Tree: TreeDef{
								Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision)},
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(strRevision),
										etcdserverpb.Compare_LEASE.String():   []byte(strRevision),
										etcdserverpb.Compare_MOD.String():     []byte(strRevision),
										etcdserverpb.Compare_VERSION.String(): []byte(strRevision),
									}},
								},
							}}

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									expectCancelFn: getExpectCancelFn("0", Equal(header), matchRunErr),
								},
							}

							events = []*mvccpb.Event{{}}
						)

						return spec{
							spec:                "no data",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, nil),
							revision:            header.Revision,
							nrun:                nrun,
							watches:             watches,
							events:              events,
							matchErr:            matchRunErr,
							matchEvents:         Equal(events),
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsTo(MatchError(context.Canceled)),
							expectMetaHead:      expectMetaHead(mh, nil, false),
						}
					}(),
					func() spec {
						var (
							revision    = int64(1)
							strRevision = revisionToString(revision)
							header      = &etcdserverpb.ResponseHeader{
								ClusterId: 2,
								MemberId:  10,
								Revision:  revision,
								RaftTerm:  uint64(revision),
							}
							matchRunErr = PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))
							nrun        = 3

							mh = &CommitDef{Message: strRevision, Tree: TreeDef{
								Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision)},
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_LEASE.String():   []byte(strRevision),
										etcdserverpb.Compare_MOD.String():     []byte(strRevision),
										etcdserverpb.Compare_VERSION.String(): []byte(strRevision),
									}},
								},
							}}

							dh = &CommitDef{Message: strRevision, Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("1")},
							}}

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									prevKv:         true,
									expectCancelFn: getExpectCancelFn("0", Equal(header), matchRunErr),
								},
							}
						)

						return spec{
							spec:                "all add, partial metadata",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, dh),
							clusterID:           header.ClusterId,
							memberID:            header.MemberId,
							revision:            header.Revision,
							nrun:                nrun,
							watches:             watches,
							matchErr:            matchRunErr,
							matchEvents:         BeNil(),
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsTo(MatchError(context.Canceled)),
							expectMetaHead:      expectMetaHead(mh, dh, false),
						}
					}(),
					func() spec {
						var (
							revision    = int64(1)
							strRevision = revisionToString(revision)
							header      = &etcdserverpb.ResponseHeader{
								ClusterId: 2,
								MemberId:  10,
								Revision:  revision,
								RaftTerm:  uint64(revision),
							}
							filterAndSendErr = context.DeadlineExceeded
							matchRunErr      = MatchError(filterAndSendErr)
							nrun             = 3

							mh = &CommitDef{Message: strRevision, Tree: TreeDef{
								Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision)},
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(strRevision),
										etcdserverpb.Compare_LEASE.String():   []byte(strRevision),
										etcdserverpb.Compare_MOD.String():     []byte(strRevision),
										etcdserverpb.Compare_VERSION.String(): []byte(strRevision),
									}},
								},
							}}

							dh = &CommitDef{Message: strRevision, Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("1")},
							}}

							matchEvents = Equal([]*mvccpb.Event{{
								Type: mvccpb.PUT,
								Kv: &mvccpb.KeyValue{
									Key:            []byte("1"),
									CreateRevision: revision,
									Lease:          revision,
									ModRevision:    revision,
									Value:          []byte("1"),
									Version:        revision,
								},
							}})

							watches = []watchSpec{
								{
									makerFn: newMockWatch,
									prevKv:  true,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"0", Equal(header), matchEvents, filterAndSendErr,
									),
									expectCancelFn: getExpectCancelFn("0", Equal(header), matchRunErr),
								},
							}
						)

						return spec{
							spec:                "all add, FilterAndSend fails",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, dh),
							clusterID:           header.ClusterId,
							memberID:            header.MemberId,
							revision:            header.Revision,
							nrun:                nrun,
							watches:             watches,
							matchErr:            matchRunErr,
							matchEvents:         matchEvents,
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsTo(MatchError(context.Canceled)),
							expectMetaHead:      expectMetaHead(mh, dh, false),
						}
					}(),
					func() spec {
						var (
							revision    = int64(1)
							strRevision = revisionToString(revision)
							header      = &etcdserverpb.ResponseHeader{
								ClusterId: 2,
								MemberId:  10,
								Revision:  revision,
								RaftTerm:  uint64(revision),
							}
							nrun = 3

							mh = &CommitDef{Message: strRevision, Tree: TreeDef{
								Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision)},
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(strRevision),
										etcdserverpb.Compare_LEASE.String():   []byte(strRevision),
										etcdserverpb.Compare_MOD.String():     []byte(strRevision),
										etcdserverpb.Compare_VERSION.String(): []byte(strRevision),
									}},
								},
							}}

							dh = &CommitDef{Message: strRevision, Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("1")},
							}}

							matchEvents = Equal([]*mvccpb.Event{{
								Type: mvccpb.PUT,
								Kv: &mvccpb.KeyValue{
									Key:            []byte("1"),
									CreateRevision: revision,
									Lease:          revision,
									ModRevision:    revision,
									Value:          []byte("1"),
									Version:        revision,
								},
							}})

							watches = []watchSpec{
								{
									makerFn: newMockWatch,
									prevKv:  true,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"0", Equal(header), matchEvents, nil,
									),
									expectCancelFn: getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
								},
							}
						)

						return spec{
							spec:                "all add, FilterAndSend succeeds",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, dh),
							clusterID:           header.ClusterId,
							memberID:            header.MemberId,
							revision:            header.Revision,
							nrun:                nrun,
							watches:             watches,
							matchErr:            Succeed(),
							matchEvents:         matchEvents,
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsTo(BeNil()),
							expectMetaHead:      expectMetaHead(mh, dh, false),
						}
					}(),
					func() spec {
						var (
							revision3    = revision2 + 1
							strRevision3 = revisionToString(revision3)
							nrun         = 3
							matchRunErr  = PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))

							header = &etcdserverpb.ResponseHeader{
								ClusterId: 2,
								MemberId:  10,
								Revision:  revision3,
								RaftTerm:  uint64(revision3),
							}

							mh = &CommitDef{
								Message: strRevision3,
								Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision3)}},
								Parents: []CommitDef{*mhAll.DeepCopy()},
							}

							dh = &CommitDef{
								Message: strRevision3,
								Parents: []CommitDef{*dhAll.DeepCopy()},
							}

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									expectCancelFn: getExpectCancelFn("0", Equal(header), matchRunErr),
								},
							}

							events = []*mvccpb.Event{{}}
						)

						delete(dh.Parents[0].Tree.Subtrees["z"].Subtrees["deep"].Blobs, "update")

						return spec{
							spec:                "mixed diff, partial old data",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, dh),
							clusterID:           header.ClusterId,
							memberID:            header.MemberId,
							revision:            revision2,
							nrun:                nrun,
							watches:             watches,
							events:              events,
							matchErr:            matchRunErr,
							matchEvents:         BeNil(),
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsTo(MatchError(context.Canceled)),
							expectMetaHead:      expectMetaHead(mh, dh, false),
						}
					}(),
					func() spec {
						var (
							revision3    = revision2 + 1
							strRevision3 = revisionToString(revision3)
							nrun         = 3
							matchRunErr  = PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))

							header = &etcdserverpb.ResponseHeader{
								ClusterId: 2,
								MemberId:  10,
								Revision:  revision3,
								RaftTerm:  uint64(revision3),
							}

							mh = &CommitDef{
								Message: strRevision3,
								Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision3)}},
								Parents: []CommitDef{*mhAll.DeepCopy()},
							}

							dh = &CommitDef{
								Message: strRevision3,
								Parents: []CommitDef{*dhAll.DeepCopy()},
							}

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									expectCancelFn: getExpectCancelFn("0", Equal(header), matchRunErr),
								},
							}
						)

						delete(
							mh.Parents[0].Tree.Subtrees["z"].Subtrees["deep"].Subtrees["update"].Blobs,
							etcdserverpb.Compare_VERSION.String(),
						)

						return spec{
							spec:                "mixed diff, partial old metadata",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, dh),
							clusterID:           header.ClusterId,
							memberID:            header.MemberId,
							revision:            revision2,
							changesOnly:         true,
							nrun:                nrun,
							watches:             watches,
							matchErr:            matchRunErr,
							matchEvents:         BeNil(),
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsTo(MatchError(context.Canceled)),
							expectMetaHead:      expectMetaHead(mh, dh, false),
						}
					}(),
					func() spec {
						var (
							revision3    = revision2 + 1
							strRevision3 = revisionToString(revision3)
							nrun         = 3

							filterAndSendErrs = []error{context.DeadlineExceeded, nil, nil, errors.New("some error")}
							finalContextErrs  = []error{context.Canceled, nil, nil, context.Canceled}

							header = &etcdserverpb.ResponseHeader{
								ClusterId: 2,
								MemberId:  10,
								Revision:  revision2,
								RaftTerm:  uint64(revision2),
							}

							mh = &CommitDef{
								Message: strRevision3,
								Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision3)}},
								Parents: []CommitDef{*mhAll.DeepCopy()},
							}

							dh = &CommitDef{
								Message: strRevision3,
								Parents: []CommitDef{*dhAll.DeepCopy()},
							}

							events         = eventsNoChangesOnlyAll
							eventsNoPrevKv = getEventsWithoutPrevKv(events)

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									cancel:         true,
									expectCancelFn: getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
								},
								{
									makerFn: newMockWatch,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"0", Equal(header), Equal(eventsNoPrevKv), filterAndSendErrs[0],
									),
									expectCancelFn: getExpectCancelFn("0", Equal(header), errorToMatcher(filterAndSendErrs[0])),
								},
								{
									makerFn:        newMockWatch,
									cancel:         true,
									expectCancelFn: getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
								},
								{
									makerFn: newMockWatch,
									prevKv:  true,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"1", Equal(header), Equal(events), filterAndSendErrs[1],
									),
									expectCancelFn: getExpectCancelFn("1", BeNil(), errorToMatcher(filterAndSendErrs[1])),
								},
								{
									makerFn: newMockWatch,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"2", Equal(header), Equal(eventsNoPrevKv), filterAndSendErrs[2],
									),
									expectCancelFn: getExpectCancelFn("2", BeNil(), errorToMatcher(filterAndSendErrs[2])),
								},
								{
									makerFn: newMockWatch,
									prevKv:  true,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"3", Equal(header), Equal(events), filterAndSendErrs[3],
									),
									expectCancelFn: getExpectCancelFn("3", Equal(header), errorToMatcher(filterAndSendErrs[3])),
								},
								{
									makerFn:        newMockWatch,
									cancel:         true,
									expectCancelFn: getExpectCancelFn("3", BeNil(), Not(HaveOccurred())),
								},
							}
						)

						return spec{
							spec:                "mixed diff, mixed watchers",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, dh),
							clusterID:           header.ClusterId,
							memberID:            header.MemberId,
							revision:            revision2,
							changesOnly:         false,
							nrun:                nrun,
							watches:             watches,
							events:              []*mvccpb.Event{{}},
							matchErr:            Equal(utilerrors.NewAggregate(filterAndSendErrs)),
							matchEvents:         Equal(events),
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsToEach(errorsToMatchers(finalContextErrs)),
							expectMetaHead:      expectMetaHead(mh, dh, false),
						}
					}(),
					func() spec {
						var (
							revision3    = revision2 + 1
							strRevision3 = revisionToString(revision3)
							nrun         = 3

							filterAndSendErrs = []error{context.DeadlineExceeded, nil, nil, errors.New("some error")}
							finalContextErrs  = []error{context.Canceled, nil, nil, context.Canceled}

							header = &etcdserverpb.ResponseHeader{
								ClusterId: 2,
								MemberId:  10,
								Revision:  revision2,
								RaftTerm:  uint64(revision2),
							}

							mh = &CommitDef{
								Message: strRevision3,
								Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision3)}},
								Parents: []CommitDef{*mhAll.DeepCopy()},
							}

							dh = &CommitDef{
								Message: strRevision3,
								Parents: []CommitDef{*dhAll.DeepCopy()},
							}

							events         = eventsChangesOnlyAll
							eventsNoPrevKv = getEventsWithoutPrevKv(events)

							watches = []watchSpec{
								{
									makerFn:        newMockWatch,
									cancel:         true,
									expectCancelFn: getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
								},
								{
									makerFn: newMockWatch,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"0", Equal(header), Equal(eventsNoPrevKv), filterAndSendErrs[0],
									),
									expectCancelFn: getExpectCancelFn("0", Equal(header), errorToMatcher(filterAndSendErrs[0])),
								},
								{
									makerFn:        newMockWatch,
									cancel:         true,
									expectCancelFn: getExpectCancelFn("0", BeNil(), Not(HaveOccurred())),
								},
								{
									makerFn: newMockWatch,
									prevKv:  true,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"1", Equal(header), Equal(events), filterAndSendErrs[1],
									),
									expectCancelFn: getExpectCancelFn("1", BeNil(), errorToMatcher(filterAndSendErrs[1])),
								},
								{
									makerFn: newMockWatch,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"2", Equal(header), Equal(eventsNoPrevKv), filterAndSendErrs[2],
									),
									expectCancelFn: getExpectCancelFn("2", BeNil(), errorToMatcher(filterAndSendErrs[2])),
								},
								{
									makerFn: newMockWatch,
									prevKv:  true,
									expectFilterAndSendFn: getExpectFilterAndSendFn(
										"3", Equal(header), Equal(events), filterAndSendErrs[3],
									),
									expectCancelFn: getExpectCancelFn("3", Equal(header), errorToMatcher(filterAndSendErrs[3])),
								},
								{
									makerFn:        newMockWatch,
									cancel:         true,
									expectCancelFn: getExpectCancelFn("3", BeNil(), Not(HaveOccurred())),
								},
							}
						)

						return spec{
							spec:                "mixed diff, mixed watchers",
							metaRefName:         git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead:            metaHeadFrom(mh, dh),
							clusterID:           header.ClusterId,
							memberID:            header.MemberId,
							revision:            revision2,
							changesOnly:         true,
							nrun:                nrun,
							watches:             watches,
							events:              []*mvccpb.Event{{}},
							matchErr:            Equal(utilerrors.NewAggregate(filterAndSendErrs)),
							matchEvents:         Equal(events),
							matchNrun:           Equal(nrun + 1),
							expectWatchContexts: expectAllWatchContextErrorsToEach(errorsToMatchers(finalContextErrs)),
							expectMetaHead:      expectMetaHead(mh, dh, false),
						}
					}(),
				} {
					func(s spec) {
						Describe(
							fmt.Sprintf(
								"%s: expired context=%t, metaRefName: %s, clusterID=%d, memberID=%d, revision=%d, changesOnly=%t, nrun=%d, events=%v, watches=%v",
								s.spec, s.ctxFn != nil, s.metaRefName, s.clusterID, s.memberID, s.revision, s.changesOnly, s.nrun, s.events, s.watches,
							),
							func() {
								var parentCtx context.Context

								BeforeEach(func() {
									rw.backend.metadataRefName = s.metaRefName
									rw.backend.clusterID = s.clusterID
									rw.backend.memberID = s.memberID
									rw.changesOnly = s.changesOnly
									rw.revision = s.revision
									rw.nrun = s.nrun
									rw.events = s.events

									if s.metaHead != nil {
										Expect(func() (err error) {
											var c git.Commit

											if c, err = s.metaHead(ctx, rw.backend.repo); err != nil {
												return
											}

											defer c.Close()

											err = CreateReferenceFromCommitID(ctx, rw.backend.repo, s.metaRefName, c.ID())
											return
										}()).To(Succeed())
									}

									rw.watches = append(rw.watches, prepareWatches(ctrl, s.ctxFn, s.watches)...)

									parentCtx = ctx
									if s.ctxFn != nil {
										ctx = s.ctxFn(ctx)
									}
								})

								AfterEach(func() {
									cancelOpenWatches(rw.watches)
								})

								It(ItSpecForMatchError(s.matchErr), func() {
									Expect(rw.Run(ctx)).To(s.matchErr, "err")
									Expect(rw.nrun).To(s.matchNrun, "nrun")
									Expect(rw.events).To(s.matchEvents, "events")

									if s.expectMetaHead != nil {
										var emh *CommitDef

										Expect(func() (err error) {
											emh, err = GetCommitDefForReferenceName(parentCtx, rw.backend.repo, s.metaRefName)
											return
										}()).To(Succeed())

										s.expectMetaHead(parentCtx, rw.backend.repo, emh, "metaHead")
									}

									if s.expectWatchContexts != nil {
										s.expectWatchContexts()
									}
								})
							},
						)
					}(s)
				}
			})
		})
	})
})

var _ = Describe("buildEvent", func() {
	type spec struct {
		typ                     mvccpb.Event_EventType
		kv, prevKV              *mvccpb.KeyValue
		kvErr, prevKVErr        error
		matchErr, matchEventPtr types.GomegaMatcher
	}

	var (
		kv = &mvccpb.KeyValue{
			Key:            []byte("key"),
			Value:          []byte("value"),
			CreateRevision: 10,
			ModRevision:    11,
			Lease:          12,
			Version:        2,
		}

		kvErr = errors.New("kv error")

		prevKV = &mvccpb.KeyValue{
			Key:            []byte("prevKey"),
			Value:          []byte("prevValue"),
			CreateRevision: 8,
			ModRevision:    9,
			Lease:          10,
			Version:        3,
		}

		prevKVErr = errors.New("prevKV error")
	)

	for _, s := range []spec{
		{matchErr: Succeed(), matchEventPtr: Equal(&mvccpb.Event{Type: mvccpb.PUT})},
		{typ: mvccpb.DELETE, matchErr: Succeed(), matchEventPtr: Equal(&mvccpb.Event{Type: mvccpb.DELETE})},
		{typ: mvccpb.PUT, kvErr: kvErr, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, kv: kv, matchErr: Succeed(), matchEventPtr: Equal(&mvccpb.Event{Type: mvccpb.PUT, Kv: kv})},
		{typ: mvccpb.DELETE, kv: kv, kvErr: kvErr, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, prevKVErr: prevKVErr, matchErr: MatchError(prevKVErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, prevKV: prevKV, matchErr: Succeed(), matchEventPtr: Equal(&mvccpb.Event{Type: mvccpb.PUT, PrevKv: prevKV})},
		{typ: mvccpb.DELETE, prevKV: prevKV, prevKVErr: prevKVErr, matchErr: MatchError(prevKVErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, kvErr: kvErr, prevKVErr: prevKVErr, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, kvErr: kvErr, prevKV: prevKV, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
		{typ: mvccpb.DELETE, kvErr: kvErr, prevKV: prevKV, prevKVErr: prevKVErr, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, kv: kv, prevKVErr: prevKVErr, matchErr: MatchError(prevKVErr), matchEventPtr: BeNil()},
		{typ: mvccpb.DELETE, kv: kv, prevKV: prevKV, matchErr: Succeed(), matchEventPtr: Equal(&mvccpb.Event{Type: mvccpb.DELETE, Kv: kv, PrevKv: prevKV})},
		{typ: mvccpb.DELETE, kv: kv, prevKV: prevKV, prevKVErr: prevKVErr, matchErr: MatchError(prevKVErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, kv: kv, kvErr: kvErr, prevKVErr: prevKVErr, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
		{typ: mvccpb.PUT, kv: kv, kvErr: kvErr, prevKV: prevKV, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
		{typ: mvccpb.DELETE, kv: kv, kvErr: kvErr, prevKV: prevKV, prevKVErr: prevKVErr, matchErr: MatchError(kvErr), matchEventPtr: BeNil()},
	} {
		func(s spec) {
			Describe(
				fmt.Sprintf(
					"typ=%d, kv=%v, kvErr=%v, prevKV=%v, prefKVErr=%v",
					s.typ, s.kv, s.kvErr, s.prevKV, s.prevKVErr,
				),
				func() {
					It(ItSpecForMatchError(s.matchErr), func() {
						var (
							kvFn, prevKVFn kvLoaderFunc
							ev             *mvccpb.Event
							err            error
						)

						if s.kv != nil || s.kvErr != nil {
							kvFn = func() (*mvccpb.KeyValue, error) { return s.kv, s.kvErr }
						}

						if s.prevKV != nil || s.prevKVErr != nil {
							prevKVFn = func() (*mvccpb.KeyValue, error) { return s.prevKV, s.prevKVErr }
						}

						ev, err = buildEvent(s.typ, kvFn, prevKVFn)

						Expect(err).To(s.matchErr, "err")
						Expect(ev).To(s.matchEventPtr, "event")
					})
				},
			)
		}(s)
	}
})

var _ = Describe("backend", func() {
	var (
		ctx context.Context
		dir string
		b   *backend
	)

	BeforeEach(func() {
		var gitImpl = git2go.New()

		b = &backend{}
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

	Describe("getKeyValueLoader", func() {
		type spec struct {
			ctxFn              ContextFunc
			keyPrefix          string
			key                string
			metaRoot, dataRoot *TreeDef
			matchErr, matchKV  types.GomegaMatcher
		}

		var (
			dataRoot = &TreeDef{
				Blobs: map[string][]byte{
					"no-metadata":   []byte("no-metadata"),
					"no-lease":      []byte("no-lease"),
					"full-metadata": []byte("full-metadata"),
				},
				Subtrees: map[string]TreeDef{
					"deep": {Blobs: map[string][]byte{
						"no-metadata":   []byte("no-metadata"),
						"no-lease":      []byte("no-lease"),
						"full-metadata": []byte("full-metadata"),
					}},
				},
			}

			metaRoot = &TreeDef{Subtrees: map[string]TreeDef{
				"no-data": {Blobs: map[string][]byte{
					etcdserverpb.Compare_CREATE.String():  []byte("1"),
					etcdserverpb.Compare_MOD.String():     []byte("2"),
					etcdserverpb.Compare_LEASE.String():   []byte("3"),
					etcdserverpb.Compare_VERSION.String(): []byte("4"),
				}},
				"no-lease": {Blobs: map[string][]byte{
					etcdserverpb.Compare_CREATE.String():  []byte("2"),
					etcdserverpb.Compare_MOD.String():     []byte("3"),
					etcdserverpb.Compare_VERSION.String(): []byte("5"),
				}},
				"full-metadata": {Blobs: map[string][]byte{
					etcdserverpb.Compare_CREATE.String():  []byte("3"),
					etcdserverpb.Compare_MOD.String():     []byte("4"),
					etcdserverpb.Compare_LEASE.String():   []byte("5"),
					etcdserverpb.Compare_VERSION.String(): []byte("6"),
				}},
				"deep": {Subtrees: map[string]TreeDef{
					"no-data": {Blobs: map[string][]byte{
						etcdserverpb.Compare_CREATE.String():  []byte("1"),
						etcdserverpb.Compare_MOD.String():     []byte("2"),
						etcdserverpb.Compare_LEASE.String():   []byte("3"),
						etcdserverpb.Compare_VERSION.String(): []byte("4"),
					}},
					"no-lease": {Blobs: map[string][]byte{
						etcdserverpb.Compare_CREATE.String():  []byte("2"),
						etcdserverpb.Compare_MOD.String():     []byte("3"),
						etcdserverpb.Compare_VERSION.String(): []byte("5"),
					}},
					"full-metadata": {Blobs: map[string][]byte{
						etcdserverpb.Compare_CREATE.String():  []byte("3"),
						etcdserverpb.Compare_MOD.String():     []byte("4"),
						etcdserverpb.Compare_LEASE.String():   []byte("5"),
						etcdserverpb.Compare_VERSION.String(): []byte("6"),
					}},
				}},
			}}
		)

		for _, s := range []spec{
			{
				ctxFn:    CanceledContext,
				metaRoot: &TreeDef{},
				dataRoot: &TreeDef{},
				matchErr: MatchError(context.Canceled),
				matchKV:  BeNil(),
			},
			{
				ctxFn:    CanceledContext,
				key:      "no-key",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: MatchError(context.Canceled),
				matchKV:  BeNil(),
			},
			{
				key:      "no-metadata",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:  BeNil(),
			},
			{
				key:      "no-data",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:  BeNil(),
			},
			{
				key:      "no-lease",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("no-lease"),
					CreateRevision: 2,
					ModRevision:    3,
					Version:        5,
					Value:          []byte("no-lease"),
				}),
			},
			{
				key:      "full-metadata",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("full-metadata"),
					CreateRevision: 3,
					ModRevision:    4,
					Lease:          5,
					Version:        6,
					Value:          []byte("full-metadata"),
				}),
			},
			{
				key:      "deep/no-metadata",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:  BeNil(),
			},
			{
				key:      "deep/no-data",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:  BeNil(),
			},
			{
				key:      "deep/no-lease",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("deep/no-lease"),
					CreateRevision: 2,
					ModRevision:    3,
					Version:        5,
					Value:          []byte("no-lease"),
				}),
			},
			{
				key:      "deep/full-metadata",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("deep/full-metadata"),
					CreateRevision: 3,
					ModRevision:    4,
					Lease:          5,
					Version:        6,
					Value:          []byte("full-metadata"),
				}),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/no-metadata",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:   BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/no-data",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:   BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/no-lease",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("/registry/no-lease"),
					CreateRevision: 2,
					ModRevision:    3,
					Version:        5,
					Value:          []byte("no-lease"),
				}),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/full-metadata",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("/registry/full-metadata"),
					CreateRevision: 3,
					ModRevision:    4,
					Lease:          5,
					Version:        6,
					Value:          []byte("full-metadata"),
				}),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/deep/no-metadata",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:   BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/deep/no-data",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:   BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/deep/no-lease",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("/registry/deep/no-lease"),
					CreateRevision: 2,
					ModRevision:    3,
					Version:        5,
					Value:          []byte("no-lease"),
				}),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/deep/full-metadata",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  Succeed(),
				matchKV: Equal(&mvccpb.KeyValue{
					Key:            []byte("/registry/deep/full-metadata"),
					CreateRevision: 3,
					ModRevision:    4,
					Lease:          5,
					Version:        6,
					Value:          []byte("full-metadata"),
				}),
			},
			{
				key:      "no-key",
				metaRoot: metaRoot,
				dataRoot: dataRoot,
				matchErr: PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:  BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "no-key",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:   BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "/no-key",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:   BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/deep",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  MatchError(ContainSubstring("unsupported ObjectType")),
				matchKV:   BeNil(),
			},
			{
				keyPrefix: "/registry",
				key:       "/registry/deep/no-key",
				metaRoot:  metaRoot,
				dataRoot:  dataRoot,
				matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
				matchKV:   BeNil(),
			},
		} {
			func(s spec) {
				Describe(
					fmt.Sprintf(
						"expired context=%t, keyPrefix=%s, key=%s, metaRoot=%v, dataRoot=%v",
						s.ctxFn != nil, s.keyPrefix, s.key, s.metaRoot, s.dataRoot,
					),
					func() {
						var metaRoot, dataRoot git.Tree

						BeforeEach(func() {
							b.keyPrefix.prefix = s.keyPrefix

							for _, t := range []struct {
								tPtr *git.Tree
								td   *TreeDef
							}{
								{tPtr: &dataRoot, td: s.dataRoot},
								{tPtr: &metaRoot, td: s.metaRoot},
							} {
								if t.td == nil {
									continue
								}

								Expect(func() (err error) {
									*t.tPtr, err = CreateAndLoadTreeFromDef(ctx, b.repo, t.td)
									return
								}()).To(Succeed())
							}

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}
						})

						AfterEach(func() {
							for _, t := range []git.Tree{dataRoot, metaRoot} {
								if t != nil {
									Expect(t.Close()).To(Succeed())
								}
							}
						})

						It(ItSpecForMatchError(s.matchErr), func() {
							var kv, err = b.getKeyValueLoader(ctx, s.key, metaRoot, dataRoot)()

							Expect(err).To(s.matchErr, "err")
							Expect(kv).To(s.matchKV, "kv")
						})
					},
				)
			}(s)
		}
	})
})

var _ = Describe("watchManager", func() {
	var wm *watchManager

	BeforeEach(func() {
		wm = &watchManager{log: getTestLogger()}
	})

	Describe("enqueue", func() {
		type spec struct {
			queue, rws []*revisionWatcher
			matchQueue types.GomegaMatcher
		}

		for _, s := range []spec{
			{matchQueue: BeNil()},
			func() spec {
				var rws = []*revisionWatcher{{revision: 2}, {revision: 1}, {revision: 4}}
				return spec{rws: rws, matchQueue: Equal(rws)}
			}(),
			func() spec {
				var queue = []*revisionWatcher{{revision: 5}, {revision: 2}, {revision: 6}}
				return spec{queue: queue, matchQueue: Equal(queue)}
			}(),
			func() spec {
				var (
					queue = []*revisionWatcher{{revision: 5}, {revision: 2}, {revision: 6}}
					rws   = []*revisionWatcher{{revision: 2}, {revision: 1}, {revision: 4}}
				)
				return spec{queue: queue, rws: rws, matchQueue: Equal(append(append([]*revisionWatcher{}, queue...), rws...))}
			}(),
		} {
			func(s spec) {
				Describe(fmt.Sprintf("queue=%v, rws=%v", s.queue, s.rws), func() {
					BeforeEach(func() {
						wm.queue = s.queue
					})

					It("should succeed", func() {
						wm.enqueue(s.rws...)
						Expect(wm.queue).To(s.matchQueue)
					})
				})
			}(s)
		}
	})

	Describe("groupQueue", func() {
		type spec struct {
			spec       string
			queue      []*revisionWatcher
			matchQueue types.GomegaMatcher
		}

		for _, s := range []spec{
			{spec: "nil", matchQueue: BeNil()},
			func() spec {
				var (
					ctx = context.Background()
					q   = []*revisionWatcher{{
						revision: 2,
						watches: []watch{
							&watchImpl{ctx: ctx, req: &etcdserverpb.WatchCreateRequest{WatchId: 2}},
						},
					}}
				)
				return spec{spec: "single entry", queue: q, matchQueue: Equal(q)}
			}(),
			func() spec {
				var (
					ctx = context.Background()
					q   = []*revisionWatcher{
						{
							revision: 2,
							watches: []watch{
								&watchImpl{ctx: ctx, req: &etcdserverpb.WatchCreateRequest{WatchId: 2}},
							},
						},
						{
							changesOnly: true,
							revision:    2,
							watches: []watch{
								&watchImpl{ctx: ctx, req: &etcdserverpb.WatchCreateRequest{WatchId: 22}},
							},
						},
					}
				)
				return spec{spec: "same revision, different changesOnly", queue: q, matchQueue: ConsistOf(q[0], q[1])}
			}(),
			func() spec {
				var (
					ctx = context.Background()
					q   = []*revisionWatcher{
						{
							revision: 2,
							interval: &closedOpenInterval{start: key("p"), end: key("r")},
							watches: []watch{
								&watchImpl{ctx: ctx, req: &etcdserverpb.WatchCreateRequest{WatchId: 2}},
							},
						},
						{
							revision: 2,
							interval: &closedOpenInterval{start: key("q"), end: key("s")},
							watches: []watch{
								&watchImpl{ctx: ctx, req: &etcdserverpb.WatchCreateRequest{WatchId: 22}},
							},
						},
					}
				)
				return spec{
					spec:  "same revision, no changesOnly",
					queue: q, matchQueue: Equal([]*revisionWatcher{{
						revision: q[0].revision,
						interval: &closedOpenInterval{start: key("p"), end: key("s")},
						watches:  append(append([]watch{}, q[0].watches...), q[1].watches...),
					}}),
				}
			}(),
			func() spec {
				var (
					ctx = context.Background()
					q   = []*revisionWatcher{
						{
							changesOnly: true,
							revision:    2,
							interval:    &closedOpenInterval{start: key("p"), end: key("r")},
							watches: []watch{
								&watchImpl{ctx: ctx, req: &etcdserverpb.WatchCreateRequest{WatchId: 2}},
							},
						},
						{
							changesOnly: true,
							revision:    2,
							interval:    &closedOpenInterval{start: key("l"), end: key("q")},
							watches: []watch{
								&watchImpl{ctx: ctx, req: &etcdserverpb.WatchCreateRequest{WatchId: 22}},
							},
						},
					}
				)
				return spec{
					spec:  "same revision, changesOnly",
					queue: q, matchQueue: Equal([]*revisionWatcher{{
						changesOnly: q[0].changesOnly,
						revision:    q[0].revision,
						interval:    &closedOpenInterval{start: key("l"), end: key("r")},
						watches:     append(append([]watch{}, q[0].watches...), q[1].watches...),
					}}),
				}
			}(),
			func() spec {
				var (
					nonExpiredCtx = context.Background()
					expiredCtx    = CanceledContext(nonExpiredCtx)
					q             = []*revisionWatcher{
						{
							revision: 3,
							interval: &closedOpenInterval{start: key("p"), end: key("r")},
							watches: []watch{
								&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 2}},
								&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 22}},
								&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 222}},
							},
						},
						{
							revision: 3,
							interval: &closedOpenInterval{start: key("l"), end: key("s")},
							watches: []watch{
								&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 3}},
								&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 33}},
								&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 333}},
								&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 3334}},
							},
						},
					}
				)

				return spec{
					spec:  "same revision, no changesOnly, expired contexts",
					queue: q, matchQueue: Equal([]*revisionWatcher{{
						revision: q[0].revision,
						interval: &closedOpenInterval{start: key("l"), end: key("s")},
						watches:  append(append([]watch{}, q[0].watches...), q[1].watches[1], q[1].watches[3]),
					}}),
				}
			}(),
			func() spec {
				var (
					nonExpiredCtx = context.Background()
					expiredCtx    = CanceledContext(nonExpiredCtx)

					revision2changesOnlyAB = &revisionWatcher{
						revision:    2,
						changesOnly: true,
						interval:    &closedOpenInterval{start: key("a"), end: key("b")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 21}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 2121}},
						},
					}
					revision2changesOnlyNilA = &revisionWatcher{
						revision:    2,
						changesOnly: true,
						interval:    &closedOpenInterval{end: key("a")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 212121}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 21212121}},
						},
					}

					revision2noChangesNullNull = &revisionWatcher{
						revision: 2,
						interval: &closedOpenInterval{start: key("\x00"), end: key("\x00")},
						watches: []watch{
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 20}},
						},
					}

					revision2noChangesPQ = &revisionWatcher{
						revision: 2,
						interval: &closedOpenInterval{start: key("p"), end: key("q")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 212121}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 21212121}},
						},
					}

					revision1noChangesOnlyNilNil = &revisionWatcher{
						revision: 1,
						interval: &closedOpenInterval{},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 10}},
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 1010}},
						},
					}

					revision1noChangesOnlyAB = &revisionWatcher{
						revision: 1,
						interval: &closedOpenInterval{start: key("a"), end: key("b")},
						watches: []watch{
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 101010}},
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 10101010}},
						},
					}

					revision1changesOnlyXY = &revisionWatcher{
						revision:    1,
						changesOnly: true,
						interval:    &closedOpenInterval{start: key("x"), end: key("y")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 11}},
						},
					}

					revision1changesOnlyNullNull = &revisionWatcher{
						revision:    1,
						changesOnly: true,
						interval:    &closedOpenInterval{start: key("\x00"), end: key("\x00")},
						watches: []watch{
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 111111}},
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 11111111}},
						},
					}

					revision3changesOnlyQNil = &revisionWatcher{
						revision:    3,
						changesOnly: true,
						interval:    &closedOpenInterval{start: key("q")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 31}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 3131}},
						},
					}

					revision3changesOnlyRS = &revisionWatcher{
						revision:    3,
						changesOnly: true,
						interval:    &closedOpenInterval{start: key("r"), end: key("s")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 313131}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 313131}},
						},
					}

					revision3noChangesOnlyCNil = &revisionWatcher{
						revision: 3,
						interval: &closedOpenInterval{start: key("c")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 30}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 3030}},
						},
					}

					revision3noChangesOnlyPQ = &revisionWatcher{
						revision: 3,
						interval: &closedOpenInterval{start: key("p"), end: key("q")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 303030}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 30303030}},
						},
					}

					revision3noChangesOnlyBD = &revisionWatcher{
						revision: 3,
						interval: &closedOpenInterval{start: key("b"), end: key("d")},
						watches: []watch{
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 303030303030}},
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 30303030}},
						},
					}

					revision4changesOnlyLM = &revisionWatcher{
						revision:    4,
						changesOnly: true,
						interval:    &closedOpenInterval{start: key("l"), end: key("m")},
						watches: []watch{
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 41}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 4141}},
						},
					}

					revision4noChangesOnlyNilNil = &revisionWatcher{
						revision: 4,
						interval: &closedOpenInterval{},
						watches: []watch{
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 4040}},
							&watchImpl{ctx: nonExpiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 40}},
						},
					}

					revision5noChangesOnlyNullNil = &revisionWatcher{
						revision: 5,
						interval: &closedOpenInterval{start: key("\x00")},
						watches: []watch{
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 40}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 4040}},
							&watchImpl{ctx: expiredCtx, req: &etcdserverpb.WatchCreateRequest{WatchId: 404040}},
						},
					}

					q = []*revisionWatcher{
						revision2changesOnlyAB,
						revision3noChangesOnlyCNil,
						revision5noChangesOnlyNullNil,
						revision1changesOnlyXY,
						revision3changesOnlyQNil,
						revision1noChangesOnlyAB,
						revision3changesOnlyRS,
						revision4noChangesOnlyNilNil,
						revision3noChangesOnlyPQ,
						revision4changesOnlyLM,
						revision2changesOnlyNilA,
						revision1changesOnlyNullNull,
						revision2noChangesPQ,
						revision3noChangesOnlyBD,
						revision2noChangesNullNull,
						revision1noChangesOnlyNilNil,
					}

					matchQueue = ConsistOf(
						&revisionWatcher{
							revision:    revision2changesOnlyAB.revision,
							changesOnly: revision2changesOnlyAB.changesOnly,
							interval:    &closedOpenInterval{end: key("b")},
							watches:     append(append([]watch{}, revision2changesOnlyAB.watches...), revision2changesOnlyNilA.watches[0]),
						},
						&revisionWatcher{
							revision:    revision3noChangesOnlyCNil.revision,
							changesOnly: revision3noChangesOnlyCNil.changesOnly,
							interval:    &closedOpenInterval{start: key("b"), end: key("q")},
							watches: append(
								append(
									append([]watch{}, revision3noChangesOnlyCNil.watches...),
									revision3noChangesOnlyPQ.watches[0],
								),
								revision3noChangesOnlyBD.watches[1],
							),
						},
						revision5noChangesOnlyNullNil,
						&revisionWatcher{
							revision:    revision1changesOnlyXY.revision,
							changesOnly: revision1changesOnlyXY.changesOnly,
							interval:    &closedOpenInterval{start: key("\x00"), end: key("y")},
							watches:     append(append([]watch{}, revision1changesOnlyXY.watches...), revision1changesOnlyNullNull.watches[1]),
						},
						&revisionWatcher{
							revision:    revision3changesOnlyQNil.revision,
							changesOnly: revision3changesOnlyQNil.changesOnly,
							interval:    &closedOpenInterval{start: key("q"), end: key("s")},
							watches:     append(append([]watch{}, revision3changesOnlyQNil.watches...), revision3changesOnlyRS.watches[0]),
						},
						&revisionWatcher{
							revision:    revision1noChangesOnlyAB.revision,
							changesOnly: revision1noChangesOnlyAB.changesOnly,
							interval:    &closedOpenInterval{end: key("b")},
							watches:     append(append([]watch{}, revision1noChangesOnlyAB.watches...), revision1noChangesOnlyNilNil.watches...),
						},
						revision4noChangesOnlyNilNil,
						revision4changesOnlyLM,
						&revisionWatcher{
							revision:    revision2noChangesPQ.revision,
							changesOnly: revision2noChangesPQ.changesOnly,
							interval:    &closedOpenInterval{start: key("\x00"), end: key("q")},
							watches:     append([]watch{}, revision2noChangesPQ.watches...),
						},
					)
				)

				return spec{spec: "mixed case", queue: q, matchQueue: matchQueue}
			}(),
		} {
			func(s spec) {
				Describe(s.spec, func() {
					It("Should succeed", func() {
						Expect(wm.groupQueue(s.queue)).To(s.matchQueue)
					})
				})
			}(s)
		}
	})

	Describe("backend", func() {
		var (
			ctx context.Context
			dir string
		)

		BeforeEach(func() {
			var gitImpl = git2go.New()

			wm.backend = &backend{}
			ctx = context.Background()

			Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
			Expect(dir).ToNot(BeEmpty())

			Expect(func() (err error) { wm.backend.repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
			Expect(wm.backend.repo).ToNot(BeNil())

			wm.backend.errors = gitImpl.Errors()
		})

		AfterEach(func() {
			if wm.backend.repo != nil {
				Expect(wm.backend.repo.Close()).To(Succeed())
			}

			if len(dir) > 0 {
				Expect(os.RemoveAll(dir))
			}
		})

		Describe("rwSpec", func() {
			type rwSpec struct {
				rw      *revisionWatcher
				watches []watchSpec
			}

			var (
				ctrl *gomock.Controller

				prepareRevisionWatchers = func(ctxFn ContextFunc, rwSpecs []rwSpec) (rws []*revisionWatcher) {
					for _, rwSpec := range rwSpecs {
						var rw = rwSpec.rw

						rw.backend = wm.backend
						rw.watches = append(rw.watches, prepareWatches(ctrl, ctxFn, rwSpec.watches)...)
						rws = append(rws, rw)
					}

					return
				}
			)

			BeforeEach(func() {
				ctrl = gomock.NewController(GinkgoT())
			})

			Describe("dispatchQueue", func() {
				type spec struct {
					spec                 string
					ctxFn                ContextFunc
					metaRefName          git.ReferenceName
					metaHead             metaHeadFunc
					queue                []rwSpec
					matchErr, matchQueue types.GomegaMatcher
				}

				for _, s := range []spec{
					{
						spec:       "context expired",
						ctxFn:      CanceledContext,
						matchErr:   MatchError(context.Canceled),
						matchQueue: BeNil(),
					},
					{
						spec:       "nil queue",
						matchErr:   Succeed(),
						matchQueue: BeNil(),
					},
					{
						spec:       "no metaRefName",
						queue:      []rwSpec{{rw: &revisionWatcher{}, watches: []watchSpec{{makerFn: newMockWatch}}}},
						matchErr:   MatchError(rpctypes.ErrGRPCCorrupt),
						matchQueue: BeNil(),
					},
					func() spec {
						var (
							revision    = int64(1)
							strRevision = revisionToString(revision)
						)
						return spec{
							spec:        "no data",
							metaRefName: git.ReferenceName("refs/gitcd/metadata/main"),
							metaHead: metaHeadFrom(
								&CommitDef{
									Message: strRevision,
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte(strRevision)}},
								},
								nil,
							),
							queue:      []rwSpec{{rw: &revisionWatcher{}, watches: []watchSpec{{makerFn: newMockWatch}}}},
							matchErr:   ConsistOf(PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))),
							matchQueue: BeNil(),
						}
					}(),
				} {
					func(s spec) {
						Describe(
							fmt.Sprintf("%s: expired context=%t, metaRefName=%s, queue=%v", s.spec, s.ctxFn != nil, s.metaRefName, s.queue),
							func() {
								var queue []*revisionWatcher

								BeforeEach(func() {
									wm.backend.metadataRefName = s.metaRefName

									if s.metaHead != nil {

									}

									queue = prepareRevisionWatchers(s.ctxFn, s.queue)

									if s.ctxFn != nil {
										ctx = s.ctxFn(ctx)
									}
								})

								AfterEach(func() {
									for _, rw := range queue {
										cancelOpenWatches(rw.watches)
									}
								})

								It(ItSpecForMatchError(s.matchErr), func() {
									var nextQ, err = wm.dispatchQueue(ctx, queue)

									fmt.Println(err)
									Expect(err).To(s.matchErr, "err")
									Expect(nextQ).To(s.matchQueue, "nextQ")
								})
							},
						)
					}(s)
				}
			})
		})
	})
})
