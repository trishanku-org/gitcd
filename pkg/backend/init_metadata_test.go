package backend

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	impl "github.com/libgit2/git2go/v31"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

var _ = Describe("backend", func() {
	var b *backend

	BeforeEach(func() {
		b = &backend{}
	})

	Describe("repo", func() {
		var (
			ctx context.Context
			dir string
		)

		BeforeEach(func() {
			var gitImpl = git2go.New()
			ctx = context.Background()

			Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
			Expect(dir).ToNot(BeEmpty())

			Expect(func() (err error) { b.repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
			Expect(b.repo).ToNot(BeNil())

			Expect(func() git.Errors { b.errors = gitImpl.Errors(); return b.errors }()).ToNot(BeNil())
		})

		AfterEach(func() {
			if b.repo != nil {
				Expect(b.repo.Close()).To(Succeed())
			}

			if len(dir) > 0 {
				Expect(os.RemoveAll(dir))
			}
		})

		Describe("getHead", func() {
			type spec struct {
				spec                        string
				ctxFn                       ContextFunc
				refName                     git.ReferenceName
				createReferenceFn           func(context.Context, git.Repository, git.ReferenceCollection) error
				matchErr, matchCommitDefPtr types.GomegaMatcher
			}

			for _, s := range []spec{
				{spec: "invalid context", ctxFn: CanceledContext, matchErr: MatchError(context.Canceled), matchCommitDefPtr: BeNil()},
				{
					spec:              "invalid reference name",
					refName:           "invalid reference name",
					matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeInvalidSpec)})),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "non-exitent reference",
					refName:           "refs/heads/non-existent",
					matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
					matchCommitDefPtr: BeNil(),
				},
				func() spec {
					var refName = git.ReferenceName("refs/heads/ref-to-tree")

					return spec{
						spec:    "reference to a non-commit",
						refName: refName,
						createReferenceFn: func(ctx context.Context, repo git.Repository, rc git.ReferenceCollection) (err error) {
							var id git.ObjectID

							if id, err = CreateTreeFromDef(ctx, repo, &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}); err != nil {
								return
							}

							return rc.Create(ctx, refName, id, false, string(refName))
						},
						matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeInvalidSpec)})),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						refName = git.ReferenceName("refs/heads/ref-to-commit")
						cd      = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					)

					return spec{
						spec:    "reference to a commit",
						refName: refName,
						createReferenceFn: func(ctx context.Context, repo git.Repository, rc git.ReferenceCollection) (err error) {
							var id git.ObjectID

							if id, err = CreateCommitFromDef(ctx, repo, cd); err != nil {
								return
							}

							return rc.Create(ctx, refName, id, false, string(refName))
						},
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(cd)),
					}
				}(),
			} {
				func(
					spec string,
					ctxFn ContextFunc,
					refName git.ReferenceName,
					createReferenceFn func(context.Context, git.Repository, git.ReferenceCollection) error,
					matchErr, matchCommitDefPtr types.GomegaMatcher,
				) {
					Describe(fmt.Sprintf("%s: canceled context=%t, refName=%s", spec, ctxFn != nil, refName), func() {
						var parentCtx context.Context

						BeforeEach(func() {
							if createReferenceFn != nil {
								var rc, err = b.repo.References()

								Expect(err).ToNot(HaveOccurred())
								Expect(rc).ToNot(BeNil())

								defer rc.Close()

								Expect(createReferenceFn(ctx, b.repo, rc)).To(Succeed())
							}

							parentCtx = ctx
							if ctxFn != nil {
								ctx = ctxFn(ctx)
							}
						})

						It(ItSpecForMatchError(matchErr), func() {
							var c, err = b.getHead(ctx, refName)

							Expect(err).To(matchErr)
							Expect(func() *CommitDef {
								if c == nil {
									return nil
								}

								return GetCommitDefByCommit(parentCtx, b.repo, c)
							}()).To(matchCommitDefPtr)
						})
					})
				}(s.spec, s.ctxFn, s.refName, s.createReferenceFn, s.matchErr, s.matchCommitDefPtr)
			}
		})

		Describe("getDataHead", func() {
			type spec struct {
				spec                        string
				ctxFn                       ContextFunc
				refName                     git.ReferenceName
				createReferenceFn           func(context.Context, git.Repository, git.ReferenceCollection) error
				matchErr, matchCommitDefPtr types.GomegaMatcher
			}

			for _, s := range []spec{
				{spec: "invalid context", ctxFn: CanceledContext, matchErr: MatchError(rpctypes.ErrGRPCCorrupt), matchCommitDefPtr: BeNil()},
				{
					spec:              "invalid context, invalid reference name",
					ctxFn:             CanceledContext,
					refName:           "invalid reference name",
					matchErr:          MatchError(context.Canceled),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "invalid context, valid reference name",
					ctxFn:             CanceledContext,
					refName:           "refs/heads/valid",
					matchErr:          MatchError(context.Canceled),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "invalid reference name",
					refName:           "invalid reference name",
					matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeInvalidSpec)})),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "non-exitent reference",
					refName:           "refs/heads/non-existent",
					matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
					matchCommitDefPtr: BeNil(),
				},
				func() spec {
					var refName = git.ReferenceName("refs/heads/ref-to-tree")

					return spec{
						spec:    "reference to a non-commit",
						refName: refName,
						createReferenceFn: func(ctx context.Context, repo git.Repository, rc git.ReferenceCollection) (err error) {
							var id git.ObjectID

							if id, err = CreateTreeFromDef(ctx, repo, &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}); err != nil {
								return
							}

							return rc.Create(ctx, refName, id, false, string(refName))
						},
						matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeInvalidSpec)})),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						refName = git.ReferenceName("refs/heads/ref-to-commit")
						cd      = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					)

					return spec{
						spec:    "reference to a commit",
						refName: refName,
						createReferenceFn: func(ctx context.Context, repo git.Repository, rc git.ReferenceCollection) (err error) {
							var id git.ObjectID

							if id, err = CreateCommitFromDef(ctx, repo, cd); err != nil {
								return
							}

							return rc.Create(ctx, refName, id, false, string(refName))
						},
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(cd)),
					}
				}(),
			} {
				func(
					spec string,
					ctxFn ContextFunc,
					refName git.ReferenceName,
					createReferenceFn func(context.Context, git.Repository, git.ReferenceCollection) error,
					matchErr, matchCommitDefPtr types.GomegaMatcher,
				) {
					Describe(fmt.Sprintf("%s: canceled context=%t, refName=%s", spec, ctxFn != nil, refName), func() {
						var parentCtx context.Context

						BeforeEach(func() {
							b.refName = refName

							if createReferenceFn != nil {
								var rc, err = b.repo.References()

								Expect(err).ToNot(HaveOccurred())
								Expect(rc).ToNot(BeNil())

								defer rc.Close()

								Expect(createReferenceFn(ctx, b.repo, rc)).To(Succeed())
							}

							parentCtx = ctx
							if ctxFn != nil {
								ctx = ctxFn(ctx)
							}
						})

						It(ItSpecForMatchError(matchErr), func() {
							var c, err = b.getDataHead(ctx)

							Expect(err).To(matchErr)
							Expect(func() *CommitDef {
								if c == nil {
									return nil
								}

								return GetCommitDefByCommit(parentCtx, b.repo, c)
							}()).To(matchCommitDefPtr)
						})
					})
				}(s.spec, s.ctxFn, s.refName, s.createReferenceFn, s.matchErr, s.matchCommitDefPtr)
			}
		})

		Describe("getMetadataHead", func() {
			type spec struct {
				spec                        string
				ctxFn                       ContextFunc
				refName                     git.ReferenceName
				createReferenceFn           func(context.Context, git.Repository, git.ReferenceCollection) error
				matchErr, matchCommitDefPtr types.GomegaMatcher
			}

			for _, s := range []spec{
				{spec: "invalid context", ctxFn: CanceledContext, matchErr: MatchError(rpctypes.ErrGRPCCorrupt), matchCommitDefPtr: BeNil()},
				{
					spec:              "invalid context, invalid reference name",
					ctxFn:             CanceledContext,
					refName:           "invalid reference name",
					matchErr:          MatchError(context.Canceled),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "invalid context, valid reference name",
					ctxFn:             CanceledContext,
					refName:           "refs/heads/valid",
					matchErr:          MatchError(context.Canceled),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "invalid reference name",
					refName:           "invalid reference name",
					matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeInvalidSpec)})),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "non-exitent reference",
					refName:           "refs/heads/non-existent",
					matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
					matchCommitDefPtr: BeNil(),
				},
				func() spec {
					var refName = git.ReferenceName("refs/heads/ref-to-tree")

					return spec{
						spec:    "reference to a non-commit",
						refName: refName,
						createReferenceFn: func(ctx context.Context, repo git.Repository, rc git.ReferenceCollection) (err error) {
							var id git.ObjectID

							if id, err = CreateTreeFromDef(ctx, repo, &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}); err != nil {
								return
							}

							return rc.Create(ctx, refName, id, false, string(refName))
						},
						matchErr:          PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeInvalidSpec)})),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() spec {
					var (
						refName = git.ReferenceName("refs/heads/ref-to-commit")
						cd      = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					)

					return spec{
						spec:    "reference to a commit",
						refName: refName,
						createReferenceFn: func(ctx context.Context, repo git.Repository, rc git.ReferenceCollection) (err error) {
							var id git.ObjectID

							if id, err = CreateCommitFromDef(ctx, repo, cd); err != nil {
								return
							}

							return rc.Create(ctx, refName, id, false, string(refName))
						},
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(cd)),
					}
				}(),
			} {
				func(
					spec string,
					ctxFn ContextFunc,
					refName git.ReferenceName,
					createReferenceFn func(context.Context, git.Repository, git.ReferenceCollection) error,
					matchErr, matchCommitDefPtr types.GomegaMatcher,
				) {
					Describe(fmt.Sprintf("%s: canceled context=%t, refName=%s", spec, ctxFn != nil, refName), func() {
						var parentCtx context.Context

						BeforeEach(func() {
							b.metadataRefName = refName

							if createReferenceFn != nil {
								var rc, err = b.repo.References()

								Expect(err).ToNot(HaveOccurred())
								Expect(rc).ToNot(BeNil())

								defer rc.Close()

								Expect(createReferenceFn(ctx, b.repo, rc)).To(Succeed())
							}

							parentCtx = ctx
							if ctxFn != nil {
								ctx = ctxFn(ctx)
							}
						})

						It(ItSpecForMatchError(matchErr), func() {
							var c, err = b.getMetadataHead(ctx)

							Expect(err).To(matchErr)
							Expect(func() *CommitDef {
								if c == nil {
									return nil
								}

								return GetCommitDefByCommit(parentCtx, b.repo, c)
							}()).To(matchCommitDefPtr)
						})
					})
				}(s.spec, s.ctxFn, s.refName, s.createReferenceFn, s.matchErr, s.matchCommitDefPtr)
			}
		})

		Describe("loadDataMetadataMapping", func() {
			type spec struct {
				spec                     string
				ctxFn                    ContextFunc
				metaHead                 metaHeadFunc
				dmMapping                map[git.ObjectID]git.ObjectID
				dmMappingFn              func(context.Context, git.Repository) (map[git.ObjectID]git.ObjectID, error)
				matchErr, matchDMMapping types.GomegaMatcher
				expectDMMappingFn        func(context.Context, git.Repository) (map[git.ObjectID]expectMetaHeadFunc, error)
			}

			type dmPair struct {
				mcd, dcd *CommitDef
			}

			var (
				expectDMMappingFnFor = func(dmPairs ...dmPair) func(ctx context.Context, repo git.Repository) (dmMapping map[git.ObjectID]expectMetaHeadFunc, err error) {
					return func(ctx context.Context, repo git.Repository) (dmMapping map[git.ObjectID]expectMetaHeadFunc, err error) {
						dmMapping = make(map[git.ObjectID]expectMetaHeadFunc, 3)

						for _, s := range dmPairs {
							var dID git.ObjectID

							if dID, err = CreateCommitFromDef(ctx, repo, s.dcd); err != nil {
								return
							}

							dmMapping[dID] = expectMetaHead(s.mcd, s.dcd, true)
						}

						return
					}
				}
			)

			for _, s := range []spec{
				{spec: "invalid context, no commits", ctxFn: CanceledContext, matchErr: Succeed(), matchDMMapping: BeNil()},
				{
					spec:           "invalid context, with a commit",
					ctxFn:          CanceledContext,
					metaHead:       metaHeadFrom(&CommitDef{Message: "1"}, nil),
					matchErr:       MatchError(context.Canceled),
					matchDMMapping: BeNil(),
				},
				{
					spec:           "no data path",
					metaHead:       metaHeadFrom(&CommitDef{Message: "1"}, nil),
					matchErr:       PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
					matchDMMapping: BeNil(),
				},
				{
					spec: "invalid data ID",
					metaHead: metaHeadFrom(
						&CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("invalid")}}},
						nil,
					),
					matchErr:       MatchError(ContainSubstring("invalid ObjectID")),
					matchDMMapping: BeNil(),
				},
				func() spec {
					var (
						strRevision = "1"
						dID         = git.ObjectID{}
						mcd         = &CommitDef{Message: strRevision, Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte(dID.String())}}}
					)

					return spec{
						spec:      "empty data ID",
						metaHead:  metaHeadFrom(mcd, nil),
						dmMapping: map[git.ObjectID]git.ObjectID{},
						matchErr:  Succeed(),
						expectDMMappingFn: func(ctx context.Context, repo git.Repository) (dmMapping map[git.ObjectID]expectMetaHeadFunc, err error) {
							dmMapping = map[git.ObjectID]expectMetaHeadFunc{dID: expectMetaHead(mcd, nil, true)}
							return
						},
					}
				}(),
				func() spec {
					var (
						strRevision = "1"
						dcd         = &CommitDef{Message: strRevision, Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
						mcd         = &CommitDef{Message: strRevision}
					)

					return spec{
						spec:              "existing data ID",
						metaHead:          metaHeadFrom(mcd, dcd),
						dmMapping:         map[git.ObjectID]git.ObjectID{},
						matchErr:          Succeed(),
						expectDMMappingFn: expectDMMappingFnFor(dmPair{mcd: mcd, dcd: dcd}),
					}
				}(),
				func() spec {
					var (
						d123 = &CommitDef{Message: "123",
							Tree: TreeDef{Blobs: map[string][]byte{"123": []byte("123")}},
							Parents: []CommitDef{
								{
									Message: "12",
									Tree:    TreeDef{Blobs: map[string][]byte{"12": []byte("12")}},
									Parents: []CommitDef{
										{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}},
									},
								},
								{
									Message: "13",
									Tree:    TreeDef{Blobs: map[string][]byte{"13": []byte("13")}},
								},
							},
						}

						d12 = &d123.Parents[0]
						d1  = &d12.Parents[0]
						d13 = &d123.Parents[1]

						m123 = &CommitDef{
							Message: "123",
							Parents: []CommitDef{
								{
									Message: "12",
									Parents: []CommitDef{
										{Message: "1"},
									},
								},
								{Message: "13"},
							},
						}

						m12 = &m123.Parents[0]
						m1  = &m12.Parents[0]
						m13 = &m123.Parents[1]
					)

					d13.Parents, m13.Parents = append(d13.Parents, *d1.DeepCopy()), append(m13.Parents, *m1.DeepCopy())

					return spec{
						spec:      "deep metadata and data",
						metaHead:  metaHeadInheritFrom(m123, d123),
						dmMapping: map[git.ObjectID]git.ObjectID{},
						matchErr:  Succeed(),
						expectDMMappingFn: expectDMMappingFnFor([]dmPair{
							{mcd: m1, dcd: d1},
							{mcd: m12, dcd: d12},
							{mcd: m13, dcd: d13},
							{mcd: m123, dcd: d123},
						}...),
					}
				}(),
				func() spec {
					var (
						d123 = &CommitDef{Message: "123",
							Tree: TreeDef{Blobs: map[string][]byte{"123": []byte("123")}},
							Parents: []CommitDef{
								{
									Message: "12",
									Tree:    TreeDef{Blobs: map[string][]byte{"12": []byte("12")}},
									Parents: []CommitDef{
										{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}},
									},
								},
								{
									Message: "13",
									Tree:    TreeDef{Blobs: map[string][]byte{"13": []byte("13")}},
								},
							},
						}

						d12 = &d123.Parents[0]
						d1  = &d12.Parents[0]
						d13 = &d123.Parents[1]

						m123 = &CommitDef{
							Message: "123",
							Parents: []CommitDef{
								{Message: "12"},
								{Message: "13"},
							},
						}

						m12 = &m123.Parents[0]
						m13 = &m123.Parents[1]
					)

					d13.Parents = append(d13.Parents, *d1.DeepCopy())

					return spec{
						spec:      "partial metadata",
						metaHead:  metaHeadInheritFrom(m123, d123),
						dmMapping: map[git.ObjectID]git.ObjectID{},
						matchErr:  Succeed(),
						expectDMMappingFn: expectDMMappingFnFor([]dmPair{
							{mcd: m12, dcd: d12},
							{mcd: m13, dcd: d13},
							{mcd: m123, dcd: d123},
						}...),
					}
				}(),
				func() spec {
					var (
						d123 = &CommitDef{Message: "123",
							Tree: TreeDef{Blobs: map[string][]byte{"123": []byte("123")}},
							Parents: []CommitDef{
								{
									Message: "12",
									Tree:    TreeDef{Blobs: map[string][]byte{"12": []byte("12")}},
									Parents: []CommitDef{
										{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}},
									},
								},
							},
						}

						d12 = &d123.Parents[0]
						d1  = &d12.Parents[0]

						m123 = &CommitDef{
							Message: "123",
							Parents: []CommitDef{
								{
									Message: "12",
									Parents: []CommitDef{
										{Message: "1"},
									},
								},
								{Message: "13"},
							},
						}

						m12 = &m123.Parents[0]
						m1  = &m12.Parents[0]
						m13 = &m123.Parents[1]
					)

					m13.Parents = append(m13.Parents, *m1.DeepCopy())

					return spec{
						spec:      "corrupted metadata ancestry",
						metaHead:  metaHeadInheritFrom(m123, d123),
						dmMapping: map[git.ObjectID]git.ObjectID{},
						matchErr:  PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)})),
						expectDMMappingFn: expectDMMappingFnFor([]dmPair{
							{mcd: m1, dcd: d1},
							{mcd: m12, dcd: d12},
							{mcd: m123, dcd: d123},
						}...),
					}
				}(),
				func() spec {
					var (
						d123 = &CommitDef{Message: "123",
							Tree: TreeDef{Blobs: map[string][]byte{"123": []byte("123")}},
							Parents: []CommitDef{
								{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}},
								{
									Message: "12",
									Tree:    TreeDef{Blobs: map[string][]byte{"12": []byte("12")}},
								},
							},
						}

						d1  = &d123.Parents[0]
						d12 = &d123.Parents[1]

						m123 = &CommitDef{
							Message: "123",
							Parents: []CommitDef{
								{Message: "1"},
								{Message: "12"},
							},
						}

						m1  = &m123.Parents[0]
						m12 = &m123.Parents[1]
					)

					d12.Parents, m12.Parents = append(d12.Parents, *d1.DeepCopy()), append(m12.Parents, *m1.DeepCopy())

					return spec{
						spec:      "repeating metadata and data",
						metaHead:  metaHeadInheritFrom(m123, d123),
						dmMapping: map[git.ObjectID]git.ObjectID{},
						matchErr:  Succeed(),
						expectDMMappingFn: expectDMMappingFnFor([]dmPair{
							{mcd: m1, dcd: d1},
							{mcd: m12, dcd: d12},
							{mcd: m123, dcd: d123},
						}...),
					}
				}(),
				func() spec {
					var (
						d123 = &CommitDef{Message: "123",
							Tree: TreeDef{Blobs: map[string][]byte{"123": []byte("123")}},
							Parents: []CommitDef{
								{
									Message: "12",
									Tree:    TreeDef{Blobs: map[string][]byte{"12": []byte("12")}},
									Parents: []CommitDef{
										{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}},
									},
								},
								{
									Message: "13",
									Tree:    TreeDef{Blobs: map[string][]byte{"13": []byte("13")}},
								},
							},
						}

						d12 = &d123.Parents[0]
						d1  = &d12.Parents[0]
						d13 = &d123.Parents[1]

						m123 = &CommitDef{
							Message: "123",
							Parents: []CommitDef{
								{
									Message: "12",
									Parents: []CommitDef{
										{Message: "1"},
									},
								},
								{Message: "13"},
							},
						}

						m12 = &m123.Parents[0]
						m1  = &m12.Parents[0]
						m13 = &m123.Parents[1]

						preM12 = &CommitDef{Message: "12", Tree: TreeDef{Blobs: map[string][]byte{"pre": []byte("pre")}}}
					)

					d13.Parents, m13.Parents = append(d13.Parents, *d1.DeepCopy()), append(m13.Parents, *m1.DeepCopy())

					return spec{
						spec:     "pre-exising mapping",
						metaHead: metaHeadInheritFrom(m123, d123),
						dmMappingFn: func(ctx context.Context, repo git.Repository) (dmMapping map[git.ObjectID]git.ObjectID, err error) {
							var (
								mc  git.Commit
								mcd CommitDef
								dID git.ObjectID
							)

							dmMapping = make(map[git.ObjectID]git.ObjectID)

							if mc, err = metaHeadFrom(preM12, d12)(ctx, repo); err != nil {
								return
							}

							defer mc.Close()

							mcd = *GetCommitDefByCommit(ctx, repo, mc)

							if dID, err = git.NewObjectID(string(mcd.Tree.Blobs[metadataPathData])); err != nil {
								return
							}

							dmMapping[dID] = mc.ID()
							return
						},
						matchErr: Succeed(),
						expectDMMappingFn: expectDMMappingFnFor([]dmPair{
							{mcd: m1, dcd: d1},
							{mcd: preM12, dcd: d12},
							{mcd: m13, dcd: d13},
							{mcd: m123, dcd: d123},
						}...),
					}
				}(),
			} {
				func(s spec) {
					Describe(s.spec, func() {
						var (
							metaHead  git.Commit
							parentCtx context.Context
						)

						BeforeEach(func() {
							if s.metaHead != nil {
								Expect(func() (err error) { metaHead, err = s.metaHead(ctx, b.repo); return }()).To(Succeed())
							}

							if s.dmMapping == nil && s.dmMappingFn != nil {
								Expect(func() (err error) { s.dmMapping, err = s.dmMappingFn(ctx, b.repo); return }()).To(Succeed())
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
							Expect(b.loadDataMetadataMapping(ctx, metaHead, s.dmMapping)).To(s.matchErr)

							if s.matchDMMapping != nil {
								Expect(s.dmMapping).To(s.matchDMMapping)
							}

							if s.matchDMMapping != nil {
								Expect(s.dmMapping).To(s.matchDMMapping)
							}

							if s.expectDMMappingFn != nil {
								var (
									expectDMMapping map[git.ObjectID]expectMetaHeadFunc
									defaultID       = git.ObjectID{}
								)

								Expect(func() (err error) {
									expectDMMapping, err = s.expectDMMappingFn(parentCtx, b.repo)
									return
								}()).To(Succeed())

								Expect(s.dmMapping).To(HaveLen(len(expectDMMapping)))

								for i, expectMetaHead := range expectDMMapping {
									var (
										edcd, amcd  *CommitDef
										amID        git.ObjectID
										actualFound bool
										iIsDefault  = reflect.DeepEqual(i, defaultID)
										spec        string
									)

									if !iIsDefault {
										edcd = GetCommitDefByID(parentCtx, b.repo, i)
									}

									// Nested loops used because sometimes the commit IDs for the same CommitDef don't match.
									for j, mID := range s.dmMapping {
										var (
											adcd       *CommitDef
											jIsDefault = reflect.DeepEqual(j, defaultID)
											dcdMatcher types.GomegaMatcher
										)

										if reflect.DeepEqual(j, i) || (jIsDefault && iIsDefault) {
											amID, actualFound = mID, true
											break
										}

										if !jIsDefault {
											adcd = GetCommitDefByID(parentCtx, b.repo, j)
										}

										if edcd == nil {
											dcdMatcher = BeNil()
										} else {
											dcdMatcher = GetCommitDefMatcher(edcd)
										}

										if actualFound, _ = dcdMatcher.Match(adcd); actualFound {
											amID = mID
											break
										}
									}

									if actualFound {
										amcd = GetCommitDefByID(parentCtx, b.repo, amID)
									} else {
										amcd = nil
									}

									spec = fmt.Sprintf("dmMapping for %v[%v]", edcd, i)
									if expectMetaHead == nil {
										Expect(amcd).To(BeNil(), spec)
									} else {
										expectMetaHead(parentCtx, b.repo, amcd, spec)
									}
								}
							}
						})
					})
				}(s)
			}
		})

		Describe("commitConfig", func() {
			BeforeEach(func() {
				b.commitConfig.committerName = "trishanku"
				b.commitConfig.committerEmail = "trishanku@heaven.com"
			})

			Describe("createEmptyCommit", func() {
				for _, s := range []struct {
					ctxFn                                      ContextFunc
					revision                                   int64
					matchErr, matchObjectID, matchCommitDefPtr types.GomegaMatcher
				}{
					{ctxFn: CanceledContext, matchErr: MatchError(context.Canceled), matchObjectID: Equal(git.ObjectID{})},
					{revision: 1234, matchErr: Succeed(), matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{Message: "1234"}))},
				} {
					func(ctxFn ContextFunc, revision int64, matchErr, matchObjectID, matchCommitDefPtr types.GomegaMatcher) {
						Describe(fmt.Sprintf("canceled context=%t", ctxFn != nil), func() {
							var parentCtx context.Context

							BeforeEach(func() {
								parentCtx = ctx
								if ctxFn != nil {
									ctx = ctxFn(ctx)
								}
							})

							It(ItSpecForMatchError(matchErr), func() {
								var id, err = b.createEmptyCommit(ctx, revision)

								Expect(err).To(matchErr)

								if matchObjectID != nil {
									Expect(id).To(matchObjectID)
								}

								if matchCommitDefPtr != nil {
									Expect(GetCommitDefByID(parentCtx, b.repo, id)).To(matchCommitDefPtr)
								}
							})
						})
					}(s.ctxFn, s.revision, s.matchErr, s.matchObjectID, s.matchCommitDefPtr)
				}
			})

			Describe("createStartingMetadata", func() {
				type spec struct {
					ctxFn                   ContextFunc
					revision                int64
					version                 string
					matchErr, matchObjectID types.GomegaMatcher
					expectMetaHead          expectMetaHeadFunc
				}

				for _, s := range []spec{
					{ctxFn: CanceledContext, matchErr: MatchError(context.Canceled), matchObjectID: Equal(git.ObjectID{})},
					func() spec {
						var (
							revision    = int64(1234)
							version     = "9.8.7"
							strRevision = revisionToString(revision)
						)

						return spec{
							revision: revision,
							version:  version,
							matchErr: Succeed(),
							expectMetaHead: expectMetaHead(
								&CommitDef{Message: strRevision, Tree: TreeDef{Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision),
									metadataPathVersion:  []byte(version),
								}}},
								&CommitDef{Message: strRevision},
								true,
							),
						}
					}(),
				} {
					func(s spec) {
						Describe(fmt.Sprintf("canceled context=%t", s.ctxFn != nil), func() {
							var parentCtx context.Context

							BeforeEach(func() {
								parentCtx = ctx
								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var id, err = b.createStartingMetadata(ctx, s.revision, s.version)

								Expect(err).To(s.matchErr)

								if s.matchObjectID != nil {
									Expect(id).To(s.matchObjectID)
								}

								if s.expectMetaHead != nil {
									s.expectMetaHead(parentCtx, b.repo, GetCommitDefByID(parentCtx, b.repo, id), "metadata")
								}
							})
						})
					}(s)
				}
			})

			// initMetadataForDataParents is tested along with initMetadataForData because recursive dependency.

			Describe("mutateMetadataForDataChange", func() {
				type spec struct {
					spec                                  string
					ctxFn                                 ContextFunc
					mtd, odtd                             *TreeDef
					ndcd                                  *CommitDef
					newRevision                           int64
					matchErr, matchMutated, matchObjectID types.GomegaMatcher
					expectMetaHeadTree                    expectMetaHeadTreeFunc
				}

				for _, s := range []spec{
					{
						spec:          "invalid context",
						ctxFn:         CanceledContext,
						matchErr:      MatchError(context.Canceled),
						matchMutated:  BeFalse(),
						matchObjectID: Equal(git.ObjectID{}),
					},
					func() spec {
						var (
							newRevision = int64(0)
							ndcd        = &CommitDef{Message: revisionToString(newRevision)}
						)
						return spec{
							spec:         "start with empty data",
							ndcd:         ndcd,
							newRevision:  newRevision,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							expectMetaHeadTree: expectMetaHeadTree(
								&TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte(revisionToString(newRevision))}},
								ndcd,
							),
						}
					}(),
					func() spec {
						var (
							newRevision    = int64(2)
							strNewRevision = revisionToString(newRevision)

							ndcd = &CommitDef{Message: strNewRevision, Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("1")},
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Blobs: map[string][]byte{"2": []byte("2")}},
									}},
								},
							}}

							keyMeta = &TreeDef{Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  []byte(strNewRevision),
								etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
								etcdserverpb.Compare_VERSION.String(): []byte("1"),
							}}
						)
						return spec{
							spec:         "start with deep data",
							ndcd:         ndcd,
							newRevision:  newRevision,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							expectMetaHeadTree: expectMetaHeadTree(
								&TreeDef{
									Blobs: map[string][]byte{metadataPathRevision: []byte(strNewRevision)},
									Subtrees: map[string]TreeDef{
										"1": *keyMeta.DeepCopy(),
										"a": {Subtrees: map[string]TreeDef{
											"b": {Subtrees: map[string]TreeDef{"2": *keyMeta.DeepCopy()}},
										}},
									},
								},
								ndcd,
							),
						}
					}(),
					func() spec {
						var (
							newRevision    = int64(3)
							strNewRevision = revisionToString(newRevision)

							odtd = &TreeDef{
								Blobs: map[string][]byte{"1": []byte("1")},
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
									}},
								},
							}

							ndcd = &CommitDef{Message: strNewRevision, Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("11"), "2": []byte("2")},
								Subtrees: map[string]TreeDef{
									"a": {
										Blobs: map[string][]byte{"4": []byte("4")},
										Subtrees: map[string]TreeDef{
											"b": {Blobs: map[string][]byte{"3": []byte("33")}},
										},
									},
								},
							}}

							keyMeta = &TreeDef{Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  []byte(strNewRevision),
								etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
								etcdserverpb.Compare_VERSION.String(): []byte("1"),
							}}
						)
						return spec{
							spec:         "diff with deep data on empty metadata",
							odtd:         odtd,
							ndcd:         ndcd,
							newRevision:  newRevision,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							expectMetaHeadTree: expectMetaHeadTree(
								&TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strNewRevision),
									},
									Subtrees: map[string]TreeDef{
										"1": *keyMeta.DeepCopy(),
										"2": *keyMeta.DeepCopy(),
										"a": {Subtrees: map[string]TreeDef{
											"4": *keyMeta.DeepCopy(),
											"b": {Subtrees: map[string]TreeDef{"3": *keyMeta.DeepCopy()}},
										}},
									},
								},
								ndcd,
							),
						}
					}(),
					func() spec {
						var (
							oldOldRevision    = int64(3)
							oldRevision       = int64(4)
							newRevision       = int64(6)
							strOldOldRevision = revisionToString(oldOldRevision)
							strOldRevision    = revisionToString(oldRevision)
							strNewRevision    = revisionToString(newRevision)

							odtd = &TreeDef{
								Blobs: map[string][]byte{"1": []byte("1")},
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
										"c": {Blobs: map[string][]byte{"4": []byte("4")}},
									}},
								},
							}

							mtd = &TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strOldRevision),
									metadataPathData:     []byte("invalid"),
								},
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(strOldOldRevision),
										etcdserverpb.Compare_MOD.String():     []byte(strOldOldRevision),
										etcdserverpb.Compare_VERSION.String(): []byte("2"),
									}},
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"2": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strOldRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strOldRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
											"3": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String(): []byte(strOldRevision),
												etcdserverpb.Compare_MOD.String():    []byte(strOldRevision),
											}},
										}},
										"c": {Subtrees: map[string]TreeDef{
											"4": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strOldOldRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strOldRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
										}},
									}},
								},
							}

							ndcd = &CommitDef{Message: strNewRevision, Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("11"), "2": []byte("2")},
								Subtrees: map[string]TreeDef{
									"a": {
										Blobs: map[string][]byte{"5": []byte("5")},
										Subtrees: map[string]TreeDef{
											"b": {Blobs: map[string][]byte{"3": []byte("33")}},
										},
									},
								},
							}}
						)
						return spec{
							spec:         "diff with deep data on existing metadata",
							mtd:          mtd,
							odtd:         odtd,
							ndcd:         ndcd,
							newRevision:  newRevision,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							expectMetaHeadTree: expectMetaHeadTree(
								&TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strNewRevision),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strOldOldRevision),
											etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
											etcdserverpb.Compare_VERSION.String(): []byte("3"),
										}},
										"2": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strNewRevision),
											etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
										"a": {Subtrees: map[string]TreeDef{
											"5": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strNewRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
											"b": {Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strOldRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}}}},
										}},
									},
								},
								ndcd,
							),
						}
					}(),
					func() spec {
						var (
							oldOldRevision    = int64(3)
							oldRevision       = int64(4)
							newRevision       = int64(6)
							strOldOldRevision = revisionToString(oldOldRevision)
							strOldRevision    = revisionToString(oldRevision)
							strNewRevision    = revisionToString(newRevision)

							odtd = &TreeDef{
								Blobs: map[string][]byte{"1": []byte("1"), "a": []byte("a")},
								Subtrees: map[string]TreeDef{
									"b": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
									"c": {Blobs: map[string][]byte{"4": []byte("4")}},
								},
							}

							mtd = &TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strOldRevision),
									metadataPathData:     []byte("invalid"),
								},
								Subtrees: map[string]TreeDef{
									"1": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(strOldOldRevision),
										etcdserverpb.Compare_MOD.String():     []byte(strOldOldRevision),
										etcdserverpb.Compare_VERSION.String(): []byte("2"),
									}},
									"a": {Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(strOldOldRevision),
										etcdserverpb.Compare_MOD.String():     []byte(strOldOldRevision),
										etcdserverpb.Compare_VERSION.String(): []byte("2"),
									}},
									"b": {Subtrees: map[string]TreeDef{
										"2": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strOldRevision),
											etcdserverpb.Compare_MOD.String():     []byte(strOldRevision),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
										"3": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String(): []byte(strOldRevision),
											etcdserverpb.Compare_MOD.String():    []byte(strOldRevision),
										}},
									}},
									"c": {Subtrees: map[string]TreeDef{
										"4": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strOldOldRevision),
											etcdserverpb.Compare_MOD.String():     []byte(strOldRevision),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
									}},
								},
							}

							ndcd = &CommitDef{Message: strNewRevision, Tree: TreeDef{
								Blobs: map[string][]byte{"1": []byte("11"), "2": []byte("2")},
								Subtrees: map[string]TreeDef{
									"a": {Blobs: map[string][]byte{"5": []byte("5")}},
									"b": {Blobs: map[string][]byte{"3": []byte("33")}},
								},
							}}
						)
						return spec{
							spec:         "diff with changes for conflicting keys on existing metadata",
							mtd:          mtd,
							odtd:         odtd,
							ndcd:         ndcd,
							newRevision:  newRevision,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							expectMetaHeadTree: expectMetaHeadTree(
								&TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strNewRevision),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strOldOldRevision),
											etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
											etcdserverpb.Compare_VERSION.String(): []byte("3"),
										}},
										"2": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strNewRevision),
											etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
										// Replacement of entry here with a deep tree should conflict during mutation.
										"a": {Subtrees: map[string]TreeDef{
											"5": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strNewRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
										}},
										"b": {Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strOldRevision),
											etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}}}},
									},
								},
								ndcd,
							),
						}
					}(),
					func() spec {
						var (
							oldRevision    = int64(4)
							newRevision    = int64(6)
							strOldRevision = revisionToString(oldRevision)
							strNewRevision = revisionToString(newRevision)

							odtd = &TreeDef{
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Blobs: map[string][]byte{"1": []byte("1")}},
									}},
								},
							}

							mtd = &TreeDef{
								Blobs: map[string][]byte{
									metadataPathRevision: []byte(strOldRevision),
									metadataPathData:     []byte("invalid"),
								},
								Subtrees: map[string]TreeDef{
									"a": {Subtrees: map[string]TreeDef{
										"b": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strOldRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strOldRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
											"orphaned": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String(): []byte(strOldRevision),
												etcdserverpb.Compare_MOD.String():    []byte(strOldRevision),
											}},
										}},
									}},
								},
							}

							ndcd = &CommitDef{Message: strNewRevision, Tree: TreeDef{Subtrees: map[string]TreeDef{
								"a": {Blobs: map[string][]byte{"2": []byte("2")}},
							}}}
						)
						return spec{
							spec:         "diff with deep data on existing orphaned metadata",
							mtd:          mtd,
							odtd:         odtd,
							ndcd:         ndcd,
							newRevision:  newRevision,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							expectMetaHeadTree: expectMetaHeadTree(
								&TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strNewRevision),
									},
									Subtrees: map[string]TreeDef{
										"a": {Subtrees: map[string]TreeDef{
											"2": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strNewRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strNewRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
											"b": {Subtrees: map[string]TreeDef{"orphaned": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String(): []byte(strOldRevision),
												etcdserverpb.Compare_MOD.String():    []byte(strOldRevision),
											}}}},
										}},
									},
								},
								ndcd,
							),
						}
					}(),
				} {
					func(s spec) {
						Describe(
							fmt.Sprintf(
								"%s: canceled context=%t, metaT=%v, oldDataT=%v, newDataC=%v, newRevision=%d",
								s.spec,
								s.ctxFn != nil,
								s.mtd,
								s.odtd,
								s.ndcd,
								s.newRevision,
							),
							func() {
								var (
									metaT, oldDataT, newDataT git.Tree
									newDataHeadID             git.ObjectID

									parentCtx context.Context
								)

								BeforeEach(func() {
									type treeLoader struct {
										td   *TreeDef
										tPtr *git.Tree
									}

									var treeLoaders = []treeLoader{
										{td: s.mtd, tPtr: &metaT},
										{td: s.odtd, tPtr: &oldDataT},
									}

									if s.ndcd != nil {
										treeLoaders = append(treeLoaders, treeLoader{td: &s.ndcd.Tree, tPtr: &newDataT})

										Expect(func() (err error) {
											newDataHeadID, err = CreateCommitFromDef(ctx, b.repo, s.ndcd)
											return
										}()).To(Succeed())
									}

									for _, l := range treeLoaders {
										if l.td != nil {
											Expect(func() (err error) {
												*l.tPtr, err = CreateAndLoadTreeFromDef(ctx, b.repo, l.td)
												return
											}()).To(Succeed())
										}
									}

									parentCtx = ctx
									if s.ctxFn != nil {
										ctx = s.ctxFn(ctx)
									}
								})

								AfterEach(func() {
									for _, t := range []git.Tree{metaT, oldDataT, newDataT} {
										if t != nil {
											Expect(t.Close()).To(Succeed())
										}
									}
								})

								It(ItSpecForMatchError(s.matchErr), func() {
									var mutated, newMetaTID, err = b.mutateMetadataForDataChange(ctx, metaT, oldDataT, newDataT, newDataHeadID, s.newRevision)

									Expect(err).To(s.matchErr)
									Expect(mutated).To(s.matchMutated)

									if s.matchObjectID != nil {
										Expect(newMetaTID).To(s.matchObjectID)
									}

									if s.expectMetaHeadTree != nil {
										s.expectMetaHeadTree(parentCtx, b.repo, GetTreeDef(parentCtx, b.repo, newMetaTID), "metadata")
									}
								})
							},
						)
					}(s)
				}
			})

			Describe("initMetadataForData", func() {
				type spec struct {
					spec                                    string
					ctxFn                                   ContextFunc
					cd                                      *CommitDef
					startRevision                           int64
					version                                 string
					dmMapping                               map[git.ObjectID]git.ObjectID
					dmMappingFn                             func(context.Context, git.Repository) (map[git.ObjectID]git.ObjectID, error)
					matchErr, matchObjectID, matchDMMapping types.GomegaMatcher
					expectMetaHead                          expectMetaHeadFunc
					expectDMMappingFn                       func(context.Context, git.Repository) (map[git.ObjectID]expectMetaHeadFunc, error)
				}

				type demPair struct {
					dcd *CommitDef
					em  expectMetaHeadFunc
				}

				var (
					expectDEMMappingFnFor = func(demPairs ...demPair) func(ctx context.Context, repo git.Repository) (dmMapping map[git.ObjectID]expectMetaHeadFunc, err error) {
						return func(ctx context.Context, repo git.Repository) (dmMapping map[git.ObjectID]expectMetaHeadFunc, err error) {
							dmMapping = make(map[git.ObjectID]expectMetaHeadFunc, len(demPairs)+1)

							for _, s := range demPairs {
								var dID git.ObjectID

								if dID, err = CreateCommitFromDef(ctx, repo, s.dcd); err != nil {
									return
								}

								dmMapping[dID] = s.em
							}

							return
						}
					}
				)

				for _, s := range []spec{
					{
						spec:           "invalid context",
						ctxFn:          CanceledContext,
						matchErr:       MatchError(context.Canceled),
						matchObjectID:  Equal(git.ObjectID{}),
						matchDMMapping: BeNil(),
					},
					func() spec {
						var (
							defaultDID  = git.ObjectID{}
							existingMID = git.ObjectID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
							dmMapping   = map[git.ObjectID]git.ObjectID{defaultDID: existingMID}
						)

						return spec{
							spec:           "invalid context, pre-existing mapping",
							ctxFn:          CanceledContext,
							dmMapping:      dmMapping,
							matchErr:       Succeed(),
							matchObjectID:  Equal(existingMID),
							matchDMMapping: Equal(dmMapping),
						}
					}(),
					{
						spec:           "invalid context",
						ctxFn:          CanceledContext,
						cd:             &CommitDef{Message: "1"},
						matchErr:       MatchError(context.Canceled),
						matchObjectID:  Equal(git.ObjectID{}),
						matchDMMapping: BeNil(),
					},
					func() spec {
						var (
							startRevision        = int64(3)
							version              = "7.3.5"
							strPrevStartRevision = revisionToString(startRevision - 1)
							defaultDID           = git.ObjectID{}
							emh                  = expectMetaHead(
								&CommitDef{
									Message: strPrevStartRevision,
									Tree: TreeDef{Blobs: map[string][]byte{
										metadataPathRevision: []byte(strPrevStartRevision),
										metadataPathVersion:  []byte(version),
									}},
								},
								&CommitDef{Message: strPrevStartRevision},
								true,
							)
						)

						return spec{
							spec:           "starting metadata",
							startRevision:  int64(startRevision),
							version:        version,
							dmMapping:      make(map[git.ObjectID]git.ObjectID),
							matchErr:       Succeed(),
							expectMetaHead: emh,
							expectDMMappingFn: func(
								ctx context.Context,
								repo git.Repository,
							) (
								dmMapping map[git.ObjectID]expectMetaHeadFunc,
								err error,
							) {
								dmMapping = map[git.ObjectID]expectMetaHeadFunc{defaultDID: emh}
								return
							},
						}
					}(),
					func() spec {
						var (
							startRevision        = int64(4)
							version              = "8.2.9"
							strStartRevision     = revisionToString(startRevision)
							strPrevStartRevision = revisionToString(startRevision - 1)
							dcp                  = &CommitDef{Message: strPrevStartRevision}
							dcd                  = &CommitDef{Message: strStartRevision}
							defaultDID           = git.ObjectID{}

							mp = &CommitDef{
								Message: strPrevStartRevision,
								Tree: TreeDef{Blobs: map[string][]byte{
									metadataPathRevision: []byte(strPrevStartRevision),
									metadataPathVersion:  []byte(version),
								}},
							}

							mh = &CommitDef{
								Message: strStartRevision,
								Tree: TreeDef{Blobs: map[string][]byte{
									metadataPathRevision: []byte(strStartRevision),
									metadataPathVersion:  []byte(version),
								}},
								Parents: []CommitDef{*mp.DeepCopy()},
							}

							emp = expectMetaHead(mp.DeepCopy(), dcp.DeepCopy(), true)
							emh = expectMetaHead(mh.DeepCopy(), dcd.DeepCopy(), true)
						)

						return spec{
							spec:           "shallow ancestry, empty data",
							cd:             dcd,
							startRevision:  int64(startRevision),
							version:        version,
							dmMapping:      make(map[git.ObjectID]git.ObjectID),
							matchErr:       Succeed(),
							expectMetaHead: emh,
							expectDMMappingFn: func(
								ctx context.Context,
								repo git.Repository,
							) (
								dmMapping map[git.ObjectID]expectMetaHeadFunc,
								err error,
							) {
								if dmMapping, err = expectDEMMappingFnFor(demPair{dcd: dcd, em: emh})(ctx, repo); err != nil {
									return
								}

								dmMapping[defaultDID] = emp
								return
							},
						}
					}(),
					func() spec {
						var (
							startRevision        = int64(1)
							version              = "0.0.1"
							strStartRevision     = revisionToString(startRevision)
							strPrevStartRevision = revisionToString(startRevision - 1)
							defaultDID           = git.ObjectID{}

							keyMeta = &TreeDef{Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
								etcdserverpb.Compare_MOD.String():     []byte(strStartRevision),
								etcdserverpb.Compare_VERSION.String(): []byte("1"),
							}}

							dp = &CommitDef{Message: strPrevStartRevision}

							mp = &CommitDef{
								Message: strPrevStartRevision,
								Tree: TreeDef{Blobs: map[string][]byte{
									metadataPathRevision: []byte(strPrevStartRevision),
									metadataPathVersion:  []byte(version),
								}},
							}

							emp = expectMetaHead(mp.DeepCopy(), dp.DeepCopy(), true)

							dh = &CommitDef{
								Message: strStartRevision,
								Tree: TreeDef{
									Blobs: map[string][]byte{"1a": []byte("1aa"), "5b": []byte("5bb")},
									Subtrees: map[string]TreeDef{
										"a1": {
											Blobs: map[string][]byte{"11": []byte("111"), "22": []byte("222")},
											Subtrees: map[string]TreeDef{
												"b2": {Blobs: map[string][]byte{"33": []byte("333"), "44": []byte("444")}},
											},
										},
										"c3": {
											Blobs: map[string][]byte{"55": []byte("555"), "66": []byte("666")},
											Subtrees: map[string]TreeDef{
												"z5": {Blobs: map[string][]byte{"aa": []byte("aaa"), "bb": []byte("bbb")}},
											},
										},
									},
								},
							}

							mh = &CommitDef{
								Message: strStartRevision,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strStartRevision),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1a": *keyMeta.DeepCopy(),
										"5b": *keyMeta.DeepCopy(),
										"a1": {
											Subtrees: map[string]TreeDef{
												"11": *keyMeta.DeepCopy(),
												"22": *keyMeta.DeepCopy(),
												"b2": {
													Subtrees: map[string]TreeDef{
														"33": *keyMeta.DeepCopy(),
														"44": *keyMeta.DeepCopy(),
													},
												},
											},
										},
										"c3": {
											Subtrees: map[string]TreeDef{
												"55": *keyMeta.DeepCopy(),
												"66": *keyMeta.DeepCopy(),
												"z5": {
													Subtrees: map[string]TreeDef{
														"aa": *keyMeta.DeepCopy(),
														"bb": *keyMeta.DeepCopy(),
													},
												},
											},
										},
									},
								},
								Parents: []CommitDef{*mp.DeepCopy()},
							}

							emh = expectMetaHead(mh.DeepCopy(), dh.DeepCopy(), true)
						)

						return spec{
							spec:           "shallow ancestry, deep data",
							cd:             dh,
							startRevision:  int64(startRevision),
							version:        version,
							dmMapping:      make(map[git.ObjectID]git.ObjectID),
							matchErr:       Succeed(),
							expectMetaHead: emh,
							expectDMMappingFn: func(
								ctx context.Context,
								repo git.Repository,
							) (
								dmMapping map[git.ObjectID]expectMetaHeadFunc,
								err error,
							) {
								if dmMapping, err = expectDEMMappingFnFor(demPair{dcd: dh, em: emh})(ctx, repo); err != nil {
									return
								}

								dmMapping[defaultDID] = emp
								return
							},
						}
					}(),
					func() spec {
						var (
							version       = "0.1.12"
							revision0     = int64(4)
							revision1     = revision0 + 1
							revision2     = revision1 + 1
							revision3     = revision2 + 1
							revision4     = revision3 + 1
							startRevision = revision1
							strRevision0  = revisionToString(revision0)
							strRevision1  = revisionToString(revision1)
							strRevision2  = revisionToString(revision2)
							strRevision3  = revisionToString(revision3)
							strRevision4  = revisionToString(revision4)
							defaultDID    = git.ObjectID{}

							d0 = &CommitDef{Message: strRevision0}

							m0 = &CommitDef{
								Message: strRevision0,
								Tree: TreeDef{Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision0),
									metadataPathVersion:  []byte(version),
								}},
							}

							em0 = expectMetaHead(m0.DeepCopy(), d0.DeepCopy(), true)

							d1 = &CommitDef{
								Message: strRevision1,
								Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "11": []byte("11")}},
							}

							m1 = &CommitDef{
								Message: strRevision1,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision1),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision1),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
										"11": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision1),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
									},
								},
								Parents: []CommitDef{*m0.DeepCopy()},
							}

							em1 = expectMetaHead(m1.DeepCopy(), d1.DeepCopy(), true)

							d2 = &CommitDef{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{"1": []byte("111")},
									Subtrees: map[string]TreeDef{
										"a": {
											Blobs: map[string][]byte{"1": []byte("1")},
											Subtrees: map[string]TreeDef{
												"b": {Blobs: map[string][]byte{"1": []byte("1")}},
											},
										},
									},
								},
								Parents: []CommitDef{*d1.DeepCopy()},
							}

							m2 = &CommitDef{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
											etcdserverpb.Compare_VERSION.String(): []byte("2"),
										}},
										"a": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision2),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
											"b": {Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision2),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
										}},
									},
								},
								Parents: []CommitDef{*m1.DeepCopy()},
							}

							em2 = expectMetaHead(m2.DeepCopy(), d2.DeepCopy(), true)

							d3 = &CommitDef{
								Message: strRevision3,
								Tree: TreeDef{
									Blobs: map[string][]byte{"1": []byte("11"), "11": []byte("11")},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{"1": []byte("11"), "b": []byte("bb")}},
										"c": {
											Blobs: map[string][]byte{"3": []byte("33")},
											Subtrees: map[string]TreeDef{
												"d": {Blobs: map[string][]byte{"4": []byte("44")}},
											},
										},
									},
								},
								Parents: []CommitDef{*d2.DeepCopy()},
							}

							m3 = &CommitDef{
								Message: strRevision3,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision3),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
											etcdserverpb.Compare_VERSION.String(): []byte("3"),
										}},
										"11": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
										"a": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision2),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
												etcdserverpb.Compare_VERSION.String(): []byte("2"),
											}},
											"b": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
										}},
										"c": {Subtrees: map[string]TreeDef{
											"3": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
												etcdserverpb.Compare_VERSION.String(): []byte("1"),
											}},
											"d": {Subtrees: map[string]TreeDef{
												"4": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
										}},
									},
								},
								Parents: []CommitDef{*m2.DeepCopy()},
							}

							em3 = expectMetaHead(m3.DeepCopy(), d3.DeepCopy(), true)

							d4 = &CommitDef{
								Message: strRevision4,
								Tree: TreeDef{
									Blobs: map[string][]byte{"11": []byte("11")},
									Subtrees: map[string]TreeDef{
										"c": {
											Subtrees: map[string]TreeDef{
												"3": {Blobs: map[string][]byte{"33": []byte("33")}},
												"d": {Blobs: map[string][]byte{"4": []byte("444")}},
											},
										},
									},
								},
								Parents: []CommitDef{*d3.DeepCopy()},
							}

							m4 = &CommitDef{
								Message: strRevision4,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision4),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"11": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
										"c": {Subtrees: map[string]TreeDef{
											"3": {Subtrees: map[string]TreeDef{
												"33": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision4),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision4),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
											"d": {Subtrees: map[string]TreeDef{
												"4": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision4),
													etcdserverpb.Compare_VERSION.String(): []byte("2"),
												}},
											}},
										}},
									},
								},
								Parents: []CommitDef{*m3.DeepCopy()},
							}

							em4 = expectMetaHead(m4.DeepCopy(), d4.DeepCopy(), true)

							dh  = d4
							emh = em4
						)

						return spec{
							spec:           "deep linear ancestry, deep data",
							cd:             dh,
							startRevision:  int64(startRevision),
							version:        version,
							dmMapping:      make(map[git.ObjectID]git.ObjectID),
							matchErr:       Succeed(),
							expectMetaHead: emh,
							expectDMMappingFn: func(
								ctx context.Context,
								repo git.Repository,
							) (
								dmMapping map[git.ObjectID]expectMetaHeadFunc,
								err error,
							) {
								if dmMapping, err = expectDEMMappingFnFor([]demPair{
									{dcd: d1, em: em1},
									{dcd: d2, em: em2},
									{dcd: d3, em: em3},
									{dcd: d4, em: em4},
								}...)(ctx, repo); err != nil {
									return
								}

								dmMapping[defaultDID] = em0
								return
							},
						}
					}(),
					func() spec {
						var (
							version       = "0.1.12"
							revision0     = int64(7)
							revision1     = revision0 + 1
							revision2     = revision1 + 1
							revision3     = revision2 + 1
							startRevision = revision1
							strRevision0  = revisionToString(revision0)
							strRevision1  = revisionToString(revision1)
							strRevision2  = revisionToString(revision2)
							strRevision3  = revisionToString(revision3)
							defaultDID    = git.ObjectID{}

							d0 = &CommitDef{Message: strRevision0}

							m0 = &CommitDef{
								Message: strRevision0,
								Tree: TreeDef{Blobs: map[string][]byte{
									metadataPathRevision: []byte(strRevision0),
									metadataPathVersion:  []byte(version),
								}},
							}

							em0 = expectMetaHead(m0.DeepCopy(), d0.DeepCopy(), true)

							d1 = &CommitDef{
								Message: strRevision1,
								Tree: TreeDef{
									Blobs: map[string][]byte{"1": []byte("1")},
									Subtrees: map[string]TreeDef{
										"a": {Blobs: map[string][]byte{"1": []byte("1")}},
									},
								},
							}

							m1 = &CommitDef{
								Message: strRevision1,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision1),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision1),
											etcdserverpb.Compare_VERSION.String(): []byte("1"),
										}},
										"a": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision1),
												etcdserverpb.Compare_VERSION.String(): []byte("1")},
											},
										}},
									},
								},
								Parents: []CommitDef{*m0.DeepCopy()},
							}

							em1 = expectMetaHead(m1.DeepCopy(), d1.DeepCopy(), true)

							d21 = &CommitDef{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{"1": []byte("11")},
									Subtrees: map[string]TreeDef{
										"a": {
											Blobs: map[string][]byte{"1": []byte("11")},
											Subtrees: map[string]TreeDef{
												"b": {Blobs: map[string][]byte{"1": []byte("11")}},
											},
										},
									},
								},
								Parents: []CommitDef{*d1.DeepCopy()},
							}

							m21 = &CommitDef{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
											etcdserverpb.Compare_VERSION.String(): []byte("2"),
										}},
										"a": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
												etcdserverpb.Compare_VERSION.String(): []byte("2"),
											}},
											"b": {Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision2),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
										}},
									},
								},
								Parents: []CommitDef{*m1.DeepCopy()},
							}

							em21 = expectMetaHead(m21.DeepCopy(), d21.DeepCopy(), true)

							d22 = &CommitDef{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{"1": []byte("111")},
									Subtrees: map[string]TreeDef{
										"a": {
											Blobs: map[string][]byte{"1": []byte("111")},
											Subtrees: map[string]TreeDef{
												"c": {Blobs: map[string][]byte{"1": []byte("111")}},
											},
										},
									},
								},
								Parents: []CommitDef{*d1.DeepCopy()},
							}

							m22 = &CommitDef{
								Message: strRevision2,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision2),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
											etcdserverpb.Compare_VERSION.String(): []byte("2"),
										}},
										"a": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
												etcdserverpb.Compare_VERSION.String(): []byte("2"),
											}},
											"c": {Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision2),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
										}},
									},
								},
								Parents: []CommitDef{*m1.DeepCopy()},
							}

							em22 = expectMetaHead(m22.DeepCopy(), d22.DeepCopy(), true)

							d3 = &CommitDef{
								Message: strRevision3,
								Tree: TreeDef{
									Blobs: map[string][]byte{"1": []byte("1111")},
									Subtrees: map[string]TreeDef{
										"a": {
											Blobs: map[string][]byte{"1": []byte("1111")},
											Subtrees: map[string]TreeDef{
												"b": {Blobs: map[string][]byte{"1": []byte("11")}},
												"c": {Blobs: map[string][]byte{"1": []byte("111")}},
												"d": {Blobs: map[string][]byte{"1": []byte("1111")}},
											},
										},
									},
								},
								Parents: []CommitDef{*d21.DeepCopy(), *d22.DeepCopy()},
							}

							m3 = &CommitDef{
								Message: strRevision3,
								Tree: TreeDef{
									Blobs: map[string][]byte{
										metadataPathRevision: []byte(strRevision3),
										metadataPathVersion:  []byte(version),
									},
									Subtrees: map[string]TreeDef{
										"1": {Blobs: map[string][]byte{
											etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
											etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
											etcdserverpb.Compare_VERSION.String(): []byte("3"),
										}},
										"a": {Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strRevision1),
												etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
												etcdserverpb.Compare_VERSION.String(): []byte("3"),
											}},
											"b": {Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision2),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision2),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
											"c": {Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
											"d": {Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strRevision3),
													etcdserverpb.Compare_MOD.String():     []byte(strRevision3),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
											}},
										}},
									},
								},
								Parents: []CommitDef{*m21.DeepCopy(), *m22.DeepCopy()},
							}

							em3 = expectMetaHead(m3.DeepCopy(), d3.DeepCopy(), true)

							dh  = d3
							emh = em3
						)

						return spec{
							spec:           "deep tree ancestry, deep data",
							cd:             dh,
							startRevision:  int64(startRevision),
							version:        version,
							dmMapping:      make(map[git.ObjectID]git.ObjectID),
							matchErr:       Succeed(),
							expectMetaHead: emh,
							expectDMMappingFn: func(
								ctx context.Context,
								repo git.Repository,
							) (
								dmMapping map[git.ObjectID]expectMetaHeadFunc,
								err error,
							) {
								if dmMapping, err = expectDEMMappingFnFor([]demPair{
									{dcd: d1, em: em1},
									{dcd: d21, em: em21},
									{dcd: d22, em: em22},
									{dcd: d3, em: em3},
								}...)(ctx, repo); err != nil {
									return
								}

								dmMapping[defaultDID] = em0
								return
							},
						}
					}(),
				} {
					func(s spec) {
						Describe(
							fmt.Sprintf(
								"%s: canceled context=%t, dataC=%v, startRevision=%d, version=%s",
								s.spec,
								s.ctxFn != nil,
								s.cd,
								s.startRevision,
								s.version,
							),
							func() {
								var (
									data      git.Commit
									parentCtx context.Context
								)

								BeforeEach(func() {
									if s.cd != nil {
										Expect(func() (err error) {
											data, err = CreateAndLoadCommitFromDef(ctx, b.repo, s.cd)
											return
										}()).To(Succeed())
									}

									if s.dmMapping == nil && s.dmMappingFn != nil {
										Expect(func() (err error) { s.dmMapping, err = s.dmMappingFn(ctx, b.repo); return }()).To(Succeed())
									}

									parentCtx = ctx
									if s.ctxFn != nil {
										ctx = s.ctxFn(ctx)
									}
								})

								AfterEach(func() {
									if data != nil {
										Expect(data.Close()).To(Succeed())
									}
								})

								It(ItSpecForMatchError(s.matchErr), func() {
									var mID, err = b.initMetadataForData(ctx, data, s.startRevision, s.version, s.dmMapping)

									Expect(err).To(s.matchErr)

									if s.matchObjectID != nil {
										Expect(mID).To(s.matchObjectID)
									}

									if s.expectMetaHead != nil {
										s.expectMetaHead(parentCtx, b.repo, GetCommitDefByID(parentCtx, b.repo, mID), "metadata")
									}

									if s.matchDMMapping != nil {
										Expect(s.dmMapping).To(s.matchDMMapping)
									}

									if s.expectDMMappingFn != nil {
										var (
											expectDMMapping map[git.ObjectID]expectMetaHeadFunc
											defaultID       = git.ObjectID{}
										)

										Expect(func() (err error) {
											expectDMMapping, err = s.expectDMMappingFn(parentCtx, b.repo)
											return
										}()).To(Succeed())

										Expect(s.dmMapping).To(HaveLen(len(expectDMMapping)))

										for i, expectMetaHead := range expectDMMapping {
											var (
												edcd, amcd  *CommitDef
												amID        git.ObjectID
												actualFound bool
												iIsDefault  = reflect.DeepEqual(i, defaultID)
												spec        string
											)

											if !iIsDefault {
												edcd = GetCommitDefByID(parentCtx, b.repo, i)
											}

											// Nested loops used because sometimes the commit IDs for the same CommitDef don't match.
											for j, mID := range s.dmMapping {
												var (
													adcd       *CommitDef
													jIsDefault = reflect.DeepEqual(j, defaultID)
													dcdMatcher types.GomegaMatcher
												)

												if reflect.DeepEqual(j, i) || (jIsDefault && iIsDefault) {
													amID, actualFound = mID, true
													break
												}

												if !jIsDefault {
													adcd = GetCommitDefByID(parentCtx, b.repo, j)
												}

												if edcd == nil {
													dcdMatcher = BeNil()
												} else {
													dcdMatcher = GetCommitDefMatcher(edcd)
												}

												if actualFound, _ = dcdMatcher.Match(adcd); actualFound {
													amID = mID
													break
												}
											}

											if actualFound {
												amcd = GetCommitDefByID(parentCtx, b.repo, amID)
											}

											spec = fmt.Sprintf("dmMapping for %v", edcd)
											if expectMetaHead == nil {
												Expect(amcd).To(BeNil(), spec)
											} else {
												expectMetaHead(parentCtx, b.repo, amcd, spec)
											}
										}
									}
								})
							},
						)
					}(s)
				}
			})

			Describe("initMetadata", func() {
				type spec struct {
					spec                            string
					ctxFn                           ContextFunc
					refName, metaRefName            git.ReferenceName
					dataHead                        *CommitDef
					metaHead                        metaHeadFunc
					startRevision                   int64
					version                         string
					force                           bool
					matchErr, matchDataCommitDefPtr types.GomegaMatcher
					expectMetaHead                  expectMetaHeadFunc
				}

				for refName, metaRefName := range map[git.ReferenceName]git.ReferenceName{
					DefaultDataReferenceName: DefaultMetadataReferenceName,
					"refs/heads/custom":      "refs/meta/custom",
				} {
					for _, s := range []spec{
						{
							spec:                  "expired context",
							ctxFn:                 CanceledContext,
							refName:               refName,
							metaRefName:           metaRefName,
							matchErr:              MatchError(context.Canceled),
							matchDataCommitDefPtr: BeNil(),
							expectMetaHead:        delegateToMatcher(BeNil()),
						},
						func() spec {
							var (
								startRevision = int64(1)
								version       = "v0.1.1"
								dcd           = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
							)

							return spec{
								spec:                  "expired context",
								ctxFn:                 CanceledContext,
								refName:               refName,
								metaRefName:           metaRefName,
								dataHead:              dcd,
								startRevision:         startRevision,
								version:               version,
								matchErr:              MatchError(context.Canceled),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        delegateToMatcher(BeNil()),
							}
						}(),
						func() spec {
							var (
								startRevision = int64(1)
								version       = "v0.1.1"
								mcd           = &CommitDef{Message: "1"}
							)

							return spec{
								spec:                  "expired context",
								ctxFn:                 CanceledContext,
								refName:               refName,
								metaRefName:           metaRefName,
								metaHead:              metaHeadFrom(mcd, nil),
								startRevision:         startRevision,
								version:               version,
								matchErr:              MatchError(context.Canceled),
								matchDataCommitDefPtr: BeNil(),
								expectMetaHead:        expectMetaHead(mcd, nil, false),
							}
						}(),
						func() spec {
							var (
								startRevision = int64(1)
								version       = "v0.1.1"
								dcd           = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								mcd           = &CommitDef{Message: "1"}
							)

							return spec{
								spec:                  "expired context",
								ctxFn:                 CanceledContext,
								refName:               refName,
								metaRefName:           metaRefName,
								dataHead:              dcd,
								metaHead:              metaHeadFrom(mcd, dcd),
								startRevision:         startRevision,
								version:               version,
								matchErr:              MatchError(context.Canceled),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        expectMetaHead(mcd, dcd, false),
							}
						}(),
						func() spec {
							var (
								startRevision        = int64(1)
								strPrevStartRevision = revisionToString(startRevision - 1)
								dcd                  = &CommitDef{Message: strPrevStartRevision}
								mcd                  = &CommitDef{
									Message: strPrevStartRevision,
									Tree: TreeDef{Blobs: map[string][]byte{
										metadataPathRevision: []byte(strPrevStartRevision),
										metadataPathVersion:  []byte(Version),
									}},
								}
							)
							return spec{
								spec:                  "no data, no metadata",
								refName:               refName,
								metaRefName:           metaRefName,
								startRevision:         startRevision,
								matchErr:              Succeed(),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        expectMetaHead(mcd, dcd, true),
							}
						}(),
						func() spec {
							var (
								startRevision        = int64(3)
								strPrevStartRevision = revisionToString(startRevision - 1)
								version              = "v9.5.3"
								dcd                  = &CommitDef{Message: strPrevStartRevision}
								mcd                  = &CommitDef{
									Message: strPrevStartRevision,
									Tree: TreeDef{Blobs: map[string][]byte{
										metadataPathRevision: []byte(strPrevStartRevision),
										metadataPathVersion:  []byte(version),
									}},
								}
							)
							return spec{
								spec:                  "no data, no metadata",
								refName:               refName,
								metaRefName:           metaRefName,
								startRevision:         startRevision,
								version:               version,
								matchErr:              Succeed(),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        expectMetaHead(mcd, dcd, true),
							}
						}(),
						func() spec {
							var (
								mcp = &CommitDef{
									Message: "invalid",
									Tree:    TreeDef{Blobs: map[string][]byte{"invalid": []byte("invalid")}},
								}
							)
							return spec{
								spec:                  "no data, invalid metadata",
								refName:               refName,
								metaRefName:           metaRefName,
								metaHead:              metaHeadFrom(mcp, nil),
								startRevision:         1,
								matchErr:              MatchError(rpctypes.ErrGRPCMemberExist),
								matchDataCommitDefPtr: BeNil(),
								expectMetaHead:        expectMetaHead(mcp, nil, false),
							}
						}(),
						func() spec {
							var (
								startRevision        = int64(3)
								strPrevStartRevision = revisionToString(startRevision - 1)

								mcp = &CommitDef{
									Message: "invalid",
									Tree:    TreeDef{Blobs: map[string][]byte{"invalid": []byte("invalid")}},
								}

								dcd = &CommitDef{Message: strPrevStartRevision}

								mcd = &CommitDef{
									Message: strPrevStartRevision,
									Tree: TreeDef{Blobs: map[string][]byte{
										metadataPathRevision: []byte(strPrevStartRevision),
										metadataPathVersion:  []byte(Version),
									}},
								}
							)
							return spec{
								spec:                  "no data, invalid metadata",
								refName:               refName,
								metaRefName:           metaRefName,
								metaHead:              metaHeadFrom(mcp, nil),
								startRevision:         startRevision,
								force:                 true,
								matchErr:              Succeed(),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        expectMetaHead(mcd, dcd, true),
							}
						}(),
						func() spec {
							var (
								startRevision        = int64(3)
								nextRevison          = startRevision + 1
								strStartRevision     = revisionToString(startRevision)
								strNextStartRevision = revisionToString(nextRevison)
								strPrevStartRevision = revisionToString(startRevision - 1)
								version              = "v12.3.135-rc1"

								dcd = &CommitDef{
									Message: "2",
									Tree: TreeDef{
										Blobs: map[string][]byte{"1": []byte("11")},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"1": []byte("11")}},
										},
									},
									Parents: []CommitDef{{
										Message: "1",
										Tree: TreeDef{
											Blobs: map[string][]byte{"1": []byte("1")},
											Subtrees: map[string]TreeDef{
												"a": {Blobs: map[string][]byte{"1": []byte("1")}},
											},
										},
									}},
								}

								mcd = &CommitDef{
									Message: strNextStartRevision,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strNextStartRevision),
											metadataPathVersion:  []byte(version),
										},
										Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strNextStartRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("2"),
											}},
											"a": {
												Subtrees: map[string]TreeDef{
													"1": {Blobs: map[string][]byte{
														etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
														etcdserverpb.Compare_MOD.String():     []byte(strNextStartRevision),
														etcdserverpb.Compare_VERSION.String(): []byte("2"),
													}},
												},
											},
										},
									},
									Parents: []CommitDef{{
										Message: strStartRevision,
										Tree: TreeDef{
											Blobs: map[string][]byte{
												metadataPathRevision: []byte(strStartRevision),
												metadataPathVersion:  []byte(version),
											},
											Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
													etcdserverpb.Compare_MOD.String():     []byte(strStartRevision),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
												"a": {
													Subtrees: map[string]TreeDef{
														"1": {Blobs: map[string][]byte{
															etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
															etcdserverpb.Compare_MOD.String():     []byte(strStartRevision),
															etcdserverpb.Compare_VERSION.String(): []byte("1"),
														}},
													},
												},
											},
										},
										Parents: []CommitDef{{
											Message: strPrevStartRevision,
											Tree: TreeDef{
												Blobs: map[string][]byte{
													metadataPathRevision: []byte(strPrevStartRevision),
													metadataPathVersion:  []byte(version),
												},
											},
										}},
									}},
								}
							)
							return spec{
								spec:                  "with data, no metadata",
								refName:               refName,
								metaRefName:           metaRefName,
								dataHead:              dcd,
								startRevision:         startRevision,
								version:               version,
								matchErr:              Succeed(),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        expectMetaHeadInherit(mcd, dcd, true),
							}
						}(),
						func() spec {
							var (
								startRevision        = int64(3)
								nextRevison          = startRevision + 1
								strStartRevision     = revisionToString(startRevision)
								strNextStartRevision = revisionToString(nextRevison)
								strPrevStartRevision = revisionToString(startRevision - 1)
								version              = "v12.3.135-rc1"

								dcd = &CommitDef{
									Message: "2",
									Tree: TreeDef{
										Blobs: map[string][]byte{"1": []byte("11")},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"1": []byte("11")}},
										},
									},
									Parents: []CommitDef{{
										Message: "1",
										Tree: TreeDef{
											Blobs: map[string][]byte{"1": []byte("1")},
											Subtrees: map[string]TreeDef{
												"a": {Blobs: map[string][]byte{"1": []byte("1")}},
											},
										},
									}},
								}

								dcp = &dcd.Parents[0]

								mcd = &CommitDef{
									Message: strNextStartRevision,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strNextStartRevision),
											metadataPathVersion:  []byte(version),
										},
										Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strNextStartRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("2"),
											}},
											"a": {
												Subtrees: map[string]TreeDef{
													"1": {Blobs: map[string][]byte{
														etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
														etcdserverpb.Compare_MOD.String():     []byte(strNextStartRevision),
														etcdserverpb.Compare_VERSION.String(): []byte("2"),
													}},
												},
											},
										},
									},
									Parents: []CommitDef{{
										Message: strStartRevision,
										Tree: TreeDef{
											Blobs: map[string][]byte{
												metadataPathRevision: []byte(strStartRevision),
												metadataPathVersion:  []byte(version),
											},
											Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
													etcdserverpb.Compare_MOD.String():     []byte(strStartRevision),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
												"a": {
													Subtrees: map[string]TreeDef{
														"1": {Blobs: map[string][]byte{
															etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
															etcdserverpb.Compare_MOD.String():     []byte(strStartRevision),
															etcdserverpb.Compare_VERSION.String(): []byte("1"),
														}},
													},
												},
											},
										},
										Parents: []CommitDef{{
											Message: strPrevStartRevision,
											Tree: TreeDef{
												Blobs: map[string][]byte{
													metadataPathRevision: []byte(strPrevStartRevision),
													metadataPathVersion:  []byte(version),
												},
											},
										}},
									}},
								}

								mcp = &mcd.Parents[0]
							)
							return spec{
								spec:                  "with data, with part metadata",
								refName:               refName,
								metaRefName:           metaRefName,
								dataHead:              dcd,
								metaHead:              metaHeadFrom(mcp, dcp),
								startRevision:         startRevision,
								version:               version,
								matchErr:              MatchError(rpctypes.ErrGRPCMemberExist),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        expectMetaHead(mcp, dcp, true),
							}
						}(),
						func() spec {
							var (
								startRevision        = int64(3)
								nextRevison          = startRevision + 1
								strStartRevision     = revisionToString(startRevision)
								strNextStartRevision = revisionToString(nextRevison)
								strPrevStartRevision = revisionToString(startRevision - 1)
								version              = "v12.3.135-rc1"

								dcd = &CommitDef{
									Message: "2",
									Tree: TreeDef{
										Blobs: map[string][]byte{"1": []byte("11")},
										Subtrees: map[string]TreeDef{
											"a": {Blobs: map[string][]byte{"1": []byte("11")}},
										},
									},
									Parents: []CommitDef{{
										Message: "1",
										Tree: TreeDef{
											Blobs: map[string][]byte{"1": []byte("1")},
											Subtrees: map[string]TreeDef{
												"a": {Blobs: map[string][]byte{"1": []byte("1")}},
											},
										},
									}},
								}

								dcp = &dcd.Parents[0]

								mcd = &CommitDef{
									Message: strNextStartRevision,
									Tree: TreeDef{
										Blobs: map[string][]byte{
											metadataPathRevision: []byte(strNextStartRevision),
											metadataPathVersion:  []byte(version),
										},
										Subtrees: map[string]TreeDef{
											"1": {Blobs: map[string][]byte{
												etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
												etcdserverpb.Compare_MOD.String():     []byte(strNextStartRevision),
												etcdserverpb.Compare_VERSION.String(): []byte("2"),
											}},
											"a": {
												Subtrees: map[string]TreeDef{
													"1": {Blobs: map[string][]byte{
														etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
														etcdserverpb.Compare_MOD.String():     []byte(strNextStartRevision),
														etcdserverpb.Compare_VERSION.String(): []byte("2"),
													}},
												},
											},
										},
									},
									Parents: []CommitDef{{
										Message: strStartRevision,
										Tree: TreeDef{
											Blobs: map[string][]byte{
												metadataPathRevision: []byte(strStartRevision),
												metadataPathVersion:  []byte(version),
											},
											Subtrees: map[string]TreeDef{
												"1": {Blobs: map[string][]byte{
													etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
													etcdserverpb.Compare_MOD.String():     []byte(strStartRevision),
													etcdserverpb.Compare_VERSION.String(): []byte("1"),
												}},
												"a": {
													Subtrees: map[string]TreeDef{
														"1": {Blobs: map[string][]byte{
															etcdserverpb.Compare_CREATE.String():  []byte(strStartRevision),
															etcdserverpb.Compare_MOD.String():     []byte(strStartRevision),
															etcdserverpb.Compare_VERSION.String(): []byte("1"),
														}},
													},
												},
											},
										},
										Parents: []CommitDef{{
											Message: strPrevStartRevision,
											Tree: TreeDef{
												Blobs: map[string][]byte{
													metadataPathRevision: []byte(strPrevStartRevision),
													metadataPathVersion:  []byte(version),
												},
											},
										}},
									}},
								}

								mcp = &mcd.Parents[0]
							)
							return spec{
								spec:                  "with data, with part metadata",
								refName:               refName,
								metaRefName:           metaRefName,
								dataHead:              dcd,
								metaHead:              metaHeadFrom(mcp, dcp),
								force:                 true,
								startRevision:         startRevision,
								version:               version,
								matchErr:              Succeed(),
								matchDataCommitDefPtr: PointTo(GetCommitDefMatcher(dcd)),
								expectMetaHead:        expectMetaHeadInherit(mcd, dcd, true),
							}
						}(),
					} {
						func(s spec) {
							Describe(
								fmt.Sprintf(
									"%s: refName=%s, metaRefName:%s, canceled context=%t, dataC=%v, metadata exists=%t startRevision=%d, version=%s, force=%t",
									s.spec,
									s.refName,
									s.metaRefName,
									s.ctxFn != nil,
									s.dataHead,
									s.metaHead != nil,
									s.startRevision,
									s.version,
									s.force,
								),
								func() {
									var parentCtx context.Context

									BeforeEach(func() {
										var refsToCreate = make(map[git.ReferenceName]metaHeadFunc, 2)

										b.refName = s.refName
										b.metadataRefName = s.metaRefName

										if s.dataHead != nil {
											refsToCreate[s.refName] = func(ctx context.Context, repo git.Repository) (git.Commit, error) {
												return CreateAndLoadCommitFromDef(ctx, repo, s.dataHead)
											}
										}

										if s.metaHead != nil {
											refsToCreate[s.metaRefName] = s.metaHead
										}

										for refName, commitFn := range refsToCreate {
											var c git.Commit

											Expect(func() (err error) { c, err = commitFn(ctx, b.repo); return }()).To(Succeed())

											defer c.Close()

											Expect(func() (err error) {
												var rc git.ReferenceCollection

												if rc, err = b.repo.References(); err != nil {
													return
												}

												defer rc.Close()

												err = rc.Create(ctx, refName, c.ID(), true, string(refName))
												return
											}()).To(Succeed())
										}

										parentCtx = ctx
										if s.ctxFn != nil {
											ctx = s.ctxFn(ctx)
										}
									})

									It(ItSpecForMatchError(s.matchErr), func() {
										Expect(b.initMetadata(ctx, s.startRevision, s.version, s.force)).To(s.matchErr)

										for refName, expectHeadFn := range map[git.ReferenceName]expectMetaHeadFunc{
											s.refName:     delegateToMatcher(s.matchDataCommitDefPtr),
											s.metaRefName: s.expectMetaHead,
										} {
											if expectHeadFn != nil {
												var cd *CommitDef

												Expect(func() (err error) {
													cd, err = GetCommitDefForReferenceName(parentCtx, b.repo, refName)
													return
												}()).To(Or(Succeed(), PointTo(MatchFields(IgnoreExtras, Fields{"Code": Equal(impl.ErrorCodeNotFound)}))))

												expectHeadFn(parentCtx, b.repo, cd, string(refName))
											}
										}
									})
								},
							)
						}(s)
					}
				}
			})
		})
	})
})
