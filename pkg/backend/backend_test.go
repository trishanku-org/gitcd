package backend

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	mockgit "github.com/trishanku/gitcd/pkg/mocks/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

var _ = Describe("validateReferenceName", func() {
	for refName, expectErr := range map[string]bool{
		"":                       true,
		"a":                      false,
		DefaultDataReferenceName: false,
	} {
		func(refName git.ReferenceName, expectErr bool) {
			var spec = fmt.Sprintf("%q should succeed", refName)

			if expectErr {
				spec = fmt.Sprintf("%q should fail", refName)
			}

			It(spec, func() {
				var (
					r   git.ReferenceName
					err error
				)

				r, err = validateReferenceName(refName)

				if expectErr {
					Expect(err).To(MatchError(rpctypes.ErrGRPCCorrupt))
					Expect(r).To(BeEmpty())
				} else {
					Expect(err).ToNot(HaveOccurred())
					Expect(r).To(Equal(refName))
				}
			})
		}(git.ReferenceName(refName), expectErr)
	}
})

var _ = Describe("backend", func() {
	var b *backend

	BeforeEach(func() {
		b = &backend{}
	})

	Describe("getDataRefName", func() {
		for refName, expectErr := range map[string]bool{
			"":                       true,
			"a":                      false,
			DefaultDataReferenceName: false,
		} {
			func(refName git.ReferenceName, expectErr bool) {
				var spec = fmt.Sprintf("%q should succeed", refName)

				if expectErr {
					spec = fmt.Sprintf("%q should fail", refName)
				}

				It(spec, func() {
					var (
						r   git.ReferenceName
						err error
					)

					b.refName = refName

					r, err = b.getDataRefName()

					if expectErr {
						Expect(err).To(MatchError(rpctypes.ErrGRPCCorrupt))
						Expect(r).To(BeEmpty())
					} else {
						Expect(err).ToNot(HaveOccurred())
						Expect(r).To(Equal(refName))
					}
				})
			}(git.ReferenceName(refName), expectErr)
		}
	})

	Describe("getMetaRefName", func() {
		for metaRefName, expectErr := range map[string]bool{
			"":                         true,
			"a":                        false,
			"refs/gitcd/metadata/main": false,
			"refs/meta/custom":         false,
		} {
			func(metaRefName git.ReferenceName, expectErr bool) {
				var spec = fmt.Sprintf("%q should succeed", metaRefName)

				if expectErr {
					spec = fmt.Sprintf("%q should fail", metaRefName)
				}

				It(spec, func() {
					var (
						r   git.ReferenceName
						err error
					)

					b.metadataRefName = metaRefName

					r, err = b.getMetadataRefName()

					if expectErr {
						Expect(err).To(MatchError(rpctypes.ErrGRPCCorrupt))
						Expect(r).To(BeEmpty())
					} else {
						Expect(err).ToNot(HaveOccurred())
						Expect(r).To(Equal(metaRefName))
					}
				})
			}(git.ReferenceName(metaRefName), expectErr)
		}
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

		Describe("getReference", func() {
			var (
				refName   git.ReferenceName
				matchErr  types.GomegaMatcher
				expectRef func(git.Reference)
			)

			JustBeforeEach(func() {
				var ref, err = b.getReference(ctx, refName)

				Expect(err).To(matchErr)
				expectRef(ref)
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					cancelFn()

					matchErr = MatchError(ctx.Err())
					expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
				})

				ItShouldFail()
			})

			Describe("empty repo", func() {
				BeforeEach(func() {
					refName = DefaultDataReferenceName
					matchErr = HaveOccurred()
					expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
				})

				ItShouldFail()
			})

			Describe("with a reference", func() {
				var cd *CommitDef

				BeforeEach(func() {
					refName = DefaultDataReferenceName
					cd = &CommitDef{
						Message: "1",
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
					}

					Expect(func() (err error) {
						var (
							rc git.ReferenceCollection
							id git.ObjectID
						)

						if rc, err = b.repo.References(); err != nil {
							return
						}

						defer rc.Close()

						if id, err = CreateCommitFromDef(ctx, b.repo, cd); err != nil {
							return
						}

						err = rc.Create(ctx, refName, id, false, "1")
						return
					}()).To(Succeed())
				})

				Describe("reference name exists", func() {
					BeforeEach(func() {
						matchErr = Succeed()
						expectRef = func(ref git.Reference) {
							Expect(ref).ToNot(BeNil())

							Expect(func() (cd CommitDef, err error) {
								var c git.Commit

								if c, err = b.repo.Peeler().PeelToCommit(ctx, ref); err != nil {
									return
								}

								defer c.Close()

								cd = *GetCommitDefByCommit(ctx, b.repo, c)
								return
							}()).To(GetCommitDefMatcher(cd))
						}
					})

					ItShouldSucceed()
				})

				Describe("reference name does not exist", func() {
					BeforeEach(func() {
						refName = "refs/heads/does_not_exist"
						matchErr = HaveOccurred()
						expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
					})

					ItShouldFail()
				})
			})
		})

		Describe("getMetadataReference", func() {
			var (
				matchErr  types.GomegaMatcher
				expectRef func(git.Reference)
			)

			BeforeEach(func() {
				b.metadataRefName = "refs/gitcd/metadata/main"
			})

			JustBeforeEach(func() {
				var ref, err = b.getMetadataReference(ctx)

				Expect(err).To(matchErr)
				expectRef(ref)
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					cancelFn()

					matchErr = MatchError(ctx.Err())
					expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
				})

				ItShouldFail()
			})

			Describe("empty repo", func() {
				BeforeEach(func() {
					matchErr = HaveOccurred()
					expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
				})

				ItShouldFail()
			})

			Describe("without metadata reference", func() {
				BeforeEach(func() {
					b.metadataRefName = ""
					matchErr = MatchError(rpctypes.ErrGRPCCorrupt)
					expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
				})

				ItShouldFail()
			})

			Describe("with metadata reference", func() {
				var cd *CommitDef

				BeforeEach(func() {
					cd = &CommitDef{
						Message: "1",
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
					}

					Expect(func() (err error) {
						var (
							refName git.ReferenceName
							rc      git.ReferenceCollection
							id      git.ObjectID
						)

						if refName, err = b.getMetadataRefName(); err != nil {
							return
						}

						if rc, err = b.repo.References(); err != nil {
							return
						}

						defer rc.Close()

						if id, err = CreateCommitFromDef(ctx, b.repo, cd); err != nil {
							return
						}

						err = rc.Create(ctx, refName, id, false, "1")
						return
					}()).To(Succeed())
				})

				Describe("reference name exists", func() {
					BeforeEach(func() {
						matchErr = Succeed()
						expectRef = func(ref git.Reference) {
							Expect(ref).ToNot(BeNil())

							Expect(func() (cd CommitDef, err error) {
								var c git.Commit

								if c, err = b.repo.Peeler().PeelToCommit(ctx, ref); err != nil {
									return
								}

								defer c.Close()

								cd = *GetCommitDefByCommit(ctx, b.repo, c)
								return
							}()).To(GetCommitDefMatcher(cd))
						}
					})

					ItShouldSucceed()
				})

				Describe("reference name does not exist", func() {
					BeforeEach(func() {
						b.metadataRefName = "refs/gitcd/metadata/does_not_exist"
						matchErr = HaveOccurred()
						expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
					})

					ItShouldFail()
				})
			})
		})

		Describe("getContent", func() {
			type check struct {
				path                   string
				ctxFn                  ContextFunc
				matchErr, matchContent types.GomegaMatcher
			}

			var (
				t git.Tree

				td = &TreeDef{
					Blobs: map[string][]byte{"empty": {}, "1": []byte("1"), "2": []byte("2")},
					Subtrees: map[string]TreeDef{
						"3": {
							Blobs:    map[string][]byte{"2": []byte("2"), "3": []byte("3")},
							Subtrees: map[string]TreeDef{"1": {Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}},
						},
					},
				}
			)

			BeforeEach(func() {
				Expect(func() (err error) {
					var id git.ObjectID

					if id, err = CreateTreeFromDef(ctx, b.repo, td); err != nil {
						return
					}

					t, err = b.repo.ObjectGetter().GetTree(ctx, id)
					return
				}()).To(Succeed())
			})

			AfterEach(func() {
				if t != nil {
					Expect(t.Close()).To(Succeed())
				}
			})

			for _, s := range []check{
				{
					path:         "expired context",
					ctxFn:        CanceledContext,
					matchErr:     MatchError("context canceled"),
					matchContent: BeNil(),
				},
				{path: "", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "/", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "empty", matchErr: Succeed(), matchContent: BeEmpty()},
				{path: "1", matchErr: Succeed(), matchContent: Equal([]byte("1"))},
				{path: "/1", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "1/1", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "2", matchErr: Succeed(), matchContent: Equal([]byte("2"))},
				{path: "3", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "3/1", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "3/1/1", matchErr: Succeed(), matchContent: Equal([]byte("1"))},
				{path: "3/1/2", matchErr: Succeed(), matchContent: Equal([]byte("2"))},
				{path: "3/2", matchErr: Succeed(), matchContent: Equal([]byte("2"))},
				{path: "3/3", matchErr: Succeed(), matchContent: Equal([]byte("3"))},
				{path: "/3/3", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "3/1/3", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "3/3/1", matchErr: HaveOccurred(), matchContent: BeNil()},
				{path: "9", matchErr: HaveOccurred(), matchContent: BeNil()},
			} {
				func(s check) {
					It(s.path, func() {
						var (
							content []byte
							err     error
						)

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						content, err = b.getContent(ctx, t, s.path)
						Expect(err).To(s.matchErr)
						Expect(content).To(s.matchContent)
					})
				}(s)
			}
		})

		Describe("getContentForTreeEntry", func() {
			type treeEntryFunc func() git.TreeEntry

			type check struct {
				spec                   string
				ctxFn                  ContextFunc
				treeEntryFn            treeEntryFunc
				matchErr, matchContent types.GomegaMatcher
			}

			var (
				t git.Tree

				td = &TreeDef{
					Blobs: map[string][]byte{"empty": {}, "1": []byte("1"), "2": []byte("2")},
					Subtrees: map[string]TreeDef{
						"3": {
							Blobs:    map[string][]byte{"2": []byte("2"), "3": []byte("3")},
							Subtrees: map[string]TreeDef{"1": {Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}},
						},
					},
				}

				makeTreeEntryFn = func(id git.ObjectID, typ git.ObjectType) treeEntryFunc {
					return func() git.TreeEntry {
						var te = mockgit.NewMockTreeEntry(gomock.NewController(GinkgoT()))

						te.EXPECT().EntryID().Return(id)
						te.EXPECT().EntryType().Return(typ)
						return te
					}
				}

				getTreeEntryFn = func(path string) treeEntryFunc {
					return func() (te git.TreeEntry) {
						Expect(func() (err error) { te, err = t.GetEntryByPath(ctx, path); return }()).To(Succeed())
						return
					}
				}
			)

			BeforeEach(func() {
				Expect(func() (err error) {
					var id git.ObjectID

					if id, err = CreateTreeFromDef(ctx, b.repo, td); err != nil {
						return
					}

					t, err = b.repo.ObjectGetter().GetTree(ctx, id)
					return
				}()).To(Succeed())
			})

			AfterEach(func() {
				if t != nil {
					Expect(t.Close()).To(Succeed())
				}
			})

			for _, s := range []check{
				{
					spec:         "expired context",
					ctxFn:        CanceledContext,
					treeEntryFn:  makeTreeEntryFn(git.ObjectID{}, git.ObjectTypeBlob),
					matchErr:     MatchError("context canceled"),
					matchContent: BeNil(),
				},
				{
					spec:         "tree",
					treeEntryFn:  makeTreeEntryFn(git.ObjectID{}, git.ObjectTypeTree),
					matchErr:     MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)),
					matchContent: BeNil(),
				},
				{
					spec:         "empty ID",
					treeEntryFn:  makeTreeEntryFn(git.ObjectID{}, git.ObjectTypeBlob),
					matchErr:     HaveOccurred(),
					matchContent: BeNil(),
				},
				{
					spec:         "invalid ID",
					treeEntryFn:  makeTreeEntryFn(git.ObjectID{1}, git.ObjectTypeBlob),
					matchErr:     HaveOccurred(),
					matchContent: BeNil(),
				},
				{spec: "empty", treeEntryFn: getTreeEntryFn("empty"), matchErr: Succeed(), matchContent: BeEmpty()},
				{spec: "1", treeEntryFn: getTreeEntryFn("1"), matchErr: Succeed(), matchContent: Equal([]byte("1"))},
				{spec: "2", treeEntryFn: getTreeEntryFn("2"), matchErr: Succeed(), matchContent: Equal([]byte("2"))},
				{
					spec:         "3",
					treeEntryFn:  getTreeEntryFn("3"),
					matchErr:     MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)),
					matchContent: BeNil(),
				},
				{
					spec:         "3/1",
					treeEntryFn:  getTreeEntryFn("3/1"),
					matchErr:     MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)),
					matchContent: BeNil(),
				},
				{spec: "3/1/1", treeEntryFn: getTreeEntryFn("3/1/1"), matchErr: Succeed(), matchContent: Equal([]byte("1"))},
				{spec: "3/1/2", treeEntryFn: getTreeEntryFn("3/1/2"), matchErr: Succeed(), matchContent: Equal([]byte("2"))},
				{spec: "3/2", treeEntryFn: getTreeEntryFn("3/2"), matchErr: Succeed(), matchContent: Equal([]byte("2"))},
				{spec: "3/3", treeEntryFn: getTreeEntryFn("3/3"), matchErr: Succeed(), matchContent: Equal([]byte("3"))},
			} {
				func(s check) {
					It(s.spec, func() {
						var (
							content []byte
							err     error
						)

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						content, err = b.getContentForTreeEntry(ctx, s.treeEntryFn())
						Expect(err).To(s.matchErr)
						Expect(content).To(s.matchContent)
					})
				}(s)
			}
		})

		Describe("readRevision", func() {
			type check struct {
				path                    string
				ctxFn                   ContextFunc
				matchErr, matchRevision types.GomegaMatcher
			}

			var (
				t git.Tree

				td = &TreeDef{
					Blobs: map[string][]byte{
						"empty":    {},
						"minInt64": []byte(revisionToString(math.MinInt64)),
						"maxInt64": []byte(revisionToString(math.MaxInt64)),
						"a":        []byte("a"),
					},
					Subtrees: map[string]TreeDef{
						"b": {
							Blobs:    map[string][]byte{"2": []byte("2"), "3": []byte("3")},
							Subtrees: map[string]TreeDef{"1": {Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}},
						},
					},
				}
			)

			BeforeEach(func() {
				Expect(func() (err error) {
					var id git.ObjectID

					if id, err = CreateTreeFromDef(ctx, b.repo, td); err != nil {
						return
					}

					t, err = b.repo.ObjectGetter().GetTree(ctx, id)
					return
				}()).To(Succeed())
			})

			AfterEach(func() {
				if t != nil {
					Expect(t.Close()).To(Succeed())
				}
			})

			for _, s := range []check{
				{
					path:          "expired context",
					ctxFn:         CanceledContext,
					matchErr:      MatchError("context canceled"),
					matchRevision: BeZero(),
				},
				{path: "", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "/", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "empty", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "minInt64", matchErr: Succeed(), matchRevision: Equal(int64(math.MinInt64))},
				{path: "/minInt64", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "minInt64/1", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "maxInt64", matchErr: Succeed(), matchRevision: Equal(int64(math.MaxInt64))},
				{path: "a", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "b", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "b/1", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "b/1/1", matchErr: Succeed(), matchRevision: Equal(int64(1))},
				{path: "b/1/2", matchErr: Succeed(), matchRevision: Equal(int64(2))},
				{path: "b/2", matchErr: Succeed(), matchRevision: Equal(int64(2))},
				{path: "b/3", matchErr: Succeed(), matchRevision: Equal(int64(3))},
				{path: "/b/3", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "b/1/3", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "b/3/1", matchErr: HaveOccurred(), matchRevision: BeZero()},
				{path: "9", matchErr: HaveOccurred(), matchRevision: BeZero()},
			} {
				func(s check) {
					It(s.path, func() {
						var (
							rev int64
							err error
						)

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						rev, err = b.readRevision(ctx, t, s.path)
						Expect(err).To(s.matchErr)
						Expect(rev).To(s.matchRevision)
					})
				}(s)
			}
		})

		Describe("readRevisionFromTreeEntry", func() {
			type treeEntryFunc func() git.TreeEntry

			type check struct {
				spec                    string
				ctxFn                   ContextFunc
				treeEntryFn             treeEntryFunc
				matchErr, matchRevision types.GomegaMatcher
			}

			var (
				t git.Tree

				td = &TreeDef{
					Blobs: map[string][]byte{
						"empty":    {},
						"minInt64": []byte(revisionToString(math.MinInt64)),
						"maxInt64": []byte(revisionToString(math.MaxInt64)),
						"a":        []byte("a"),
					},
					Subtrees: map[string]TreeDef{
						"b": {
							Blobs:    map[string][]byte{"2": []byte("2"), "3": []byte("3")},
							Subtrees: map[string]TreeDef{"1": {Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}},
						},
					},
				}

				makeTreeEntryFn = func(id git.ObjectID, typ git.ObjectType) treeEntryFunc {
					return func() git.TreeEntry {
						var te = mockgit.NewMockTreeEntry(gomock.NewController(GinkgoT()))

						te.EXPECT().EntryID().Return(id)
						te.EXPECT().EntryType().Return(typ)
						return te
					}
				}

				getTreeEntryFn = func(path string) treeEntryFunc {
					return func() (te git.TreeEntry) {
						Expect(func() (err error) { te, err = t.GetEntryByPath(ctx, path); return }()).To(Succeed())
						return
					}
				}
			)

			BeforeEach(func() {
				Expect(func() (err error) {
					var id git.ObjectID

					if id, err = CreateTreeFromDef(ctx, b.repo, td); err != nil {
						return
					}

					t, err = b.repo.ObjectGetter().GetTree(ctx, id)
					return
				}()).To(Succeed())
			})

			AfterEach(func() {
				if t != nil {
					Expect(t.Close()).To(Succeed())
				}
			})

			for _, s := range []check{
				{
					spec:          "expired context",
					ctxFn:         CanceledContext,
					treeEntryFn:   makeTreeEntryFn(git.ObjectID{}, git.ObjectTypeBlob),
					matchErr:      MatchError("context canceled"),
					matchRevision: BeZero(),
				},
				{
					spec:          "tree",
					treeEntryFn:   makeTreeEntryFn(git.ObjectID{}, git.ObjectTypeTree),
					matchErr:      MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)),
					matchRevision: BeZero(),
				},
				{
					spec:          "empty ID",
					treeEntryFn:   makeTreeEntryFn(git.ObjectID{}, git.ObjectTypeBlob),
					matchErr:      HaveOccurred(),
					matchRevision: BeZero(),
				},
				{
					spec:          "invalid ID",
					treeEntryFn:   makeTreeEntryFn(git.ObjectID{1}, git.ObjectTypeBlob),
					matchErr:      HaveOccurred(),
					matchRevision: BeZero(),
				},
				{spec: "empty", treeEntryFn: getTreeEntryFn("empty"), matchErr: HaveOccurred(), matchRevision: BeZero()},
				{spec: "minInt64", treeEntryFn: getTreeEntryFn("minInt64"), matchErr: Succeed(), matchRevision: Equal(int64(math.MinInt64))},
				{spec: "maxInt64", treeEntryFn: getTreeEntryFn("maxInt64"), matchErr: Succeed(), matchRevision: Equal(int64(math.MaxInt64))},
				{spec: "a", treeEntryFn: getTreeEntryFn("a"), matchErr: HaveOccurred(), matchRevision: BeZero()},
				{
					spec:          "b",
					treeEntryFn:   getTreeEntryFn("b"),
					matchErr:      MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)),
					matchRevision: BeZero(),
				},
				{
					spec:          "b/1",
					treeEntryFn:   getTreeEntryFn("b/1"),
					matchErr:      MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)),
					matchRevision: BeZero(),
				},
				{spec: "b/1/1", treeEntryFn: getTreeEntryFn("b/1/1"), matchErr: Succeed(), matchRevision: Equal(int64(1))},
				{spec: "b/1/2", treeEntryFn: getTreeEntryFn("b/1/2"), matchErr: Succeed(), matchRevision: Equal(int64(2))},
				{spec: "b/2", treeEntryFn: getTreeEntryFn("b/2"), matchErr: Succeed(), matchRevision: Equal(int64(2))},
				{spec: "b/3", treeEntryFn: getTreeEntryFn("b/3"), matchErr: Succeed(), matchRevision: Equal(int64(3))},
			} {
				func(s check) {
					It(s.spec, func() {
						var (
							rev int64
							err error
						)

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						rev, err = b.readRevisionFromTreeEntry(ctx, s.treeEntryFn())
						Expect(err).To(s.matchErr)
						Expect(rev).To(s.matchRevision)
					})
				}(s)
			}
		})

		Describe("readOjectID", func() {
			type check struct {
				path              string
				ctxFn             ContextFunc
				matchErr, matchID types.GomegaMatcher
			}

			var (
				t git.Tree

				emptyID = git.ObjectID{}
				id1     = "1111111111111111111111111111111111111111"
				id2     = "2222222222222222222222222222222222222222"
				id3     = "3333333333333333333333333333333333333333"

				td = &TreeDef{
					Blobs: map[string][]byte{
						"empty": {},
						"1":     []byte(id1),
						"a":     []byte("a"),
						"short": []byte("1111"),
					},
					Subtrees: map[string]TreeDef{
						"b": {
							Blobs: map[string][]byte{"2": []byte(id2), "3": []byte(id3)},
							Subtrees: map[string]TreeDef{
								"1": {
									Blobs: map[string][]byte{
										"1": []byte(id1),
										"2": []byte(id2),
									},
								},
							},
						},
					},
				}

				aToID = func(s string) (id git.ObjectID) {
					id, _ = git.NewObjectID(s)
					return
				}
			)

			BeforeEach(func() {
				Expect(func() (err error) {
					var id git.ObjectID

					if id, err = CreateTreeFromDef(ctx, b.repo, td); err != nil {
						return
					}

					t, err = b.repo.ObjectGetter().GetTree(ctx, id)
					return
				}()).To(Succeed())
			})

			AfterEach(func() {
				if t != nil {
					Expect(t.Close()).To(Succeed())
				}
			})

			for _, s := range []check{
				{
					path:     "expired context",
					ctxFn:    CanceledContext,
					matchErr: MatchError("context canceled"),
					matchID:  Equal(emptyID),
				},
				{path: "", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "/", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "empty", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "1", matchErr: Succeed(), matchID: Equal(aToID(id1))},
				{path: "/1", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "a", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "short", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "b", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "b/1", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "b/1/1", matchErr: Succeed(), matchID: Equal(aToID(id1))},
				{path: "b/1/2", matchErr: Succeed(), matchID: Equal(aToID(id2))},
				{path: "b/2", matchErr: Succeed(), matchID: Equal(aToID(id2))},
				{path: "b/3", matchErr: Succeed(), matchID: Equal(aToID(id3))},
				{path: "/b/3", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "b/1/3", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "b/3/1", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
				{path: "9", matchErr: HaveOccurred(), matchID: Equal(emptyID)},
			} {
				func(s check) {
					It(s.path, func() {
						var (
							id  git.ObjectID
							err error
						)

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						id, err = b.readOjectID(ctx, t, s.path)
						Expect(err).To(s.matchErr)
						Expect(id).To(s.matchID)
					})
				}(s)
			}
		})

		Describe("getMetadataFor", func() {
			const (
				createRevision = int64(iota + 1)
				lease
				modRevision
				version
			)

			var (
				t git.Tree

				td = &TreeDef{
					Blobs: map[string][]byte{
						"emptyBlob":  {},
						"stringBlob": []byte("a"),
						"intBlob":    []byte("1"),
					},
					Subtrees: map[string]TreeDef{
						"emptyTree":            {},
						"emptyCreateRevision":  {Blobs: map[string][]byte{etcdserverpb.Compare_CREATE.String(): {}}},
						"treeCreateRevision":   {Subtrees: map[string]TreeDef{etcdserverpb.Compare_CREATE.String(): {}}},
						"stringCreateRevision": {Blobs: map[string][]byte{etcdserverpb.Compare_CREATE.String(): []byte("a")}},
						"onlyCreateRevision":   {Blobs: map[string][]byte{etcdserverpb.Compare_CREATE.String(): []byte(revisionToString(createRevision))}},
						"missingCreateRevision": {
							Blobs: map[string][]byte{
								etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(lease)),
								etcdserverpb.Compare_MOD.String():     []byte(revisionToString(modRevision)),
								etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(version)),
							},
						},
						"emptyLease": {Blobs: map[string][]byte{etcdserverpb.Compare_LEASE.String(): {}}},
						"treeLease": {
							Blobs:    map[string][]byte{etcdserverpb.Compare_CREATE.String(): []byte(revisionToString(createRevision))},
							Subtrees: map[string]TreeDef{etcdserverpb.Compare_LEASE.String(): {}},
						},
						"stringLease": {Blobs: map[string][]byte{etcdserverpb.Compare_LEASE.String(): []byte("a")}},
						"onlyLease":   {Blobs: map[string][]byte{etcdserverpb.Compare_LEASE.String(): []byte(revisionToString(lease))}},
						"missingLease": {
							Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(createRevision)),
								etcdserverpb.Compare_MOD.String():     []byte(revisionToString(modRevision)),
								etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(version)),
							},
						},
						"emptyModRevision": {Blobs: map[string][]byte{etcdserverpb.Compare_MOD.String(): {}}},
						"treeModRevision": {
							Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String(): []byte(revisionToString(createRevision)),
								etcdserverpb.Compare_LEASE.String():  []byte(revisionToString(lease)),
							},
							Subtrees: map[string]TreeDef{etcdserverpb.Compare_MOD.String(): {}},
						},
						"stringModRevision": {Blobs: map[string][]byte{etcdserverpb.Compare_MOD.String(): []byte("a")}},
						"onlyModRevision":   {Blobs: map[string][]byte{etcdserverpb.Compare_MOD.String(): []byte(revisionToString(modRevision))}},
						"missingModRevision": {
							Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(createRevision)),
								etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(lease)),
								etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(version)),
							},
						},
						"emptyVersion": {Blobs: map[string][]byte{etcdserverpb.Compare_VERSION.String(): {}}},
						"treeVersion": {
							Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String(): []byte(revisionToString(createRevision)),
								etcdserverpb.Compare_LEASE.String():  []byte(revisionToString(lease)),
								etcdserverpb.Compare_MOD.String():    []byte(revisionToString(modRevision)),
							},
							Subtrees: map[string]TreeDef{etcdserverpb.Compare_VERSION.String(): {}},
						},
						"stringVersion": {Blobs: map[string][]byte{etcdserverpb.Compare_VERSION.String(): []byte("a")}},
						"onlyVersion":   {Blobs: map[string][]byte{etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(1))}},
						"missingVersion": {
							Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String(): []byte(revisionToString(createRevision)),
								etcdserverpb.Compare_LEASE.String():  []byte(revisionToString(lease)),
								etcdserverpb.Compare_MOD.String():    []byte(revisionToString(modRevision)),
							},
						},
						"valid": {
							Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(createRevision)),
								etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(lease)),
								etcdserverpb.Compare_MOD.String():     []byte(revisionToString(modRevision)),
								etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(version)),
							},
						},
						"a": {
							Subtrees: map[string]TreeDef{
								"b": {
									Blobs: map[string][]byte{
										etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(createRevision + 100)),
										etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(lease + 100)),
										etcdserverpb.Compare_MOD.String():     []byte(revisionToString(modRevision + 100)),
										etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(version + 100)),
									},
								},
							},
						},
					},
				}

				k                 string
				matchErr, matchKV types.GomegaMatcher
			)

			BeforeEach(func() {
				Expect(func() (err error) {
					var id git.ObjectID

					if id, err = CreateTreeFromDef(ctx, b.repo, td); err != nil {
						return
					}

					t, err = b.repo.ObjectGetter().GetTree(ctx, id)
					return
				}()).To(Succeed())
				Expect(GetTreeDef(ctx, b.repo, t.ID())).To(PointTo(GetTreeDefMatcher(td)))
			})

			JustBeforeEach(func() {
				var kv, err = b.getMetadataFor(ctx, t, k)
				Expect(err).To(matchErr)
				Expect(kv).To(matchKV)
			})

			AfterEach(func() {
				if t != nil {
					Expect(t.Close()).To(Succeed())
				}
			})

			Describe("with expired context", func() {
				BeforeEach(func() {
					ctx = CanceledContext(ctx)

					k = ""
					matchErr = MatchError(ctx.Err())
					matchKV = BeNil()
				})

				ItShouldFail()
			})

			for _, prefix := range []string{"", "/", "registry", "registry/", "/registry", "/registry/", "/registry/prefix"} {
				func(prefix string) {
					Describe(fmt.Sprintf("with prefix %q", prefix), func() {
						BeforeEach(func() {
							b.keyPrefix.prefix = prefix
						})

						for _, s := range []struct {
							k                 string
							matchErr, matchKV types.GomegaMatcher
						}{
							{k: prefix + "", matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: "does_not_exist", matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "emptyBlob"), matchErr: MatchError(NewUnsupportedObjectType(git.ObjectTypeBlob)), matchKV: BeNil()},
							{k: path.Join(prefix, "stringBlob"), matchErr: MatchError(NewUnsupportedObjectType(git.ObjectTypeBlob)), matchKV: BeNil()},
							{k: path.Join(prefix, "intBlob"), matchErr: MatchError(NewUnsupportedObjectType(git.ObjectTypeBlob)), matchKV: BeNil()},
							{k: path.Join(prefix, "emptyTree"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "emptyCreateRevision"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "treeCreateRevision"), matchErr: MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)), matchKV: BeNil()},
							{k: path.Join(prefix, "stringCreateRevision"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "onlyCreateRevsion"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "missingCreateRevision"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "emptyLease"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "treeLease"), matchErr: MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)), matchKV: BeNil()},
							{k: path.Join(prefix, "stringLease"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "onlyLease"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{
								k:        path.Join(prefix, "missingLease"),
								matchErr: Succeed(),
								matchKV: Equal(&mvccpb.KeyValue{
									Key:            []byte(path.Join(prefix, "missingLease")),
									CreateRevision: createRevision,
									ModRevision:    modRevision,
									Version:        version,
								}),
							},
							{k: path.Join(prefix, "emptyModRevision"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "treeModRevision"), matchErr: MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)), matchKV: BeNil()},
							{k: path.Join(prefix, "stringModRevision"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "onlyModRevision"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "missingModRevision"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "emptyVersion"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "treeVersion"), matchErr: MatchError(NewUnsupportedObjectType(git.ObjectTypeTree)), matchKV: BeNil()},
							{k: path.Join(prefix, "stringVersion"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "onlyVersion"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{k: path.Join(prefix, "missingVersion"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{
								k:        path.Join(prefix, "valid"),
								matchErr: Succeed(),
								matchKV: Equal(&mvccpb.KeyValue{
									Key:            []byte(path.Join(prefix, "valid")),
									CreateRevision: createRevision,
									Lease:          lease,
									ModRevision:    modRevision,
									Version:        version,
								}),
							},
							{k: path.Join(prefix, "a"), matchErr: HaveOccurred(), matchKV: BeNil()},
							{
								k:        path.Join(prefix, "a/b"),
								matchErr: Succeed(),
								matchKV: Equal(&mvccpb.KeyValue{
									Key:            []byte(path.Join(prefix, "a/b")),
									CreateRevision: createRevision + 100,
									Lease:          lease + 100,
									ModRevision:    modRevision + 100,
									Version:        version + 100,
								}),
							},
							{k: path.Join(prefix, "a/c"), matchErr: HaveOccurred(), matchKV: BeNil()},
						} {
							func(key string, mErr, mKV types.GomegaMatcher) {
								Describe(key, func() {
									BeforeEach(func() {
										k = key
										matchErr = mErr
										matchKV = mKV
									})

									if matchErr == Succeed() {
										ItShouldSucceed()
									} else {
										ItShouldFail()
									}
								})
							}(s.k, s.matchErr, s.matchKV)
						}
					})
				}(prefix)
			}
		})

		Describe("getMetadataPeelableForRevision", func() {
			type check struct {
				spec                        string
				ctxFn                       ContextFunc
				cd                          *CommitDef
				revision                    int64
				matchErr, matchCommitDefPtr types.GomegaMatcher
			}

			var c git.Commit

			AfterEach(func() {
				if c != nil {
					Expect(c.Close()).To(Succeed())
				}
			})

			for _, s := range []check{
				{
					spec:              "expired context",
					ctxFn:             CanceledContext,
					cd:                &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte(revisionToString(1))}}},
					revision:          int64(1),
					matchErr:          MatchError("context canceled"),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "empty",
					cd:                &CommitDef{},
					matchErr:          And(HaveOccurred(), Not(MatchError(rpctypes.ErrGRPCFutureRev)), Not(MatchError(rpctypes.ErrGRPCCompacted))),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "no-revision",
					cd:                &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("1")}}},
					matchErr:          And(HaveOccurred(), Not(MatchError(rpctypes.ErrGRPCFutureRev)), Not(MatchError(rpctypes.ErrGRPCCompacted))),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "string-revision",
					cd:                &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("a")}}},
					matchErr:          And(HaveOccurred(), Not(MatchError(rpctypes.ErrGRPCFutureRev)), Not(MatchError(rpctypes.ErrGRPCCompacted))),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "future-revision",
					cd:                &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}},
					revision:          int64(2),
					matchErr:          MatchError(rpctypes.ErrGRPCFutureRev),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "compacted-revision",
					cd:                &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}}},
					revision:          int64(1),
					matchErr:          MatchError(rpctypes.ErrGRPCCompacted),
					matchCommitDefPtr: BeNil(),
				},
				func() check {
					var headCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}

					return check{
						spec:              "HEAD-only",
						cd:                headCD,
						revision:          int64(1),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(headCD)),
					}
				}(),
				func() check {
					var headCD = &CommitDef{
						Message: "2", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}},
						Parents: []CommitDef{{Message: "1"}},
					}

					return check{
						spec:              "HEAD-empty",
						cd:                headCD,
						revision:          int64(2),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(headCD)),
					}
				}(),
				func() check {
					return check{
						spec: "^1-future",
						cd: &CommitDef{
							Message: "2",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}},
							Parents: []CommitDef{{Message: "no-revision"}},
						},
						revision:          int64(3),
						matchErr:          MatchError(rpctypes.ErrGRPCFutureRev),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() check {
					return check{
						spec: "^1-compacted",
						cd: &CommitDef{
							Message: "2",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}},
							Parents: []CommitDef{{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}},
						},
						revision:          int64(0),
						matchErr:          MatchError(rpctypes.ErrGRPCCompacted),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() check {
					var revCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}

					return check{
						spec: "^1",
						cd: &CommitDef{
							Message: "2",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}},
							Parents: []CommitDef{*revCD},
						},
						revision:          int64(1),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(revCD)),
					}
				}(),
				func() check {
					var revCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}

					return check{
						spec: "^1-no-revision",
						cd: &CommitDef{
							Message: "3",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
							Parents: []CommitDef{
								*revCD,
								{Message: "2"},
							},
						},
						revision:          int64(1),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(revCD)),
					}
				}(),
				func() check {
					return check{
						spec: "no-revision^1",
						cd: &CommitDef{
							Message: "2",
							Parents: []CommitDef{{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}},
						},
						revision:          int64(1),
						matchErr:          And(HaveOccurred(), Not(MatchError(rpctypes.ErrGRPCFutureRev)), Not(MatchError(rpctypes.ErrGRPCCompacted))),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() check {
					var revCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}

					return check{
						spec: "^2",
						cd: &CommitDef{
							Message: "3",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
							Parents: []CommitDef{
								{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}}},
								*revCD,
							},
						},
						revision:          int64(1),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(revCD)),
					}
				}(),
				func() check {
					var revCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}

					return check{
						spec: "^1^1",
						cd: &CommitDef{
							Message: "3",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
							Parents: []CommitDef{
								{
									Message: "2",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}},
									Parents: []CommitDef{*revCD},
								},
							},
						},
						revision:          int64(1),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(revCD)),
					}
				}(),
				func() check {
					var revCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}

					return check{
						spec: "^2^1",
						cd: &CommitDef{
							Message: "4",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("4")}},
							Parents: []CommitDef{
								{Message: "3", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}}},
								{
									Message: "2",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}},
									Parents: []CommitDef{
										*revCD,
										{Message: "no-revision"},
									},
								},
							},
						},
						revision:          int64(1),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(revCD)),
					}
				}(),
				func() check {
					return check{
						spec: "^2-string-revsion-^2",
						cd: &CommitDef{
							Message: "5",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("5")}},
							Parents: []CommitDef{
								{Message: "4", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("4")}}},
								{
									Message: "3",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}},
									Parents: []CommitDef{
										{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}}},
										{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}},
									},
								},
							},
						},
						revision:          int64(1),
						matchErr:          And(HaveOccurred(), Not(MatchError(rpctypes.ErrGRPCFutureRev)), Not(MatchError(rpctypes.ErrGRPCCompacted))),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() check {
					var revCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}

					return check{
						spec: "^2^2",
						cd: &CommitDef{
							Message: "5",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("5")}},
							Parents: []CommitDef{
								{Message: "4", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("4")}}},
								{
									Message: "3",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
									Parents: []CommitDef{
										{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}}},
										*revCD,
									},
								},
							},
						},
						revision:          int64(1),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(revCD)),
					}
				}(),
				func() check {
					return check{
						spec: "^2^2-compacted",
						cd: &CommitDef{
							Message: "5",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("5")}},
							Parents: []CommitDef{
								{Message: "4", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("4")}}},
								{
									Message: "3",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
									Parents: []CommitDef{
										{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}}},
										{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}},
									},
								},
							},
						},
						revision:          int64(0),
						matchErr:          MatchError(rpctypes.ErrGRPCCompacted),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() check {
					return check{
						spec: "^2^2-missing",
						cd: &CommitDef{
							Message: "5",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("5")}},
							Parents: []CommitDef{
								{
									Message: "0",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}},
									Parents: []CommitDef{{Message: "no-revision"}},
								},
								{
									Message: "3",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("3")}},
									Parents: []CommitDef{
										{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("2")}}},
										{Message: "-1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("-1")}}},
									},
								},
							},
						},
						revision:          int64(1),
						matchErr:          MatchError(rpctypes.ErrGRPCCompacted),
						matchCommitDefPtr: BeNil(),
					}
				}(),
				func() check {
					var revCD = &CommitDef{Message: "100", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("100")}}}

					return check{
						spec: "^1^1^1",
						cd: &CommitDef{
							Message: "400",
							Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("400")}},
							Parents: []CommitDef{
								{
									Message: "300",
									Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("300")}},
									Parents: []CommitDef{
										{
											Message: "200",
											Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("200")}},
											Parents: []CommitDef{*revCD},
										},
									},
								},
							},
						},
						revision:          int64(100),
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(revCD)),
					}
				}(),
			} {
				func(s check) {
					It(s.spec, func() {
						var (
							p   git.Peelable
							err error
						)

						Expect(func() (err error) { c, err = CreateAndLoadCommitFromDef(ctx, b.repo, s.cd); return }()).To(Succeed())
						Expect(c).ToNot(BeNil())

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						if p, err = b.getMetadataPeelableForRevision(ctx, c, s.revision); p != nil {
							defer p.Close()
						}

						Expect(err).To(s.matchErr)
						Expect(func() (cd *CommitDef, err error) {
							var c git.Commit

							if p == nil {
								return
							}

							if c, err = b.repo.Peeler().PeelToCommit(ctx, p); err != nil {
								return
							}

							cd = GetCommitDefByCommit(ctx, b.repo, c)
							return
						}()).To(s.matchCommitDefPtr)
					})
				}(s)
			}
		})

		Describe("getDataCommitForMetadata", func() {
			type check struct {
				spec                        string
				ctxFn                       ContextFunc
				td                          *TreeDef
				dataIDFn                    func() (git.ObjectID, error)
				matchErr, matchCommitDefPtr types.GomegaMatcher
			}

			for _, s := range []check{
				{
					spec:              "expired context",
					ctxFn:             CanceledContext,
					td:                &TreeDef{},
					matchErr:          MatchError("context canceled"),
					matchCommitDefPtr: BeNil(),
				},
				{spec: "no data", td: &TreeDef{}, matchErr: HaveOccurred(), matchCommitDefPtr: BeNil()},
				{
					spec:              "tree data",
					td:                &TreeDef{Subtrees: map[string]TreeDef{metadataPathData: {}}},
					matchErr:          HaveOccurred(),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "int data",
					td:                &TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("1")}},
					matchErr:          HaveOccurred(),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "string data",
					td:                &TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("abcdefghijklmnopqrstuvwxyz1234567890ABCD")}},
					matchErr:          HaveOccurred(),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec:              "non-existing ID as data",
					td:                &TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("1111111111111111111111111111111111111111")}},
					matchErr:          HaveOccurred(),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec: "blob data",
					td:   &TreeDef{},
					dataIDFn: func() (id git.ObjectID, err error) {
						id, err = CreateBlob(ctx, b.repo, []byte{})
						return
					},
					matchErr:          HaveOccurred(),
					matchCommitDefPtr: BeNil(),
				},
				{
					spec: "tree data",
					td:   &TreeDef{},
					dataIDFn: func() (id git.ObjectID, err error) {
						id, err = CreateTreeFromDef(ctx, b.repo, &TreeDef{})
						return
					},
					matchErr:          HaveOccurred(),
					matchCommitDefPtr: BeNil(),
				},
				func() check {
					var cd = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					return check{
						spec: "commit data",
						td:   &TreeDef{},
						dataIDFn: func() (id git.ObjectID, err error) {
							id, err = CreateCommitFromDef(ctx, b.repo, cd)
							return
						},
						matchErr:          Succeed(),
						matchCommitDefPtr: PointTo(GetCommitDefMatcher(cd)),
					}
				}(),
			} {
				func(s check) {
					var metaRoot git.Tree

					Describe(s.spec, func() {
						BeforeEach(func() {
							Expect(func() (err error) {
								metaRoot, err = CreateAndLoadTreeFromDef(ctx, b.repo, s.td)
								return
							}()).To(Succeed())
							Expect(metaRoot).ToNot(BeNil())

							if s.dataIDFn != nil {
								Expect(func() (err error) {
									var (
										tb git.TreeBuilder
										id git.ObjectID
									)

									if tb, err = b.repo.TreeBuilderFromTree(ctx, metaRoot); err != nil {
										return
									}

									defer tb.Close()

									if id, err = s.dataIDFn(); err != nil {
										return
									}

									if id, err = CreateBlob(ctx, b.repo, []byte(id.String())); err != nil {
										return
									}

									if err = tb.AddEntry(metadataPathData, id, git.FilemodeBlob); err != nil {
										return
									}

									if id, err = tb.Build(ctx); err != nil {
										return
									}

									Expect(metaRoot.Close()).To(Succeed())

									metaRoot, err = b.repo.ObjectGetter().GetTree(ctx, id)
									return
								}()).To(Succeed())
								Expect(metaRoot).ToNot(BeNil())
							}

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
							var (
								c   git.Commit
								err error
							)

							if c, err = b.getDataCommitForMetadata(ctx, metaRoot); c != nil {
								defer c.Close()
							}

							Expect(err).To(s.matchErr)
							Expect(func() (cd *CommitDef, err error) {
								if c == nil {
									return
								}

								cd = GetCommitDefByCommit(ctx, b.repo, c)
								return
							}()).To(s.matchCommitDefPtr)
						})
					})
				}(s)
			}
		})

		Describe("newResponseHeaderFromMetaTree", func() {
			type check struct {
				clusterID, memberID uint64
				ctxFn               ContextFunc
				td                  *TreeDef
				responseHeader      *etcdserverpb.ResponseHeader
			}

			for _, s := range []check{
				{responseHeader: &etcdserverpb.ResponseHeader{}},
				{td: &TreeDef{}, responseHeader: &etcdserverpb.ResponseHeader{}},
				{td: &TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}, responseHeader: &etcdserverpb.ResponseHeader{}},
				{td: &TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}, responseHeader: &etcdserverpb.ResponseHeader{}},
				{
					td:             &TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
					responseHeader: &etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
				},
				{
					ctxFn:          CanceledContext,
					td:             &TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{clusterID: 10, memberID: 2, responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{clusterID: 10, memberID: 2, td: &TreeDef{}, responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{
					clusterID:      10,
					memberID:       2,
					td:             &TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}},
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					clusterID:      10,
					memberID:       2,
					td:             &TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}},
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					clusterID:      10,
					memberID:       2,
					td:             &TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: 1, RaftTerm: 1},
				},
				{
					clusterID:      10,
					memberID:       2,
					ctxFn:          CanceledContext,
					td:             &TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
			} {
				func(s check) {
					var itSpec = fmt.Sprintf("clusterID=%d, memberID=%d, td=%v", s.clusterID, s.memberID, s.td)

					if s.ctxFn != nil {
						itSpec = "expired context"
					}

					It(itSpec, func() {
						var metaRoot git.Tree

						b.clusterID = s.clusterID
						b.memberID = s.memberID

						if s.td != nil {
							Expect(func() (err error) { metaRoot, err = CreateAndLoadTreeFromDef(ctx, b.repo, s.td); return }()).To(Succeed())
							Expect(metaRoot).ToNot((BeNil()))
						}

						if metaRoot != nil {
							defer metaRoot.Close()
						}

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						Expect(b.newResponseHeaderFromMetaTree(ctx, metaRoot)).To(Equal(s.responseHeader))
					})
				}(s)
			}
		})

		Describe("newResponseHeaderFromMetaPeelable", func() {
			type peelableFunc func() (git.Peelable, error)

			type check struct {
				spec                string
				ctxFn               ContextFunc
				clusterID, memberID uint64
				peelableFn          peelableFunc
				responseHeader      *etcdserverpb.ResponseHeader
			}

			var (
				blobPeelable = func(content []byte) peelableFunc {
					return func() (p git.Peelable, err error) { p, err = CreateAndLoadBlob(ctx, b.repo, content); return }
				}

				treePeelable = func(td *TreeDef) peelableFunc {
					return func() (p git.Peelable, err error) { p, err = CreateAndLoadTreeFromDef(ctx, b.repo, td); return }
				}

				commitPeelable = func(cd *CommitDef) peelableFunc {
					return func() (p git.Peelable, err error) { p, err = CreateAndLoadCommitFromDef(ctx, b.repo, cd); return }
				}

				referencePeelable = func(cd *CommitDef) peelableFunc {
					return func() (git.Peelable, error) {
						return CreateAndLoadReferenceFromDef(ctx, b.repo, DefaultDataReferenceName, cd)
					}
				}
			)

			for _, s := range []check{
				{spec: "nil", responseHeader: &etcdserverpb.ResponseHeader{}},
				{spec: "nil", clusterID: 10, memberID: 2, responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{spec: "blob empty", peelableFn: blobPeelable([]byte{}), responseHeader: &etcdserverpb.ResponseHeader{}},
				{spec: "blob string", peelableFn: blobPeelable([]byte("a")), responseHeader: &etcdserverpb.ResponseHeader{}},
				{spec: "blob int", peelableFn: blobPeelable([]byte("1")), responseHeader: &etcdserverpb.ResponseHeader{}},
				{spec: "blob with expired context", ctxFn: CanceledContext, peelableFn: blobPeelable([]byte("1")), responseHeader: &etcdserverpb.ResponseHeader{}},
				{spec: "blob empty", clusterID: 10, memberID: 2, peelableFn: blobPeelable([]byte{}), responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{spec: "blob string", clusterID: 10, memberID: 2, peelableFn: blobPeelable([]byte("a")), responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{spec: "blob int", clusterID: 10, memberID: 2, peelableFn: blobPeelable([]byte("1")), responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{spec: "blob with expired context", ctxFn: CanceledContext, clusterID: 10, memberID: 2, peelableFn: blobPeelable([]byte("1")), responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{spec: "tree - empty", peelableFn: treePeelable(&TreeDef{}), responseHeader: &etcdserverpb.ResponseHeader{}},
				{
					spec:           "tree with tree revision",
					peelableFn:     treePeelable(&TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "tree with string revision",
					peelableFn:     treePeelable(&TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "tree with int revision",
					peelableFn:     treePeelable(&TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}),
					responseHeader: &etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
				},
				{
					spec:           "tree with expired context",
					ctxFn:          CanceledContext,
					peelableFn:     treePeelable(&TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{spec: "tree - empty", clusterID: 10, memberID: 2, peelableFn: treePeelable(&TreeDef{}), responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
				{
					spec:           "tree with tree revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     treePeelable(&TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "tree with string revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     treePeelable(&TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "tree with int revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     treePeelable(&TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: 1, RaftTerm: 1},
				},
				{
					spec:           "tree with expired context",
					ctxFn:          CanceledContext,
					clusterID:      10,
					memberID:       2,
					peelableFn:     treePeelable(&TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{spec: "commit - empty", peelableFn: commitPeelable(&CommitDef{}), responseHeader: &etcdserverpb.ResponseHeader{}},
				{
					spec:           "commit with tree revision",
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "commit with string revision",
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "commit with int revision",
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
				},
				{
					spec:           "commit with expired context",
					ctxFn:          CanceledContext,
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "commit - empty",
					clusterID:      10,
					memberID:       2,
					peelableFn:     commitPeelable(&CommitDef{}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "commit with tree revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "commit with string revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "commit with int revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: 1, RaftTerm: 1},
				},
				{
					spec:           "commit with expired context",
					ctxFn:          CanceledContext,
					clusterID:      10,
					memberID:       2,
					peelableFn:     commitPeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{spec: "reference - empty", peelableFn: referencePeelable(&CommitDef{}), responseHeader: &etcdserverpb.ResponseHeader{}},
				{
					spec:           "reference with tree revision",
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "reference with string revision",
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "reference with int revision",
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
				},
				{
					spec:           "reference with expired context",
					ctxFn:          CanceledContext,
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{},
				},
				{
					spec:           "reference - empty",
					clusterID:      10,
					memberID:       2,
					peelableFn:     referencePeelable(&CommitDef{}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "reference with tree revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "reference with string revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:           "reference with int revision",
					clusterID:      10,
					memberID:       2,
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: 1, RaftTerm: 1},
				},
				{
					spec:           "reference with expired context",
					ctxFn:          CanceledContext,
					clusterID:      10,
					memberID:       2,
					peelableFn:     referencePeelable(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
			} {
				func(s check) {
					It(fmt.Sprintf("%s clusterID=%d, memberID=%d", s.spec, s.clusterID, s.memberID), func() {
						var metaP git.Peelable

						b.clusterID = s.clusterID
						b.memberID = s.memberID

						if s.peelableFn != nil {
							Expect(func() (err error) { metaP, err = s.peelableFn(); return }()).To(Succeed())
							Expect(metaP).ToNot((BeNil()))
						}

						if metaP != nil {
							defer metaP.Close()
						}

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						Expect(b.newResponseHeaderFromMetaPeelable(ctx, metaP)).To(Equal(s.responseHeader))
					})
				}(s)
			}
		})

		Describe("newResponseHeader", func() {
			type createReferenceFunc func(git.ReferenceName) error

			type check struct {
				spec                string
				ctxFn               ContextFunc
				clusterID, memberID uint64
				createReferenceFn   createReferenceFunc
				responseHeader      *etcdserverpb.ResponseHeader
			}

			var (
				noReference     = func(_ git.ReferenceName) error { return nil }
				createReference = func(cd *CommitDef) createReferenceFunc {
					return func(refName git.ReferenceName) error {
						return CreateReferenceFromDef(ctx, b.repo, refName, cd)
					}
				}
			)

			for _, s := range []check{
				{spec: "no reference", createReferenceFn: noReference, responseHeader: &etcdserverpb.ResponseHeader{}},
				{
					spec:              "no reference with expired context",
					ctxFn:             CanceledContext,
					createReferenceFn: noReference,
					responseHeader:    &etcdserverpb.ResponseHeader{},
				},
				{spec: "reference - empty", createReferenceFn: createReference(&CommitDef{}), responseHeader: &etcdserverpb.ResponseHeader{}},
				{
					spec:              "reference with tree revision",
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{},
				},
				{
					spec:              "reference with string revision",
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{},
				},
				{
					spec:              "reference with int revision",
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
				},
				{
					spec:              "reference with expired contet",
					ctxFn:             CanceledContext,
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{},
				},
				{
					spec:              "no reference",
					clusterID:         10,
					memberID:          2,
					createReferenceFn: noReference,
					responseHeader:    &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:              "no reference with expired context",
					ctxFn:             CanceledContext,
					clusterID:         10,
					memberID:          2,
					createReferenceFn: noReference,
					responseHeader:    &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:              "reference - empty",
					clusterID:         10,
					memberID:          2,
					createReferenceFn: createReference(&CommitDef{}),
					responseHeader:    &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:              "reference with tree revision",
					clusterID:         10,
					memberID:          2,
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Subtrees: map[string]TreeDef{metadataPathRevision: {}}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:              "reference with string revision",
					clusterID:         10,
					memberID:          2,
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("a")}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
				{
					spec:              "reference with int revision",
					clusterID:         10,
					memberID:          2,
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: 1, RaftTerm: 1},
				},
				{
					spec:              "reference with expired contet",
					clusterID:         10,
					memberID:          2,
					ctxFn:             CanceledContext,
					createReferenceFn: createReference(&CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}}),
					responseHeader:    &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2},
				},
			} {
				func(s check) {
					Describe(fmt.Sprintf("%s clusterID=%d memberID=%d", s.spec, s.clusterID, s.memberID), func() {
						var metaRefName git.ReferenceName

						BeforeEach(func() {
							b.refName = DefaultDataReferenceName
							b.metadataRefName = DefaultMetadataReferenceName
							b.clusterID = s.clusterID
							b.memberID = s.memberID

							Expect(func() (err error) { metaRefName, err = b.getMetadataRefName(); return }()).To(Succeed())
							Expect(metaRefName).ToNot(BeEmpty())
						})

						It("should succeed", func() {
							if s.createReferenceFn != nil {
								Expect(s.createReferenceFn(metaRefName)).To(Succeed())
							}

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}

							Expect(b.newResponseHeader(ctx)).To(Equal(s.responseHeader))
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

			Describe("replaceCurrentCommit", func() {
				type expectCommitIDFunc func(git.ObjectID)

				type check struct {
					spec           string
					ctxFn          ContextFunc
					message        string
					td             *TreeDef
					cd             *CommitDef
					matchErr       types.GomegaMatcher
					expectCommitID expectCommitIDFunc
				}

				var (
					emptyIDFn   = func(id git.ObjectID) { Expect(id).To(Equal(git.ObjectID{})) }
					commitDefFn = func(cd *CommitDef) expectCommitIDFunc {
						return func(id git.ObjectID) {
							Expect(*GetCommitDefByID(ctx, b.repo, id)).To(GetCommitDefMatcher(cd))
						}
					}
				)

				for _, s := range []check{
					{
						spec:           "expired context",
						ctxFn:          CanceledContext,
						td:             &TreeDef{},
						matchErr:       MatchError("context canceled"),
						expectCommitID: emptyIDFn,
					},
					{
						spec:           "new empty tree",
						message:        "new",
						td:             &TreeDef{},
						matchErr:       Succeed(),
						expectCommitID: commitDefFn(&CommitDef{Message: "new"}),
					},
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"new": []byte("new")}}

						return check{
							spec:           "new non-empty tree",
							message:        "new",
							td:             td,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Tree: *td}),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"new": []byte("new")}}

						return check{
							spec:           "new non-empty tree replacing old empty tree",
							message:        "new",
							td:             td,
							cd:             &CommitDef{Message: "old"},
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Tree: *td}),
						}
					}(),
					{
						spec:           "new empty tree replace old non-empty tree",
						message:        "new",
						td:             &TreeDef{},
						cd:             &CommitDef{Message: "old", Tree: TreeDef{Blobs: map[string][]byte{"old": []byte("old")}}},
						matchErr:       Succeed(),
						expectCommitID: commitDefFn(&CommitDef{Message: "new"}),
					},
					func() check {
						var (
							td = &TreeDef{Blobs: map[string][]byte{"new": []byte("new")}}
							cd = &CommitDef{
								Message: "old",
								Parents: []CommitDef{
									{
										Message: "^1",
										Tree:    TreeDef{Blobs: map[string][]byte{"^1": []byte("^1")}},
										Parents: []CommitDef{
											{
												Message: "^1^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^1": []byte("^1^1")}},
											},
											{
												Message: "^1^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^2": []byte("^1^2")}},
											},
										},
									},
									{
										Message: "^2",
										Tree:    TreeDef{Blobs: map[string][]byte{"^2": []byte("^2")}},
										Parents: []CommitDef{
											{
												Message: "^2^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^1": []byte("^2^1")}},
											},
											{
												Message: "^2^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^2": []byte("^2^2")}},
											},
										},
									},
								},
							}
						)

						return check{
							spec:           "new non-empty tree replacing old empty tree with parents",
							message:        "new",
							td:             td,
							cd:             cd,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Tree: *td, Parents: cd.Parents}),
						}
					}(),
					func() check {
						var (
							cd = &CommitDef{
								Message: "old",
								Tree:    TreeDef{Blobs: map[string][]byte{"old": []byte("old")}},
								Parents: []CommitDef{
									{
										Message: "^1",
										Tree:    TreeDef{Blobs: map[string][]byte{"^1": []byte("^1")}},
										Parents: []CommitDef{
											{
												Message: "^1^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^1": []byte("^1^1")}},
											},
											{
												Message: "^1^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^2": []byte("^1^2")}},
											},
										},
									},
									{
										Message: "^2",
										Tree:    TreeDef{Blobs: map[string][]byte{"^2": []byte("^2")}},
										Parents: []CommitDef{
											{
												Message: "^2^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^1": []byte("^2^1")}},
											},
											{
												Message: "^2^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^2": []byte("^2^2")}},
											},
										},
									},
								},
							}
						)

						return check{
							spec:           "new empty tree replace old non-empty tree with parents",
							message:        "new",
							td:             &TreeDef{},
							cd:             cd,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Parents: cd.Parents}),
						}
					}(),
				} {
					func(s check) {
						Describe(s.spec, func() {
							var (
								tID git.ObjectID
								c   git.Commit
							)

							BeforeEach(func() {
								Expect(func() (err error) { tID, err = CreateTreeFromDef(ctx, b.repo, s.td); return }()).To(Succeed())

								if s.cd != nil {
									Expect(func() (err error) { c, err = CreateAndLoadCommitFromDef(ctx, b.repo, s.cd); return }()).To(Succeed())
								}

								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							AfterEach(func() {
								if c != nil {
									Expect(c.Close()).To(Succeed())
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var id, err = b.replaceCurrentCommit(ctx, s.message, tID, c)
								Expect(err).To(s.matchErr)
								s.expectCommitID(id)
							})
						})
					}(s)
				}
			})

			Describe("inheritCurrentCommit", func() {
				type expectCommitIDFunc func(git.ObjectID)

				type check struct {
					spec           string
					ctxFn          ContextFunc
					message        string
					td             *TreeDef
					cd             *CommitDef
					matchErr       types.GomegaMatcher
					expectCommitID expectCommitIDFunc
				}

				var (
					emptyIDFn   = func(id git.ObjectID) { Expect(id).To(Equal(git.ObjectID{})) }
					commitDefFn = func(cd *CommitDef) expectCommitIDFunc {
						return func(id git.ObjectID) {
							Expect(*GetCommitDefByID(ctx, b.repo, id)).To(GetCommitDefMatcher(cd))
						}
					}
				)

				for _, s := range []check{
					{
						spec:           "expired context",
						ctxFn:          CanceledContext,
						td:             &TreeDef{},
						matchErr:       MatchError("context canceled"),
						expectCommitID: emptyIDFn,
					},
					{
						spec:           "new empty tree",
						message:        "new",
						td:             &TreeDef{},
						matchErr:       Succeed(),
						expectCommitID: commitDefFn(&CommitDef{Message: "new"}),
					},
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"new": []byte("new")}}

						return check{
							spec:           "new non-empty tree",
							message:        "new",
							td:             td,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Tree: *td}),
						}
					}(),
					func() check {
						var (
							td = &TreeDef{Blobs: map[string][]byte{"new": []byte("new")}}
							cd = &CommitDef{Message: "old"}
						)

						return check{
							spec:           "new non-empty tree replacing old empty tree",
							message:        "new",
							td:             td,
							cd:             cd,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Tree: *td, Parents: []CommitDef{*cd}}),
						}
					}(),
					func() check {
						var cd = &CommitDef{Message: "old", Tree: TreeDef{Blobs: map[string][]byte{"old": []byte("old")}}}

						return check{
							spec:           "new empty tree replace old non-empty tree",
							message:        "new",
							td:             &TreeDef{},
							cd:             cd,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Parents: []CommitDef{*cd}}),
						}
					}(),
					func() check {
						var (
							td = &TreeDef{Blobs: map[string][]byte{"new": []byte("new")}}
							cd = &CommitDef{
								Message: "old",
								Parents: []CommitDef{
									{
										Message: "^1",
										Tree:    TreeDef{Blobs: map[string][]byte{"^1": []byte("^1")}},
										Parents: []CommitDef{
											{
												Message: "^1^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^1": []byte("^1^1")}},
											},
											{
												Message: "^1^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^2": []byte("^1^2")}},
											},
										},
									},
									{
										Message: "^2",
										Tree:    TreeDef{Blobs: map[string][]byte{"^2": []byte("^2")}},
										Parents: []CommitDef{
											{
												Message: "^2^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^1": []byte("^2^1")}},
											},
											{
												Message: "^2^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^2": []byte("^2^2")}},
											},
										},
									},
								},
							}
						)

						return check{
							spec:           "new non-empty tree replacing old empty tree with parents",
							message:        "new",
							td:             td,
							cd:             cd,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Tree: *td, Parents: []CommitDef{*cd}}),
						}
					}(),
					func() check {
						var (
							cd = &CommitDef{
								Message: "old",
								Tree:    TreeDef{Blobs: map[string][]byte{"old": []byte("old")}},
								Parents: []CommitDef{
									{
										Message: "^1",
										Tree:    TreeDef{Blobs: map[string][]byte{"^1": []byte("^1")}},
										Parents: []CommitDef{
											{
												Message: "^1^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^1": []byte("^1^1")}},
											},
											{
												Message: "^1^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^1^2": []byte("^1^2")}},
											},
										},
									},
									{
										Message: "^2",
										Tree:    TreeDef{Blobs: map[string][]byte{"^2": []byte("^2")}},
										Parents: []CommitDef{
											{
												Message: "^2^1",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^1": []byte("^2^1")}},
											},
											{
												Message: "^2^2",
												Tree:    TreeDef{Blobs: map[string][]byte{"^2^2": []byte("^2^2")}},
											},
										},
									},
								},
							}
						)

						return check{
							spec:           "new empty tree replace old non-empty tree with parents",
							message:        "new",
							td:             &TreeDef{},
							cd:             cd,
							matchErr:       Succeed(),
							expectCommitID: commitDefFn(&CommitDef{Message: "new", Parents: []CommitDef{*cd}}),
						}
					}(),
				} {
					func(s check) {
						Describe(s.spec, func() {
							var (
								tID git.ObjectID
								c   git.Commit
							)

							BeforeEach(func() {
								Expect(func() (err error) { tID, err = CreateTreeFromDef(ctx, b.repo, s.td); return }()).To(Succeed())

								if s.cd != nil {
									Expect(func() (err error) { c, err = CreateAndLoadCommitFromDef(ctx, b.repo, s.cd); return }()).To(Succeed())
								}

								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							AfterEach(func() {
								if c != nil {
									Expect(c.Close()).To(Succeed())
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var id, err = b.inheritCurrentCommit(ctx, s.message, tID, c)
								Expect(err).To(s.matchErr)
								s.expectCommitID(id)
							})
						})
					}(s)
				}
			})

			Describe("advanceReferences", func() {
				type check struct {
					spec                                                           string
					ctxFn                                                          ContextFunc
					refName, metaRefName                                           git.ReferenceName
					currentMetaHead, currentDataHead                               *CommitDef
					metaMutated, dataMutated                                       bool
					newMetaHead, newDataHead                                       *CommitDef
					revision                                                       int64
					matchErr, matchMetaHeadCommitDefPtr, matchDataHeadCommitDefPtr types.GomegaMatcher
				}

				for refName, metaRefName := range map[git.ReferenceName]git.ReferenceName{
					DefaultDataReferenceName: DefaultMetadataReferenceName,
					"refs/heads/custom":      "refs/meta/custom",
				} {
					for _, s := range []check{
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							refName:                   refName,
							metaRefName:               metaRefName,
							revision:                  1,
							matchErr:                  Succeed(),
							matchMetaHeadCommitDefPtr: BeNil(),
							matchDataHeadCommitDefPtr: BeNil(),
						},
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							refName:                   refName,
							metaRefName:               metaRefName,
							metaMutated:               true,
							newMetaHead:               &CommitDef{},
							revision:                  1,
							matchErr:                  MatchError("context canceled"),
							matchMetaHeadCommitDefPtr: BeNil(),
							matchDataHeadCommitDefPtr: BeNil(),
						},
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							refName:                   refName,
							metaRefName:               metaRefName,
							dataMutated:               true,
							newDataHead:               &CommitDef{},
							revision:                  1,
							matchErr:                  MatchError("context canceled"),
							matchMetaHeadCommitDefPtr: BeNil(),
							matchDataHeadCommitDefPtr: BeNil(),
						},
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							refName:                   refName,
							metaRefName:               metaRefName,
							metaMutated:               true,
							newMetaHead:               &CommitDef{},
							dataMutated:               true,
							newDataHead:               &CommitDef{},
							revision:                  1,
							matchErr:                  MatchError("context canceled"),
							matchMetaHeadCommitDefPtr: BeNil(),
							matchDataHeadCommitDefPtr: BeNil(),
						},
						{
							refName:                   refName,
							metaRefName:               metaRefName,
							revision:                  1,
							matchErr:                  Succeed(),
							matchMetaHeadCommitDefPtr: BeNil(),
							matchDataHeadCommitDefPtr: BeNil(),
						},
						func() check {
							var cd = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								metaMutated:               true,
								newMetaHead:               cd,
								revision:                  1,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cd)),
								matchDataHeadCommitDefPtr: BeNil(),
							}
						}(),
						func() check {
							var cd = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								dataMutated:               true,
								newDataHead:               cd,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: BeNil(),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cd)),
							}
						}(),
						func() check {
							var (
								metaCD = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								dataCD = &CommitDef{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
							)

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								metaMutated:               true,
								newMetaHead:               metaCD,
								dataMutated:               true,
								newDataHead:               dataCD,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(metaCD)),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(dataCD)),
							}
						}(),
						func() check {
							var (
								currentMetaHead = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								currentDataHead = &CommitDef{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
							)

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								currentMetaHead:           currentMetaHead,
								currentDataHead:           currentDataHead,
								revision:                  1,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(currentMetaHead)),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(currentDataHead)),
							}
						}(),
						func() check {
							var (
								currentMetaHead = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								newMetaHead     = &CommitDef{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
							)

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								currentMetaHead:           currentMetaHead,
								metaMutated:               true,
								newMetaHead:               newMetaHead,
								revision:                  1,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newMetaHead)),
								matchDataHeadCommitDefPtr: BeNil(),
							}
						}(),
						func() check {
							var (
								currentMetaHead = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								currentDataHead = &CommitDef{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
								newMetaHead     = &CommitDef{Message: "3", Tree: TreeDef{Blobs: map[string][]byte{"3": []byte("3")}}}
							)

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								currentMetaHead:           currentMetaHead,
								currentDataHead:           currentDataHead,
								metaMutated:               true,
								newMetaHead:               newMetaHead,
								revision:                  1,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newMetaHead)),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(currentDataHead)),
							}
						}(),
						func() check {
							var (
								currentDataHead = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								newDataHead     = &CommitDef{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
							)

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								currentDataHead:           currentDataHead,
								dataMutated:               true,
								newDataHead:               newDataHead,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: BeNil(),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
							}
						}(),
						func() check {
							var (
								currentMetaHead = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								currentDataHead = &CommitDef{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
								newDataHead     = &CommitDef{Message: "3", Tree: TreeDef{Blobs: map[string][]byte{"3": []byte("3")}}}
							)

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								currentMetaHead:           currentMetaHead,
								currentDataHead:           currentDataHead,
								dataMutated:               true,
								newDataHead:               newDataHead,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(currentMetaHead)),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
							}
						}(),
						func() check {
							var (
								currentMetaHead = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
								newMetaHead     = &CommitDef{Message: "2", Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
								currentDataHead = &CommitDef{Message: "3", Tree: TreeDef{Blobs: map[string][]byte{"3": []byte("3")}}}
								newDataHead     = &CommitDef{Message: "4", Tree: TreeDef{Blobs: map[string][]byte{"4": []byte("4")}}}
							)

							return check{
								refName:                   refName,
								metaRefName:               metaRefName,
								currentMetaHead:           currentMetaHead,
								currentDataHead:           currentDataHead,
								metaMutated:               true,
								newMetaHead:               newMetaHead,
								dataMutated:               true,
								newDataHead:               newDataHead,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newMetaHead)),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
							}
						}(),
						func() check {
							var (
								currentMetaHead = &CommitDef{
									Message: "1",
									Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
									Parents: []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}},
								}
								newMetaHead = &CommitDef{
									Message: "2",
									Tree:    TreeDef{Blobs: map[string][]byte{"2": []byte("2")}},
									Parents: []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}},
								}
								currentDataHead = &CommitDef{
									Message: "4",
									Tree:    TreeDef{Blobs: map[string][]byte{"4": []byte("4")}},
									Parents: []CommitDef{{Message: "3", Tree: TreeDef{Blobs: map[string][]byte{"3": []byte("3")}}}},
								}
								newDataHead = &CommitDef{
									Message: "5",
									Tree:    TreeDef{Blobs: map[string][]byte{"5": []byte("5")}},
									Parents: []CommitDef{{Message: "3", Tree: TreeDef{Blobs: map[string][]byte{"3": []byte("3")}}}},
								}
							)

							return check{
								spec:                      "replace current commits",
								refName:                   refName,
								metaRefName:               metaRefName,
								currentMetaHead:           currentMetaHead,
								currentDataHead:           currentDataHead,
								metaMutated:               true,
								newMetaHead:               newMetaHead,
								dataMutated:               true,
								newDataHead:               newDataHead,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newMetaHead)),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
							}
						}(),
						func() check {
							var (
								currentMetaHead = &CommitDef{
									Message: "1",
									Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
									Parents: []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}},
								}
								newMetaHead = &CommitDef{
									Message: "2",
									Tree:    TreeDef{Blobs: map[string][]byte{"2": []byte("2")}},
									Parents: []CommitDef{*currentMetaHead},
								}
								currentDataHead = &CommitDef{
									Message: "4",
									Tree:    TreeDef{Blobs: map[string][]byte{"4": []byte("4")}},
									Parents: []CommitDef{{Message: "3", Tree: TreeDef{Blobs: map[string][]byte{"3": []byte("3")}}}},
								}
								newDataHead = &CommitDef{
									Message: "5",
									Tree:    TreeDef{Blobs: map[string][]byte{"5": []byte("5")}},
									Parents: []CommitDef{*currentDataHead},
								}
							)

							return check{
								spec:                      "inherit current commits",
								refName:                   refName,
								metaRefName:               metaRefName,
								currentMetaHead:           currentMetaHead,
								currentDataHead:           currentDataHead,
								metaMutated:               true,
								newMetaHead:               newMetaHead,
								dataMutated:               true,
								newDataHead:               newDataHead,
								matchErr:                  Succeed(),
								matchMetaHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newMetaHead)),
								matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
							}
						}(),
					} {
						func(s check) {
							Describe(
								fmt.Sprintf(
									"%s refName=%q metaRefName=%q hasCurrentMetaHead=%t, hasCurrentDataHead=%t metaMutated=%t dataMutated=%t, revision=%d",
									s.spec,
									s.refName,
									s.metaRefName,
									s.currentMetaHead != nil,
									s.currentDataHead != nil,
									s.metaMutated,
									s.dataMutated,
									s.revision,
								),
								func() {
									var (
										newMetaID, newDataID git.ObjectID
										parentCtx            context.Context
									)

									BeforeEach(func() {
										b.refName, b.metadataRefName = s.refName, s.metaRefName

										for _, s := range []struct {
											refNameFn func() (git.ReferenceName, error)
											cd        *CommitDef
										}{
											{refNameFn: b.getMetadataRefName, cd: s.currentMetaHead},
											{refNameFn: b.getDataRefName, cd: s.currentDataHead},
										} {
											if s.cd == nil {
												continue
											}

											Expect(func() (err error) {
												var refName git.ReferenceName

												if refName, err = s.refNameFn(); err != nil {
													return
												}

												err = CreateReferenceFromDef(ctx, b.repo, refName, s.cd)
												return
											}()).To(Succeed())
										}

										for _, s := range []struct {
											cd    *CommitDef
											idPtr *git.ObjectID
										}{
											{cd: s.newMetaHead, idPtr: &newMetaID},
											{cd: s.newDataHead, idPtr: &newDataID},
										} {
											if s.cd == nil {
												continue
											}

											Expect(func() (err error) {
												*s.idPtr, err = CreateCommitFromDef(ctx, b.repo, s.cd)
												return
											}()).To(Succeed())
										}

										parentCtx = ctx

										if s.ctxFn != nil {
											ctx = s.ctxFn(ctx)
										}
									})

									It(ItSpecForMatchError(s.matchErr), func() {
										Expect(
											b.advanceReferences(ctx, s.metaMutated, newMetaID, s.dataMutated, newDataID, s.revision),
										).To(s.matchErr)

										{
											var ctx = parentCtx

											for _, s := range []struct {
												refFn             func() (git.Reference, error)
												matchCommitDefPtr types.GomegaMatcher
											}{
												{
													refFn:             func() (git.Reference, error) { return b.getMetadataReference(ctx) },
													matchCommitDefPtr: s.matchMetaHeadCommitDefPtr,
												},
												{
													refFn: func() (ref git.Reference, err error) {
														var refName git.ReferenceName

														if refName, err = b.getDataRefName(); err != nil {
															return
														}

														ref, err = b.getReference(ctx, refName)
														return
													},
													matchCommitDefPtr: s.matchDataHeadCommitDefPtr,
												},
											} {
												Expect(func() (cd *CommitDef, err error) {
													var (
														ref git.Reference
														c   git.Commit
													)

													if ref, _ = s.refFn(); ref == nil {
														return
													}

													defer ref.Close()

													if c, err = b.repo.Peeler().PeelToCommit(ctx, ref); err != nil {
														return
													}

													defer c.Close()

													cd = GetCommitDefByCommit(ctx, b.repo, c)
													return
												}()).To(s.matchCommitDefPtr)
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

		Describe("createBlob", func() {
			type check struct {
				spec                   string
				content                []byte
				ctxFn                  ContextFunc
				matchErr, matchContent types.GomegaMatcher
			}

			var (
				createBytes = func(b byte, n int) (buf []byte) {
					buf = make([]byte, n)
					for i := 0; i < n; i++ {
						buf[i] = b
					}
					return
				}

				contentCheck = func(content []byte) check {
					return check{spec: fmt.Sprintf("content of length %d", len(content)), content: content, matchErr: Succeed(), matchContent: Equal(content)}
				}
			)

			for _, s := range []check{
				{spec: "expired context", content: nil, ctxFn: CanceledContext, matchErr: MatchError("context canceled"), matchContent: BeNil()},
				{spec: "nil content", content: nil, matchErr: Succeed(), matchContent: BeEmpty()},
				contentCheck([]byte("0123456789")),
				contentCheck(createBytes('a', 256)),
				contentCheck(createBytes('b', 1024)),
				contentCheck(createBytes('c', 1024*1024)),
			} {
				func(s check) {
					It(s.spec, func() {
						var (
							id  git.ObjectID
							err error
						)

						if s.ctxFn != nil {
							ctx = s.ctxFn(ctx)
						}

						id, err = b.createBlob(ctx, s.content)

						Expect(err).To(s.matchErr)

						if err == nil {
							Expect(func() (content []byte, err error) {
								var blob git.Blob

								if blob, err = b.repo.ObjectGetter().GetBlob(ctx, id); err != nil {
									return
								}

								defer blob.Close()

								content, err = blob.Content()
								return
							}()).To(s.matchContent)
						}
					})
				}(s)
			}
		})

		Describe("addOrReplaceTreeEntry", func() {
			type check struct {
				spec                                 string
				ctxFn                                ContextFunc
				td                                   *TreeDef
				entryName                            string
				newContent                           []byte
				te                                   git.TreeEntry
				matchErr, matchMutated, matchTreeDef types.GomegaMatcher
			}

			var (
				c         = gomock.NewController(GinkgoT())
				treeEntry = func() git.TreeEntry { return mockgit.NewMockTreeEntry(c) }
			)

			for _, s := range []check{
				{
					spec:         "new tree add entry with expired context",
					ctxFn:        CanceledContext,
					entryName:    "1",
					matchErr:     MatchError("context canceled"),
					matchMutated: BeFalse(),
					matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
				},
				{
					spec:         "new tree add entry",
					entryName:    "1",
					newContent:   []byte("1"),
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}),
				},
				{
					spec:         "new tree replace entry",
					entryName:    "1",
					newContent:   []byte("10"),
					te:           treeEntry(),
					matchErr:     HaveOccurred(),
					matchMutated: BeFalse(),
					matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
				},
				func() check {
					var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

					return check{
						spec:         "existing tree add entry with expired context",
						ctxFn:        CanceledContext,
						td:           td,
						entryName:    "1",
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(td),
					}
				}(),
				{
					spec:         "existing tree add new entry",
					td:           &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
					entryName:    "2",
					newContent:   []byte("2"),
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}),
				},
				{
					spec:         "existing tree replace entry",
					td:           &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
					entryName:    "1",
					newContent:   []byte("10"),
					te:           treeEntry(),
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("10")}}),
				},
				func() check {
					var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

					return check{
						spec:         "existing tree replace non-existent entry",
						td:           td,
						entryName:    "2",
						newContent:   []byte("2"),
						te:           treeEntry(),
						matchErr:     HaveOccurred(),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(td),
					}
				}(),
			} {
				func(s check) {
					Describe(s.spec, func() {
						var (
							tb        git.TreeBuilder
							parentCtx context.Context
						)

						BeforeEach(func() {
							if s.td != nil {
								Expect(func() (err error) {
									var t git.Tree

									if t, err = CreateAndLoadTreeFromDef(ctx, b.repo, s.td); err != nil {
										return
									}

									defer t.Close()

									tb, err = b.repo.TreeBuilderFromTree(ctx, t)
									return
								}()).To(Succeed())
							} else {
								Expect(func() (err error) { tb, err = b.repo.TreeBuilder(ctx); return }()).To(Succeed())
							}

							parentCtx = ctx

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}
						})

						AfterEach(func() {
							if tb != nil {
								Expect(tb.Close()).To(Succeed())
							}
						})

						It(ItSpecForMatchError(s.matchErr), func() {
							var mutated, err = b.addOrReplaceTreeEntry(ctx, tb, s.entryName, s.newContent, s.te)
							Expect(err).To(s.matchErr)
							Expect(mutated).To(s.matchMutated)
							Expect(func() (td TreeDef, err error) {
								var id git.ObjectID

								if id, err = tb.Build(parentCtx); err != nil {
									return
								}

								td = *GetTreeDef(parentCtx, b.repo, id)
								return
							}()).To(s.matchTreeDef)
						})
					})
				}(s)
			}
		})
	})

	Describe("newResponseHeaderWithRevision", func() {
		var toUnit64 = func(i int64) uint64 { return uint64(i) }

		for _, s := range []struct {
			clusterID, memberID uint64
			revision            int64
			responseHeader      *etcdserverpb.ResponseHeader
		}{
			{responseHeader: &etcdserverpb.ResponseHeader{}},
			{revision: 1, responseHeader: &etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1}},
			{revision: math.MinInt64, responseHeader: &etcdserverpb.ResponseHeader{Revision: math.MinInt64, RaftTerm: toUnit64(math.MinInt64)}}, // TODO ?
			{revision: math.MaxInt64, responseHeader: &etcdserverpb.ResponseHeader{Revision: math.MaxInt64, RaftTerm: toUnit64(math.MaxInt64)}},
			{clusterID: 10, memberID: 2, responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2}},
			{
				clusterID:      10,
				memberID:       2,
				revision:       1,
				responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: 1, RaftTerm: 1},
			},
			{
				clusterID:      10,
				memberID:       2,
				revision:       math.MinInt64,
				responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: math.MinInt64, RaftTerm: toUnit64(math.MinInt64)},
			}, // TODO ?
			{
				clusterID:      10,
				memberID:       2,
				revision:       math.MaxInt64,
				responseHeader: &etcdserverpb.ResponseHeader{ClusterId: 10, MemberId: 2, Revision: math.MaxInt64, RaftTerm: toUnit64(math.MaxInt64)},
			},
		} {
			func(clusterID, memberID uint64, revision int64, responseHeader *etcdserverpb.ResponseHeader) {
				It(fmt.Sprintf("clusterID=%d, memberID=%d, revision=%d", clusterID, memberID, revision), func() {
					b.clusterID = clusterID
					b.memberID = memberID

					Expect(b.newResponseHeaderWithRevision(revision)).To(Equal(responseHeader))
				})
			}(s.clusterID, s.memberID, s.revision, s.responseHeader)
		}
	})
})

var _ = Describe("newClosedOpenInterval", func() {
	for _, s := range []struct {
		start, end    []byte
		matchInterval types.GomegaMatcher
	}{
		{matchInterval: Equal(&closedOpenInterval{})},
		{start: []byte{}, matchInterval: Equal(&closedOpenInterval{start: key([]byte{})})},
		{start: []byte("a"), matchInterval: Equal(&closedOpenInterval{start: key([]byte("a"))})},
		{start: []byte("a"), end: []byte{}, matchInterval: Equal(&closedOpenInterval{start: key([]byte("a")), end: key([]byte{})})},
		{start: []byte("a"), end: []byte("b"), matchInterval: Equal(&closedOpenInterval{start: key([]byte("a")), end: key([]byte("b"))})},
	} {
		func(start, end []byte, matchInterval types.GomegaMatcher) {
			It(fmt.Sprintf("[%v, %v)", start, end), func() {
				Expect(newClosedOpenInterval(start, end)).To(matchInterval)
			})
		}(s.start, s.end, s.matchInterval)
	}
})

var _ = Describe("setHeaderRevision", func() {
	type check struct {
		rh, erh  etcdserverpb.ResponseHeader
		mutated  bool
		revision int64
	}

	var toUnit64 = func(i int64) uint64 { return uint64(i) }

	for _, s := range []check{
		{},
		func() check {
			var rh = etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1}
			return check{rh: rh, revision: 2, erh: rh}
		}(),
		{mutated: true},
		{mutated: true, revision: 1, erh: etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1}},
		{mutated: true, revision: math.MinInt64, erh: etcdserverpb.ResponseHeader{Revision: math.MinInt64, RaftTerm: toUnit64(math.MinInt64)}},
		{mutated: true, revision: math.MaxInt64, erh: etcdserverpb.ResponseHeader{Revision: math.MaxInt64, RaftTerm: toUnit64(math.MaxInt64)}},
		{
			rh:      etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
			mutated: true,
			erh:     etcdserverpb.ResponseHeader{},
		},
		func() check {
			var rh = etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1}
			return check{rh: rh, mutated: true, revision: 1, erh: rh}
		}(),
		{
			rh:       etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
			mutated:  true,
			revision: math.MinInt64,
			erh:      etcdserverpb.ResponseHeader{Revision: math.MinInt64, RaftTerm: toUnit64(math.MinInt64)},
		},
		{
			rh:       etcdserverpb.ResponseHeader{Revision: 1, RaftTerm: 1},
			mutated:  true,
			revision: math.MaxInt64,
			erh:      etcdserverpb.ResponseHeader{Revision: math.MaxInt64, RaftTerm: toUnit64(math.MaxInt64)},
		},
	} {
		func(s check) {
			It(fmt.Sprintf("responseHeader=%v, mutated=%t, revision=%d", s.rh, s.mutated, s.revision), func() {
				setHeaderRevision(&s.rh, s.mutated, s.revision)
				Expect(s.rh).To(Equal(s.erh))
			})
		}(s)
	}
})
