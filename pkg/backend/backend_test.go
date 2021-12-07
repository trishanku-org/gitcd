package backend

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strconv"

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

var _ = Describe("backend", func() {
	var b *backend

	BeforeEach(func() {
		b = &backend{}
	})

	Describe("getDataRefName", func() {
		for refName, expectErr := range map[string]bool{
			"":                true,
			"a":               false,
			"refs/heads/main": false,
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

	Describe("getMetadataRefName", func() {
		for _, s := range []struct {
			refName, metaRefNamePrefix, expectedMetaRefName string
			matchErr                                        types.GomegaMatcher
		}{
			{matchErr: MatchError(rpctypes.ErrGRPCCorrupt)},
			{refName: "a", expectedMetaRefName: DefaultMetadataReferencePrefix + "a", matchErr: Succeed()},
			{refName: "refs/heads/main", expectedMetaRefName: DefaultMetadataReferencePrefix + "refs/heads/main", matchErr: Succeed()},
			{metaRefNamePrefix: "refs/p", matchErr: MatchError(rpctypes.ErrGRPCCorrupt)},
			{refName: "a", metaRefNamePrefix: "refs/p", expectedMetaRefName: "refs/p/a", matchErr: Succeed()},
			{refName: "refs/heads/main", metaRefNamePrefix: "refs/p", expectedMetaRefName: "refs/p/refs/heads/main", matchErr: Succeed()},
			{metaRefNamePrefix: "refs/p/", matchErr: MatchError(rpctypes.ErrGRPCCorrupt)},
			{refName: "a", metaRefNamePrefix: "refs/p/", expectedMetaRefName: "refs/p/a", matchErr: Succeed()},
			{refName: "refs/heads/main", metaRefNamePrefix: "refs/p/", expectedMetaRefName: "refs/p/refs/heads/main", matchErr: Succeed()},
		} {
			func(refName, metaRefNamePrefix, expectedMetaRefName git.ReferenceName, matchErr types.GomegaMatcher) {
				It(fmt.Sprintf("refName=%q, metadataRefNamePrefix=%q", refName, metaRefNamePrefix), func() {
					var (
						r   git.ReferenceName
						err error
					)

					b.refName = refName
					b.metadataRefNamePrefix = metaRefNamePrefix

					r, err = b.getMetadataRefName()

					Expect(err).To(matchErr)
					Expect(r).To(Equal(expectedMetaRefName))
				})
			}(git.ReferenceName(s.refName), git.ReferenceName(s.metaRefNamePrefix), git.ReferenceName(s.expectedMetaRefName), s.matchErr)
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
					refName = "refs/heads/main"
					matchErr = HaveOccurred()
					expectRef = func(ref git.Reference) { Expect(ref).To(BeNil()) }
				})

				ItShouldFail()
			})

			Describe("with a reference", func() {
				var cd *CommitDef

				BeforeEach(func() {
					refName = "refs/heads/main"
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
				b.refName = "refs/heads/main"
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
						b.refName = "refs/heads/does_not_exist"
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
						return CreateAndLoadReferenceFromDef(ctx, b.repo, "refs/heads/main", cd)
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
							b.refName = "refs/heads/main"
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
					refName, metaRefNamePrefix                                     git.ReferenceName
					currentMetaHead, currentDataHead                               *CommitDef
					metaMutated, dataMutated                                       bool
					newMetaHead, newDataHead                                       *CommitDef
					revision                                                       int64
					matchErr, matchMetaHeadCommitDefPtr, matchDataHeadCommitDefPtr types.GomegaMatcher
				}

				for refName, metaRefNamePrefix := range map[git.ReferenceName]git.ReferenceName{
					"refs/heads/main":   "",
					"refs/heads/custom": "refs/custom/prefix",
				} {
					for _, s := range []check{
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							refName:                   refName,
							metaRefNamePrefix:         metaRefNamePrefix,
							revision:                  1,
							matchErr:                  Succeed(),
							matchMetaHeadCommitDefPtr: BeNil(),
							matchDataHeadCommitDefPtr: BeNil(),
						},
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							refName:                   refName,
							metaRefNamePrefix:         metaRefNamePrefix,
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
							metaRefNamePrefix:         metaRefNamePrefix,
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
							metaRefNamePrefix:         metaRefNamePrefix,
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
							metaRefNamePrefix:         metaRefNamePrefix,
							revision:                  1,
							matchErr:                  Succeed(),
							matchMetaHeadCommitDefPtr: BeNil(),
							matchDataHeadCommitDefPtr: BeNil(),
						},
						func() check {
							var cd = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}

							return check{
								refName:                   refName,
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
								metaRefNamePrefix:         metaRefNamePrefix,
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
									"%s refName=%q metaRefNamePrefix=%q hasCurrentMetaHead=%t, hasCurrentDataHead=%t metaMutated=%t dataMutated=%t, revision=%d",
									s.spec,
									s.refName,
									s.metaRefNamePrefix,
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
										b.refName, b.metadataRefNamePrefix = s.refName, s.metaRefNamePrefix

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

		Describe("isTreeEmpty", func() {
			type idFunc func() git.ObjectID
			type check struct {
				spec                 string
				ctxFn                ContextFunc
				idFn                 idFunc
				matchErr, matchEmpty types.GomegaMatcher
			}

			var (
				aToID = func(s string) idFunc {
					return func() (id git.ObjectID) {
						Expect(func() (err error) { id, err = git.NewObjectID(s); return }()).To(Succeed())
						return
					}
				}

				blobID = func() idFunc {
					return func() (id git.ObjectID) {
						Expect(func() (err error) { id, err = CreateBlob(ctx, b.repo, []byte{}); return }()).To(Succeed())
						return
					}
				}

				treeID = func(td *TreeDef) idFunc {
					return func() (id git.ObjectID) {
						Expect(func() (err error) { id, err = CreateTreeFromDef(ctx, b.repo, td); return }()).To(Succeed())
						return
					}
				}

				commitID = func() idFunc {
					return func() (id git.ObjectID) {
						Expect(func() (err error) { id, err = CreateCommitFromDef(ctx, b.repo, &CommitDef{}); return }()).To(Succeed())
						return
					}
				}
			)

			for _, s := range []check{
				{spec: "expired context", ctxFn: CanceledContext, matchErr: MatchError("context canceled"), matchEmpty: BeFalse()},
				{spec: "empty id", matchErr: HaveOccurred(), matchEmpty: BeFalse()},
				{spec: "non-existent id", idFn: aToID("1111111111111111111111111111111111111111"), matchErr: HaveOccurred(), matchEmpty: BeFalse()},
				{spec: "blob ID", idFn: blobID(), matchErr: HaveOccurred(), matchEmpty: BeFalse()},
				{spec: "commit ID", idFn: commitID(), matchErr: HaveOccurred(), matchEmpty: BeFalse()},
				{spec: "empty tree", idFn: treeID(&TreeDef{}), matchErr: Succeed(), matchEmpty: BeTrue()},
				{
					spec:       "tree with blobs",
					idFn:       treeID(&TreeDef{Blobs: map[string][]byte{"blob": []byte("blob")}}),
					matchErr:   Succeed(),
					matchEmpty: BeFalse(),
				},
				{
					spec:       "tree with subtree",
					idFn:       treeID(&TreeDef{Subtrees: map[string]TreeDef{"subtree": {}}}),
					matchErr:   Succeed(),
					matchEmpty: BeFalse(),
				},
				{
					spec:       "tree with blob subtree",
					idFn:       treeID(&TreeDef{Blobs: map[string][]byte{"blob": []byte("blob")}, Subtrees: map[string]TreeDef{"subtree": {}}}),
					matchErr:   Succeed(),
					matchEmpty: BeFalse(),
				},
			} {
				func(s check) {
					Describe(s.spec, func() {
						var id git.ObjectID

						BeforeEach(func() {
							if s.idFn != nil {
								id = s.idFn()
							}

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}
						})

						It(ItSpecForMatchError(s.matchErr), func() {
							var empty, err = b.isTreeEmpty(ctx, id)
							Expect(err).To(s.matchErr)
							Expect(empty).To(s.matchEmpty)
						})
					})
				}(s)
			}
		})

		Describe("tree mutation", func() {
			var (
				nop = func(mutated bool) mutateTreeEntryFunc {
					return func(_ context.Context, _ git.TreeBuilder, _ string, _ git.TreeEntry) (bool, error) {
						return mutated, nil
					}
				}

				addOrReplaceWithBlob = func(content []byte) mutateTreeEntryFunc {
					return func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
						var id git.ObjectID

						if te != nil {
							if err = tb.RemoveEntry(entryName); err != nil {
								return
							}

							mutated = true
						}

						if id, err = CreateBlob(ctx, b.repo, content); err != nil {
							return
						}

						if err = tb.AddEntry(entryName, id, git.FilemodeBlob); err != nil {
							return
						}

						mutated = true

						return
					}
				}

				addOrReplaceWithTree = func(td *TreeDef) mutateTreeEntryFunc {
					return func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
						var id git.ObjectID

						if te != nil {
							if err = tb.RemoveEntry(entryName); err != nil {
								return
							}

							mutated = true
						}

						if id, err = CreateTreeFromDef(ctx, b.repo, td); err != nil {
							return
						}

						if err = tb.AddEntry(entryName, id, git.FilemodeTree); err != nil {
							return
						}

						mutated = true

						return
					}
				}

				remove = func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
					if te != nil {
						err = tb.RemoveEntry(entryName)
						mutated = err == nil
					}

					return
				}

				lotsOfMutationsOnNilTree = func() (tm *treeMutation, etd *TreeDef) {
					var subtreePrefix = "subtree-"

					tm = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
					etd = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

					for i := 0; i < 1000; i++ {
						var (
							k   = strconv.Itoa(i)
							pk  = strconv.Itoa(i / 10 * 10)
							ppk = strconv.Itoa(i / 100 * 100)
						)

						switch k {
						case ppk:
							tm.Entries[k] = addOrReplaceWithBlob([]byte(k))
							etd.Blobs[k] = []byte(k)

							tm.Subtrees[subtreePrefix+ppk] = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
							etd.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

							fallthrough
						case pk:
							tm.Subtrees[subtreePrefix+ppk].Entries[k] = addOrReplaceWithBlob(([]byte(k)))
							etd.Subtrees[subtreePrefix+ppk].Blobs[k] = []byte(k)

							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
							etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
						default:
							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Entries[k] = addOrReplaceWithBlob([]byte(k))
							etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Blobs[k] = []byte(k)
						}
					}

					return
				}

				lotsOfMutationsOnExistingTree = func() (td *TreeDef, tm *treeMutation, etd *TreeDef) {
					var subtreePrefix = "subtree-"
					td = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
					tm = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
					etd = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

					for i := 0; i < 2000; i++ {
						var (
							k   = strconv.Itoa(i)
							pk  = strconv.Itoa(i / 10 * 10)
							ppk = strconv.Itoa(i / 100 * 100)

							mutation mutateTreeEntryFunc
							ev       []byte
						)

						switch rand.Int() % 4 {
						case 1:
							mutation = nop(false)
							ev = []byte(k)
						case 2:
							mutation = addOrReplaceWithBlob([]byte(k + "1000"))
							ev = []byte(k + "1000")
						case 3:
							mutation = remove
						default:
							// skip mutation
							ev = []byte(k)
						}

						switch k {
						case ppk:
							td.Blobs[k] = []byte(k)
							if mutation != nil {
								tm.Entries[k] = mutation
							}
							if ev != nil {
								etd.Blobs[k] = ev
							}

							tm.Subtrees[subtreePrefix+ppk] = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
							td.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
							etd.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

							fallthrough
						case pk:
							td.Subtrees[subtreePrefix+ppk].Blobs[k] = []byte(k)
							if mutation != nil {
								tm.Subtrees[subtreePrefix+ppk].Entries[k] = mutation
							}
							if ev != nil {
								etd.Subtrees[subtreePrefix+ppk].Blobs[k] = ev
							}

							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
							td.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
							etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
						default:
							td.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Blobs[k] = []byte(k)
							if mutation != nil {
								tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Entries[k] = mutation
							}
							if ev != nil {
								etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Blobs[k] = ev
							}
						}
					}
					return
				}

				lotsOfDeletionsOnExistingTree = func() (td *TreeDef, tm *treeMutation, etd *TreeDef) {
					var subtreePrefix = "subtree-"
					td = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
					tm = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
					etd = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

					for i := 0; i < 1000; i++ {
						var (
							k   = strconv.Itoa(i)
							pk  = strconv.Itoa(i / 10 * 10)
							ppk = strconv.Itoa(i / 100 * 100)
						)

						switch k {
						case ppk:
							td.Blobs[k] = []byte(k)
							tm.Entries[k] = remove

							tm.Subtrees[subtreePrefix+ppk] = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
							td.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
							etd.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

							fallthrough
						case pk:
							td.Subtrees[subtreePrefix+ppk].Blobs[k] = []byte(k)
							tm.Subtrees[subtreePrefix+ppk].Entries[k] = remove

							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}}
							td.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
							etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
						default:
							td.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Blobs[k] = []byte(k)
							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Entries[k] = remove
						}
					}
					return
				}
			)

			Describe("mutateTreeBuilder", func() {
				type treeBuilderFunc func(t git.Tree) (git.TreeBuilder, error)

				type check struct {
					spec                                 string
					ctxFn                                ContextFunc
					td                                   *TreeDef
					tbFn                                 treeBuilderFunc
					tm                                   *treeMutation
					cleanupEmptySubtrees                 bool
					matchErr, matchMutated, matchTreeDef types.GomegaMatcher
				}

				var (
					treeBuilderFn = func(t git.Tree) (tb git.TreeBuilder, err error) {
						if t == nil {
							tb, err = b.repo.TreeBuilder(ctx)
						} else {
							tb, err = b.repo.TreeBuilderFromTree(ctx, t)
						}

						return
					}
				)

				for _, s := range []check{
					{
						spec:         "nil tree, nil tree builder with empty tree mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &treeMutation{},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with empty tree mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with nop blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": nop(false)}},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with nop-mutate blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": nop(true)}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with nop subtrees only mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with blob entries only add/replace tree mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec: "nil tree with blob entries only add/replace tree mutation",
						tbFn: treeBuilderFn,
						tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{
							"1": addOrReplaceWithBlob([]byte("1")),
							"2": addOrReplaceWithBlob([]byte("2")),
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}),
					},
					{
						spec:         "nil tree with subtrees only nop tree mutation",
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}, "2": {}}},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec: "nil tree with subtrees only nop-mutate tree mutation",
						tbFn: treeBuilderFn,
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": nop(true)}},
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": nop(true)}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{"1": {}, "2": {}}}),
					},
					{
						spec: "nil tree with subtrees only tree mutation",
						tbFn: treeBuilderFn,
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("1")}},
							"2": {Blobs: map[string][]byte{"2": []byte("2")}},
						}}),
					},
					{
						spec: "nil tree with entries and subtrees tree mutation",
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"4": {Entries: map[string]mutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("4"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")},
							Subtrees: map[string]TreeDef{
								"3": {Blobs: map[string][]byte{"3": []byte("3")}},
								"4": {Blobs: map[string][]byte{"4": []byte("4")}},
							},
						}),
					},
					{
						spec: "nil tree with entries and subtrees with one clashing key tree mutation",
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entry should win because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}},
						}),
					},
					{
						spec: "nil tree with entries and subtrees with all clashing keys tree mutation",
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*treeMutation{
								"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entries should win and no subtree is expected because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}),
					},
					func() check {
						var tm, td = lotsOfMutationsOnNilTree()

						return check{
							spec:         "nil tree with a lot of entries and subtrees tree mutation",
							tbFn:         treeBuilderFn,
							tm:           tm,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							// The blob entries should win and no subtree is expected because of depth first mutation.
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "empty tree mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           &treeMutation{},
							matchErr:     Succeed(),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "nop blob entries only mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"2": nop(false)}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "nop-mutate blob entries only mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"2": nop(true)}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "nop subtrees only mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "blob entries only add/replace tree mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "blob entries only remove tree mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": remove}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					{
						spec: "blob entries only add/replace tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
						tbFn: treeBuilderFn,
						tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{
							"1": addOrReplaceWithBlob([]byte("10")),
							"2": addOrReplaceWithBlob([]byte("20")),
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("10"), "2": []byte("20")}}),
					},
					{
						spec:         "blob entries only remove tree mutation",
						td:           &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": remove}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}),
					},
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "subtrees only nop tree mutation",
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}, "2": {}}},
							matchErr:     Succeed(),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					{
						spec: "subtrees only nop-mutate tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tbFn: treeBuilderFn,
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": nop(true)}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"2": []byte("2")},
							Subtrees: map[string]TreeDef{"1": {}},
						}),
					},
					{
						spec: "subtrees only tree mutation",
						td: &TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("1")}},
						}},
						tbFn: treeBuilderFn,
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("10")}},
							"2": {Blobs: map[string][]byte{"2": []byte("20")}},
						}}),
					},
					{
						spec: "subtrees only remove one blob entry tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tbFn: treeBuilderFn,
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": remove}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{"2": {Blobs: map[string][]byte{"3": []byte("3")}}},
						}),
					},
					{
						spec: "subtrees only remove all blob entries tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tbFn: treeBuilderFn,
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": remove, "3": remove}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{"2": {}},
						}),
					},
					{
						spec: "subtrees only remove all blob entries tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tbFn: treeBuilderFn,
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": remove, "3": remove}},
						}},
						cleanupEmptySubtrees: true,
						matchErr:             Succeed(),
						matchMutated:         BeTrue(),
						matchTreeDef:         GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}),
					},
					{
						spec: "subtrees only remove subtree entry tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tbFn:         treeBuilderFn,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"2": remove}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}),
					},
					{
						spec: "addOrReplace blob entry with subtree tree mutation",
						td: &TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("1")}},
						}},
						tbFn: treeBuilderFn,
						tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{
							"1": addOrReplaceWithTree(&TreeDef{Blobs: map[string][]byte{"1": []byte("10")}}),
							"2": addOrReplaceWithTree(&TreeDef{Blobs: map[string][]byte{"2": []byte("20")}}),
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("10")}},
							"2": {Blobs: map[string][]byte{"2": []byte("20")}},
						}}),
					},
					{
						spec: "entries and subtrees tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3"), "4": []byte("4")},
							Subtrees: map[string]TreeDef{
								"5": {Blobs: map[string][]byte{"5": []byte("5"), "7": []byte("7")}},
								"8": {Blobs: map[string][]byte{"8": []byte("8")}},
							},
						},
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*treeMutation{
								"4": {Entries: map[string]mutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutateTreeEntryFunc{"8": remove}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("10"), "2": []byte("20")},
							Subtrees: map[string]TreeDef{
								"4": {Blobs: map[string][]byte{"4": []byte("40")}},
								"5": {Blobs: map[string][]byte{"5": []byte("50")}},
								"6": {Blobs: map[string][]byte{"6": []byte("60")}},
								"8": {},
							},
						}),
					},
					{
						spec: "entries and subtrees tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3"), "4": []byte("4")},
							Subtrees: map[string]TreeDef{
								"5": {Blobs: map[string][]byte{"5": []byte("5"), "7": []byte("7")}},
								"8": {Blobs: map[string][]byte{"8": []byte("8")}},
							},
						},
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*treeMutation{
								"4": {Entries: map[string]mutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutateTreeEntryFunc{"8": remove}},
							},
						},
						cleanupEmptySubtrees: true,
						matchErr:             Succeed(),
						matchMutated:         BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("10"), "2": []byte("20")},
							Subtrees: map[string]TreeDef{
								"4": {Blobs: map[string][]byte{"4": []byte("40")}},
								"5": {Blobs: map[string][]byte{"5": []byte("50")}},
								"6": {Blobs: map[string][]byte{"6": []byte("60")}},
							},
						}),
					},
					{
						spec: "entries and subtrees with one clashing key add/replace tree mutation",
						td: &TreeDef{
							Blobs:    map[string][]byte{"2": []byte("2")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}},
						},
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": addOrReplaceWithBlob([]byte("20"))},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entry should win because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("10"), "2": []byte("20")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("30")}}},
						}),
					},
					{
						spec: "entries and subtrees with one clashing key remove tree mutation",
						td: &TreeDef{
							Blobs:    map[string][]byte{"2": []byte("2")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}},
						},
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entry should win because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("10")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("30")}}},
						}),
					},
					{
						spec: "entries and subtrees with all clashing keys tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tbFn: treeBuilderFn,
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*treeMutation{
								"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entries should win and no subtree is expected because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("10")}}),
					},
					func() check {
						var td, tm, etd = lotsOfMutationsOnExistingTree()

						return check{
							spec:         "a lot of entries and subtrees tree mutation",
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           tm,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							matchTreeDef: GetTreeDefMatcher(etd),
						}
					}(),
					func() check {
						var td, tm, etd = lotsOfDeletionsOnExistingTree()

						return check{
							spec:         "remove mutation for all blob entries in a large existing tree",
							td:           td,
							tbFn:         treeBuilderFn,
							tm:           tm,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							matchTreeDef: GetTreeDefMatcher(etd),
						}
					}(),
					func() check {
						var td, tm, _ = lotsOfDeletionsOnExistingTree()

						return check{
							spec:                 "remove mutation for all blob entries in a large existing tree",
							td:                   td,
							tbFn:                 treeBuilderFn,
							tm:                   tm,
							cleanupEmptySubtrees: true,
							matchErr:             Succeed(),
							matchMutated:         BeTrue(),
							// All subtrees should be cleaned up.
							matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
						}
					}(),
				} {
					func(s check) {
						Describe(fmt.Sprintf("%s cleanupEmptySubtrees=%t", s.spec, s.cleanupEmptySubtrees), func() {
							var (
								t         git.Tree
								tb        git.TreeBuilder
								parentCtx context.Context
							)

							BeforeEach(func() {
								if s.td != nil {
									Expect(func() (err error) { t, err = CreateAndLoadTreeFromDef(ctx, b.repo, s.td); return }()).To(Succeed())
									Expect(t).ToNot(BeNil())
								}

								if s.tbFn != nil {
									Expect(func() (err error) { tb, err = s.tbFn(t); return }()).To(Succeed())
								}

								parentCtx = ctx

								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							AfterEach(func() {
								if t != nil {
									defer t.Close()
								}

								if tb != nil {
									defer tb.Close()
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var mutated, err = b.mutateTreeBuilder(ctx, t, tb, s.tm, s.cleanupEmptySubtrees)
								Expect(err).To(s.matchErr)
								Expect(mutated).To(s.matchMutated)

								if tb != nil {
									Expect(func() (td TreeDef, err error) {
										var id git.ObjectID

										if id, err = tb.Build(parentCtx); err != nil {
											return
										}

										td = *GetTreeDef(parentCtx, b.repo, id)
										return
									}()).To(s.matchTreeDef)
								}
							})
						})
					}(s)
				}
			})

			Describe("mutateTree", func() {
				type check struct {
					spec                                 string
					ctxFn                                ContextFunc
					td                                   *TreeDef
					tm                                   *treeMutation
					cleanupEmptySubtrees                 bool
					matchErr, matchMutated, matchTreeDef types.GomegaMatcher
				}

				for _, s := range []check{
					{
						spec:         "nil tree with empty tree mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &treeMutation{},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with nop blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": nop(false)}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with nop-mutate blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": nop(true)}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with nop subtrees only mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with blob entries only add/replace tree mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec: "nil tree with blob entries only add/replace tree mutation",
						tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{
							"1": addOrReplaceWithBlob([]byte("1")),
							"2": addOrReplaceWithBlob([]byte("2")),
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}),
					},
					{
						spec:         "nil tree with subtrees only nop tree mutation",
						tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}, "2": {}}},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
					},
					{
						spec: "nil tree with subtrees only nop-mutate tree mutation",
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": nop(true)}},
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": nop(true)}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{"1": {}, "2": {}}}),
					},
					{
						spec: "nil tree with subtrees only tree mutation",
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("1")}},
							"2": {Blobs: map[string][]byte{"2": []byte("2")}},
						}}),
					},
					{
						spec: "nil tree with entries and subtrees tree mutation",
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"4": {Entries: map[string]mutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("4"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")},
							Subtrees: map[string]TreeDef{
								"3": {Blobs: map[string][]byte{"3": []byte("3")}},
								"4": {Blobs: map[string][]byte{"4": []byte("4")}},
							},
						}),
					},
					{
						spec: "nil tree with entries and subtrees with one clashing key tree mutation",
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entry should win because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}},
						}),
					},
					{
						spec: "nil tree with entries and subtrees with all clashing keys tree mutation",
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*treeMutation{
								"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entries should win and no subtree is expected because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}),
					},
					func() check {
						var tm, td = lotsOfMutationsOnNilTree()

						return check{
							spec:         "nil tree with a lot of entries and subtrees tree mutation",
							tm:           tm,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							// The blob entries should win and no subtree is expected because of depth first mutation.
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "empty tree mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tm:           &treeMutation{},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "nop blob entries only mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"2": nop(false)}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "nop-mutate blob entries only mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"2": nop(true)}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "nop subtrees only mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "blob entries only add/replace tree mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
						}
					}(),
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "blob entries only remove tree mutation with expired context",
							ctxFn:        CanceledContext,
							td:           td,
							tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": remove}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
						}
					}(),
					{
						spec: "blob entries only add/replace tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
						tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{
							"1": addOrReplaceWithBlob([]byte("10")),
							"2": addOrReplaceWithBlob([]byte("20")),
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("10"), "2": []byte("20")}}),
					},
					{
						spec:         "blob entries only remove tree mutation",
						td:           &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": remove}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}),
					},
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "subtrees only nop tree mutation",
							td:           td,
							tm:           &treeMutation{Subtrees: map[string]*treeMutation{"1": {}, "2": {}}},
							matchErr:     Succeed(),
							matchMutated: BeFalse(),
						}
					}(),
					{
						spec: "subtrees only nop-mutate tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": nop(true)}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"2": []byte("2")},
							Subtrees: map[string]TreeDef{"1": {}},
						}),
					},
					{
						spec: "subtrees only tree mutation",
						td: &TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("1")}},
						}},
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("10")}},
							"2": {Blobs: map[string][]byte{"2": []byte("20")}},
						}}),
					},
					{
						spec: "subtrees only remove one blob entry tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": remove}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{"2": {Blobs: map[string][]byte{"3": []byte("3")}}},
						}),
					},
					{
						spec: "subtrees only remove all blob entries tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": remove, "3": remove}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{"2": {}},
						}),
					},
					{
						spec: "subtrees only remove all blob entries tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tm: &treeMutation{Subtrees: map[string]*treeMutation{
							"2": {Entries: map[string]mutateTreeEntryFunc{"2": remove, "3": remove}},
						}},
						cleanupEmptySubtrees: true,
						matchErr:             Succeed(),
						matchMutated:         BeTrue(),
						matchTreeDef:         GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}),
					},
					{
						spec: "subtrees only remove subtree entry tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2"), "3": []byte("3")}},
							},
						},
						tm:           &treeMutation{Entries: map[string]mutateTreeEntryFunc{"2": remove}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}),
					},
					{
						spec: "addOrReplace blob entry with subtree tree mutation",
						td: &TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("1")}},
						}},
						tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{
							"1": addOrReplaceWithTree(&TreeDef{Blobs: map[string][]byte{"1": []byte("10")}}),
							"2": addOrReplaceWithTree(&TreeDef{Blobs: map[string][]byte{"2": []byte("20")}}),
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("10")}},
							"2": {Blobs: map[string][]byte{"2": []byte("20")}},
						}}),
					},
					{
						spec: "entries and subtrees tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3"), "4": []byte("4")},
							Subtrees: map[string]TreeDef{
								"5": {Blobs: map[string][]byte{"5": []byte("5"), "7": []byte("7")}},
								"8": {Blobs: map[string][]byte{"8": []byte("8")}},
							},
						},
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*treeMutation{
								"4": {Entries: map[string]mutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutateTreeEntryFunc{"8": remove}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("10"), "2": []byte("20")},
							Subtrees: map[string]TreeDef{
								"4": {Blobs: map[string][]byte{"4": []byte("40")}},
								"5": {Blobs: map[string][]byte{"5": []byte("50")}},
								"6": {Blobs: map[string][]byte{"6": []byte("60")}},
								"8": {},
							},
						}),
					},
					{
						spec: "entries and subtrees tree mutation",
						td: &TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3"), "4": []byte("4")},
							Subtrees: map[string]TreeDef{
								"5": {Blobs: map[string][]byte{"5": []byte("5"), "7": []byte("7")}},
								"8": {Blobs: map[string][]byte{"8": []byte("8")}},
							},
						},
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*treeMutation{
								"4": {Entries: map[string]mutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutateTreeEntryFunc{"8": remove}},
							},
						},
						cleanupEmptySubtrees: true,
						matchErr:             Succeed(),
						matchMutated:         BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("10"), "2": []byte("20")},
							Subtrees: map[string]TreeDef{
								"4": {Blobs: map[string][]byte{"4": []byte("40")}},
								"5": {Blobs: map[string][]byte{"5": []byte("50")}},
								"6": {Blobs: map[string][]byte{"6": []byte("60")}},
							},
						}),
					},
					{
						spec: "entries and subtrees with one clashing key add/replace tree mutation",
						td: &TreeDef{
							Blobs:    map[string][]byte{"2": []byte("2")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}},
						},
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": addOrReplaceWithBlob([]byte("20"))},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entry should win because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("10"), "2": []byte("20")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("30")}}},
						}),
					},
					{
						spec: "entries and subtrees with one clashing key remove tree mutation",
						td: &TreeDef{
							Blobs:    map[string][]byte{"2": []byte("2")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}},
						},
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*treeMutation{
								"3": {Entries: map[string]mutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entry should win because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("10")},
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("30")}}},
						}),
					},
					{
						spec: "entries and subtrees with all clashing keys tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tm: &treeMutation{
							Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*treeMutation{
								"1": {Entries: map[string]mutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
								"2": {Entries: map[string]mutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
							},
						},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						// The blob entries should win and no subtree is expected because of depth first mutation.
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("10")}}),
					},
					func() check {
						var td, tm, etd = lotsOfMutationsOnExistingTree()

						return check{
							spec:         "a lot of entries and subtrees tree mutation",
							td:           td,
							tm:           tm,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							matchTreeDef: GetTreeDefMatcher(etd),
						}
					}(),
					func() check {
						var td, tm, etd = lotsOfDeletionsOnExistingTree()

						return check{
							spec:         "remove mutation for all blob entries in a large existing tree",
							td:           td,
							tm:           tm,
							matchErr:     Succeed(),
							matchMutated: BeTrue(),
							matchTreeDef: GetTreeDefMatcher(etd),
						}
					}(),
					func() check {
						var td, tm, _ = lotsOfDeletionsOnExistingTree()

						return check{
							spec:                 "remove mutation for all blob entries in a large existing tree",
							td:                   td,
							tm:                   tm,
							cleanupEmptySubtrees: true,
							matchErr:             Succeed(),
							matchMutated:         BeTrue(),
							// All subtrees should be cleaned up.
							matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
						}
					}(),
				} {
					func(s check) {
						Describe(fmt.Sprintf("%s cleanupEmptySubtrees=%t", s.spec, s.cleanupEmptySubtrees), func() {
							var (
								t         git.Tree
								parentCtx context.Context
							)

							BeforeEach(func() {
								if s.td != nil {
									Expect(func() (err error) { t, err = CreateAndLoadTreeFromDef(ctx, b.repo, s.td); return }()).To(Succeed())
									Expect(t).ToNot(BeNil())
								}

								parentCtx = ctx

								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							AfterEach(func() {
								if t != nil {
									defer t.Close()
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var mutated, id, err = b.mutateTree(ctx, t, s.tm, s.cleanupEmptySubtrees)
								Expect(err).To(s.matchErr)
								Expect(mutated).To(s.matchMutated)
								if mutated && err == nil {
									Expect(*GetTreeDef(parentCtx, b.repo, id)).To(s.matchTreeDef)
								}
							})
						})
					}(s)
				}
			})

			Describe("mutateRevisionTo", func() {
				type check struct {
					spec                                    string
					ctxFn                                   ContextFunc
					td                                      TreeDef
					path                                    string
					newRevision                             int64
					matchErr, matchMutated, matchTreeDefPtr types.GomegaMatcher
				}

				for _, s := range []check{
					{
						spec:            "expired context with non-existent path",
						ctxFn:           CanceledContext,
						path:            "a",
						newRevision:     4,
						matchErr:        MatchError("context canceled"),
						matchMutated:    BeFalse(),
						matchTreeDefPtr: BeNil(),
					},
					{
						spec:            "expired context with existing path with invalid value",
						ctxFn:           CanceledContext,
						td:              TreeDef{Blobs: map[string][]byte{"a": []byte("a")}},
						path:            "a",
						newRevision:     4,
						matchErr:        MatchError("context canceled"),
						matchMutated:    BeFalse(),
						matchTreeDefPtr: BeNil(),
					},
					{
						spec:            "expired context with existing path with non-matching value",
						ctxFn:           CanceledContext,
						td:              TreeDef{Blobs: map[string][]byte{"a": []byte("1")}},
						path:            "a",
						newRevision:     4,
						matchErr:        MatchError("context canceled"),
						matchMutated:    BeFalse(),
						matchTreeDefPtr: BeNil(),
					},
					{
						spec:            "expired context with existing path with tree value",
						ctxFn:           CanceledContext,
						td:              TreeDef{Subtrees: map[string]TreeDef{"a": {}}},
						path:            "a",
						newRevision:     4,
						matchErr:        MatchError("context canceled"),
						matchMutated:    BeFalse(),
						matchTreeDefPtr: BeNil(),
					},
					{
						spec:            "expired context with existing path with matching value",
						ctxFn:           CanceledContext,
						td:              TreeDef{Blobs: map[string][]byte{"a": []byte("4")}},
						path:            "a",
						newRevision:     4,
						matchErr:        MatchError("context canceled"),
						matchMutated:    BeFalse(),
						matchTreeDefPtr: BeNil(),
					},
					{
						spec:            "non-existent path",
						path:            "a",
						newRevision:     4,
						matchErr:        Succeed(),
						matchMutated:    BeTrue(),
						matchTreeDefPtr: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"a": []byte("4")}})),
					},
					{
						spec:            "existing path with invalid value",
						td:              TreeDef{Blobs: map[string][]byte{"a": []byte("a")}},
						path:            "a",
						newRevision:     4,
						matchErr:        MatchError(ContainSubstring("ParseInt")),
						matchMutated:    BeFalse(),
						matchTreeDefPtr: BeNil(),
					},
					{
						spec:            "existing path with non-matching value",
						td:              TreeDef{Blobs: map[string][]byte{"a": []byte("1")}},
						path:            "a",
						newRevision:     4,
						matchErr:        Succeed(),
						matchMutated:    BeTrue(),
						matchTreeDefPtr: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"a": []byte("4")}})),
					},
					{
						spec:            "existing path with tree value",
						td:              TreeDef{Subtrees: map[string]TreeDef{"a": {}}},
						path:            "a",
						newRevision:     4,
						matchErr:        Succeed(),
						matchMutated:    BeTrue(),
						matchTreeDefPtr: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"a": []byte("4")}})),
					},
					{
						spec:            "existing path with matching value",
						td:              TreeDef{Blobs: map[string][]byte{"a": []byte("4")}},
						path:            "a",
						newRevision:     4,
						matchErr:        Succeed(),
						matchMutated:    BeFalse(),
						matchTreeDefPtr: BeNil(),
					},
				} {
					func(s check) {
						Describe(fmt.Sprintf("%s path=%q newRevision=%d", s.spec, s.path, s.newRevision), func() {
							var (
								t         git.Tree
								parentCtx context.Context
							)

							BeforeEach(func() {
								Expect(func() (err error) { t, err = CreateAndLoadTreeFromDef(ctx, b.repo, &s.td); return }()).To(Succeed())

								parentCtx = ctx

								if s.ctxFn != nil {
									ctx = s.ctxFn(ctx)
								}
							})

							AfterEach(func() {
								if t != nil {
									Expect(t.Close()).To(Succeed())
								}
							})

							It(ItSpecForMatchError(s.matchErr), func() {
								var (
									tm      *treeMutation
									mutated bool
									ntID    git.ObjectID
									err     error
								)

								Expect(func() (err error) { tm, err = addMutation(tm, s.path, b.mutateRevisionTo(s.newRevision)); return }()).To(Succeed())

								mutated, ntID, err = b.mutateTree(ctx, t, tm, false)

								Expect(err).To(s.matchErr)
								Expect(mutated).To(s.matchMutated)

								Expect(func() (td *TreeDef) {
									if reflect.DeepEqual(ntID, git.ObjectID{}) {
										return
									}

									td = GetTreeDef(parentCtx, b.repo, ntID)
									return
								}()).To(s.matchTreeDefPtr)
							})
						})
					}(s)
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

var _ = Describe("deleteEntry", func() {
	var (
		ctx  context.Context
		ctrl = gomock.NewController(GinkgoT())

		mockTreeBuilder = func(entryName string, err error) git.TreeBuilder {
			var tb = mockgit.NewMockTreeBuilder(ctrl)

			tb.EXPECT().RemoveEntry(gomock.Any()).DoAndReturn(
				func(n string) error {
					Expect(n).To(Equal(entryName))
					return err
				},
			)

			return tb
		}

		mockTreeEntry = func() git.TreeEntry { return mockgit.NewMockTreeEntry(ctrl) }
	)

	BeforeEach(func() {
		ctx = context.Background()
	})

	for _, ctxFn := range []ContextFunc{nil, CanceledContext} {
		func(ctxFn ContextFunc) {
			Describe(fmt.Sprintf("expired-context=%t", ctxFn != nil), func() {
				BeforeEach(func() {
					if ctxFn != nil {
						ctx = ctxFn(ctx)
					}
				})

				for _, s := range []struct {
					tb                     git.TreeBuilder
					entryName              string
					te                     git.TreeEntry
					matchMutated, matchErr types.GomegaMatcher
				}{
					{matchMutated: BeFalse(), matchErr: Succeed()},
					{entryName: "entry", matchMutated: BeFalse(), matchErr: Succeed()},
					{
						tb:           mockTreeBuilder("", nil),
						te:           mockTreeEntry(),
						matchMutated: BeTrue(),
						matchErr:     Succeed(),
					},
					{
						tb:           mockTreeBuilder("entry", nil),
						entryName:    "entry",
						te:           mockTreeEntry(),
						matchMutated: BeTrue(),
						matchErr:     Succeed(),
					},
					{
						tb:           mockTreeBuilder("entry", errors.New("error")),
						entryName:    "entry",
						te:           mockTreeEntry(),
						matchMutated: BeFalse(),
						matchErr:     MatchError("error"),
					},
				} {
					func(tb git.TreeBuilder, entryName string, te git.TreeEntry, matchMutated, matchErr types.GomegaMatcher) {
						It(
							fmt.Sprintf(
								"hasTreeBuilder=%t entryName=%q hasTreeEntry=%t %s",
								tb != nil,
								entryName,
								te != nil,
								ItSpecForMatchError(matchErr),
							),
							func() {
								var mutated, err = deleteEntry(ctx, tb, entryName, te)
								Expect(err).To(matchErr)
								Expect(mutated).To(matchMutated)
							},
						)
					}(s.tb, s.entryName, s.te, s.matchMutated, s.matchErr)
				}
			})
		}(ctxFn)
	}
})

var _ = Describe("treeMutation", func() {
	var (
		tmPtrMatcher func(tm *treeMutation) types.GomegaMatcher
		nop          = func(_ context.Context, _ git.TreeBuilder, _ string, _ git.TreeEntry) (mutated bool, err error) {
			return
		}
	)

	tmPtrMatcher = func(tm *treeMutation) types.GomegaMatcher {
		var (
			entryKeys   = Keys{}
			subtreeKeys = Keys{}
		)

		for name := range tm.Entries {
			entryKeys[name] = Not(BeNil())
		}

		for name, stm := range tm.Subtrees {
			subtreeKeys[name] = tmPtrMatcher(stm)
		}

		return PointTo(MatchAllFields(Fields{
			"Entries":  MatchAllKeys(entryKeys),
			"Subtrees": MatchAllKeys(subtreeKeys),
		}))
	}

	Describe("addMutationPathSlice", func() {
		type check struct {
			spec                 string
			tm                   *treeMutation
			ps                   pathSlice
			entryName            string
			mutateFn             mutateTreeEntryFunc
			matchErr, matchTMPtr types.GomegaMatcher
		}

		for _, s := range []check{
			{
				spec:       "nil tm, add top level entry",
				entryName:  "1",
				mutateFn:   nop,
				matchErr:   Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": nop}}),
			},
			{
				spec:      "nil tm, add deep entry",
				ps:        pathSlice{"a", "b", "c"},
				entryName: "1",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Subtrees: map[string]*treeMutation{"a": {
						Subtrees: map[string]*treeMutation{"b": {
							Subtrees: map[string]*treeMutation{"c": {Entries: map[string]mutateTreeEntryFunc{"1": nop}}},
						}},
					}},
				}),
			},
			{
				spec:      "existing tm with subtrees but with no top-level entries, add top level entry",
				tm:        &treeMutation{Subtrees: map[string]*treeMutation{"a": {Entries: map[string]mutateTreeEntryFunc{"a": nop}}}},
				entryName: "b",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries:  map[string]mutateTreeEntryFunc{"b": nop},
					Subtrees: map[string]*treeMutation{"a": {Entries: map[string]mutateTreeEntryFunc{"a": nop}}},
				}),
			},
			{
				spec: "existing tm with subtrees and top-level entries, add top level entry",
				tm: &treeMutation{
					Entries:  map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {Entries: map[string]mutateTreeEntryFunc{"b": nop}}},
				},
				entryName: "c",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries:  map[string]mutateTreeEntryFunc{"a": nop, "c": nop},
					Subtrees: map[string]*treeMutation{"b": {Entries: map[string]mutateTreeEntryFunc{"b": nop}}},
				}),
			},
			{
				spec:      "existing tm with top-level entries but with no subtrees, add deep entry",
				tm:        &treeMutation{Entries: map[string]mutateTreeEntryFunc{"a": nop}},
				ps:        pathSlice{"b", "c"},
				entryName: "d",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries: map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {
						Subtrees: map[string]*treeMutation{"c": {
							Entries: map[string]mutateTreeEntryFunc{"d": nop},
						}},
					}},
				}),
			},
			{
				spec: "existing tm with top-level entries and subtrees, add deep entry",
				tm: &treeMutation{
					Entries: map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {
						Subtrees: map[string]*treeMutation{"c": {
							Entries: map[string]mutateTreeEntryFunc{"d": nop},
						}},
					}},
				},
				ps:        pathSlice{"b", "d", "e"},
				entryName: "f",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries: map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {
						Subtrees: map[string]*treeMutation{
							"c": {Entries: map[string]mutateTreeEntryFunc{"d": nop}},
							"d": {
								Subtrees: map[string]*treeMutation{"e": {Entries: map[string]mutateTreeEntryFunc{"f": nop}}},
							},
						},
					}},
				}),
			},
		} {
			func(s check) {
				It(s.spec, func() {
					var tm, err = addMutationPathSlice(s.tm, s.ps, s.entryName, s.mutateFn)
					Expect(err).To(s.matchErr)
					Expect(tm).To(s.matchTMPtr)
				})
			}(s)
		}
	})

	Describe("addMutation", func() {
		type check struct {
			spec                 string
			tm                   *treeMutation
			p                    string
			mutateFn             mutateTreeEntryFunc
			matchErr, matchTMPtr types.GomegaMatcher
		}

		for _, s := range []check{
			{
				spec:       "nil tm, empty path",
				p:          "",
				mutateFn:   nop,
				matchErr:   MatchError(rpctypes.ErrGRPCEmptyKey),
				matchTMPtr: BeNil(),
			},
			{
				spec:       "nil tm, effectively empty path",
				p:          "///.",
				mutateFn:   nop,
				matchErr:   MatchError(rpctypes.ErrGRPCEmptyKey),
				matchTMPtr: BeNil(),
			},
			{
				spec:       "nil tm, add top level entry",
				p:          "1",
				mutateFn:   nop,
				matchErr:   Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{Entries: map[string]mutateTreeEntryFunc{"1": nop}}),
			},
			{
				spec:     "nil tm, add deep entry",
				p:        "a/b/c/1",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Subtrees: map[string]*treeMutation{"a": {
						Subtrees: map[string]*treeMutation{"b": {
							Subtrees: map[string]*treeMutation{"c": {Entries: map[string]mutateTreeEntryFunc{"1": nop}}},
						}},
					}},
				}),
			},
			func() check {
				var tm = &treeMutation{Entries: map[string]mutateTreeEntryFunc{"a": nop}}

				return check{
					spec:       "existing tm, empty path",
					tm:         tm,
					p:          "",
					mutateFn:   nop,
					matchErr:   MatchError(rpctypes.ErrGRPCEmptyKey),
					matchTMPtr: tmPtrMatcher(tm),
				}
			}(),
			func() check {
				var tm = &treeMutation{Entries: map[string]mutateTreeEntryFunc{"a": nop}}

				return check{
					spec:       "existing tm, effectively empty path",
					tm:         tm,
					p:          "",
					mutateFn:   nop,
					matchErr:   MatchError(rpctypes.ErrGRPCEmptyKey),
					matchTMPtr: tmPtrMatcher(tm),
				}
			}(),
			{
				spec:     "existing tm with subtrees but with no top-level entries, add top level entry",
				tm:       &treeMutation{Subtrees: map[string]*treeMutation{"a": {Entries: map[string]mutateTreeEntryFunc{"a": nop}}}},
				p:        "b",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries:  map[string]mutateTreeEntryFunc{"b": nop},
					Subtrees: map[string]*treeMutation{"a": {Entries: map[string]mutateTreeEntryFunc{"a": nop}}},
				}),
			},
			{
				spec: "existing tm with subtrees and top-level entries, add top level entry",
				tm: &treeMutation{
					Entries:  map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {Entries: map[string]mutateTreeEntryFunc{"b": nop}}},
				},
				p:        "c",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries:  map[string]mutateTreeEntryFunc{"a": nop, "c": nop},
					Subtrees: map[string]*treeMutation{"b": {Entries: map[string]mutateTreeEntryFunc{"b": nop}}},
				}),
			},
			{
				spec:     "existing tm with top-level entries but with no subtrees, add deep entry",
				tm:       &treeMutation{Entries: map[string]mutateTreeEntryFunc{"a": nop}},
				p:        "b/c/d",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries: map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {
						Subtrees: map[string]*treeMutation{"c": {
							Entries: map[string]mutateTreeEntryFunc{"d": nop},
						}},
					}},
				}),
			},
			{
				spec: "existing tm with top-level entries and subtrees, add deep entry",
				tm: &treeMutation{
					Entries: map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {
						Subtrees: map[string]*treeMutation{"c": {
							Entries: map[string]mutateTreeEntryFunc{"d": nop},
						}},
					}},
				},
				p:        "b/d/e/f",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&treeMutation{
					Entries: map[string]mutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*treeMutation{"b": {
						Subtrees: map[string]*treeMutation{
							"c": {Entries: map[string]mutateTreeEntryFunc{"d": nop}},
							"d": {
								Subtrees: map[string]*treeMutation{"e": {Entries: map[string]mutateTreeEntryFunc{"f": nop}}},
							},
						},
					}},
				}),
			},
		} {
			func(s check) {
				It(s.spec, func() {
					var tm, err = addMutation(s.tm, s.p, s.mutateFn)
					Expect(err).To(s.matchErr)
					Expect(tm).To(s.matchTMPtr)
				})
			}(s)
		}
	})

	Describe("isMutationNOP", func() {
		type check struct {
			spec     string
			tm       *treeMutation
			matchNOP types.GomegaMatcher
		}

		for _, s := range []check{
			{spec: "nil tm", matchNOP: BeTrue()},
			{spec: "nop shallow tm", tm: &treeMutation{}, matchNOP: BeTrue()},
			{spec: "nop shallow tm with empty entries", tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{}}, matchNOP: BeTrue()},
			{spec: "nop shallow tm with empty subtrees", tm: &treeMutation{Subtrees: map[string]*treeMutation{}}, matchNOP: BeTrue()},
			{
				spec:     "nop shallow tm with empty entries and subtrees",
				tm:       &treeMutation{Entries: map[string]mutateTreeEntryFunc{}, Subtrees: map[string]*treeMutation{}},
				matchNOP: BeTrue(),
			},
			{
				spec: "nop deep tm",
				tm: &treeMutation{
					Entries: map[string]mutateTreeEntryFunc{},
					Subtrees: map[string]*treeMutation{
						"a": {
							Subtrees: map[string]*treeMutation{"b": {
								Subtrees: map[string]*treeMutation{"c": {}},
							}},
						},
						"d": {Subtrees: map[string]*treeMutation{"e": {}}},
					},
				},
				matchNOP: BeTrue(),
			},
			{spec: "non-nop shallow tm", tm: &treeMutation{Entries: map[string]mutateTreeEntryFunc{"a": nop}}, matchNOP: BeFalse()},
			{
				spec: "non-nop deep tm",
				tm: &treeMutation{
					Entries: map[string]mutateTreeEntryFunc{},
					Subtrees: map[string]*treeMutation{
						"a": {
							Subtrees: map[string]*treeMutation{"b": {
								Subtrees: map[string]*treeMutation{"c": {}},
							}},
						},
						"d": {Subtrees: map[string]*treeMutation{"e": {Entries: map[string]mutateTreeEntryFunc{"f": nop}}}},
					},
				},
				matchNOP: BeFalse(),
			},
		} {
			func(s check) {
				It(s.spec, func() {
					Expect(isMutationNOP(s.tm)).To(s.matchNOP)
				})
			}(s)
		}
	})
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
