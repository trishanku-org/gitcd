package git2go

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"

	impl "github.com/libgit2/git2go/v31"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
)

var _ = Describe("toImplObjectType", func() {
	for gt, it := range map[git.ObjectType]impl.ObjectType{
		git.ObjectTypeInvalid: impl.ObjectInvalid,
		git.ObjectTypeBlob:    impl.ObjectBlob,
		git.ObjectTypeTree:    impl.ObjectTree,
		git.ObjectTypeCommit:  impl.ObjectCommit,
		git.ObjectTypeTag:     impl.ObjectTag,
	} {
		func(gt git.ObjectType, it impl.ObjectType) {
			It(fmt.Sprintf("of %d should be %d", gt, it), func() {
				Expect(toImplObjectType(gt)).To(Equal(it))
			})
		}(gt, it)
	}
})

var _ = Describe("toGitObjectType", func() {
	for it, gt := range map[impl.ObjectType]git.ObjectType{
		impl.ObjectInvalid: git.ObjectTypeInvalid,
		impl.ObjectAny:     git.ObjectTypeInvalid,
		impl.ObjectBlob:    git.ObjectTypeBlob,
		impl.ObjectTree:    git.ObjectTypeTree,
		impl.ObjectCommit:  git.ObjectTypeCommit,
		impl.ObjectTag:     git.ObjectTypeTag,
	} {
		func(it impl.ObjectType, gt git.ObjectType) {
			It(fmt.Sprintf("of %d should be %d", it, gt), func() {
				Expect(toGitObjectType(it)).To(Equal(gt))
			})
		}(it, gt)
	}
})

type idFunc func() git.ObjectID

func itShouldPeel(ctxFn func() context.Context, objectFn func() git.Object, checks map[git.ObjectType]idFunc) {
	for t, eIDFn := range checks {
		func(t git.ObjectType, eIDFn idFunc) {
			It(fmt.Sprintf("should be fail to peel to %d with expired context", t), func() {
				var ctx, cancelFn = context.WithCancel(ctxFn())

				cancelFn()

				Expect(objectFn().Peel(ctx, nil)).To(MatchError(ctx.Err()))
			})

			if eIDFn != nil {
				It(fmt.Sprintf("should be peelable to %d", t), func() {
					var po git.Object

					Expect(objectFn().Peel(ctxFn(), git.NewObjectReceiver(t, func(_ context.Context, ro git.Object) (err error) {
						po = ro
						return
					}))).To(Succeed())

					defer po.Close()

					Expect(po.ID()).To(Equal(eIDFn()))
					Expect(po.Type()).To(Equal(t))
				})

				It(fmt.Sprintf("should fail to peel to %d if the receiver returns error", t), func() {
					var (
						po  git.Object
						err = errors.New("receive error")
					)

					Expect(objectFn().Peel(ctxFn(), git.NewObjectReceiver(t, func(_ context.Context, ro git.Object) error {
						defer ro.Close()
						return err
					}))).To(MatchError(err))

					Expect(po).To(BeNil())
				})
			} else {
				It(fmt.Sprintf("should not be peelable to %d", t), func() {
					var po git.Object

					Expect(objectFn().Peel(ctxFn(), git.NewObjectReceiver(t, func(_ context.Context, ro git.Object) (err error) {
						po = ro
						return
					}))).ToNot(Succeed())

					if po != nil {
						defer po.Close()
					}

					Expect(po).To(BeNil())
				})
			}
		}(t, eIDFn)
	}
}

var _ = Describe("Object", func() {
	var (
		ctx           context.Context
		repo          git.Repository
		bID, tID, cID git.ObjectID
	)

	BeforeEach(func() {
		var dir string

		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { repo, err = New().OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(repo).ToNot(BeNil())

		Expect(func() (err error) { bID, tID, cID, err = createEmptyObjects(ctx, repo); return }()).To(Succeed())
	})

	AfterEach(func() {
		if repo != nil {
			Expect(repo.Close()).To(Succeed())
		}
	})

	Describe("Peel", func() {
		var (
			blobID   = func() git.ObjectID { return bID }
			treeID   = func() git.ObjectID { return tID }
			commitID = func() git.ObjectID { return cID }
		)

		for _, s := range []struct {
			spec   string
			idFn   idFunc
			checks map[git.ObjectType]idFunc
		}{
			{
				spec: "blob",
				idFn: blobID,
				checks: map[git.ObjectType]idFunc{
					git.ObjectTypeInvalid: nil,
					git.ObjectTypeBlob:    blobID,
					git.ObjectTypeTree:    nil,
					git.ObjectTypeCommit:  nil,
					git.ObjectTypeTag:     nil,
				},
			},
			{
				spec: "tree",
				idFn: treeID,
				checks: map[git.ObjectType]idFunc{
					git.ObjectTypeInvalid: nil,
					git.ObjectTypeBlob:    nil,
					git.ObjectTypeTree:    treeID,
					git.ObjectTypeCommit:  nil,
					git.ObjectTypeTag:     nil,
				},
			},
			{
				spec: "commit",
				idFn: commitID,
				checks: map[git.ObjectType]idFunc{
					git.ObjectTypeInvalid: nil,
					git.ObjectTypeBlob:    nil,
					git.ObjectTypeTree:    treeID,
					git.ObjectTypeCommit:  commitID,
					git.ObjectTypeTag:     nil,
				},
			},
		} {
			func(spec string, idFn func() git.ObjectID, checks map[git.ObjectType]idFunc) {
				Describe(spec, func() {
					var o git.Object

					BeforeEach(func() {
						Expect(func() (oo git.Object, err error) {
							oo, err = repo.ObjectGetter().GetObject(ctx, idFn())
							o = oo
							return
						}()).ToNot(BeNil())
					})

					AfterEach(func() {
						if o != nil {
							Expect(o.Close()).To(Succeed())
						}
					})

					itShouldPeel(
						func() context.Context { return ctx },
						func() git.Object { return o },
						checks,
					)
				})
			}(s.spec, s.idFn, s.checks)
		}
	})
})

var _ = Describe("Blob", func() {
	var (
		ctx  context.Context
		repo git.Repository

		createBytes = func(b byte, n int) (buf []byte) {
			buf = make([]byte, n)
			for i := 0; i < n; i++ {
				buf[i] = b
			}
			return
		}
	)

	BeforeEach(func() {
		var dir string

		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { repo, err = New().OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(repo).ToNot(BeNil())
	})

	AfterEach(func() {
		if repo != nil {
			Expect(repo.Close()).To(Succeed())
		}
	})

	for _, content := range [][]byte{
		nil,
		[]byte("0123456789"),
		createBytes('a', 256),
		createBytes('b', 1024),
		createBytes('b', 1024*1024),
	} {
		func(content []byte) {
			Describe(fmt.Sprintf("of length %d", len(content)), func() {
				var b git.Blob

				BeforeEach(func() {
					Expect(func() (err error) {
						var id git.ObjectID

						if id, err = createBlob(ctx, repo, content); err != nil {
							return
						}

						b, err = repo.ObjectGetter().GetBlob(ctx, id)
						return
					}()).To(Succeed())
				})

				AfterEach(func() {
					if b != nil {
						Expect(b.Close()).To(Succeed())
					}
				})

				It(fmt.Sprintf("should be of size %d and match the content", len(content)), func() {
					Expect(b.Size()).To(Equal(git.BlobSize(len(content))))
					Expect(b.Content()).To(Equal(content))
				})

				Describe("Peel", func() {
					itShouldPeel(
						func() context.Context { return ctx },
						func() git.Object { return b },
						map[git.ObjectType]idFunc{
							git.ObjectTypeInvalid: nil,
							git.ObjectTypeBlob:    func() git.ObjectID { return b.ID() },
							git.ObjectTypeTree:    nil,
							git.ObjectTypeCommit:  nil,
							git.ObjectTypeTag:     nil,
						},
					)
				})
			})
		}(content)
	}
})

var _ = Describe("Tree", func() {
	var (
		ctx  context.Context
		repo git.Repository
		t    git.Tree

		td31 = &treeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}
		td3  = &treeDef{
			Blobs:    map[string][]byte{"2": []byte("2"), "3": []byte("3")},
			Subtrees: map[string]treeDef{"1": *td31},
		}
		td43 = &treeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}
		td4  = &treeDef{
			Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2")},
			Subtrees: map[string]treeDef{"3": *td43},
		}
		td = &treeDef{
			Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2"), "5": []byte("5")},
			Subtrees: map[string]treeDef{"3": *td3, "4": *td4},
		}
	)

	BeforeEach(func() {
		var dir string

		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { repo, err = New().OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(repo).ToNot(BeNil())

		Expect(func() (err error) {
			var id git.ObjectID

			if id, err = createTreeFromDef(ctx, repo, td); err != nil {
				return
			}

			t, err = repo.ObjectGetter().GetTree(ctx, id)
			return
		}())
	})

	AfterEach(func() {
		if repo != nil {
			defer repo.Close()
		}

		if t != nil {
			defer t.Close()
		}
	})

	Describe("Peel", func() {
		itShouldPeel(
			func() context.Context { return ctx },
			func() git.Object { return t },
			map[git.ObjectType]idFunc{
				git.ObjectTypeInvalid: nil,
				git.ObjectTypeBlob:    nil,
				git.ObjectTypeTree:    func() git.ObjectID { return t.ID() },
				git.ObjectTypeCommit:  nil,
				git.ObjectTypeTag:     nil,
			},
		)
	})

	Describe("GetEntryByPath", func() {
		for _, s := range []struct {
			path     string
			typ      git.ObjectType
			mode     git.Filemode
			matchErr types.GomegaMatcher
			treeDef  *treeDef
		}{
			{path: "0", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
			{path: "1", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "1/1", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
			{path: "2", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "2/2", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
			{path: "3", typ: git.ObjectTypeTree, mode: git.FilemodeTree, matchErr: Succeed(), treeDef: td3},
			{path: "3/1", typ: git.ObjectTypeTree, mode: git.FilemodeTree, matchErr: Succeed(), treeDef: td31},
			{path: "3/1/1", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "3/1/2", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "3/1/3", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
			{path: "3/2", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "3/3", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "3/4", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
			{path: "4", typ: git.ObjectTypeTree, mode: git.FilemodeTree, matchErr: Succeed(), treeDef: td4},
			{path: "4/1", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "4/2", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "4/3", typ: git.ObjectTypeTree, mode: git.FilemodeTree, matchErr: Succeed(), treeDef: td43},
			{path: "4/3/1", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "4/3/2", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "4/3/4", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
			{path: "4/4", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
			{path: "5", typ: git.ObjectTypeBlob, mode: git.FilemodeBlob, matchErr: Succeed()},
			{path: "6", typ: git.ObjectTypeInvalid, matchErr: HaveOccurred()},
		} {
			func(path string, typ git.ObjectType, mode git.Filemode, matchErr types.GomegaMatcher, td *treeDef) {
				Describe(path, func() {
					var te git.TreeEntry

					JustBeforeEach(func() {
						Expect(func() (err error) { te, err = t.GetEntryByPath(ctx, path); return }()).To(matchErr)
					})

					if typ != git.ObjectTypeInvalid {
						It(fmt.Sprintf("should be of type %d", typ), func() {
							Expect(te).ToNot(BeNil())
							Expect(te.EntryType()).To(Equal(typ))
							Expect(te.EntryMode()).To(Equal(mode))
						})
					} else {
						It("should fail", func() { Expect(te).To(BeNil()) })
					}

					if td != nil {
						It("should match the subtree", func() {
							Expect(getTreeDef(ctx, repo, te.EntryID())).To(PointTo(getTreeDefMatcher(td)))
						})
					}

					Describe("with expired context", func() {
						BeforeEach(func() {
							var cancelFn context.CancelFunc

							ctx, cancelFn = context.WithCancel(ctx)
							cancelFn()

							matchErr = MatchError(ctx.Err())
						})

						It("should fail", func() { Expect(te).To(BeNil()) })
					})
				})
			}(s.path, s.typ, s.mode, s.matchErr, s.treeDef)
		}
	})

	Describe("ForEachEntry", func() {
		It("should fail with expired context", func() {
			var cancelFn context.CancelFunc

			ctx, cancelFn = context.WithCancel(ctx)
			cancelFn()

			Expect(t.ForEachEntry(ctx, nil)).To(MatchError(ctx.Err()))
		})

		It("should match the subtree", func() {
			Expect(getTreeDef(ctx, repo, t.ID())).To(PointTo(getTreeDefMatcher(td)))
		})

		It("should terminate early successfully if done", func() {
			var (
				actual   []string
				expected = []string{"1", "2", "3"}
			)

			Expect(t.ForEachEntry(ctx, func(_ context.Context, te git.TreeEntry) (done bool, err error) {
				actual = append(actual, te.EntryName())
				done = te.EntryType() == git.ObjectTypeTree
				return
			})).To(Succeed())

			Expect(actual).To(Equal(expected))
		})

		It("should terminate early with error if error", func() {
			var (
				actual      []string
				expected    = []string{"1", "2", "3"}
				expectedErr = errors.New("error")
			)

			Expect(t.ForEachEntry(ctx, func(_ context.Context, te git.TreeEntry) (done bool, err error) {
				actual = append(actual, te.EntryName())
				if te.EntryType() == git.ObjectTypeTree {
					err = expectedErr
				}
				return
			})).To(MatchError(expectedErr))

			Expect(actual).To(Equal(expected))
		})
	})
})

var _ = Describe("Commit", func() {
	var (
		ctx  context.Context
		repo git.Repository

		cd0 = &commitDef{
			Message: "0",
		}
		cd01 = &commitDef{
			Message: "01",
			Tree:    treeDef{Blobs: map[string][]byte{"1": []byte("1")}},
			Parents: []commitDef{*cd0},
		}
		cd012 = &commitDef{
			Message: "012",
			Tree:    treeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
			Parents: []commitDef{*cd01},
		}
		cd013 = &commitDef{
			Message: "013",
			Tree:    treeDef{Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3")}},
			Parents: []commitDef{*cd01},
		}
		cd0123 = &commitDef{
			Message: "0123",
			Tree:    treeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3")}},
			Parents: []commitDef{*cd012, *cd013},
		}
		cd014 = &commitDef{
			Message: "014",
			Tree:    treeDef{Blobs: map[string][]byte{"1": []byte("1"), "4": []byte("4")}},
			Parents: []commitDef{*cd01},
		}
		cd01234 = &commitDef{
			Message: "01234",
			Tree:    treeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3"), "4": []byte("4")}},
			Parents: []commitDef{*cd0123, *cd014},
		}

		c git.Commit
	)

	BeforeEach(func() {
		var dir string

		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { repo, err = New().OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(repo).ToNot(BeNil())

		Expect(func() (err error) {
			var id git.ObjectID

			if id, err = createCommitFromDef(ctx, repo, cd01234); err != nil {
				return
			}

			c, err = repo.ObjectGetter().GetCommit(ctx, id)
			return
		}()).To(Succeed())
	})

	AfterEach(func() {
		if repo != nil {
			defer repo.Close()
		}

		if c != nil {
			defer c.Close()
		}
	})

	Describe("Peel", func() {
		itShouldPeel(
			func() context.Context { return ctx },
			func() git.Object { return c },
			map[git.ObjectType]idFunc{
				git.ObjectTypeInvalid: nil,
				git.ObjectTypeBlob:    nil,
				git.ObjectTypeTree:    func() git.ObjectID { return c.TreeID() },
				git.ObjectTypeCommit:  func() git.ObjectID { return c.ID() },
				git.ObjectTypeTag:     nil,
			},
		)
	})

	Describe("ForEachParentID", func() {
		It("should fail with expired context", func() {
			var cancelFn context.CancelFunc

			ctx, cancelFn = context.WithCancel(ctx)
			cancelFn()

			Expect(c.ForEachParentID(ctx, nil)).To(MatchError(ctx.Err()))
		})

		It("should match the commit definition", func() {
			var cd = getCommitDefByID(ctx, repo, c.ID())

			Expect(cd).To(PointTo(getCommitDefMatcher(cd01234)))
			Expect(cd).ToNot(PointTo(Or(
				getCommitDefMatcher(cd0),
				getCommitDefMatcher(cd01),
				getCommitDefMatcher(cd012),
				getCommitDefMatcher(cd013),
				getCommitDefMatcher(cd0123),
				getCommitDefMatcher(cd014),
			)))
		})

		It("should terminate early successfully if done", func() {
			var (
				cd  = commitDef{Message: cd01234.Message, Tree: cd01234.Tree}
				ecd = commitDef{Message: cd01234.Message, Tree: cd01234.Tree, Parents: cd01234.Parents[0:1]}
			)

			Expect(c.ForEachParentID(ctx, func(_ context.Context, id git.ObjectID) (done bool, err error) {
				cd.Parents = append(cd.Parents, *getCommitDefByID(ctx, repo, id))
				done = true
				return
			})).To(Succeed())

			Expect(cd).To(getCommitDefMatcher(&ecd))
		})

		It("should terminate early with error if error", func() {
			var (
				cd          = commitDef{Message: cd01234.Message, Tree: cd01234.Tree}
				ecd         = commitDef{Message: cd01234.Message, Tree: cd01234.Tree, Parents: cd01234.Parents[0:1]}
				expectedErr = errors.New("ForEachParentID")
			)

			Expect(c.ForEachParentID(ctx, func(_ context.Context, id git.ObjectID) (done bool, err error) {
				cd.Parents = append(cd.Parents, *getCommitDefByID(ctx, repo, id))
				err = expectedErr
				return
			})).To(MatchError(expectedErr))

			Expect(cd).To(getCommitDefMatcher(&ecd))
		})
	})

	Describe("ForEachParent", func() {
		It("should fail with expired context", func() {
			var cancelFn context.CancelFunc

			ctx, cancelFn = context.WithCancel(ctx)
			cancelFn()

			Expect(c.ForEachParent(ctx, nil)).To(MatchError(ctx.Err()))
		})

		It("should match the commit definition", func() {
			var cd = getCommitDefByCommit(ctx, repo, c)

			Expect(cd).To(PointTo(getCommitDefMatcher(cd01234)))
			Expect(cd).ToNot(PointTo(Or(
				getCommitDefMatcher(cd0),
				getCommitDefMatcher(cd01),
				getCommitDefMatcher(cd012),
				getCommitDefMatcher(cd013),
				getCommitDefMatcher(cd0123),
				getCommitDefMatcher(cd014),
			)))
		})

		It("should terminate early successfully if done", func() {
			var (
				cd  = commitDef{Message: cd01234.Message, Tree: cd01234.Tree}
				ecd = commitDef{Message: cd01234.Message, Tree: cd01234.Tree, Parents: cd01234.Parents[0:1]}
			)

			Expect(c.ForEachParent(ctx, func(_ context.Context, c git.Commit) (done bool, err error) {
				cd.Parents = append(cd.Parents, *getCommitDefByCommit(ctx, repo, c))
				done = true
				return
			})).To(Succeed())

			Expect(cd).To(getCommitDefMatcher(&ecd))
		})

		It("should terminate early with error if error", func() {
			var (
				cd          = commitDef{Message: cd01234.Message, Tree: cd01234.Tree}
				ecd         = commitDef{Message: cd01234.Message, Tree: cd01234.Tree, Parents: cd01234.Parents[0:1]}
				expectedErr = errors.New("ForEachParent")
			)

			Expect(c.ForEachParent(ctx, func(_ context.Context, c git.Commit) (done bool, err error) {
				cd.Parents = append(cd.Parents, *getCommitDefByCommit(ctx, repo, c))
				err = expectedErr
				return
			})).To(MatchError(expectedErr))

			Expect(cd).To(getCommitDefMatcher(&ecd))
		})
	})
})
