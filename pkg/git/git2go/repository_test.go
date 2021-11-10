package git2go

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
)

var _ = Describe("repository", func() {
	var (
		ctx  context.Context
		repo git.Repository
		dir  string
	)

	BeforeEach(func() {
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

		if len(dir) > 0 {
			Expect(os.RemoveAll(dir))
		}
	})

	Describe("ObjectGetter", func() {
		var (
			og                git.ObjectGetter
			id, bID, tID, cID git.ObjectID
		)

		BeforeEach(func() {
			og = repo.ObjectGetter()
			Expect(og).ToNot(BeNil())

			Expect(func() (err error) {
				bID, tID, cID, err = createEmptyObjects(ctx, repo)
				return
			}()).To(Succeed())
		})

		Describe("GetObject", func() {
			var matchObject, matchID, matchType, matchErr types.GomegaMatcher

			JustBeforeEach(func() {
				var o, err = og.GetObject(ctx, id)

				Expect(err).To(matchErr)
				Expect(o).To(matchObject)

				if o != nil {
					defer Expect(o.Close()).To(Succeed())

					Expect(o.ID()).To(matchID)
					Expect(o.Type()).To(matchType)
				}
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					cancelFn()

					matchErr = MatchError(ctx.Err())
					matchObject = BeNil()
				})

				ItShouldFail()
			})

			Describe("invalid ID", func() {
				BeforeEach(func() {
					id = git.ObjectID{}

					matchErr = HaveOccurred()
					matchObject = BeNil()
				})

				ItShouldFail()
			})

			for _, s := range []struct {
				spec string
				id   func() git.ObjectID
				typ  git.ObjectType
			}{
				{spec: "blob", id: func() git.ObjectID { return bID }, typ: git.ObjectTypeBlob},
				{spec: "tree", id: func() git.ObjectID { return tID }, typ: git.ObjectTypeTree},
				{spec: "commit", id: func() git.ObjectID { return cID }, typ: git.ObjectTypeCommit},
			} {
				func(spec string, idFn func() git.ObjectID, typ git.ObjectType) {
					Describe(spec, func() {
						BeforeEach(func() {
							var objectID = idFn()

							id = objectID

							matchErr = Succeed()
							matchObject = Not(BeNil())
							matchID = Equal(objectID)
							matchType = Equal(typ)
						})

						ItShouldSucceed()
					})
				}(s.spec, s.id, s.typ)
			}
		})

		Describe("GetBlob", func() {
			var matchObject, matchID, matchErr types.GomegaMatcher

			JustBeforeEach(func() {
				var b, err = og.GetBlob(ctx, id)

				Expect(err).To(matchErr)
				Expect(b).To(matchObject)

				if b != nil {
					defer Expect(b.Close()).To(Succeed())

					Expect(b.ID()).To(matchID)
					Expect(b.Type()).To(Equal(git.ObjectTypeBlob))
				}
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					cancelFn()

					matchErr = MatchError(ctx.Err())
					matchObject = BeNil()
				})

				ItShouldFail()
			})

			Describe("invalid ID", func() {
				for _, s := range []struct {
					spec string
					id   func() git.ObjectID
				}{
					{spec: "invalid", id: func() git.ObjectID { return git.ObjectID{} }},
					{spec: "tree", id: func() git.ObjectID { return tID }},
					{spec: "commit", id: func() git.ObjectID { return cID }},
				} {
					func(spec string, idFn func() git.ObjectID) {
						Describe(spec, func() {
							BeforeEach(func() {
								id = idFn()

								matchErr = HaveOccurred()
								matchObject = BeNil()
							})

							ItShouldFail()
						})
					}(s.spec, s.id)
				}
			})

			Describe("content", func() {
				Describe("empty content", func() {
					BeforeEach(func() {
						id = bID

						matchErr = Succeed()
						matchObject = Not(BeNil())
						matchID = Equal(bID)
					})

					ItShouldSucceed()
				})

				Describe("non-empty content", func() {
					BeforeEach(func() {
						var content = []byte("content")

						Expect(func() (err error) {
							bID, err = CreateBlob(ctx, repo, content)
							return
						}()).To((Succeed()))

						id = bID

						matchErr = Succeed()
						matchObject = Not(BeNil())
						matchID = Equal(bID)
					})

					ItShouldSucceed()
				})
			})
		})

		Describe("GetTree", func() {
			var matchObject, matchID, matchErr types.GomegaMatcher

			JustBeforeEach(func() {
				var t, err = og.GetTree(ctx, id)

				Expect(err).To(matchErr)
				Expect(t).To(matchObject)

				if t != nil {
					defer Expect(t.Close()).To(Succeed())
					Expect(t.ID()).To(matchID)
					Expect(t.Type()).To(Equal(git.ObjectTypeTree))
				}
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					cancelFn()

					matchErr = MatchError(ctx.Err())
					matchObject = BeNil()
				})

				ItShouldFail()
			})

			Describe("invalid ID", func() {
				for _, s := range []struct {
					spec string
					id   func() git.ObjectID
				}{
					{spec: "invalid", id: func() git.ObjectID { return git.ObjectID{} }},
					{spec: "blob", id: func() git.ObjectID { return bID }},
					{spec: "commit", id: func() git.ObjectID { return cID }},
				} {
					func(spec string, idFn func() git.ObjectID) {
						Describe(spec, func() {
							BeforeEach(func() {
								id = idFn()

								matchErr = HaveOccurred()
								matchObject = BeNil()
							})

							ItShouldFail()
						})
					}(s.spec, s.id)
				}
			})

			Describe("tree entries", func() {
				Describe("no entries", func() {
					BeforeEach(func() {
						id = tID

						matchErr = Succeed()
						matchObject = Not(BeNil())
						matchID = Equal(tID)
					})

					ItShouldSucceed()
				})

				Describe("some entries", func() {
					BeforeEach(func() {
						Expect(func() (err error) {
							tID, err = CreateTreeFromDef(ctx, repo, &TreeDef{Blobs: map[string][]byte{"entry": []byte("entry")}})
							return
						}()).To((Succeed()))

						id = tID

						matchErr = Succeed()
						matchObject = Not(BeNil())
						matchID = Equal(tID)
					})

					ItShouldSucceed()
				})
			})
		})

		Describe("GetCommit", func() {
			var matchObject, matchID, matchErr types.GomegaMatcher

			JustBeforeEach(func() {
				var c, err = og.GetCommit(ctx, id)

				Expect(err).To(matchErr)
				Expect(c).To(matchObject)

				if c != nil {
					defer Expect(c.Close()).To(Succeed())

					Expect(c.ID()).To(matchID)
					Expect(c.Type()).To(Equal(git.ObjectTypeCommit))
				}
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					cancelFn()

					matchErr = MatchError(ctx.Err())
					matchObject = BeNil()
				})

				ItShouldFail()
			})

			Describe("invalid ID", func() {
				for _, s := range []struct {
					spec string
					id   func() git.ObjectID
				}{
					{spec: "invalid", id: func() git.ObjectID { return git.ObjectID{} }},
					{spec: "blob", id: func() git.ObjectID { return bID }},
					{spec: "tree", id: func() git.ObjectID { return tID }},
				} {
					func(spec string, idFn func() git.ObjectID) {
						Describe(spec, func() {
							BeforeEach(func() {
								id = idFn()

								matchErr = HaveOccurred()
								matchObject = BeNil()
							})

							ItShouldFail()
						})
					}(s.spec, s.id)
				}
			})

			Describe("commit ID", func() {
				BeforeEach(func() {
					id = cID

					matchErr = Succeed()
					matchObject = Not(BeNil())
					matchID = Equal(cID)
				})

				ItShouldSucceed()
			})
		})
	})

	Describe("ObjectConverter", func() {
		var (
			oc            git.ObjectConverter
			bID, tID, cID git.ObjectID
		)

		BeforeEach(func() {
			oc = repo.ObjectConverter()
			Expect(oc).ToNot(BeNil())

			Expect(func() (err error) {
				bID, tID, cID, err = createEmptyObjects(ctx, repo)
				return
			}()).To(Succeed())
		})

		Describe("ToBlob", func() {
			var (
				objectFn                     func() (git.Object, error)
				matchBlob, matchID, matchErr types.GomegaMatcher
			)

			JustBeforeEach(func() {
				var (
					o   git.Object
					b   git.Blob
					err error
				)

				Expect(func() (err error) { o, err = objectFn(); return }()).To(Succeed())

				b, err = oc.ToBlob(ctx, o)

				if o == nil {
					return
				}

				defer Expect(o.Close()).To(Succeed())

				Expect(err).To(matchErr)
				Expect(b).To(matchBlob)

				if b != nil {
					defer b.Close()

					Expect(b.ID()).To(matchID)
					Expect(b.Type()).To(Equal(git.ObjectTypeBlob))
				}
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					defer cancelFn()

					objectFn = func() (git.Object, error) { return nil, nil }

					matchErr = MatchError(ctx.Err())
					matchBlob = BeNil()
				})

				ItShouldFail()
			})

			Describe("invalid object", func() {
				for _, s := range []struct {
					spec string
					id   func() git.ObjectID
				}{
					{spec: "tree", id: func() git.ObjectID { return tID }},
					{spec: "commit", id: func() git.ObjectID { return cID }},
				} {
					func(spec string, idFn func() git.ObjectID) {
						Describe(spec, func() {
							BeforeEach(func() {
								objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, idFn()) }

								matchErr = HaveOccurred()
								matchBlob = BeNil()
							})

							ItShouldFail()
						})
					}(s.spec, s.id)
				}
			})

			Describe("content", func() {
				Describe("empty content", func() {
					BeforeEach(func() {
						objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, bID) }

						matchErr = Succeed()
						matchBlob = Not(BeNil())
						matchID = Equal(bID)
					})

					ItShouldSucceed()
				})

				Describe("non-empty content", func() {
					BeforeEach(func() {
						var content = []byte("content")

						Expect(func() (err error) {
							bID, err = CreateBlob(ctx, repo, content)
							return
						}()).To((Succeed()))

						objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, bID) }

						matchErr = Succeed()
						matchBlob = Not(BeNil())
						matchID = Equal(bID)
					})

					ItShouldSucceed()
				})
			})
		})

		Describe("ToTree", func() {
			var (
				objectFn                     func() (git.Object, error)
				matchTree, matchID, matchErr types.GomegaMatcher
			)

			JustBeforeEach(func() {
				var (
					o   git.Object
					t   git.Tree
					err error
				)

				Expect(func() (err error) { o, err = objectFn(); return }()).To(Succeed())

				t, err = oc.ToTree(ctx, o)

				if o == nil {
					return
				}

				defer Expect(o.Close()).To(Succeed())

				Expect(err).To(matchErr)
				Expect(t).To(matchTree)

				if t != nil {
					defer t.Close()

					Expect(t.ID()).To(matchID)
					Expect(t.Type()).To(Equal(git.ObjectTypeTree))
				}
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					defer cancelFn()

					objectFn = func() (git.Object, error) { return nil, nil }

					matchErr = MatchError(ctx.Err())
					matchTree = BeNil()
				})

				ItShouldFail()
			})

			Describe("invalid object", func() {
				for _, s := range []struct {
					spec string
					id   func() git.ObjectID
				}{
					{spec: "blob", id: func() git.ObjectID { return bID }},
					{spec: "commit", id: func() git.ObjectID { return cID }},
				} {
					func(spec string, idFn func() git.ObjectID) {
						Describe(spec, func() {
							BeforeEach(func() {
								objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, idFn()) }

								matchErr = HaveOccurred()
								matchTree = BeNil()
							})

							ItShouldFail()
						})
					}(s.spec, s.id)
				}
			})

			Describe("tree entries", func() {
				Describe("no entries", func() {
					BeforeEach(func() {
						objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, tID) }

						matchErr = Succeed()
						matchTree = Not(BeNil())
						matchID = Equal(tID)
					})

					ItShouldSucceed()
				})

				Describe("some entries", func() {
					BeforeEach(func() {
						Expect(func() (err error) {
							tID, err = CreateTreeFromDef(ctx, repo, &TreeDef{Blobs: map[string][]byte{"entry": []byte("entry")}})
							return
						}()).To((Succeed()))

						objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, tID) }

						matchErr = Succeed()
						matchTree = Not(BeNil())
						matchID = Equal(tID)
					})

					ItShouldSucceed()
				})
			})
		})

		Describe("ToCommit", func() {
			var (
				objectFn                       func() (git.Object, error)
				matchCommit, matchID, matchErr types.GomegaMatcher
			)

			JustBeforeEach(func() {
				var (
					o   git.Object
					c   git.Commit
					err error
				)

				Expect(func() (err error) { o, err = objectFn(); return }()).To(Succeed())

				c, err = oc.ToCommit(ctx, o)

				if o == nil {
					return
				}

				defer Expect(o.Close()).To(Succeed())

				Expect(err).To(matchErr)
				Expect(c).To(matchCommit)

				if c != nil {
					defer c.Close()

					Expect(c.ID()).To(matchID)
					Expect(c.Type()).To(Equal(git.ObjectTypeCommit))
				}
			})

			Describe("expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					defer cancelFn()

					objectFn = func() (git.Object, error) { return nil, nil }

					matchErr = MatchError(ctx.Err())
					matchCommit = BeNil()
				})

				ItShouldFail()
			})

			Describe("invalid object", func() {
				for _, s := range []struct {
					spec string
					id   func() git.ObjectID
				}{
					{spec: "blob", id: func() git.ObjectID { return bID }},
					{spec: "tree", id: func() git.ObjectID { return tID }},
				} {
					func(spec string, idFn func() git.ObjectID) {
						Describe(spec, func() {
							BeforeEach(func() {
								objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, idFn()) }

								matchErr = HaveOccurred()
								matchCommit = BeNil()
							})

							ItShouldFail()
						})
					}(s.spec, s.id)
				}
			})

			Describe("commit", func() {
				BeforeEach(func() {
					objectFn = func() (git.Object, error) { return repo.ObjectGetter().GetObject(ctx, cID) }

					matchErr = Succeed()
					matchCommit = Not(BeNil())
					matchID = Equal(cID)
				})

				ItShouldSucceed()
			})
		})
	})

	Describe("Peeler", func() {
		type peelableFunc func() git.Peelable

		var (
			peeler git.Peeler
			b      git.Blob
			t      git.Tree
			c      git.Commit

			blobID   = func() git.ObjectID { return b.ID() }
			treeID   = func() git.ObjectID { return t.ID() }
			commitID = func() git.ObjectID { return c.ID() }
		)

		BeforeEach(func() {
			Expect(func() git.Peeler { peeler = repo.Peeler(); return peeler }()).ToNot(BeNil())
			Expect(func() (err error) {
				var bID, tID, cID git.ObjectID

				if bID, tID, cID, err = createEmptyObjects(ctx, repo); err != nil {
					return
				}

				if b, err = repo.ObjectGetter().GetBlob(ctx, bID); err != nil {
					return
				}

				if t, err = repo.ObjectGetter().GetTree(ctx, tID); err != nil {
					return
				}

				c, err = repo.ObjectGetter().GetCommit(ctx, cID)
				return
			}()).To(Succeed())
		})

		AfterEach(func() {
			if b != nil {
				defer b.Close()
			}

			if t != nil {
				defer t.Close()
			}

			if c != nil {
				defer c.Close()
			}
		})

		for _, s := range []struct {
			spec       string
			peelableFn peelableFunc
			checks     map[git.ObjectType]idFunc
		}{
			{
				spec:       "blob",
				peelableFn: func() git.Peelable { return b },
				checks: map[git.ObjectType]idFunc{
					git.ObjectTypeInvalid: nil,
					git.ObjectTypeBlob:    blobID,
					git.ObjectTypeTree:    nil,
					git.ObjectTypeCommit:  nil,
				},
			},
			{
				spec:       "tree",
				peelableFn: func() git.Peelable { return t },
				checks: map[git.ObjectType]idFunc{
					git.ObjectTypeInvalid: nil,
					git.ObjectTypeBlob:    nil,
					git.ObjectTypeTree:    treeID,
					git.ObjectTypeCommit:  nil,
				},
			},
			{
				spec:       "commit",
				peelableFn: func() git.Peelable { return c },
				checks: map[git.ObjectType]idFunc{
					git.ObjectTypeInvalid: nil,
					git.ObjectTypeBlob:    nil,
					git.ObjectTypeTree:    treeID,
					git.ObjectTypeCommit:  commitID,
				},
			},
		} {
			func(spec string, peelableFn peelableFunc, checks map[git.ObjectType]idFunc) {
				Describe(spec, func() {
					var (
						p                        git.Peelable
						unsupportedObjectTypeErr = fmt.Errorf("unsupported object type %d", t)
						peelTo                   = func(ctx context.Context, peeler git.Peeler, typ git.ObjectType) (o git.Object, err error) {
							switch typ {
							case git.ObjectTypeBlob:
								o, err = peeler.PeelToBlob(ctx, p)
							case git.ObjectTypeTree:
								o, err = peeler.PeelToTree(ctx, p)
							case git.ObjectTypeCommit:
								o, err = peeler.PeelToCommit(ctx, p)
							default:
								err = unsupportedObjectTypeErr
							}

							return
						}
					)

					BeforeEach(func() {
						p = peelableFn()
					})

					for t, idFn := range checks {
						func(t git.ObjectType, idFn idFunc) {
							var spec string

							switch t {
							case git.ObjectTypeBlob:
								spec = "PeelToBlob"
							case git.ObjectTypeTree:
								spec = "PeelToTree"
							case git.ObjectTypeCommit:
								spec = "PeelToCommit"
							}

							It(spec, func() {
								var (
									o        git.Object
									matchErr = Succeed()
								)

								if idFn == nil {
									matchErr = HaveOccurred()
								}

								defer func() {
									if o != nil {
										defer o.Close()
									}
								}()

								Expect(func() (err error) {
									o, err = peelTo(ctx, peeler, t)
									return
								}()).To(matchErr)

								if idFn != nil {
									Expect(o).ToNot(BeNil())
									Expect(o.ID()).To(Equal(idFn()))
									Expect(o.Type()).To(Equal(t))
								} else {
									Expect(o).To(BeNil())
								}
							})

							It(fmt.Sprintf("%s should fail with expired context", spec), func() {
								var (
									cancelFn context.CancelFunc
									o        git.Object
								)

								ctx, cancelFn = context.WithCancel(ctx)
								cancelFn()

								Expect(func() (err error) {
									o, err = peelTo(ctx, peeler, t)
									return
								}()).To(Or(MatchError(ctx.Err()), MatchError(unsupportedObjectTypeErr)))

								Expect(o).To(BeNil())
							})
						}(t, idFn)
					}
				})
			}(s.spec, s.peelableFn, s.checks)
		}
	})

	Describe("TreeWalker", func() {
		var (
			tw git.TreeWalker
			t  git.Tree

			td = &TreeDef{
				Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "5": []byte("5")},
				Subtrees: map[string]TreeDef{
					"3": {
						Blobs:    map[string][]byte{"2": []byte("2"), "3": []byte("3")},
						Subtrees: map[string]TreeDef{"1": {Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}},
					},
					"4": {
						Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2")},
						Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}},
					},
				},
			}
		)

		BeforeEach(func() {
			Expect(func() (err error) {
				var id git.ObjectID

				if id, err = CreateTreeFromDef(ctx, repo, td); err != nil {
					return
				}

				t, err = repo.ObjectGetter().GetTree(ctx, id)
				return
			}())

			Expect(t).ToNot(BeNil())
			Expect(GetTreeDef(ctx, repo, t.ID())).To(PointTo(GetTreeDefMatcher(td)))

			tw = repo.TreeWalker()
			Expect(tw).ToNot(BeNil())
		})

		AfterEach(func() {
			if t != nil {
				defer t.Close()
			}

			if tw != nil {
				Expect(tw.Close()).To(Succeed())
			}
		})

		It("should fail with expired context", func() {
			var cancelFn context.CancelFunc

			ctx, cancelFn = context.WithCancel(ctx)
			cancelFn()

			Expect(tw.ForEachTreeEntry(ctx, nil, nil)).To(MatchError(ctx.Err()))
		})

		type check struct {
			path string
			typ  git.ObjectType
		}

		for _, s := range []struct {
			spec       string
			receiverFn git.TreeWalkerReceiverFunc
			matchErr   types.GomegaMatcher
			checks     []check
		}{
			{
				spec: "should skip all entries with error if error on first entry",
				receiverFn: func(_ context.Context, _ string, _ git.TreeEntry) (bool, bool, error) {
					return false, false, errors.New("first error")
				},
				matchErr: MatchError("first error"),
			},
			{
				spec:       "should visit only one entry if done on first entry",
				receiverFn: func(_ context.Context, _ string, _ git.TreeEntry) (done, skip bool, err error) { done = true; return },
				matchErr:   Succeed(),
				checks:     []check{{path: "1", typ: git.ObjectTypeBlob}},
			},
			{
				spec: "should visit entries until done",
				receiverFn: func(_ context.Context, parentPath string, te git.TreeEntry) (done, skip bool, err error) {
					done = path.Join(parentPath, te.EntryName()) == "4/3/1"
					return
				},
				matchErr: Succeed(),
				checks: []check{
					{path: "1", typ: git.ObjectTypeBlob},
					{path: "2", typ: git.ObjectTypeBlob},
					{path: "3", typ: git.ObjectTypeTree},
					{path: "3/1", typ: git.ObjectTypeTree},
					{path: "3/1/1", typ: git.ObjectTypeBlob},
					{path: "3/1/2", typ: git.ObjectTypeBlob},
					{path: "3/2", typ: git.ObjectTypeBlob},
					{path: "3/3", typ: git.ObjectTypeBlob},
					{path: "4", typ: git.ObjectTypeTree},
					{path: "4/1", typ: git.ObjectTypeBlob},
					{path: "4/2", typ: git.ObjectTypeBlob},
					{path: "4/3", typ: git.ObjectTypeTree},
					{path: "4/3/1", typ: git.ObjectTypeBlob},
				},
			},
			{
				spec: "should skip subtrees",
				receiverFn: func(_ context.Context, _ string, te git.TreeEntry) (done, skip bool, err error) {
					skip = te.EntryType() == git.ObjectTypeTree
					return
				},
				matchErr: Succeed(),
				checks: []check{
					{path: "1", typ: git.ObjectTypeBlob},
					{path: "2", typ: git.ObjectTypeBlob},
					{path: "5", typ: git.ObjectTypeBlob},
				},
			},
			{
				spec: "should skip a specific subtree",
				receiverFn: func(_ context.Context, parentPath string, te git.TreeEntry) (done, skip bool, err error) {
					skip = path.Join(parentPath, te.EntryName()) == "3/1"
					return
				},
				matchErr: Succeed(),
				checks: []check{
					{path: "1", typ: git.ObjectTypeBlob},
					{path: "2", typ: git.ObjectTypeBlob},
					{path: "3", typ: git.ObjectTypeTree},
					{path: "3/2", typ: git.ObjectTypeBlob},
					{path: "3/3", typ: git.ObjectTypeBlob},
					{path: "4", typ: git.ObjectTypeTree},
					{path: "4/1", typ: git.ObjectTypeBlob},
					{path: "4/2", typ: git.ObjectTypeBlob},
					{path: "4/3", typ: git.ObjectTypeTree},
					{path: "4/3/1", typ: git.ObjectTypeBlob},
					{path: "4/3/2", typ: git.ObjectTypeBlob},
					{path: "5", typ: git.ObjectTypeBlob},
				},
			},
			{
				spec:       "should visit all entries in pre-order",
				receiverFn: func(_ context.Context, _ string, _ git.TreeEntry) (done, skip bool, err error) { return },
				matchErr:   Succeed(),
				checks: []check{
					{path: "1", typ: git.ObjectTypeBlob},
					{path: "2", typ: git.ObjectTypeBlob},
					{path: "3", typ: git.ObjectTypeTree},
					{path: "3/1", typ: git.ObjectTypeTree},
					{path: "3/1/1", typ: git.ObjectTypeBlob},
					{path: "3/1/2", typ: git.ObjectTypeBlob},
					{path: "3/2", typ: git.ObjectTypeBlob},
					{path: "3/3", typ: git.ObjectTypeBlob},
					{path: "4", typ: git.ObjectTypeTree},
					{path: "4/1", typ: git.ObjectTypeBlob},
					{path: "4/2", typ: git.ObjectTypeBlob},
					{path: "4/3", typ: git.ObjectTypeTree},
					{path: "4/3/1", typ: git.ObjectTypeBlob},
					{path: "4/3/2", typ: git.ObjectTypeBlob},
					{path: "5", typ: git.ObjectTypeBlob},
				},
			},
		} {
			func(spec string, receiverFn git.TreeWalkerReceiverFunc, matchErr types.GomegaMatcher, checks []check) {
				It(spec, func() {
					var actuals []check

					Expect(tw.ForEachTreeEntry(
						ctx,
						t,
						func(ctx context.Context, parentPath string, te git.TreeEntry) (done, skip bool, err error) {
							if done, skip, err = receiverFn(ctx, parentPath, te); err == nil && !skip {
								actuals = append(actuals, check{path: path.Join(parentPath, te.EntryName()), typ: te.EntryType()})
							}

							return
						},
					)).To(matchErr)

					Expect(actuals).To(Equal(checks))
				})
			}(s.spec, s.receiverFn, s.matchErr, s.checks)
		}
	})

	Describe("CommitWalker", func() {
		var (
			cw git.CommitWalker
			c  git.Commit

			cd0 = &CommitDef{Message: "0"}

			cd01 = &CommitDef{
				Message: "01",
				Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
				Parents: []CommitDef{*cd0},
			}
			cd012 = &CommitDef{
				Message: "012",
				Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
				Parents: []CommitDef{*cd01},
			}

			cd013 = &CommitDef{
				Message: "013",
				Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3")}},
				Parents: []CommitDef{*cd01},
			}
			cd0123 = &CommitDef{
				Message: "0123",
				Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3")}},
				Parents: []CommitDef{*cd012, *cd013},
			}
			cd014 = &CommitDef{
				Message: "014",
				Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "4": []byte("4")}},
				Parents: []CommitDef{*cd01},
			}

			cd01234 = &CommitDef{
				Message: "01234",
				Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3"), "4": []byte("4")}},
				Parents: []CommitDef{*cd0123, *cd014},
			}
		)

		BeforeEach(func() {
			Expect(func() (err error) {
				var id git.ObjectID

				if id, err = CreateCommitFromDef(ctx, repo, cd01234); err != nil {
					return
				}

				c, err = repo.ObjectGetter().GetCommit(ctx, id)
				return
			}()).To(Succeed())

			Expect(c).ToNot(BeNil())
			Expect(GetCommitDefByCommit(ctx, repo, c)).To(PointTo(GetCommitDefMatcher(cd01234)))

			cw = repo.CommitWalker()
			Expect(cw).ToNot(BeNil())
		})

		AfterEach(func() {
			if c != nil {
				defer c.Close()
			}

			if cw != nil {
				Expect(cw.Close()).To(Succeed())
			}
		})

		It("should fail with expired context", func() {
			var cancelFn context.CancelFunc

			ctx, cancelFn = context.WithCancel(ctx)
			cancelFn()

			Expect(cw.ForEachCommit(ctx, nil, nil)).To(MatchError(ctx.Err()))
		})

		for _, s := range []struct {
			spec            string
			receiverFn      git.CommitWalkerReceiverFunc
			matchErr        types.GomegaMatcher
			matchCommitDefs []types.GomegaMatcher
		}{
			{
				spec: "should skip all commits with error if error on first commit",
				receiverFn: func(_ context.Context, _ git.Commit) (bool, bool, error) {
					return false, false, errors.New("first error")
				},
				matchErr: MatchError("first error"),
			},
			{
				spec: "should skip all commits after error",
				receiverFn: func(_ context.Context, c git.Commit) (done, skip bool, err error) {
					if c.Message() == "013" {
						err = errors.New("013")
					}
					return
				},
				matchErr: MatchError("013"),
				matchCommitDefs: []types.GomegaMatcher{
					GetCommitDefMatcher(cd01234),
					GetCommitDefMatcher(cd0123),
					GetCommitDefMatcher(cd012),
					GetCommitDefMatcher(cd01),
					GetCommitDefMatcher(cd0),
				},
			},
			{
				spec:            "should visit only latest commit if done on first commit",
				receiverFn:      func(_ context.Context, _ git.Commit) (done, skip bool, err error) { done = true; return },
				matchErr:        Succeed(),
				matchCommitDefs: []types.GomegaMatcher{GetCommitDefMatcher(cd01234)},
			},
			{
				spec: "should visit commits until done",
				receiverFn: func(_ context.Context, c git.Commit) (done, skip bool, err error) {
					done = c.Message() == "014"
					return
				},
				matchErr: Succeed(),
				matchCommitDefs: []types.GomegaMatcher{
					GetCommitDefMatcher(cd01234),
					GetCommitDefMatcher(cd0123),
					GetCommitDefMatcher(cd012),
					GetCommitDefMatcher(cd01),
					GetCommitDefMatcher(cd0),
					GetCommitDefMatcher(cd013),
					GetCommitDefMatcher(cd01),
					GetCommitDefMatcher(cd0),
					GetCommitDefMatcher(cd014),
				},
			},
			{
				spec: "should skip specific commits",
				receiverFn: func(_ context.Context, c git.Commit) (done, skip bool, err error) {
					skip = c.Message() == "0123"
					return
				},
				matchErr: Succeed(),
				matchCommitDefs: []types.GomegaMatcher{
					GetCommitDefMatcher(cd01234),
					GetCommitDefMatcher(cd014),
					GetCommitDefMatcher(cd01),
					GetCommitDefMatcher(cd0),
				},
			},
			{
				spec:       "should visit all commits in pre-order",
				receiverFn: func(_ context.Context, _ git.Commit) (done, skip bool, err error) { return },
				matchErr:   Succeed(),
				matchCommitDefs: []types.GomegaMatcher{
					GetCommitDefMatcher(cd01234),
					GetCommitDefMatcher(cd0123),
					GetCommitDefMatcher(cd012),
					GetCommitDefMatcher(cd01),
					GetCommitDefMatcher(cd0),
					GetCommitDefMatcher(cd013),
					GetCommitDefMatcher(cd01),
					GetCommitDefMatcher(cd0),
					GetCommitDefMatcher(cd014),
					GetCommitDefMatcher(cd01),
					GetCommitDefMatcher(cd0),
				},
			},
		} {
			func(spec string, receiverFn git.CommitWalkerReceiverFunc, matchErr types.GomegaMatcher, matchCommitDefs []types.GomegaMatcher) {
				It(spec, func() {
					var actuals []CommitDef

					Expect(cw.ForEachCommit(
						ctx,
						c,
						func(ctx context.Context, c git.Commit) (done, skip bool, err error) {
							if done, skip, err = receiverFn(ctx, c); err == nil && !skip {
								actuals = append(actuals, *GetCommitDefByCommit(ctx, repo, c))
							}

							return
						},
					)).To(matchErr)

					Expect(actuals).To(HaveLen(len(matchCommitDefs)))

					for i := range actuals {
						Expect(actuals[i]).To(matchCommitDefs[i])
					}
				})
			}(s.spec, s.receiverFn, s.matchErr, s.matchCommitDefs)
		}
	})

	Describe("TreeBuilderFromTree", func() {
		var (
			tb git.TreeBuilder
			t  git.Tree

			td = &TreeDef{
				Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "5": []byte("5")},
				Subtrees: map[string]TreeDef{
					"3": {Blobs: map[string][]byte{"1": []byte("1")}},
					"4": {Blobs: map[string][]byte{"1": []byte("1")}},
				},
			}
		)

		BeforeEach(func() {
			Expect(func() (err error) {
				var id git.ObjectID

				if id, err = CreateTreeFromDef(ctx, repo, td); err != nil {
					return
				}

				t, err = repo.ObjectGetter().GetTree(ctx, id)
				return
			}()).To(Succeed())

			Expect(t).ToNot(BeNil())
			Expect(GetTreeDef(ctx, repo, t.ID())).To(PointTo(GetTreeDefMatcher(td)))
		})

		AfterEach(func() {
			if t != nil {
				defer t.Close()
			}

			if tb != nil {
				Expect(tb.Close()).To(Succeed())
			}
		})

		It("should fail with expired context", func() {
			var cancelFn context.CancelFunc

			ctx, cancelFn = context.WithCancel(ctx)
			cancelFn()

			Expect(func() (err error) {
				tb, err = repo.TreeBuilderFromTree(ctx, t)
				return
			}()).To(MatchError(ctx.Err()))

			Expect(tb).To(BeNil())
		})

		Describe("with valid context", func() {
			BeforeEach(func() {
				Expect(func() (err error) {
					tb, err = repo.TreeBuilderFromTree(ctx, t)
					return
				}()).To(Succeed())
			})

			Describe("RemoveEntry", func() {
				for _, s := range []struct {
					entryNames             []string
					matchErr, matchTreeDef types.GomegaMatcher
				}{
					{
						matchErr:     Succeed(),
						matchTreeDef: GetTreeDefMatcher(td),
					},
					{
						entryNames:   []string{"0"},
						matchErr:     HaveOccurred(),
						matchTreeDef: GetTreeDefMatcher(td),
					},
					{
						entryNames: []string{"1"},
						matchErr:   Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"2": []byte("2"), "5": []byte("5")},
							Subtrees: td.Subtrees,
						}),
					},
					{
						entryNames: []string{"3"},
						matchErr:   Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    td.Blobs,
							Subtrees: map[string]TreeDef{"4": td.Subtrees["4"]},
						}),
					},
					{
						entryNames: []string{"2", "4"},
						matchErr:   Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1"), "5": []byte("5")},
							Subtrees: map[string]TreeDef{"3": td.Subtrees["3"]},
						}),
					},
				} {
					func(entryNames []string, matchErr, matchTreeDef types.GomegaMatcher) {
						It(fmt.Sprintf("%v", entryNames), func() {
							for _, entryName := range entryNames {
								Expect(tb.RemoveEntry(entryName)).To(matchErr)
							}

							Expect(func() (td TreeDef, err error) {
								var id git.ObjectID

								if id, err = tb.Build(ctx); err != nil {
									return
								}

								td = *GetTreeDef(ctx, repo, id)
								return
							}()).To(matchTreeDef)
						})
					}(s.entryNames, s.matchErr, s.matchTreeDef)
				}
			})

			Describe("AddEntry", func() {
				for _, s := range []struct {
					spec                   string
					td                     TreeDef
					matchErr, matchTreeDef types.GomegaMatcher
				}{
					{
						spec:         "nothing",
						matchErr:     Succeed(),
						matchTreeDef: GetTreeDefMatcher(td),
					},
					{
						spec:     "new blob entry",
						td:       TreeDef{Blobs: map[string][]byte{"6": []byte("6")}},
						matchErr: Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2"), "5": []byte("5"), "6": []byte("6")},
							Subtrees: td.Subtrees,
						}),
					},
					{
						spec:     "replace existing blob entry",
						td:       TreeDef{Blobs: map[string][]byte{"1": []byte("11")}},
						matchErr: Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("11"), "2": []byte("2"), "5": []byte("5")},
							Subtrees: td.Subtrees,
						}),
					},
					{
						spec:     "replace existing tree entry with a blob entry",
						td:       TreeDef{Blobs: map[string][]byte{"3": []byte("3")}},
						matchErr: Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3"), "5": []byte("5")},
							Subtrees: map[string]TreeDef{"4": td.Subtrees["4"]},
						}),
					},
					{
						spec:     "new tree entry",
						td:       TreeDef{Subtrees: map[string]TreeDef{"6": td.Subtrees["3"]}},
						matchErr: Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    td.Blobs,
							Subtrees: map[string]TreeDef{"3": td.Subtrees["3"], "4": td.Subtrees["4"], "6": td.Subtrees["3"]},
						}),
					},
					{
						spec:     "replace existing tree entry",
						td:       TreeDef{Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}}},
						matchErr: Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    td.Blobs,
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}, "4": td.Subtrees["4"]},
						}),
					},
					{
						spec:     "replace existing blob entry with a tree entry",
						td:       TreeDef{Subtrees: map[string]TreeDef{"2": {Blobs: map[string][]byte{"2": []byte("2")}}}},
						matchErr: Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "5": []byte("5")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2")}},
								"3": td.Subtrees["3"],
								"4": td.Subtrees["4"],
							},
						}),
					},
					{
						spec: "new and replace multiple entries",
						td: TreeDef{
							Blobs: map[string][]byte{"1": []byte("11"), "3": []byte("3"), "6": []byte("6")},
							Subtrees: map[string]TreeDef{
								"2": td.Subtrees["3"],
								"4": {Blobs: map[string][]byte{"4": []byte("4")}},
								"7": td.Subtrees["3"],
							},
						},
						matchErr: Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("11"), "3": []byte("3"), "5": []byte("5"), "6": []byte("6")},
							Subtrees: map[string]TreeDef{
								"2": td.Subtrees["3"],
								"4": {Blobs: map[string][]byte{"4": []byte("4")}},
								"7": td.Subtrees["3"],
							},
						}),
					},
				} {
					func(spec string, td TreeDef, matchErr, matchTreeDef types.GomegaMatcher) {
						It(spec, func() {
							for name, content := range td.Blobs {
								var id git.ObjectID

								Expect(func() (err error) { id, err = CreateBlob(ctx, repo, content); return }()).To(Succeed())

								Expect(tb.AddEntry(name, id, git.FilemodeBlob)).To(matchErr)
							}

							for name, td := range td.Subtrees {
								var id git.ObjectID

								Expect(func() (err error) { id, err = CreateTreeFromDef(ctx, repo, &td); return }()).To(Succeed())

								Expect(tb.AddEntry(name, id, git.FilemodeTree)).To(matchErr)
							}

							Expect(func() (td TreeDef, err error) {
								var id git.ObjectID

								if id, err = tb.Build(ctx); err != nil {
									return
								}

								td = *GetTreeDef(ctx, repo, id)
								return
							}()).To(matchTreeDef)
						})
					}(s.spec, s.td, s.matchErr, s.matchTreeDef)
				}
			})

			Describe("RemoveEntry followed by AddEntry", func() {
				for _, s := range []struct {
					spec                   string
					removeEntryNames       []string
					td                     TreeDef
					matchErr, matchTreeDef types.GomegaMatcher
				}{
					{
						spec:             "new blob entry",
						removeEntryNames: []string{"5"},
						td:               TreeDef{Blobs: map[string][]byte{"6": []byte("6")}},
						matchErr:         Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2"), "6": []byte("6")},
							Subtrees: td.Subtrees,
						}),
					},
					{
						spec:             "replace existing blob entry",
						removeEntryNames: []string{"1"},
						td:               TreeDef{Blobs: map[string][]byte{"1": []byte("11")}},
						matchErr:         Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("11"), "2": []byte("2"), "5": []byte("5")},
							Subtrees: td.Subtrees,
						}),
					},
					{
						spec:             "replace existing tree entry with a blob entry",
						removeEntryNames: []string{"3"},
						td:               TreeDef{Blobs: map[string][]byte{"3": []byte("3")}},
						matchErr:         Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3"), "5": []byte("5")},
							Subtrees: map[string]TreeDef{"4": td.Subtrees["4"]},
						}),
					},
					{
						spec:             "new tree entry",
						removeEntryNames: []string{"4"},
						td:               TreeDef{Subtrees: map[string]TreeDef{"6": td.Subtrees["3"]}},
						matchErr:         Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    td.Blobs,
							Subtrees: map[string]TreeDef{"3": td.Subtrees["3"], "6": td.Subtrees["3"]},
						}),
					},
					{
						spec:             "replace existing tree entry",
						removeEntryNames: []string{"3"},
						td:               TreeDef{Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}}},
						matchErr:         Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs:    td.Blobs,
							Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}, "4": td.Subtrees["4"]},
						}),
					},
					{
						spec:             "replace existing blob entry with a tree entry",
						removeEntryNames: []string{"2"},
						td:               TreeDef{Subtrees: map[string]TreeDef{"2": {Blobs: map[string][]byte{"2": []byte("2")}}}},
						matchErr:         Succeed(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{
							Blobs: map[string][]byte{"1": []byte("1"), "5": []byte("5")},
							Subtrees: map[string]TreeDef{
								"2": {Blobs: map[string][]byte{"2": []byte("2")}},
								"3": td.Subtrees["3"],
								"4": td.Subtrees["4"],
							},
						}),
					},
				} {
					func(spec string, removeEntryNames []string, td TreeDef, matchErr, matchTreeDef types.GomegaMatcher) {
						It(spec, func() {
							for _, entryName := range removeEntryNames {
								Expect(tb.RemoveEntry(entryName)).To(matchErr)
							}

							for name, content := range td.Blobs {
								var id git.ObjectID

								Expect(func() (err error) { id, err = CreateBlob(ctx, repo, content); return }()).To(Succeed())

								Expect(tb.AddEntry(name, id, git.FilemodeBlob)).To(matchErr)
							}

							for name, td := range td.Subtrees {
								var id git.ObjectID

								Expect(func() (err error) { id, err = CreateTreeFromDef(ctx, repo, &td); return }()).To(Succeed())

								Expect(tb.AddEntry(name, id, git.FilemodeTree)).To(matchErr)
							}

							Expect(func() (td TreeDef, err error) {
								var id git.ObjectID

								if id, err = tb.Build(ctx); err != nil {
									return
								}

								td = *GetTreeDef(ctx, repo, id)
								return
							}()).To(matchTreeDef)
						})
					}(s.spec, s.removeEntryNames, s.td, s.matchErr, s.matchTreeDef)
				}
			})
		})
	})
})
