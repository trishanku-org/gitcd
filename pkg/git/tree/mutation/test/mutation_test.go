package test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
	mockgit "github.com/trishanku/gitcd/pkg/mocks/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"github.com/trishanku/gitcd/pkg/util"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

var _ = Describe("backend", func() {
	var repo git.Repository

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

			Expect(func() (err error) { repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
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

		Describe("IsTreeEmpty", func() {
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
						Expect(func() (err error) { id, err = CreateBlob(ctx, repo, []byte{}); return }()).To(Succeed())
						return
					}
				}

				treeID = func(td *TreeDef) idFunc {
					return func() (id git.ObjectID) {
						Expect(func() (err error) { id, err = CreateTreeFromDef(ctx, repo, td); return }()).To(Succeed())
						return
					}
				}

				commitID = func() idFunc {
					return func() (id git.ObjectID) {
						Expect(func() (err error) { id, err = CreateCommitFromDef(ctx, repo, &CommitDef{}); return }()).To(Succeed())
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
							var empty, err = mutation.IsTreeEmpty(ctx, repo, id)
							Expect(err).To(s.matchErr)
							Expect(empty).To(s.matchEmpty)
						})
					})
				}(s)
			}
		})

		Describe("tree mutation", func() {
			var (
				nop = func(mutated bool) mutation.MutateTreeEntryFunc {
					return func(_ context.Context, _ git.TreeBuilder, _ string, _ git.TreeEntry) (bool, error) {
						return mutated, nil
					}
				}

				addOrReplaceWithBlob = func(content []byte) mutation.MutateTreeEntryFunc {
					return func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
						var id git.ObjectID

						if te != nil {
							if err = tb.RemoveEntry(entryName); err != nil {
								return
							}

							mutated = true
						}

						if id, err = CreateBlob(ctx, repo, content); err != nil {
							return
						}

						if err = tb.AddEntry(entryName, id, git.FilemodeBlob); err != nil {
							return
						}

						mutated = true

						return
					}
				}

				addOrReplaceWithTree = func(td *TreeDef) mutation.MutateTreeEntryFunc {
					return func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
						var id git.ObjectID

						if te != nil {
							if err = tb.RemoveEntry(entryName); err != nil {
								return
							}

							mutated = true
						}

						if id, err = CreateTreeFromDef(ctx, repo, td); err != nil {
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

				lotsOfMutationsOnNilTree = func() (tm *mutation.TreeMutation, etd *TreeDef) {
					var subtreePrefix = "subtree-"

					tm = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
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

							tm.Subtrees[subtreePrefix+ppk] = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
							etd.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

							fallthrough
						case pk:
							tm.Subtrees[subtreePrefix+ppk].Entries[k] = addOrReplaceWithBlob(([]byte(k)))
							etd.Subtrees[subtreePrefix+ppk].Blobs[k] = []byte(k)

							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
							etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
						default:
							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Entries[k] = addOrReplaceWithBlob([]byte(k))
							etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Blobs[k] = []byte(k)
						}
					}

					return
				}

				lotsOfMutationsOnExistingTree = func() (td *TreeDef, tm *mutation.TreeMutation, etd *TreeDef) {
					var subtreePrefix = "subtree-"
					td = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
					tm = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
					etd = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

					for i := 0; i < 2000; i++ {
						var (
							k   = strconv.Itoa(i)
							pk  = strconv.Itoa(i / 10 * 10)
							ppk = strconv.Itoa(i / 100 * 100)

							mutate mutation.MutateTreeEntryFunc
							ev     []byte
						)

						switch rand.Int() % 4 {
						case 1:
							mutate = nop(false)
							ev = []byte(k)
						case 2:
							mutate = addOrReplaceWithBlob([]byte(k + "1000"))
							ev = []byte(k + "1000")
						case 3:
							mutate = remove
						default:
							// skip mutation
							ev = []byte(k)
						}

						switch k {
						case ppk:
							td.Blobs[k] = []byte(k)
							if mutate != nil {
								tm.Entries[k] = mutate
							}
							if ev != nil {
								etd.Blobs[k] = ev
							}

							tm.Subtrees[subtreePrefix+ppk] = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
							td.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
							etd.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

							fallthrough
						case pk:
							td.Subtrees[subtreePrefix+ppk].Blobs[k] = []byte(k)
							if mutate != nil {
								tm.Subtrees[subtreePrefix+ppk].Entries[k] = mutate
							}
							if ev != nil {
								etd.Subtrees[subtreePrefix+ppk].Blobs[k] = ev
							}

							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
							td.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
							etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = TreeDef{Blobs: map[string][]byte{}}
						default:
							td.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Blobs[k] = []byte(k)
							if mutate != nil {
								tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Entries[k] = mutate
							}
							if ev != nil {
								etd.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+pk].Blobs[k] = ev
							}
						}
					}
					return
				}

				lotsOfDeletionsOnExistingTree = func() (td *TreeDef, tm *mutation.TreeMutation, etd *TreeDef) {
					var subtreePrefix = "subtree-"
					td = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
					tm = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
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

							tm.Subtrees[subtreePrefix+ppk] = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
							td.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}
							etd.Subtrees[subtreePrefix+ppk] = TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

							fallthrough
						case pk:
							td.Subtrees[subtreePrefix+ppk].Blobs[k] = []byte(k)
							tm.Subtrees[subtreePrefix+ppk].Entries[k] = remove

							tm.Subtrees[subtreePrefix+ppk].Subtrees[subtreePrefix+k] = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}}
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

			Describe("MutateTreeBuilder", func() {
				type treeBuilderFunc func(t git.Tree) (git.TreeBuilder, error)

				type check struct {
					spec                                 string
					ctxFn                                ContextFunc
					td                                   *TreeDef
					tbFn                                 treeBuilderFunc
					tm                                   *mutation.TreeMutation
					cleanupEmptySubtrees                 bool
					matchErr, matchMutated, matchTreeDef types.GomegaMatcher
				}

				var (
					treeBuilderFn = func(t git.Tree) (tb git.TreeBuilder, err error) {
						if t == nil {
							tb, err = repo.TreeBuilder(ctx)
						} else {
							tb, err = repo.TreeBuilderFromTree(ctx, t)
						}

						return
					}
				)

				for _, s := range []check{
					{
						spec:         "nil tree, nil tree builder with empty tree mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &mutation.TreeMutation{},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with empty tree mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &mutation.TreeMutation{},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with nop blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(false)}},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with nop-mutate blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(true)}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with nop subtrees only mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec:         "nil tree with blob entries only add/replace tree mutation with expired context",
						ctxFn:        CanceledContext,
						tbFn:         treeBuilderFn,
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec: "nil tree with blob entries only add/replace tree mutation",
						tbFn: treeBuilderFn,
						tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{
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
						tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}, "2": {}}},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{}),
					},
					{
						spec: "nil tree with subtrees only nop-mutate tree mutation",
						tbFn: treeBuilderFn,
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(true)}},
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": nop(true)}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{"1": {}, "2": {}}}),
					},
					{
						spec: "nil tree with subtrees only tree mutation",
						tbFn: treeBuilderFn,
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"4": {Entries: map[string]mutation.MutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("4"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
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
							tm:           &mutation.TreeMutation{},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"2": nop(false)}},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"2": nop(true)}},
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
							tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}}},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": remove}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					{
						spec: "blob entries only add/replace tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
						tbFn: treeBuilderFn,
						tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{
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
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": remove}},
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
							tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}, "2": {}}},
							matchErr:     Succeed(),
							matchMutated: BeFalse(),
							matchTreeDef: GetTreeDefMatcher(td),
						}
					}(),
					{
						spec: "subtrees only nop-mutate tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tbFn: treeBuilderFn,
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(true)}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove, "3": remove}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove, "3": remove}},
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
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove}},
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
						tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*mutation.TreeMutation{
								"4": {Entries: map[string]mutation.MutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutation.MutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutation.MutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutation.MutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutation.MutateTreeEntryFunc{"8": remove}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*mutation.TreeMutation{
								"4": {Entries: map[string]mutation.MutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutation.MutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutation.MutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutation.MutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutation.MutateTreeEntryFunc{"8": remove}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": addOrReplaceWithBlob([]byte("20"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*mutation.TreeMutation{
								"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
									Expect(func() (err error) { t, err = CreateAndLoadTreeFromDef(ctx, repo, s.td); return }()).To(Succeed())
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
								var mutated, err = mutation.MutateTreeBuilder(ctx, repo, t, tb, s.tm, s.cleanupEmptySubtrees)
								Expect(err).To(s.matchErr)
								Expect(mutated).To(s.matchMutated)

								if tb != nil {
									Expect(func() (td TreeDef, err error) {
										var id git.ObjectID

										if id, err = tb.Build(parentCtx); err != nil {
											return
										}

										td = *GetTreeDef(parentCtx, repo, id)
										return
									}()).To(s.matchTreeDef)
								}
							})
						})
					}(s)
				}
			})

			Describe("MutateTree", func() {
				type check struct {
					spec                                 string
					ctxFn                                ContextFunc
					td                                   *TreeDef
					tm                                   *mutation.TreeMutation
					cleanupEmptySubtrees                 bool
					matchErr, matchMutated, matchTreeDef types.GomegaMatcher
				}

				for _, s := range []check{
					{
						spec:         "nil tree with empty tree mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &mutation.TreeMutation{},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with nop blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(false)}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with nop-mutate blob entries only mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(true)}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with nop subtrees only mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec:         "nil tree with blob entries only add/replace tree mutation with expired context",
						ctxFn:        CanceledContext,
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
						matchErr:     MatchError("context canceled"),
						matchMutated: BeFalse(),
					},
					{
						spec: "nil tree with blob entries only add/replace tree mutation",
						tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{
							"1": addOrReplaceWithBlob([]byte("1")),
							"2": addOrReplaceWithBlob([]byte("2")),
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}),
					},
					{
						spec:         "nil tree with subtrees only nop tree mutation",
						tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}, "2": {}}},
						matchErr:     Succeed(),
						matchMutated: BeFalse(),
					},
					{
						spec: "nil tree with subtrees only nop-mutate tree mutation",
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(true)}},
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": nop(true)}},
						}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Subtrees: map[string]TreeDef{"1": {}, "2": {}}}),
					},
					{
						spec: "nil tree with subtrees only tree mutation",
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"4": {Entries: map[string]mutation.MutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("4"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("3"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1")), "2": addOrReplaceWithBlob([]byte("2"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("1"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("2"))}},
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
							tm:           &mutation.TreeMutation{},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"2": nop(false)}},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"2": nop(true)}},
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
							tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}}},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
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
							tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": remove}},
							matchErr:     MatchError("context canceled"),
							matchMutated: BeFalse(),
						}
					}(),
					{
						spec: "blob entries only add/replace tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}},
						tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{
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
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": remove}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}),
					},
					func() check {
						var td = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

						return check{
							spec:         "subtrees only nop tree mutation",
							td:           td,
							tm:           &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"1": {}, "2": {}}},
							matchErr:     Succeed(),
							matchMutated: BeFalse(),
						}
					}(),
					{
						spec: "subtrees only nop-mutate tree mutation",
						td:   &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop(true)}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove, "3": remove}},
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
						tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{
							"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove, "3": remove}},
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
						tm:           &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"2": remove}},
						matchErr:     Succeed(),
						matchMutated: BeTrue(),
						matchTreeDef: GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}),
					},
					{
						spec: "addOrReplace blob entry with subtree tree mutation",
						td: &TreeDef{Subtrees: map[string]TreeDef{
							"1": {Blobs: map[string][]byte{"1": []byte("1")}},
						}},
						tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*mutation.TreeMutation{
								"4": {Entries: map[string]mutation.MutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutation.MutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutation.MutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutation.MutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutation.MutateTreeEntryFunc{"8": remove}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{
								"1": addOrReplaceWithBlob([]byte("10")),
								"2": addOrReplaceWithBlob([]byte("20")),
								"3": remove,
							},
							Subtrees: map[string]*mutation.TreeMutation{
								"4": {Entries: map[string]mutation.MutateTreeEntryFunc{"4": addOrReplaceWithBlob([]byte("40"))}},
								"5": {Entries: map[string]mutation.MutateTreeEntryFunc{
									"5": addOrReplaceWithBlob([]byte("50")),
									"7": remove,
								}},
								"6": {Entries: map[string]mutation.MutateTreeEntryFunc{"6": addOrReplaceWithBlob([]byte("60"))}},
								"7": {Entries: map[string]mutation.MutateTreeEntryFunc{"7": remove}},
								"8": {Entries: map[string]mutation.MutateTreeEntryFunc{"8": remove}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": addOrReplaceWithBlob([]byte("20"))},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*mutation.TreeMutation{
								"3": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": addOrReplaceWithBlob([]byte("30"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
						tm: &mutation.TreeMutation{
							Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10")), "2": remove},
							Subtrees: map[string]*mutation.TreeMutation{
								"1": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": addOrReplaceWithBlob([]byte("10"))}},
								"2": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": addOrReplaceWithBlob([]byte("20"))}},
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
									Expect(func() (err error) { t, err = CreateAndLoadTreeFromDef(ctx, repo, s.td); return }()).To(Succeed())
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
								var mutated, id, err = mutation.MutateTree(ctx, repo, t, s.tm, s.cleanupEmptySubtrees)
								Expect(err).To(s.matchErr)
								Expect(mutated).To(s.matchMutated)
								if mutated && err == nil {
									Expect(*GetTreeDef(parentCtx, repo, id)).To(s.matchTreeDef)
								}
							})
						})
					}(s)
				}
			})
		})
	})
})

var _ = Describe("DeleteEntry", func() {
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
								var mutated, err = mutation.DeleteEntry(ctx, tb, entryName, te)
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

var _ = Describe("TreeMutation", func() {
	var (
		tmPtrMatcher func(tm *mutation.TreeMutation) types.GomegaMatcher
		nop          = func(_ context.Context, _ git.TreeBuilder, _ string, _ git.TreeEntry) (mutated bool, err error) {
			return
		}
	)

	tmPtrMatcher = func(tm *mutation.TreeMutation) types.GomegaMatcher {
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

	Describe("AddMutationPathSlice", func() {
		type check struct {
			spec                 string
			tm                   *mutation.TreeMutation
			ps                   util.PathSlice
			entryName            string
			mutateFn             mutation.MutateTreeEntryFunc
			matchErr, matchTMPtr types.GomegaMatcher
		}

		for _, s := range []check{
			{
				spec:       "nil tm, add top level entry",
				entryName:  "1",
				mutateFn:   nop,
				matchErr:   Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop}}),
			},
			{
				spec:      "nil tm, add deep entry",
				ps:        util.PathSlice{"a", "b", "c"},
				entryName: "1",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Subtrees: map[string]*mutation.TreeMutation{"a": {
						Subtrees: map[string]*mutation.TreeMutation{"b": {
							Subtrees: map[string]*mutation.TreeMutation{"c": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop}}},
						}},
					}},
				}),
			},
			{
				spec:      "existing tm with subtrees but with no top-level entries, add top level entry",
				tm:        &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"a": {Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}}}},
				entryName: "b",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries:  map[string]mutation.MutateTreeEntryFunc{"b": nop},
					Subtrees: map[string]*mutation.TreeMutation{"a": {Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}}},
				}),
			},
			{
				spec: "existing tm with subtrees and top-level entries, add top level entry",
				tm: &mutation.TreeMutation{
					Entries:  map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {Entries: map[string]mutation.MutateTreeEntryFunc{"b": nop}}},
				},
				entryName: "c",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries:  map[string]mutation.MutateTreeEntryFunc{"a": nop, "c": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {Entries: map[string]mutation.MutateTreeEntryFunc{"b": nop}}},
				}),
			},
			{
				spec:      "existing tm with top-level entries but with no subtrees, add deep entry",
				tm:        &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}},
				ps:        util.PathSlice{"b", "c"},
				entryName: "d",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {
						Subtrees: map[string]*mutation.TreeMutation{"c": {
							Entries: map[string]mutation.MutateTreeEntryFunc{"d": nop},
						}},
					}},
				}),
			},
			{
				spec: "existing tm with top-level entries and subtrees, add deep entry",
				tm: &mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {
						Subtrees: map[string]*mutation.TreeMutation{"c": {
							Entries: map[string]mutation.MutateTreeEntryFunc{"d": nop},
						}},
					}},
				},
				ps:        util.PathSlice{"b", "d", "e"},
				entryName: "f",
				mutateFn:  nop,
				matchErr:  Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {
						Subtrees: map[string]*mutation.TreeMutation{
							"c": {Entries: map[string]mutation.MutateTreeEntryFunc{"d": nop}},
							"d": {
								Subtrees: map[string]*mutation.TreeMutation{"e": {Entries: map[string]mutation.MutateTreeEntryFunc{"f": nop}}},
							},
						},
					}},
				}),
			},
		} {
			func(s check) {
				It(s.spec, func() {
					var tm, err = mutation.AddMutationPathSlice(s.tm, s.ps, s.entryName, s.mutateFn)
					Expect(err).To(s.matchErr)
					Expect(tm).To(s.matchTMPtr)
				})
			}(s)
		}
	})

	Describe("AddMutation", func() {
		type check struct {
			spec                 string
			tm                   *mutation.TreeMutation
			p                    string
			mutateFn             mutation.MutateTreeEntryFunc
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
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop}}),
			},
			{
				spec:     "nil tm, add deep entry",
				p:        "a/b/c/1",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Subtrees: map[string]*mutation.TreeMutation{"a": {
						Subtrees: map[string]*mutation.TreeMutation{"b": {
							Subtrees: map[string]*mutation.TreeMutation{"c": {Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop}}},
						}},
					}},
				}),
			},
			func() check {
				var tm = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}}

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
				var tm = &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}}

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
				tm:       &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{"a": {Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}}}},
				p:        "b",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries:  map[string]mutation.MutateTreeEntryFunc{"b": nop},
					Subtrees: map[string]*mutation.TreeMutation{"a": {Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}}},
				}),
			},
			{
				spec: "existing tm with subtrees and top-level entries, add top level entry",
				tm: &mutation.TreeMutation{
					Entries:  map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {Entries: map[string]mutation.MutateTreeEntryFunc{"b": nop}}},
				},
				p:        "c",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries:  map[string]mutation.MutateTreeEntryFunc{"a": nop, "c": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {Entries: map[string]mutation.MutateTreeEntryFunc{"b": nop}}},
				}),
			},
			{
				spec:     "existing tm with top-level entries but with no subtrees, add deep entry",
				tm:       &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}},
				p:        "b/c/d",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {
						Subtrees: map[string]*mutation.TreeMutation{"c": {
							Entries: map[string]mutation.MutateTreeEntryFunc{"d": nop},
						}},
					}},
				}),
			},
			{
				spec: "existing tm with top-level entries and subtrees, add deep entry",
				tm: &mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {
						Subtrees: map[string]*mutation.TreeMutation{"c": {
							Entries: map[string]mutation.MutateTreeEntryFunc{"d": nop},
						}},
					}},
				},
				p:        "b/d/e/f",
				mutateFn: nop,
				matchErr: Succeed(),
				matchTMPtr: tmPtrMatcher(&mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop},
					Subtrees: map[string]*mutation.TreeMutation{"b": {
						Subtrees: map[string]*mutation.TreeMutation{
							"c": {Entries: map[string]mutation.MutateTreeEntryFunc{"d": nop}},
							"d": {
								Subtrees: map[string]*mutation.TreeMutation{"e": {Entries: map[string]mutation.MutateTreeEntryFunc{"f": nop}}},
							},
						},
					}},
				}),
			},
		} {
			func(s check) {
				It(s.spec, func() {
					var tm, err = mutation.AddMutation(s.tm, s.p, s.mutateFn)
					Expect(err).To(s.matchErr)
					Expect(tm).To(s.matchTMPtr)
				})
			}(s)
		}
	})

	Describe("IsMutationNOP", func() {
		type check struct {
			spec     string
			tm       *mutation.TreeMutation
			matchNOP types.GomegaMatcher
		}

		for _, s := range []check{
			{spec: "nil tm", matchNOP: BeTrue()},
			{spec: "nop shallow tm", tm: &mutation.TreeMutation{}, matchNOP: BeTrue()},
			{spec: "nop shallow tm with empty entries", tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}}, matchNOP: BeTrue()},
			{spec: "nop shallow tm with empty subtrees", tm: &mutation.TreeMutation{Subtrees: map[string]*mutation.TreeMutation{}}, matchNOP: BeTrue()},
			{
				spec:     "nop shallow tm with empty entries and subtrees",
				tm:       &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{}, Subtrees: map[string]*mutation.TreeMutation{}},
				matchNOP: BeTrue(),
			},
			{
				spec: "nop deep tm",
				tm: &mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{},
					Subtrees: map[string]*mutation.TreeMutation{
						"a": {
							Subtrees: map[string]*mutation.TreeMutation{"b": {
								Subtrees: map[string]*mutation.TreeMutation{"c": {}},
							}},
						},
						"d": {Subtrees: map[string]*mutation.TreeMutation{"e": {}}},
					},
				},
				matchNOP: BeTrue(),
			},
			{spec: "non-nop shallow tm", tm: &mutation.TreeMutation{Entries: map[string]mutation.MutateTreeEntryFunc{"a": nop}}, matchNOP: BeFalse()},
			{
				spec: "non-nop deep tm",
				tm: &mutation.TreeMutation{
					Entries: map[string]mutation.MutateTreeEntryFunc{},
					Subtrees: map[string]*mutation.TreeMutation{
						"a": {
							Subtrees: map[string]*mutation.TreeMutation{"b": {
								Subtrees: map[string]*mutation.TreeMutation{"c": {}},
							}},
						},
						"d": {Subtrees: map[string]*mutation.TreeMutation{"e": {Entries: map[string]mutation.MutateTreeEntryFunc{"f": nop}}}},
					},
				},
				matchNOP: BeFalse(),
			},
		} {
			func(s check) {
				It(s.spec, func() {
					Expect(mutation.IsMutationNOP(s.tm)).To(s.matchNOP)
				})
			}(s)
		}
	})

	Describe("IsConflict", func() { // isConflict is also tested indirectly here because it is an internal function.
		type check struct {
			tm                      *mutation.TreeMutation
			p                       string
			matchErr, matchConflict types.GomegaMatcher
		}

		var (
			tm = &mutation.TreeMutation{
				Entries: map[string]mutation.MutateTreeEntryFunc{"1": nop, "4": nop},
				Subtrees: map[string]*mutation.TreeMutation{
					"a": {
						Entries: map[string]mutation.MutateTreeEntryFunc{"3": nop, "6": nop},
						Subtrees: map[string]*mutation.TreeMutation{
							"i": {Entries: map[string]mutation.MutateTreeEntryFunc{"8": nop, "10": nop}},
							"j": {Entries: map[string]mutation.MutateTreeEntryFunc{"2": nop, "5": nop}},
						},
					},
					"b": {
						Entries: map[string]mutation.MutateTreeEntryFunc{"7": nop, "9": nop},
						Subtrees: map[string]*mutation.TreeMutation{
							"p": {Entries: map[string]mutation.MutateTreeEntryFunc{"3": nop, "8": nop}},
							"q": {Entries: map[string]mutation.MutateTreeEntryFunc{"0": nop, "7": nop}},
						},
					},
				},
			}
		)

		for _, c := range []check{
			{matchErr: MatchError(rpctypes.ErrGRPCEmptyKey), matchConflict: BeFalse()},
			{p: "a", matchErr: Succeed(), matchConflict: BeFalse()},
			{p: "a/b", matchErr: Succeed(), matchConflict: BeFalse()},
			{p: "a/b/c", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "1", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "1/a/b/c/d", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "4", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "4/2/1/2/3", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "2", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "2/1/2/a/b", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/3", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/3/1/2/a", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/6", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/6/a/b/c/d", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/4", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a/4/a/b/1/2", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a/i", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/i/8", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/i/8/1/a/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/i/10", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/i/10/2/a/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/i/9", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a/i/9/3/a/b", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a/j", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/j/2", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/j/2/a/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/j/5", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/j/5/a/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "a/j/6", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a/j/6/a/b", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a/x", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "a/x/1/2", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "z/1", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/7", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/7/2/a", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/9", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/9/b/c/d", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/2", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b/2/b/1/2", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b/p", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/p/3", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/p/3/1/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/p/8", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/p/8/2/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/p/5", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b/p/5/3/b", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b/q", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/q/0", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/q/0/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/q/7", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/q/7/b", matchErr: Succeed(), matchConflict: BeTrue()},
			{tm: tm, p: "b/q/1", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b/q/1/b", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b/y", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "b/y/d/5", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "z", matchErr: Succeed(), matchConflict: BeFalse()},
			{tm: tm, p: "z/2/1/a/b", matchErr: Succeed(), matchConflict: BeFalse()},
		} {
			func(c check) {
				Describe(fmt.Sprintf("tm exists=%t, p=%s", c.tm != nil, c.p), func() {
					It(ItSpecForMatchError(c.matchErr), func() {
						var conflict, err = mutation.IsConflict(c.tm, c.p)

						Expect(err).To(c.matchErr)
						Expect(conflict).To(c.matchConflict)
					})
				})
			}(c)
		}
	})
})
