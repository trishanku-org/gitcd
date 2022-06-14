package git2go

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
)

var _ = FDescribe("merger", func() {
	var (
		ctx     context.Context
		merger  git.Merger
		gitImpl git.Interface
		repo    git.Repository
		dir     string
	)

	BeforeEach(func() {
		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		gitImpl = New()

		Expect(func() (err error) { repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(repo).ToNot(BeNil())

		merger = repo.Merger(gitImpl.Errors())
		Expect(merger).ToNot(BeNil())
	})

	AfterEach(func() {
		if merger != nil {
			Expect(merger.Close()).To(Succeed())
		}

		if repo != nil {
			Expect(repo.Close()).To(Succeed())
		}

		if len(dir) > 0 {
			Expect(os.RemoveAll(dir))
		}
	})

	Describe("MergeTrees", func() {
		type spec struct {
			ctxFn                                          ContextFunc
			conflictResolution                             git.MergeConfictResolution
			retentionPolicy                                git.MergeRetentionPolicy
			ancestor, ours, theirs                         *TreeDef
			matchMutated, matchTreeID, matchTree, matchErr types.GomegaMatcher
		}

		for _, s := range []spec{
			{
				ctxFn:        CanceledContext,
				matchErr:     MatchError(context.Canceled),
				matchMutated: BeFalse(),
				matchTreeID:  Equal(git.ObjectID{}),
			},
			func() spec {
				var ours = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

				return spec{
					ours:         ours,
					matchErr:     Succeed(),
					matchMutated: BeFalse(),
					matchTreeID:  Equal(git.ObjectID{}),
				}
			}(),
			func() spec {
				var theirs = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}

				return spec{
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree:    PointTo(GetTreeDefMatcher(theirs)),
				}
			}(),
			func() spec {
				var (
					ours   = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					theirs = ours.DeepCopy()
				)

				return spec{
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeFalse(),
					matchTreeID:  Equal(git.ObjectID{}),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					ours     = ancestor.DeepCopy()
					theirs   = ancestor.DeepCopy()
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeFalse(),
					matchTreeID:  Equal(git.ObjectID{}),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					ours     = ancestor.DeepCopy()
					theirs   = ancestor.DeepCopy()
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeFalse(),
					matchTreeID:  Equal(git.ObjectID{}),
				}
			}(),
			func() spec {
				var (
					ours   = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					theirs = &TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}
				)

				return spec{
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("1"),
						"2": []byte("2"),
					}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("1"),
						"2": []byte("2"),
						"3": []byte("3"),
					}})),
				}
			}(),
			func() spec {
				var (
					ours   = &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}
					theirs = &TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}}
				)

				return spec{
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("one"),
						"2": []byte("2"),
						"3": []byte("3"),
					}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("one"),
						"2": []byte("2"),
						"3": []byte("3"),
					}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"1": []byte("11"), "2": []byte("2")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("one"),
						"2": []byte("2"),
						"3": []byte("3"),
					}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "2": []byte("2")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"3": []byte("3")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"2": []byte("2"),
						"3": []byte("3"),
					}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("one"),
						"2": []byte("2"),
						"3": []byte("3"),
					}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"2": []byte("two"), "3": []byte("3")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "4": []byte("4")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("one"),
						"3": []byte("3"),
						"4": []byte("4"),
					}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"2": []byte("two"), "3": []byte("33"), "4": []byte("4")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("three"), "5": []byte("5")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("one"),
						"3": []byte("three"),
						"4": []byte("4"),
						"5": []byte("5"),
					}})),
				}
			}(),
			func() spec {
				var (
					ours   = &TreeDef{Blobs: map[string][]byte{"1": []byte("1\na\n2\n3\n")}}
					theirs = &TreeDef{Blobs: map[string][]byte{"1": []byte("1\n2\nb\n3\n")}}
				)

				return spec{
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("1\n2\nb\n3\n")}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Blobs: map[string][]byte{"1": []byte("1\n2\n3\n")}}
					ours     = &TreeDef{Blobs: map[string][]byte{"1": []byte("1\na\n2\n3\n")}}
					theirs   = &TreeDef{Blobs: map[string][]byte{"1": []byte("1\n2\nb\n3\n")}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(&TreeDef{Blobs: map[string][]byte{
						"1": []byte("1\na\n2\nb\n3\n")}})),
				}
			}(),
			func() spec {
				var (
					ancestor = &TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
kind: Pod
metadata:
  name: nginx
  namespace: default
spec:
  containers:
  - name: nginx
	image: nginx:1
	imagePullPolicy: IfNotPresent
status:
  phase: Scheduled
`)}}}}

					ours = &TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
kind: Pod
metadata:
  name: nginx
  namespace: default
spec:
  containers:
  - name: nginx
	image: nginx:1
	imagePullPolicy: IfNotPresent
status:
  phase: Scheduled
  conditions:
  - type: MemoryPressure
    status: False
    message: "Pod has sufficient memory"
`)}}}}

					theirs = &TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
kind: Pod
metadata:
  name: nginx
  namespace: default
  annotations:
    app: nginx
spec:
  containers:
  - name: nginx
	image: nginx:1
	imagePullPolicy: IfNotPresent
status:
  phase: Scheduled
`)}}}}
				)

				return spec{
					ancestor:     ancestor,
					ours:         ours,
					theirs:       theirs,
					matchErr:     Succeed(),
					matchMutated: BeTrue(),
					matchTreeID:  Not(Equal(git.ObjectID{})),
					matchTree: PointTo(GetTreeDefMatcher(
						&TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
kind: Pod
metadata:
  name: nginx
  namespace: default
  annotations:
    app: nginx
spec:
  containers:
  - name: nginx
	image: nginx:1
	imagePullPolicy: IfNotPresent
status:
  phase: Scheduled
  conditions:
  - type: MemoryPressure
    status: False
    message: "Pod has sufficient memory"
`)}}}},
					)),
				}
			}(),
		} {
			func(s spec) {
				Describe(
					fmt.Sprintf(
						"conflictResolution=%d, retentionPolicy=%v, ancestor=%v, ours=%v, theirs=%v",
						s.conflictResolution,
						s.retentionPolicy,
						s.ancestor,
						s.ours,
						s.theirs,
					),
					func() {
						var (
							parentCtx              context.Context
							ancestor, ours, theirs git.Tree
						)

						BeforeEach(func() {
							for _, t := range []struct {
								tPtr *git.Tree
								td   *TreeDef
							}{
								{tPtr: &ancestor, td: s.ancestor},
								{tPtr: &ours, td: s.ours},
								{tPtr: &theirs, td: s.theirs},
							} {
								Expect(
									func(tPtr *git.Tree, td *TreeDef) (err error) {
										if td == nil {
											return
										}

										*tPtr, err = CreateAndLoadTreeFromDef(ctx, repo, td)
										return
									}(t.tPtr, t.td),
								).To(Succeed())
							}

							if s.conflictResolution > 0 {
								merger.SetConflictResolution(s.conflictResolution)
							}

							if s.retentionPolicy != nil {
								merger.SetRetentionPolicy(s.retentionPolicy)
							}

							parentCtx = ctx

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}
						})

						AfterEach(func() {
							for _, t := range []git.Tree{ancestor, ours, theirs} {
								if t != nil {
									Expect(t.Close()).To(Succeed())
								}
							}
						})

						It(ItSpecForMatchError(s.matchErr), func() {
							var (
								mutated bool
								tID     git.ObjectID
								err     error
							)

							mutated, tID, err = merger.MergeTrees(ctx, ancestor, ours, theirs)

							for _, e := range []struct {
								spec   string
								actual interface{}
								match  types.GomegaMatcher
							}{
								{spec: "error", actual: err, match: s.matchErr},
								{spec: "mutated", actual: mutated, match: s.matchMutated},
								{spec: "treeID", actual: tID, match: s.matchTreeID},
							} {
								func(spec string, actual interface{}, match types.GomegaMatcher) {
									Expect(actual).To(match, spec)
								}(e.spec, e.actual, e.match)
							}

							if s.matchTree != nil {
								Expect(GetTreeDef(parentCtx, repo, tID)).To(s.matchTree, "tree")
							}
						})
					},
				)
			}(s)
		}
	})
})
