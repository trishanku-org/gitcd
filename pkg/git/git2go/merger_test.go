package git2go

import (
	"context"
	"errors"
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

var _ = Describe("merger", func() {
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

	Describe("MergeCommits", func() {
		type spec struct {
			ctxFn                                                  ContextFunc
			conflictResolution                                     git.MergeConfictResolution
			retentionPolicy                                        git.MergeRetentionPolicy
			ours, theirs                                           *CommitDef
			noFastForward                                          bool
			createCommitFn                                         git.CreateCommitFunc
			matchMutated, matchHeadID, matchCommitDefPtr, matchErr types.GomegaMatcher
		}

		var (
			createCommitFn = func(ctx context.Context, treeID git.ObjectID, parents ...git.Commit) (commitID git.ObjectID, err error) {
				var cb git.CommitBuilder

				if cb, err = repo.CommitBuilder(ctx); err != nil {
					return
				}

				defer cb.Close()

				if err = cb.SetAuthorName("trishanku"); err != nil {
					return
				}

				if err = cb.SetAuthorEmail("trishanku@heaven.com"); err != nil {
					return
				}

				if err = cb.SetTreeID(treeID); err != nil {
					return
				}

				if err = cb.AddParents(parents...); err != nil {
					return
				}

				commitID, err = cb.Build(ctx)
				return
			}
		)

		for _, s := range []spec{
			{
				ctxFn:        CanceledContext,
				matchErr:     MatchError(context.Canceled),
				matchMutated: BeFalse(),
				matchHeadID:  Equal(git.ObjectID{}),
			},
			func() spec {
				var ours = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}

				return spec{
					ours:              ours,
					matchErr:          Succeed(),
					matchMutated:      BeFalse(),
					matchHeadID:       Not(Equal((git.ObjectID{}))),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(ours)),
				}
			}(),
			func() spec {
				var ours = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}

				return spec{
					ours:              ours,
					noFastForward:     true,
					matchErr:          Succeed(),
					matchMutated:      BeFalse(),
					matchHeadID:       Not(Equal((git.ObjectID{}))),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(ours)),
				}
			}(),
			func() spec {
				var theirs = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}

				return spec{
					theirs:            theirs,
					matchErr:          Succeed(),
					matchMutated:      BeTrue(),
					matchHeadID:       Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(theirs)),
				}
			}(),
			func() spec {
				var theirs = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}

				return spec{
					theirs:         theirs,
					noFastForward:  true,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree:    theirs.Tree,
						Parents: []CommitDef{*theirs},
					})),
				}
			}(),
			func() spec {
				var (
					theirs          = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					createCommitErr = errors.New("error creating commit")
				)

				return spec{
					theirs:        theirs,
					noFastForward: true,
					createCommitFn: func(
						ctx context.Context,
						treeID git.ObjectID,
						parents ...git.Commit,
					) (commitID git.ObjectID, err error) {
						err = createCommitErr
						return
					},
					matchErr:     MatchError(createCommitErr),
					matchMutated: BeFalse(),
					matchHeadID:  Equal(git.ObjectID{}),
				}
			}(),
			func() spec {
				var (
					ours   = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					theirs = ours.DeepCopy()
				)

				return spec{
					ours:              ours,
					theirs:            theirs,
					matchErr:          Succeed(),
					matchMutated:      BeFalse(),
					matchHeadID:       Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(ours)),
				}
			}(),
			func() spec {
				var (
					ours   = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					theirs = ours.DeepCopy()
				)

				return spec{
					ours:              ours,
					theirs:            theirs,
					noFastForward:     true,
					matchErr:          Succeed(),
					matchMutated:      BeFalse(),
					matchHeadID:       Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(ours)),
				}
			}(),
			func() spec {
				var (
					ours            = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					theirs          = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
					createCommitErr = errors.New("error creating commit")
				)

				return spec{
					ours:   ours,
					theirs: theirs,
					createCommitFn: func(
						ctx context.Context,
						treeID git.ObjectID,
						parents ...git.Commit,
					) (commitID git.ObjectID, err error) {
						err = createCommitErr
						return
					},
					matchErr:     MatchError(createCommitErr),
					matchMutated: BeFalse(),
					matchHeadID:  Equal(git.ObjectID{}),
				}
			}(),
			func() spec {
				var (
					ours   = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					theirs = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("1"),
							"2": []byte("2"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours   = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}
					theirs = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("2")}}}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					noFastForward:  true,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("1"),
							"2": []byte("2"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("1"),
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "3": []byte("3")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					noFastForward:  true,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("1"),
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours   = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}}
					theirs = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}}}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("one"),
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours   = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}}
					theirs = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}}}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					noFastForward:  true,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("one"),
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("one"),
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("11"), "2": []byte("2")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("one"),
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "2": []byte("2")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"3": []byte("3")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"2": []byte("2")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("3")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("one"),
							"2": []byte("2"),
							"3": []byte("3"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"2": []byte("two"), "3": []byte("3")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "4": []byte("4")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("one"),
							"3": []byte("3"),
							"4": []byte("4"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{"2": []byte("two"), "3": []byte("33"), "4": []byte("4")}},
						Parents: []CommitDef{{
							Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1"), "2": []byte("2"), "3": []byte("3")}},
						}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("one"), "3": []byte("three"), "5": []byte("5")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Blobs: map[string][]byte{
							"1": []byte("one"),
							"3": []byte("three"),
							"4": []byte("4"),
							"5": []byte("5"),
						}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours   = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1\na\n2\n3\n")}}}
					theirs = &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1\n2\nb\n3\n")}}}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1\n2\nb\n3\n")}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1\na\n2\n3\n")}},
						Parents: []CommitDef{{Tree: TreeDef{Blobs: map[string][]byte{"1": []byte("1\n2\n3\n")}}}},
					}
					theirs = &CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1\n2\nb\n3\n")}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree:    TreeDef{Blobs: map[string][]byte{"1": []byte("1\na\n2\nb\n3\n")}},
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
			func() spec {
				var (
					ours = &CommitDef{
						Tree: TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
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
`)}}}},
						Parents: []CommitDef{{
							Tree: TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
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
`)}}}},
						}},
					}

					theirs = &CommitDef{
						Tree: TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
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
`)}}}},
						Parents: ours.Parents,
					}
				)

				return spec{
					ours:           ours,
					theirs:         theirs,
					createCommitFn: createCommitFn,
					matchErr:       Succeed(),
					matchMutated:   BeTrue(),
					matchHeadID:    Not(Equal(git.ObjectID{})),
					matchCommitDefPtr: PointTo(GetCommitDefMatcher(&CommitDef{
						Tree: TreeDef{Subtrees: map[string]TreeDef{"pods": {Blobs: map[string][]byte{"nginx": []byte(`apiVersion: v1
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
						Parents: []CommitDef{*ours, *theirs},
					})),
				}
			}(),
		} {
			func(s spec) {
				Describe(
					fmt.Sprintf(
						"conflictResolution=%d, retentionPolicy=%v, ours=%v, theirs=%v, noFastForward=%t",
						s.conflictResolution,
						s.retentionPolicy,
						s.ours,
						s.theirs,
						s.noFastForward,
					),
					func() {
						var (
							parentCtx    context.Context
							ours, theirs git.Commit
						)

						BeforeEach(func() {
							for _, c := range []struct {
								cPtr *git.Commit
								cd   *CommitDef
							}{
								{cPtr: &ours, cd: s.ours},
								{cPtr: &theirs, cd: s.theirs},
							} {
								Expect(
									func(cPtr *git.Commit, cd *CommitDef) (err error) {
										if cd == nil {
											return
										}

										*cPtr, err = CreateAndLoadCommitFromDef(ctx, repo, cd)
										return
									}(c.cPtr, c.cd),
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
							for _, c := range []git.Commit{ours, theirs} {
								if c != nil {
									Expect(c.Close()).To(Succeed())
								}
							}
						})

						It(ItSpecForMatchError(s.matchErr), func() {
							var (
								mutated bool
								headID  git.ObjectID
								err     error
							)

							mutated, headID, err = merger.MergeCommits(ctx, ours, theirs, s.noFastForward, s.createCommitFn)

							for _, e := range []struct {
								spec   string
								actual interface{}
								match  types.GomegaMatcher
							}{
								{spec: "error", actual: err, match: s.matchErr},
								{spec: "mutated", actual: mutated, match: s.matchMutated},
								{spec: "headID", actual: headID, match: s.matchHeadID},
							} {
								func(spec string, actual interface{}, match types.GomegaMatcher) {
									Expect(actual).To(match, spec)
								}(e.spec, e.actual, e.match)
							}

							if s.matchCommitDefPtr != nil {
								Expect(GetCommitDefByID(parentCtx, repo, headID)).To(s.matchCommitDefPtr, "commit")
							}
						})
					},
				)
			}(s)
		}
	})
})
