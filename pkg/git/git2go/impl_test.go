package git2go

import (
	"context"
	"io/ioutil"
	"os"
	"path"

	impl "github.com/libgit2/git2go/v31"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
)

var _ = Describe("New", func() {
	var (
		git2go git.Interface
		repo   git.Repository
	)

	BeforeEach(func() {
		git2go = New()
		Expect(git2go).ToNot(BeNil())
	})

	Describe("OpenOrInitBareRepository", func() {
		var (
			ctx                 context.Context
			baseDir, dir        string
			matchRepo, matchErr types.GomegaMatcher

			itShouldBeAValidGitRepo = func(spec string, gitDir func() string) {
				It(spec, func() {
					var (
						fis, err = os.ReadDir(gitDir())
						fNames   []string
					)

					Expect(err).ToNot(HaveOccurred())
					Expect(fis).ToNot(BeEmpty())

					for _, fi := range fis {
						fNames = append(fNames, fi.Name())
					}

					Expect(fNames).To(ConsistOf("config", "description", "hooks", "info", "objects", "refs", "HEAD"))
				})
			}
		)

		BeforeEach(func() {
			ctx = context.Background()

			Expect(func() (err error) { baseDir, err = ioutil.TempDir("", "OpenOrInitBareRepository"); return }()).To(Succeed())
			Expect(baseDir).ToNot(BeEmpty())
		})

		JustBeforeEach(func() {
			var err error

			repo, err = git2go.OpenOrInitBareRepository(ctx, dir)

			Expect(err).To(matchErr)
			Expect(repo).To(matchRepo)

		})

		AfterEach(func() {
			if repo != nil {
				Expect(repo.Close()).To(Succeed())
			}

			if len(baseDir) > 0 {
				Expect(os.RemoveAll(baseDir))
			}
		})

		Describe("dir does not exist", func() {
			BeforeEach(func() {
				dir = path.Join(baseDir, "repo")

				matchRepo = Not(BeNil())
				matchErr = Succeed()
			})

			Describe("repo does not exist", func() {
				itShouldBeAValidGitRepo("should init a bare Git repo", func() string { return dir })
			})
		})

		Describe("dir exists", func() {
			BeforeEach(func() {
				dir = baseDir

				matchRepo = Not(BeNil())
				matchErr = Succeed()
			})

			Describe("repo does not exist", func() {
				itShouldBeAValidGitRepo("should init a bare Git repo", func() string { return dir })
			})

			Describe("repo exists", func() {
				var createRepo = func(bare bool) (err error) {
					_, err = impl.InitRepository(dir, bare)
					return
				}

				Describe("with bare == true", func() {
					BeforeEach(func() {
						Expect(createRepo(true)).To(Succeed())
					})

					itShouldBeAValidGitRepo("should open a bare Git repo", func() string { return dir })
				})

				Describe("with bare == false", func() {
					BeforeEach(func() {
						Expect(createRepo(false)).To(Succeed())
					})

					itShouldBeAValidGitRepo("should open a non-bare Git repo", func() string { return path.Join(dir, ".git") })
				})
			})
		})
	})
})
