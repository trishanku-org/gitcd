package git2go

import (
	"context"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
)

var _ = Describe("References", func() {
	var (
		ctx     context.Context
		repo    git.Repository
		rc      git.ReferenceCollection
		refName = git.ReferenceName("refs/heads/main")
	)

	BeforeEach(func() {
		var dir string

		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { repo, err = New().OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(repo).ToNot(BeNil())

		Expect(func() (err error) { rc, err = repo.References(); return }()).To(Succeed())
		Expect(rc).ToNot(BeNil())
	})

	AfterEach(func() {
		if rc != nil {
			Expect(rc.Close()).To(Succeed())
		}

		if repo != nil {
			Expect(repo.Close()).To(Succeed())
		}
	})

	Describe("Get", func() {
		var matchRef, matchErr types.GomegaMatcher

		JustBeforeEach(func() {
			var ref, err = rc.Get(ctx, refName)

			Expect(err).To(matchErr)
			Expect(ref).To(matchRef)
		})

		Describe("with expired context", func() {
			BeforeEach(func() {
				var cancelFn context.CancelFunc

				ctx, cancelFn = context.WithCancel(ctx)
				cancelFn()

				matchErr = MatchError(ctx.Err())
				matchRef = BeNil()
			})

			itShouldFail()
		})

		Describe("non-existing reference", func() {
			BeforeEach(func() {
				matchErr = HaveOccurred()
				matchRef = BeNil()
			})

			itShouldFail()
		})

		Describe("existing reference", func() {
			BeforeEach(func() {
				Expect(func() (err error) {
					var (
						bb  git.BlobBuilder
						bID git.ObjectID
					)

					if bb, err = repo.BlobBuilder(ctx); err != nil {
						return
					}

					defer bb.Close()

					if bID, err = bb.Build(ctx); err != nil {
						return
					}

					err = rc.Create(ctx, refName, bID, true, "")

					return
				}()).To(Succeed())

				matchErr = Succeed()
				matchRef = Not(BeNil())
			})

			itShouldSucceed()
		})
	})

	Describe("Create", func() {
		var (
			force    bool
			matchErr types.GomegaMatcher
			bID      git.ObjectID
		)

		BeforeEach(func() {
			Expect(func() (err error) { bID, err = createBlob(ctx, repo, []byte{}); return }()).To(Succeed())
		})

		JustBeforeEach(func() {
			Expect(rc.Create(ctx, refName, bID, force, "")).To(matchErr)
		})

		Describe("with expired context", func() {
			BeforeEach(func() {
				var cancelFn context.CancelFunc

				ctx, cancelFn = context.WithCancel(ctx)
				cancelFn()

				matchErr = MatchError(ctx.Err())
			})

			itShouldFail()
		})

		Describe("non-existing reference", func() {
			BeforeEach(func() {
				matchErr = Succeed()
			})

			Describe("force == false", func() {
				BeforeEach(func() {
					force = false
				})

				itShouldSucceed()
			})

			Describe("force == true", func() {
				BeforeEach(func() {
					force = true
				})

				itShouldSucceed()
			})
		})

		Describe("existing reference", func() {
			BeforeEach(func() {
				Expect(func() (err error) {
					var (
						bb  git.BlobBuilder
						bID git.ObjectID
					)

					if bb, err = repo.BlobBuilder(ctx); err != nil {
						return
					}

					defer bb.Close()

					if bID, err = bb.Build(ctx); err != nil {
						return
					}

					err = rc.Create(ctx, refName, bID, false, "")

					return
				}()).To(Succeed())
			})

			Describe("force == false", func() {
				BeforeEach(func() {
					force = false

					matchErr = HaveOccurred()
				})

				itShouldFail()
			})

			Describe("force == true", func() {
				BeforeEach(func() {
					force = true

					matchErr = Succeed()
				})

				itShouldSucceed()
			})
		})
	})
})
