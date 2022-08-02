package git2go

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	net_url "net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
)

var _ = Describe("Remotes", func() {
	var (
		ctx        context.Context
		repo       git.Repository
		dir        string
		rc         git.RemoteCollection
		remoteName = git.RemoteName("remote1")

		getImpl = func(rc git.RemoteCollection) (impl *remoteCollection, err error) {
			var ok bool

			if impl, ok = rc.(*remoteCollection); !ok {
				err = fmt.Errorf("Unsupported implementation %T", rc)
			}

			return
		}
	)

	BeforeEach(func() {
		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { repo, err = New().OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(repo).ToNot(BeNil())

		Expect(func() (err error) { rc, err = repo.Remotes(); return }()).To(Succeed())
		Expect(rc).ToNot(BeNil())
	})

	AfterEach(func() {
		if rc != nil {
			Expect(rc.Close()).To(Succeed())
		}

		if repo != nil {
			Expect(repo.Close()).To(Succeed())
		}

		if len(dir) > 0 {
			Expect(os.RemoveAll(dir))
		}
	})

	Describe("Get", func() {
		var matchRemote, matchErr types.GomegaMatcher

		JustBeforeEach(func() {
			var remote, err = rc.Get(ctx, remoteName)

			Expect(err).To(matchErr)
			Expect(remote).To(matchRemote)

			if remote != nil {
				Expect(remote.Close()).To(Succeed())
			}
		})

		Describe("with expired context", func() {
			BeforeEach(func() {
				var cancelFn context.CancelFunc

				ctx, cancelFn = context.WithCancel(ctx)
				cancelFn()

				matchErr = MatchError(ctx.Err())
				matchRemote = BeNil()
			})

			ItShouldFail()
		})

		Describe("non-existing remote", func() {
			BeforeEach(func() {
				matchErr = HaveOccurred()
				matchRemote = BeNil()
			})

			ItShouldFail()
		})

		Describe("existing remote", func() {
			BeforeEach(func() {
				var (
					impl   *remoteCollection
					remote freeable
				)

				Expect(func() (err error) { impl, err = getImpl(rc); return }()).To(Succeed())

				Expect(func() (err error) {
					remote, err = impl.impl().Create(string(remoteName), "https://invalid.url/")
					return
				}()).To(Succeed())

				Expect(remote).ToNot(BeNil())

				defer remote.Free()

				matchErr = Succeed()
				matchRemote = Not(BeNil())
			})

			ItShouldSucceed()
		})
	})

	Describe("Remote", func() {
		var (
			remoteName = git.RemoteName("remote1")
			url        string
			remote     git.Remote
			parentCtx  context.Context
		)

		BeforeEach(func() {
			parentCtx = ctx
		})

		JustBeforeEach(func() {
			var (
				impl *remoteCollection
				rf   freeable
			)

			Expect(func() (err error) { impl, err = getImpl(rc); return }()).To(Succeed())

			Expect(func() (err error) {
				rf, err = impl.impl().Create(string(remoteName), url)
				return
			}()).To(Succeed())

			Expect(rf).ToNot(BeNil())

			rf.Free()

			Expect(func() (_ git.Remote, err error) { remote, err = rc.Get(parentCtx, remoteName); return remote, err }()).ToNot(BeNil())
		})

		AfterEach(func() {
			if remote != nil {
				Expect(remote.Close()).To(Succeed())
			}
		})

		Describe("Fetch", func() {
			var (
				refSpecs []git.RefSpec
				matchErr types.GomegaMatcher
				expectFn func(context.Context)
			)

			JustBeforeEach(func() {
				Expect(remote.Fetch(ctx, refSpecs)).To(matchErr)
				if expectFn != nil {
					expectFn(parentCtx)
				}
			})

			Describe("with expired context", func() {
				BeforeEach(func() {
					var cancelFn context.CancelFunc

					ctx, cancelFn = context.WithCancel(ctx)
					cancelFn()

					url = "http://invalid.url"
					matchErr = MatchError(context.Canceled)
					expectFn = func(_ context.Context) {
						Expect(remote).ToNot(BeNil())
						Expect(remote.Name()).To(Equal(remoteName))
					}
				})

				ItShouldFail()
			})

			Describe("remote with invalid URL", func() {
				BeforeEach(func() {
					url = "http://invalid.url"
					matchErr = MatchError(ContainSubstring("invalid.url"))
					expectFn = func(_ context.Context) {
						Expect(remote).ToNot(BeNil())
						Expect(remote.Name()).To(Equal(remoteName))
					}
				})

				ItShouldFail()
			})

			Describe("valid remote", func() {
				var (
					remoteDir  string
					remoteRepo git.Repository
					expectedID git.ObjectID
				)

				BeforeEach(func() {
					Expect(func() (err error) { remoteDir, err = ioutil.TempDir("", "remote"); return }()).To(Succeed())
					Expect(remoteDir).ToNot(BeEmpty())

					Expect(func() (err error) { remoteRepo, err = New().OpenOrInitBareRepository(ctx, remoteDir); return }()).To(Succeed())
					Expect(remoteRepo).ToNot(BeNil())

					Expect(func() (err error) {
						var (
							bb   git.BlobBuilder
							bID  git.ObjectID
							refC git.ReferenceCollection
						)

						if bb, err = remoteRepo.BlobBuilder(ctx); err != nil {
							return
						}

						defer bb.Close()

						if bID, err = bb.Build(ctx); err != nil {
							return
						}

						expectedID = bID

						if refC, err = remoteRepo.References(); err != nil {
							return
						}

						defer refC.Close()

						err = refC.Create(ctx, "refs/heads/main", bID, false, "")

						return
					}()).To(Succeed())

					url = (&net_url.URL{Scheme: "file", Path: remoteDir}).String()
					matchErr = Succeed()
					expectFn = func(_ context.Context) {
						var ref git.Reference

						Expect(remote).ToNot(BeNil())
						Expect(remote.Name()).To(Equal(remoteName))

						Expect(func() (err error) {
							var refC git.ReferenceCollection

							if refC, err = repo.References(); err != nil {
								return
							}

							defer refC.Close()

							ref, err = refC.Get(parentCtx, "refs/remotes/remote1/main")

							return
						}()).To(Succeed())

						Expect(ref).ToNot(BeNil())
						Expect(ref.IsRemote()).To(BeTrue())
						Expect(func() (_ git.ObjectID, err error) {
							var o git.Object

							if o, err = repo.Peeler().PeelToBlob(parentCtx, ref); err != nil {
								return
							}

							defer o.Close()

							return o.ID(), nil
						}()).To(Equal(expectedID))
					}
				})

				AfterEach(func() {
					if remoteRepo != nil {
						Expect(remoteRepo.Close()).To(Succeed())
					}

					if len(remoteDir) > 0 {
						Expect(os.RemoveAll(remoteDir))
					}
				})

				ItShouldSucceed()
			})
		})
	})
})
