package git2go

import (
	"context"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/trishanku/gitcd/pkg/git"
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
})
