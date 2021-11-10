package git2go

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/trishanku/gitcd/pkg/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"

	"testing"
)

func TestGit2Go(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "git2go Suite")
}

func createEmptyObjects(ctx context.Context, repo git.Repository) (blobID, treeID, commitID git.ObjectID, err error) {
	if blobID, err = CreateBlob(ctx, repo, []byte{}); err != nil {
		return
	}

	if treeID, err = CreateTreeFromDef(ctx, repo, &TreeDef{}); err != nil {
		return
	}

	commitID, err = CreateCommitFromDef(ctx, repo, &CommitDef{})
	return
}
