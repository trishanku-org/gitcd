package git2go

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"

	"testing"
)

func TestGit2Go(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "git2go Suite")
}

var emptyTime = time.Time{}

func nop()             {}
func itShouldSucceed() { It("should succeed", nop) }
func itShouldFail()    { It("should fail", nop) }

func createBlob(ctx context.Context, repo git.Repository, content []byte) (id git.ObjectID, err error) {
	var (
		contentCopy = make([]byte, len(content))
		bb          git.BlobBuilder
	)

	copy(contentCopy, content)

	if bb, err = repo.BlobBuilder(ctx); err != nil {
		return
	}

	defer bb.Close()

	if err = bb.SetContent(contentCopy); err != nil {
		return
	}

	id, err = bb.Build(ctx)
	return
}

func getBlobContent(ctx context.Context, repo git.Repository, id git.ObjectID) (content []byte, err error) {
	var b git.Blob

	if b, err = repo.ObjectGetter().GetBlob(ctx, id); err != nil {
		return
	}

	defer b.Close()

	content, err = b.Content()
	return
}

type treeDef struct {
	Blobs    map[string][]byte
	Subtrees map[string]treeDef
}

func createTreeFromDef(ctx context.Context, repo git.Repository, td *treeDef) (id git.ObjectID, err error) {
	var tb git.TreeBuilder

	if tb, err = repo.TreeBuilder(ctx); err != nil {
		return
	}

	defer tb.Close()

	for name, content := range td.Blobs {
		var bID git.ObjectID

		if bID, err = createBlob(ctx, repo, content); err != nil {
			return
		}

		if err = tb.AddEntry(name, bID, git.FilemodeBlob); err != nil {
			return
		}
	}

	for name, td := range td.Subtrees {
		var tID git.ObjectID

		if tID, err = createTreeFromDef(ctx, repo, &td); err != nil {
			return
		}

		if err = tb.AddEntry(name, tID, git.FilemodeTree); err != nil {
			return
		}
	}

	id, err = tb.Build(ctx)
	return
}

func getTreeDefMatcher(td *treeDef) types.GomegaMatcher {
	var (
		blobKeys    = Keys{}
		subtreeKeys = Keys{}
	)

	for name, content := range td.Blobs {
		blobKeys[name] = Equal(func() []byte { return content }())
	}

	for name, td := range td.Subtrees {
		subtreeKeys[name] = getTreeDefMatcher(&td)
	}

	return MatchAllFields(Fields{
		"Blobs":    MatchAllKeys(blobKeys),
		"Subtrees": MatchAllKeys(subtreeKeys),
	})
}

func getTreeDef(ctx context.Context, repo git.Repository, id git.ObjectID) (td *treeDef) {
	var t git.Tree

	Expect(func() (err error) { t, err = repo.ObjectGetter().GetTree(ctx, id); return }()).To(Succeed())

	defer t.Close()

	td = &treeDef{Blobs: map[string][]byte{}, Subtrees: map[string]treeDef{}}

	Expect(func() (err error) {
		err = t.ForEachEntry(ctx, func(_ context.Context, te git.TreeEntry) (done bool, err error) {
			if te.EntryType() == git.ObjectTypeBlob {
				var content []byte

				if content, err = getBlobContent(ctx, repo, te.EntryID()); err != nil {
					return
				}

				td.Blobs[te.EntryName()] = content
				return
			}

			td.Subtrees[te.EntryName()] = *getTreeDef(ctx, repo, te.EntryID())
			return
		})
		return
	}()).To(Succeed())

	return
}

type commitDef struct {
	Message                                                string
	AuthorName, AuthorEmail, CommitterName, CommitterEmail string
	AuthorTime, CommitterTime                              time.Time
	Tree                                                   treeDef
	Parents                                                []commitDef
}

func createCommitFromDef(ctx context.Context, repo git.Repository, cd *commitDef) (id git.ObjectID, err error) {
	var cb git.CommitBuilder

	if cb, err = repo.CommitBuilder(ctx); err != nil {
		return
	}

	defer cb.Close()

	if err = cb.SetAuthorName(cd.AuthorName); err != nil {
		return
	}

	if err = cb.SetAuthorEmail(cd.AuthorEmail); err != nil {
		return
	}

	if err = cb.SetAuthorTime(cd.AuthorTime); err != nil {
		return
	}

	if err = cb.SetCommitterName(cd.CommitterName); err != nil {
		return
	}

	if err = cb.SetCommitterEmail(cd.CommitterEmail); err != nil {
		return
	}

	if err = cb.SetCommitterTime(cd.CommitterTime); err != nil {
		return
	}

	if len(cd.AuthorName) == 0 && len(cd.CommitterName) == 0 {
		if err = cb.SetAuthorName("trishanku"); err != nil {
			return
		}
	}

	if len(cd.AuthorEmail) == 0 && len(cd.CommitterEmail) == 0 {
		if err = cb.SetAuthorEmail("trishanku@heaven.com"); err != nil {
			return
		}
	}

	if cd.AuthorTime == emptyTime && cd.CommitterTime == emptyTime {
		if err = cb.SetAuthorTime(time.Now()); err != nil {
			return
		}
	}

	if err = cb.SetMessage(cd.Message); err != nil {
		return
	}

	if err = func() (err error) {
		var id git.ObjectID

		if id, err = createTreeFromDef(ctx, repo, &cd.Tree); err != nil {
			return
		}

		err = cb.SetTreeID(id)
		return
	}(); err != nil {
		return
	}

	for _, cd := range cd.Parents {
		if err = func() (err error) {
			var id git.ObjectID

			if id, err = createCommitFromDef(ctx, repo, &cd); err != nil {
				return
			}

			err = cb.AddParentIDs(id)
			return
		}(); err != nil {
			return
		}
	}

	id, err = cb.Build(ctx)
	return
}

func getCommitDefMatcher(cd *commitDef) types.GomegaMatcher {
	var (
		parentMatchers = make([]interface{}, len(cd.Parents))
	)

	for i, cd := range cd.Parents {
		parentMatchers[i] = getCommitDefMatcher(&cd)
	}

	return MatchAllFields(Fields{
		"AuthorName":     BeEmpty(),
		"AuthorEmail":    BeEmpty(),
		"CommitterTime":  Equal(emptyTime),
		"CommitterName":  BeEmpty(),
		"CommitterEmail": BeEmpty(),
		"AuthorTime":     Equal(emptyTime),
		"Message":        Equal(cd.Message),
		"Tree":           getTreeDefMatcher(&cd.Tree),
		"Parents":        ConsistOf(parentMatchers...),
	})
}

func getCommitDefByID(ctx context.Context, repo git.Repository, id git.ObjectID) (cd *commitDef) {
	var c git.Commit

	Expect(func() (err error) { c, err = repo.ObjectGetter().GetCommit(ctx, id); return }()).To(Succeed())

	defer c.Close()

	cd = &commitDef{}

	cd.Message = c.Message()
	cd.Tree = *getTreeDef(ctx, repo, c.TreeID())

	Expect(c.ForEachParentID(ctx, func(_ context.Context, id git.ObjectID) (done bool, err error) {
		cd.Parents = append(cd.Parents, *getCommitDefByID(ctx, repo, id))
		return
	})).To(Succeed())

	return
}

func getCommitDefByCommit(ctx context.Context, repo git.Repository, c git.Commit) (cd *commitDef) {
	cd = &commitDef{}

	cd.Message = c.Message()
	cd.Tree = *getTreeDef(ctx, repo, c.TreeID())

	Expect(c.ForEachParent(ctx, func(_ context.Context, c git.Commit) (done bool, err error) {
		cd.Parents = append(cd.Parents, *getCommitDefByCommit(ctx, repo, c))
		return
	})).To(Succeed())

	return
}

func createEmptyObjects(ctx context.Context, repo git.Repository) (blobID, treeID, commitID git.ObjectID, err error) {
	if blobID, err = createBlob(ctx, repo, []byte{}); err != nil {
		return
	}

	if treeID, err = createTreeFromDef(ctx, repo, &treeDef{}); err != nil {
		return
	}

	commitID, err = createCommitFromDef(ctx, repo, &commitDef{})
	return
}
