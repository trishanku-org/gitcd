package tests_util

import (
	"context"
	"reflect"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
)

func NOP()             {}
func ItShouldSucceed() { ginkgo.It("should succeed", NOP) }
func ItShouldFail()    { ginkgo.It("should fail", NOP) }

var emptyTime = time.Time{}

func CreateBlob(ctx context.Context, repo git.Repository, content []byte) (id git.ObjectID, err error) {
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

func CreateAndLoadBlob(ctx context.Context, repo git.Repository, content []byte) (b git.Blob, err error) {
	var id git.ObjectID

	if id, err = CreateBlob(ctx, repo, content); err != nil {
		return
	}

	b, err = repo.ObjectGetter().GetBlob(ctx, id)
	return
}

func GetBlobContent(ctx context.Context, repo git.Repository, id git.ObjectID) (content []byte, err error) {
	var b git.Blob

	if b, err = repo.ObjectGetter().GetBlob(ctx, id); err != nil {
		return
	}

	defer b.Close()

	content, err = b.Content()
	return
}

type TreeDef struct {
	Blobs    map[string][]byte
	Subtrees map[string]TreeDef
}

func (s *TreeDef) DeepCopy() (t *TreeDef) {
	t = new(TreeDef)

	if len(s.Blobs) > 0 {
		t.Blobs = make(map[string][]byte, len(s.Blobs))
		for k, sv := range s.Blobs {
			var tv = make([]byte, len(sv))
			copy(tv, sv)
			t.Blobs[k] = tv
		}
	}

	if len(s.Subtrees) > 0 {
		t.Subtrees = make(map[string]TreeDef, len(s.Subtrees))
		for k, sv := range s.Subtrees {
			t.Subtrees[k] = *sv.DeepCopy()
		}
	}

	return
}

func CreateTreeFromDef(ctx context.Context, repo git.Repository, td *TreeDef) (id git.ObjectID, err error) {
	var tb git.TreeBuilder

	if tb, err = repo.TreeBuilder(ctx); err != nil {
		return
	}

	defer tb.Close()

	for name, content := range td.Blobs {
		var bID git.ObjectID

		if bID, err = CreateBlob(ctx, repo, content); err != nil {
			return
		}

		if err = tb.AddEntry(name, bID, git.FilemodeBlob); err != nil {
			return
		}
	}

	for name, td := range td.Subtrees {
		var tID git.ObjectID

		if tID, err = CreateTreeFromDef(ctx, repo, &td); err != nil {
			return
		}

		if err = tb.AddEntry(name, tID, git.FilemodeTree); err != nil {
			return
		}
	}

	id, err = tb.Build(ctx)
	return
}

func CreateAndLoadTreeFromDef(ctx context.Context, repo git.Repository, td *TreeDef) (t git.Tree, err error) {
	var id git.ObjectID

	if id, err = CreateTreeFromDef(ctx, repo, td); err != nil {
		return
	}

	t, err = repo.ObjectGetter().GetTree(ctx, id)
	return
}

func GetTreeDefMatcher(td *TreeDef) types.GomegaMatcher {
	var (
		blobKeys    = gstruct.Keys{}
		subtreeKeys = gstruct.Keys{}
	)

	for name, content := range td.Blobs {
		blobKeys[name] = gomega.Equal(func() []byte { return content }())
	}

	for name, td := range td.Subtrees {
		subtreeKeys[name] = GetTreeDefMatcher(&td)
	}

	return gstruct.MatchAllFields(gstruct.Fields{
		"Blobs":    gstruct.MatchAllKeys(blobKeys),
		"Subtrees": gstruct.MatchAllKeys(subtreeKeys),
	})
}

func GetTreeDef(ctx context.Context, repo git.Repository, id git.ObjectID) (td *TreeDef) {
	var t git.Tree

	gomega.Expect(func() (err error) { t, err = repo.ObjectGetter().GetTree(ctx, id); return }()).To(gomega.Succeed())

	defer t.Close()

	td = GetTreeDefByTree(ctx, repo, t)

	return
}

func GetTreeDefByTree(ctx context.Context, repo git.Repository, t git.Tree) (td *TreeDef) {
	td = &TreeDef{Blobs: map[string][]byte{}, Subtrees: map[string]TreeDef{}}

	gomega.Expect(func() (err error) {
		err = t.ForEachEntry(ctx, func(_ context.Context, te git.TreeEntry) (done bool, err error) {
			if te.EntryType() == git.ObjectTypeBlob {
				var content []byte

				if content, err = GetBlobContent(ctx, repo, te.EntryID()); err != nil {
					return
				}

				td.Blobs[te.EntryName()] = content
				return
			}

			td.Subtrees[te.EntryName()] = *GetTreeDef(ctx, repo, te.EntryID())
			return
		})
		return
	}()).To(gomega.Succeed())

	return
}

type CommitDef struct {
	Message                                                string
	AuthorName, AuthorEmail, CommitterName, CommitterEmail string
	AuthorTime, CommitterTime                              time.Time
	Tree                                                   TreeDef
	Parents                                                []CommitDef
}

func (s *CommitDef) DeepCopy() (t *CommitDef) {
	t = new(CommitDef)
	*t = *s

	t.Tree = *s.Tree.DeepCopy()

	if len(s.Parents) > 0 {
		t.Parents = make([]CommitDef, len(s.Parents))
		for i, sp := range s.Parents {
			t.Parents[i] = *sp.DeepCopy()
		}
	}

	return
}

func CreateCommitFromDef(ctx context.Context, repo git.Repository, cd *CommitDef) (id git.ObjectID, err error) {
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

		if id, err = CreateTreeFromDef(ctx, repo, &cd.Tree); err != nil {
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

			if id, err = CreateCommitFromDef(ctx, repo, &cd); err != nil {
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

func CreateAndLoadCommitFromDef(ctx context.Context, repo git.Repository, cd *CommitDef) (c git.Commit, err error) {
	var id git.ObjectID

	if id, err = CreateCommitFromDef(ctx, repo, cd); err != nil {
		return
	}

	c, err = repo.ObjectGetter().GetCommit(ctx, id)
	return
}

func GetCommitDefMatcher(cd *CommitDef) types.GomegaMatcher {
	var (
		parentMatchers = make([]interface{}, len(cd.Parents))
	)

	for i, cd := range cd.Parents {
		parentMatchers[i] = GetCommitDefMatcher(&cd)
	}

	return gstruct.MatchAllFields(gstruct.Fields{
		"AuthorName":     gomega.BeEmpty(),
		"AuthorEmail":    gomega.BeEmpty(),
		"CommitterTime":  gomega.Equal(emptyTime),
		"CommitterName":  gomega.BeEmpty(),
		"CommitterEmail": gomega.BeEmpty(),
		"AuthorTime":     gomega.Equal(emptyTime),
		"Message":        gomega.Equal(cd.Message),
		"Tree":           GetTreeDefMatcher(&cd.Tree),
		"Parents":        gomega.ConsistOf(parentMatchers...),
	})
}

func GetCommitDefByID(ctx context.Context, repo git.Repository, id git.ObjectID) (cd *CommitDef) {
	var c git.Commit

	gomega.Expect(func() (err error) { c, err = repo.ObjectGetter().GetCommit(ctx, id); return }()).To(gomega.Succeed())

	defer c.Close()

	cd = &CommitDef{}

	cd.Message = c.Message()
	cd.Tree = *GetTreeDef(ctx, repo, c.TreeID())

	gomega.Expect(c.ForEachParentID(ctx, func(_ context.Context, id git.ObjectID) (done bool, err error) {
		cd.Parents = append(cd.Parents, *GetCommitDefByID(ctx, repo, id))
		return
	})).To(gomega.Succeed())

	return
}

func GetCommitDefByCommit(ctx context.Context, repo git.Repository, c git.Commit) (cd *CommitDef) {
	cd = &CommitDef{}

	cd.Message = c.Message()
	cd.Tree = *GetTreeDef(ctx, repo, c.TreeID())

	gomega.Expect(c.ForEachParent(ctx, func(_ context.Context, c git.Commit) (done bool, err error) {
		cd.Parents = append(cd.Parents, *GetCommitDefByCommit(ctx, repo, c))
		return
	})).To(gomega.Succeed())

	return
}

func CreateReferenceFromCommitID(ctx context.Context, repo git.Repository, refName git.ReferenceName, id git.ObjectID) (err error) {
	var rc git.ReferenceCollection

	if rc, err = repo.References(); err != nil {
		return
	}

	defer rc.Close()

	err = rc.Create(ctx, refName, id, false, id.String())
	return
}

func CreateReferenceFromDef(ctx context.Context, repo git.Repository, refName git.ReferenceName, cd *CommitDef) (err error) {
	var (
		rc git.ReferenceCollection
		id git.ObjectID
	)

	if rc, err = repo.References(); err != nil {
		return
	}

	defer rc.Close()

	if id, err = CreateCommitFromDef(ctx, repo, cd); err != nil {
		return
	}

	err = rc.Create(ctx, refName, id, false, id.String())
	return
}

func CreateAndLoadReferenceFromDef(ctx context.Context, repo git.Repository, refName git.ReferenceName, cd *CommitDef) (p git.Peelable, err error) {
	var rc git.ReferenceCollection

	if err = CreateReferenceFromDef(ctx, repo, refName, cd); err != nil {
		return
	}

	if rc, err = repo.References(); err != nil {
		return
	}

	defer rc.Close()

	p, err = rc.Get(ctx, refName)
	return
}

func GetCommitDefForReferenceName(ctx context.Context, repo git.Repository, refName git.ReferenceName) (cd *CommitDef, err error) {
	var (
		rc git.ReferenceCollection
		p  git.Peelable
		c  git.Commit
	)

	if rc, err = repo.References(); err != nil {
		return
	}

	defer rc.Close()

	if p, err = rc.Get(ctx, refName); err != nil {
		return
	}

	defer p.Close()

	if c, err = repo.Peeler().PeelToCommit(ctx, p); err != nil {
		return
	}

	defer c.Close()

	cd = GetCommitDefByCommit(ctx, repo, c)
	return
}

type ContextFunc func(context.Context) context.Context

var _ ContextFunc = CanceledContext

func CanceledContext(parent context.Context) (ctx context.Context) {
	var cancelFn context.CancelFunc

	ctx, cancelFn = context.WithCancel(parent)
	defer cancelFn()

	return
}

func ItSpecForMatchError(matchErr types.GomegaMatcher) string {
	switch {
	case reflect.DeepEqual(matchErr, gomega.Succeed()):
		return "should succeed"
	default:
		return "should fail"
	}
}
