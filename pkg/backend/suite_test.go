package backend

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"testing"
)

func TestBackend(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Backend Suite")
}

type metaHeadFunc func(context.Context, git.Repository) (git.Commit, error)

type expectMetaHeadFunc func(ctx context.Context, repo git.Repository, newMetaHead *CommitDef, spec string)

func metaHeadFrom(metaHead, dataHead *CommitDef) metaHeadFunc {
	return func(ctx context.Context, repo git.Repository) (mh git.Commit, err error) {
		if metaHead == nil {
			return
		}

		metaHead = metaHead.DeepCopy()

		if dataHead != nil {
			var dID git.ObjectID

			if dID, err = CreateCommitFromDef(ctx, repo, dataHead); err != nil {
				return
			}

			if metaHead.Tree.Blobs == nil {
				metaHead.Tree.Blobs = map[string][]byte{}
			}

			metaHead.Tree.Blobs[metadataPathData] = []byte(dID.String())
		}

		mh, err = CreateAndLoadCommitFromDef(ctx, repo, metaHead)
		return
	}
}

func expectMetaHead(em, ed *CommitDef) expectMetaHeadFunc {
	return func(ctx context.Context, repo git.Repository, am *CommitDef, spec string) {
		var dID git.ObjectID

		Expect(func() (err error) {
			if bData, ok := am.Tree.Blobs[metadataPathData]; ok {
				dID, err = git.NewObjectID(string(bData))
			}

			return
		}()).To(Succeed(), spec)

		em, am = em.DeepCopy(), am.DeepCopy()
		delete(em.Tree.Blobs, metadataPathData)
		delete(am.Tree.Blobs, metadataPathData)

		Expect(*am).To(GetCommitDefMatcher(em), spec)

		if ed == nil {
			Expect(dID).To(Equal(git.ObjectID{}), spec)
		} else {
			Expect(dID).ToNot(Equal(git.ObjectID{}), spec)
			Expect(*GetCommitDefByID(ctx, repo, dID)).To(GetCommitDefMatcher(ed), spec)
		}
	}
}

func expectMetaHeadInherit(em, ed *CommitDef, dataMutated bool) expectMetaHeadFunc {
	return func(ctx context.Context, repo git.Repository, am *CommitDef, spec string) {
		var emNoParents, amNoParents = em.DeepCopy(), am.DeepCopy()

		emNoParents.Parents, amNoParents.Parents = nil, nil

		expectMetaHead(emNoParents, ed)(ctx, repo, amNoParents, spec)

		Expect(am.Parents).To(HaveLen(len(em.Parents)), spec)

		if len(em.Parents) <= 0 {
			Expect(am.Parents).To(BeEmpty(), spec)
		} else {
			Expect(am.Parents).To(HaveLen(1), spec)

			if dataMutated {
				Expect(ed.Parents).To(HaveLen(1), spec)
				expectMetaHead(&em.Parents[0], &ed.Parents[0])(ctx, repo, &am.Parents[0], spec)
			} else {
				expectMetaHead(&em.Parents[0], ed)(ctx, repo, &am.Parents[0], spec)
			}
		}
	}
}

func delegateToMatcher(match types.GomegaMatcher) expectMetaHeadFunc {
	return func(_ context.Context, _ git.Repository, am *CommitDef, spec string) { Expect(am).To(match, spec) }
}

var allMetaKeys = []string{
	etcdserverpb.Compare_CREATE.String(),
	etcdserverpb.Compare_LEASE.String(),
	etcdserverpb.Compare_MOD.String(),
	etcdserverpb.Compare_VERSION.String(),
}
