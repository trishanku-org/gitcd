package backend

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"

	"testing"
)

func TestBackend(t *testing.T) {
	format.MaxLength, format.MaxDepth = 0, 100

	RegisterFailHandler(Fail)
	RunSpecs(t, "Backend Suite")
}

type metaHeadFunc func(context.Context, git.Repository) (git.Commit, error)

type expectMetaHeadTreeFunc func(ctx context.Context, repo git.Repository, newMetaHead *TreeDef, spec string)

type expectMetaHeadFunc func(ctx context.Context, repo git.Repository, newMetaHead *CommitDef, spec string)

func metaHeadFrom(metaHead, dataHead *CommitDef) metaHeadFunc {
	return func(ctx context.Context, repo git.Repository) (mh git.Commit, err error) {
		if metaHead == nil {
			return
		}

		metaHead = metaHead.DeepCopy()

		if dataHead != nil {
			var (
				dID         git.ObjectID
				metaParents []CommitDef
			)

			if dID, err = CreateCommitFromDef(ctx, repo, dataHead); err != nil {
				return
			}

			if metaHead.Tree.Blobs == nil {
				metaHead.Tree.Blobs = map[string][]byte{}
			}

			metaHead.Tree.Blobs[metadataPathData] = []byte(dID.String())

			for i, mpd := range metaHead.Parents {
				var (
					dpd *CommitDef
					mpc git.Commit
				)

				if i < len(dataHead.Parents) {
					dpd = &dataHead.Parents[i]
				}

				if dpd == nil {
					metaParents = append(metaParents, mpd)
					continue
				}

				if mpc, err = metaHeadFrom(&mpd, dpd)(ctx, repo); err != nil {
					return
				}

				defer mpc.Close()

				metaParents = append(metaParents, *GetCommitDefByCommit(ctx, repo, mpc))
			}

			metaHead.Parents = metaParents
		}

		mh, err = CreateAndLoadCommitFromDef(ctx, repo, metaHead)
		return
	}
}

func metaHeadInheritFrom(metaHead, dataHead *CommitDef) metaHeadFunc {
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

		for i := range metaHead.Parents {
			var (
				mp = &metaHead.Parents[i]
				dp *CommitDef
			)

			if dataHead != nil && i < len(dataHead.Parents) {
				dp = &dataHead.Parents[i]
			}

			if err = func() (err error) {
				var c git.Commit

				if c, err = metaHeadInheritFrom(mp, dp)(ctx, repo); err != nil {
					return
				}

				defer c.Close()

				mp = GetCommitDefByCommit(ctx, repo, c)
				return
			}(); err != nil {
				return
			}

			metaHead.Parents[i] = *mp
		}

		mh, err = CreateAndLoadCommitFromDef(ctx, repo, metaHead)
		return
	}
}

func deepCopyWithoutMetadataDataPath(mcd *CommitDef) *CommitDef {
	var parents = mcd.Parents

	mcd = mcd.DeepCopy()
	delete(mcd.Tree.Blobs, metadataPathData)

	for i, mpd := range parents {
		mcd.Parents[i] = *deepCopyWithoutMetadataDataPath(&mpd)
	}

	return mcd
}

func expectMetaHeadTree(em *TreeDef, ed *CommitDef) expectMetaHeadTreeFunc {
	return func(ctx context.Context, repo git.Repository, am *TreeDef, spec string) {
		var dID git.ObjectID

		Expect(func() (err error) {
			if bData, ok := am.Blobs[metadataPathData]; ok {
				dID, err = git.NewObjectID(string(bData))
			}

			return
		}()).To(Succeed(), spec)

		em, am = em.DeepCopy(), am.DeepCopy()
		delete(em.Blobs, metadataPathData)
		delete(am.Blobs, metadataPathData)

		Expect(*am).To(GetTreeDefMatcher(em), spec)

		if ed == nil {
			Expect(dID).To(Equal(git.ObjectID{}), spec)
		} else {
			Expect(dID).ToNot(Equal(git.ObjectID{}), spec)
			Expect(*GetCommitDefByID(ctx, repo, dID)).To(GetCommitDefMatcher(ed), fmt.Sprintf("%s-data", spec))
		}
	}
}

func expectMetaHead(em, ed *CommitDef, expectData bool) expectMetaHeadFunc {
	return func(ctx context.Context, repo git.Repository, am *CommitDef, spec string) {
		var dID git.ObjectID

		Expect(am).ToNot(BeNil(), spec)

		if expectData {
			Expect(am.Tree.Blobs).ToNot(BeEmpty(), spec)
			Expect(am.Tree.Blobs).To(HaveKey(metadataPathData), spec)
		}

		Expect(func() (err error) {
			if bData, ok := am.Tree.Blobs[metadataPathData]; ok {
				dID, err = git.NewObjectID(string(bData))
			}

			return
		}()).To(Succeed(), spec)

		em, am = deepCopyWithoutMetadataDataPath(em), deepCopyWithoutMetadataDataPath(am)

		Expect(*am).To(GetCommitDefMatcher(em), spec)

		if ed == nil {
			Expect(dID).To(Equal(git.ObjectID{}), spec)
		} else {
			Expect(dID).ToNot(Equal(git.ObjectID{}), spec)
			Expect(*GetCommitDefByID(ctx, repo, dID)).To(GetCommitDefMatcher(ed), fmt.Sprintf("%s-data", spec))
		}
	}
}

func expectMetaHeadInherit(em, ed *CommitDef, dataMutated bool) expectMetaHeadFunc {
	return func(ctx context.Context, repo git.Repository, am *CommitDef, spec string) {
		var emNoParents, amNoParents = em.DeepCopy(), am.DeepCopy()

		emNoParents.Parents, amNoParents.Parents = nil, nil

		expectMetaHead(emNoParents, ed, false)(ctx, repo, amNoParents, spec)

		Expect(am.Parents).To(HaveLen(len(em.Parents)), spec)

		if len(em.Parents) <= 0 {
			Expect(am.Parents).To(BeEmpty(), spec)
		} else {
			Expect(am.Parents).To(HaveLen(1), spec)

			if dataMutated {
				Expect(ed.Parents).To(HaveLen(1), spec)
				expectMetaHead(&em.Parents[0], &ed.Parents[0], false)(ctx, repo, &am.Parents[0], fmt.Sprintf("%s-data-mutated", spec))
			} else {
				expectMetaHead(&em.Parents[0], ed, false)(ctx, repo, &am.Parents[0], fmt.Sprintf("%s-data-not-mutated", spec))
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

func getTestLogger() (log logr.Logger) {
	Expect(func() (err error) {
		var zl *zap.Logger

		if zl, err = zap.NewDevelopment(); err != nil {
			return
		}

		log = zapr.NewLogger(zl)
		return
	}()).To(Succeed())

	return
}
