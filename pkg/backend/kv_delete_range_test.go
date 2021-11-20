package backend

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var _ = Describe("backend", func() {
	type metaHeadFunc func() (git.Commit, error)
	type expectMetaHeadFunc func(newMetaHead *CommitDef, spec string)

	var (
		b   *backend
		ctx context.Context
		dir string

		metaHeadFrom = func(metaHead, dataHead *CommitDef) metaHeadFunc {
			return func() (mh git.Commit, err error) {
				metaHead = metaHead.DeepCopy()

				if dataHead != nil {
					var dID git.ObjectID

					if dID, err = CreateCommitFromDef(ctx, b.repo, dataHead); err != nil {
						return
					}

					if metaHead.Tree.Blobs == nil {
						metaHead.Tree.Blobs = map[string][]byte{}
					}

					metaHead.Tree.Blobs[metadataPathData] = []byte(dID.String())
				}

				mh, err = CreateAndLoadCommitFromDef(ctx, b.repo, metaHead)
				return
			}
		}

		expectMetaHead = func(em, ed *CommitDef) expectMetaHeadFunc {
			return func(am *CommitDef, spec string) {
				var dID git.ObjectID

				Expect(func() (id git.ObjectID, err error) {
					dID, err = git.NewObjectID(string(am.Tree.Blobs[metadataPathData]))
					id = dID
					return
				}()).ToNot(Equal(git.ObjectID{}), spec)

				em, am = em.DeepCopy(), am.DeepCopy()
				delete(em.Tree.Blobs, metadataPathData)
				delete(am.Tree.Blobs, metadataPathData)

				Expect(*am).To(GetCommitDefMatcher(em), spec)
				Expect(*GetCommitDefByID(ctx, b.repo, dID)).To(GetCommitDefMatcher(ed), spec)
			}
		}

		expectMetaHeadInherit = func(em, ed *CommitDef) expectMetaHeadFunc {
			return func(am *CommitDef, spec string) {
				var emNoParents, amNoParents = em.DeepCopy(), am.DeepCopy()

				emNoParents.Parents, amNoParents.Parents = nil, nil

				expectMetaHead(emNoParents, ed)(amNoParents, spec)

				Expect(em.Parents).To(HaveLen(1), spec)
				Expect(am.Parents).To(HaveLen(1), spec)
				Expect(ed.Parents).To(HaveLen(1), spec)
				expectMetaHead(&em.Parents[0], &ed.Parents[0])(&am.Parents[0], spec)
			}
		}

		delegate = func(match types.GomegaMatcher) expectMetaHeadFunc {
			return func(am *CommitDef, spec string) { Expect(am).To(match, spec) }
		}

		allMetaKeys = []string{
			etcdserverpb.Compare_CREATE.String(),
			etcdserverpb.Compare_LEASE.String(),
			etcdserverpb.Compare_MOD.String(),
			etcdserverpb.Compare_VERSION.String(),
		}

		addMetaKeyEntry = func(td *TreeDef, key string, metaKeys []string, revision int64) {
			if len(metaKeys) <= 0 {
				return
			}

			td.Subtrees[key] = TreeDef{Blobs: map[string][]byte{}}

			for _, k := range metaKeys {
				td.Subtrees[key].Blobs[k] = []byte(revisionToString(revision))
			}
		}

		makeKeyValue = func(k, v []byte, metaKeys []string, revision int64) (kv *mvccpb.KeyValue) {
			kv = &mvccpb.KeyValue{Key: k, Value: v}

			for _, mk := range metaKeys {
				switch etcdserverpb.Compare_CompareTarget(etcdserverpb.Compare_CompareTarget_value[mk]) {
				case etcdserverpb.Compare_CREATE:
					kv.CreateRevision = revision
				case etcdserverpb.Compare_LEASE:
					kv.Lease = revision
				case etcdserverpb.Compare_MOD:
					kv.ModRevision = revision
				case etcdserverpb.Compare_VERSION:
					kv.Version = revision
				}
			}

			return
		}

		setupTrees func(
			depth int,
			metaKeys []string,
			currentMeta, currentData, newMeta, newData *TreeDef,
			oldRevision int64,
			prefix string,
			delRange interval,
			prevKvs []*mvccpb.KeyValue,
		) (newPrevKvs []*mvccpb.KeyValue)

		setupCommits = func(
			depth int,
			metaKeys []string,
			oldRevision, newRevision int64,
			prefix string,
			delRange interval,
		) (
			currentMetaHead, currentDataHead, newMetaHead, newDataHead *CommitDef,
			prevKvs []*mvccpb.KeyValue,
		) {
			currentMetaHead = &CommitDef{Message: revisionToString(oldRevision)}
			currentDataHead = &CommitDef{Message: revisionToString(oldRevision)}
			newMetaHead = &CommitDef{Message: revisionToString(newRevision)}
			newDataHead = &CommitDef{Message: revisionToString(newRevision)}
			prevKvs = setupTrees(
				depth,
				metaKeys,
				&currentMetaHead.Tree,
				&currentDataHead.Tree,
				&newMetaHead.Tree,
				&newDataHead.Tree,
				oldRevision,
				prefix,
				delRange,
				prevKvs,
			)

			if currentMetaHead.Tree.Blobs == nil {
				currentMetaHead.Tree.Blobs = map[string][]byte{}
			}

			if newMetaHead.Tree.Blobs == nil {
				newMetaHead.Tree.Blobs = map[string][]byte{}
			}

			currentMetaHead.Tree.Blobs[metadataPathRevision] = []byte(revisionToString(oldRevision))
			newMetaHead.Tree.Blobs[metadataPathRevision] = []byte(revisionToString(newRevision))

			return
		}
	)

	setupTrees = func(
		depth int,
		metaKeys []string,
		currentMeta, currentData, newMeta, newData *TreeDef,
		oldRevision int64,
		prefix string,
		delRange interval,
		prevKvs []*mvccpb.KeyValue,
	) (newPrevKvs []*mvccpb.KeyValue) {
		defer func() {
			newPrevKvs = prevKvs
		}()

		if depth <= 0 {
			return
		}

		currentData.Blobs, newData.Blobs = map[string][]byte{}, map[string][]byte{}
		if len(metaKeys) > 0 {
			currentMeta.Subtrees, newMeta.Subtrees = map[string]TreeDef{}, map[string]TreeDef{}
		}

		for i := 0; i < 10; i++ {
			var (
				si  = strconv.Itoa(i)
				key = []byte(path.Join(prefix, si))
				v   = []byte(si)
			)

			currentData.Blobs[si] = v
			addMetaKeyEntry(currentMeta, si, metaKeys, oldRevision)

			if delRange.Check(key) == checkResultInRange {
				prevKvs = append(prevKvs, makeKeyValue(key, v, metaKeys, oldRevision))

				continue
			}

			newData.Blobs[si] = v
			addMetaKeyEntry(newMeta, si, metaKeys, oldRevision)
		}

		if depth <= 1 {
			return // No subtrees for the last level
		}

		currentData.Subtrees, newData.Subtrees = map[string]TreeDef{}, map[string]TreeDef{}

		for c := byte('a'); c < byte('k'); c++ {
			var (
				sc                         = string(c)
				scmtd, scdtd, snmtd, sndtd = &TreeDef{}, &TreeDef{}, &TreeDef{}, &TreeDef{}
			)

			prevKvs = setupTrees(depth-1, metaKeys, scmtd, scdtd, snmtd, sndtd, oldRevision, path.Join(prefix, sc), delRange, prevKvs)

			if len(metaKeys) > 0 {
				currentMeta.Subtrees[sc] = *scmtd
			}

			currentData.Subtrees[sc] = *scdtd

			if len(sndtd.Blobs) > 0 {
				if len(metaKeys) > 0 {
					newMeta.Subtrees[sc] = *snmtd
				}
				newData.Subtrees[sc] = *sndtd
			}
		}

		return
	}

	BeforeEach(func() {
		var gitImpl = git2go.New()

		b = &backend{
			commitConfig: commitConfig{
				committerName:  "trishanku",
				committerEmail: "trishanku@heaven.com",
			},
		}

		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { b.repo, err = gitImpl.OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(b.repo).ToNot(BeNil())

		b.errors = gitImpl.Errors()
	})

	AfterEach(func() {
		if b.repo != nil {
			Expect(b.repo.Close()).To(Succeed())
		}

		if len(dir) > 0 {
			Expect(os.RemoveAll(dir))
		}
	})

	Describe("doDeleteRange", func() {
		type check struct {
			spec                                        string
			ctxFn                                       ContextFunc
			keyPrefix                                   keyPrefix
			metaHeadFn                                  metaHeadFunc
			req                                         *etcdserverpb.DeleteRangeRequest
			res                                         *etcdserverpb.DeleteRangeResponse
			newRevision                                 int64
			commitTreeFn                                func() commitTreeFunc
			matchErr                                    types.GomegaMatcher
			matchMetaMutated                            types.GomegaMatcher
			expectMetaHead                              expectMetaHeadFunc
			matchDataMutated, matchDataHeadCommitDefPtr types.GomegaMatcher
			matchResponse                               types.GomegaMatcher
		}

		var (
			replace = func() commitTreeFunc { return b.replaceCurrentCommit }
			inherit = func() commitTreeFunc { return b.inheritCurrentCommit }

			appendChecksWithIncompleteMetadata = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead, newMetaHead, newDataHead *CommitDef,
				prefix string,
				key, rangeEnd []byte,
				newRevision int64,
				expectedResult *etcdserverpb.DeleteRangeResponse) []check {
				var keyPrefix = keyPrefix{prefix: prefix}

				checks = append(
					checks,
					check{
						spec:                      fmt.Sprintf("%s, replace commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              replace,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeTrue(),
						expectMetaHead:            expectMetaHead(newMetaHead, newDataHead),
						matchDataMutated:          BeTrue(),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
						matchResponse:             Equal(expectedResult),
					},
					check{
						spec:                      fmt.Sprintf("%s, replace commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              replace,
						matchErr:                  HaveOccurred(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{}),
					},
				)

				currentMetaHead, currentDataHead = currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()
				newMetaHead, newDataHead = newMetaHead.DeepCopy(), newDataHead.DeepCopy()
				newMetaHead.Parents, newDataHead.Parents = []CommitDef{*currentMetaHead}, []CommitDef{*currentDataHead}
				expectedResult = &etcdserverpb.DeleteRangeResponse{Deleted: expectedResult.Deleted}

				return append(
					checks,
					check{
						spec:                      fmt.Sprintf("%s, inherit commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              inherit,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeTrue(),
						expectMetaHead:            expectMetaHeadInherit(newMetaHead, newDataHead),
						matchDataMutated:          BeTrue(),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
						matchResponse:             Equal(expectedResult),
					},
					check{
						spec:                      fmt.Sprintf("%s, inherit commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              inherit,
						matchErr:                  HaveOccurred(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{}),
					},
				)
			}

			appendChecksWithFullMetadata = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead, newMetaHead, newDataHead *CommitDef,
				prevKvs []*mvccpb.KeyValue,
				prefix string,
				key, rangeEnd []byte,
				newRevision int64,
				expectedResult *etcdserverpb.DeleteRangeResponse) []check {
				var keyPrefix = keyPrefix{prefix: prefix}

				checks = append(
					checks,
					check{
						spec:                      fmt.Sprintf("%s, replace commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              replace,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeTrue(),
						expectMetaHead:            expectMetaHead(newMetaHead, newDataHead),
						matchDataMutated:          BeTrue(),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
						matchResponse:             Equal(expectedResult),
					},
					check{
						spec:                      fmt.Sprintf("%s, replace commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              replace,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeTrue(),
						expectMetaHead:            expectMetaHead(newMetaHead, newDataHead),
						matchDataMutated:          BeTrue(),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{Deleted: expectedResult.Deleted, PrevKvs: prevKvs}),
					},
				)

				currentMetaHead, currentDataHead = currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()
				newMetaHead, newDataHead = newMetaHead.DeepCopy(), newDataHead.DeepCopy()
				newMetaHead.Parents, newDataHead.Parents = []CommitDef{*currentMetaHead}, []CommitDef{*currentDataHead}

				return append(
					checks,
					check{
						spec:                      fmt.Sprintf("%s, inherit commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              inherit,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeTrue(),
						expectMetaHead:            expectMetaHeadInherit(newMetaHead, newDataHead),
						matchDataMutated:          BeTrue(),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{Deleted: expectedResult.Deleted}),
					},
					check{
						spec:                      fmt.Sprintf("%s, inherit commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              inherit,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeTrue(),
						expectMetaHead:            expectMetaHeadInherit(newMetaHead.DeepCopy(), newDataHead.DeepCopy()),
						matchDataMutated:          BeTrue(),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead.DeepCopy())),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{Deleted: expectedResult.Deleted, PrevKvs: prevKvs}),
					},
				)
			}

			appendChecksWithFullMetadataMatchingNone = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead *CommitDef,
				prefix string,
				key, rangeEnd []byte,
				newRevision int64,
			) []check {
				var keyPrefix = keyPrefix{prefix: prefix}

				checks = append(
					checks,
					check{
						spec:                      fmt.Sprintf("%s, replace commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              replace,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{}),
					},
					check{
						spec:                      fmt.Sprintf("%s, replace commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              replace,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{}),
					},
				)

				return append(
					checks,
					check{
						spec:                      fmt.Sprintf("%s, inherit commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              inherit,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{}),
					},
					check{
						spec:                      fmt.Sprintf("%s, inherit commit", spec),
						keyPrefix:                 keyPrefix,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						res:                       &etcdserverpb.DeleteRangeResponse{},
						newRevision:               newRevision,
						commitTreeFn:              inherit,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{}),
					},
				)
			}
		)

		BeforeEach(func() {
			b.refName = "refs/heads/main"
		})

		for _, prefix := range []string{"", "/", "registry", "/registry"} {
			var checks = []check{
				{
					spec:                      "expired context",
					ctxFn:                     CanceledContext,
					keyPrefix:                 keyPrefix{prefix: prefix},
					matchErr:                  Succeed(), // TODO ?
					matchMetaMutated:          BeFalse(),
					expectMetaHead:            delegate(BeNil()),
					matchDataMutated:          BeFalse(),
					matchDataHeadCommitDefPtr: BeNil(),
					matchResponse:             BeNil(),
				},
				{
					spec:                      "expired context",
					ctxFn:                     CanceledContext,
					keyPrefix:                 keyPrefix{prefix: prefix},
					metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, nil),
					matchErr:                  MatchError("context canceled"),
					matchMetaMutated:          BeFalse(),
					expectMetaHead:            delegate(BeNil()),
					matchDataMutated:          BeFalse(),
					matchDataHeadCommitDefPtr: BeNil(),
					matchResponse:             BeNil(),
				},
				{
					spec:                      "expired context",
					ctxFn:                     CanceledContext,
					keyPrefix:                 keyPrefix{prefix: prefix},
					metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "1"}),
					matchErr:                  MatchError("context canceled"),
					matchMetaMutated:          BeFalse(),
					expectMetaHead:            delegate(BeNil()),
					matchDataMutated:          BeFalse(),
					matchDataHeadCommitDefPtr: BeNil(),
					matchResponse:             BeNil(),
				},
				{
					keyPrefix:                 keyPrefix{prefix: prefix},
					matchErr:                  Succeed(),
					matchMetaMutated:          BeFalse(),
					expectMetaHead:            delegate(BeNil()),
					matchDataMutated:          BeFalse(),
					matchDataHeadCommitDefPtr: BeNil(),
					matchResponse:             BeNil(),
				},
				{
					spec:                      "metadata without data",
					keyPrefix:                 keyPrefix{prefix: prefix},
					metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, nil),
					matchErr:                  HaveOccurred(),
					matchMetaMutated:          BeFalse(),
					expectMetaHead:            delegate(BeNil()),
					matchDataMutated:          BeFalse(),
					matchDataHeadCommitDefPtr: BeNil(),
					matchResponse:             BeNil(),
				},
				{
					spec:      "metadata with invalid data ID in metadata",
					keyPrefix: keyPrefix{prefix: prefix},
					metaHeadFn: metaHeadFrom(
						&CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("a")}}},
						nil,
					),
					matchErr:                  HaveOccurred(),
					matchMetaMutated:          BeFalse(),
					expectMetaHead:            delegate(BeNil()),
					matchDataMutated:          BeFalse(),
					matchDataHeadCommitDefPtr: BeNil(),
					matchResponse:             BeNil(),
				},
				{
					spec:                      "empty data",
					keyPrefix:                 keyPrefix{prefix: prefix},
					metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "d"}),
					matchErr:                  Succeed(),
					matchMetaMutated:          BeFalse(),
					expectMetaHead:            delegate(BeNil()),
					matchDataMutated:          BeFalse(),
					matchDataHeadCommitDefPtr: BeNil(),
					matchResponse:             BeNil(),
				},
				func() check {
					var res = &etcdserverpb.DeleteRangeResponse{}

					return check{
						spec:                      "empty data",
						keyPrefix:                 keyPrefix{prefix: prefix},
						metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "d"}),
						req:                       &etcdserverpb.DeleteRangeRequest{},
						res:                       res,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(res),
					}
				}(),
				func() check {
					var res = &etcdserverpb.DeleteRangeResponse{}

					return check{
						spec:                      "empty data",
						keyPrefix:                 keyPrefix{prefix: prefix},
						metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "d"}),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: []byte(path.Join(prefix, "1"))},
						res:                       res,
						matchErr:                  Succeed(),
						matchMetaMutated:          BeFalse(),
						expectMetaHead:            delegate(BeNil()),
						matchDataMutated:          BeFalse(),
						matchDataHeadCommitDefPtr: BeNil(),
						matchResponse:             Equal(res),
					}
				}(),
			}

			{
				var (
					key                   = []byte(path.Join(prefix, "1"))
					newRevision           = int64(2)
					cmh, cdh, nmh, ndh, _ = setupCommits(
						1,
						nil,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				delete(cmh.Tree.Blobs, metadataPathRevision)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithIncompleteMetadata(
					checks,
					"metadata without revision and keys, delete one key",
					cmh, cdh, nmh, ndh,
					prefix,
					key, nil,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 1},
				)
			}

			{
				var (
					key                   = []byte(path.Join(prefix, "1"))
					newRevision           = int64(2)
					cmh, cdh, nmh, ndh, _ = setupCommits(
						1,
						nil,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithIncompleteMetadata(
					checks,
					"metadata without keys, delete one key",
					cmh, cdh, nmh, ndh,
					prefix,
					key, nil,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 1},
				)
			}

			{
				var (
					key                   = []byte(path.Join(prefix, "1"))
					newRevision           = int64(2)
					cmh, cdh, nmh, ndh, _ = setupCommits(
						1,
						[]string{etcdserverpb.Compare_CREATE.String()},
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithIncompleteMetadata(
					checks,
					"metadata with incomplete keys, delete one key",
					cmh, cdh, nmh, ndh,
					prefix,
					key, nil,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 1},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "1"))
					newRevision                 = int64(2)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						1,
						[]string{
							etcdserverpb.Compare_CREATE.String(),
							etcdserverpb.Compare_MOD.String(),
							etcdserverpb.Compare_VERSION.String(),
						},
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"metadata with everything except lease, delete one key",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, nil,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 1},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "1"))
					newRevision                 = int64(2)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						1,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete one key",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, nil,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 1},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "0"))
					rangeEnd                    = []byte("\x00")
					newRevision                 = int64(2)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						1,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete all",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 10},
				)
			}

			{
				var (
					key               = []byte(path.Join(prefix, "xx"))
					rangeEnd          = []byte(path.Join(prefix, "yy"))
					newRevision       = int64(2)
					cmh, cdh, _, _, _ = setupCommits(
						1,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

				checks = appendChecksWithFullMetadataMatchingNone(
					checks,
					"full metadata, delete none singleton",
					cmh, cdh,
					prefix,
					key, nil,
					newRevision,
				)

				checks = appendChecksWithFullMetadataMatchingNone(
					checks,
					"full metadata, delete none range",
					cmh, cdh,
					prefix,
					key, rangeEnd,
					newRevision,
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "a/5"))
					newRevision                 = int64(2)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete one blob",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, nil,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 1},
				)
			}

			{
				var (
					key               = []byte(path.Join(prefix, "a"))
					newRevision       = int64(2)
					cmh, cdh, _, _, _ = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

				checks = appendChecksWithFullMetadataMatchingNone(
					checks,
					"full metadata, delete one subtree",
					cmh, cdh,
					prefix,
					key, nil,
					newRevision,
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "a"))
					rangeEnd                    = []byte(path.Join(prefix, "d"))
					newRevision                 = int64(2)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete range",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 30},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "3"))
					rangeEnd                    = []byte(path.Join(prefix, "g/5"))
					newRevision                 = int64(2)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete range",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 72},
				)
			}

			{
				var (
					key                         = []byte("+")
					rangeEnd                    = []byte(path.Join(prefix, "h"))
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete range",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 80},
				)
			}

			{
				var (
					key                         = []byte("\x00")
					rangeEnd                    = []byte(path.Join(prefix, "h"))
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete range",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 80},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "f/7"))
					rangeEnd                    = []byte("z")
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete range",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 43},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "f/7"))
					rangeEnd                    = []byte("\x00")
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete range",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 43},
				)
			}

			{
				var (
					key                         = []byte("\x00")
					rangeEnd                    = []byte("\x00")
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						2,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, delete all",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 110},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "a/a"))
					rangeEnd                    = []byte(path.Join(prefix, "a/b"))
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						3,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, 3-deep data, delete subtree",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 10},
				)
			}

			{
				var (
					key                         = []byte(path.Join(prefix, "a"))
					rangeEnd                    = []byte(path.Join(prefix, "b"))
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						3,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, 3-deep data, delete subtree",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 110},
				)
			}

			{
				var (
					key                         = []byte("\x00")
					rangeEnd                    = []byte("\x00")
					newRevision                 = int64(4)
					cmh, cdh, nmh, ndh, prevKvs = setupCommits(
						3,
						allMetaKeys,
						1,
						newRevision,
						prefix,
						&closedOpenInterval{start: key, end: rangeEnd},
					)
				)

				cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
				cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}
				nmh.Parents = cmh.Parents
				ndh.Parents = cdh.Parents

				checks = appendChecksWithFullMetadata(
					checks,
					"full metadata, 3-deep data, delete everything",
					cmh, cdh, nmh, ndh,
					prevKvs,
					prefix,
					key, rangeEnd,
					newRevision,
					&etcdserverpb.DeleteRangeResponse{Deleted: 1110},
				)
			}

			for _, s := range checks {
				func(s check) {
					Describe(fmt.Sprintf("%s keyPrefix=%q hasMetaHead=%t req=%v res=%v newRevision=%d", s.spec, s.keyPrefix.prefix, s.metaHeadFn != nil, s.req, s.res, s.newRevision), func() {
						var (
							metaHead  git.Commit
							parentCtx context.Context
						)

						BeforeEach(func() {
							b.keyPrefix = s.keyPrefix

							if s.metaHeadFn != nil {
								Expect(func() (err error) { metaHead, err = s.metaHeadFn(); return }()).To(Succeed())
							}

							parentCtx = ctx

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}
						})

						AfterEach(func() {
							if metaHead != nil {
								Expect(metaHead.Close()).To(Succeed())
							}
						})

						It(ItSpecForMatchError(s.matchErr), func() {
							var (
								commitTreeFn                 commitTreeFunc
								metaMutated, dataMutated     bool
								newMetaHeadID, newDataHeadID git.ObjectID
								err                          error
							)

							if s.commitTreeFn != nil {
								commitTreeFn = s.commitTreeFn()
							}

							metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doDeleteRange(
								ctx,
								metaHead,
								s.req,
								s.res,
								s.newRevision,
								commitTreeFn,
							)

							Expect(err).To(s.matchErr)

							for _, s := range []struct {
								spec            string
								mutated         bool
								id              git.ObjectID
								matchMutated    types.GomegaMatcher
								expectCommitDef expectMetaHeadFunc
							}{
								{
									spec:            "data", // Check data first to see the details of the difference on error
									mutated:         dataMutated,
									id:              newDataHeadID,
									matchMutated:    s.matchDataMutated,
									expectCommitDef: delegate(s.matchDataHeadCommitDefPtr),
								},
								{
									spec:            "metadata",
									mutated:         metaMutated,
									id:              newMetaHeadID,
									matchMutated:    s.matchMetaMutated,
									expectCommitDef: s.expectMetaHead,
								},
							} {
								Expect(s.mutated).To(s.matchMutated, s.spec)

								s.expectCommitDef(
									func() *CommitDef {
										if reflect.DeepEqual(s.id, git.ObjectID{}) {
											return nil
										}

										return GetCommitDefByID(parentCtx, b.repo, s.id)
									}(),
									s.spec,
								)
							}

							Expect(s.res).To(s.matchResponse)
						})
					})
				}(s)
			}
		}
	})

	Describe("DeleteRange", func() {
		type check struct {
			spec                      string
			ctxFn                     ContextFunc
			metaHeadFn                metaHeadFunc
			dataHead                  *CommitDef
			req                       *etcdserverpb.DeleteRangeRequest
			matchErr                  types.GomegaMatcher
			expectMetaHead            expectMetaHeadFunc
			matchDataHeadCommitDefPtr types.GomegaMatcher
			matchResponse             types.GomegaMatcher
		}

		var (
			appendChecksWithIncompleteMetadata = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead, newMetaHead, newDataHead *CommitDef,
				key, rangeEnd []byte,
				oldRevision, newRevision int64,
				expectedResult *etcdserverpb.DeleteRangeResponse,
			) []check {

				currentMetaHead, currentDataHead = currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()
				newMetaHead, newDataHead = newMetaHead.DeepCopy(), newDataHead.DeepCopy()
				newMetaHead.Parents, newDataHead.Parents = []CommitDef{*currentMetaHead}, []CommitDef{*currentDataHead}

				return append(
					checks,
					check{
						spec:                      spec,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						dataHead:                  currentDataHead,
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						matchErr:                  Succeed(),
						expectMetaHead:            expectMetaHeadInherit(newMetaHead, newDataHead),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
						matchResponse:             Equal(expectedResult),
					},
					check{
						spec:                      spec,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						dataHead:                  currentDataHead.DeepCopy(),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						matchErr:                  HaveOccurred(),
						expectMetaHead:            expectMetaHead(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(currentDataHead.DeepCopy())),
						matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{Header: &etcdserverpb.ResponseHeader{
							ClusterId: expectedResult.Header.ClusterId,
							MemberId:  expectedResult.Header.MemberId,
							Revision:  oldRevision,
							RaftTerm:  uint64(oldRevision),
						}}),
					},
				)
			}

			appendChecksWithFullMetadata = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead, newMetaHead, newDataHead *CommitDef,
				prevKvs []*mvccpb.KeyValue,
				key, rangeEnd []byte,
				newRevision int64,
				expectedResult *etcdserverpb.DeleteRangeResponse,
			) []check {

				currentMetaHead, currentDataHead = currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()
				newMetaHead, newDataHead = newMetaHead.DeepCopy(), newDataHead.DeepCopy()
				newMetaHead.Parents, newDataHead.Parents = []CommitDef{*currentMetaHead}, []CommitDef{*currentDataHead}

				return append(
					checks,
					check{
						spec:                      spec,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						dataHead:                  currentDataHead,
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						matchErr:                  Succeed(),
						expectMetaHead:            expectMetaHeadInherit(newMetaHead, newDataHead),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead)),
						matchResponse:             Equal(expectedResult),
					},
					check{
						spec:                      spec,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						dataHead:                  currentDataHead.DeepCopy(),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						matchErr:                  Succeed(),
						expectMetaHead:            expectMetaHeadInherit(newMetaHead.DeepCopy(), newDataHead.DeepCopy()),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(newDataHead.DeepCopy())),
						matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{
							Header:  expectedResult.Header,
							Deleted: expectedResult.Deleted,
							PrevKvs: prevKvs,
						}),
					},
				)
			}

			appendChecksWithFullMetadataMatchingNone = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead *CommitDef,
				key, rangeEnd []byte,
				expectedResult *etcdserverpb.DeleteRangeResponse,
			) []check {
				return append(
					checks,
					check{
						spec:                      spec,
						metaHeadFn:                metaHeadFrom(currentMetaHead, currentDataHead),
						dataHead:                  currentDataHead,
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
						matchErr:                  Succeed(),
						expectMetaHead:            expectMetaHead(currentMetaHead, currentDataHead),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(currentDataHead)),
						matchResponse:             Equal(expectedResult),
					},
					check{
						spec:                      spec,
						metaHeadFn:                metaHeadFrom(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						dataHead:                  currentDataHead.DeepCopy(),
						req:                       &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, PrevKv: true},
						matchErr:                  Succeed(),
						expectMetaHead:            expectMetaHead(currentMetaHead.DeepCopy(), currentDataHead.DeepCopy()),
						matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(currentDataHead.DeepCopy())),
						matchResponse:             Equal(&etcdserverpb.DeleteRangeResponse{Header: expectedResult.Header}),
					},
				)
			}
		)

		for clusterID, memberID := range map[uint64]uint64{
			0:  0,
			10: 2,
		} {
			func(clusterID, memberID uint64) {
				Describe(fmt.Sprintf("clusterID=%d, memberID=%d", clusterID, memberID), func() {
					BeforeEach(func() {
						b.clusterID = clusterID
						b.memberID = memberID
					})

					for refName, metaRefNamePrefix := range map[git.ReferenceName]git.ReferenceName{
						"refs/heads/main":   "",
						"refs/heads/custom": "refs/custom/prefix",
					} {
						func(refName, metametaRefNamePrefix git.ReferenceName) {
							Describe(fmt.Sprintf("refName=%q, metaRefNamePrefix=%q", refName, metaRefNamePrefix), func() {
								BeforeEach(func() {
									b.refName = refName
									b.metadataRefNamePrefix = metaRefNamePrefix
								})

								var checks = []check{
									{
										spec:                      "expired context",
										ctxFn:                     CanceledContext,
										matchErr:                  MatchError("context canceled"),
										expectMetaHead:            delegate(BeNil()),
										matchDataHeadCommitDefPtr: BeNil(),
										matchResponse:             BeNil(),
									},
									func() check {
										var cmh = &CommitDef{Message: "1"}

										return check{
											spec:                      "expired context",
											ctxFn:                     CanceledContext,
											metaHeadFn:                metaHeadFrom(cmh, nil),
											matchErr:                  MatchError("context canceled"),
											expectMetaHead:            delegate(PointTo(GetCommitDefMatcher(cmh))),
											matchDataHeadCommitDefPtr: BeNil(),
											matchResponse:             BeNil(),
										}
									}(),
									func() check {
										var (
											cmh = &CommitDef{Message: "1"}
											cdh = &CommitDef{Message: "1"}
										)

										return check{
											spec:                      "expired context",
											ctxFn:                     CanceledContext,
											metaHeadFn:                metaHeadFrom(cmh, cdh),
											dataHead:                  cdh,
											matchErr:                  MatchError("context canceled"),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse:             BeNil(),
										}
									}(),
									{
										matchErr:                  Succeed(),
										expectMetaHead:            delegate(BeNil()),
										matchDataHeadCommitDefPtr: BeNil(),
										matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID},
										}),
									},
									func() check {
										var cmh = &CommitDef{Message: "1"}

										return check{
											spec:                      "metadata without data",
											metaHeadFn:                metaHeadFrom(cmh, nil),
											matchErr:                  HaveOccurred(),
											expectMetaHead:            delegate(PointTo(GetCommitDefMatcher(cmh))),
											matchDataHeadCommitDefPtr: BeNil(),
											matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{
												Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID},
											}),
										}
									}(),
									func() check {
										var cmh = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("a")}}}

										return check{
											spec:                      "metadata with invalid data ID in metadata",
											metaHeadFn:                metaHeadFrom(cmh, nil),
											matchErr:                  HaveOccurred(),
											expectMetaHead:            delegate(PointTo(GetCommitDefMatcher(cmh))),
											matchDataHeadCommitDefPtr: BeNil(),
											matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{
												Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID},
											}),
										}
									}(),
									func() check {
										var (
											cmh = &CommitDef{Message: "1"}
											cdh = &CommitDef{Message: "d"}
										)

										return check{
											spec:                      "empty data",
											metaHeadFn:                metaHeadFrom(cmh, cdh),
											dataHead:                  cdh,
											matchErr:                  Succeed(),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{
												Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID},
											}),
										}
									}(),
									func() check {
										var (
											cmh = &CommitDef{Message: "1"}
											cdh = &CommitDef{Message: "d"}
										)

										return check{
											spec:                      "empty data",
											metaHeadFn:                metaHeadFrom(cmh, cdh),
											dataHead:                  cdh,
											req:                       &etcdserverpb.DeleteRangeRequest{},
											matchErr:                  Succeed(),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{
												Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID},
											}),
										}
									}(),
									func() check {
										var (
											cmh = &CommitDef{Message: "1"}
											cdh = &CommitDef{Message: "d"}
										)

										return check{
											spec:                      "empty data",
											metaHeadFn:                metaHeadFrom(cmh, cdh),
											dataHead:                  cdh,
											req:                       &etcdserverpb.DeleteRangeRequest{Key: []byte("1")},
											matchErr:                  Succeed(),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse: Equal(&etcdserverpb.DeleteRangeResponse{
												Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID},
											}),
										}
									}(),
								}

								{
									var (
										key                   = []byte("1")
										oldRevision           = int64(0)
										newRevision           = int64(1)
										cmh, cdh, nmh, ndh, _ = setupCommits(
											1,
											nil,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									delete(cmh.Tree.Blobs, metadataPathRevision)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithIncompleteMetadata(
										checks,
										"metadata without revision and keys, delete one key",
										cmh, cdh, nmh, ndh,
										key, nil,
										oldRevision, newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 1,
										},
									)
								}

								{
									var (
										key                   = []byte("1")
										oldRevision           = int64(1)
										newRevision           = int64(2)
										cmh, cdh, nmh, ndh, _ = setupCommits(
											1,
											nil,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithIncompleteMetadata(
										checks,
										"metadata without keys, delete one key",
										cmh, cdh, nmh, ndh,
										key, nil,
										oldRevision, newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 1,
										},
									)
								}

								{
									var (
										key                   = []byte("1")
										oldRevision           = int64(1)
										newRevision           = int64(2)
										cmh, cdh, nmh, ndh, _ = setupCommits(
											1,
											[]string{etcdserverpb.Compare_CREATE.String()},
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithIncompleteMetadata(
										checks,
										"metadata with incomplete keys, delete one key",
										cmh, cdh, nmh, ndh,
										key, nil,
										oldRevision, newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 1,
										},
									)
								}

								{
									var (
										key                         = []byte("1")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											1,
											[]string{
												etcdserverpb.Compare_CREATE.String(),
												etcdserverpb.Compare_MOD.String(),
												etcdserverpb.Compare_VERSION.String(),
											},
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"metadata with everything except lease, delete one key",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, nil,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 1,
										},
									)
								}

								{
									var (
										key                         = []byte("1")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											1,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete one key",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, nil,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 1,
										},
									)
								}

								{
									var (
										key                         = []byte("0")
										rangeEnd                    = []byte("\x00")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											1,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete all",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 10,
										},
									)
								}

								{
									var (
										key               = []byte("xx")
										rangeEnd          = []byte("yy")
										oldRevision       = int64(2)
										newRevision       = int64(2)
										cmh, cdh, _, _, _ = setupCommits(
											1,
											allMetaKeys,
											oldRevision,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadataMatchingNone(
										checks,
										"full metadata, delete none singleton",
										cmh, cdh,
										key, nil,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  oldRevision,
												RaftTerm:  uint64(oldRevision),
											},
										},
									)

									checks = appendChecksWithFullMetadataMatchingNone(
										checks,
										"full metadata, delete none range",
										cmh, cdh,
										key, rangeEnd,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  oldRevision,
												RaftTerm:  uint64(oldRevision),
											},
										},
									)
								}

								{
									var (
										key                         = []byte("a/5")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete one blob",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, nil,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 1,
										},
									)
								}

								{
									var (
										key               = []byte("a")
										oldRevision       = int64(1)
										newRevision       = int64(2)
										cmh, cdh, _, _, _ = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadataMatchingNone(
										checks,
										"full metadata, delete one subtree",
										cmh, cdh,
										key, nil,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  oldRevision,
												RaftTerm:  uint64(oldRevision),
											},
										},
									)
								}

								{
									var (
										key                         = []byte("a")
										rangeEnd                    = []byte("d")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete range",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 30,
										},
									)
								}

								{
									var (
										key                         = []byte("3")
										rangeEnd                    = []byte("g/5")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete range",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 72,
										},
									)
								}

								{
									var (
										key                         = []byte("+")
										rangeEnd                    = []byte("h")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete range",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 80,
										},
									)
								}

								{
									var (
										key                         = []byte("\x00")
										rangeEnd                    = []byte("h")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete range",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 80,
										},
									)
								}

								{
									var (
										key                         = []byte("f/7")
										rangeEnd                    = []byte("z")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete range",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 43,
										},
									)
								}

								{
									var (
										key                         = []byte("f/7")
										rangeEnd                    = []byte("\x00")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete range",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 43,
										},
									)
								}

								{
									var (
										key                         = []byte("\x00")
										rangeEnd                    = []byte("\x00")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											2,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, delete all",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 110,
										},
									)
								}

								{
									var (
										key                         = []byte("a/a")
										rangeEnd                    = []byte("a/b")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											3,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, 3-deep data, delete subtree",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 10,
										},
									)
								}

								{
									var (
										key                         = []byte("a")
										rangeEnd                    = []byte("b")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											3,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, 3-deep data, delete subtree",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 110,
										},
									)
								}

								{
									var (
										key                         = []byte("\x00")
										rangeEnd                    = []byte("\x00")
										newRevision                 = int64(2)
										cmh, cdh, nmh, ndh, prevKvs = setupCommits(
											3,
											allMetaKeys,
											1,
											newRevision,
											"",
											&closedOpenInterval{start: key, end: rangeEnd},
										)
									)

									cmh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}}}}
									cdh.Parents = []CommitDef{{Message: "0", Tree: TreeDef{Blobs: map[string][]byte{"0": []byte("0")}}}}

									checks = appendChecksWithFullMetadata(
										checks,
										"full metadata, 3-deep data, delete everything",
										cmh, cdh, nmh, ndh,
										prevKvs,
										key, rangeEnd,
										newRevision,
										&etcdserverpb.DeleteRangeResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
												Revision:  newRevision,
												RaftTerm:  uint64(newRevision),
											},
											Deleted: 1110,
										},
									)
								}

								for _, s := range checks {
									func(s check) {
										Describe(
											fmt.Sprintf(
												"%s hasMetaHead=%t, hasDataHead=%t req=%v",
												s.spec,
												s.metaHeadFn != nil,
												s.dataHead != nil,
												s.req,
											),
											func() {
												var parentCtx context.Context

												BeforeEach(func() {
													if s.metaHeadFn != nil {
														Expect(func() (err error) {
															var (
																refName git.ReferenceName
																c       git.Commit
															)

															if refName, err = b.getMetadataRefName(); err != nil {
																return
															}

															if c, err = s.metaHeadFn(); err != nil {
																return
															}

															defer c.Close()

															err = CreateReferenceFromCommitID(ctx, b.repo, refName, c.ID())
															return
														}()).To(Succeed())
													}

													if s.dataHead != nil {
														Expect(func() (err error) {
															var (
																refName git.ReferenceName
																id      git.ObjectID
															)

															if refName, err = b.getDataRefName(); err != nil {
																return
															}

															if id, err = CreateCommitFromDef(ctx, b.repo, s.dataHead); err != nil {
																return
															}

															err = CreateReferenceFromCommitID(ctx, b.repo, refName, id)
															return
														}()).To(Succeed())
													}

													parentCtx = ctx

													if s.ctxFn != nil {
														ctx = s.ctxFn(ctx)
													}
												})

												It(ItSpecForMatchError(s.matchErr), func() {
													var res, err = b.DeleteRange(ctx, s.req)

													Expect(err).To(s.matchErr)
													Expect(res).To(s.matchResponse)

													ctx = parentCtx

													for _, s := range []struct {
														spec         string
														refNameFn    func() (git.ReferenceName, error)
														expectHeadFn expectMetaHeadFunc
													}{
														{
															spec:         "data",
															refNameFn:    b.getDataRefName,
															expectHeadFn: delegate(s.matchDataHeadCommitDefPtr),
														},
														{
															spec:         "metadata",
															refNameFn:    b.getMetadataRefName,
															expectHeadFn: s.expectMetaHead,
														},
													} {
														s.expectHeadFn(
															func() (cd *CommitDef) {
																var refName git.ReferenceName

																Expect(func() (err error) { refName, err = s.refNameFn(); return }()).To(Succeed())

																cd, _ = GetCommitDefForReferenceName(ctx, b.repo, refName)
																return
															}(),
															s.spec,
														)
													}
												})
											},
										)
									}(s)
								}
							})
						}(refName, metaRefNamePrefix)
					}
				})
			}(clusterID, memberID)
		}
	})
})
