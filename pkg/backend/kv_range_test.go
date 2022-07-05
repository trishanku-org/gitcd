package backend

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"

	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	. "github.com/trishanku/gitcd/pkg/tests_util"
)

var _ = Describe("checkMinConstraint", func() {
	for _, s := range []struct {
		constraint, value int64
		match             types.GomegaMatcher
	}{
		{constraint: -1, value: -2, match: BeTrue()},
		{constraint: -1, value: -1, match: BeTrue()},
		{constraint: -1, value: 0, match: BeTrue()},
		{constraint: 0, value: -1, match: BeTrue()},
		{constraint: 0, value: 0, match: BeTrue()},
		{constraint: 0, value: 1, match: BeTrue()},
		{constraint: 1, value: 0, match: BeFalse()},
		{constraint: 1, value: 1, match: BeTrue()},
		{constraint: 1, value: 2, match: BeTrue()},
		{constraint: 2, value: 1, match: BeFalse()},
		{constraint: 2, value: 2, match: BeTrue()},
		{constraint: 2, value: 3, match: BeTrue()},
	} {
		func(constraint, value int64, match types.GomegaMatcher) {
			It(fmt.Sprintf("constraint=%d value=%d", constraint, value), func() {
				Expect(checkMinConstraint(constraint, value)).To(match)
			})
		}(s.constraint, s.value, s.match)
	}
})

var _ = Describe("checkMaxConstraint", func() {
	for _, s := range []struct {
		constraint, value int64
		match             types.GomegaMatcher
	}{
		{constraint: -1, value: -2, match: BeTrue()},
		{constraint: -1, value: -1, match: BeTrue()},
		{constraint: -1, value: 0, match: BeTrue()},
		{constraint: 0, value: -1, match: BeTrue()},
		{constraint: 0, value: 0, match: BeTrue()},
		{constraint: 0, value: 1, match: BeTrue()},
		{constraint: 1, value: 0, match: BeTrue()},
		{constraint: 1, value: 1, match: BeTrue()},
		{constraint: 1, value: 2, match: BeFalse()},
		{constraint: 2, value: 1, match: BeTrue()},
		{constraint: 2, value: 2, match: BeTrue()},
		{constraint: 2, value: 3, match: BeFalse()},
	} {
		func(constraint, value int64, match types.GomegaMatcher) {
			It(fmt.Sprintf("constraint=%d value=%d", constraint, value), func() {
				Expect(checkMaxConstraint(constraint, value)).To(match)
			})
		}(s.constraint, s.value, s.match)
	}
})

var _ = Describe("backend", func() {
	type revisionFunc func(string) int64

	var (
		b   *backend
		ctx context.Context
		dir string

		constant = func(revision int64) revisionFunc {
			return func(_ string) int64 { return revision }
		}

		identical = func(strRevision string) int64 {
			var revision, _ = strconv.ParseInt(strRevision, 10, 64)
			return revision
		}

		incrementOf = func(revisionFn revisionFunc, inc int64) revisionFunc {
			return func(strRevision string) int64 {
				return revisionFn(strRevision) + inc
			}
		}

		addMetaKeyEntry = func(td *TreeDef, key string, metaFields map[string]revisionFunc) {
			if len(metaFields) <= 0 {
				return
			}

			td.Subtrees[key] = TreeDef{Blobs: map[string][]byte{}}

			for k, revisionFn := range metaFields {
				td.Subtrees[key].Blobs[k] = []byte(revisionToString(revisionFn(key)))
			}
		}

		makeKeyValue = func(k, v []byte, metaFields map[string]revisionFunc) (kv *mvccpb.KeyValue) {
			kv = &mvccpb.KeyValue{Key: k, Value: v}

			for mk, revisionFn := range metaFields {
				switch etcdserverpb.Compare_CompareTarget(etcdserverpb.Compare_CompareTarget_value[mk]) {
				case etcdserverpb.Compare_CREATE:
					kv.CreateRevision = revisionFn(string(v))
				case etcdserverpb.Compare_LEASE:
					kv.Lease = revisionFn(string(v))
				case etcdserverpb.Compare_MOD:
					kv.ModRevision = revisionFn(string(v))
				case etcdserverpb.Compare_VERSION:
					kv.Version = revisionFn(string(v))
				}
			}

			return
		}

		setupTrees func(
			depth int,
			metaFields map[string]revisionFunc,
			metaHead, dataHead *TreeDef,
			prefix string,
			selRange interval,
			kvs []*mvccpb.KeyValue,
		) (newKvs []*mvccpb.KeyValue)

		setupCommits = func(
			depth int,
			metaFields map[string]revisionFunc,
			revision int64,
			prefix string,
			selRange interval,
		) (
			metaHead, dataHead *CommitDef,
			kvs []*mvccpb.KeyValue,
		) {
			metaHead = &CommitDef{Message: revisionToString(revision)}
			dataHead = &CommitDef{Message: revisionToString(revision)}

			kvs = setupTrees(
				depth,
				metaFields,
				&metaHead.Tree,
				&dataHead.Tree,
				prefix,
				selRange,
				kvs,
			)

			if metaHead.Tree.Blobs == nil {
				metaHead.Tree.Blobs = map[string][]byte{}
			}

			metaHead.Tree.Blobs[metadataPathRevision] = []byte(revisionToString(revision))

			return
		}
	)

	setupTrees = func(
		depth int,
		metaFields map[string]revisionFunc,
		meta, data *TreeDef,
		prefix string,
		selRange interval,
		kvs []*mvccpb.KeyValue,
	) (newKvs []*mvccpb.KeyValue) {
		defer func() {
			newKvs = kvs
		}()

		if depth <= 0 {
			return
		}

		data.Blobs = map[string][]byte{}
		if len(metaFields) > 0 {
			meta.Subtrees = map[string]TreeDef{}
		}

		for i := 0; i < 10; i++ {
			var (
				si  = strconv.Itoa(i)
				key = []byte(path.Join(prefix, si))
				v   = []byte(si)
			)

			data.Blobs[si] = v
			addMetaKeyEntry(meta, si, metaFields)

			if selRange.Check(key) == checkResultInRange {
				kvs = append(kvs, makeKeyValue(key, v, metaFields))
			}
		}

		if depth <= 1 {
			return // No subtrees for the last level
		}

		data.Subtrees = map[string]TreeDef{}

		for c := byte('a'); c < byte('k'); c++ {
			var (
				sc         = string(c)
				smtd, sdtd = &TreeDef{}, &TreeDef{}
			)

			kvs = setupTrees(depth-1, metaFields, smtd, sdtd, path.Join(prefix, sc), selRange, kvs)

			if len(metaFields) > 0 {
				meta.Subtrees[sc] = *smtd
			}

			data.Subtrees[sc] = *sdtd
		}

		return
	}

	BeforeEach(func() {
		var gitImpl = git2go.New()

		b = &backend{
			log: getTestLogger(),
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

	Describe("doRange", func() {
		type check struct {
			spec                    string
			ctxFn                   ContextFunc
			metaHeadFn              metaHeadFunc
			req                     *etcdserverpb.RangeRequest
			matchErr, matchResponse types.GomegaMatcher
		}

		var (
			appendChecks = func(checks []check, spec string, mh, dh *CommitDef, req *etcdserverpb.RangeRequest, res *etcdserverpb.RangeResponse) []check {
				var (
					reqCountOnly = &etcdserverpb.RangeRequest{
						Key:               req.Key,
						RangeEnd:          req.RangeEnd,
						Limit:             req.Limit,
						Revision:          req.Revision,
						KeysOnly:          false,
						CountOnly:         true,
						MinModRevision:    req.MinModRevision,
						MaxModRevision:    req.MaxModRevision,
						MinCreateRevision: req.MinCreateRevision,
						MaxCreateRevision: req.MaxCreateRevision,
					}
					reqKeysOnly = &etcdserverpb.RangeRequest{
						Key:               req.Key,
						RangeEnd:          req.RangeEnd,
						Limit:             req.Limit,
						Revision:          req.Revision,
						KeysOnly:          true,
						CountOnly:         false,
						MinModRevision:    req.MinModRevision,
						MaxModRevision:    req.MaxModRevision,
						MinCreateRevision: req.MinCreateRevision,
						MaxCreateRevision: req.MaxCreateRevision,
					}
					resCountOnly = &etcdserverpb.RangeResponse{
						Header: &etcdserverpb.ResponseHeader{
							ClusterId: res.Header.ClusterId,
							MemberId:  res.Header.MemberId,
							Revision:  res.Header.Revision,
							RaftTerm:  res.Header.RaftTerm,
						},
						Count: res.Count,
						More:  false,
					}
					resKeysOnly = &etcdserverpb.RangeResponse{
						Header: &etcdserverpb.ResponseHeader{
							ClusterId: res.Header.ClusterId,
							MemberId:  res.Header.MemberId,
							Revision:  res.Header.Revision,
							RaftTerm:  res.Header.RaftTerm,
						},
						Kvs: func() (kvs []*mvccpb.KeyValue) {
							for _, kv := range res.Kvs {
								kvs = append(kvs, &mvccpb.KeyValue{Key: kv.Key})
							}
							return
						}(),
						Count: int64(len(res.Kvs)),
						More:  res.More,
					}
				)

				res.Count = int64(len(res.Kvs))

				return append(
					checks,
					check{
						spec:          spec,
						metaHeadFn:    metaHeadFrom(mh, dh),
						req:           req,
						matchErr:      Succeed(),
						matchResponse: Equal(res),
					},
					check{
						spec:          spec,
						metaHeadFn:    metaHeadFrom(mh, dh),
						req:           reqCountOnly,
						matchErr:      Succeed(),
						matchResponse: Equal(resCountOnly),
					},
					check{
						spec:          spec,
						metaHeadFn:    metaHeadFrom(mh, dh),
						req:           reqKeysOnly,
						matchErr:      Succeed(),
						matchResponse: Equal(resKeysOnly),
					},
				)
			}
		)

		BeforeEach(func() {
			b.refName = DefaultDataReferenceName
		})

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

					for _, prefix := range []string{"", "/", "/registry", "/registry/"} {
						func(prefix string) {
							Describe(fmt.Sprintf("prefix=%q", prefix), func() {
								BeforeEach(func() {
									b.keyPrefix.prefix = prefix
								})

								var checks = []check{
									{
										spec:          "expired context",
										ctxFn:         CanceledContext,
										matchErr:      Succeed(),
										matchResponse: BeNil(),
									},
									{
										spec:          "expired context",
										ctxFn:         CanceledContext,
										metaHeadFn:    metaHeadFrom(&CommitDef{Message: "1"}, nil),
										matchErr:      MatchError("context canceled"),
										matchResponse: BeNil(),
									},
									{
										spec:          "expired context",
										ctxFn:         CanceledContext,
										metaHeadFn:    metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "d"}),
										matchErr:      MatchError("context canceled"),
										matchResponse: BeNil(),
									},
									{
										matchErr:      Succeed(),
										matchResponse: BeNil(),
									},
									{
										spec:          "metadata without data",
										metaHeadFn:    metaHeadFrom(&CommitDef{Message: "1"}, nil),
										matchErr:      HaveOccurred(),
										matchResponse: BeNil(),
									},
									{
										spec: "metadata with invalid data ID in metadata",
										metaHeadFn: metaHeadFrom(
											&CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("a")}}},
											nil,
										),
										matchErr:      HaveOccurred(),
										matchResponse: BeNil(),
									},
									{
										spec:          "metadata without revision, empty data",
										metaHeadFn:    metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "d"}),
										matchErr:      HaveOccurred(),
										matchResponse: BeNil(),
									},
								}

								{
									var (
										key       = []byte(path.Join(prefix, "1"))
										mh, dh, _ = setupCommits(
											1,
											map[string]revisionFunc{etcdserverpb.Compare_CREATE.String(): constant(1)},
											1,
											prefix,
											&closedOpenInterval{start: key},
										)
									)

									checks = append(
										checks,
										check{
											spec:       "partial metadata, existing data",
											metaHeadFn: metaHeadFrom(mh, dh),
											req:        &etcdserverpb.RangeRequest{Key: []byte("z")},
											matchErr:   Succeed(),
											matchResponse: Equal(&etcdserverpb.RangeResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
													Revision:  1,
													RaftTerm:  1,
												},
											}),
										},
										check{
											spec:       "partial metadata, existing data",
											metaHeadFn: metaHeadFrom(mh, dh),
											req:        &etcdserverpb.RangeRequest{Key: []byte("z"), CountOnly: true},
											matchErr:   Succeed(),
											matchResponse: Equal(&etcdserverpb.RangeResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
													Revision:  1,
													RaftTerm:  1,
												},
											}),
										},
										check{
											spec:       "partial metadata, existing data",
											metaHeadFn: metaHeadFrom(mh, dh),
											req:        &etcdserverpb.RangeRequest{Key: []byte("z"), KeysOnly: true},
											matchErr:   Succeed(),
											matchResponse: Equal(&etcdserverpb.RangeResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
													Revision:  1,
													RaftTerm:  1,
												},
											}),
										},
										check{
											spec:       "partial metadata, existing data",
											metaHeadFn: metaHeadFrom(mh, dh),
											req:        &etcdserverpb.RangeRequest{Key: key},
											matchErr:   HaveOccurred(),
											matchResponse: Equal(&etcdserverpb.RangeResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
													Revision:  1,
													RaftTerm:  1,
												},
											}),
										},
										check{
											spec:       "partial metadata, existing data",
											metaHeadFn: metaHeadFrom(mh, dh),
											req:        &etcdserverpb.RangeRequest{Key: key, CountOnly: true},
											matchErr:   Succeed(),
											matchResponse: Equal(&etcdserverpb.RangeResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
													Revision:  1,
													RaftTerm:  1,
												},
												Count: 1,
											}),
										},
										check{
											spec:       "partial metadata, existing data",
											metaHeadFn: metaHeadFrom(mh, dh),
											req:        &etcdserverpb.RangeRequest{Key: key, KeysOnly: true},
											matchErr:   Succeed(),
											matchResponse: Equal(&etcdserverpb.RangeResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
													Revision:  1,
													RaftTerm:  1,
												},
												Count: 1,
												Kvs:   []*mvccpb.KeyValue{{Key: key}},
											}),
										},
									)

									for _, req := range []*etcdserverpb.RangeRequest{
										{Key: key, MinCreateRevision: 1},
										{Key: key, MaxCreateRevision: 1},
										{Key: key, MinModRevision: 1},
										{Key: key, MaxModRevision: 1},
									} {
										var (
											reqCountOnly = &etcdserverpb.RangeRequest{
												Key:               req.Key,
												RangeEnd:          req.RangeEnd,
												Limit:             req.Limit,
												Revision:          req.Revision,
												KeysOnly:          false,
												CountOnly:         true,
												MinModRevision:    req.MinModRevision,
												MaxModRevision:    req.MaxModRevision,
												MinCreateRevision: req.MinCreateRevision,
												MaxCreateRevision: req.MaxCreateRevision,
											}
											reqKeysOnly = &etcdserverpb.RangeRequest{
												Key:               req.Key,
												RangeEnd:          req.RangeEnd,
												Limit:             req.Limit,
												Revision:          req.Revision,
												KeysOnly:          true,
												CountOnly:         false,
												MinModRevision:    req.MinModRevision,
												MaxModRevision:    req.MaxModRevision,
												MinCreateRevision: req.MinCreateRevision,
												MaxCreateRevision: req.MaxCreateRevision,
											}
										)

										checks = append(
											checks,
											check{
												spec:       "partial metadata, existing data",
												metaHeadFn: metaHeadFrom(mh, dh),
												req:        req,
												matchErr:   HaveOccurred(),
												matchResponse: Equal(&etcdserverpb.RangeResponse{
													Header: &etcdserverpb.ResponseHeader{
														ClusterId: clusterID,
														MemberId:  memberID,
														Revision:  1,
														RaftTerm:  1,
													},
												}),
											},
											check{
												spec:       "partial metadata, existing data",
												metaHeadFn: metaHeadFrom(mh, dh),
												req:        reqCountOnly,
												matchErr:   HaveOccurred(),
												matchResponse: Equal(&etcdserverpb.RangeResponse{
													Header: &etcdserverpb.ResponseHeader{
														ClusterId: clusterID,
														MemberId:  memberID,
														Revision:  1,
														RaftTerm:  1,
													},
												}),
											},
											check{
												spec:       "partial metadata, existing data",
												metaHeadFn: metaHeadFrom(mh, dh),
												req:        reqKeysOnly,
												matchErr:   HaveOccurred(),
												matchResponse: Equal(&etcdserverpb.RangeResponse{
													Header: &etcdserverpb.ResponseHeader{
														ClusterId: clusterID,
														MemberId:  memberID,
														Revision:  1,
														RaftTerm:  1,
													},
												}),
											},
										)
									}
								}

								{
									var (
										mh, dh, _ = setupCommits(
											1,
											map[string]revisionFunc{
												etcdserverpb.Compare_CREATE.String():  incrementOf(identical, 1),
												etcdserverpb.Compare_MOD.String():     incrementOf(identical, 2),
												etcdserverpb.Compare_VERSION.String(): identical,
											},
											1,
											prefix,
											&closedOpenInterval{},
										)
									)

									for _, s := range []struct {
										req   *etcdserverpb.RangeRequest
										kvs   []*mvccpb.KeyValue
										count int64
										more  bool
									}{
										{
											req: &etcdserverpb.RangeRequest{},
										},
										{
											req: &etcdserverpb.RangeRequest{Key: []byte(path.Join(prefix, "1"))},
											kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte(path.Join(prefix, "1")),
													CreateRevision: 2,
													ModRevision:    3,
													Value:          []byte("1"),
													Version:        1,
												},
											},
											count: 1,
										},
										{
											req: &etcdserverpb.RangeRequest{Key: []byte(path.Join(prefix, "1")), Limit: 1},
											kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte(path.Join(prefix, "1")),
													CreateRevision: 2,
													ModRevision:    3,
													Value:          []byte("1"),
													Version:        1,
												},
											},
											count: 1,
										},
										{
											req: &etcdserverpb.RangeRequest{Key: []byte(path.Join(prefix, "1")), Limit: 2},
											kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte(path.Join(prefix, "1")),
													CreateRevision: 2,
													ModRevision:    3,
													Value:          []byte("1"),
													Version:        1,
												},
											},
											count: 1,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:      []byte(path.Join(prefix, "3")),
												RangeEnd: []byte(path.Join(prefix, "5")),
											},
											kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte(path.Join(prefix, "3")),
													CreateRevision: 4,
													ModRevision:    5,
													Value:          []byte("3"),
													Version:        3,
												},
												{
													Key:            []byte(path.Join(prefix, "4")),
													CreateRevision: 5,
													ModRevision:    6,
													Value:          []byte("4"),
													Version:        4,
												},
											},
											count: 2,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:      []byte(path.Join(prefix, "3")),
												RangeEnd: []byte(path.Join(prefix, "5")),
												Limit:    1,
											},
											kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte(path.Join(prefix, "3")),
													CreateRevision: 4,
													ModRevision:    5,
													Value:          []byte("3"),
													Version:        3,
												},
											},
											count: 2,
											more:  true,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:      []byte(path.Join(prefix, "3")),
												RangeEnd: []byte(path.Join(prefix, "5")),
												Limit:    2,
											},
											kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte(path.Join(prefix, "3")),
													CreateRevision: 4,
													ModRevision:    5,
													Value:          []byte("3"),
													Version:        3,
												},
												{
													Key:            []byte(path.Join(prefix, "4")),
													CreateRevision: 5,
													ModRevision:    6,
													Value:          []byte("4"),
													Version:        4,
												},
											},
											count: 2,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:      []byte(path.Join(prefix, "4")),
												RangeEnd: []byte(path.Join(prefix, "6")),
												Limit:    1,
											},
											kvs: []*mvccpb.KeyValue{
												{
													Key:            []byte(path.Join(prefix, "4")),
													CreateRevision: 5,
													ModRevision:    6,
													Value:          []byte("4"),
													Version:        4,
												},
											},
											count: 2,
											more:  true,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:      []byte("\x00"),
												RangeEnd: []byte(path.Join(prefix, "5")),
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 0; i < 5; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 5,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:      []byte(path.Join(prefix, "5")),
												RangeEnd: []byte("\x00"),
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 5; i < 10; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 5,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:      []byte("\x00"),
												RangeEnd: []byte("\x00"),
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 0; i < 10; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 10,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:               []byte(path.Join(prefix, "1")),
												RangeEnd:          []byte("\x00"),
												MinCreateRevision: 3,
												MaxModRevision:    8,
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 2; i < 7; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 5,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:               []byte("\x00"),
												RangeEnd:          []byte(path.Join(prefix, "9")),
												MinModRevision:    3,
												MaxCreateRevision: 8,
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 1; i < 8; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 7,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:               []byte("\x00"),
												RangeEnd:          []byte("\x00"),
												MinCreateRevision: 5,
												MinModRevision:    3,
												MaxCreateRevision: 8,
												MaxModRevision:    7,
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 4; i < 6; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 2,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:               []byte("\x00"),
												RangeEnd:          []byte("\x00"),
												MinCreateRevision: 3,
												MinModRevision:    5,
												MaxCreateRevision: 7,
												MaxModRevision:    8,
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 3; i < 7; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 4,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:               []byte(path.Join(prefix, "4")),
												RangeEnd:          []byte("\x00"),
												MinCreateRevision: 3,
												MinModRevision:    5,
												MaxCreateRevision: 7,
												MaxModRevision:    8,
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 4; i < 7; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 3,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:               []byte("\x00"),
												RangeEnd:          []byte(path.Join(prefix, "6")),
												MinCreateRevision: 3,
												MinModRevision:    5,
												MaxCreateRevision: 7,
												MaxModRevision:    8,
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 3; i < 6; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 3,
										},
										{
											req: &etcdserverpb.RangeRequest{
												Key:               []byte("\x00"),
												RangeEnd:          []byte(path.Join(prefix, "6")),
												MinCreateRevision: 3,
												MinModRevision:    5,
												MaxCreateRevision: 7,
												MaxModRevision:    8,
												Limit:             2,
											},
											kvs: func() (kvs []*mvccpb.KeyValue) {
												for i := 3; i < 5; i++ {
													var p = strconv.Itoa(i)
													kvs = append(kvs, &mvccpb.KeyValue{
														Key:            []byte(path.Join(prefix, p)),
														CreateRevision: int64(i + 1),
														ModRevision:    int64(i + 2),
														Value:          []byte(p),
														Version:        int64(i),
													})
												}
												return
											}(),
											count: 3,
											more:  true,
										},
									} {
										checks = appendChecks(
											checks,
											"metadata without lease",
											mh, dh,
											s.req,
											&etcdserverpb.RangeResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
													Revision:  1,
													RaftTerm:  1,
												},
												Kvs:   s.kvs,
												Count: s.count,
												More:  s.more,
											},
										)
									}
								}

								{
									var (
										spec     = "full metadata and data with multiple revisions"
										interval = &closedOpenInterval{start: []byte(path.Join(prefix, "4")), end: []byte(path.Join(prefix, "f/6"))}

										mh0 = &CommitDef{
											Message: "0",
											Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}},
										}
										dh0 = &CommitDef{Message: "0"}

										mh1, dh1, kvs1 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = setupCommits(
												1,
												map[string]revisionFunc{
													etcdserverpb.Compare_CREATE.String():  constant(1),
													etcdserverpb.Compare_LEASE.String():   constant(1),
													etcdserverpb.Compare_MOD.String():     constant(2),
													etcdserverpb.Compare_VERSION.String(): constant(1),
												},
												1,
												prefix,
												interval,
											)

											mh.Parents = []CommitDef{*mh0.DeepCopy()}
											dh.Parents = []CommitDef{*dh0.DeepCopy()}
											return
										}()

										mh2, dh2, kvs2 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = setupCommits(
												2,
												map[string]revisionFunc{
													etcdserverpb.Compare_CREATE.String():  identical,
													etcdserverpb.Compare_LEASE.String():   identical,
													etcdserverpb.Compare_MOD.String():     incrementOf(identical, 1),
													etcdserverpb.Compare_VERSION.String(): identical,
												},
												2,
												prefix,
												interval,
											)

											mh.Parents = []CommitDef{*mh1.DeepCopy()}
											dh.Parents = []CommitDef{*dh1.DeepCopy()}
											return
										}()

										mh_1, dh_1, kvs_1 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = mh1.DeepCopy(), dh1.DeepCopy(), make([]*mvccpb.KeyValue, len(kvs1))

											for i, kv := range kvs1 {
												var nkv = new(mvccpb.KeyValue)
												*nkv = *kv
												kvs[i] = nkv
											}

											mh.Tree.Blobs[metadataPathRevision] = []byte(revisionToString(-1))

											mh.Parents = nil
											dh.Parents = nil
											return
										}()

										mh3, dh3, kvs3 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = setupCommits(
												3,
												map[string]revisionFunc{
													etcdserverpb.Compare_CREATE.String():  identical,
													etcdserverpb.Compare_LEASE.String():   identical,
													etcdserverpb.Compare_MOD.String():     incrementOf(identical, 1),
													etcdserverpb.Compare_VERSION.String(): identical,
												},
												3,
												prefix,
												interval,
											)

											mh.Parents = []CommitDef{*mh2.DeepCopy(), *mh_1.DeepCopy()}
											dh.Parents = []CommitDef{*dh2.DeepCopy(), *dh_1.DeepCopy()}
											return
										}()
									)

									checks = append(
										checks,
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: 3,
											},
											matchErr:      MatchError(rpctypes.ErrGRPCFutureRev),
											matchResponse: BeNil(),
										},
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: 4,
											},
											matchErr:      MatchError(rpctypes.ErrGRPCFutureRev),
											matchResponse: BeNil(),
										},
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: -2,
											},
											matchErr:      MatchError(rpctypes.ErrGRPCCompacted),
											matchResponse: BeNil(),
										},
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: -3,
											},
											matchErr:      MatchError(rpctypes.ErrGRPCCompacted),
											matchResponse: BeNil(),
										},
									)

									{
										var (
											mh, dh, kvs                       = mh2, dh2, []*mvccpb.KeyValue(nil)
											revision                          = int64(2)
											minCreateRevision, maxModRevision = 3, 7
										)

										for _, kv := range kvs2 {
											if kv.CreateRevision >= int64(minCreateRevision) && kv.ModRevision <= int64(maxModRevision) {
												var nkv = new(mvccpb.KeyValue)
												*nkv = *kv
												kvs = append(kvs, nkv)
											}
										}

										checks = append(
											checks,
											check{
												spec:       spec,
												metaHeadFn: metaHeadInheritFrom(mh, dh),
												req: &etcdserverpb.RangeRequest{
													Key:               interval.start,
													RangeEnd:          interval.end,
													MinCreateRevision: int64(minCreateRevision),
													MaxModRevision:    int64(maxModRevision),
												},
												matchErr: Succeed(),
												matchResponse: Equal(&etcdserverpb.RangeResponse{
													Header: &etcdserverpb.ResponseHeader{
														ClusterId: clusterID,
														MemberId:  memberID,
														Revision:  revision,
														RaftTerm:  uint64(revision),
													},
													Kvs:   kvs,
													Count: int64(len(kvs)),
												}),
											},
										)
									}

									for _, s := range []struct {
										mh, dh                     *CommitDef
										kvs                        []*mvccpb.KeyValue
										limit                      int64
										revision, expectedRevision int64
									}{
										{mh: mh3, dh: dh3, kvs: kvs3, limit: 50, expectedRevision: 3},
										{mh: mh3, dh: dh3, kvs: kvs3, revision: 3, expectedRevision: 3},
										{mh: mh2, dh: dh2, kvs: kvs2, revision: 2, expectedRevision: 2},
										{mh: mh1, dh: dh1, kvs: kvs1, revision: 1, expectedRevision: 1},
										{mh: mh_1, dh: dh_1, kvs: kvs_1, revision: -1, expectedRevision: -1},
									} {
										var kvs = s.kvs

										if s.limit > 0 && s.limit < int64(len(kvs)) {
											kvs = kvs[:s.limit]
										}

										checks = append(
											checks,
											check{
												spec:       spec,
												metaHeadFn: metaHeadInheritFrom(s.mh, s.dh),
												req: &etcdserverpb.RangeRequest{
													Key:      interval.start,
													RangeEnd: interval.end,
													Revision: s.revision,
													Limit:    s.limit,
												},
												matchErr: Succeed(),
												matchResponse: Equal(&etcdserverpb.RangeResponse{
													Header: &etcdserverpb.ResponseHeader{
														ClusterId: clusterID,
														MemberId:  memberID,
														Revision:  s.expectedRevision,
														RaftTerm:  uint64(s.expectedRevision),
													},
													Kvs:   kvs,
													Count: int64(len(kvs)),
													More:  len(kvs) < len(s.kvs),
												}),
											},
										)
									}
								}

								for _, s := range checks {
									func(s check) {
										Describe(fmt.Sprintf("%s hasMetaHead=%t req=%v", s.spec, s.metaHeadFn != nil, s.req), func() {
											var metaHead git.Commit

											BeforeEach(func() {
												if s.metaHeadFn != nil {
													Expect(func() (err error) { metaHead, err = s.metaHeadFn(ctx, b.repo); return }()).To(Succeed())
												}

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
												var res, err = b.doRange(ctx, metaHead, s.req)

												Expect(err).To(s.matchErr)
												Expect(res).To(s.matchResponse)
											})
										})
									}(s)
								}
							})
						}(prefix)
					}
				})
			}(clusterID, memberID)
		}
	})

	Describe("Range", func() {
		type check struct {
			spec                      string
			ctxFn                     ContextFunc
			metaHeadFn                metaHeadFunc
			dataHead                  *CommitDef
			req                       *etcdserverpb.RangeRequest
			matchErr                  types.GomegaMatcher
			expectMetaHead            expectMetaHeadFunc
			matchDataHeadCommitDefPtr types.GomegaMatcher
			matchResponse             types.GomegaMatcher
		}

		var expectMetaHeadDeep func(em, ed *CommitDef) expectMetaHeadFunc

		expectMetaHeadDeep = func(em, ed *CommitDef) expectMetaHeadFunc {
			return func(ctx context.Context, repo git.Repository, am *CommitDef, spec string) {
				var emNoParents, amNoParents = em.DeepCopy(), am.DeepCopy()

				emNoParents.Parents, amNoParents.Parents = nil, nil

				expectMetaHead(emNoParents, ed)(ctx, repo, amNoParents, spec)

				Expect(am.Parents).To(HaveLen(len(em.Parents)), spec)

				for i := range em.Parents {
					var (
						emp, amp = &em.Parents[i], &am.Parents[i]
						edp      *CommitDef
					)

					if i < len(ed.Parents) {
						edp = &ed.Parents[i]
					}

					expectMetaHeadDeep(emp, edp)(ctx, repo, amp, spec)
				}
			}
		}

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

					for refName, metaRefName := range map[git.ReferenceName]git.ReferenceName{
						DefaultDataReferenceName: DefaultMetadataReferenceName,
						"refs/heads/custom":      "refs/meta/custom",
					} {
						func(refName, metaRefName git.ReferenceName) {
							Describe(fmt.Sprintf("refName=%q, metaRefName=%q", refName, metaRefName), func() {
								BeforeEach(func() {
									b.refName = refName
									b.metadataRefName = metaRefName
								})

								var checks = []check{
									{
										spec:                      "expired context",
										ctxFn:                     CanceledContext,
										matchErr:                  MatchError("context canceled"),
										expectMetaHead:            delegateToMatcher(BeNil()),
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
											expectMetaHead:            delegateToMatcher(PointTo(GetCommitDefMatcher(cmh))),
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
										matchErr:                  HaveOccurred(),
										expectMetaHead:            delegateToMatcher(BeNil()),
										matchDataHeadCommitDefPtr: BeNil(),
										matchResponse:             BeNil(),
									},
									func() check {
										var cmh = &CommitDef{Message: "1"}

										return check{
											spec:                      "metadata without data",
											metaHeadFn:                metaHeadFrom(cmh, nil),
											matchErr:                  HaveOccurred(),
											expectMetaHead:            delegateToMatcher(PointTo(GetCommitDefMatcher(cmh))),
											matchDataHeadCommitDefPtr: BeNil(),
											matchResponse:             BeNil(),
										}
									}(),
									func() check {
										var cmh = &CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("a")}}}

										return check{
											spec:                      "metadata with invalid data ID in metadata",
											metaHeadFn:                metaHeadFrom(cmh, nil),
											matchErr:                  HaveOccurred(),
											expectMetaHead:            delegateToMatcher(PointTo(GetCommitDefMatcher(cmh))),
											matchDataHeadCommitDefPtr: BeNil(),
											matchResponse:             BeNil(),
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
											matchErr:                  HaveOccurred(),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse:             BeNil(),
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
											req:                       &etcdserverpb.RangeRequest{},
											matchErr:                  HaveOccurred(),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse:             BeNil(),
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
											req:                       &etcdserverpb.RangeRequest{Key: []byte("1")},
											matchErr:                  HaveOccurred(),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse:             BeNil(),
										}
									}(),
								}

								{
									var (
										spec     = "full metadata and data with multiple revisions"
										interval = &closedOpenInterval{start: []byte("4"), end: []byte("f/6")}

										mh0 = &CommitDef{
											Message: "0",
											Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("0")}},
										}
										dh0 = &CommitDef{Message: "0"}

										mh1, dh1, kvs1 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = setupCommits(
												1,
												map[string]revisionFunc{
													etcdserverpb.Compare_CREATE.String():  constant(1),
													etcdserverpb.Compare_LEASE.String():   constant(1),
													etcdserverpb.Compare_MOD.String():     constant(2),
													etcdserverpb.Compare_VERSION.String(): constant(1),
												},
												1,
												"",
												interval,
											)

											mh.Parents = []CommitDef{*mh0.DeepCopy()}
											dh.Parents = []CommitDef{*dh0.DeepCopy()}
											return
										}()

										mh2, dh2, kvs2 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = setupCommits(
												2,
												map[string]revisionFunc{
													etcdserverpb.Compare_CREATE.String():  identical,
													etcdserverpb.Compare_LEASE.String():   identical,
													etcdserverpb.Compare_MOD.String():     incrementOf(identical, 1),
													etcdserverpb.Compare_VERSION.String(): identical,
												},
												2,
												"",
												interval,
											)

											mh.Parents = []CommitDef{*mh1.DeepCopy()}
											dh.Parents = []CommitDef{*dh1.DeepCopy()}
											return
										}()

										mh_1, dh_1, kvs_1 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = mh1.DeepCopy(), dh1.DeepCopy(), make([]*mvccpb.KeyValue, len(kvs1))

											for i, kv := range kvs1 {
												var nkv = new(mvccpb.KeyValue)
												*nkv = *kv
												kvs[i] = nkv
											}

											mh.Tree.Blobs[metadataPathRevision] = []byte(revisionToString(-1))

											mh.Parents = nil
											dh.Parents = nil
											return
										}()

										mh3, dh3, kvs3 = func() (mh, dh *CommitDef, kvs []*mvccpb.KeyValue) {
											mh, dh, kvs = setupCommits(
												3,
												map[string]revisionFunc{
													etcdserverpb.Compare_CREATE.String():  identical,
													etcdserverpb.Compare_LEASE.String():   identical,
													etcdserverpb.Compare_MOD.String():     incrementOf(identical, 1),
													etcdserverpb.Compare_VERSION.String(): identical,
												},
												3,
												"",
												interval,
											)

											mh.Parents = []CommitDef{*mh2.DeepCopy(), *mh_1.DeepCopy()}
											dh.Parents = []CommitDef{*dh2.DeepCopy(), *dh_1.DeepCopy()}
											return
										}()
									)

									checks = append(
										checks,
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											dataHead:   dh2,
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: 3,
											},
											matchErr:                  MatchError(rpctypes.ErrGRPCFutureRev),
											expectMetaHead:            expectMetaHeadDeep(mh2, dh2),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(dh2)),
											matchResponse:             BeNil(),
										},
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											dataHead:   dh2,
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: 4,
											},
											matchErr:                  MatchError(rpctypes.ErrGRPCFutureRev),
											expectMetaHead:            expectMetaHeadDeep(mh2, dh2),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(dh2)),
											matchResponse:             BeNil(),
										},
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											dataHead:   dh2,
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: -2,
											},
											matchErr:                  MatchError(rpctypes.ErrGRPCCompacted),
											expectMetaHead:            expectMetaHeadDeep(mh2, dh2),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(dh2)),
											matchResponse:             BeNil(),
										},
										check{
											spec:       spec,
											metaHeadFn: metaHeadInheritFrom(mh2, dh2),
											dataHead:   dh2,
											req: &etcdserverpb.RangeRequest{
												Key:      interval.start,
												RangeEnd: interval.end,
												Revision: -3,
											},
											matchErr:                  MatchError(rpctypes.ErrGRPCCompacted),
											expectMetaHead:            expectMetaHeadDeep(mh2, dh2),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(dh2)),
											matchResponse:             BeNil(),
										},
									)

									{
										var (
											mh, dh, kvs                       = mh2, dh2, []*mvccpb.KeyValue(nil)
											revision                          = int64(2)
											minCreateRevision, maxModRevision = 3, 7
										)

										for _, kv := range kvs2 {
											if kv.CreateRevision >= int64(minCreateRevision) && kv.ModRevision <= int64(maxModRevision) {
												var nkv = new(mvccpb.KeyValue)
												*nkv = *kv
												kvs = append(kvs, nkv)
											}
										}

										checks = append(
											checks,
											check{
												spec:       spec,
												metaHeadFn: metaHeadInheritFrom(mh, dh),
												dataHead:   dh,
												req: &etcdserverpb.RangeRequest{
													Key:               interval.start,
													RangeEnd:          interval.end,
													MinCreateRevision: int64(minCreateRevision),
													MaxModRevision:    int64(maxModRevision),
												},
												matchErr:                  Succeed(),
												expectMetaHead:            expectMetaHeadDeep(mh, dh),
												matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(dh)),
												matchResponse: Equal(&etcdserverpb.RangeResponse{
													Header: &etcdserverpb.ResponseHeader{
														ClusterId: clusterID,
														MemberId:  memberID,
														Revision:  revision,
														RaftTerm:  uint64(revision),
													},
													Kvs:   kvs,
													Count: int64(len(kvs)),
												}),
											},
										)
									}

									for _, s := range []struct {
										mh, dh                     *CommitDef
										kvs                        []*mvccpb.KeyValue
										limit                      int64
										revision, expectedRevision int64
									}{
										{mh: mh3, dh: dh3, kvs: kvs3, limit: 50, expectedRevision: 3},
										{mh: mh3, dh: dh3, kvs: kvs3, revision: 3, expectedRevision: 3},
										{mh: mh2, dh: dh2, kvs: kvs2, revision: 2, expectedRevision: 2},
										{mh: mh1, dh: dh1, kvs: kvs1, revision: 1, expectedRevision: 1},
										{mh: mh_1, dh: dh_1, kvs: kvs_1, revision: -1, expectedRevision: -1},
									} {
										var kvs = s.kvs

										if s.limit > 0 && s.limit < int64(len(kvs)) {
											kvs = kvs[:s.limit]
										}

										checks = append(
											checks,
											check{
												spec:       spec,
												metaHeadFn: metaHeadInheritFrom(s.mh, s.dh),
												dataHead:   s.dh,
												req: &etcdserverpb.RangeRequest{
													Key:      interval.start,
													RangeEnd: interval.end,
													Revision: s.revision,
													Limit:    s.limit,
												},
												matchErr:                  Succeed(),
												expectMetaHead:            expectMetaHeadDeep(s.mh, s.dh),
												matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(s.dh)),
												matchResponse: Equal(&etcdserverpb.RangeResponse{
													Header: &etcdserverpb.ResponseHeader{
														ClusterId: clusterID,
														MemberId:  memberID,
														Revision:  s.expectedRevision,
														RaftTerm:  uint64(s.expectedRevision),
													},
													Kvs:   kvs,
													Count: int64(len(kvs)),
													More:  len(kvs) < len(s.kvs),
												}),
											},
										)
									}
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

															if c, err = s.metaHeadFn(ctx, b.repo); err != nil {
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
													var res, err = b.Range(ctx, s.req)

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
															expectHeadFn: delegateToMatcher(s.matchDataHeadCommitDefPtr),
														},
														{
															spec:         "metadata",
															refNameFn:    b.getMetadataRefName,
															expectHeadFn: s.expectMetaHead,
														},
													} {
														s.expectHeadFn(
															ctx,
															b.repo,
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
						}(refName, metaRefName)
					}
				})
			}(clusterID, memberID)
		}
	})
})
