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
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"

	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"github.com/trishanku/gitcd/pkg/util"
)

var _ = Describe("backend", func() {
	var (
		b   *backend
		ctx context.Context
		dir string

		nonExistingKeys = []string{"1", "a/b/1", "a/b/c/d/e/f/g/h/i/j/1"}

		existingShallowDataWithoutKey = &CommitDef{
			Message: "1",
			Tree: TreeDef{
				Blobs:    map[string][]byte{"2": []byte("2")},
				Subtrees: map[string]TreeDef{"3": {Blobs: map[string][]byte{"3": []byte("3")}}},
			},
		}

		existing2LevelDataWithoutKey = &CommitDef{
			Message: "1",
			Tree: TreeDef{
				Blobs: map[string][]byte{"2": []byte("2")},
				Subtrees: map[string]TreeDef{
					"a": {Blobs: map[string][]byte{"3": []byte("3")}},
					"z": {Blobs: map[string][]byte{"4": []byte("4")}},
				},
			},
		}

		existing3LevelDataWithoutKey = &CommitDef{
			Message: "1",
			Tree: TreeDef{
				Blobs: map[string][]byte{"2": []byte("2")},
				Subtrees: map[string]TreeDef{
					"a": {
						Blobs: map[string][]byte{"3": []byte("3")},
						Subtrees: map[string]TreeDef{
							"b": {Blobs: map[string][]byte{"5": []byte("5")}},
							"y": {Blobs: map[string][]byte{"6": []byte("6")}},
						},
					},
					"z": {Blobs: map[string][]byte{"4": []byte("4")}},
				},
			},
		}

		existingDataWithKey = map[string]*CommitDef{
			"1": func() (cd *CommitDef) {
				cd = existingShallowDataWithoutKey.DeepCopy()
				cd.Tree.Blobs["1"] = []byte("1")
				return
			}(),
			"a/b/1": func() (cd *CommitDef) {
				var std TreeDef

				cd = existing2LevelDataWithoutKey.DeepCopy()
				std = cd.Tree.Subtrees["a"]
				std.Subtrees = map[string]TreeDef{"b": {Blobs: map[string][]byte{"1": []byte("1")}}}
				cd.Tree.Subtrees["a"] = std
				return
			}(),
		}

		fillMetadataForData = func(mh, dh *CommitDef, metaFields map[string]int64) (nmh *CommitDef) {
			var fillMetadataTree func(mt, dt *TreeDef)

			fillMetadataTree = func(mt, dt *TreeDef) {
				if mt.Subtrees == nil {
					mt.Subtrees = map[string]TreeDef{}
				}

				for k := range dt.Blobs {
					var td = &TreeDef{Blobs: make(map[string][]byte, len(metaFields))}

					for mk, mv := range metaFields {
						td.Blobs[mk] = []byte(revisionToString(mv))
					}

					mt.Subtrees[k] = *td
				}

				for k, dstd := range dt.Subtrees {
					var (
						mstd TreeDef
						ok   bool
					)

					if mstd, ok = mt.Subtrees[k]; !ok {
						mstd = TreeDef{}
					}

					fillMetadataTree(&mstd, &dstd)
					mt.Subtrees[k] = mstd
				}
			}

			fillMetadataTree(&mh.Tree, &dh.Tree)
			nmh = mh
			return
		}

		putToHead = func(
			currentMetaHead, currentDataHead *CommitDef,
			newRevision int64,
			p string,
			value []byte,
			lease int64,
			ignoreValue,
			ignoreLease bool,
		) (newMetaHead, newDataHead *CommitDef) {
			var (
				bNewRevision = []byte(revisionToString(newRevision))
				addTreeEntry func(td *TreeDef, ps util.PathSlice, callback func(td *TreeDef, entryName string))
			)

			addTreeEntry = func(td *TreeDef, ps util.PathSlice, callback func(td *TreeDef, entryName string)) {
				switch len(ps) {
				case 0:
					return
				case 1:
					if td.Blobs == nil {
						td.Blobs = map[string][]byte{}
					}

					if td.Subtrees == nil {
						td.Subtrees = map[string]TreeDef{}
					}

					callback(td, ps[0])
					return
				}

				var (
					entryName = ps[0]
					std       TreeDef
					ok        bool
				)

				ps = ps[1:]

				if td.Blobs != nil {
					delete(td.Blobs, entryName) // Clear any blob entries for any parent path.
				}

				if td.Subtrees == nil {
					td.Subtrees = map[string]TreeDef{}
				}

				if std, ok = td.Subtrees[entryName]; !ok {
					std = TreeDef{}
				}

				addTreeEntry(&std, ps, callback)
				td.Subtrees[entryName] = std
			}

			if currentMetaHead != nil {
				newMetaHead = currentMetaHead.DeepCopy()
			} else {
				newMetaHead = &CommitDef{}
			}

			if currentDataHead != nil {
				newDataHead = currentDataHead.DeepCopy()
			} else {
				newDataHead = &CommitDef{}
			}

			newMetaHead.Message, newDataHead.Message = string(bNewRevision), string(bNewRevision)

			if !ignoreValue {
				addTreeEntry(&newDataHead.Tree, util.SplitPath(p), func(td *TreeDef, entryName string) {
					delete(td.Subtrees, entryName)
					td.Blobs[entryName] = value
				})

				addTreeEntry(&newMetaHead.Tree, util.SplitPath(p), func(td *TreeDef, entryName string) {
					var (
						version = int64(0)
						std     TreeDef
						ok      bool
					)

					delete(td.Blobs, entryName)

					if std, ok = td.Subtrees[entryName]; !ok {
						std = TreeDef{Blobs: map[string][]byte{}}
					}

					if bVersion, ok := std.Blobs[etcdserverpb.Compare_VERSION.String()]; ok {
						version, _ = strconv.ParseInt(string(bVersion), 10, 64)
					}

					version++

					std.Blobs[etcdserverpb.Compare_VERSION.String()] = []byte(revisionToString(version))

					td.Subtrees[entryName] = std
				})
			}

			if !ignoreLease {
				addTreeEntry(&newMetaHead.Tree, util.SplitPath(p), func(td *TreeDef, entryName string) {
					var (
						std TreeDef
						ok  bool
					)

					delete(td.Blobs, entryName)

					if std, ok = td.Subtrees[entryName]; !ok {
						std = TreeDef{Blobs: map[string][]byte{}}
					}

					std.Blobs[etcdserverpb.Compare_LEASE.String()] = bNewRevision
					td.Subtrees[entryName] = std
				})
			}

			if !(ignoreValue && ignoreLease) {
				addTreeEntry(&newMetaHead.Tree, util.SplitPath(p), func(td *TreeDef, entryName string) {
					var (
						std TreeDef
						ok  bool
					)

					delete(td.Blobs, entryName)

					if std, ok = td.Subtrees[entryName]; !ok {
						std = TreeDef{Blobs: map[string][]byte{}}
					}

					if _, ok := std.Blobs[etcdserverpb.Compare_CREATE.String()]; !ok {
						std.Blobs[etcdserverpb.Compare_CREATE.String()] = bNewRevision
					}

					std.Blobs[etcdserverpb.Compare_MOD.String()] = bNewRevision
					td.Subtrees[entryName] = std
				})

				addTreeEntry(&newMetaHead.Tree, util.SplitPath(metadataPathRevision), func(td *TreeDef, entryName string) {
					delete(td.Subtrees, entryName)
					td.Blobs[entryName] = bNewRevision
				})
			}

			return
		}
	)

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

	Describe("doPut", func() {
		type check struct {
			spec                                        string
			ctxFn                                       ContextFunc
			metaHeadFn                                  metaHeadFunc
			req                                         *etcdserverpb.PutRequest
			res                                         *etcdserverpb.PutResponse
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

			appendChecks = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead *CommitDef,
				prevKv *mvccpb.KeyValue,
				prefix, p string,
				v []byte,
				newRevision int64,
				prevKVShouldSucceed bool,
			) []check {
				var (
					k = []byte(path.Join(prefix, p))

					safeCopy = func(cd *CommitDef) *CommitDef {
						if cd == nil {
							return cd
						}

						return cd.DeepCopy()
					}
				)

				for _, commitStrategy := range []struct {
					spec             string
					adjustParentsFn  func(ch, nh *CommitDef)
					commitTreeFn     func() commitTreeFunc
					expectMetaHeadFn func(nmh, ndh *CommitDef, dataMutated bool) expectMetaHeadFunc
				}{
					{
						spec: "replace",
						adjustParentsFn: func(ch, nh *CommitDef) {
							if ch != nil {
								nh.Parents = ch.Parents
							}
						},
						commitTreeFn:     replace,
						expectMetaHeadFn: func(nmh, ndh *CommitDef, dataMutated bool) expectMetaHeadFunc { return expectMetaHead(nmh, ndh) },
					},
					{
						spec: "inherit",
						adjustParentsFn: func(ch, nh *CommitDef) {
							if ch != nil {
								nh.Parents = []CommitDef{*safeCopy(ch)}
							}
						},
						commitTreeFn:     inherit,
						expectMetaHeadFn: expectMetaHeadInherit,
					},
				} {
					for _, s := range []struct {
						ignoreValue, ignoreLease bool
					}{
						{ignoreValue: true, ignoreLease: true},
						{ignoreValue: false, ignoreLease: true},
						{ignoreValue: true, ignoreLease: false},
						{ignoreValue: false, ignoreLease: false},
					} {
						var (
							metaHeadFn                = metaHeadFrom(currentMetaHead, currentDataHead)
							newMetaHead, newDataHead  *CommitDef
							expectMH                  = delegateToMatcher(BeNil())
							matchDataHeadCommitDefPtr = BeNil()
						)

						newMetaHead, newDataHead = putToHead(currentMetaHead, currentDataHead, newRevision, p, v, 1, s.ignoreValue, s.ignoreLease)
						commitStrategy.adjustParentsFn(currentMetaHead, newMetaHead)
						commitStrategy.adjustParentsFn(currentDataHead, newDataHead)

						if !(s.ignoreValue && s.ignoreLease) {
							if !s.ignoreValue {
								matchDataHeadCommitDefPtr = PointTo(GetCommitDefMatcher(newDataHead))
								expectMH = commitStrategy.expectMetaHeadFn(newMetaHead, newDataHead, true)
							} else {
								expectMH = commitStrategy.expectMetaHeadFn(newMetaHead, currentDataHead, false)
							}
						}

						if currentMetaHead == nil {
							metaHeadFn = nil
						}

						checks = append(checks, check{
							spec:       fmt.Sprintf("%s, %s commit", spec, commitStrategy.spec),
							metaHeadFn: metaHeadFn,
							req: &etcdserverpb.PutRequest{
								Key:         k,
								Value:       v,
								IgnoreValue: s.ignoreValue,
								IgnoreLease: s.ignoreLease,
								Lease:       newRevision,
							},
							res:                       &etcdserverpb.PutResponse{},
							newRevision:               newRevision,
							commitTreeFn:              commitStrategy.commitTreeFn,
							matchErr:                  Succeed(),
							matchMetaMutated:          Equal(!(s.ignoreValue && s.ignoreLease)),
							expectMetaHead:            expectMH,
							matchDataMutated:          Equal(!s.ignoreValue),
							matchDataHeadCommitDefPtr: matchDataHeadCommitDefPtr,
							matchResponse:             Equal(&etcdserverpb.PutResponse{}),
						})

						metaHeadFn = metaHeadFrom(safeCopy(currentMetaHead), safeCopy(currentDataHead))
						if currentMetaHead == nil {
							metaHeadFn = nil
						}

						if prevKVShouldSucceed {
							expectMH = delegateToMatcher(BeNil())
							matchDataHeadCommitDefPtr = BeNil()

							if !(s.ignoreValue && s.ignoreLease) {
								if !s.ignoreValue {
									matchDataHeadCommitDefPtr = PointTo(GetCommitDefMatcher(newDataHead.DeepCopy()))
									expectMH = commitStrategy.expectMetaHeadFn(newMetaHead.DeepCopy(), newDataHead.DeepCopy(), true)
								} else {
									expectMH = commitStrategy.expectMetaHeadFn(newMetaHead.DeepCopy(), safeCopy(currentDataHead), false)
								}
							}

							checks = append(checks, check{
								spec:       fmt.Sprintf("%s, %s commit", spec, commitStrategy.spec),
								metaHeadFn: metaHeadFn,
								req: &etcdserverpb.PutRequest{
									Key:         k,
									Value:       v,
									IgnoreValue: s.ignoreValue,
									IgnoreLease: s.ignoreLease,
									Lease:       newRevision,
									PrevKv:      true,
								},
								res:                       &etcdserverpb.PutResponse{},
								newRevision:               newRevision,
								commitTreeFn:              commitStrategy.commitTreeFn,
								matchErr:                  Succeed(),
								matchMetaMutated:          Equal(!(s.ignoreValue && s.ignoreLease)),
								expectMetaHead:            expectMH,
								matchDataMutated:          Equal(!s.ignoreValue),
								matchDataHeadCommitDefPtr: matchDataHeadCommitDefPtr,
								matchResponse:             Equal(&etcdserverpb.PutResponse{PrevKv: prevKv}),
							})
						} else {
							checks = append(checks, check{
								spec:       fmt.Sprintf("%s, %s commit", spec, commitStrategy.spec),
								metaHeadFn: metaHeadFn,
								req: &etcdserverpb.PutRequest{
									Key:         k,
									Value:       v,
									IgnoreValue: s.ignoreValue,
									IgnoreLease: s.ignoreLease,
									Lease:       newRevision,
									PrevKv:      true,
								},
								res:                       &etcdserverpb.PutResponse{},
								newRevision:               newRevision,
								commitTreeFn:              commitStrategy.commitTreeFn,
								matchErr:                  HaveOccurred(),
								matchMetaMutated:          BeFalse(),
								expectMetaHead:            delegateToMatcher(BeNil()),
								matchDataMutated:          BeFalse(),
								matchDataHeadCommitDefPtr: BeNil(),
								matchResponse:             Equal(&etcdserverpb.PutResponse{}),
							})
						}
					}
				}

				return checks
			}
		)

		BeforeEach(func() {
			b.refName = "refs/heads/main"
		})

		for _, prefix := range []string{"", "/", "/registry", "/registry/"} {
			func(prefix string) {
				Describe(fmt.Sprintf("prefix=%q", prefix), func() {
					BeforeEach(func() {
						b.keyPrefix.prefix = prefix
					})

					var checks = []check{
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							matchErr:                  MatchError(rpctypes.ErrGRPCEmptyKey),
							matchMetaMutated:          BeFalse(),
							expectMetaHead:            delegateToMatcher(BeNil()),
							matchDataMutated:          BeFalse(),
							matchDataHeadCommitDefPtr: BeNil(),
							matchResponse:             BeNil(),
						},
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, nil),
							matchErr:                  MatchError("context canceled"),
							matchMetaMutated:          BeFalse(),
							expectMetaHead:            delegateToMatcher(BeNil()),
							matchDataMutated:          BeFalse(),
							matchDataHeadCommitDefPtr: BeNil(),
							matchResponse:             BeNil(),
						},
						{
							spec:                      "expired context",
							ctxFn:                     CanceledContext,
							metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "1"}),
							matchErr:                  MatchError("context canceled"),
							matchMetaMutated:          BeFalse(),
							expectMetaHead:            delegateToMatcher(BeNil()),
							matchDataMutated:          BeFalse(),
							matchDataHeadCommitDefPtr: BeNil(),
							matchResponse:             BeNil(),
						},
						{
							spec:                      "no metadata, no data",
							matchErr:                  MatchError(rpctypes.ErrGRPCEmptyKey),
							matchMetaMutated:          BeFalse(),
							expectMetaHead:            delegateToMatcher(BeNil()),
							matchDataMutated:          BeFalse(),
							matchDataHeadCommitDefPtr: BeNil(),
							matchResponse:             BeNil(),
						},
						{
							spec:                      "metadata without data",
							metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, nil),
							matchErr:                  HaveOccurred(),
							matchMetaMutated:          BeFalse(),
							expectMetaHead:            delegateToMatcher(BeNil()),
							matchDataMutated:          BeFalse(),
							matchDataHeadCommitDefPtr: BeNil(),
							matchResponse:             BeNil(),
						},
						{
							spec: "metadata with invalid data ID in metadata",
							metaHeadFn: metaHeadFrom(
								&CommitDef{Message: "1", Tree: TreeDef{Blobs: map[string][]byte{metadataPathData: []byte("a")}}},
								nil,
							),
							matchErr:                  HaveOccurred(),
							matchMetaMutated:          BeFalse(),
							expectMetaHead:            delegateToMatcher(BeNil()),
							matchDataMutated:          BeFalse(),
							matchDataHeadCommitDefPtr: BeNil(),
							matchResponse:             BeNil(),
						},
						{
							spec:                      "empty data",
							metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "d"}),
							matchErr:                  MatchError(rpctypes.ErrGRPCEmptyKey),
							matchMetaMutated:          BeFalse(),
							expectMetaHead:            delegateToMatcher(BeNil()),
							matchDataMutated:          BeFalse(),
							matchDataHeadCommitDefPtr: BeNil(),
							matchResponse:             BeNil(),
						},
						func() check {
							var res = &etcdserverpb.PutResponse{}

							return check{
								spec:                      "empty data",
								metaHeadFn:                metaHeadFrom(&CommitDef{Message: "1"}, &CommitDef{Message: "d"}),
								req:                       &etcdserverpb.PutRequest{},
								res:                       res,
								matchErr:                  MatchError(rpctypes.ErrGRPCEmptyKey),
								matchMetaMutated:          BeFalse(),
								expectMetaHead:            delegateToMatcher(BeNil()),
								matchDataMutated:          BeFalse(),
								matchDataHeadCommitDefPtr: BeNil(),
								matchResponse:             Equal(res),
							}
						}(),
					}

					for _, p := range nonExistingKeys {
						checks = appendChecks(
							checks,
							"no metadata, no data, add",
							nil, nil,
							nil,
							prefix, p, []byte("1"),
							int64(2),
							true,
						)
					}

					for _, p := range nonExistingKeys {
						checks = appendChecks(
							checks,
							"empty data, add",
							&CommitDef{Message: "1"}, &CommitDef{Message: "d"},
							nil,
							prefix, p, []byte("1"),
							int64(4),
							true,
						)
					}

					for _, p := range nonExistingKeys {
						checks = appendChecks(
							checks,
							"partial metadata, existing shallow data, add",
							fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								existingShallowDataWithoutKey,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String(): 1,
								},
							),
							existingShallowDataWithoutKey,
							nil,
							prefix, p, []byte("1"),
							int64(4),
							true,
						)
					}

					for _, p := range nonExistingKeys {
						checks = appendChecks(
							checks,
							"full metadata, existing shallow data, add",
							fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								existingShallowDataWithoutKey,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String():  1,
									etcdserverpb.Compare_LEASE.String():   1,
									etcdserverpb.Compare_MOD.String():     1,
									etcdserverpb.Compare_VERSION.String(): 1,
								},
							),
							existingShallowDataWithoutKey,
							nil,
							prefix, p, []byte("1"),
							int64(4),
							true,
						)
					}

					for _, p := range nonExistingKeys {
						checks = appendChecks(
							checks,
							"full metadata, existing 2-level data, add",
							fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								existing2LevelDataWithoutKey,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String():  1,
									etcdserverpb.Compare_LEASE.String():   1,
									etcdserverpb.Compare_MOD.String():     1,
									etcdserverpb.Compare_VERSION.String(): 1,
								},
							),
							existing2LevelDataWithoutKey,
							nil,
							prefix, p, []byte("1"),
							int64(4),
							true,
						)
					}

					for _, p := range nonExistingKeys {
						checks = appendChecks(
							checks,
							"full metadata, existing 3-level data, add",
							fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								existing3LevelDataWithoutKey,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String():  1,
									etcdserverpb.Compare_LEASE.String():   1,
									etcdserverpb.Compare_MOD.String():     1,
									etcdserverpb.Compare_VERSION.String(): 1,
								},
							),
							existing3LevelDataWithoutKey,
							nil,
							prefix, p, []byte("1"),
							int64(4),
							true,
						)
					}

					for p, dh := range existingDataWithKey {
						checks = appendChecks(
							checks,
							"partial metadata, existing data, replace",
							fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								dh,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String(): 1,
								},
							),
							dh,
							nil,
							prefix, p, []byte("10"),
							int64(4),
							false,
						)
					}

					for p, dh := range existingDataWithKey {
						checks = appendChecks(
							checks,
							"metadata without lease, existing data, replace",
							fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								dh,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String():  1,
									etcdserverpb.Compare_MOD.String():     1,
									etcdserverpb.Compare_VERSION.String(): 1,
								},
							),
							dh,
							&mvccpb.KeyValue{
								Key:            []byte(path.Join(prefix, p)),
								CreateRevision: 1,
								ModRevision:    1,
								Version:        1,
								Value:          []byte("1"),
							},
							prefix, p, []byte("10"),
							int64(4),
							true,
						)
					}

					for p, dh := range existingDataWithKey {
						checks = appendChecks(
							checks,
							"full metadata, existing data, replace",
							fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								dh,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String():  1,
									etcdserverpb.Compare_LEASE.String():   1,
									etcdserverpb.Compare_MOD.String():     1,
									etcdserverpb.Compare_VERSION.String(): 1,
								},
							),
							dh,
							&mvccpb.KeyValue{
								Key:            []byte(path.Join(prefix, p)),
								CreateRevision: 1,
								Lease:          1,
								ModRevision:    1,
								Version:        1,
								Value:          []byte("1"),
							},
							prefix, p, []byte("10"),
							int64(4),
							true,
						)
					}

					{
						var (
							spec        = "full metadata with new revisions, existing data, replace with only first metadata mutation"
							newRevision = int64(4)
						)

						for p, dh := range existingDataWithKey {
							checks = appendChecks(
								checks,
								spec,
								fillMetadataForData(
									&CommitDef{
										Message: revisionToString(newRevision),
										Tree: TreeDef{
											Blobs: map[string][]byte{metadataPathRevision: []byte(revisionToString(newRevision))},
										},
										Parents: []CommitDef{{Message: "0"}},
									},
									dh,
									map[string]int64{
										etcdserverpb.Compare_CREATE.String():  newRevision,
										etcdserverpb.Compare_LEASE.String():   1,
										etcdserverpb.Compare_MOD.String():     newRevision,
										etcdserverpb.Compare_VERSION.String(): 1,
									},
								),
								dh,
								&mvccpb.KeyValue{
									Key:            []byte(path.Join(prefix, p)),
									CreateRevision: newRevision,
									Lease:          1,
									ModRevision:    newRevision,
									Version:        1,
									Value:          []byte("1"),
								},
								prefix, p, []byte("10"),
								newRevision,
								true,
							)
						}
					}

					{
						var (
							entryName   = "a"
							k, v        = []byte(path.Join(prefix, entryName)), []byte(entryName)
							newRevision = int64(4)
							cdh         = existing3LevelDataWithoutKey.DeepCopy()
							cmh         = fillMetadataForData(
								&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
								cdh,
								map[string]int64{
									etcdserverpb.Compare_CREATE.String():  1,
									etcdserverpb.Compare_LEASE.String():   1,
									etcdserverpb.Compare_MOD.String():     1,
									etcdserverpb.Compare_VERSION.String(): 1,
								},
							)

							ndh = cdh.DeepCopy()
							nmh = cmh.DeepCopy()
						)

						ndh.Message = revisionToString(newRevision)
						ndh.Tree.Blobs[entryName] = v
						delete(ndh.Tree.Subtrees, entryName)

						nmh.Message = revisionToString(newRevision)
						nmh.Tree.Subtrees[entryName] = TreeDef{
							Blobs: map[string][]byte{
								etcdserverpb.Compare_CREATE.String():  []byte(revisionToString(newRevision)),
								etcdserverpb.Compare_LEASE.String():   []byte(revisionToString(newRevision)),
								etcdserverpb.Compare_MOD.String():     []byte(revisionToString(newRevision)),
								etcdserverpb.Compare_VERSION.String(): []byte(revisionToString(1)),
							},
						}

						nmh.Tree.Blobs = map[string][]byte{metadataPathRevision: []byte(revisionToString(newRevision))}

						checks = append(checks, check{
							spec:       "full metadata, existing data, replace subtree",
							metaHeadFn: metaHeadFrom(cmh, cdh),
							req: &etcdserverpb.PutRequest{
								Key:    k,
								Value:  v,
								Lease:  newRevision,
								PrevKv: true,
							},
							res:                       &etcdserverpb.PutResponse{},
							newRevision:               newRevision,
							commitTreeFn:              replace,
							matchErr:                  Succeed(),
							matchMetaMutated:          BeTrue(),
							expectMetaHead:            expectMetaHead(nmh, ndh),
							matchDataMutated:          BeTrue(),
							matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(ndh)),
							matchResponse:             Equal(&etcdserverpb.PutResponse{}),
						})
					}

					for _, s := range checks {
						func(s check) {
							Describe(
								fmt.Sprintf(
									"%s hasMetaHead=%t req=%v res=%v newRevision=%d",
									s.spec,
									s.metaHeadFn != nil,
									s.req,
									s.res,
									s.newRevision,
								),
								func() {
									var (
										metaHead  git.Commit
										parentCtx context.Context
									)

									BeforeEach(func() {
										if s.metaHeadFn != nil {
											Expect(func() (err error) { metaHead, err = s.metaHeadFn(ctx, b.repo); return }()).To(Succeed())
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

										metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doPut(
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
												expectCommitDef: delegateToMatcher(s.matchDataHeadCommitDefPtr),
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
												parentCtx,
												b.repo,
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
								},
							)
						}(s)
					}
				})
			}(prefix)
		}
	})

	Describe("Put", func() {
		type check struct {
			spec                      string
			ctxFn                     ContextFunc
			metaHeadFn                metaHeadFunc
			dataHead                  *CommitDef
			req                       *etcdserverpb.PutRequest
			matchErr                  types.GomegaMatcher
			expectMetaHead            expectMetaHeadFunc
			matchDataHeadCommitDefPtr types.GomegaMatcher
			matchResponse             types.GomegaMatcher
		}

		var (
			appendChecks = func(
				checks []check,
				spec string,
				currentMetaHead, currentDataHead *CommitDef,
				prevKv *mvccpb.KeyValue,
				prefix, p string,
				v []byte,
				oldRevision, newRevision int64,
				prevKVShouldSucceed bool,
				expectedResult *etcdserverpb.PutResponse,
			) []check {
				var (
					k = []byte(path.Join(prefix, p))

					safeCopy = func(cd *CommitDef) *CommitDef {
						if cd == nil {
							return cd
						}

						return cd.DeepCopy()
					}
				)

				for _, s := range []struct {
					ignoreValue, ignoreLease bool
				}{
					{ignoreValue: true, ignoreLease: true},
					{ignoreValue: false, ignoreLease: true},
					{ignoreValue: true, ignoreLease: false},
					{ignoreValue: false, ignoreLease: false},
				} {
					var (
						metaHeadFn                = metaHeadFrom(currentMetaHead, currentDataHead)
						newMetaHead, newDataHead  *CommitDef
						expectMH                  = delegateToMatcher(BeNil())
						matchDataHeadCommitDefPtr = BeNil()
						revision                  = oldRevision
					)

					newMetaHead, newDataHead = putToHead(currentMetaHead, currentDataHead, newRevision, p, v, 1, s.ignoreValue, s.ignoreLease)

					if currentMetaHead != nil {
						newMetaHead.Parents = []CommitDef{*safeCopy(currentMetaHead)}
						expectMH = expectMetaHead(currentMetaHead, currentDataHead)
					}

					if currentDataHead != nil {
						newDataHead.Parents = []CommitDef{*safeCopy(currentDataHead)}
						matchDataHeadCommitDefPtr = PointTo(GetCommitDefMatcher(currentDataHead))
					}

					if !(s.ignoreValue && s.ignoreLease) {
						revision = newRevision

						if !s.ignoreValue {
							matchDataHeadCommitDefPtr = PointTo(GetCommitDefMatcher(newDataHead))
							expectMH = expectMetaHeadInherit(newMetaHead, newDataHead, true)
						} else {
							expectMH = expectMetaHeadInherit(newMetaHead, currentDataHead, false)
						}
					}

					if currentMetaHead == nil {
						metaHeadFn = nil
					}

					checks = append(checks, check{
						spec:       spec,
						metaHeadFn: metaHeadFn,
						dataHead:   currentDataHead,
						req: &etcdserverpb.PutRequest{
							Key:         k,
							Value:       v,
							IgnoreValue: s.ignoreValue,
							IgnoreLease: s.ignoreLease,
							Lease:       newRevision,
						},
						matchErr:                  Succeed(),
						expectMetaHead:            expectMH,
						matchDataHeadCommitDefPtr: matchDataHeadCommitDefPtr,
						matchResponse: Equal(&etcdserverpb.PutResponse{
							Header: &etcdserverpb.ResponseHeader{
								ClusterId: expectedResult.Header.ClusterId,
								MemberId:  expectedResult.Header.MemberId,
								Revision:  revision,
								RaftTerm:  uint64(revision),
							},
						}),
					})

					metaHeadFn = metaHeadFrom(safeCopy(currentMetaHead), safeCopy(currentDataHead))
					if currentMetaHead == nil {
						metaHeadFn = nil
					}

					expectMH = delegateToMatcher(BeNil())
					matchDataHeadCommitDefPtr = BeNil()
					revision = oldRevision

					if currentMetaHead != nil {
						expectMH = expectMetaHead(safeCopy(currentMetaHead), safeCopy(currentDataHead))
					}

					if currentDataHead != nil {
						matchDataHeadCommitDefPtr = PointTo(GetCommitDefMatcher(currentDataHead.DeepCopy()))
					}

					if prevKVShouldSucceed {
						if !(s.ignoreValue && s.ignoreLease) {
							revision = newRevision

							if !s.ignoreValue {
								matchDataHeadCommitDefPtr = PointTo(GetCommitDefMatcher(newDataHead.DeepCopy()))
								expectMH = expectMetaHeadInherit(newMetaHead.DeepCopy(), newDataHead.DeepCopy(), true)
							} else {
								expectMH = expectMetaHeadInherit(newMetaHead.DeepCopy(), safeCopy(currentDataHead), false)
							}
						}

						checks = append(checks, check{
							spec:       spec,
							metaHeadFn: metaHeadFn,
							dataHead:   safeCopy(currentDataHead),
							req: &etcdserverpb.PutRequest{
								Key:         k,
								Value:       v,
								IgnoreValue: s.ignoreValue,
								IgnoreLease: s.ignoreLease,
								Lease:       newRevision,
								PrevKv:      true,
							},
							matchErr:                  Succeed(),
							expectMetaHead:            expectMH,
							matchDataHeadCommitDefPtr: matchDataHeadCommitDefPtr,
							matchResponse: Equal(&etcdserverpb.PutResponse{
								Header: &etcdserverpb.ResponseHeader{
									ClusterId: expectedResult.Header.ClusterId,
									MemberId:  expectedResult.Header.MemberId,
									Revision:  revision,
									RaftTerm:  uint64(revision),
								},
								PrevKv: prevKv,
							}),
						})
					} else {
						checks = append(checks, check{
							spec:       spec,
							metaHeadFn: metaHeadFn,
							dataHead:   safeCopy(currentDataHead),
							req: &etcdserverpb.PutRequest{
								Key:         k,
								Value:       v,
								IgnoreValue: s.ignoreValue,
								IgnoreLease: s.ignoreLease,
								Lease:       newRevision,
								PrevKv:      true,
							},
							matchErr:                  HaveOccurred(),
							expectMetaHead:            expectMH,
							matchDataHeadCommitDefPtr: matchDataHeadCommitDefPtr,
							matchResponse: Equal(&etcdserverpb.PutResponse{
								Header: &etcdserverpb.ResponseHeader{
									ClusterId: expectedResult.Header.ClusterId,
									MemberId:  expectedResult.Header.MemberId,
									Revision:  oldRevision,
									RaftTerm:  uint64(oldRevision),
								},
							}),
						})
					}
				}

				return checks
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
						func(refName, metaRefNamePrefix git.ReferenceName) {
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
											expectMetaHead:            expectMetaHead(cmh, nil),
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
										spec:                      "no metadata, no data",
										matchErr:                  MatchError(rpctypes.ErrGRPCEmptyKey),
										expectMetaHead:            delegateToMatcher(BeNil()),
										matchDataHeadCommitDefPtr: BeNil(),
										matchResponse: Equal(&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										}),
									},
									func() check {
										var cmh = &CommitDef{Message: "1"}

										return check{
											spec:                      "metadata without data",
											metaHeadFn:                metaHeadFrom(cmh, nil),
											matchErr:                  HaveOccurred(),
											expectMetaHead:            expectMetaHead(cmh, nil),
											matchDataHeadCommitDefPtr: BeNil(),
											matchResponse: Equal(&etcdserverpb.PutResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
												},
											}),
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
											matchResponse: Equal(&etcdserverpb.PutResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
												},
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
											matchErr:                  MatchError(rpctypes.ErrGRPCEmptyKey),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse: Equal(&etcdserverpb.PutResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
												},
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
											req:                       &etcdserverpb.PutRequest{},
											matchErr:                  MatchError(rpctypes.ErrGRPCEmptyKey),
											expectMetaHead:            expectMetaHead(cmh, cdh),
											matchDataHeadCommitDefPtr: PointTo(GetCommitDefMatcher(cdh)),
											matchResponse: Equal(&etcdserverpb.PutResponse{
												Header: &etcdserverpb.ResponseHeader{
													ClusterId: clusterID,
													MemberId:  memberID,
												},
											}),
										}
									}(),
								}

								for _, p := range nonExistingKeys {
									checks = appendChecks(
										checks,
										"no metadata, no data, add",
										nil, nil,
										nil,
										"", p, []byte("1"),
										0, 1,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for _, p := range nonExistingKeys {
									checks = appendChecks(
										checks,
										"no revision, empty data, add",
										&CommitDef{Message: "1"}, &CommitDef{Message: "d"},
										nil,
										"", p, []byte("1"),
										0, 1,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for _, p := range nonExistingKeys {
									checks = appendChecks(
										checks,
										"partial metadata, existing shallow data, add",
										fillMetadataForData(
											&CommitDef{Message: "1", Parents: []CommitDef{{Message: "0"}}},
											existingShallowDataWithoutKey,
											map[string]int64{
												etcdserverpb.Compare_CREATE.String(): 1,
											},
										),
										existingShallowDataWithoutKey,
										nil,
										"", p, []byte("1"),
										0, 1,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for _, p := range nonExistingKeys {
									checks = appendChecks(
										checks,
										"full metadata, existing shallow data, add",
										fillMetadataForData(
											&CommitDef{
												Message: "1",
												Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
												Parents: []CommitDef{{Message: "0"}},
											},
											existingShallowDataWithoutKey,
											map[string]int64{
												etcdserverpb.Compare_CREATE.String():  1,
												etcdserverpb.Compare_LEASE.String():   1,
												etcdserverpb.Compare_MOD.String():     1,
												etcdserverpb.Compare_VERSION.String(): 1,
											},
										),
										existingShallowDataWithoutKey,
										nil,
										"", p, []byte("1"),
										1, 2,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for _, p := range nonExistingKeys {
									checks = appendChecks(
										checks,
										"full metadata, existing 2-level data, add",
										fillMetadataForData(
											&CommitDef{
												Message: "1",
												Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
												Parents: []CommitDef{{Message: "0"}},
											},
											existing2LevelDataWithoutKey,
											map[string]int64{
												etcdserverpb.Compare_CREATE.String():  1,
												etcdserverpb.Compare_LEASE.String():   1,
												etcdserverpb.Compare_MOD.String():     1,
												etcdserverpb.Compare_VERSION.String(): 1,
											},
										),
										existing2LevelDataWithoutKey,
										nil,
										"", p, []byte("1"),
										1, 2,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for _, p := range nonExistingKeys {
									checks = appendChecks(
										checks,
										"full metadata, existing 3-level data, add",
										fillMetadataForData(
											&CommitDef{
												Message: "1",
												Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
												Parents: []CommitDef{{Message: "0"}},
											},
											existing3LevelDataWithoutKey,
											map[string]int64{
												etcdserverpb.Compare_CREATE.String():  1,
												etcdserverpb.Compare_LEASE.String():   1,
												etcdserverpb.Compare_MOD.String():     1,
												etcdserverpb.Compare_VERSION.String(): 1,
											},
										),
										existing3LevelDataWithoutKey,
										nil,
										"", p, []byte("1"),
										1, 2,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for p, dh := range existingDataWithKey {
									checks = appendChecks(
										checks,
										"partial metadata, existing data, replace",
										fillMetadataForData(
											&CommitDef{
												Message: "1",
												Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
												Parents: []CommitDef{{Message: "0"}},
											},
											dh,
											map[string]int64{
												etcdserverpb.Compare_CREATE.String(): 1,
											},
										),
										dh,
										nil,
										"", p, []byte("10"),
										1, 2,
										false,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for p, dh := range existingDataWithKey {
									checks = appendChecks(
										checks,
										"metadata without lease, existing data, replace",
										fillMetadataForData(
											&CommitDef{
												Message: "1",
												Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
												Parents: []CommitDef{{Message: "0"}},
											},
											dh,
											map[string]int64{
												etcdserverpb.Compare_CREATE.String():  1,
												etcdserverpb.Compare_MOD.String():     1,
												etcdserverpb.Compare_VERSION.String(): 1,
											},
										),
										dh,
										&mvccpb.KeyValue{
											Key:            []byte(p),
											CreateRevision: 1,
											ModRevision:    1,
											Version:        1,
											Value:          []byte("1"),
										},
										"", p, []byte("10"),
										1, 2,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
										},
									)
								}

								for p, dh := range existingDataWithKey {
									checks = appendChecks(
										checks,
										"full metadata, existing data, replace",
										fillMetadataForData(
											&CommitDef{
												Message: "1",
												Tree:    TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}},
												Parents: []CommitDef{{Message: "0"}},
											},
											dh,
											map[string]int64{
												etcdserverpb.Compare_CREATE.String():  1,
												etcdserverpb.Compare_LEASE.String():   1,
												etcdserverpb.Compare_MOD.String():     1,
												etcdserverpb.Compare_VERSION.String(): 1,
											},
										),
										dh,
										&mvccpb.KeyValue{
											Key:            []byte(p),
											CreateRevision: 1,
											Lease:          1,
											ModRevision:    1,
											Version:        1,
											Value:          []byte("1"),
										},
										"", p, []byte("10"),
										1, 2,
										true,
										&etcdserverpb.PutResponse{
											Header: &etcdserverpb.ResponseHeader{
												ClusterId: clusterID,
												MemberId:  memberID,
											},
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
													var res, err = b.Put(ctx, s.req)

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
						}(refName, metaRefNamePrefix)
					}
				})
			}(clusterID, memberID)
		}
	})
})
