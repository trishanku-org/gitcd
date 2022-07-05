package backend

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	. "github.com/trishanku/gitcd/pkg/tests_util"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

var _ = Describe("backend", func() {
	var (
		b   *backend
		ctx context.Context
		dir string
	)

	BeforeEach(func() {
		b = &backend{log: getTestLogger()}
		ctx = context.Background()

		Expect(func() (err error) { dir, err = ioutil.TempDir("", "repository"); return }()).To(Succeed())
		Expect(dir).ToNot(BeEmpty())

		Expect(func() (err error) { b.repo, err = git2go.New().OpenOrInitBareRepository(ctx, dir); return }()).To(Succeed())
		Expect(b.repo).ToNot(BeNil())
	})

	AfterEach(func() {
		if b.repo != nil {
			Expect(b.repo.Close()).To(Succeed())
		}

		if len(dir) > 0 {
			Expect(os.RemoveAll(dir))
		}
	})

	Describe("Compact", func() {
		type check struct {
			spec                string
			ctxFn               ContextFunc
			clusterID, memberID uint64
			metaHead            *CommitDef
			response            etcdserverpb.CompactionResponse
		}

		for clusterID, memberID := range map[uint64]uint64{
			0:  0,
			10: 2,
		} {
			for _, s := range []check{
				{
					spec:      "no reference",
					clusterID: clusterID,
					memberID:  memberID,
					response:  etcdserverpb.CompactionResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID}},
				},
				{
					spec:      "no reference with expired context",
					ctxFn:     CanceledContext,
					clusterID: clusterID,
					memberID:  memberID,
					response:  etcdserverpb.CompactionResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID}},
				},
				{
					spec:      "reference with expired context",
					ctxFn:     CanceledContext,
					clusterID: clusterID,
					memberID:  memberID,
					metaHead:  &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}},
					response:  etcdserverpb.CompactionResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID}},
				},
				{
					spec:      "reference with int revision",
					clusterID: clusterID,
					memberID:  memberID,
					metaHead:  &CommitDef{Tree: TreeDef{Blobs: map[string][]byte{metadataPathRevision: []byte("1")}}},
					response: etcdserverpb.CompactionResponse{Header: &etcdserverpb.ResponseHeader{
						ClusterId: clusterID,
						MemberId:  memberID,
						Revision:  1,
						RaftTerm:  1,
					}},
				},
			} {
				func(s check) {
					Describe(fmt.Sprintf("%s clusterID=%d memberID=%d", s.spec, s.clusterID, s.memberID), func() {
						var metaRefName git.ReferenceName

						BeforeEach(func() {
							b.refName = DefaultDataReferenceName
							b.metadataRefName = DefaultMetadataReferenceName
							b.clusterID = s.clusterID
							b.memberID = s.memberID

							Expect(func() (err error) { metaRefName, err = b.getMetadataRefName(); return }()).To(Succeed())
							Expect(metaRefName).ToNot(BeEmpty())

							if s.metaHead != nil {
								Expect(func() (err error) {
									var refName git.ReferenceName

									if refName, err = b.getMetadataRefName(); err != nil {
										return
									}

									err = CreateReferenceFromDef(ctx, b.repo, refName, s.metaHead)
									return
								}()).To(Succeed())
							}

							if s.ctxFn != nil {
								ctx = s.ctxFn(ctx)
							}
						})

						It("should succeed", func() {
							Expect(b.Compact(ctx, nil)).To(Equal(&s.response))
						})
					})
				}(s)
			}
		}
	})
})
