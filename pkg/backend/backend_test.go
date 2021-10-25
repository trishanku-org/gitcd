package backend

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

var _ = Describe("backend", func() {
	var b *backend

	BeforeEach(func() {
		b = &backend{}
	})

	Describe("getDataRefName", func() {
		for refName, expectErr := range map[string]bool{
			"":                true,
			"a":               false,
			"refs/heads/main": false,
		} {
			func(refName git.ReferenceName, expectErr bool) {
				var spec = fmt.Sprintf("%q should succeed", refName)

				if expectErr {
					spec = fmt.Sprintf("%q should fail", refName)
				}

				It(spec, func() {
					var (
						r   git.ReferenceName
						err error
					)

					b.refName = refName

					r, err = b.getDataRefName()

					if expectErr {
						Expect(err).To(MatchError(rpctypes.ErrGRPCCorrupt))
						Expect(r).To(BeEmpty())
					} else {
						Expect(err).ToNot(HaveOccurred())
						Expect(r).To(Equal(refName))
					}
				})
			}(git.ReferenceName(refName), expectErr)
		}
	})

	Describe("getMetadataRefName", func() {
		for _, s := range []struct {
			refName, metaRefNamePrefix, expectedMetaRefName string
			matchErr                                        types.GomegaMatcher
		}{
			{matchErr: MatchError(rpctypes.ErrGRPCCorrupt)},
			{refName: "a", expectedMetaRefName: defaultMetadataReferencePrefix + "a", matchErr: Succeed()},
			{refName: "refs/heads/main", expectedMetaRefName: defaultMetadataReferencePrefix + "refs/heads/main", matchErr: Succeed()},
			{metaRefNamePrefix: "refs/p", matchErr: MatchError(rpctypes.ErrGRPCCorrupt)},
			{refName: "a", metaRefNamePrefix: "refs/p", expectedMetaRefName: "refs/p/a", matchErr: Succeed()},
			{refName: "refs/heads/main", metaRefNamePrefix: "refs/p", expectedMetaRefName: "refs/p/refs/heads/main", matchErr: Succeed()},
			{metaRefNamePrefix: "refs/p/", matchErr: MatchError(rpctypes.ErrGRPCCorrupt)},
			{refName: "a", metaRefNamePrefix: "refs/p/", expectedMetaRefName: "refs/p/a", matchErr: Succeed()},
			{refName: "refs/heads/main", metaRefNamePrefix: "refs/p/", expectedMetaRefName: "refs/p/refs/heads/main", matchErr: Succeed()},
		} {
			func(refName, metaRefNamePrefix, expectedMetaRefName git.ReferenceName, matchErr types.GomegaMatcher) {
				It(fmt.Sprintf("refName=%q, metadataRefNamePrefix=%q", refName, metaRefNamePrefix), func() {
					var (
						r   git.ReferenceName
						err error
					)

					b.refName = refName
					b.metadataRefNamePrefix = metaRefNamePrefix

					r, err = b.getMetadataRefName()

					Expect(err).To(matchErr)
					Expect(r).To(Equal(expectedMetaRefName))
				})
			}(git.ReferenceName(s.refName), git.ReferenceName(s.metaRefNamePrefix), git.ReferenceName(s.expectedMetaRefName), s.matchErr)
		}
	})
})
