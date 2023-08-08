package backend

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
)

var _ = Describe("puller", func() {
	var p *puller

	BeforeEach(func() {
		p = &puller{}
	})

	AfterEach(func() {
		p = nil
	})

	Describe("ensureRemoteSpecs", func() {
		for _, s := range []struct {
			init  []*remoteSpec
			n     int
			match types.GomegaMatcher
		}{
			{init: nil, n: 0, match: BeNil()},
			{init: nil, n: 1, match: Equal([]*remoteSpec{{}})},
			{init: nil, n: 4, match: Equal([]*remoteSpec{{}, {}, {}, {}})},
			{init: []*remoteSpec{{}, {}}, n: 0, match: Equal([]*remoteSpec{{}, {}})},
			{init: []*remoteSpec{{}, {}}, n: 1, match: Equal([]*remoteSpec{{}, {}})},
			{init: []*remoteSpec{{}, {}}, n: 2, match: Equal([]*remoteSpec{{}, {}})},
			{init: []*remoteSpec{{}, {}}, n: 3, match: Equal([]*remoteSpec{{}, {}, {}})},
			{init: []*remoteSpec{{}, {}}, n: 5, match: Equal([]*remoteSpec{{}, {}, {}, {}, {}})},
		} {
			func(init []*remoteSpec, n int, match types.GomegaMatcher) {
				Context(fmt.Sprintf("init=%#v, n=%d", init, n), func() {
					BeforeEach(func() {
						p.remoteSpecs = init
					})

					It("should succeed", func() {
						p.ensureRemoteSpecs(n)
						Expect(len(p.remoteSpecs)).To(BeNumerically(">=", n))
						Expect(p.remoteSpecs).To(match)
					})
				})
			}(s.init, s.n, s.match)
		}
	})
})

var _ = Describe("pullOpts", func() {
	var pOpts *pullOpts

	BeforeEach(func() {
		pOpts = &pullOpts{}
	})

	AfterEach(func() {
		pOpts = nil
	})

	Describe("WithRemoteNames", func() {
		for _, s := range []struct {
			spec                       string
			init                       []*remoteSpec
			remoteNames                []git.RemoteName
			matchErr, matchRemoteSpecs types.GomegaMatcher
		}{
			{init: nil, remoteNames: nil, matchErr: Succeed(), matchRemoteSpecs: BeNil()},
			{init: nil, remoteNames: []git.RemoteName{}, matchErr: Succeed(), matchRemoteSpecs: BeNil()},
			{init: nil, remoteNames: []git.RemoteName{"1"}, matchErr: Succeed(), matchRemoteSpecs: Equal([]*remoteSpec{{name: "1"}})},
			{init: nil, remoteNames: []git.RemoteName{"1", "2"}, matchErr: Succeed(), matchRemoteSpecs: Equal([]*remoteSpec{{name: "1"}, {name: "2"}})},
			{
				init:             []*remoteSpec{{name: "-1"}, {name: "0"}},
				remoteNames:      nil,
				matchErr:         Succeed(),
				matchRemoteSpecs: Equal([]*remoteSpec{{name: "-1"}, {name: "0"}}),
			},
			{
				init:             []*remoteSpec{{name: "-1"}, {name: "0"}},
				remoteNames:      []git.RemoteName{},
				matchErr:         Succeed(),
				matchRemoteSpecs: Equal([]*remoteSpec{{name: "-1"}, {name: "0"}}),
			},
			{
				init:             []*remoteSpec{{name: "-1"}, {name: "0"}},
				remoteNames:      []git.RemoteName{"1"},
				matchErr:         Succeed(),
				matchRemoteSpecs: Equal([]*remoteSpec{{name: "1"}, {name: "0"}}),
			},
			{
				init:             []*remoteSpec{{name: "-1"}, {name: "0"}},
				remoteNames:      []git.RemoteName{"1", "2"},
				matchErr:         Succeed(),
				matchRemoteSpecs: Equal([]*remoteSpec{{name: "1"}, {name: "2"}}),
			},
			{
				init:             []*remoteSpec{{name: "-1"}, {name: "0"}},
				remoteNames:      []git.RemoteName{"1", "2", "3"},
				matchErr:         Succeed(),
				matchRemoteSpecs: Equal([]*remoteSpec{{name: "1"}, {name: "2"}, {name: "3"}}),
			},
		} {
			func(init []*remoteSpec, remoteNames []git.RemoteName, matchErr, matchRemoteSpecs types.GomegaMatcher) {
				Context(fmt.Sprintf("init=%v, remoteNames=%v", init, remoteNames), func() {
					var p *puller

					BeforeEach(func() {
						p = &puller{remoteSpecs: init}
					})

					AfterEach(func() {
						p = nil
					})

					It("should succeed", func() {
						Expect(pOpts.WithRemoteNames(remoteNames)(p)).To(matchErr)
						Expect(p.remoteSpecs).To(matchRemoteSpecs)
					})
				})
			}(s.init, s.remoteNames, s.matchErr, s.matchRemoteSpecs)
		}
	})
})
