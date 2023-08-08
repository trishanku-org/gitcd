/*
Copyright © 2022 Amshuman K R <amshuman.kr@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/trishanku/gitcd/pkg/backend"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// pullCmd represents the pull command
var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Pull changes from remote into the backend.",
	Long:  `Pull changes from remote into the backend.`,
	Run: func(cmd *cobra.Command, args []string) {
		var (
			log        = getLogger().WithName("pull")
			ctx        context.Context
			cancelFn   context.CancelFunc
			gitImpl    = git2go.New()
			repo       git.Repository
			errs       = gitImpl.Errors()
			pis        []*pullerInfo
			err        error
			closers    []io.Closer
			waitOnDone bool
		)

		log.Info(fmt.Sprintf("Found %#v", pullFlags), "version", backend.Version)

		defer func() {
			if err != nil {
				log.Error(err, "Error pulling")
			}

			for _, cs := range closers {
				cs.Close()
			}

			log.Info("Stopped.")
		}()

		if err = os.MkdirAll(pullFlags.repoPath, 0755); err != nil {
			return
		}

		ctx, cancelFn = context.WithCancel(getContext())
		defer cancelFn()

		if repo, err = gitImpl.OpenOrInitBareRepository(ctx, pullFlags.repoPath); err != nil {
			return
		}

		defer repo.Close()

		if pis, err = organizePullerInfo(pullFlags, pullFlags); err != nil {
			return
		}

		for _, pi := range pis {
			var (
				log         = log.WithValues("dataRefName", pi.dataRefName, "remoteNames", pi.remoteNames)
				kvs         etcdserverpb.KVServer
				pullOptsFns []backend.PullOptionFunc
			)

			if kvs, err = backend.NewKVServer(
				backend.KVOptions.WithRefName(pi.dataRefName),
				backend.KVOptions.WithMetadataRefName(pi.metaRefName),
				backend.KVOptions.WithCommitterName(pullFlags.committerName),
				backend.KVOptions.WithCommitterEmail(pullFlags.committerEmail),
				backend.KVOptions.WithLogger(log),
				backend.KVOptions.WithRepoAndErrors(repo, errs),
			); err != nil {
				return
			}

			pullOptsFns = []backend.PullOptionFunc{
				backend.PullOptions.WithBackend(kvs),
				backend.PullOptions.WithRemoteNames(pi.remoteNames),
				backend.PullOptions.WithRemoteDataRefNames(pi.remoteDataRefNames),
				backend.PullOptions.WithRemoteMetadataRefNames(pi.remoteMetadataRefNames),
				backend.PullOptions.WithMergeConfictResolutions(pi.mergeConflictResolutions),
				backend.PullOptions.WithMergeRetentionPolicies(pi.mergeRetentionPolicies),
				backend.PullOptions.WithNoFastForward(pi.noFastForward),
				backend.PullOptions.WithNoFetch(pi.noFetch),
				backend.PullOptions.WithPushAfterMerge(pi.pushAfterMerge),
				backend.PullOptions.WithLogger(log),
			}

			if pullFlags.pullTickerDuration > 0 {
				var ticker = time.NewTicker(pullFlags.pullTickerDuration)

				closers = append(closers, funcCloser(ticker.Stop))

				pullOptsFns = append(
					pullOptsFns,
					backend.PullOptions.WithTicker(ticker.C),
					backend.PullOptions.WithContext(ctx),
				)

				waitOnDone = true
			} else {
				pullOptsFns = append(pullOptsFns, backend.PullOptions.WithOnce(ctx))
			}

			if err = backend.NewPull(pullOptsFns...); err != nil {
				log.Error(err, "Error pulling")
			}
		}

		if waitOnDone {
			<-ctx.Done()
		}
	},
}

type commonPullerInfo interface {
	DataRefName() *git.ReferenceName
	MetaRefName() *git.ReferenceName
	RemoteNames() *[]git.RemoteName
	RemoteDataRefNames() *[]git.ReferenceName
	RemoteMetadataRefNames() *[]git.ReferenceName
	MergeConflictResolutions() *[]git.MergeConfictResolution
	MergeRetentionPolicies() *[]git.MergeRetentionPolicy
	NoFastForward() *bool
	NoFetch() *bool
	PushAfterMerge() *bool
	DataPushRefSpec() *git.RefSpec
	MetadataPushRefSpec() *git.RefSpec
}

type pullerInfo struct {
	dataRefName, metaRefName                   git.ReferenceName
	remoteDataRefNames, remoteMetadataRefNames []git.ReferenceName
	remoteNames                                []git.RemoteName
	mergeConflictResolutions                   []git.MergeConfictResolution
	mergeRetentionPolicies                     []git.MergeRetentionPolicy
	dataPushRefSpec, metadataPushRefSpec       git.RefSpec
	noFastForward, noFetch, pushAfterMerge     bool
}

var _ commonPullerInfo = (*pullerInfo)(nil)

func (p *pullerInfo) DataRefName() *git.ReferenceName              { return &p.dataRefName }
func (p *pullerInfo) MetaRefName() *git.ReferenceName              { return &p.metaRefName }
func (p *pullerInfo) RemoteNames() *[]git.RemoteName               { return &p.remoteNames }
func (p *pullerInfo) RemoteDataRefNames() *[]git.ReferenceName     { return &p.remoteDataRefNames }
func (p *pullerInfo) RemoteMetadataRefNames() *[]git.ReferenceName { return &p.remoteMetadataRefNames }
func (p *pullerInfo) NoFastForward() *bool                         { return &p.noFastForward }
func (p *pullerInfo) NoFetch() *bool                               { return &p.noFetch }
func (p *pullerInfo) PushAfterMerge() *bool                        { return &p.pushAfterMerge }
func (p *pullerInfo) DataPushRefSpec() *git.RefSpec                { return &p.dataPushRefSpec }
func (p *pullerInfo) MetadataPushRefSpec() *git.RefSpec            { return &p.metadataPushRefSpec }

func (p *pullerInfo) MergeConflictResolutions() *[]git.MergeConfictResolution {
	return &p.mergeConflictResolutions
}

func (p *pullerInfo) MergeRetentionPolicies() *[]git.MergeRetentionPolicy {
	return &p.mergeRetentionPolicies
}

func parseMergeRetentionPolicies(regexes string) (mrp git.MergeRetentionPolicy, err error) {
	var mrps []git.MergeRetentionPolicy

	if len(regexes) <= 0 {
		mrp = git.NoneMergeRetentionPolicy()
		return
	}

	for _, s := range strings.Split(regexes, ",") {
		var re *regexp.Regexp

		if re, err = regexp.Compile(s); err != nil {
			return
		}

		mrps = append(mrps, git.RegexpMergeRetentionPolicy(re))
	}

	if len(mrps) <= 0 {
		mrp = git.NoneMergeRetentionPolicy()
		return
	}

	mrp = git.OrMergeRetentionPolicy(mrps...)
	return
}

func toRemoteNames(ss []string) (r []git.RemoteName) {
	r = make([]git.RemoteName, len(ss))

	for i, s := range ss {
		r[i] = git.RemoteName(s)
	}

	return
}

func splitByColon(s string) []string {
	return strings.Split(s, ":")
}

func toReferenceNames(ss []string) (r []git.ReferenceName) {
	r = make([]git.ReferenceName, len(ss))

	for i, s := range ss {
		r[i] = git.ReferenceName(s)
	}

	return
}

func toMergeConflictResolutions(ss []string) (r []git.MergeConfictResolution, err error) {
	r = make([]git.MergeConfictResolution, len(ss))

	for i, s := range ss {
		var v int

		if len(s) <= 0 {
			r[i] = git.DefaultConflictResolution
			continue
		}

		if v, err = strconv.Atoi(s); err != nil {
			return
		}

		r[i] = git.MergeConfictResolution(v)
	}

	return
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}

func toMergeRetentionPolicies(includes, excludes []string) (r []git.MergeRetentionPolicy, err error) {
	type retentionPolicies struct {
		includes, excludes git.MergeRetentionPolicy
	}
	var (
		defaultRetentionPoliciesInclude = git.AllMergeRetentionPolicy()
		defaultRetentionPoliciesExclude = git.NoneMergeRetentionPolicy()
		mrps                            = make([]retentionPolicies, max(len(includes), len(excludes)))
	)

	for _, spec := range []struct {
		ss   []string
		rpFn func(*retentionPolicies) *git.MergeRetentionPolicy
	}{
		{ss: includes, rpFn: func(rp *retentionPolicies) *git.MergeRetentionPolicy { return &rp.includes }},
		{ss: excludes, rpFn: func(rp *retentionPolicies) *git.MergeRetentionPolicy { return &rp.excludes }},
	} {
		for i, s := range spec.ss {
			if *spec.rpFn(&mrps[i]), err = parseMergeRetentionPolicies(s); err != nil {
				return
			}
		}
	}

	r = make([]git.MergeRetentionPolicy, len(mrps))
	for i, mrp := range mrps {
		if mrp.includes == nil {
			mrp.includes = defaultRetentionPoliciesInclude
		}

		if mrp.excludes == nil {
			mrp.excludes = defaultRetentionPoliciesExclude
		}
		r[i] = git.AndMergeRetentionPolicy(mrp.includes, git.NotMergeRetentionPolicy(mrp.excludes))
	}

	return
}

func loadPullerInfoFor(pi commonPullerInfo, backendFlags commonBackendFlags, pullFlags commonPullFlags, key string) (err error) {
	var (
		s  string
		ok bool
	)

	if *pi.MetaRefName(), err = getReferenceNameFor(*backendFlags.getMetaRefNames(), key); err != nil {
		return
	}

	if s, ok = (*pullFlags.getRemoteNames())[key]; ok {
		*pi.RemoteNames() = toRemoteNames(splitByColon(s))
	}

	if s, ok = (*pullFlags.getRemoteDataRefNames())[key]; ok {
		*pi.RemoteDataRefNames() = toReferenceNames(splitByColon(s))
	}

	if s, ok = (*pullFlags.getRemoteMetaRefNames())[key]; ok {
		*pi.RemoteMetadataRefNames() = toReferenceNames(splitByColon(s))
	}

	if s, ok = (*pullFlags.getMergeConflictResolutions())[key]; ok {
		if *pi.MergeConflictResolutions(), err = toMergeConflictResolutions(splitByColon(s)); err != nil {
			return
		}
	}

	if s, ok = (*pullFlags.getNoFastForwards())[key]; ok {
		if *pi.NoFastForward(), err = strconv.ParseBool(s); err != nil {
			return
		}
	}

	if s, ok = (*pullFlags.getNoFetches())[key]; ok {
		if *pi.NoFetch(), err = strconv.ParseBool(s); err != nil {
			return
		}
	}

	if s, ok = (*pullFlags.getPushAfterMerges())[key]; ok {
		if *pi.PushAfterMerge(), err = strconv.ParseBool(s); err != nil {
			return
		}
	}

	if *pi.MergeRetentionPolicies(), err = toMergeRetentionPolicies(
		splitByColon((*pullFlags.getMergeRetentionPoliciesInclude())[key]),
		splitByColon((*pullFlags.getMergeRetentionPoliciesExclude())[key]),
	); err != nil {
		return
	}

	return
}

func organizePullerInfo(backendFlags commonBackendFlags, pullFlags commonPullFlags) (pis []*pullerInfo, err error) {
	var m = make(map[string]*pullerInfo, len(*backendFlags.getDataRefNames()))

	defer func() {
		if err == nil {
			for _, pi := range m {
				pis = append(pis, pi)
			}
		}
	}()

	for k, v := range *backendFlags.getDataRefNames() {
		var pi = &pullerInfo{dataRefName: git.ReferenceName(v)}

		if err = loadPullerInfoFor(pi, backendFlags, pullFlags, k); err != nil {
			return
		}

		m[k] = pi
	}

	return
}

type commonPullFlags interface {
	getRemoteNames() *map[string]string
	getRemoteDataRefNames() *map[string]string
	getRemoteMetaRefNames() *map[string]string
	getMergeConflictResolutions() *map[string]string
	getMergeRetentionPoliciesInclude() *map[string]string
	getMergeRetentionPoliciesExclude() *map[string]string
	getNoFastForwards() *map[string]string
	getNoFetches() *map[string]string
	getPushAfterMerges() *map[string]string
	getPullTickerDuration() *time.Duration
}

const (
	defaultMergeRetentionPoliciesInclude = ".*" // Everything is retained
	defaultMergeRetentionPoliciesExclude = "^$" // Nothing is excluded
	defaultNoFastForward                 = false
	defaultNoFetch                       = false
	defaultPushAfterMerges               = false
)

func addCommonPullFlags(flags *pflag.FlagSet, commonFlags commonPullFlags) {
	flags.StringToStringVar(
		commonFlags.getRemoteNames(),
		"remote-names",
		map[string]string{"default": backend.DefaultRemoteName},
		"Git remote names to be used as the remotes for the backend data and metadata. Multiple values separated by `:` are supported.",
	)
	flags.StringToStringVar(
		commonFlags.getRemoteDataRefNames(),
		"remote-data-reference-names",
		map[string]string{"default": backend.DefaultRemoteDataRefName},
		"Git remote reference names to be used as remotes for the backend data. Multiple values separated by `:` are supported.",
	)
	flags.StringToStringVar(
		commonFlags.getRemoteMetaRefNames(),
		"remote-meta-reference-names",
		map[string]string{"default": backend.DefaultRemoteMetadataRefName},
		"Git remote reference names to be used as remotes for the backend metadata. Multiple values separated by `:` are supported",
	)

	flags.StringToStringVar(
		commonFlags.getMergeConflictResolutions(),
		"merge-conflict-resolutions",
		map[string]string{"default": strconv.Itoa(int(git.DefaultConflictResolution))},
		"Conflict resolution policy (favor ours or theirs) in case of merge conflicts. Multiple values separated by `:` are supported",
	)

	flags.StringToStringVar(
		commonFlags.getMergeRetentionPoliciesInclude(),
		"merge-retention-policies-include",
		map[string]string{"default": defaultMergeRetentionPoliciesInclude},
		"Comma separated list of regular expressions which match the paths to be retained in the merge changes. The individual regular expressions will be ORed. Multiple values separated by `:` are supported",
	)
	flags.StringToStringVar(
		commonFlags.getMergeRetentionPoliciesExclude(),
		"merge-retention-policies-exclude",
		map[string]string{"default": defaultMergeRetentionPoliciesExclude},
		"Comma separated list of regular expressions which match the paths to be excluded in the merge changes. The individual regular expressions will be ORed. Multiple values separated by `:` are supported",
	)

	flags.StringToStringVar(
		commonFlags.getNoFastForwards(),
		"no-fast-forwards",
		map[string]string{"default": strconv.FormatBool(defaultNoFastForward)},
		"Enable this if merge commits should always be created even when fast-forward merges are possible.",
	)
	flags.StringToStringVar(
		commonFlags.getNoFetches(),
		"no-fetches",
		map[string]string{"default": strconv.FormatBool(defaultNoFetch)},
		"Enable this if fetch should be skipped before merging from remotes.",
	)
	flags.StringToStringVar(
		commonFlags.getPushAfterMerges(),
		"push-after-merges",
		map[string]string{"default": strconv.FormatBool(defaultPushAfterMerges)},
		"Enable this if backend data and metadata should be pushed to remotes after merging merging from remotes.",
	)

	flags.DurationVar(
		commonFlags.getPullTickerDuration(),
		"pull-ticker-duration",
		defaultPullTickerDuration,
		"Interval duration to pull changes from remote. Set this to non-zero to schedule pull merges.",
	)
}

type pullFlagsImpl struct {
	repoPath                               string
	committerName                          string
	committerEmail                         string
	dataRefNames, metaRefNames             map[string]string
	remoteNames                            map[string]string
	remoteDataRefNames, remoteMetaRefNames map[string]string
	mergeConflictResolutions               map[string]string
	mergeRetentionPoliciesInclude          map[string]string
	mergeRetentionPoliciesExclude          map[string]string
	noFastForwards                         map[string]string
	noFetch                                map[string]string
	pushAfterMerges                        map[string]string
	pullTickerDuration                     time.Duration
}

var (
	_ commonBackendFlags = (*pullFlagsImpl)(nil)
	_ commonPullFlags    = (*pullFlagsImpl)(nil)

	pullFlags = &pullFlagsImpl{
		dataRefNames:                  map[string]string{},
		remoteNames:                   map[string]string{},
		remoteDataRefNames:            map[string]string{},
		remoteMetaRefNames:            map[string]string{},
		mergeConflictResolutions:      map[string]string{},
		mergeRetentionPoliciesInclude: map[string]string{},
		mergeRetentionPoliciesExclude: map[string]string{},
		noFastForwards:                map[string]string{},
		pushAfterMerges:               map[string]string{},
	}
)

func (p *pullFlagsImpl) getRepoPath() *string                { return &p.repoPath }
func (p *pullFlagsImpl) getCommitterName() *string           { return &p.committerName }
func (p *pullFlagsImpl) getCommitterEmail() *string          { return &p.committerEmail }
func (p *pullFlagsImpl) getDataRefNames() *map[string]string { return &p.dataRefNames }
func (p *pullFlagsImpl) getMetaRefNames() *map[string]string { return &p.metaRefNames }

func (p *pullFlagsImpl) getRemoteNames() *map[string]string        { return &p.remoteNames }
func (p *pullFlagsImpl) getRemoteDataRefNames() *map[string]string { return &p.remoteDataRefNames }
func (p *pullFlagsImpl) getRemoteMetaRefNames() *map[string]string { return &p.remoteMetaRefNames }
func (p *pullFlagsImpl) getNoFastForwards() *map[string]string     { return &p.noFastForwards }
func (p *pullFlagsImpl) getNoFetches() *map[string]string          { return &p.noFetch }
func (p *pullFlagsImpl) getPushAfterMerges() *map[string]string    { return &p.pushAfterMerges }
func (p *pullFlagsImpl) getPullTickerDuration() *time.Duration     { return &p.pullTickerDuration }

func (p *pullFlagsImpl) getMergeConflictResolutions() *map[string]string {
	return &p.mergeConflictResolutions
}

func (p *pullFlagsImpl) getMergeRetentionPoliciesInclude() *map[string]string {
	return &p.mergeRetentionPoliciesInclude
}

func (p *pullFlagsImpl) getMergeRetentionPoliciesExclude() *map[string]string {
	return &p.mergeRetentionPoliciesExclude
}

func init() {
	rootCmd.AddCommand(pullCmd)

	addCommonBackendFlags(pullCmd.Flags(), pullFlags)

	addCommonPullFlags(pullCmd.Flags(), pullFlags)
}
