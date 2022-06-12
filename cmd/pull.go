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
	"os"
	"regexp"
	"strconv"
	"strings"

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
			log      = getLogger().WithName("pull")
			ctx      context.Context
			cancelFn context.CancelFunc
			gitImpl  = git2go.New()
			repo     git.Repository
			errs     = gitImpl.Errors()
			pis      []*pullerInfo
			err      error
		)

		log.Info(fmt.Sprintf("Found %#v", pullFlags), "version", backend.Version)

		defer log.Info("Stopped.")

		defer func() {
			if err != nil {
				log.Error(err, "Error pulling")
			}
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
				log = log.WithValues("dataRefName", pi.dataRefName, "remoteName", pi.remoteName)
				kvs etcdserverpb.KVServer
			)

			if kvs, err = backend.NewKVServer(
				backend.KVOptions.WithRefName(pi.dataRefName),
				backend.KVOptions.WithMetadataRefNamePrefix(git.ReferenceName(pullFlags.metaRefNamePrefix)),
				backend.KVOptions.WithCommitterName(serveFlags.committerName),
				backend.KVOptions.WithCommitterEmail(serveFlags.committerEmail),
				backend.KVOptions.WithLogger(log),
				backend.KVOptions.WithRepoAndErrors(repo, errs),
			); err != nil {
				return
			}

			if err = backend.NewPull(
				backend.PullOptions.WithBackend(kvs),
				backend.PullOptions.WithRemoteName(pi.remoteName),
				backend.PullOptions.WithRemoteDataRefName(pi.remoteDataRefName),
				backend.PullOptions.WithRemoteMetaRefName(pi.remoteMetaRefName),
				backend.PullOptions.WithMergeConfictResolution(pi.mergeConflictResolution),
				backend.PullOptions.WithMergeRetentionPolicy(pi.mergeRetentionPolicy),
				backend.PullOptions.WithNoFastForward(pi.noFastForward),
				backend.PullOptions.WithNoFetch(pi.noFetch),
				backend.PullOptions.WithLogger(log),
				backend.PullOptions.WithOnce(ctx),
				backend.PullOptions.WithMergerClose(ctx),
			); err != nil {
				log.Error(err, "Error pulling")
			}
		}
	},
}

type commonPullerInfo interface {
	DataRefName() *git.ReferenceName
	RemoteName() *git.RemoteName
	RemoteDataRefName() *git.ReferenceName
	RemoteMetaRefName() *git.ReferenceName
	MergeConflictResolution() *git.MergeConfictResolution
	MergeRetentionPolicy() *git.MergeRetentionPolicy
	NoFastForward() *bool
	NoFetch() *bool
}

type pullerInfo struct {
	dataRefName, remoteDataRefName, remoteMetaRefName git.ReferenceName
	remoteName                                        git.RemoteName
	mergeConflictResolution                           git.MergeConfictResolution
	mergeRetentionPolicy                              git.MergeRetentionPolicy
	noFastForward, noFetch                            bool
}

var _ commonPullerInfo = (*pullerInfo)(nil)

func (p *pullerInfo) DataRefName() *git.ReferenceName                 { return &p.dataRefName }
func (p *pullerInfo) RemoteName() *git.RemoteName                     { return &p.remoteName }
func (p *pullerInfo) RemoteDataRefName() *git.ReferenceName           { return &p.remoteDataRefName }
func (p *pullerInfo) RemoteMetaRefName() *git.ReferenceName           { return &p.remoteMetaRefName }
func (p *pullerInfo) MergeRetentionPolicy() *git.MergeRetentionPolicy { return &p.mergeRetentionPolicy }
func (p *pullerInfo) NoFastForward() *bool                            { return &p.noFastForward }
func (p *pullerInfo) NoFetch() *bool                                  { return &p.noFetch }

func (p *pullerInfo) MergeConflictResolution() *git.MergeConfictResolution {
	return &p.mergeConflictResolution
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

func loadPullerInfoFor(pi commonPullerInfo, backendFlags commonBackendFlags, pullFlags commonPullFlags, key string) (err error) {
	var (
		s                      string
		ok                     bool
		i                      int
		mrpInclude, mrpExclude git.MergeRetentionPolicy
	)

	if s, ok = (*pullFlags.getRemoteNames())[key]; ok {
		*pi.RemoteName() = git.RemoteName(s)
	}

	if s, ok = (*pullFlags.getRemoteDataRefNames())[key]; ok {
		*pi.RemoteDataRefName() = git.ReferenceName(s)
	}

	if s, ok = (*pullFlags.getRemoteMetaRefNames())[key]; ok {
		*pi.RemoteMetaRefName() = git.ReferenceName(s)
	}

	if s, ok = (*pullFlags.getMergeConflictResolutions())[key]; ok {
		if i, err = strconv.Atoi(s); err != nil {
			return
		}

		*pi.MergeConflictResolution() = git.MergeConfictResolution(i)
	} else {
		*pi.MergeConflictResolution() = defaultMergeConflictResolution
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

	for _, spec := range []struct {
		m          map[string]string
		defaultMRP string
		mrp        *git.MergeRetentionPolicy
	}{
		{m: *pullFlags.getMergeRetentionPoliciesInclude(), defaultMRP: defaultMergeRetentionPoliciesInclude, mrp: &mrpInclude},
		{m: *pullFlags.getMergeRetentionPoliciesExclude(), defaultMRP: defaultMergeRetentionPoliciesExclude, mrp: &mrpExclude},
	} {
		if s, ok = spec.m[key]; !ok {
			s = spec.defaultMRP
		}

		if *spec.mrp, err = parseMergeRetentionPolicies(s); err != nil {
			return
		}
	}

	*pi.MergeRetentionPolicy() = git.AndMergeRetentionPolicy(mrpInclude, git.NotMergeRetentionPolicy(mrpExclude))

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
}

const (
	defaultRemoteName                    = "origin"
	defaultRemoteDataRefName             = "refs/remotes/origin/main"
	defaultRemoteMetaRefName             = "refs/remotes/origin/gitcd/metadata/refs/heads/main"
	defaultMergeConflictResolution       = git.MergeConfictResolutionFavorTheirs
	defaultMergeRetentionPoliciesInclude = ".*" // Everything is retained
	defaultMergeRetentionPoliciesExclude = "^$" // Nothing is excluded
	defaultNoFastForward                 = false
	defaultNoFetch                       = false
)

func addCommonPullFlags(flags *pflag.FlagSet, commonFlags commonPullFlags) {
	flags.StringToStringVar(
		commonFlags.getRemoteNames(),
		"remote-names",
		map[string]string{"default": defaultRemoteName},
		"Git remote names to be used as the remotes for the backend data.",
	)
	flags.StringToStringVar(
		commonFlags.getRemoteDataRefNames(),
		"remote-data-reference-names",
		map[string]string{"default": defaultRemoteDataRefName},
		"Git remote reference names to be used as remotes for the data backend data.",
	)
	flags.StringToStringVar(
		commonFlags.getRemoteMetaRefNames(),
		"remote-meta-reference-names",
		map[string]string{"default": defaultRemoteMetaRefName},
		"Git remote reference names to be used as remotes for the data backend metadata.",
	)

	flags.StringToStringVar(
		commonFlags.getMergeConflictResolutions(),
		"merge-conflict-resolutions",
		map[string]string{"default": strconv.Itoa(int(defaultMergeConflictResolution))},
		"Conflict resolution policy (favor ours or theirs) in case of merge conflicts.",
	)

	flags.StringToStringVar(
		commonFlags.getMergeRetentionPoliciesInclude(),
		"merge-retention-policies-include",
		map[string]string{"default": defaultMergeRetentionPoliciesInclude},
		"Comma separated list of regular expressions which match the paths to be retained in the merge changes. The individual regular expressions will be ORed",
	)
	flags.StringToStringVar(
		commonFlags.getMergeRetentionPoliciesExclude(),
		"merge-retention-policies-exclude",
		map[string]string{"default": defaultMergeRetentionPoliciesExclude},
		"Comma separated list of regular expressions which match the paths to be excluded in the merge changes. The individual regular expressions will be ORed",
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
}

type pullFlagsImpl struct {
	repoPath                      string
	committerName                 string
	committerEmail                string
	dataRefNames                  map[string]string
	metaRefNamePrefix             string
	remoteNames                   map[string]string
	remoteDataRefNames            map[string]string
	remoteMetaRefNames            map[string]string
	mergeConflictResolutions      map[string]string
	mergeRetentionPoliciesInclude map[string]string
	mergeRetentionPoliciesExclude map[string]string
	noFastForwards                map[string]string
	noFetch                       map[string]string
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
	}
)

func (p *pullFlagsImpl) getRepoPath() *string                { return &p.repoPath }
func (p *pullFlagsImpl) getCommitterName() *string           { return &p.committerName }
func (p *pullFlagsImpl) getCommitterEmail() *string          { return &p.committerEmail }
func (p *pullFlagsImpl) getMetadataRefNamePrefix() *string   { return &p.metaRefNamePrefix }
func (p *pullFlagsImpl) getDataRefNames() *map[string]string { return &p.dataRefNames }

func (p *pullFlagsImpl) getRemoteNames() *map[string]string        { return &p.remoteNames }
func (p *pullFlagsImpl) getRemoteDataRefNames() *map[string]string { return &p.remoteDataRefNames }
func (p *pullFlagsImpl) getRemoteMetaRefNames() *map[string]string { return &p.remoteMetaRefNames }
func (p *pullFlagsImpl) getNoFastForwards() *map[string]string     { return &p.noFastForwards }
func (p *pullFlagsImpl) getNoFetches() *map[string]string          { return &p.noFetch }

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
