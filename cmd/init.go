/*
Copyright © 2021 Amshuman K R <amshuman.kr@gmail.com>

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

	"github.com/spf13/cobra"
	"github.com/trishanku/gitcd/pkg/backend"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
)

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Init a Git repo for use as a backend.",
	Long:  `Init a Git repo for use as a backend.`,
	Run: func(cmd *cobra.Command, args []string) {
		var (
			log      = getLogger().WithName("init")
			ctx      context.Context
			cancelFn context.CancelFunc
			gitImpl  = git2go.New()
			repo     git.Repository
			ris      []*refInfo
			err      error
		)

		defer func() {
			if err != nil {
				log.Error(err, "Error initializing", "err", fmt.Sprintf("%#v", err))
			}
		}()

		if ris, err = organizeRefInfo(); err != nil {
			return
		}

		if err = os.MkdirAll(serveFlags.repoPath, 0755); err != nil {
			return
		}

		ctx, cancelFn = context.WithCancel(getContext())
		defer cancelFn()

		if repo, err = gitImpl.OpenOrInitBareRepository(ctx, initFlags.repoPath); err != nil {
			return
		}

		defer repo.Close()

		for _, ri := range ris {
			var (
				log  = log.WithName(string(ri.dataRefName))
				opts = &backend.InitOpts{
					Repo:            repo,
					Errors:          gitImpl.Errors(),
					DataRefName:     ri.dataRefName,
					MetadataRefName: ri.metaRefName,
					StartRevision:   ri.startRevision,
					Version:         ri.version,
					Force:           initFlags.force,
					CommitterName:   initFlags.committerName,
					CommitterEmail:  initFlags.committerEmail,
				}
			)

			log.Info("Initializing", "repoPath", initFlags.repoPath, "options", opts)

			if err = backend.InitMetadata(ctx, opts, log); err != nil {
				log.Error(err, "Initialization failed")
				return
			}

			log.Info("Initialized successfully")
		}
	},
}

type refInfo struct {
	dataRefName   git.ReferenceName
	metaRefName   git.ReferenceName
	startRevision int64
	version       string
}

func organizeRefInfo() (ris []*refInfo, err error) {
	var m = make(map[string]*refInfo, len(initFlags.dataRefNames))

	defer func() {
		if err == nil {
			for _, ri := range m {
				ris = append(ris, ri)
			}
		}
	}()

	for k, v := range initFlags.dataRefNames {
		var (
			ri = &refInfo{dataRefName: git.ReferenceName(v)}
			ok bool
			s  string
			i  int64
		)

		if ri.metaRefName, err = getReferenceNameFor(initFlags.metaRefNames, k); err != nil {
			return
		}

		if i, ok = initFlags.startRevisions[k]; !ok {
			i = defaultStartRevision
		}
		ri.startRevision = i

		if s, ok = initFlags.versions[k]; !ok {
			s = backend.Version
		}
		ri.version = s

		m[k] = ri
	}

	return
}

const (
	defaultStartRevision = int64(1)
)

type initFlagsImpl struct {
	repoPath       string
	committerName  string
	committerEmail string
	dataRefNames   map[string]string
	metaRefNames   map[string]string
	startRevisions map[string]int64
	versions       map[string]string
	force          bool
}

var (
	_ commonBackendFlags = (*initFlagsImpl)(nil)

	initFlags = &initFlagsImpl{
		dataRefNames:   map[string]string{},
		startRevisions: map[string]int64{},
		versions:       map[string]string{},
	}
)

func (i *initFlagsImpl) getRepoPath() *string                { return &i.repoPath }
func (i *initFlagsImpl) getCommitterName() *string           { return &i.committerName }
func (i *initFlagsImpl) getCommitterEmail() *string          { return &i.committerEmail }
func (i *initFlagsImpl) getMetaRefNames() *map[string]string { return &i.metaRefNames }
func (i *initFlagsImpl) getDataRefNames() *map[string]string { return &i.dataRefNames }

func init() {
	rootCmd.AddCommand(initCmd)

	addCommonBackendFlags(initCmd.Flags(), initFlags)

	initCmd.Flags().StringToInt64Var(
		&initFlags.startRevisions,
		"start-revisions",
		map[string]int64{"default": defaultStartRevision},
		"Revision to be used for the oldest commit in the data reference.",
	)
	initCmd.Flags().StringToStringVar(
		&initFlags.versions,
		"versions",
		map[string]string{"default": backend.Version},
		"Revision to be used for the oldest commit in the data reference.",
	)

	initCmd.Flags().BoolVar(&initFlags.force, "force", false, "Force re-create/overwrite the metadata reference even if it exists.")
}
