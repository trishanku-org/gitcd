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
					Repo:                  repo,
					Errors:                gitImpl.Errors(),
					DataRefName:           ri.dataRefName,
					MetadataRefNamePrefix: git.ReferenceName(initFlags.metaRefNamePrefix),
					StartRevision:         ri.startRevision,
					Version:               ri.version,
					Force:                 initFlags.force,
					CommitterName:         initFlags.committerName,
					CommitterEmail:        initFlags.committerEmail,
				}
			)

			log.Info("Initializing", "options", opts)

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

var initFlags = struct {
	repoPath          string
	committerName     string
	committerEmail    string
	dataRefNames      map[string]string
	metaRefNamePrefix string
	startRevisions    map[string]int64
	versions          map[string]string
	force             bool
}{}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.Flags().StringVar(&initFlags.repoPath, "repo", defaultRepoPath, "Path to the Git repo to be used as the backend.")
	initCmd.Flags().StringVar(
		&initFlags.committerName,
		"committer-name",
		defaultCommitterName,
		"Name of the committer to use while making changes to the Git repo backend.",
	)
	initCmd.Flags().StringVar(
		&initFlags.committerEmail,
		"committer-email",
		defaultCommitterEmail,
		"Email of the committer to use while making changes to the Git repo backend.",
	)
	initCmd.Flags().StringVar(
		&initFlags.metaRefNamePrefix,
		"metadata-reference-name-prefix",
		backend.DefaultMetadataReferencePrefix,
		`Prefix for the Git reference name to be used as the metadata backend.
The full metadata Git referene name will be the path concatenation of the prefix and the data Git reference name.`)

	initCmd.Flags().StringToStringVar(
		&initFlags.dataRefNames,
		"data-reference-names",
		map[string]string{"default": "refs/heads/main"},
		"Git reference names to be used as the data backend.",
	)
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
