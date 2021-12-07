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
	"os"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/spf13/cobra"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gitcd",
	Short: "Git as a distributed key-value store.",
	Long:  `Gitcd - Git as a distributed key-value store.`,
}

var (
	debug     bool
	verbosity int8
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Enable debug logging")
	rootCmd.PersistentFlags().Int8Var(&verbosity, "verbosity", 0, "Logging verbosity")
}

var log *logr.Logger

func getLogger() *logr.Logger {
	var (
		opts []zap.Option
		zl   *zap.Logger
		err  error
	)

	if log != nil {
		return log
	}

	opts = []zap.Option{zap.IncreaseLevel(zapcore.Level(verbosity))}

	if debug {
		zl, err = zap.NewDevelopment(opts...)
	} else {
		zl, err = zap.NewProduction(opts...)
	}

	if err != nil {
		panic(err)
	}

	log = func(l logr.Logger) *logr.Logger { return &l }(zapr.NewLogger(zl))
	return log
}

func getContext() context.Context {
	return signals.SetupSignalHandler()
}
