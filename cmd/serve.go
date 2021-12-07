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
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"github.com/trishanku/gitcd/pkg/backend"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/git2go"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start GRPC service compatible with any ETCD client.",
	Long:  `Start a GRPC service compatible with any ETCD client.`,
	Run: func(cmd *cobra.Command, args []string) {
		var (
			log         = getLogger().WithName("serve")
			ctx         context.Context
			cancelFn    context.CancelFunc
			listener    net.Listener
			grpcSrv     *grpc.Server
			kvs         etcdserverpb.KVServer
			watchTicker *time.Ticker
			err         error
		)

		defer func() {
			if err != nil {
				log.Error(err, "error serving")
			}
		}()

		if err = os.MkdirAll(repoPath, 0755); err != nil {
			return
		}

		ctx, cancelFn = context.WithCancel(getContext())
		defer cancelFn()

		if listener, err = newListener(); err != nil {
			return
		}

		if grpcSrv, err = newGRPCServer(); err != nil {
			return
		}

		if kvs, err = registerKVServer(ctx, log, grpcSrv); err != nil {
			return
		}

		if err = registerClusterServer(kvs, grpcSrv); err != nil {
			return
		}

		if err = registerLeaseServer(kvs, grpcSrv); err != nil {
			return
		}

		if err = registerMaintenanceServer(kvs, grpcSrv); err != nil {
			return
		}

		if watchTicker, err = registerWatchServer(ctx, kvs, log.WithName("watch"), grpcSrv); err != nil {
			return
		}

		defer watchTicker.Stop()

		if err = registerHealthServer(grpcSrv); err != nil {
			return
		}

		err = grpcSrv.Serve(listener)
	},
}

func newListener() (l net.Listener, err error) {
	var u *url.URL

	if u, err = url.Parse(listenURL); err != nil {
		return
	}

	switch {
	case strings.HasPrefix(u.Scheme, "http"):
		return net.Listen("tcp", u.Host)
	case strings.HasPrefix(u.Scheme, "unix"):
		return net.Listen("unix", u.Path)
	default:
		return nil, fmt.Errorf("unsupported listen URL %q", listenURL)
	}
}

func newGRPCServer() (grpcSrv *grpc.Server, err error) {
	return grpc.NewServer(
		// TODO TLS
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             embed.DefaultGRPCKeepAliveMinTime,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    embed.DefaultGRPCKeepAliveInterval,
			Timeout: embed.DefaultGRPCKeepAliveTimeout,
		}),
	), nil
}

func registerKVServer(ctx context.Context, log logr.Logger, grpcSrv *grpc.Server) (kvs etcdserverpb.KVServer, err error) {
	var (
		gitImpl = git2go.New()
		repo    git.Repository
	)

	if repo, err = gitImpl.OpenOrInitBareRepository(ctx, repoPath); err != nil {
		return
	}

	if kvs, err = backend.NewKVServer(
		backend.KVOptions.WithKeyPrefix(keyPrefix),
		backend.KVOptions.WithRefName(git.ReferenceName(refName)),
		backend.KVOptions.WithMetadataRefNamePrefix(git.ReferenceName(metadataRefNamePrefix)),
		backend.KVOptions.WithClusterId(clusterId),
		backend.KVOptions.WithMemberId(memberId),
		backend.KVOptions.WithCommitterName(committerName),
		backend.KVOptions.WithCommitterEmail(committerEmail),
		backend.KVOptions.WithLogger(log),
		backend.KVOptions.WithRepoAndErrors(repo, gitImpl.Errors()),
	); err != nil {
		return
	}

	etcdserverpb.RegisterKVServer(grpcSrv, kvs)
	return
}

func registerClusterServer(kvs etcdserverpb.KVServer, grpcSrv *grpc.Server) (err error) {
	var cs etcdserverpb.ClusterServer

	if cs, err = backend.NewClusterServer(kvs, clientURLs); err != nil {
		return
	}

	etcdserverpb.RegisterClusterServer(grpcSrv, cs)
	return
}

func registerLeaseServer(kvs etcdserverpb.KVServer, grpcSrv *grpc.Server) (err error) {
	var ls etcdserverpb.LeaseServer

	if ls, err = backend.NewLeaseServer(kvs); err != nil {
		return
	}

	etcdserverpb.RegisterLeaseServer(grpcSrv, ls)
	return
}

func registerMaintenanceServer(kvs etcdserverpb.KVServer, grpcSrv *grpc.Server) (err error) {
	var ms etcdserverpb.MaintenanceServer

	if ms, err = backend.NewMaintenanceServer(kvs); err != nil {
		return
	}

	etcdserverpb.RegisterMaintenanceServer(grpcSrv, ms)
	return
}

func registerWatchServer(ctx context.Context, kvs etcdserverpb.KVServer, log logr.Logger, grpcSrv *grpc.Server) (ticker *time.Ticker, err error) {
	var (
		ws etcdserverpb.WatchServer
	)

	ticker = time.NewTicker(watchTickerDuration)

	if ws, err = backend.NewWatchServer(
		backend.WatchOptions.WithBackend(kvs),
		backend.WatchOptions.WithLogger(log),
		backend.WatchOptions.WithTicker(ticker.C),
		backend.WatchOptions.WithContext(ctx),
	); err != nil {
		return
	}

	etcdserverpb.RegisterWatchServer(grpcSrv, ws)
	return
}

func registerHealthServer(grpcSrv *grpc.Server) (err error) {
	var healthSvc = health.NewServer()

	healthSvc.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	healthpb.RegisterHealthServer(grpcSrv, healthSvc)
	return
}

var (
	repoPath              string
	keyPrefix             string
	refName               string
	metadataRefNamePrefix string
	clusterId             uint64
	memberId              uint64
	committerName         string
	committerEmail        string
	listenURL             string
	clientURLs            []string
	watchTickerDuration   time.Duration
)

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringVar(&repoPath, "repo", "/tmp/trishanku/gitcd", "Path to the Git repo to be used as the backend.")
	serveCmd.Flags().StringVar(&keyPrefix, "key-prefix", "/", "Prefix for all the keys stored in the backend.")
	serveCmd.Flags().StringVar(&refName, "reference-name", "refs/heads/main", "Git reference name to be used as the data backend.")
	serveCmd.Flags().StringVar(
		&metadataRefNamePrefix,
		"metadata-reference-name-prefix",
		backend.DefaultMetadataReferencePrefix,
		`Prefix for the Git reference name to be used as the metadata backend.
The full metadata Git referene name will be the path concatenation of the prefix and the data Git reference name.`)

	serveCmd.Flags().Uint64Var(&clusterId, "cluster-id", 0, "Id for the ETCD cluster to serve as.")
	serveCmd.Flags().Uint64Var(&memberId, "member-id", 0, "Id for the ETCD member to serve as.")

	serveCmd.Flags().StringVar(&committerName, "committer-name", "trishanku", "Name of the committer to use while making changes to the Git repo backend.")
	serveCmd.Flags().StringVar(&committerEmail, "committer-email", "trishanku@heaven.com", "Email of the committer to use while making changes to the Git repo backend.")

	serveCmd.Flags().StringVar(&listenURL, "listen-url", "http://0.0.0.0:2379/", "URL to listen for client requests to serve.")
	serveCmd.Flags().StringSliceVar(&clientURLs, "advertise-client-urls", []string{"http://127.0.0.1:2379/"}, "URLs to advertise for clients to make requests to.")

	serveCmd.Flags().DurationVar(&watchTickerDuration, "watch-ticker-duration", time.Second, "Interval duration to poll and dispatch any pending events to watches.")
}
