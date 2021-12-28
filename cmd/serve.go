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
	"io"
	"net"
	"net/url"
	"os"
	"path"
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
			gitImpl     = git2go.New()
			repo        git.Repository
			sis         []*serverInfo
			closers     []io.Closer
			grpcServers map[git.ReferenceName]*grpc.Server
			watchTicker *time.Ticker
			err         error
		)

		log.Info(
			"Found",
			"repoPath", repoPath,
			"committerName", committerName,
			"committerEmail", committerEmail,
			"keyPrefix", keyPrefix,
			"dataRefNames", dataRefNames,
			"metadataRefNamePrefix", metadataRefNamePrefix,
			"clusterId", clusterId,
			"memberId", memberId,
			"listenURL", listenURL,
			"clientURLs", clientURLs,
			"watchTickerDuration", watchTickerDuration,
		)

		defer log.Info("Stopped.")

		defer func() {
			if err != nil {
				log.Error(err, "Error serving")
			}

			log.Info("Stopping GRPC servers.")
			for _, grpcSrv := range grpcServers {
				grpcSrv.Stop()
			}

			log.Info("Closing listeners.")
			for _, c := range closers {
				c.Close()
			}
		}()

		if err = os.MkdirAll(repoPath, 0755); err != nil {
			return
		}

		ctx, cancelFn = context.WithCancel(getContext())
		defer cancelFn()

		if repo, err = gitImpl.OpenOrInitBareRepository(ctx, repoPath); err != nil {
			return
		}

		defer repo.Close()

		if sis, err = organizeServerInfo(); err != nil {
			return
		}

		watchTicker = time.NewTicker(watchTickerDuration)
		defer watchTicker.Stop()

		if grpcServers, err = registerGRPCServers(sis, ctx, repo, gitImpl.Errors(), watchTicker, log); err != nil {
			return
		}

		if closers, err = startServers(sis, ctx, grpcServers, log); err != nil {
			return
		}

		<-ctx.Done()

		log.Info("Received stop signal.")
	},
}

type serverInfo struct {
	dataRefName         git.ReferenceName
	keyPrefix           string
	listenURL           *url.URL
	clientURLs          []*url.URL
	clusterId, memberId uint64
}

func organizeServerInfo() (sis []*serverInfo, err error) {
	var m = make(map[string]*serverInfo, len(dataRefNames))

	defer func() {
		if err == nil {
			for _, si := range m {
				sis = append(sis, si)
			}
		}
	}()

	for k, v := range dataRefNames {
		var (
			si = &serverInfo{dataRefName: git.ReferenceName(v)}
			ok bool
			s  string
			ss []string
			i  int64
		)

		if si.keyPrefix, ok = keyPrefix[k]; !ok {
			si.keyPrefix = defaultKeyPrefix
		}

		if s, ok = listenURL[k]; !ok {
			s = defaultListenURL
		}

		if si.listenURL, err = url.Parse(s); err != nil {
			return
		}

		if s, ok = clientURLs[k]; ok {
			ss = strings.Split(s, ",")
		} else {
			ss = defaultClientURLs
		}

		for _, s = range ss {
			var u *url.URL

			if u, err = url.Parse(s); err != nil {
				return
			}

			si.clientURLs = append(si.clientURLs, u)
		}

		if i, ok = clusterId[k]; !ok {
			i = int64(defaultClusterId)
		}

		si.clusterId = uint64(i)

		if i, ok = memberId[k]; !ok {
			i = int64(defaultMemberId)
		}

		si.memberId = uint64(i)

		m[k] = si
	}

	return
}

func registerGRPCServers(sis []*serverInfo, ctx context.Context, repo git.Repository, errs git.Errors, watchTicker *time.Ticker, log logr.Logger) (servers map[git.ReferenceName]*grpc.Server, err error) {
	if len(sis) > 0 {
		servers = make(map[git.ReferenceName]*grpc.Server, len(sis))
	}

	defer func() {
		if err != nil {
			for _, grpcSrv := range servers {
				grpcSrv.Stop()
			}

			servers = nil
		}
	}()

	for _, si := range sis {
		var (
			grpcSrv *grpc.Server
			kvs     etcdserverpb.KVServer
			log     = log.WithName(string(si.dataRefName))
		)

		if grpcSrv, err = newGRPCServer(); err != nil {
			return
		}

		if kvs, err = registerKVServer(si, ctx, repo, errs, git.ReferenceName(metadataRefNamePrefix), log, grpcSrv); err != nil {
			return
		}

		if err = registerClusterServer(si, kvs, grpcSrv); err != nil {
			return
		}

		if err = registerLeaseServer(kvs, grpcSrv); err != nil {
			return
		}

		if err = registerMaintenanceServer(kvs, grpcSrv); err != nil {
			return
		}

		if err = registerWatchServer(ctx, kvs, watchTicker, log.WithName("watch"), grpcSrv); err != nil {
			return
		}

		if err = registerHealthServer(grpcSrv); err != nil {
			return
		}

		servers[si.dataRefName] = grpcSrv
	}

	return
}

type noErrorCloser func()

var _ io.Closer = noErrorCloser(nil)

func (c noErrorCloser) Close() error {
	c()
	return nil
}

func startServers(sis []*serverInfo, ctx context.Context, grpcServers map[git.ReferenceName]*grpc.Server, log logr.Logger) (cs []io.Closer, err error) {
	for _, si := range sis {
		var (
			u       = si.listenURL
			l       net.Listener
			grpcSrv = grpcServers[si.dataRefName]
		)

		if grpcSrv == nil {
			err = fmt.Errorf("no GRPC Server found for the Git reference %q", si.dataRefName)
			return
		}

		switch {
		case strings.HasPrefix(u.Scheme, "http"):
			l, err = net.Listen("tcp", u.Host)
		case strings.HasPrefix(u.Scheme, "unix"):
			l, err = newUnixListener(ctx, u.Path)
		default:
			err = fmt.Errorf("unsupported listen URL %q", u.String())
		}

		if err != nil {
			return
		}

		cs = append(cs, l)

		go func(l net.Listener, srv *grpc.Server, log logr.Logger) {
			var err error

			log.Info("Serving")
			defer func() { log.Error(err, "Stopped.") }()

			err = srv.Serve(l)
		}(l, grpcSrv, log.WithValues("dataRefName", si.dataRefName, "url", u.String()))
	}

	return
}

func newUnixListener(ctx context.Context, pipePath string) (l net.Listener, err error) {
	if err = os.MkdirAll(path.Dir(pipePath), 0755); err != nil {
		return
	}

	if err = os.Remove(pipePath); err != nil && !os.IsNotExist(err) {
		return
	}

	defer func() {
		if err != nil && l != nil {
			l.Close()
			l = nil
		}
	}()

	if l, err = net.Listen("unix", pipePath); err != nil {
		return
	}

	go func(path string) {
		var err error

		<-ctx.Done()

		err = os.Remove(pipePath)

		log.Info("Deleting pipe", "path", pipePath, "error", err)
	}(pipePath)

	err = os.Chmod(pipePath, 0600)
	return
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

func registerKVServer(si *serverInfo, ctx context.Context, repo git.Repository, errs git.Errors, metaRefNamePrefix git.ReferenceName, log logr.Logger, grpcSrv *grpc.Server) (kvs etcdserverpb.KVServer, err error) {
	if kvs, err = backend.NewKVServer(
		backend.KVOptions.WithKeyPrefix(si.keyPrefix),
		backend.KVOptions.WithRefName(si.dataRefName),
		backend.KVOptions.WithMetadataRefNamePrefix(metaRefNamePrefix),
		backend.KVOptions.WithClusterId(si.clusterId),
		backend.KVOptions.WithMemberId(si.memberId),
		backend.KVOptions.WithCommitterName(committerName),
		backend.KVOptions.WithCommitterEmail(committerEmail),
		backend.KVOptions.WithLogger(log),
		backend.KVOptions.WithRepoAndErrors(repo, errs),
	); err != nil {
		return
	}

	etcdserverpb.RegisterKVServer(grpcSrv, kvs)
	return
}

func registerClusterServer(si *serverInfo, kvs etcdserverpb.KVServer, grpcSrv *grpc.Server) (err error) {
	var (
		cs   etcdserverpb.ClusterServer
		urls []string
	)

	for _, u := range si.clientURLs {
		urls = append(urls, u.String())
	}

	if cs, err = backend.NewClusterServer(kvs, urls); err != nil {
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

func registerWatchServer(ctx context.Context, kvs etcdserverpb.KVServer, ticker *time.Ticker, log logr.Logger, grpcSrv *grpc.Server) (err error) {
	var (
		ws etcdserverpb.WatchServer
	)

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

const (
	defaultClusterId = uint64(0)
	defaultMemberId  = uint64(0)
	defaultKeyPrefix = "/"
	defaultListenURL = "http://0.0.0.0:2379/"
)

var (
	defaultClientURLs = []string{"http://127.0.0.1:2379/"}
)

var (
	repoPath              string
	keyPrefix             = map[string]string{}
	dataRefNames          = map[string]string{}
	metadataRefNamePrefix string
	clusterId             = map[string]int64{}
	memberId              = map[string]int64{}
	committerName         string
	committerEmail        string
	listenURL             = map[string]string{}
	clientURLs            = map[string]string{}
	watchTickerDuration   time.Duration
)

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().StringVar(&repoPath, "repo", "/tmp/trishanku/gitcd", "Path to the Git repo to be used as the backend.")
	serveCmd.Flags().StringVar(
		&committerName,
		"committer-name",
		"trishanku",
		"Name of the committer to use while making changes to the Git repo backend.",
	)
	serveCmd.Flags().StringVar(
		&committerEmail,
		"committer-email",
		"trishanku@heaven.com",
		"Email of the committer to use while making changes to the Git repo backend.",
	)
	serveCmd.Flags().StringVar(
		&metadataRefNamePrefix,
		"metadata-reference-name-prefix",
		backend.DefaultMetadataReferencePrefix,
		`Prefix for the Git reference name to be used as the metadata backend.
The full metadata Git referene name will be the path concatenation of the prefix and the data Git reference name.`)

	serveCmd.Flags().StringToStringVar(
		&dataRefNames,
		"data-reference-names",
		map[string]string{"default": "refs/heads/main"},
		"Git reference names to be used as the data backend.",
	)

	serveCmd.Flags().StringToStringVar(
		&keyPrefix,
		"key-prefix",
		map[string]string{"default": "/"},
		"Prefix for all the keys stored in the backend.",
	)

	serveCmd.Flags().StringToInt64Var(
		&clusterId,
		"cluster-id",
		map[string]int64{"default": int64(defaultClusterId)},
		"Id for the ETCD cluster to serve as.",
	)
	serveCmd.Flags().StringToInt64Var(
		&memberId,
		"member-id",
		map[string]int64{"default": int64(defaultMemberId)},
		"Id for the ETCD member to serve as.",
	)
	serveCmd.Flags().StringToStringVar(
		&listenURL,
		"listen-url",
		map[string]string{"default": defaultListenURL},
		"URL to listen for client requests to serve.",
	)
	serveCmd.Flags().StringToStringVar(
		&clientURLs,
		"advertise-client-urls",
		map[string]string{"default": strings.Join(defaultClientURLs, ";")},
		`URLs to advertise for clients to make requests to. Multiple URLs separated by ";" can be specified per backend.`,
	)

	serveCmd.Flags().DurationVar(
		&watchTickerDuration,
		"watch-ticker-duration",
		time.Second,
		"Interval duration to poll and dispatch any pending events to watches.",
	)
}
