// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command ocilayer-nydus runs the out-of-process OCILayerService gRPC adapter
// daemon for Nydus RAFS v6 + Dragonfly P2P (containerd-nydus-grpc over Kernel
// EROFS + fscache).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
)

func main() {
	var (
		listenSocket       = flag.String("listen-socket", adapter.DefaultListenSocket, "Unix domain socket path for OCILayerService")
		snapshotterSocket  = flag.String("snapshotter-socket", DefaultNydusSocket, "Unix domain socket path for containerd-nydus-grpc")
		dockerConfigPath   = flag.String("docker-config", DefaultNydusDockerConfigPath, "Path to Docker config.json shared with containerd-nydus-grpc for registry credentials")
		dragonflyProxyAddr = flag.String("dragonfly-proxy-addr", "", "Dragonfly P2P dfdaemon HTTP proxy endpoint (defaults to DRAGONFLY_PROXY_ADDR env var or http://127.0.0.1:65001)")
		procFilesystems    = flag.String("proc-filesystems", DefaultProcFilesystemsPath, "Path to /proc/filesystems used to verify Kernel EROFS registration")
		cachefilesDev      = flag.String("cachefiles-dev", DefaultCachefilesDevPath, "Path to /dev/cachefiles device required for Kernel EROFS fscache mode")
		workDir            = flag.String("work-dir", "", "Directory for layer wrapper symlinks (defaults to /var/lib/ateom-gvisor/streaming/nydus)")
		namespace          = flag.String("namespace", adapter.DefaultNamespace, "containerd namespace header sent to containerd-nydus-grpc")
		snapshotterName    = flag.String("snapshotter-name", "nydus", "Snapshotter plugin name sent in Prepare/View requests")
		idleTTL            = flag.Duration("idle-ttl", adapter.DefaultLeaseIdleTTL, "Duration to keep layer views mounted after refCount drops to 0")
		declineTTL         = flag.Duration("decline-ttl", adapter.DefaultDeclineTTL, "Duration to cache declined (non-streamable) images")
		listableTimeout    = flag.Duration("listable-timeout", adapter.DefaultListableTimeout, "Maximum time to wait for a mounted EROFS layer directory to become listable")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opts := []adapter.Option{
		adapter.WithSocketPath(*snapshotterSocket),
		adapter.WithNamespace(*namespace),
		adapter.WithSnapshotterName(*snapshotterName),
		adapter.WithLeaseIdleTTL(*idleTTL),
		adapter.WithDeclineTTL(*declineTTL),
		adapter.WithListableTimeout(*listableTimeout),
	}
	if *workDir != "" {
		opts = append(opts, adapter.WithWorkDir(*workDir))
	}

	driver, err := NewNydus(NydusConfig{
		DockerConfigPath:    *dockerConfigPath,
		DragonflyProxyAddr:  *dragonflyProxyAddr,
		ProcFilesystemsPath: *procFilesystems,
		CachefilesDevPath:   *cachefilesDev,
		AdapterOpts:         opts,
	})
	if err != nil {
		slog.Error("Failed to initialize Nydus EROFS/fscache OCILayerService driver", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() { _ = driver.Close() }()

	slog.Info("Initialized Nydus + Dragonfly (Kernel EROFS + fscache) OCILayerService driver",
		slog.String("dragonflyProxyAddr", driver.DragonflyProxyAddr()),
		slog.String("procFilesystems", *procFilesystems),
		slog.String("cachefilesDev", *cachefilesDev))

	if err := adapter.ServeUnixSocket(ctx, *listenSocket, driver.Driver); err != nil {
		slog.Error("OCILayerService gRPC server exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}
