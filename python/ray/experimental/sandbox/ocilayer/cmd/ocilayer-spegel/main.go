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

// Command ocilayer-spegel runs the out-of-process OCILayerService gRPC adapter
// daemon for stateless P2P OCI layer distribution via Spegel
// (--setup-image-streaming=spegel).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
)

func main() {
	var (
		listenSocket       = flag.String("listen-socket", adapter.DefaultListenSocket, "UNIX domain socket path where OCILayerService listens")
		workDir            = flag.String("work-dir", "/var/lib/ateom-gvisor/streaming/spegel", "Base directory for Spegel layer wrapper symlinks")
		storeDir           = flag.String("store-dir", DefaultSpegelStoreDir, "Base directory for content-addressed Spegel OCI blobs and unpacked layers")
		dockerConfig       = flag.String("docker-config", DefaultDockerConfigPath, "Path to Docker config.json used for upstream registry credentials")
		spegelMirrorAddr   = flag.String("spegel-mirror-addr", "", "Local Spegel OCI registry mirror endpoint (default http://127.0.0.1:5000 or $SPEGEL_MIRROR_ADDR)")
		spegelRouterAddr   = flag.String("spegel-router-addr", "", "Spegel P2P router/DHT listen address (default :5001 or $SPEGEL_ROUTER_ADDR)")
		spegelBootstrapDNS = flag.String("spegel-bootstrap-dns", "", "Headless Service DNS domain for discovering cluster Spegel peers (or $SPEGEL_BOOTSTRAP_DNS)")
		spegelPeerPort     = flag.Int("spegel-peer-port", DefaultSpegelPeerPort, "Port used to query discovered cluster Spegel peers")
		spegelPeers        = flag.String("spegel-peers", "", "Comma-separated static Spegel peer host:port endpoints")
		upstreamMirrorAddr = flag.String("spegel-upstream-mirror-addr", "", "Optional secondary local Spegel daemon endpoint (e.g. http://127.0.0.1:5003)")
		enableP2PServer    = flag.Bool("enable-p2p-server", false, "Start built-in Spegel-compatible P2P OCI registry mirror HTTP server on --p2p-listen-addr")
		p2pListenAddr      = flag.String("p2p-listen-addr", ":5000", "TCP listen address for the built-in Spegel P2P OCI registry mirror server")
		verifySpegelReady  = flag.Bool("verify-spegel-ready", true, "Probe local Spegel mirror /healthz during CanProvide and PrepareImage")
		idleTTL            = flag.Duration("idle-ttl", adapter.DefaultLeaseIdleTTL, "How long to keep an image's layer lease active after refCount drops to 0 (0 disables)")
		declineTTL         = flag.Duration("decline-ttl", adapter.DefaultDeclineTTL, "How long to cache declined (non-streamable) images")
		listableTimeout    = flag.Duration("listable-timeout", adapter.DefaultListableTimeout, "Maximum time to wait for a layer directory to become listable")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var staticPeers []string
	if strings.TrimSpace(*spegelPeers) != "" {
		for _, p := range strings.Split(*spegelPeers, ",") {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				staticPeers = append(staticPeers, trimmed)
			}
		}
	}

	driver, err := NewSpegel(SpegelConfig{
		StoreDir:                 *storeDir,
		DockerConfigPath:         *dockerConfig,
		SpegelMirrorAddr:         *spegelMirrorAddr,
		SpegelRouterAddr:         *spegelRouterAddr,
		SpegelBootstrapDNS:       *spegelBootstrapDNS,
		SpegelPeerPort:           *spegelPeerPort,
		SpegelPeers:              staticPeers,
		SpegelUpstreamMirrorAddr: *upstreamMirrorAddr,
		EnableP2PServer:          *enableP2PServer,
		P2PListenAddr:            *p2pListenAddr,
		VerifySpegelReady:        *verifySpegelReady,
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(*workDir),
			adapter.WithLeaseIdleTTL(*idleTTL),
			adapter.WithDeclineTTL(*declineTTL),
			adapter.WithListableTimeout(*listableTimeout),
		},
	})
	if err != nil {
		slog.Error("Failed to initialize Spegel P2P OCILayerService driver", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() { _ = driver.Close() }()

	slog.Info("Initialized Spegel P2P OCILayerService driver",
		slog.String("spegelMirrorAddr", driver.SpegelMirrorAddr()),
		slog.String("spegelRouterAddr", driver.SpegelRouterAddr()),
		slog.String("spegelBootstrapDNS", driver.SpegelBootstrapDNS()),
		slog.String("storeDir", driver.StoreDir()),
		slog.Bool("enableP2PServer", *enableP2PServer),
		slog.Bool("verifySpegelReady", *verifySpegelReady),
		slog.Duration("idleTTL", *idleTTL))

	if err := adapter.ServeUnixSocket(ctx, *listenSocket, driver.Driver); err != nil {
		slog.Error("OCILayerService server failed", slog.Any("err", err))
		os.Exit(1)
	}
}
