# Ray Sandbox `OCILayerService` Image Streaming Integration & Live GKE E2E Guide

This document details the architecture, code changes, build & test commands, live GKE cluster end-to-end (E2E) verification steps, verbatim logs/outputs, and live-cluster bug fixes for integrating the `OCILayerService` image-streaming API (from `substrate` branch `image-streaming-oci-layer-service-v2`) into Ray's sandbox [`ImageManager`](../image_manager.py).

---

## 1. Architecture & Workflow Overview

Instead of downloading and extracting full OCI layer tarballs locally via `crane` before starting a gVisor (`runsc`) sandbox, [`ImageManager.pull_image`](../image_manager.py) queries a node-local `OCILayerService` gRPC Unix domain socket (`RAY_SANDBOX_OCI_LAYER_SOCKET`, defaulting to `/run/ate-image-streaming/ocilayer.sock`).

```mermaid
sequenceDiagram
    participant Backend as GVisorSandboxBackend
    participant IM as ImageManager (Python)
    participant Client as oci_layer_client (gRPC)
    participant Adapter as OCILayerService Adapter (Go)
    participant Snap as Riptide (gcfsd) / Nydus+Dragonfly (EROFS)
    participant Runsc as gVisor (runsc)

    Backend->>IM: pull_image(image_ref, instance_id="sbx-1")
    IM->>Client: can_provide(image_ref)
    Client->>Adapter: CanProvide(CanProvideRequest)
    Adapter-->>Client: CanProvideResponse(supported=true)
    IM->>Client: prepare_image(image_ref)
    Client->>Adapter: PrepareImage(PrepareImageRequest)
    Adapter->>Snap: Resolve manifest & mount lazy layer views
    Adapter->>Adapter: Expose merged read-only <imageWorkDir>/rootfs<br/>(1 layer: symlink -> layer-0/fs;<br/>>1 layers: read-only kernel overlayfs)
    Adapter-->>Client: PrepareImageResponse(rootfs_dir, layer_dirs, env, working_dir, provider)
    IM->>IM: Write <image_dir>/.streamed.json & record reference count
    Backend->>IM: prepare_oci_bundle(...) / create_oci_spec(...)
    IM-->>Backend: config.json (root.path = rootfs_dir,<br/>rootfs.source = rootfs_dir, rootfs.type = "bind", rootfs.overlay = "memory")
    Backend->>Runsc: runsc run <sandbox_id>
    Backend->>IM: release_image(image_ref, instance_id="sbx-1")
    IM->>Client: release_image(image_ref)
    Client->>Adapter: ReleaseImage(ReleaseImageRequest)
```

### Key Design Decisions
1. **Merged Read-Only `rootfs_dir` in the Go Adapter**:
   - **Single-layer providers (`nydus-dragonfly`)**: `<imageWorkDir>/rootfs` is a symlink to `layer-0/fs` (which points to the committed Nydus RAFS v6 kernel EROFS mount `/var/lib/ateom-gvisor/containerd-nydus/snapshots/<id>/mnt`).
   - **Multi-layer providers (`riptide`, `soci`, `spegel`)**: `<imageWorkDir>/rootfs` is a read-only kernel `overlayfs` mount (`ro,redirect_dir=on`) stacking `layer-(N-1)/fs` (topmost) down to `layer-0/fs` (bottommost).
2. **Per-Sandbox Reference Counting & Pre-Pull Claim Semantics**:
   - Every `PrepareImage` RPC increments the adapter's per-image `refCount`, and every `ReleaseImage` RPC decrements it (starting an idle TTL timer when `refCount == 0`).
   - In [`ImageManager._pull_streamed_image`](../image_manager.py), a pre-pull (`instance_id=None`) acquires one adapter ref (`_streamed_prepulled_refs`), and the first sandbox instance (`instance_id="sbx-1"`) claims that pre-pulled ref without issuing an extra `PrepareImage` RPC, preventing leaked references.
3. **Graceful Fallback**:
   - If the `OCILayerService` socket is absent, `CanProvide` returns `false`, or `PrepareImage` returns `grpc.StatusCode.FAILED_PRECONDITION` (e.g., a standard non-RAFS OCI image sent to `ocilayer-nydus-dragonfly`), [`ImageManager.pull_image`](../image_manager.py) logs and falls back to `pull_and_extract_container_image` (`crane`).

---

## 2. Commit Structure

To keep the upstream `substrate` copy cleanly separated from Ray-specific adjustments, the changes are organized into separate DCO-signed (`-s`) commits:

| Commit | SHA | Description |
| :--- | :--- | :--- |
| **Commit 1** | `a622e3224e` | `[sandbox] Copy Go ocilayer adapter verbatim from substrate (image-streaming-oci-layer-service-v2)` — Verbatim copy of 40 files (`14,438` insertions) from `substrate` (`1cf91aea`) into `python/ray/experimental/sandbox/ocilayer/`. |
| **Commit 2** | `d55af15d1e` | `[sandbox] Integrate OCILayerService rootfs_dir streaming into ImageManager` — All Go `rootfs_dir` adjustments, Python gRPC client, `ImageManager` integration, live-cluster fixes, and unit tests (12 files changed, `957` insertions). |
| **Commit 3** | *(this commit)* | `[sandbox] Add OCILayerService streaming integration and E2E verification doc` |

To inspect the exact delta between the verbatim `substrate` adapter and the Ray integration:
```bash
git diff a622e3224e..d55af15d1e -- python/ray/experimental/sandbox/ocilayer/
```

---

## 3. Local Build & Unit Test Commands and Outputs

### 3.1 Build & Test the Go `OCILayerService` Adapters

```bash
# Run Go unit tests (client, server, driver lifecycle, orphan sweep, rootfs_dir)
go -C python/ray/experimental/sandbox/ocilayer test -v ./internal/ocilayer/...

# Build static Linux amd64 binaries for Riptide and Nydus-Dragonfly
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C python/ray/experimental/sandbox/ocilayer build -o /tmp/ocilayer-riptide ./cmd/ocilayer-riptide

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C python/ray/experimental/sandbox/ocilayer build -o /tmp/ocilayer-nydus-dragonfly ./cmd/ocilayer-nydus-dragonfly
```

**Output:**
```text
=== RUN   TestGRPCClientLifecycle
--- PASS: TestGRPCClientLifecycle (0.01s)
=== RUN   TestGRPCClientNotSupportedMapping
--- PASS: TestGRPCClientNotSupportedMapping (0.00s)
PASS
ok  	github.com/agent-substrate/substrate/internal/ocilayer	0.015s
=== RUN   TestServerAndDriverLifecycle
2026/10/17 04:23:29 INFO PrepareImage timing breakdown provider=test-provider image=us-central1-docker.pkg.dev/my-proj/repo/agent:v1 digest=sha256:9999999999999999999999999999999999999999999999999999999999999999 layers=2 resolve_ms=0.001 stat_ms=0.002 prepare_ms=0.002 view_ms=0.001 listable_ms=0.234 wrapper_ms=1.877 total_ms=2.181
--- PASS: TestServerAndDriverLifecycle (0.01s)
=== RUN   TestServerDeclineReturnsErrNotSupported
--- PASS: TestServerDeclineReturnsErrNotSupported (0.00s)
=== RUN   TestServerReconcileImagesRestoresAndSweepsOrphans
2026/10/17 04:23:29 INFO Reconciled active streamed image lease provider=test-provider image=example.com/active:v1 refCount=3
--- PASS: TestServerReconcileImagesRestoresAndSweepsOrphans (0.01s)
PASS
ok  	github.com/agent-substrate/substrate/internal/ocilayer/adapter	0.094s
```

### 3.2 Run Python `ImageManager` Streaming Unit Tests

The test suite in [`test_image_manager_streaming.py`](../tests/test_image_manager_streaming.py) starts an in-process gRPC `OCILayerService` server on a temporary Unix socket and tests:
1. Streamed `pull_image`, `.streamed.json` persistence, `is_image_extracted`, `get_workdir`, `get_envs`, and `create_oci_spec`.
2. Multi-sandbox reference counting and `release_image`.
3. Pre-pull (`instance_id=None`) followed by sandbox claim (`instance_id="sbx-1"`) without leaking adapter references.
4. Automatic fallback to `pull_and_extract_container_image` when `PrepareImage` returns `FAILED_PRECONDITION` or when no socket is present.

```bash
pytest -v python/ray/experimental/sandbox/tests/test_image_manager_streaming.py
```

---

## 4. Live GKE Cluster Environment (`kuiyue-gke-dev`)

- **Cluster**: `gke_kuiyue-gke-dev_us-central1-a_substrate-stream-test` (Zone: `us-central1-a`, Project: `kuiyue-gke-dev`)
- **Nodes & Active Daemons**:
  1. **Riptide Node (COS)**: `gke-substrate-stream-substrate-node-p-01c8a8ea-65vz`
     - Pod: `ate-system/ate-ocilayer-riptide-19129425-6nk77`
     - Snapshotter socket: `/run/ate-image-streaming/gcfs-snapshotter.sock`
     - Keychain socket: `/run/ate-image-streaming/gcfsd/keychain.sock`
     - Test OCI Image (10 layers, ~1.19 GB):
       `us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4` (`:benchmark-9layer`)
  2. **Nydus + Dragonfly v2 Node (Ubuntu w/ Kernel EROFS + fscache)**: `gke-substrate-stream-c3-std22-ubuntu--5a335c0d-5h2p`
     - Helper Pod: `ate-system/bench-helper-5h2p` (`hostPID: true`, `privileged: true`)
     - Nydus snapshotter socket: `/run/containerd-nydus/containerd-nydus-grpc.sock`
     - Dragonfly local peer proxy: `http://127.0.0.1:65001` (Scheduler: `10.96.1.32:8002`, Manager: `10.96.1.31:65003`)
     - Test Nydus RAFS v6 Image (~1.21 GB):
       `us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6`

---

## 5. Step-by-Step Live Cluster E2E Verification: Riptide (`ocilayer-riptide`)

### 5.1 E2E Test Command ([`benchmarks/run_ray_streaming_e2e.py`](benchmarks/run_ray_streaming_e2e.py) `--provider=riptide`)

On node `gke-substrate-stream-substrate-node-p-01c8a8ea-65vz` (inside `ate-ocilayer-riptide-19129425-6nk77`), the E2E harness launches the compiled `ocilayer-riptide` binary against the live `gcfs-snapshotter.sock` and `gcfsd/keychain.sock`, pulls the 10-layer image via `ImageManager`, verifies the kernel `overlayfs` mount at `rootfs_dir`, boots a real `runsc` container on the streamed rootfs, and verifies teardown:

```bash
kubectl exec -n ate-system ate-ocilayer-riptide-19129425-6nk77 -c riptide-provider -- sh -c '
export PATH=/home/kubernetes/bin/ray-e2e/bin:/home/kubernetes/bin:$PATH
export PYTHONPATH=/home/kubernetes/bin/ray-e2e/site-packages
python3 /home/kubernetes/bin/ray-e2e/run_ray_streaming_e2e.py \
  --provider=riptide \
  --adapter-bin=/home/kubernetes/bin/ray-e2e/bin/ocilayer-riptide \
  --image-ref=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4
'
```

### 5.2 Verbatim Riptide E2E Output & Adapter Logs ([`benchmarks/logs/e2e_riptide.log`](benchmarks/logs/e2e_riptide.log))

```text
=== [1/2] Running in-process gRPC unit test suite (test_image_manager_streaming) ===
PASS: All in-process gRPC unit tests succeeded!

=== [2/2] Running Live E2E Test for provider=riptide ===
[riptide] can_provide(us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4) -> True
[riptide] Cold pull_image completed in 330.4 ms -> /tmp/ray-images-riptide-58x_k9_q/images/us-central1-docker.pkg.dev_kuiyue-gke-dev_kuiyue-gke-dev-repo_ate-images_sandbox-04176181c57f0bf23e61506b0ddcf1fd_sha256_c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4
[riptide] .streamed.json: {
  "rootfs_dir": "/var/lib/ateom-gvisor/streaming/ray-e2e-riptide/us-central1-docker.pkg.dev_kuiyue-gke-dev_kuiyue-gke-dev-repo_ate-images_sandbox-04176181c57f0bf23e61506b0ddcf1fd_sha256_c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4/rootfs",
  "provider": "riptide",
  "image_digest": "sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4",
  "image_ref": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4"
}
[riptide] rootfs_dir top-level entries (26): ['bin', 'boot', 'builder', 'dev', 'etc', 'home', 'ko-app', 'lib', 'lib32', 'lib64', 'libx32', 'media', 'mnt', 'opt', 'proc']
[riptide] Backing mount for rootfs_dir: none /var/lib/ateom-gvisor/streaming/ray-e2e-riptide/.../rootfs overlay ro,relatime,lowerdir+=/var/lib/ateom-gvisor/streaming/ray-e2e-riptide/.../layer-9/fs,lowerdir+=.../layer-8/fs,lowerdir+=.../layer-7/fs,lowerdir+=.../layer-6/fs,lowerdir+=.../layer-5/fs,lowerdir+=.../layer-4/fs,lowerdir+=.../layer-3/fs,lowerdir+=.../layer-2/fs,lowerdir+=.../layer-1/fs,lowerdir+=.../layer-0/fs,redirect_dir=on 0 0
[riptide] Warm pull_image completed in 3.55 ms
[riptide] OCI Spec root={'path': '/var/lib/ateom-gvisor/streaming/ray-e2e-riptide/.../rootfs', 'readonly': False} annotations={'dev.gvisor.spec.rootfs.source': '/var/lib/ateom-gvisor/streaming/ray-e2e-riptide/.../rootfs', 'dev.gvisor.spec.rootfs.type': 'bind', 'dev.gvisor.spec.rootfs.overlay': 'memory', 'dev.gvisor.spec.rootfs.options': 'size=268435456'} cwd=/ env_count=2
[riptide] Executing gVisor container on streamed rootfs: runsc --root=/tmp/runsc-ray-e2e-riptide --network=none --ignore-cgroups run --bundle /tmp/ray-images-riptide-58x_k9_q/bundle ray-e2e-riptide-1792210158
[riptide] runsc exit_code=0
[riptide] runsc stdout:
GVISOR_STREAMED_OK provider=riptide
Linux runsc 4.19.0-gvisor #1 SMP Sun Jan 10 15:06:54 PST 2016 x86_64 x86_64 x86_64 GNU/Linux
Filesystem      Size  Used Avail Use% Mounted on
none            256M     0  256M   0% /
-r-xr-xr-x 1 root root 9855748 Jan  1  1970 /ko-app/sandbox
-rw-r--r-- 1 root root 0 Oct 17 04:09 /overlay_write_test
[riptide] Verified lease release and rootfs_dir cleanup!
--- [riptide] Adapter daemon logs ---
2026/10/17 04:09:17 INFO OCILayerService adapter listening provider=riptide listenSocket=/run/ate-image-streaming/ocilayer-ray-e2e-riptide.sock snapshotterSocket=/run/ate-image-streaming/gcfs-snapshotter.sock
2026/10/17 04:09:18 INFO Pushed credentials to Riptide gcfsd keychain socket=/run/ate-image-streaming/gcfsd/keychain.sock image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4 server=us-central1-docker.pkg.dev username=_token
2026/10/17 04:09:18 INFO PrepareImage timing breakdown provider=riptide image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd@sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4 digest=sha256:c8c61a46b939c1b801e08930a2a7cc850b6f8790672634dd26cb8a38ba2965d4 layers=10 resolve_ms=293.455 stat_ms=4.197 prepare_ms=0 view_ms=23.643 listable_ms=1.211 wrapper_ms=2.429 total_ms=326.591
2026/10/17 04:09:18 INFO Shutting down OCILayerService server socket=/run/ate-image-streaming/ocilayer-ray-e2e-riptide.sock
```

### 5.3 Full `GVisorSandboxBackend` Lifecycle Verification with Riptide

We also tested the higher-level [`GVisorSandboxBackend`](../backend/gvisor.py) (`create_sandbox` $\rightarrow$ `exec_command` $\rightarrow$ `write_file` $\rightarrow$ `read_file` $\rightarrow$ `delete_sandbox`) backed by `ocilayer-riptide`:

```text
[GVisorSandboxBackend + Riptide] create_sandbox(ray-sandbox-a81c4f9201b3) completed in 464.6 ms: status=SandboxStatus.RUNNING
[GVisorSandboxBackend + Riptide] exec_command exit_code= 0
Linux runsc 4.19.0-gvisor #1 SMP Sun Jan 10 15:06:54 PST 2016 x86_64 x86_64 x86_64 GNU/Linux
Filesystem      Size  Used Avail Use% Mounted on
none            256M     0  256M   0% /
-r-xr-xr-x 1 root root 9.4M Jan  1  1970 /ko-app/sandbox
[GVisorSandboxBackend + Riptide] read_file(/tmp/hello_ray.txt) -> b'streamed via riptide!'
[GVisorSandboxBackend + Riptide] delete_sandbox(ray-sandbox-a81c4f9201b3) OK!
```

---

## 6. Step-by-Step Live Cluster E2E Verification: Nydus + Dragonfly v2 (`ocilayer-nydus-dragonfly`)

### 6.1 E2E Test Command ([`benchmarks/run_ray_streaming_e2e.py`](benchmarks/run_ray_streaming_e2e.py) `--provider=nydus-dragonfly`)

On node `gke-substrate-stream-c3-std22-ubuntu--5a335c0d-5h2p` (via `bench-helper-5h2p` using `nsenter -t 1 -m -u -i -n -p`), we ran two consecutive passes of `run_ray_streaming_e2e.py` to verify both cold/warm streaming and that `ReleaseImage(bypass_ttl=True)` preserves the underlying Nydus kernel EROFS mount across teardown:

```bash
kubectl exec -n ate-system bench-helper-5h2p -- nsenter -t 1 -m -u -i -n -p -- sh -c '
export PATH=/var/lib/ateom-gvisor/ray-e2e/bin:$PATH
export PYTHONPATH=/var/lib/ateom-gvisor/ray-e2e/site-packages
python3 /var/lib/ateom-gvisor/ray-e2e/run_ray_streaming_e2e.py \
  --provider=nydus-dragonfly \
  --adapter-bin=/var/lib/ateom-gvisor/ray-e2e/bin/ocilayer-nydus-dragonfly \
  --image-ref=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6 && \
echo "=== Verified first run and bypass_ttl release; checking EROFS mount preservation and running second pass ===" && \
grep -F "/var/lib/ateom-gvisor/containerd-nydus/snapshots/" /proc/mounts && \
python3 /var/lib/ateom-gvisor/ray-e2e/run_ray_streaming_e2e.py \
  --provider=nydus-dragonfly \
  --adapter-bin=/var/lib/ateom-gvisor/ray-e2e/bin/ocilayer-nydus-dragonfly \
  --image-ref=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6
'
```

### 6.2 Verbatim Nydus + Dragonfly v2 E2E Output & Adapter Logs ([`benchmarks/logs/e2e_nydus_dragonfly.log`](benchmarks/logs/e2e_nydus_dragonfly.log))

```text
=== [1/2] Running in-process gRPC unit test suite (test_image_manager_streaming) ===
PASS: All in-process gRPC unit tests succeeded!

=== [2/2] Running Live E2E Test for provider=nydus-dragonfly ===
[nydus-dragonfly] can_provide(us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6) -> True
[nydus-dragonfly] Cold pull_image completed in 440.5 ms -> /tmp/ray-images-nydus-dragonfly-rawho4pj/images/us-central1-docker.pkg.dev_kuiyue-gke-dev_kuiyue-gke-dev-repo_ate-images_sandbox-04176181c57f0bf23e61506b0ddcf1fd_nydus-v6
[nydus-dragonfly] .streamed.json: {
  "rootfs_dir": "/var/lib/ateom-gvisor/streaming/ray-e2e-nydus-dragonfly/us-central1-docker.pkg.dev_kuiyue-gke-dev_kuiyue-gke-dev-repo_ate-images_sandbox-04176181c57f0bf23e61506b0ddcf1fd_nydus-v6/rootfs",
  "provider": "nydus-dragonfly",
  "image_digest": "sha256:63efd7bfec93de795766e03c46b21496df0679548e46a37eb6291b3f5a9fff7c",
  "image_ref": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6"
}
[nydus-dragonfly] rootfs_dir top-level entries (25): ['bin', 'boot', 'builder', 'dev', 'etc', 'home', 'ko-app', 'lib', 'lib32', 'lib64', 'libx32', 'media', 'mnt', 'opt', 'proc']
[nydus-dragonfly] Backing mount for rootfs_dir: erofs /var/lib/ateom-gvisor/containerd-nydus/snapshots/21/mnt erofs ro,relatime,user_xattr,acl,cache_strategy=readaround,fsid=99737bbb3fb54b88420978f3c2b88b1ff7df5e848f1bfb06870ff4f8f057ce76 0 0
[nydus-dragonfly] Warm pull_image completed in 3.36 ms
[nydus-dragonfly] OCI Spec root={'path': '/var/lib/ateom-gvisor/streaming/ray-e2e-nydus-dragonfly/us-central1-docker.pkg.dev_kuiyue-gke-dev_kuiyue-gke-dev-repo_ate-images_sandbox-04176181c57f0bf23e61506b0ddcf1fd_nydus-v6/rootfs', 'readonly': False} annotations={'dev.gvisor.spec.rootfs.source': '/var/lib/ateom-gvisor/streaming/ray-e2e-nydus-dragonfly/us-central1-docker.pkg.dev_kuiyue-gke-dev_kuiyue-gke-dev-repo_ate-images_sandbox-04176181c57f0bf23e61506b0ddcf1fd_nydus-v6/rootfs', 'dev.gvisor.spec.rootfs.type': 'bind', 'dev.gvisor.spec.rootfs.overlay': 'memory'} cwd=/ env_count=2
[nydus-dragonfly] Executing gVisor container on streamed rootfs: runsc --root=/tmp/runsc-ray-e2e-nydus-dragonfly --network=none --ignore-cgroups run --bundle /tmp/ray-images-nydus-dragonfly-rawho4pj/bundle ray-e2e-nydus-dragonfly-1792211255
[nydus-dragonfly] runsc exit_code=0
[nydus-dragonfly] runsc stdout:
GVISOR_STREAMED_OK provider=nydus-dragonfly
Linux runsc 4.19.0-gvisor #1 SMP Sun Jan 10 15:06:54 PST 2016 x86_64 x86_64 x86_64 GNU/Linux
-r-xr-xr-x 1 root root 9855748 Jan  1  1970 /ko-app/sandbox
-rw-r--r-- 1 root root 0 Oct 17 04:27 /overlay_write_test
[nydus-dragonfly] Verified lease release and rootfs_dir cleanup!
--- [nydus-dragonfly] Adapter daemon logs ---
2026/10/17 04:27:35 INFO Initialized cross-node Nydus + Dragonfly v2 (Kernel EROFS + fscache) OCILayerService driver dragonflyProxyAddr=http://127.0.0.1:65001 dragonflySchedulerAddr=10.96.1.32:8002 dragonflyManagerAddr=10.96.1.31:65003 verifyDragonflyPing=true procFilesystems=/proc/filesystems cachefilesDev=/dev/cachefiles
2026/10/17 04:27:35 INFO OCILayerService adapter listening provider=nydus-dragonfly listenSocket=/run/ate-image-streaming/ocilayer-ray-e2e-nydus-dragonfly.sock snapshotterSocket=/run/containerd-nydus/containerd-nydus-grpc.sock
2026/10/17 04:27:35 INFO Synchronized registry credentials to docker config path=/run/containerd-nydus/docker/config.json image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6 server=us-central1-docker.pkg.dev username=_token
2026/10/17 04:27:35 INFO PrepareImage timing breakdown provider=nydus-dragonfly image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6 digest=sha256:63efd7bfec93de795766e03c46b21496df0679548e46a37eb6291b3f5a9fff7c layers=1 resolve_ms=332.341 stat_ms=1.856 prepare_ms=0 view_ms=1.457 listable_ms=0.026 wrapper_ms=0.242 total_ms=435.787
2026/10/17 04:27:35 INFO Shutting down OCILayerService server socket=/run/ate-image-streaming/ocilayer-ray-e2e-nydus-dragonfly.sock

=== Verified first run and bypass_ttl release; checking EROFS mount preservation and running second pass ===
erofs /var/lib/ateom-gvisor/containerd-nydus/snapshots/21/mnt erofs ro,relatime,user_xattr,acl,cache_strategy=readaround,fsid=99737bbb3fb54b88420978f3c2b88b1ff7df5e848f1bfb06870ff4f8f057ce76 0 0

=== [2/2] Running Live E2E Test for provider=nydus-dragonfly ===
[nydus-dragonfly] can_provide(us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6) -> True
[nydus-dragonfly] Cold pull_image completed in 387.2 ms -> /tmp/ray-images-nydus-dragonfly-aflu3aum/images/us-central1-docker.pkg.dev_kuiyue-gke-dev_kuiyue-gke-dev-repo_ate-images_sandbox-04176181c57f0bf23e61506b0ddcf1fd_nydus-v6
[nydus-dragonfly] Warm pull_image completed in 3.70 ms
[nydus-dragonfly] runsc exit_code=0
[nydus-dragonfly] Verified lease release and rootfs_dir cleanup!
```

### 6.3 Live Non-RAFS `FAILED_PRECONDITION` Fallback Verification

When requesting a standard OCI image (`:benchmark-9layer`, without Nydus RAFS bootstrap layers) against `ocilayer-nydus-dragonfly`, `PrepareImage` inspects the remote manifest, sees no `io.containerd.nydus.v2.bootstrap` layer, and returns `codes.FailedPrecondition` (`ErrNotSupported`), causing [`ImageManager.pull_image`](../image_manager.py) to fall back cleanly to `pull_and_extract_container_image`:

```text
[nydus-dragonfly] Live FAILED_PRECONDITION fallback for non-RAFS standard OCI image verified! -> /tmp/tmprod814x9/fallback_extracted
```

---

## 7. Bugs Caught & Fixed During Live GKE Testing

### Bug 1: gVisor `RootfsHint` Annotations for Shared Read-Only Streamed Mounts
- **Symptom on Live Cluster**:
  1. When only `dev.gvisor.spec.rootfs.overlay` was set without `source` and `type`, `runsc` (`release-20261012.0`) failed with:
     `error creating rootfs hint: rootfs annotations missing required field(s)`
  2. When `dev.gvisor.spec.rootfs.source = rootfs`, `dev.gvisor.spec.rootfs.type = "bind"`, and `dev.gvisor.spec.rootfs.overlay = "self"` were set, `runsc` failed with:
     `failed to create filestore file ".gvisor.filestore.ray-e2e-riptide-1792209495" inside "/var/lib/ateom-gvisor/streaming/.../rootfs": read-only file system`
- **Root Cause**:
  In `runsc/boot/rootfs.go` (`NewRootfsHint`), if any `dev.gvisor.spec.rootfs.*` annotation is present, both `source` and `type` (`"bind"` or `"erofs"`) are required. Furthermore, `overlay="self"` directs `createGoferFilestoreInSelf` to create `.gvisor.filestore.<cid>` directly inside the lower rootfs directory (`spec["root"]["path"]`). Because streamed `rootfs_dir` mounts are read-only kernel mounts (`overlay ro` for Riptide, `erofs ro` for Nydus-Dragonfly), `"self"` fails with `EROFS`.
- **Fix in [`image_manager.py`](../image_manager.py)**:
  When `streamed_meta is not None`, `create_oci_spec` sets:
  ```python
  annotations["dev.gvisor.spec.rootfs.source"] = rootfs
  annotations["dev.gvisor.spec.rootfs.type"] = "bind"
  overlay_medium = "memory"
  ```
  (and when `not readonly`, sets `annotations["dev.gvisor.spec.rootfs.overlay"] = "memory"` and honors `annotations["dev.gvisor.spec.rootfs.options"] = f"size={disk_bytes}"`).

### Bug 2: `unix.Unmount` Following Symlinks on Single-Layer `rootfs_dir` Teardown
- **Symptom on Live Cluster**:
  On `nydus-dragonfly` (`len(lowerDirs) == 1`), `<imageWorkDir>/rootfs` is a symlink to `layer-0/fs`, which symlinks to `/var/lib/ateom-gvisor/containerd-nydus/snapshots/<id>/mnt`. During `ReleaseImage(bypass_ttl=True)`, calling `unix.Unmount(rootfsDir, unix.MNT_DETACH)` without `UMOUNT_NOFOLLOW` caused the Linux kernel to follow the symlink chain and detach `containerd-nydus-grpc`'s own committed EROFS mount!
- **Fix in [`driver.go`](internal/ocilayer/adapter/driver.go)**:
  Updated `unmountRootfsDir` to check `os.Lstat(rootfsDir)` first—removing the symlink and returning immediately if `fi.Mode()&os.ModeSymlink != 0`—and to pass `unix.MNT_DETACH | unix.UMOUNT_NOFOLLOW` to `unix.Unmount`:
  ```go
  func unmountRootfsDir(rootfsDir string) error {
      fi, err := os.Lstat(rootfsDir)
      if err != nil {
          if os.IsNotExist(err) {
              return nil
          }
          return err
      }
      if fi.Mode()&os.ModeSymlink != 0 {
          return os.Remove(rootfsDir)
      }
      if err := unix.Unmount(rootfsDir, unix.MNT_DETACH|unix.UMOUNT_NOFOLLOW); err != nil {
          if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
              return nil
          }
          return fmt.Errorf("unmount rootfs %s: %w", rootfsDir, err)
      }
      return nil
  }
  ```

### Bug 3: `mountPropagation: Bidirectional` in Riptide DaemonSet Manifest
- **Fix in [`ocilayer-riptide.yaml`](cmd/ocilayer-riptide/ocilayer-riptide.yaml)**:
  Added `mountPropagation: Bidirectional` on the `run-ateom` (`/var/lib/ateom-gvisor`) volume mount of the `ocilayer-riptide` container so kernel `overlayfs` mounts created at `<imageWorkDir>/rootfs` propagate into the host and Ray worker mount namespaces.

---

## 8. Same-Node Apples-to-Apples Benchmark (`crane + mkfs.erofs` vs. `ocilayer-riptide` vs. `ocilayer-nydus-dragonfly`)

### 8.1 Benchmark Environment & Workload
To eliminate node-to-node hardware, kernel, and network variance, all three modes were benchmarked back-to-back on the **exact same GKE worker node**:

- **GKE Node**: `gke-substrate-stream-c3-std22-ubuntu--5a335c0d-5h2p` (`c3-standard-22`, 22 vCPUs, 88 GiB RAM, NVMe local SSD, `us-central1-a`)
- **Kernel & Runtime**: `Linux 6.8.0-1062-gke x86_64 (glibc 2.39)`, `runsc release-20261012.0`, `mkfs.erofs 1.7.1`, `nydusd 2.3.0`, `containerd-nydus-grpc 0.15.0`, `dfget daemon (Dragonfly v2)`
- **Target Image (`2.87 GiB` uncompressed, 9 layers)**:
  - Standard OCI (for Baseline & Riptide):
    `us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer`
  - Nydus RAFS v6 conversion of the same image (for Nydus + Dragonfly v2):
    `us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6`
- **Container Workload Executed via `runsc run`**:
  ```sh
  /bin/sh -ec "echo BENCH_OK mode=<provider>; uname -r; python3 --version; ls -lh /ko-app/sandbox"
  ```

### 8.2 Summary Comparison Table

| Metric | Baseline Ray (`pull_and_extract_container_image` + `mkfs.erofs`) | Streamed Riptide (`ocilayer-riptide` + `gcfsd-v2`) | Streamed Nydus + Dragonfly v2 (`ocilayer-nydus-dragonfly` + Kernel EROFS/fscache) |
| :--- | ---: | ---: | ---: |
| **Cold `pull_image` (Cold Snapshotter)** | `53,910.0 ms` | **`1,091.5 ms`** (**49.4x faster**) | **`939.3 ms`** (**57.4x faster**) |
| **Cold Adapter / Warm Snapshotter `pull_image`** | N/A (`53,910.0 ms` per image dir) | **`16.7 ms`** (**3,228x faster**) | **`6.8 ms`** (**7,928x faster**) |
| **Warm Lease `pull_image` (Adapter Cache Hit)** | `0.31 ms` | `3.94 ms` | `4.13 ms` |
| **`prepare_oci_bundle`** | `8.1 ms` | `9.4 ms` | `10.0 ms` |
| **Cold `runsc run` (First Container Execution)** | `46.4 ms` (local EROFS) | `345.5 ms` (on-demand FUSE) | `5,560.7 ms` (cold P2P chunk fetch) |
| **Total Cold Sandbox Start (`pull` + `bundle` + `runsc`)** | `53,964.5 ms` (`54.0 s`) | **`1,446.5 ms` (`1.45 s`, 37.3x faster)** | **`6,509.9 ms` (`6.51 s`, 8.3x faster)** |
| **Ray Worker Local Disk Footprint (`images_dir`)** | `2,872.4 MiB` (`rootfs.erofs`) | **`0.016 MiB` (`16 KiB` metadata)** | **`0.016 MiB` (`16 KiB` metadata)** |

### 8.3 Adapter `PrepareImage` Server-Side Breakdown

| Phase | Riptide Cold Snapshotter | Riptide Warm Snapshotter | Nydus-Dragonfly Cold Snapshotter | Nydus-Dragonfly Warm Snapshotter |
| :--- | ---: | ---: | ---: | ---: |
| `resolve_ms` (GAR Manifest + Config) | `299.4 ms` | `0.0 ms` (cached) | `303.5 ms` | `0.0 ms` (cached) |
| `stat_ms` (`Snapshots.Stat`) | `6.2 ms` | `1.5 ms` | `2.9 ms` | `1.3 ms` |
| `prepare_ms` (`Snapshots.Prepare` + Commit) | `744.5 ms` (9 layers) | `0.0 ms` (committed) | `22.9 ms` (RAFS v6 bootstrap) | `0.0 ms` (committed) |
| `view_ms` (`Snapshots.View`) | `10.6 ms` | `9.2 ms` | `32.1 ms` | `1.1 ms` |
| `listable_ms` (Mount Readiness Poll) | `1.2 ms` | `0.9 ms` | `0.03 ms` | `0.02 ms` |
| `wrapper_ms` (Symlink + `rootfs_dir` Mount) | `2.1 ms` (`overlayfs`) | `1.5 ms` (`overlayfs`) | `0.22 ms` (`symlink`) | `0.20 ms` (`symlink`) |
| **Server-Side `total_ms`** | **`1,065.5 ms`** | **`13.6 ms`** | **`928.9 ms`** | **`3.2 ms`** |

### 8.4 Exact Commands to Run the Same-Node Benchmark ([`benchmarks/run_same_node_benchmark.py`](benchmarks/run_same_node_benchmark.py))

```bash
# 1. Copy the compiled Go adapters, Ray sandbox package, and benchmark script to the target node
kubectl cp /tmp/ocilayer-riptide ate-system/bench-helper-5h2p:/host/var/lib/ateom-gvisor/ray-bench/bin/ocilayer-riptide
kubectl cp /tmp/ocilayer-nydus-dragonfly ate-system/bench-helper-5h2p:/host/var/lib/ateom-gvisor/ray-bench/bin/ocilayer-nydus-dragonfly
kubectl cp python/ray/experimental/sandbox/ocilayer/benchmarks/run_same_node_benchmark.py ate-system/bench-helper-5h2p:/host/var/lib/ateom-gvisor/ray-bench/run_same_node_benchmark.py

# 2. Execute the benchmark in the node root namespaces on gke-substrate-stream-c3-std22-ubuntu--5a335c0d-5h2p
kubectl exec -n ate-system bench-helper-5h2p -- nsenter -t 1 -m -u -i -n -p -- \
  python3 /var/lib/ateom-gvisor/ray-bench/run_same_node_benchmark.py
```

### 8.5 Verbatim Benchmark Logs & Outputs

#### Mode 1 & Mode 2 Verbatim Output ([`benchmarks/logs/same_node_benchmark_crane_and_riptide.log`](benchmarks/logs/same_node_benchmark_crane_and_riptide.log))
```text
=== Same-Node Ray Sandbox Image Streaming Benchmark ===
Node: gke-substrate-stream-c3-std22-ubuntu--5a335c0d-5h2p (Linux-6.8.0-1062-gke-x86_64-with-glibc2.39)

======================================================================
[1/3] Benchmarking Baseline Ray ImageManager (crane pull + tar extract)
======================================================================
[crane] Cold pull_image: 53910.0 ms (extracted disk=2872.4 MiB)
[crane] Warm pull_image: 0.31 ms
[crane] OCI bundle=8.1 ms, runsc run=46.4 ms, Total Cold E2E=53964.5 ms
[crane] Container output:
BENCH_OK mode=crane
4.19.0-gvisor
Python 3.10.12
-r-xr-xr-x 1 root root 9.4M Jan  1  1970 /ko-app/sandbox

======================================================================
[2/3] Benchmarking Streamed Riptide (ocilayer-riptide + gcfsd-v2)
======================================================================
[riptide] Cold pull_image (cold snapshotter): 1091.5 ms (local disk=0.016 MiB)
[riptide] OCI bundle=9.4 ms, runsc run=345.5 ms, Total Cold E2E=1446.5 ms
[riptide] Container output:
BENCH_OK mode=riptide
4.19.0-gvisor
Python 3.10.12
-r-xr-xr-x 1 root root 9.4M Jan  1  1970 /ko-app/sandbox
[riptide] Warm lease pull_image (adapter cache hit): 3.94 ms
[riptide] Cold adapter / warm snapshotter pull_image: 16.7 ms
--- [riptide] Adapter Logs ---
2026/10/17 16:51:44 INFO OCILayerService adapter listening provider=riptide listenSocket=/run/ate-image-streaming/ocilayer-bench-riptide.sock snapshotterSocket=/run/ate-image-streaming/gcfs-snapshotter.sock
2026/10/17 16:51:45 INFO Pushed registry credentials to gcfsd KeychainServiceSocket=/run/ate-image-streaming/gcfsd/keychain.sock image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer
2026/10/17 16:51:46 INFO PrepareImage timing breakdown provider=riptide image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer digest=sha256:dfca0ecfbffdfc283118b35aa7e8c28a5fcba0c4fd7a8858b70c79bc6202e88a layers=9 resolve_ms=299.423 stat_ms=6.167 prepare_ms=744.527 view_ms=10.625 listable_ms=1.168 wrapper_ms=2.064 total_ms=1065.46
2026/10/17 16:51:46 INFO Pushed registry credentials to gcfsd KeychainServiceSocket=/run/ate-image-streaming/gcfsd/keychain.sock image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer
2026/10/17 16:51:46 INFO PrepareImage timing breakdown provider=riptide image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer digest=sha256:dfca0ecfbffdfc283118b35aa7e8c28a5fcba0c4fd7a8858b70c79bc6202e88a layers=9 resolve_ms=0 stat_ms=1.471 prepare_ms=0 view_ms=9.211 listable_ms=0.943 wrapper_ms=1.542 total_ms=13.599
2026/10/17 16:51:46 INFO Shutting down OCILayerService server socket=/run/ate-image-streaming/ocilayer-bench-riptide.sock
```

#### Mode 3 Verbatim Output ([`benchmarks/logs/same_node_benchmark_nydus_dragonfly.log`](benchmarks/logs/same_node_benchmark_nydus_dragonfly.log))
```text
======================================================================
[3/3] Benchmarking Streamed Nydus + Dragonfly v2 (Kernel EROFS + fscache)
======================================================================
[nydus-dragonfly] Cold pull_image: 939.3 ms (local disk=0.016 MiB)
[nydus-dragonfly] OCI bundle=10.0 ms, runsc run=5560.7 ms, Total Cold E2E=6509.9 ms
[nydus-dragonfly] Container output:
BENCH_OK mode=nydus-dragonfly
4.19.0-gvisor
Python 3.10.12
-r-xr-xr-x 1 root root 9.4M Jan  1  1970 /ko-app/sandbox
[nydus-dragonfly] Warm lease pull_image (adapter cache hit): 4.13 ms
[nydus-dragonfly] Cold adapter / warm snapshotter pull_image: 6.8 ms
--- [nydus-dragonfly] Adapter Logs ---
2026/10/17 21:14:04 INFO Initialized cross-node Nydus + Dragonfly v2 (Kernel EROFS + fscache) OCILayerService driver dragonflyProxyAddr=http://127.0.0.1:65001 dragonflySchedulerAddr=10.96.1.32:8002 dragonflyManagerAddr=10.96.1.31:65003 verifyDragonflyPing=true procFilesystems=/proc/filesystems cachefilesDev=/dev/cachefiles
2026/10/17 21:14:04 INFO OCILayerService adapter listening provider=nydus-dragonfly listenSocket=/run/ate-image-streaming/ocilayer-bench-nydus.sock snapshotterSocket=/run/ate-image-streaming/nydus-bench-snapshotter.sock
2026/10/17 21:14:04 INFO Synchronized registry credentials to docker config path=/run/containerd-nydus/docker/config.json image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6 server=us-central1-docker.pkg.dev username=_token
2026/10/17 21:14:05 INFO Materialized and committed Nydus RAFS v6 EROFS bootstrap layer layerIndex=8 layerDigest=sha256:ba487201a54373ecc7dd03cbd402f32d1b887bc39c30992a58128e9b7bf35554 chainID=sha256:a5b54ba5e3b69630ae40d93a14d683bcb1939d0b74176a4e2f108ffe9601d5e8 targetDir=/var/lib/ateom-gvisor/nydus-bench/snapshots/9/fs dragonflyProxy=http://127.0.0.1:65001 dragonflyScheduler=10.96.1.32:8002 dragonflyManager=10.96.1.31:65003
2026/10/17 21:14:05 INFO PrepareImage timing breakdown provider=nydus-dragonfly image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6 digest=sha256:63efd7bfec93de795766e03c46b21496df0679548e46a37eb6291b3f5a9fff7c layers=1 resolve_ms=303.488 stat_ms=2.863 prepare_ms=22.868 view_ms=32.087 listable_ms=0.033 wrapper_ms=0.22 total_ms=928.926
2026/10/17 21:14:11 INFO Synchronized registry credentials to docker config path=/run/containerd-nydus/docker/config.json image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6 server=us-central1-docker.pkg.dev username=_token
2026/10/17 21:14:11 INFO PrepareImage timing breakdown provider=nydus-dragonfly image=us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6 digest=sha256:63efd7bfec93de795766e03c46b21496df0679548e46a37eb6291b3f5a9fff7c layers=1 resolve_ms=0 stat_ms=1.295 prepare_ms=0 view_ms=1.064 listable_ms=0.023 wrapper_ms=0.199 total_ms=3.21
2026/10/17 21:14:11 INFO Shutting down OCILayerService server socket=/run/ate-image-streaming/ocilayer-bench-nydus.sock
```

#### Combined Structured Benchmark Summary JSON ([`benchmarks/logs/same_node_benchmark_summary.json`](benchmarks/logs/same_node_benchmark_summary.json))
```json
[
  {
    "provider": "baseline-crane",
    "image": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer",
    "cold_pull_ms": 53910.0,
    "cold_adapter_warm_snap_ms": null,
    "warm_pull_ms": 0.31,
    "bundle_ms": 8.1,
    "runsc_ms": 46.4,
    "total_cold_e2e_ms": 53964.5,
    "disk_mb": 2872.4
  },
  {
    "provider": "riptide",
    "image": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer",
    "cold_pull_ms": 1091.5,
    "cold_adapter_warm_snap_ms": 16.7,
    "warm_pull_ms": 3.94,
    "bundle_ms": 9.4,
    "runsc_ms": 345.5,
    "total_cold_e2e_ms": 1446.5,
    "disk_mb": 0.016
  },
  {
    "provider": "nydus-dragonfly",
    "image": "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6",
    "cold_pull_ms": 939.3,
    "cold_adapter_warm_snap_ms": 6.8,
    "warm_pull_ms": 4.13,
    "bundle_ms": 10.0,
    "runsc_ms": 5560.7,
    "total_cold_e2e_ms": 6509.9,
    "disk_mb": 0.016
  }
]
```
