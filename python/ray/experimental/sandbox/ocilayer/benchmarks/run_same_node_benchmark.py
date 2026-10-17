import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import types

# Pre-register namespace packages so ray.experimental.sandbox.image_manager can
# be imported without requiring compiled Cython extensions (ray._raylet / ray.actor).
for _sp in sys.path:
    _ray_dir = os.path.join(_sp, "ray")
    if os.path.isdir(os.path.join(_ray_dir, "experimental", "sandbox")):
        for _mod_name, _rel in (
            ("ray", ""),
            ("ray.util", "util"),
            ("ray.experimental", "experimental"),
            ("ray.experimental.sandbox", "experimental/sandbox"),
        ):
            _m = types.ModuleType(_mod_name)
            _m.__path__ = [os.path.join(_ray_dir, _rel)]
            sys.modules[_mod_name] = _m
        break

from ray.experimental.sandbox._internal import image_utils, oci_layer_client
from ray.experimental.sandbox.image_manager import ImageManager

_orig_get_auth = image_utils.get_registry_auth_headers


def _gke_get_registry_auth_headers(registry, repo, reference="latest", timeout=30.0):
    if registry.endswith("-docker.pkg.dev") or registry == "gcr.io":
        import urllib.request

        req = urllib.request.Request(
            "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"},
        )
        tok = json.loads(urllib.request.urlopen(req, timeout=5).read())["access_token"]
        return {"Authorization": f"Bearer {tok}"}
    return _orig_get_auth(registry, repo, reference=reference, timeout=timeout)


image_utils.get_registry_auth_headers = _gke_get_registry_auth_headers

OCI_IMAGE_REF = (
    "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/"
    "ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:benchmark-9layer"
)
NYDUS_IMAGE_REF = (
    "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/"
    "ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:nydus-v6"
)
NYDUS_SWEBENCH_REF = (
    "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/"
    "ate-images/sandbox-04176181c57f0bf23e61506b0ddcf1fd:swebench-nydus-v6"
)


def dir_size_mb(path: str) -> float:
    out = subprocess.check_output(["du", "-sk", path], text=True).split()[0]
    return int(out) / 1024.0


def run_gvisor_container(mgr: ImageManager, image_ref: str, label: str, tmp_dir: str):
    t0 = time.monotonic()
    bundle_dir = os.path.join(tmp_dir, f"bundle-{label}")
    workdir_path = os.path.join(tmp_dir, f"workdir-{label}")
    os.makedirs(bundle_dir, exist_ok=True)
    os.makedirs(workdir_path, exist_ok=True)

    config_json_path = mgr.prepare_oci_bundle(
        root_dir=bundle_dir,
        workdir_path=workdir_path,
        container_cwd=mgr.get_workdir(image_ref) or "/",
        image=image_ref,
        readonly=False,
        network="none",
        disk="256Mi",
    )
    with open(config_json_path, "r", encoding="utf-8") as f:
        spec = json.load(f)

    spec["process"]["args"] = [
        "/bin/sh",
        "-ec",
        f"echo BENCH_OK mode={label}; uname -r; python3 --version; ls -lh /ko-app/sandbox",
    ]
    with open(config_json_path, "w", encoding="utf-8") as f:
        json.dump(spec, f)

    t_bundle_ms = (time.monotonic() - t0) * 1000.0

    cid = f"ray-bench-{label}-{int(time.time() * 1000)}"
    runsc_root = f"/tmp/runsc-ray-bench-{label}"
    os.makedirs(runsc_root, exist_ok=True)
    t_run0 = time.monotonic()
    try:
        proc = subprocess.run(
            [
                "runsc",
                f"--root={runsc_root}",
                "--network=none",
                "--ignore-cgroups",
                "run",
                "--bundle",
                bundle_dir,
                cid,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        t_runsc_ms = (time.monotonic() - t_run0) * 1000.0
        assert proc.returncode == 0, f"runsc failed ({proc.returncode}): {proc.stderr}"
        return t_bundle_ms, t_runsc_ms, proc.stdout.strip()
    finally:
        subprocess.run(
            ["runsc", f"--root={runsc_root}", "delete", "-force", cid],
            capture_output=True,
            timeout=10,
        )
        subprocess.run(
            ["umount", "-l", os.path.join(runsc_root, "null-netns")],
            capture_output=True,
        )
        shutil.rmtree(runsc_root, ignore_errors=True)


def bench_crane() -> dict:
    print("\n======================================================================")
    print("[1/3] Benchmarking Baseline Ray ImageManager (crane pull + tar extract)")
    print("======================================================================")
    os.environ.pop("RAY_SANDBOX_OCI_LAYER_SOCKET", None)
    with tempfile.TemporaryDirectory(prefix="ray-bench-crane-") as tmp_dir:
        images_dir = os.path.join(tmp_dir, "images")
        mgr = ImageManager(images_dir=images_dir)

        t0 = time.monotonic()
        image_dir = mgr.pull_image(OCI_IMAGE_REF, instance_id="crane-sbx-1", timeout_seconds=300)
        cold_pull_ms = (time.monotonic() - t0) * 1000.0
        disk_mb = dir_size_mb(image_dir)
        print(f"[crane] Cold pull_image: {cold_pull_ms:.1f} ms (extracted disk={disk_mb:.1f} MiB)")

        t1 = time.monotonic()
        mgr.pull_image(OCI_IMAGE_REF, instance_id="crane-sbx-2", timeout_seconds=300)
        warm_pull_ms = (time.monotonic() - t1) * 1000.0
        print(f"[crane] Warm pull_image: {warm_pull_ms:.2f} ms")

        bundle_ms, runsc_ms, stdout = run_gvisor_container(mgr, OCI_IMAGE_REF, "crane", tmp_dir)
        total_cold_e2e_ms = cold_pull_ms + bundle_ms + runsc_ms
        print(
            f"[crane] OCI bundle={bundle_ms:.1f} ms, runsc run={runsc_ms:.1f} ms, "
            f"Total Cold E2E={total_cold_e2e_ms:.1f} ms"
        )
        print(f"[crane] Container output:\n{stdout}")
        mgr.release_image(OCI_IMAGE_REF, instance_id="crane-sbx-1")
        mgr.release_image(OCI_IMAGE_REF, instance_id="crane-sbx-2")
        return {
            "provider": "baseline-crane",
            "image": OCI_IMAGE_REF,
            "cold_pull_ms": round(cold_pull_ms, 1),
            "cold_adapter_warm_snap_ms": None,
            "warm_pull_ms": round(warm_pull_ms, 2),
            "bundle_ms": round(bundle_ms, 1),
            "runsc_ms": round(runsc_ms, 1),
            "total_cold_e2e_ms": round(total_cold_e2e_ms, 1),
            "disk_mb": round(disk_mb, 2),
        }


def bench_riptide(adapter_bin: str) -> dict:
    print("\n======================================================================")
    print("[2/3] Benchmarking Streamed Riptide (ocilayer-riptide + gcfsd-v2)")
    print("======================================================================")
    sock_path = "/run/ate-image-streaming/ocilayer-bench-riptide.sock"
    work_dir = "/var/lib/ateom-gvisor/streaming/bench-riptide"
    if os.path.exists(sock_path):
        os.remove(sock_path)
    shutil.rmtree(work_dir, ignore_errors=True)

    proc = subprocess.Popen(
        [
            adapter_bin,
            f"--listen-socket={sock_path}",
            "--snapshotter-socket=/run/ate-image-streaming/gcfs-snapshotter.sock",
            "--keychain-socket=/run/ate-image-streaming/gcfsd/keychain.sock",
            f"--work-dir={work_dir}",
            "--idle-ttl=10m",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        for _ in range(50):
            if os.path.exists(sock_path):
                break
            time.sleep(0.1)
        assert os.path.exists(sock_path), "Riptide adapter socket did not appear"
        os.environ["RAY_SANDBOX_OCI_LAYER_SOCKET"] = sock_path

        with tempfile.TemporaryDirectory(prefix="ray-bench-riptide-") as tmp_dir:
            images_dir = os.path.join(tmp_dir, "images")
            mgr = ImageManager(images_dir=images_dir)

            # 1. Cold pull (first pull on clean gcfsd-v2 + adapter)
            t0 = time.monotonic()
            image_dir = mgr.pull_image(OCI_IMAGE_REF, instance_id="riptide-sbx-1")
            cold_pull_ms = (time.monotonic() - t0) * 1000.0
            disk_mb = dir_size_mb(image_dir)
            print(f"[riptide] Cold pull_image (cold snapshotter): {cold_pull_ms:.1f} ms (local disk={disk_mb:.3f} MiB)")

            # 2. Bundle + runsc execution on cold streamed rootfs
            bundle_ms, runsc_ms, stdout = run_gvisor_container(mgr, OCI_IMAGE_REF, "riptide", tmp_dir)
            total_cold_e2e_ms = cold_pull_ms + bundle_ms + runsc_ms
            print(
                f"[riptide] OCI bundle={bundle_ms:.1f} ms, runsc run={runsc_ms:.1f} ms, "
                f"Total Cold E2E={total_cold_e2e_ms:.1f} ms"
            )
            print(f"[riptide] Container output:\n{stdout}")

            # 3. Warm lease pull (new ImageManager instance hitting warm adapter lease)
            mgr_warm = ImageManager(images_dir=os.path.join(tmp_dir, "images-warm"))
            t1 = time.monotonic()
            mgr_warm.pull_image(OCI_IMAGE_REF, instance_id="riptide-sbx-2")
            warm_pull_ms = (time.monotonic() - t1) * 1000.0
            print(f"[riptide] Warm lease pull_image (adapter cache hit): {warm_pull_ms:.2f} ms")

            # Release all and bypass TTL so adapter tears down overlayfs, then re-pull over warm snapshotter
            mgr.release_image(OCI_IMAGE_REF, instance_id="riptide-sbx-1")
            mgr_warm.release_image(OCI_IMAGE_REF, instance_id="riptide-sbx-2")
            oci_layer_client.release_image(OCI_IMAGE_REF, bypass_ttl=True)

            mgr_re = ImageManager(images_dir=os.path.join(tmp_dir, "images-re"))
            t2 = time.monotonic()
            mgr_re.pull_image(OCI_IMAGE_REF, instance_id="riptide-sbx-3")
            warm_snap_ms = (time.monotonic() - t2) * 1000.0
            print(f"[riptide] Cold adapter / warm snapshotter pull_image: {warm_snap_ms:.1f} ms")
            mgr_re.release_image(OCI_IMAGE_REF, instance_id="riptide-sbx-3")
            oci_layer_client.release_image(OCI_IMAGE_REF, bypass_ttl=True)

            return {
                "provider": "riptide",
                "image": OCI_IMAGE_REF,
                "cold_pull_ms": round(cold_pull_ms, 1),
                "cold_adapter_warm_snap_ms": round(warm_snap_ms, 1),
                "warm_pull_ms": round(warm_pull_ms, 2),
                "bundle_ms": round(bundle_ms, 1),
                "runsc_ms": round(runsc_ms, 1),
                "total_cold_e2e_ms": round(total_cold_e2e_ms, 1),
                "disk_mb": round(disk_mb, 3),
            }
    finally:
        proc.terminate()
        out, _ = proc.communicate(timeout=5)
        print("--- [riptide] Adapter Logs ---")
        print(out.strip())


def bench_nydus_dragonfly(
    adapter_bin: str,
    snapshotter_sock: str = "/run/containerd-nydus/containerd-nydus-grpc.sock",
) -> dict:
    print("\n======================================================================")
    print("[3/3] Benchmarking Streamed Nydus + Dragonfly v2 (Kernel EROFS + fscache)")
    print("======================================================================")
    sock_path = "/run/ate-image-streaming/ocilayer-bench-nydus.sock"
    work_dir = "/var/lib/ateom-gvisor/streaming/bench-nydus"
    if os.path.exists(sock_path):
        os.remove(sock_path)
    shutil.rmtree(work_dir, ignore_errors=True)

    env = os.environ.copy()
    env["DRAGONFLY_PROXY_ADDR"] = "http://127.0.0.1:65001"
    env["DRAGONFLY_SCHEDULER_ADDR"] = "10.96.1.32:8002"
    env["DRAGONFLY_MANAGER_ADDR"] = "10.96.1.31:65003"

    proc = subprocess.Popen(
        [
            adapter_bin,
            f"--listen-socket={sock_path}",
            f"--snapshotter-socket={snapshotter_sock}",
            "--docker-config=/run/containerd-nydus/docker/config.json",
            f"--work-dir={work_dir}",
            "--verify-dragonfly-ping=true",
            "--idle-ttl=10m",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        for _ in range(50):
            if os.path.exists(sock_path):
                break
            time.sleep(0.1)
        assert os.path.exists(sock_path), "Nydus-Dragonfly adapter socket did not appear"
        os.environ["RAY_SANDBOX_OCI_LAYER_SOCKET"] = sock_path

        with tempfile.TemporaryDirectory(prefix="ray-bench-nydus-") as tmp_dir:
            images_dir = os.path.join(tmp_dir, "images")
            mgr = ImageManager(images_dir=images_dir)

            # 1. Pull image (cold adapter / warm EROFS or cold bootstrap)
            t0 = time.monotonic()
            image_dir = mgr.pull_image(NYDUS_IMAGE_REF, instance_id="nydus-sbx-1")
            cold_pull_ms = (time.monotonic() - t0) * 1000.0
            disk_mb = dir_size_mb(image_dir)
            print(f"[nydus-dragonfly] Cold pull_image: {cold_pull_ms:.1f} ms (local disk={disk_mb:.3f} MiB)")

            # 2. Bundle + runsc execution on streamed EROFS rootfs
            bundle_ms, runsc_ms, stdout = run_gvisor_container(mgr, NYDUS_IMAGE_REF, "nydus-dragonfly", tmp_dir)
            total_cold_e2e_ms = cold_pull_ms + bundle_ms + runsc_ms
            print(
                f"[nydus-dragonfly] OCI bundle={bundle_ms:.1f} ms, runsc run={runsc_ms:.1f} ms, "
                f"Total Cold E2E={total_cold_e2e_ms:.1f} ms"
            )
            print(f"[nydus-dragonfly] Container output:\n{stdout}")

            # 3. Warm lease pull (new ImageManager instance hitting warm adapter lease)
            mgr_warm = ImageManager(images_dir=os.path.join(tmp_dir, "images-warm"))
            t1 = time.monotonic()
            mgr_warm.pull_image(NYDUS_IMAGE_REF, instance_id="nydus-sbx-2")
            warm_pull_ms = (time.monotonic() - t1) * 1000.0
            print(f"[nydus-dragonfly] Warm lease pull_image (adapter cache hit): {warm_pull_ms:.2f} ms")

            mgr.release_image(NYDUS_IMAGE_REF, instance_id="nydus-sbx-1")
            mgr_warm.release_image(NYDUS_IMAGE_REF, instance_id="nydus-sbx-2")
            oci_layer_client.release_image(NYDUS_IMAGE_REF, bypass_ttl=True)

            # 4. Second pull after bypass_ttl release
            mgr_re = ImageManager(images_dir=os.path.join(tmp_dir, "images-re"))
            t2 = time.monotonic()
            mgr_re.pull_image(NYDUS_IMAGE_REF, instance_id="nydus-sbx-3")
            warm_snap_ms = (time.monotonic() - t2) * 1000.0
            print(f"[nydus-dragonfly] Cold adapter / warm snapshotter pull_image: {warm_snap_ms:.1f} ms")
            mgr_re.release_image(NYDUS_IMAGE_REF, instance_id="nydus-sbx-3")
            oci_layer_client.release_image(NYDUS_IMAGE_REF, bypass_ttl=True)

            return {
                "provider": "nydus-dragonfly",
                "image": NYDUS_IMAGE_REF,
                "cold_pull_ms": round(cold_pull_ms, 1),
                "cold_adapter_warm_snap_ms": round(warm_snap_ms, 1),
                "warm_pull_ms": round(warm_pull_ms, 2),
                "bundle_ms": round(bundle_ms, 1),
                "runsc_ms": round(runsc_ms, 1),
                "total_cold_e2e_ms": round(total_cold_e2e_ms, 1),
                "disk_mb": round(disk_mb, 3),
            }
    finally:
        proc.terminate()
        out, _ = proc.communicate(timeout=5)
        print("--- [nydus-dragonfly] Adapter Logs ---")
        print(out.strip())


if __name__ == "__main__":
    print(f"=== Same-Node Ray Sandbox Image Streaming Benchmark ===")
    print(f"Node: {platform.node()} ({platform.platform()})")
    bin_dir = "/var/lib/ateom-gvisor/ray-bench/bin"
    results = []
    results.append(bench_crane())
    results.append(bench_riptide(os.path.join(bin_dir, "ocilayer-riptide")))
    results.append(bench_nydus_dragonfly(os.path.join(bin_dir, "ocilayer-nydus-dragonfly")))

    print("\n======================================================================")
    print("FINAL SAME-NODE BENCHMARK SUMMARY JSON:")
    print(json.dumps(results, indent=2))
