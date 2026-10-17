import argparse
from concurrent import futures
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import types
from unittest.mock import patch

import grpc

try:
    import pytest  # noqa: F401
except ImportError:
    sys.modules["pytest"] = types.SimpleNamespace(
        fixture=lambda *a, **kw: (a[0] if a and callable(a[0]) else (lambda f: f))
    )

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

from ray.experimental.sandbox._internal import (
    oci_layer_client,
    ocilayer_pb2,
    ocilayer_pb2_grpc,
)
from ray.experimental.sandbox.image_manager import STREAMED_MARKER, ImageManager
from ray.experimental.sandbox.tests.test_image_manager_streaming import (
    FakeOCILayerServicer,
    _minimal_base_spec,
)


def run_inproc_unit_tests():
    print("=== [1/2] Running in-process gRPC unit test suite (test_image_manager_streaming) ===")
    with tempfile.TemporaryDirectory() as tmp:
        sock_path = os.path.join(tmp, "ocilayer-unit.sock")
        servicer = FakeOCILayerServicer(os.path.join(tmp, "streamed_mounts"))
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        ocilayer_pb2_grpc.add_OCILayerServiceServicer_to_server(servicer, server)
        server.add_insecure_port(f"unix://{sock_path}")
        server.start()
        old_sock = os.environ.get("RAY_SANDBOX_OCI_LAYER_SOCKET")
        os.environ["RAY_SANDBOX_OCI_LAYER_SOCKET"] = sock_path
        try:
            images_dir = os.path.join(tmp, "images")
            mgr = ImageManager(images_dir=images_dir)
            image_ref = "my-registry.example.com/team/agent:v1"

            image_dir = mgr.pull_image(image_ref, instance_id="sbx-1")
            assert mgr.is_image_extracted(image_ref)
            assert mgr.get_workdir(image_ref) == "/workspace/agent"
            assert mgr.get_envs(image_ref) == [
                "PATH=/usr/local/bin:/usr/bin",
                "STREAMED_VAR=1",
            ]
            spec = mgr.create_oci_spec(
                image=image_ref,
                container_cwd="/workspace/agent",
                readonly=False,
                disk="512Mi",
                base_spec=_minimal_base_spec(),
            )
            assert spec["root"]["path"] == servicer.get_rootfs_dir(image_ref)
            assert spec["annotations"]["dev.gvisor.spec.rootfs.source"] == servicer.get_rootfs_dir(image_ref)
            assert spec["annotations"]["dev.gvisor.spec.rootfs.type"] == "bind"
            assert spec["annotations"]["dev.gvisor.spec.rootfs.overlay"] == "memory"
            assert servicer.ref_counts[image_ref] == 1

            mgr.pull_image(image_ref, instance_id="sbx-2")
            assert servicer.ref_counts[image_ref] == 2
            mgr.release_image(image_ref, instance_id="sbx-1")
            assert servicer.ref_counts[image_ref] == 1
            mgr.release_image(image_ref, instance_id="sbx-2")
            assert servicer.ref_counts[image_ref] == 0

            # Pre-pull claim test
            image_ref2 = "my-registry.example.com/team/agent:v2"
            mgr.pull_image(image_ref2)
            assert servicer.ref_counts[image_ref2] == 1
            mgr.pull_image(image_ref2, instance_id="sbx-claim")
            assert servicer.ref_counts[image_ref2] == 1
            mgr.release_image(image_ref2, instance_id="sbx-claim")
            assert servicer.ref_counts[image_ref2] == 0

            # Fallback on FAILED_PRECONDITION
            decline_ref = "docker.io/library/alpine:latest"
            servicer.fail_precondition_images.add(decline_ref)
            with patch(
                "ray.experimental.sandbox.image_manager.pull_and_extract_container_image",
                return_value=os.path.join(images_dir, "alpine_latest"),
            ) as mock_fb:
                res = mgr.pull_image(decline_ref, instance_id="sbx-fb")
                assert res == os.path.join(images_dir, "alpine_latest")
                mock_fb.assert_called_once()
        finally:
            if old_sock is None:
                os.environ.pop("RAY_SANDBOX_OCI_LAYER_SOCKET", None)
            else:
                os.environ["RAY_SANDBOX_OCI_LAYER_SOCKET"] = old_sock
            server.stop(grace=0)
    print("PASS: All in-process gRPC unit tests succeeded!")


def find_mount_info(target_path: str) -> str:
    real_target = os.path.realpath(target_path)
    best_line = ""
    best_len = -1
    with open("/proc/mounts", "r", encoding="utf-8") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 3:
                mnt_point = parts[1]
                if (
                    target_path == mnt_point
                    or real_target == mnt_point
                    or real_target.startswith(mnt_point.rstrip("/") + "/")
                ):
                    if len(mnt_point) > best_len:
                        best_len = len(mnt_point)
                        best_line = line.strip()
    return best_line


def run_live_e2e(provider: str, adapter_bin: str, image_ref: str):
    print(f"\n=== [2/2] Running Live E2E Test for provider={provider} ===")
    sock_path = f"/run/ate-image-streaming/ocilayer-ray-e2e-{provider}.sock"
    work_dir = f"/var/lib/ateom-gvisor/streaming/ray-e2e-{provider}"
    if os.path.exists(sock_path):
        os.remove(sock_path)
    shutil.rmtree(work_dir, ignore_errors=True)

    env = os.environ.copy()
    env["PATH"] = f"/home/kubernetes/bin:{env.get('PATH', '')}"
    if provider == "riptide":
        cmd = [
            adapter_bin,
            f"--listen-socket={sock_path}",
            "--snapshotter-socket=/run/ate-image-streaming/gcfs-snapshotter.sock",
            "--keychain-socket=/run/ate-image-streaming/gcfsd/keychain.sock",
            f"--work-dir={work_dir}",
            "--idle-ttl=10m",
        ]
    elif provider == "nydus-dragonfly":
        env["DRAGONFLY_PROXY_ADDR"] = "http://127.0.0.1:65001"
        env["DRAGONFLY_SCHEDULER_ADDR"] = "10.96.1.32:8002"
        env["DRAGONFLY_MANAGER_ADDR"] = "10.96.1.31:65003"
        cmd = [
            adapter_bin,
            f"--listen-socket={sock_path}",
            "--snapshotter-socket=/run/containerd-nydus/containerd-nydus-grpc.sock",
            "--docker-config=/run/containerd-nydus/docker/config.json",
            f"--work-dir={work_dir}",
            "--verify-dragonfly-ping=true",
            "--idle-ttl=10m",
        ]
    else:
        raise ValueError(f"Unknown provider: {provider}")

    adapter_proc = subprocess.Popen(
        cmd,
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
        assert os.path.exists(sock_path), f"Adapter socket {sock_path} did not appear"

        os.environ["RAY_SANDBOX_OCI_LAYER_SOCKET"] = sock_path
        with tempfile.TemporaryDirectory(prefix=f"ray-images-{provider}-") as tmp_dir:
            images_dir = os.path.join(tmp_dir, "images")
            mgr = ImageManager(images_dir=images_dir)

            can_prov = oci_layer_client.can_provide(image_ref)
            print(f"[{provider}] can_provide({image_ref}) -> {can_prov}")
            assert can_prov is True, f"Expected can_provide=True for {provider}"

            t0 = time.monotonic()
            image_dir = mgr.pull_image(image_ref, instance_id="ray-e2e-sbx-1")
            cold_ms = (time.monotonic() - t0) * 1000.0
            print(f"[{provider}] Cold pull_image completed in {cold_ms:.1f} ms -> {image_dir}")

            assert mgr.is_image_extracted(image_ref)
            with open(os.path.join(image_dir, STREAMED_MARKER), "r", encoding="utf-8") as f:
                streamed_meta = json.load(f)
            print(f"[{provider}] .streamed.json: {json.dumps(streamed_meta, indent=2)}")

            assert streamed_meta["provider"] == provider
            rootfs_dir = streamed_meta["rootfs_dir"]
            assert os.path.isdir(rootfs_dir), f"rootfs_dir {rootfs_dir} is not a directory"

            top_entries = sorted(os.listdir(rootfs_dir))
            print(f"[{provider}] rootfs_dir top-level entries ({len(top_entries)}): {top_entries[:15]}")
            mnt_info = find_mount_info(rootfs_dir)
            print(f"[{provider}] Backing mount for rootfs_dir: {mnt_info}")

            if provider == "riptide":
                assert not os.path.islink(rootfs_dir), "Multi-layer Riptide rootfs_dir should be an overlay mountpoint"
                assert "overlay" in mnt_info, f"Expected kernel overlayfs mount for Riptide rootfs_dir, got: {mnt_info}"
            elif provider == "nydus-dragonfly":
                assert os.path.islink(rootfs_dir), "Single-layer Nydus-Dragonfly rootfs_dir should be a symlink to layer-0/fs"
                assert "erofs" in mnt_info, f"Expected kernel erofs mount backing Nydus-Dragonfly rootfs_dir, got: {mnt_info}"

            # Check Warm Hit latency
            t1 = time.monotonic()
            mgr.pull_image(image_ref, instance_id="ray-e2e-sbx-2")
            warm_ms = (time.monotonic() - t1) * 1000.0
            print(f"[{provider}] Warm pull_image completed in {warm_ms:.2f} ms")

            # Prepare OCI bundle via ImageManager
            bundle_dir = os.path.join(tmp_dir, "bundle")
            workdir_path = os.path.join(tmp_dir, "workdir")
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

            print(
                f"[{provider}] OCI Spec root={spec['root']} "
                f"annotations={spec.get('annotations')} "
                f"cwd={spec['process']['cwd']} env_count={len(spec['process'].get('env', []))}"
            )
            assert spec["root"]["path"] == rootfs_dir
            assert spec["annotations"]["dev.gvisor.spec.rootfs.source"] == rootfs_dir
            assert spec["annotations"]["dev.gvisor.spec.rootfs.type"] == "bind"
            assert spec["annotations"]["dev.gvisor.spec.rootfs.overlay"] == "memory"
            assert spec["annotations"]["dev.gvisor.spec.rootfs.options"] == f"size={256 * 1024**2}"

            # Boot a real gVisor sandbox container using runsc with the streamed rootfs!
            if os.path.exists(os.path.join(rootfs_dir, "bin/sh")):
                spec["process"]["args"] = [
                    "/bin/sh",
                    "-ec",
                    "echo GVISOR_STREAMED_OK provider="
                    + provider
                    + "; uname -a; df -h /; ls -la /ko-app/sandbox; touch /overlay_write_test && ls -la /overlay_write_test",
                ]
            else:
                spec["process"]["args"] = ["/ko-app/sandbox", "--help"]
            with open(config_json_path, "w", encoding="utf-8") as f:
                json.dump(spec, f, indent=2)

            cid = f"ray-e2e-{provider}-{int(time.time())}"
            runsc_root = f"/tmp/runsc-ray-e2e-{provider}"
            os.makedirs(runsc_root, exist_ok=True)
            runsc_cmd = [
                "runsc",
                f"--root={runsc_root}",
                "--network=none",
                "--ignore-cgroups",
                "run",
                "--bundle",
                bundle_dir,
                cid,
            ]
            print(f"[{provider}] Executing gVisor container on streamed rootfs: {' '.join(runsc_cmd)}")
            try:
                proc = subprocess.run(runsc_cmd, capture_output=True, text=True, timeout=30)
                print(f"[{provider}] runsc exit_code={proc.returncode}")
                if proc.stdout.strip():
                    print(f"[{provider}] runsc stdout:\n{proc.stdout.strip()}")
                if proc.stderr.strip():
                    print(f"[{provider}] runsc stderr:\n{proc.stderr.strip()}")
                assert proc.returncode == 0, f"runsc failed with code {proc.returncode}: {proc.stderr}"
                assert not os.path.exists(os.path.join(rootfs_dir, "overlay_write_test")), (
                    "Write inside gVisor memory overlay must not mutate read-only streamed rootfs_dir"
                )
            finally:
                subprocess.run(
                    ["runsc", f"--root={runsc_root}", "delete", "-force", cid],
                    capture_output=True,
                    timeout=10,
                )

            # Release both sandbox instances and verify teardown with bypass_ttl
            mgr.release_image(image_ref, instance_id="ray-e2e-sbx-1")
            mgr.release_image(image_ref, instance_id="ray-e2e-sbx-2")
            oci_layer_client.release_image(image_ref, bypass_ttl=True)
            assert not os.path.exists(rootfs_dir), f"Expected {rootfs_dir} to be unmounted and removed after bypass_ttl release"
            print(f"[{provider}] Verified lease release and rootfs_dir cleanup!")

    finally:
        adapter_proc.terminate()
        try:
            out, _ = adapter_proc.communicate(timeout=5)
        except Exception:
            adapter_proc.kill()
            out, _ = adapter_proc.communicate()
        print(f"--- [{provider}] Adapter daemon logs ---")
        print(out.strip())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True, choices=["riptide", "nydus-dragonfly"])
    parser.add_argument("--adapter-bin", required=True)
    parser.add_argument("--image-ref", required=True)
    args = parser.parse_args()

    run_inproc_unit_tests()
    run_live_e2e(args.provider, args.adapter_bin, args.image_ref)
