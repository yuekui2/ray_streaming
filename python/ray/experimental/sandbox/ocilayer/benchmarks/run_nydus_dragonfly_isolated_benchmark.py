import base64
import glob
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

# 1. Refresh /run/containerd-nydus/docker/config.json with fresh GKE metadata token
req = urllib.request.Request(
    "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
    headers={"Metadata-Flavor": "Google"},
)
tok = json.loads(urllib.request.urlopen(req, timeout=5).read())["access_token"]
auth = base64.b64encode(("oauth2accesstoken:" + tok).encode()).decode()
cfg = {
    "auths": {
        "us-central1-docker.pkg.dev": {"auth": auth},
        "https://us-central1-docker.pkg.dev": {"auth": auth},
    }
}
for p in ["/run/containerd-nydus/docker/config.json", "/root/.docker/config.json"]:
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        json.dump(cfg, f)
print("Refreshed docker config.json", flush=True)

# 2. Ensure clean isolated nydus root for benchmark
nydus_bench_root = "/var/lib/ateom-gvisor/nydus-bench"
nydus_bench_sock = "/run/ate-image-streaming/nydus-bench-snapshotter.sock"
for mnt in glob.glob(f"{nydus_bench_root}/snapshots/*/mnt"):
    subprocess.run(["umount", "-l", mnt], capture_output=True)
subprocess.run(["umount", "-l", nydus_bench_root], capture_output=True)
shutil.rmtree(nydus_bench_root, ignore_errors=True)
if os.path.exists(nydus_bench_sock):
    os.remove(nydus_bench_sock)

os.makedirs(f"{nydus_bench_root}/cache/metadata", exist_ok=True)
os.makedirs(f"{nydus_bench_root}/mnt", exist_ok=True)
subprocess.run(["mount", "--bind", nydus_bench_root, nydus_bench_root], check=True)
subprocess.run(["mount", "--make-rshared", nydus_bench_root], check=True)

nydusd_cfg = {
    "type": "bootstrap",
    "config": {
        "backend_type": "registry",
        "backend_config": {
            "scheme": "https",
            "proxy": {
                "url": "http://127.0.0.1:65001",
                "ping_url": "http://127.0.0.1:40901/server/ping",
                "fallback": False,
                "check_interval": 5,
                "use_http": True,
            },
            "timeout": 10,
            "connect_timeout": 10,
            "retry_limit": 2,
        },
        "cache_type": "fscache",
        "cache_config": {
            "work_dir": f"{nydus_bench_root}/cache",
        },
        "metadata_path": f"{nydus_bench_root}/cache/metadata",
    },
}
with open(f"{nydus_bench_root}/nydusd-config.json", "w") as f:
    json.dump(nydusd_cfg, f, indent=2)

env = os.environ.copy()
env["DOCKER_CONFIG"] = "/run/containerd-nydus/docker"
env["PATH"] = "/var/lib/ateom-gvisor/ray-bench/bin:/home/kubernetes/bin:" + env.get("PATH", "")

snap_log = open("/var/lib/ateom-gvisor/ray-bench/nydus-bench-snapshotter.log", "w")
snap_proc = subprocess.Popen(
    [
        "/home/kubernetes/bin/containerd-nydus-grpc",
        "--log-level=info",
        "--log-to-stdout",
        "--fs-driver=fscache",
        "--daemon-mode=shared",
        "--nydusd=/home/kubernetes/bin/nydusd",
        f"--nydusd-config={nydus_bench_root}/nydusd-config.json",
        f"--address={nydus_bench_sock}",
        f"--root={nydus_bench_root}",
    ],
    env=env,
    stdout=snap_log,
    stderr=subprocess.STDOUT,
)

try:
    for _ in range(100):
        if os.path.exists(nydus_bench_sock):
            break
        time.sleep(0.1)
    assert os.path.exists(nydus_bench_sock), "nydus-bench-snapshotter.sock did not appear"
    print("Started isolated containerd-nydus-grpc on", nydus_bench_sock, flush=True)

    os.environ["PATH"] = env["PATH"]
    sys.path.insert(0, "/var/lib/ateom-gvisor/ray-bench/site-packages")
    sys.path.insert(0, "/var/lib/ateom-gvisor/ray-bench")

    import run_same_node_benchmark

    res = run_same_node_benchmark.bench_nydus_dragonfly(
        "/var/lib/ateom-gvisor/ray-bench/bin/ocilayer-nydus-dragonfly",
        snapshotter_sock=nydus_bench_sock,
    )
    print("NYDUS RESULT:", json.dumps(res, indent=2), flush=True)
finally:
    snap_proc.terminate()
    try:
        snap_proc.wait(timeout=5)
    except Exception:
        snap_proc.kill()
    for mnt in glob.glob(f"{nydus_bench_root}/snapshots/*/mnt"):
        subprocess.run(["umount", "-l", mnt], capture_output=True)
    subprocess.run(["umount", "-l", nydus_bench_root], capture_output=True)
