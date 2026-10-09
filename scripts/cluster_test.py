#!/usr/bin/env python3
"""
End-to-end test + benchmark for the distributed SecureShare cluster.

What it demonstrates (each section prints its own result):
  1. functionality     upload -> encrypted, chunked, replicated -> download -> SHA-256 identical
  2. load balancing    requests are spread over the API nodes (X-Served-By header)
  3. parallel speed-up throughput with 1, 2, 4, 8 concurrent clients
  4. fault tolerance   (optional) kill a storage node: downloads keep working,
                       uploads keep working, the cluster re-replicates by itself

Usage
  pip install requests
  python scripts/cluster_test.py                                  # sections 1-3
  python scripts/cluster_test.py --kill-cmd  "docker stop secureshare-storage2" \
                                 --revive-cmd "docker start secureshare-storage2" \
                                 --victim s2                       # + section 4 (Docker)
  python scripts/cluster_test.py --kill-cmd  "./scripts/local_cluster.sh kill-storage s2" \
                                 --revive-cmd "./scripts/local_cluster.sh start-storage s2" \
                                 --victim s2                       # + section 4 (no Docker)
"""
import argparse
import hashlib
import os
import subprocess
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

PASSWORD = "Passw0rd!"
FILE_PASSWORD = "sharepw1"


def hr(title):
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


def expiry(hours=1):
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


class Client:
    def __init__(self, base, label):
        self.base = base.rstrip("/")
        self.label = label
        self.email = f"{label}-{uuid.uuid4().hex[:8]}@test.local"
        self.token = None
        self.s = requests.Session()

    def register_and_login(self):
        r = self.s.post(f"{self.base}/auth/register", json={
            "name": self.label, "email": self.email,
            "password": PASSWORD, "passwordConfirm": PASSWORD}, timeout=120)
        r.raise_for_status()
        r = self.s.post(f"{self.base}/auth/login", json={"email": self.email, "password": PASSWORD}, timeout=60)
        r.raise_for_status()
        self.token = r.json()["token"]
        self.s.headers["Authorization"] = f"Bearer {self.token}"

    def upload(self, to_email, name, data):
        r = self.s.post(f"{self.base}/file/upload",
                        data={"recipient_email": to_email, "password": FILE_PASSWORD, "expiration_date": expiry()},
                        files={"fileUpload": (name, data)}, timeout=300)
        r.raise_for_status()
        return r.headers.get("X-Served-By")

    def inbox(self):
        r = self.s.get(f"{self.base}/list/receive", params={"page": 1, "limit": 50}, timeout=30)
        r.raise_for_status()
        return {f["file_name"]: f["file_id"] for f in r.json()["files"]}

    def download(self, shared_id):
        r = self.s.post(f"{self.base}/file/retrieve",
                        json={"shared_id": shared_id, "password": FILE_PASSWORD}, timeout=300)
        r.raise_for_status()
        return r.content


def status(base):
    return requests.get(f"{base}/cluster/status", timeout=10).json()


def sha(b):
    return hashlib.sha256(b).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8080/api")
    ap.add_argument("--size-mb", type=float, default=8, help="size of the functional test file")
    ap.add_argument("--bench-files", type=int, default=16, help="files per benchmark run")
    ap.add_argument("--bench-size-mb", type=float, default=4)
    ap.add_argument("--skip-bench", action="store_true")
    ap.add_argument("--kill-cmd")
    ap.add_argument("--revive-cmd")
    ap.add_argument("--victim", default="s2", help="storage node id that --kill-cmd stops")
    a = ap.parse_args()
    base = a.base.rstrip("/")

    hr("0. Cluster")
    for _ in range(60):
        try:
            if requests.get(f"{base}/health", timeout=3).ok:
                break
        except requests.RequestException:
            pass
        time.sleep(1)
    st = status(base)
    print(f"storage nodes alive: {st['alive_storage_nodes']}/{st['total_storage_nodes']}   "
          f"R={st['replication_factor']} W={st['write_quorum']}   leader={st['leader']}")

    sender, receiver = Client(base, "alice"), Client(base, "bob")
    print("registering users (RSA-2048 key generation, can take a few seconds)...")
    t = time.time()
    with ThreadPoolExecutor(2) as ex:
        list(ex.map(lambda c: c.register_and_login(), [sender, receiver]))
    print(f"2 users registered in parallel in {time.time() - t:.1f}s")

    # ------------------------------------------------------------------ 1
    hr("1. Functionality: encrypt -> chunk -> replicate -> download -> verify")
    payload = os.urandom(int(a.size_mb * 1024 * 1024))
    name = f"functional-{uuid.uuid4().hex[:6]}.bin"
    t = time.time()
    node = sender.upload(receiver.email, name, payload)
    up = time.time() - t
    sid = receiver.inbox()[name]
    t = time.time()
    got = receiver.download(sid)
    down = time.time() - t
    ok = sha(got) == sha(payload)
    print(f"uploaded {len(payload)/1e6:.1f} MB via {node} in {up:.2f}s, downloaded in {down:.2f}s")
    print(f"sha256 identical: {'PASS' if ok else 'FAIL'}")
    if not ok:
        sys.exit(1)
    time.sleep(2.5)  # per-node usage numbers are refreshed by the 2s heartbeat
    st = status(base)
    print(f"chunks={st['metadata']['chunks']}  replica copies={st['metadata']['replicas']}  "
          f"under-replicated={st['metadata']['under_replicated']}")
    for n in st["storage_nodes"]:
        print(f"   {n['id']}: {n['chunks']:>4} chunks  {n['bytes']/1e6:8.2f} MB  {'UP' if n['alive'] else 'DOWN'}")

    # ------------------------------------------------------------------ 2
    hr("2. Load balancing across API nodes")
    def ping(_):
        return requests.get(f"{base}/cluster/status", timeout=10).headers.get("X-Served-By")
    with ThreadPoolExecutor(8) as ex:
        counts = Counter(ex.map(ping, range(60)))
    for k, v in sorted(counts.items()):
        print(f"   {k}: {v:>3} requests  " + "#" * v)

    # ------------------------------------------------------------------ 3
    if not a.skip_bench:
        hr("3. Parallel speed-up (N concurrent clients, same total work)")
        size = int(a.bench_size_mb * 1024 * 1024)
        blobs = [os.urandom(size) for _ in range(min(a.bench_files, 4))]
        total_mb = a.bench_files * a.bench_size_mb
        print(f"each run uploads {a.bench_files} files x {a.bench_size_mb} MB = {total_mb:.0f} MB, "
              f"then downloads them again\n")
        print(f"{'clients':>8} {'upload s':>10} {'MB/s':>8} {'speedup':>8} {'download s':>11} {'MB/s':>8} {'speedup':>8}")
        base_up = base_down = None
        for workers in (1, 2, 4, 8):
            names = [f"bench-{workers}-{i}-{uuid.uuid4().hex[:5]}.bin" for i in range(a.bench_files)]
            t = time.time()
            with ThreadPoolExecutor(workers) as ex:
                list(ex.map(lambda i: sender.upload(receiver.email, names[i], blobs[i % len(blobs)]),
                            range(a.bench_files)))
            up = time.time() - t
            inbox = receiver.inbox()
            t = time.time()
            with ThreadPoolExecutor(workers) as ex:
                results = list(ex.map(lambda i: receiver.download(inbox[names[i]]), range(a.bench_files)))
            down = time.time() - t
            assert all(sha(r) == sha(blobs[i % len(blobs)]) for i, r in enumerate(results)), "corrupt download!"
            base_up = base_up or up
            base_down = base_down or down
            print(f"{workers:>8} {up:>10.2f} {total_mb/up:>8.1f} {base_up/up:>7.2f}x {down:>11.2f} {total_mb/down:>8.1f} {base_down/down:>7.2f}x")
        print("\n(speed-up is limited by your CPU cores, the disk, and the fact that everything runs on one machine)")

    # ------------------------------------------------------------------ 4
    if a.kill_cmd:
        hr(f"4. Fault tolerance: crash storage node '{a.victim}'")
        print(f"$ {a.kill_cmd}")
        subprocess.run(a.kill_cmd, shell=True, check=False)
        t0 = time.time()
        while time.time() - t0 < 30:
            st = status(base)
            if any(n["id"] == a.victim and not n["alive"] for n in st["storage_nodes"]):
                break
            time.sleep(1)
        print(f"failure detected after {time.time() - t0:.1f}s "
              f"(alive: {st['alive_storage_nodes']}/{st['total_storage_nodes']}, "
              f"under-replicated chunks: {st['metadata']['under_replicated']})")

        t = time.time()
        got = receiver.download(sid)
        print(f"download during outage: {'PASS' if sha(got) == sha(payload) else 'FAIL'} ({time.time() - t:.2f}s) -> served by the surviving replicas")

        name2 = f"during-outage-{uuid.uuid4().hex[:6]}.bin"
        data2 = os.urandom(3 * 1024 * 1024)
        sender.upload(receiver.email, name2, data2)
        got2 = receiver.download(receiver.inbox()[name2])
        print(f"upload during outage:   {'PASS' if sha(got2) == sha(data2) else 'FAIL'} (write quorum W={st['write_quorum']} still reachable)")

        print("waiting for the leader to re-replicate (self-healing)...")
        t0 = time.time()
        while time.time() - t0 < 120:
            st = status(base)
            u = st["metadata"]["under_replicated"]
            print(f"   t+{time.time() - t0:4.0f}s  under-replicated chunks: {u}")
            if u == 0:
                break
            time.sleep(4)
        print("self-healing:", "COMPLETE" if st["metadata"]["under_replicated"] == 0 else "not finished (increase wait / check logs)")
        got = receiver.download(sid)
        print(f"download after healing: {'PASS' if sha(got) == sha(payload) else 'FAIL'}")

        if a.revive_cmd:
            print(f"\n$ {a.revive_cmd}")
            subprocess.run(a.revive_cmd, shell=True, check=False)
            for _ in range(30):
                st = status(base)
                if st["alive_storage_nodes"] == st["total_storage_nodes"]:
                    print(f"node '{a.victim}' recovered, all {st['total_storage_nodes']} nodes UP")
                    break
                time.sleep(1)

    hr("DONE")


if __name__ == "__main__":
    main()
