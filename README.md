# SecureShare — Distributed Edition

An end-to-end-encrypted file sharing system, re-architected as a **distributed system**
(Parallel & Distributed Computing course project). The original app was one Rust server +
one database holding the file bytes. This version splits the system into independent
cooperating nodes that tolerate failures and process data in parallel.

```
                       ┌──────────────┐
   Browser ──────────▶ │  Next.js UI  │
      │                └──────────────┘
      ▼
┌───────────────┐   least-connections load balancing
│ nginx  :8080  │───────────────┬──────────────┬──────────────┐
└───────────────┘               ▼              ▼              ▼
                           ┌─────────┐    ┌─────────┐    ┌─────────┐   stateless API nodes
                           │ api-1   │    │ api-2   │    │ api-3   │   (JWT auth, any node serves any request)
                           └────┬────┘    └────┬────┘    └────┬────┘
        metadata + leader lock  │              │              │   parallel chunk upload / download,
              ┌─────────────────┘              │              │   replication, failover
              ▼                                ▼              ▼
        ┌────────────┐          ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐
        │ PostgreSQL │          │ s1 │ │ s2 │ │ s3 │ │ s4 │ │ s5 │   storage nodes: encrypted chunks only
        └────────────┘          └────┘ └────┘ └────┘ └────┘ └────┘   each chunk lives on R=3 of them
```

## What changed vs. the original

| Original (single server)                          | Distributed edition                                                              |
|---------------------------------------------------|----------------------------------------------------------------------------------|
| One backend process                               | N identical **stateless API nodes** behind an **nginx load balancer**             |
| Whole file stored as one `BYTEA` in PostgreSQL    | File split into **1 MiB encrypted chunks**, stored on **5 storage nodes**         |
| One copy of the data                              | Every chunk **replicated R=3×**; upload succeeds with **write quorum W=2**        |
| Sequential encrypt / store / decrypt              | **Parallel pipeline**: chunks encrypted, replicated, fetched and decrypted concurrently |
| Server dies → everything is gone                  | **Failure detection** (heartbeats), **failover reads**, **self-healing re-replication** |
| Hourly cron on the one server                     | **Leader election** (PostgreSQL advisory lock) so cluster jobs run exactly once    |
| Private keys in `assets/private_keys/*.pem` on one disk | Private keys sealed (AES-256-GCM, `MASTER_KEY`) in the DB, usable from any API node |
| AES-CBC (no integrity)                            | **AES-256-GCM** per chunk + SHA-256 content addressing (tamper/corruption detection) |
| Hard-coded `localhost:8000` in the frontend       | `NEXT_PUBLIC_API_URL`; new **Cluster** page showing live cluster state             |

Full design explanation, trade-offs and report material: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

---

## How to run — Option A: Docker (easiest, recommended)

Requires Docker + Docker Compose. Starts 1 PostgreSQL, 5 storage nodes, 3 API nodes, nginx and the frontend.

```bash
cp .env.example .env
# put a real key in .env:   openssl rand -hex 32   -> MASTER_KEY=...
#                           (on Windows without openssl: any 64 hex characters)
docker compose up --build
```

* UI: <http://localhost:3000> (register two users, upload a file from one to the other)
* Cluster page in the UI: <http://localhost:3000/cluster>
* Raw cluster state: <http://localhost:8080/api/cluster/status>

> The Docker files were written to match the setup I tested, but the sandbox I developed in had no
> Docker, so `docker compose` itself was not executed there. Option B was run end to end.

Stop / reset: `docker compose down` (add `-v` to wipe all data).

## How to run — Option B: without Docker (Linux / macOS / WSL)

Requires: Rust (`cargo`), PostgreSQL, Node.js 18+, optionally `nginx`, `openssl`.

```bash
# 1) database (once)
createuser -P secureshare            # password: secureshare
createdb -O secureshare secureshare

# 2) whole backend cluster: 5 storage nodes + 3 API nodes (+ nginx on :8080 if installed)
./scripts/local_cluster.sh start      # first run compiles the Rust code (a few minutes)
./scripts/local_cluster.sh status

# 3) frontend
cd frontend
cp .env.example .env.local            # defaults already point at http://localhost:8080/api
npm install
npm run dev                           # http://localhost:3000
```

No nginx installed? Set `NEXT_PUBLIC_API_URL=http://localhost:8001/api` and `API_BASE_URL=http://localhost:8001/api`
in `frontend/.env.local` to talk to a single API node.

Custom DB: `DATABASE_URL=postgres://user:pass@host:5432/db ./scripts/local_cluster.sh start`
Stop everything: `./scripts/local_cluster.sh stop`   ·   logs: `./scripts/local_cluster.sh logs api-1`

> Tip: always run the backend in `--release` mode (the script does). RSA-2048 key generation at registration
> is painfully slow in debug builds.

---

## Demo & tests for your presentation

```bash
pip install requests
python scripts/cluster_test.py          # functionality, load balancing, parallel speed-up benchmark
```

With fault injection (kills a storage node mid-test, proves reads/writes survive and the cluster heals itself):

```bash
# Docker
python scripts/cluster_test.py --victim s2 \
   --kill-cmd "docker stop secureshare-storage2" --revive-cmd "docker start secureshare-storage2"

# no Docker
python scripts/cluster_test.py --victim s2 \
   --kill-cmd "./scripts/local_cluster.sh kill-storage s2" --revive-cmd "./scripts/local_cluster.sh start-storage s2"
```

Manual demos worth showing live:

| Demo                       | How                                                                                         | What the audience sees |
|----------------------------|---------------------------------------------------------------------------------------------|------------------------|
| Load balancing             | refresh `/cluster` page or `curl -i localhost:8080/api/health` repeatedly                    | `X-Served-By` changes between api-1/2/3 |
| Storage node crash         | `docker stop secureshare-storage2` (or `kill-storage s2`) then download a file               | still works; node shows DOWN; "under-replicated" rises then falls to 0 |
| Leader failover            | find the leader in `/cluster`, stop that API node                                            | another node becomes leader within ~2 s |
| Where do chunks live?      | `psql ... -c "select node_id, count(*) from chunk_replicas group by 1"`                      | chunks spread over s1–s5, 3 copies each |
| Corrupted disk             | overwrite bytes of a chunk file on one storage node, download the file                        | download is still correct; log shows `CORRUPT` + `read-repair` |

## Results measured while developing (1 vCPU sandbox, everything on one machine)

* 8 MB file: upload → download → SHA-256 identical ✔
* 60 requests through nginx were split 20 / 23 / 17 over the 3 API nodes ✔
* Storage node killed: detected in ~4 s; download and upload during the outage succeeded; the leader restored
  all 78 under-replicated chunks to 3 copies ~16 s later ✔
* Leader killed: new leader in 2 s ✔
* All chunk files of one node corrupted: 6/6 downloads still correct, replicas repaired ✔
* Only 1 of 5 storage nodes alive (W=2): upload rejected with HTTP 503, no data leaked ✔
* Expired files: metadata and all chunk copies removed (DB replica rows == files on disk) ✔
* Parallel speed-up with 1→8 concurrent clients was only ~1.1–1.2× **because the sandbox had a single CPU core
  and all nodes shared it**. Run the benchmark on a multi-core machine (or spread the nodes over several
  machines) for meaningful speed-up numbers, and discuss this honestly (Amdahl's law) in your report.

## Configuration (environment variables of the API node)

| Variable | Default | Meaning |
|---|---|---|
| `STORAGE_NODES` | – | `s1=http://host:9001,s2=...` list of storage nodes |
| `REPLICATION_FACTOR` (R) | 3 | copies kept of every chunk |
| `WRITE_QUORUM` (W) | 2 | acks needed for an upload to succeed |
| `CHUNK_SIZE` | 1048576 | bytes per chunk |
| `PARALLELISM` | 8 | chunks processed concurrently per request |
| `MASTER_KEY` | – | 64 hex chars; seals users' private keys |
| `CLUSTER_TOKEN` | dev token | shared secret API ↔ storage nodes |
| `HEALTH_INTERVAL_MS` / `FAIL_THRESHOLD` | 2000 / 2 | heartbeat period / failures before "DOWN" |
| `REPAIR_AFTER_SECS` / `REPAIR_INTERVAL_SECS` | 20 / 10 | how long a node must be down before its data is re-replicated / how often the leader checks |
| `NODE_ID`, `PORT`, `DATABASE_URL`, `JWT_SECRET_KEY`, `JWT_MAXAGE`, `CORS_ORIGIN` | | as in the original |

## Project layout

```
backend/src/main.rs            API node entry point
backend/src/bin/storage_node.rs  storage node (separate binary)
backend/src/cluster/           ring.rs (consistent hashing) · mod.rs (failure detection, quorum replication,
                               failover reads, read repair) · leader.rs (election) · jobs.rs (cleanup, self-healing)
backend/src/handler/file.rs    parallel chunk upload / download pipeline
backend/src/utils/crypto.rs    AES-GCM, RSA-OAEP, SHA-256, key sealing
backend/migrations/            original schema + new distributed schema
nginx/nginx.conf               load balancer      docker-compose.yml  full deployment
scripts/                       local_cluster.sh · cluster_test.py
frontend/                      original UI + /cluster page, configurable API URL
```
