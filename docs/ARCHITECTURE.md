# Design document — SecureShare distributed edition

## 1. Goals
1. **Scalability** – add API nodes (compute/bandwidth) and storage nodes (capacity) horizontally.
2. **Availability / fault tolerance** – survive crashes of storage nodes and API nodes without losing data.
3. **Parallelism** – use all cores and all nodes for the heavy work (cryptography, I/O).
4. **Security preserved** – files remain encrypted at rest; storage nodes never see plaintext.

## 2. Components
| Component | Count | Role | State |
|---|---|---|---|
| nginx | 1 | load balancer (least-connections, passive health checks) | none |
| API node (`secureshare_backend`) | 3+ | auth, upload/download orchestration, failure detection, background jobs | **stateless** (JWT) |
| Storage node (`storage_node`) | 5+ | stores encrypted chunks on local disk, content-addressed | chunk files |
| PostgreSQL | 1 | **metadata only**: users, files, chunk list, replica map, shares | durable |

## 3. Distributed-systems concepts and where they are implemented
| Concept | Implementation | File |
|---|---|---|
| Horizontal scaling / stateless services | JWT auth; no sessions; private keys moved from local disk to DB | `middleware.rs`, `utils/keys.rs` |
| Load balancing | nginx `least_conn`, `X-Served-By` header for visibility | `nginx/nginx.conf`, `router.rs` |
| Data partitioning | files cut into chunks; **consistent hashing** with 128 virtual nodes decides placement | `cluster/ring.rs` |
| Replication | every chunk sent to R=3 nodes in parallel | `Cluster::put_chunk` |
| Quorum | write succeeds with W=2 acks (R=3, W=2) — tolerates 1 slow/dead replica per write | `Cluster::put_chunk` |
| Sloppy quorum | if a primary replica fails, the next live node on the ring takes its place | `Cluster::put_chunk` |
| Failure detection | heartbeat every 2 s + passive detection from failed RPCs; node DOWN after 2 consecutive failures | `Cluster::probe_all`, `record_failure` |
| Failover | reads try live replicas (shuffled for load spreading), then others | `Cluster::get_chunk` |
| Integrity | chunk id = SHA-256(ciphertext); verified by storage node on write and by API node on read; AES-GCM tag verified on decrypt | `bin/storage_node.rs`, `utils/crypto.rs` |
| Read repair | replica that returns "missing"/corrupt is rewritten in the background | `Cluster::get_chunk` |
| Self-healing (anti-entropy) | leader finds chunks with < R copies on healthy nodes and copies them | `cluster/jobs.rs::repair_round` |
| Leader election / mutual exclusion | PostgreSQL session advisory lock on a dedicated connection; released automatically if the leader dies | `cluster/leader.rs` |
| Atomic commit of metadata | chunks are written first, then one DB transaction publishes file + chunks + replica map + share link | `DBClient::save_file` |
| Parallel computing | per-chunk encrypt/hash on the blocking thread pool (data parallelism over cores) + bounded concurrent network I/O (`buffer_unordered`, `buffered`) | `handler/file.rs` |
| Ordered parallel streaming | download uses `buffered(P)`: chunks fetched/decrypted out of order in parallel, emitted in order | `handler/file.rs` |
| Garbage collection | leader deletes expired files' metadata and then all chunk copies | `jobs.rs::cleanup_round` |

## 4. Upload data flow
1. Client → nginx → any API node (`POST /api/file/upload`, multipart).
2. API node generates a random 256-bit file key; wraps it with the recipient's RSA public key (OAEP-SHA256).
3. File is cut into 1 MiB chunks. For up to `PARALLELISM` chunks at once:
   * encrypt with AES-256-GCM (random nonce, AAD = file id ‖ chunk index) and compute SHA-256 → *chunk id*;
   * send ciphertext to the first R live nodes of the chunk's preference list, **in parallel**; wait for W acks.
4. One DB transaction stores file, chunk list (id, nonce, size), replica map and share link.
   If anything fails before commit, already-written chunks are deleted (no partial files become visible).

## 5. Download data flow
1. Verify share link (recipient, not expired) and share password (Argon2).
2. Load the user's sealed private key from the DB, open it with `MASTER_KEY`, unwrap the file key.
3. For each chunk (P in parallel, order preserved): fetch from a live replica → verify SHA-256 → decrypt/verify GCM tag.
4. Stream plaintext to the client with `Content-Length`.

## 6. Failure scenarios
| Failure | Behaviour |
|---|---|
| 1–2 storage nodes down (R=3) | all reads succeed from remaining replicas; writes continue (quorum 2); after `REPAIR_AFTER_SECS` the leader restores 3 copies on other nodes |
| Storage node returns after being down | rejoins automatically (heartbeat); its old copies remain valid |
| Chunk corrupted / lost on a node | detected by hash check; other replica serves the read; read-repair rewrites it |
| < W storage nodes alive | uploads rejected with 503 (consistency over availability for writes) |
| API node crash | nginx stops routing to it (`max_fails`); other nodes serve; no user-visible state lost |
| Leader crash | its DB session ends → advisory lock released → another node becomes leader (≈2 s) |
| PostgreSQL down | the system is unavailable (see limitations) |

## 7. Consistency & CAP discussion (good for the report)
* Chunks are **immutable and content-addressed**, so replicas can never disagree about a chunk's content — the usual
  replica-conflict problem disappears. Only *which nodes hold it* can be stale, and that is tracked in PostgreSQL.
* Metadata is strongly consistent (single PostgreSQL). Under a partition the system chooses **consistency**
  (writes need W live nodes and the DB) over availability for writes, but remains **available for reads** as long as
  one replica and the DB are reachable.

## 8. Known limitations / future work
* **PostgreSQL is a single point of failure** — production would use streaming replication + automatic failover
  (Patroni) or a distributed SQL store. Static storage-node membership (`STORAGE_NODES`) could be replaced by dynamic registration.
* The API node buffers the uploaded file in memory (limit `MAX_UPLOAD_BYTES`, default 256 MiB); true streaming would remove that.
* Chunks left on a storage node that was down during a file deletion become orphans (an orphan-scrubbing job is future work).
* Server-side key custody: like the original app, the server can decrypt on the recipient's behalf (keys are sealed
  under `MASTER_KEY`). True end-to-end encryption would require client-side keys.
* Leader election by advisory lock can briefly overlap during network partitions; the leader jobs are idempotent, so this is harmless.
* Speed-up is bounded by Amdahl's law and by shared resources when all nodes run on one machine.

## 9. Suggested experiments for the report
1. Throughput vs. number of concurrent clients (1,2,4,8,16) → speed-up/efficiency curve (`cluster_test.py`).
2. `PARALLELISM=1` vs `8` for one large upload (intra-request parallelism) on a multi-core host.
3. Replication factor vs. write latency (`REPLICATION_FACTOR` 1,2,3).
4. Recovery time after killing a node (detection time + repair time) — the test script prints both.
5. Chunk distribution across nodes (`select node_id,count(*) from chunk_replicas group by 1`) to evaluate the hash ring balance.
