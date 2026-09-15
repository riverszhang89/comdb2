# Benchmark: `private_blkseq_skip_standalone`

## The tunable

`private_blkseq_skip_standalone` controls how much private-blkseq
(replay/dupe-detection) work to skip **while the database is standalone** (a
single sanctioned node). It is re-evaluated per commit via `bdb_is_standalone()`,
so full blkseq resumes automatically once the cluster grows past one node
(e.g. `bdb cluster add` + election).

| value | name | in-memory replay tracking | WAL blkseq record |
|------:|------|:--:|:--:|
| 0 | stock       | yes | yes |
| 1 | wal-only    | **yes** | **no** |
| 2 | everything  | no  | no |

Default: **1 (wal-only)**. Forced to **0** under `legacy_defaults`
(`legacy_options[]` in `db/config.c`).

Rationale for the default: wal-only drops the replicated/recoverable WAL record
(the actual cost) but keeps the in-memory table, so a client that retries after
a dropped ack is still recognized as a replay on the lone node. It gives up only
*crash/failover durability* of the dedup — moot when there's nothing to fail
over to. mode 2 additionally drops the in-memory table (no live replay
protection while standalone); mode 0 is today's always-on behavior.

Both skip modes only change master-side commit behavior on a standalone node;
the WAL-apply path (`bdb_blkseq_recover`) and the startup scan
(`bdb_recover_blkseq`) are untouched — if nothing was logged there's simply
nothing to replay.

## What the benchmark measures

Standalone node, parallel insert-heavy load (128 writer connections, autocommit
single-row inserts, each its own transaction). Metrics per mode:

- **rows/s** — wall-clock throughput (mean ± stdev).
- **WAL bytes/insert** — transaction-log bytes per insert, from the LSN delta
  (`bdb cluster`). Deterministic; isolates the blkseq WAL-record cost.
- **region_wait** — berkeley log-region mutex contention (`bdb logstat`
  `st_region_wait`).
- **wcount_fill** — log-buffer-full flushes (`st_wcount_fill`); the
  log-buffer-pressure signal.
- **blkseq tracked** — `comdb2_blkseq` row delta; functional check of which
  mode is active.

Methodology matters: the three modes are compared in **strict per-trial
round-robin** (mode 0, 1, 2, 0, 1, 2, …), not one mode fully then the next.
An earlier block-ordered run (all of mode A, then all of mode B, on one db)
manufactured a fake ~6–15% "skip is faster" delta purely from the db's
log/checkpoint state degrading over the run; round-robin removes that bias.
`blkseq_standalone_bench.sh` implements the round-robin.

## Results (128 writers, round-robin, rounds 2–N)

### Deterministic (stable across all trials)

| metric | stock | wal-only | everything |
|--------|------:|---------:|-----------:|
| WAL bytes/insert | ~561 | ~431 (**−23%**) | ~431 (**−23%**) |
| log-region waits | ~21,700 | ~16,300 (**−25%**) | ~16,000 (**−26%**) |
| buffer-full flushes (256KB buf) | ~375 | ~280 (**−25%**) | ~280 (**−25%**) |
| `comdb2_blkseq` tracked / N | all | all | 0 |

The WAL-record drop accounts for the entire log-volume cut and essentially all
the contention relief — identical for wal-only and everything.

### Throughput — a tie

In a clean round-robin (10 rounds, 256 KB log buffer, 22-core host):

| mode | rows/s (mean ± stdev) |
|------|----------------------:|
| stock | 12,408 ± 601 |
| wal-only | 12,202 ± 649 |
| everything | 12,276 ± 773 |

**All three are within ~2% — statistically indistinguishable.** There is **no
throughput win** from skipping blkseq at this operating point.

Why: even under a small (256 KB) log buffer and 6× core-count concurrency, the
log region is not the bottleneck (region-wait ratio stayed ~1.6%). A blkseq
find/insert is a cached in-memory btree op (single-digit µs) under 1-of-8 stripe
mutexes, against a ~10 ms/commit pipeline dominated by the client round-trip,
SQL processing, the real data-row write **and its own WAL record**, and the lock
manager. Dropping the ~130-byte blkseq WAL record cuts log *volume* and region
*contention*, but neither is the throughput limiter here.

Earlier notes in this file that claimed a ~6–15% throughput gain were a
measurement artifact of block-ordered trials and are retracted; the round-robin
above is the trustworthy result.

## Interpretation

- The win from skipping blkseq on a standalone node is **~23% less log volume
  and ~25% less log-region contention** — i.e. disk, log-shipping, and
  recovery-scan savings — **not throughput**, on this workload/hardware.
- **wal-only ≈ everything** on every performance metric (the extra per-commit
  in-memory btree op in wal-only is not measurable), so wal-only is preferred:
  same savings, keeps live standalone replay protection.
- A workload that is genuinely log-I/O- or log-shipping-bound (slower disks,
  fsync-per-commit, WAN log shipping) would convert the volume/contention
  reduction into more benefit than this box shows.

## Running it yourself

```
./blkseq_standalone_bench.sh [BUILDDIR]     # BUILDDIR defaults to ./build
```

Stands up a throwaway single-node db (starting pmux if needed), sweeps two log
buffer sizes (10 MB roomy, 256 KB pressured), round-robins the three modes, and
prints a per-mode summary. Edit the knobs at the top: `WRITERS` (set to ~6× your
core count to actually pressure the log region), `PER`, `ROUNDS`, `LOGBUFS`.
Run on a real Linux box (not WSL) for meaningful timing. It tears down the db,
pmux (if it started it), and its temp dir on exit.
