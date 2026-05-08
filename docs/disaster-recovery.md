# Disaster recovery

This runbook covers how Loveliness backs up cluster state, what the
archives contain, and how to restore them after data loss.

For day-to-day backup configuration (S3 bucket, retention, schedule),
see [configuration.md](configuration.md). This document focuses on
operational procedures.

## What gets archived

A Loveliness backup is a single gzipped tarball containing:

- **Per-shard databases** — `shard-N` and `shard-N.wal` for every shard.
  Both files are required; the WAL alone carries any writes since the
  last Kuzu checkpoint, including DDL.
- **Replication WAL** — `wal/wal-shard-*.log`.
- **Raft directory** — `raft/snapshots/...` and `raft/raft-log.bolt`.
  This is the FSM state: database catalog, shard map, schema
  annotations. Without it a restored cluster has data but no idea what
  tables exist.
- **Manifest** — `manifest.json` at the end of the archive listing every
  file with its SHA-256 and size. Manifest version 3 (the current
  format) records:
  - `version: 3`
  - `created_at`, `node_id`, `shard_count`, `wal_sequence`
  - `loveliness_version` — the binary that produced the archive
  - `files` — `{archive_path: {sha256, size}}` for integrity verification

`CreateBackup` forces a Raft snapshot before archiving so the snapshot
store is current — without that step a quiet cluster archives only the
BoltDB log and the FSM has to replay every command on startup.

## Taking a backup

### Scheduled

Set `LOVELINESS_BACKUP_INTERVAL_MIN` and either `LOVELINESS_S3_BUCKET`
or `LOVELINESS_BACKUP_DIR`. The scheduler runs in-process on every
node; only the leader's archive is authoritative for the FSM state, but
all nodes upload — operators choose which archive to restore from.

`LOVELINESS_BACKUP_RETENTION` controls how many archives the scheduler
keeps in the configured store before pruning oldest-first.

### On demand

```sh
curl -s http://node:8080/backup -o snapshot-$(date +%Y%m%d-%H%M%S).tar.gz
```

`GET /backup` streams a fresh archive. The server forces an FSM
snapshot first.

To upload directly to the configured store:

```sh
curl -s -X POST http://node:8080/backup/store
```

## Restoring

### Pre-flight

1. **Stop the target node.** Kuzu holds open file handles on
   `shard-N` and will overwrite restored bytes on shutdown otherwise.
2. **Decide on the data dir.** The default is `LOVELINESS_DATA_DIR` or
   `./data`. Restore writes into this directory; existing `raft/`
   contents are wiped before extraction so stale FSM log entries don't
   mix with the restored snapshot.
3. **Confirm the node identity.** `LOVELINESS_NODE_ID` should match
   the archive's `node_id` unless you intend to clone a cluster — see
   [Cross-cluster restores](#cross-cluster-restores) below.

### From a local file

```sh
loveliness restore --file snapshot.tar.gz --data-dir ./data
```

### From the configured store

If `LOVELINESS_S3_BUCKET` or `LOVELINESS_BACKUP_DIR` is set, the CLI
can pull directly:

```sh
# Most recent archive in the store.
loveliness restore

# A specific key.
loveliness restore --key backup-20260509-120000.tar.gz
```

### Inspect a manifest without extracting

Useful when you have an unknown archive on disk and want to check what
it contains before touching the data dir:

```sh
loveliness restore --file snapshot.tar.gz --manifest-only
```

This prints the parsed `manifest.json` as JSON and exits without
writing anything to disk.

## Integrity verification

Manifest v3 archives carry a SHA-256 for every archived file, computed
streaming during backup creation (no double-read). `RestoreBackup`
re-hashes each file as it extracts via `io.MultiWriter` and aborts
with a `checksum mismatch` error if any file's hash does not match the
manifest. Files extracted before the mismatch are left in place — the
operator should wipe the data dir before retrying.

v2 archives (no `files` map) restore without verification — they
predate this feature and are accepted for backward compatibility.

## Cross-cluster restores

The CLI refuses to restore an archive whose `node_id` differs from
this node's `LOVELINESS_NODE_ID`:

```
restore: refusing cross-cluster restore: archive node_id="prod-1", this node="staging-1"
  Pass --confirm-cross-cluster to override (e.g. when intentionally cloning a cluster).
```

This is a foot-gun guard, not a security boundary. Pass
`--confirm-cross-cluster` when intentionally cloning state from one
cluster to another (e.g. seeding a staging environment from a
production snapshot).

## After a restore

1. **Restart the cluster.** The CLI reminds you. Until restart, nothing
   has reloaded the new shard files or FSM snapshot.
2. **Verify the data.** Run a known-shape query (e.g.
   `MATCH (n) RETURN count(n)`) before re-enabling traffic.
3. **Check Raft membership.** If you restored on a node that was
   removed from the cluster while the archive was being taken, the
   restored FSM may carry stale membership. Use the cluster admin
   endpoints to reconcile.

## Common failure modes

| Symptom                                              | Cause                                                              | Fix                                                                          |
| ---------------------------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| `checksum mismatch for "shard-0"`                    | Archive was corrupted in transit (S3, scp, etc.)                   | Re-fetch the archive; do not bypass verification.                            |
| `refusing cross-cluster restore`                     | `node_id` mismatch; usually wrong archive selected                 | Verify with `--manifest-only`; pass `--confirm-cross-cluster` if intentional. |
| Restored cluster has no tables                       | Archive predates v2 (no Raft directory)                            | Re-take the backup with a current binary.                                    |
| Cluster won't form quorum after restore              | Raft log lineage diverged between archive and other live nodes     | Restore all nodes from snapshots taken close in time, then re-bootstrap.     |
| `read manifest: backup archive missing manifest.json` | Truncated or non-Loveliness archive                                | Verify the file is the full archive (size, gzip magic).                      |
