# Restore ClickHouse Cloud native S3 backup to OSS ClickHouse

Restores a ClickHouse Cloud backup with propriatery Shared engines onto an OSS (or self-managed) cluster that uses OSS database and table engines.

Automation: [`restore_shared_to_replicated.py`](restore_shared_to_replicated.py).

This is **not** `clickhouse-backup` extension. The source is the embedded backup created by `BACKUP … TO S3` DDL statement. The feature request to `clickhouse-backup` is submitted as [#1508](https://github.com/Altinity/clickhouse-backup/issues/1508).

## Problem

ClickHouse Cloud (Shared / Shared\*MergeTree) backups do not restore cleanly onto OSS:

1. **Engine mismatch** — Cloud DDL uses database engein `Shared` and table engines `Shared*MergeTree`. OSS expects `Atomic` or `Replicated` and `Replicated*MergeTree` correspondingly. A plain `RESTORE` fails complaining about unknown engines.
2. **Checksum object layout** — With `data_file_name_generator=Checksum`, logical names in `.backup` (`metadata/db/table.sql`) are **not** the S3 keys. Blobs live under checksum paths (`NNN/<checksum>`). The extra name resolution step is required.
3. **Packed parts** — ClickHouse Cloud parts are stored as a single `data.packed` archive instead of a full part directory (`columns.txt`, `*.bin`, marks, …). `RESTORE` on a server that cannot open Packed part format fails with `NO_FILE_IN_DATA_PART` / missing `columns.txt`, even after DDL rewrite succeeds.

The script solves (1) and (2). **(3) is a ClickHouse version requirement**, not something the script can rewrite.

## Solution

For each database (then each table) in the manifest:

1. Read `.backup` and map logical metadata paths to S3 blobs
2. Download `metadata/*.sql` / `metadata/*/*.sql`
3. Rewrite schema:
   - `ENGINE = Shared` → `Atomic`
   - `Shared*MergeTree` → matching `Replicated*MergeTree` (Replacing, Aggregating, …)
   - If Replicated DDL has no `(zk_path, replica)` args, insert configurable defaults
4. `CREATE … IF NOT EXISTS` with the rewritten DDL
5. `RESTORE TABLE … FROM S3(…) SETTINGS allow_different_database_def=1, allow_different_table_def=1`
6. Compare restored `system.parts` size/rows against backup `data/<db>/<table>/` byte sums

System databases (`system`, `information_schema`, …) are skipped. Optional filters: `--database`, `--table`, `--skip-empty-tables`, `--continue-on-error`, `--dry-run`.

### Example

```bash
python3 tools/restore_shared_to_replicated.py \
  --bucket my-bucket \
  --access-key ... \
  --secret-key ... \
  --prefix path/to/backup \
  --region us-east-1 \
  --clickhouse-url http://127.0.0.1:8123 \
  --clickhouse-user default \
  --clickhouse-password ''
```

Useful overrides:

| Flag | When |
| --- | --- |
| `--endpoint-url` / `--s3-restore-url` | MinIO or path-style S3; HTTP vs HTTPS mismatches |
| `--base-prefix` | Incremental backups with `use_base` |
| `--replicated-zk-path` / `--replicated-replica` | Cloud DDL without Keeper args |

## Dependency: Packed parts (`data.packed`)

### What Packed is

ClickHouse organizes MergeTree parts with **two independent axes** (see [part types and storage formats](https://clickhouse.com/docs/resources/support-center/knowledge-base/data-management/understanding-part-types-and-storage-formats)):

1. **Part type** (`system.parts.part_type`) — how column data is laid out inside the part  
2. **Storage format** (`system.parts.part_storage_type`) — how those files are stored on disk / object storage  

Packed is **not** a third part type beside Wide/Compact. It is a storage format that can wrap either.

#### Part types (Wide vs Compact)

| Part type | Layout | Typical when |
| --- | --- | --- |
| **Wide** | Each column has its own data file(s) and marks file | Larger parts; selective column reads |
| **Compact** | All columns share one data file and one marks file | Smaller parts; faster inserts; queries that need most columns |

Controlled by `min_bytes_for_wide_part` / `min_rows_for_wide_part`: below either threshold → Compact; otherwise → Wide.

#### Storage formats (Full vs Packed)

| Storage | On-disk layout | Typical when |
| --- | --- | --- |
| **Full** | Each part file is a separate object in the part directory (`columns.txt`, checksums, column/marks files, …) | Default on OSS (`min_*_for_full_part_storage = 0`) |
| **Packed** | Most part files are bundled into one archive named **`data.packed`** (projections and a few service files such as `txn_version.txt` stay separate) | Cloud / object-storage heavy setups; small or low-level parts |

Controlled by `min_bytes_for_full_part_storage`, `min_rows_for_full_part_storage`, and `min_level_for_full_part_storage`. If any threshold is met, the part is Packed. OSS defaults keep all three at `0` (always Full). Cloud often writes Packed for insert-level parts to cut object-storage API calls.

This restore problem is specifically **Packed storage**: the backup part directory contains `data.packed` instead of a loose `columns.txt`. Wide vs Compact still applies *inside* that archive; older OSS loaders never open the archive, so they fail before part type matters.

ClickHouse Cloud often writes Packed for small / low-level parts. OSS added the same format in [ClickHouse#108118](https://github.com/ClickHouse/ClickHouse/pull/108118) that is only available in 26.8 OSS version. **Reading** Packed does not require raising `min_*_for_full_part_storage` on the target; the server must contain the Packed loader.

### Failure mode

Typical error during `RESTORE TABLE` (DDL rewrite already succeeded):

```text
Code: 226. DB::Exception: No columns.txt in part …:
Part contains files: data.packed (… bytes): Part is empty
(NO_FILE_IN_DATA_PART)
```

`RESTORE` copied `data.packed` into `tmp_restore_…`, then the server loaded the part as **Full** and looked for a loose `columns.txt`. In Packed storage that file is **inside** the archive. Without `DataPartStorageOnDiskPacked` detection (look for `data.packed` in `MergeTreeDataPartBuilder`), restore treats the part as empty.

### Version reality (important)

Changelog mentions of Packed in **26.7** are **not** enough by themselves. Tag `v26.7.1.1315-stable` (and thus **26.7.4.58**) still only wires **Full** storage in that builder path (“unused here, but used in private repo”). The same `NO_FILE_IN_DATA_PART` appears on 26.3 and 26.7.4 when restoring Cloud Packed parts. ClickHouse build is needed that actually includes Packed **load** from [#108118](https://github.com/ClickHouse/ClickHouse/pull/108118) (master / a later release line where `getPartStorageAndMarkType` selects Packed when `data.packed` is present), **or** restore on ClickHouse Cloud / a private fork that already had Packed I/O.


### How to tell a backup uses Packed

In the `.backup` manifest or under the backup’s data objects, part directories that list only (or primarily) `data.packed` instead of `columns.txt` are Packed. After a failed restore, the error text `Part contains files: data.packed` is definitive.

### Workarounds if the target cannot load Packed

1. Restore on a server/version that understands Packed, then replicate or dump out
2. On the source (Cloud), force Full parts (merge / adjust thresholds) and take a **new** backup that no longer stores `data.packed` for those parts
3. Restore only tables whose backup parts are Full (inspect part file lists in the manifest / objects)

## Scope and non-goals

- Does not convert access entities / users (Cloud access DDL may not parse on older OSS; prefer `EXCEPT` / `restore_access_entities=0` when using native `RESTORE` for that)
- Does not unpack Packed parts in Python — ClickHouse must do that
- Does not replace operator / CHI lifecycle; point `--clickhouse-url` at a reachable HTTP interface
