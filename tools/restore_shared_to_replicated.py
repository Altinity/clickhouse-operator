#!/usr/bin/env python3
"""Restore a ClickHouse native S3 backup onto Atomic / ReplicatedMergeTree.

Reads the `.backup` manifest, downloads checksum-named metadata SQL, rewrites
Shared database engines to Atomic and Shared*MergeTree to Replicated*MergeTree,
applies the DDL, then RESTORE DATABASE with allow_different_*_def.

Uses only the Python standard library (AWS Signature V4 for S3 GET).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import io
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from urllib.parse import quote, unquote

DEFAULT_PREFIX_LENGTH = 3
SKIP_DATABASES = {
    "system",
    "information_schema",
    "INFORMATION_SCHEMA",
    "_temporary_and_external_tables",
}

# Longer names first so SharedMergeTree does not eat SharedReplacingMergeTree.
SHARED_MERGETREE_RE = re.compile(
    r"\bShared("
    r"VersionedCollapsing|"
    r"Replacing|"
    r"Aggregating|"
    r"Summing|"
    r"Collapsing|"
    r"Graphite|"
    r"Coalescing|"
    r""
    r")MergeTree\b"
)

# Database engine Shared, but not SharedMergeTree / SharedSet / etc.
SHARED_DATABASE_ENGINE_RE = re.compile(
    r"(ENGINE\s*=\s*)Shared\b(?![A-Za-z])",
    re.IGNORECASE,
)

# Cloud Shared* often has no zk args; OSS Replicated* needs them.
REPLICATED_MISSING_ARGS_RE = re.compile(
    r"(ENGINE\s*=\s*Replicated(?:VersionedCollapsing|Replacing|Aggregating|"
    r"Summing|Collapsing|Graphite|Coalescing)?MergeTree)"
    r"(?!\s*\()",
    re.IGNORECASE,
)

IF_NOT_EXISTS_RE = re.compile(
    r"^\s*CREATE\s+(DATABASE|TABLE|VIEW|MATERIALIZED\s+VIEW|DICTIONARY)\s+IF\s+NOT\s+EXISTS\b",
    re.IGNORECASE,
)
CREATE_KIND_RE = re.compile(
    r"^\s*CREATE\s+(DATABASE|MATERIALIZED\s+VIEW|VIEW|DICTIONARY|TABLE)\b",
    re.IGNORECASE,
)


@dataclass
class ManifestFile:
    name: str
    size: int = 0
    checksum: str = ""
    data_file: str = ""
    use_base: bool = False
    object_key: str = ""


@dataclass
class Manifest:
    data_file_name_generator: str = "FirstFileName"
    data_file_name_prefix_length: int = DEFAULT_PREFIX_LENGTH
    files: list[ManifestFile] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True)
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)
    p.add_argument(
        "--prefix",
        required=True,
        help="S3 key prefix of the backup (directory that contains .backup)",
    )
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--endpoint-url", default="", help="Custom S3 endpoint (MinIO, etc.)")
    p.add_argument(
        "--base-prefix",
        default="",
        help="S3 prefix of the base backup (incrementals with use_base)",
    )
    p.add_argument(
        "--s3-restore-url",
        default="",
        help="URL passed to RESTORE ... FROM S3('...'). "
        "Default: https://<bucket>.s3.<region>.amazonaws.com/<prefix>",
    )
    p.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    p.add_argument("--clickhouse-user", default="default")
    p.add_argument("--clickhouse-password", default="")
    p.add_argument("--clickhouse-database", default="default")
    p.add_argument(
        "--replicated-zk-path",
        default="'/clickhouse/tables/{uuid}/{shard}'",
        help="First Replicated*MergeTree argument when Cloud DDL has none",
    )
    p.add_argument(
        "--replicated-replica",
        default="'{replica}'",
        help="Second Replicated*MergeTree argument when Cloud DDL has none",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-apply", action="store_true", help="Do not run CREATE/RESTORE")
    p.add_argument("--database", action="append", default=[], help="Only this database (repeatable)")
    p.add_argument("--table", action="append", default=[], help="Only this table name (repeatable)")
    p.add_argument("--continue-on-error", action="store_true")
    p.add_argument(
        "--skip-empty-tables",
        action="store_true",
        help="Skip objects with no data/<db>/<table>/ files in the backup "
        "(empty MergeTree tables; also skips views and dictionaries)",
    )
    return p.parse_args()


def join_key(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p and p.strip("/"))


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_key(secret: str, datestamp: str, region: str, service: str) -> bytes:
    k_date = _sign(("AWS4" + secret).encode("utf-8"), datestamp)
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()


def ensure_url_scheme(url: str) -> str:
    """urlparse('host:9000') treats 'host' as scheme and leaves netloc empty."""
    url = url.strip()
    if "://" in url:
        return url
    hostport = url.split("/", 1)[0]
    port = hostport.rsplit(":", 1)[-1] if ":" in hostport else ""
    scheme = "http" if port in {"80", "8080", "9000", "9090"} else "https"
    return f"{scheme}://{url}"


class S3Client:
    """Minimal GetObject client (SigV4). Path-style; scheme taken from endpoint / restore URL."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.access_key = args.access_key
        self.secret_key = args.secret_key
        self.region = args.region
        self.bucket = args.bucket
        self.endpoint_url = ensure_url_scheme(args.endpoint_url) if args.endpoint_url else ""
        self.s3_restore_url = ensure_url_scheme(args.s3_restore_url) if args.s3_restore_url else ""

    def _target(self, key: str) -> tuple[str, str, str]:
        """Return (scheme, host, canonical_uri) for a bucket-relative key."""
        quoted_key = quote(key, safe="/")
        if self.endpoint_url:
            parsed = urllib.parse.urlparse(self.endpoint_url)
            host = parsed.netloc
            if not host:
                raise RuntimeError(
                    f"Invalid --endpoint-url {self.endpoint_url!r}: missing host. "
                    "Use e.g. http://minio:9000 or https://s3.amazonaws.com"
                )
            return parsed.scheme or "https", host, f"/{self.bucket}/{quoted_key}"
        if self.s3_restore_url:
            parsed = urllib.parse.urlparse(self.s3_restore_url)
            host = parsed.netloc
            # Restore URL is the backup root (includes prefix). GET keys are still
            # bucket-relative (prefix + blob); use host/scheme only, path-style.
            path = parsed.path.rstrip("/")
            # Virtual-hosted: host starts with bucket. Path is /<prefix>.
            if host.startswith(self.bucket + ".") or host.startswith(self.bucket + ".s3"):
                return parsed.scheme or "https", host, f"/{quoted_key}"
            # Path-style restore URL: https://s3.../bucket/prefix  or http://minio/bucket/prefix
            if path == f"/{self.bucket}" or path.startswith(f"/{self.bucket}/"):
                return parsed.scheme or "https", host, f"/{quoted_key}" if quoted_key.startswith(self.bucket + "/") else f"/{self.bucket}/{quoted_key}"
            return parsed.scheme or "https", host, f"{path}/{quoted_key}" if path else f"/{quoted_key}"
        host = f"s3.{self.region}.amazonaws.com"
        return "https", host, f"/{self.bucket}/{quoted_key}"

    def get_bytes(self, bucket: str, key: str) -> bytes:
        self.bucket = bucket
        now = dt.datetime.now(dt.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        payload_hash = "UNSIGNED-PAYLOAD"
        scheme, host, canonical_uri = self._target(key)
        url = f"{scheme}://{host}{canonical_uri}"

        canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = "\n".join(
            [
                "GET",
                canonical_uri,
                "",
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{datestamp}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = hmac.new(
            _sigv4_key(self.secret_key, datestamp, self.region, "s3"),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        req = urllib.request.Request(url, method="GET")
        req.add_header("x-amz-date", amz_date)
        req.add_header("x-amz-content-sha256", payload_hash)
        req.add_header("Authorization", authorization)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 404:
                raise FileNotFoundError(f"{url}: {body.strip()}") from e
            raise RuntimeError(f"S3 GET {url} HTTP {e.code}: {body.strip()}") from e
        except urllib.error.URLError as e:
            hint = ""
            err = str(e.reason) if e.reason else str(e)
            if "WRONG_VERSION_NUMBER" in err or "wrong version number" in err.lower():
                hint = (
                    " HTTPS was used against a server speaking HTTP. "
                    "Pass the same scheme as BACKUP/RESTORE S3(), e.g. "
                    "--endpoint-url http://minio:9000 or --s3-restore-url http://..."
                )
            raise RuntimeError(f"S3 GET {url}: {e}{hint}") from e


def parse_manifest(xml_bytes: bytes) -> Manifest:
    manifest = Manifest()
    for _event, elem in ET.iterparse(io.BytesIO(xml_bytes), events=("end",)):
        tag = elem.tag
        if tag == "data_file_name_generator":
            manifest.data_file_name_generator = (elem.text or "").strip() or manifest.data_file_name_generator
        elif tag == "data_file_name_prefix_length":
            try:
                manifest.data_file_name_prefix_length = int(elem.text or DEFAULT_PREFIX_LENGTH)
            except ValueError:
                pass
        elif tag == "file":
            name = (elem.findtext("name") or "").strip()
            size_text = elem.findtext("size") or "0"
            try:
                size = int(size_text)
            except ValueError:
                size = 0
            checksum = (elem.findtext("checksum") or "").strip().lower()
            data_file = (elem.findtext("data_file") or "").strip()
            use_base = (elem.findtext("use_base") or "").strip().lower() in {"1", "true"}
            object_key = (elem.findtext("object_key") or "").strip()
            manifest.files.append(
                ManifestFile(
                    name=name,
                    size=size,
                    checksum=checksum,
                    data_file=data_file,
                    use_base=use_base,
                    object_key=object_key,
                )
            )
            elem.clear()
        elif tag in {"contents", "config"}:
            elem.clear()
    return manifest


def blob_key(manifest: Manifest, info: ManifestFile) -> str:
    if info.data_file:
        return info.data_file
    gen = manifest.data_file_name_generator.lower()
    if gen == "checksum" and info.checksum:
        n = manifest.data_file_name_prefix_length
        c = info.checksum
        if 0 < n < len(c):
            return f"{c[:n]}/{c[n:]}"
        return c
    return info.name


def is_metadata_sql(name: str) -> bool:
    return name.startswith("metadata/") and name.endswith(".sql")


def metadata_kind(name: str) -> str:
    """'database' if metadata/<db>.sql, 'table' if metadata/<db>/<table>.sql."""
    rel = name[len("metadata/") :]
    parts = rel.split("/")
    if len(parts) == 1:
        return "database"
    return "table"


def logical_names(name: str) -> tuple[str, str]:
    rel = name[len("metadata/") : -len(".sql")]
    parts = rel.split("/")
    db = unquote(parts[0])
    table = unquote(parts[1]) if len(parts) > 1 else ""
    return db, table


def rewrite_schema(sql: str, kind: str, args: argparse.Namespace) -> str:
    out = sql
    if kind == "table":
        out = SHARED_MERGETREE_RE.sub(r"Replicated\1MergeTree", out)
        args_sql = f"({args.replicated_zk_path}, {args.replicated_replica})"
        out = REPLICATED_MISSING_ARGS_RE.sub(rf"\1{args_sql}", out)
    elif kind == "database":
        out = SHARED_DATABASE_ENGINE_RE.sub(r"\1Atomic", out)
    if CREATE_KIND_RE.search(out) and not IF_NOT_EXISTS_RE.search(out):
        out = CREATE_KIND_RE.sub(lambda m: m.group(0) + " IF NOT EXISTS", out, count=1)
    return out


def apply_order(kind: str, sql: str) -> int:
    m = CREATE_KIND_RE.search(sql)
    token = (m.group(1) if m else kind).upper().replace("  ", " ")
    order = {
        "DATABASE": 0,
        "DICTIONARY": 1,
        "TABLE": 2,
        "VIEW": 3,
        "MATERIALIZED VIEW": 4,
    }
    return order.get(token, 5)


def redact_secrets(text: str, args: argparse.Namespace) -> str:
    secrets = [args.secret_key, args.access_key, args.clickhouse_password]
    out = text
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        out = out.replace(secret, "***")
    return out


def clickhouse_query(
    args: argparse.Namespace,
    sql: str,
    *,
    database: str | None = None,
) -> str:
    params = {
        "user": args.clickhouse_user,
        "password": args.clickhouse_password,
        "database": database or args.clickhouse_database,
    }
    url = args.clickhouse_url.rstrip("/") + "/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"ClickHouse HTTP {e.code}: {redact_secrets(body.strip(), args)}\n"
            f"--- SQL ---\n{redact_secrets(sql, args)}"
        ) from None


def default_s3_restore_url(args: argparse.Namespace) -> str:
    if args.s3_restore_url:
        return args.s3_restore_url.rstrip("/")
    prefix = args.prefix.strip("/")
    if args.endpoint_url:
        return f"{ensure_url_scheme(args.endpoint_url).rstrip('/')}/{args.bucket}/{prefix}"
    return f"https://s3.{args.region}.amazonaws.com/{args.bucket}/{prefix}"


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def restore_table_sql(args: argparse.Namespace, database: str, table: str) -> str:
    url = default_s3_restore_url(args)
    return (
        f"RESTORE TABLE {quote_ident(database)}.{quote_ident(table)} FROM S3("
        f"'{url}', '{args.access_key}', '{args.secret_key}'"
        f") SETTINGS allow_different_database_def=1, allow_different_table_def=1"
    )


def fetch_blob(s3: S3Client, args: argparse.Namespace, info: ManifestFile, blob: str) -> bytes:
    prefixes: list[str] = []
    if info.use_base and args.base_prefix:
        prefixes.append(args.base_prefix)
        prefixes.append(args.prefix)
    else:
        prefixes.append(args.prefix)
        if args.base_prefix:
            prefixes.append(args.base_prefix)
    last_error: Exception | None = None
    for prefix in prefixes:
        key = join_key(prefix, blob)
        try:
            return s3.get_bytes(args.bucket, key)
        except FileNotFoundError as e:
            last_error = e
            continue
    raise FileNotFoundError(
        f"blob {blob!r} for {info.name!r} not found under {prefixes}: {last_error}"
    )


def group_metadata(manifest: Manifest) -> dict[str, tuple[ManifestFile | None, list[ManifestFile]]]:
    """Map database -> (database.sql entry or None, table/view/dict .sql entries)."""
    grouped: dict[str, tuple[ManifestFile | None, list[ManifestFile]]] = {}
    for info in manifest.files:
        if not is_metadata_sql(info.name):
            continue
        db, _table = logical_names(info.name)
        if db not in grouped:
            grouped[db] = (None, [])
        db_info, tables = grouped[db]
        if metadata_kind(info.name) == "database":
            grouped[db] = (info, tables)
        else:
            tables.append(info)
    return grouped


def backup_data_sizes(manifest: Manifest) -> dict[tuple[str, str], int]:
    """Logical bytes under data/<db>/<table>/ in the manifest (part files)."""
    sizes: dict[tuple[str, str], int] = defaultdict(int)
    for info in manifest.files:
        if not info.name.startswith("data/"):
            continue
        parts = info.name.split("/")
        if len(parts) < 3:
            continue
        db = unquote(parts[1])
        table = unquote(parts[2])
        sizes[(db, table)] += info.size
    return sizes


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def restored_table_stats(args: argparse.Namespace, database: str, table: str) -> tuple[int, int, int]:
    """Return (bytes_on_disk, rows, parts) from system.parts."""
    q = (
        "SELECT "
        "coalesce(sum(bytes_on_disk), 0), "
        "coalesce(sum(rows), 0), "
        "count() "
        "FROM system.parts "
        f"WHERE database = {sql_string(database)} AND table = {sql_string(table)} "
        "AND active "
        "FORMAT TabSeparated"
    )
    out = clickhouse_query(args, q).strip()
    if not out:
        return 0, 0, 0
    cols = out.split("\t")
    return int(cols[0]), int(cols[1]), int(cols[2])


def check_restored_size(
    args: argparse.Namespace,
    database: str,
    table: str,
    backup_bytes: int,
    create_sql: str,
) -> None:
    if args.dry_run or args.skip_apply:
        print(
            f"size check skipped: backup data/{database}/{table}/ = {backup_bytes} bytes",
            file=sys.stderr,
        )
        return
    is_view = bool(re.search(r"\bCREATE\s+(MATERIALIZED\s+)?VIEW\b", create_sql, re.IGNORECASE))
    is_dict = bool(re.search(r"\bCREATE\s+DICTIONARY\b", create_sql, re.IGNORECASE))
    if is_view or is_dict:
        print(f"size check skipped for {database}.{table} (view/dictionary)", file=sys.stderr)
        return

    restored_bytes, restored_rows, restored_parts = restored_table_stats(args, database, table)
    print(
        f"size check {database}.{table}: "
        f"backup_bytes={backup_bytes} restored_bytes={restored_bytes} "
        f"rows={restored_rows} parts={restored_parts}",
        file=sys.stderr,
    )
    if backup_bytes > 0 and restored_bytes == 0:
        raise RuntimeError(
            f"{database}.{table}: backup has {backup_bytes} bytes of data files "
            f"but restored table is empty (0 bytes, {restored_rows} rows, {restored_parts} parts)"
        )
    if backup_bytes > 0 and restored_rows == 0:
        raise RuntimeError(
            f"{database}.{table}: backup has {backup_bytes} bytes of data files "
            f"but restored table has 0 rows ({restored_bytes} bytes_on_disk, {restored_parts} parts)"
        )
    # bytes_on_disk is usually close to the sum of part files in the backup, not identical.
    if backup_bytes and restored_bytes:
        delta = abs(backup_bytes - restored_bytes)
        limit = max(4096, int(backup_bytes * 0.05))
        if delta > limit:
            raise RuntimeError(
                f"{database}.{table}: size mismatch backup_bytes={backup_bytes} "
                f"restored_bytes={restored_bytes} delta={delta} (limit {limit})"
            )


def load_rewritten(
    s3: S3Client,
    args: argparse.Namespace,
    manifest: Manifest,
    info: ManifestFile,
    kind: str,
) -> str | None:
    if info.size == 0:
        print(f"skip empty {info.name}", file=sys.stderr)
        return None
    blob = blob_key(manifest, info)
    print(f"fetch {info.name} -> {blob} ({info.size} bytes)", file=sys.stderr)
    raw = fetch_blob(s3, args, info, blob)
    return rewrite_schema(raw.decode("utf-8"), kind, args)


def run_sql(args: argparse.Namespace, sql: str, what: str) -> None:
    print(f"=== {what} ===")
    print(redact_secrets(sql.rstrip(), args) + "\n")
    if args.dry_run or args.skip_apply:
        return
    print(redact_secrets(clickhouse_query(args, sql), args))


def main() -> int:
    args = parse_args()
    s3 = S3Client(args)
    backup_key = join_key(args.prefix, ".backup")
    print(f"Reading s3://{args.bucket}/{backup_key}", file=sys.stderr)
    xml_bytes = s3.get_bytes(args.bucket, backup_key)
    manifest = parse_manifest(xml_bytes)
    print(
        f"Manifest generator={manifest.data_file_name_generator} "
        f"prefix_length={manifest.data_file_name_prefix_length} "
        f"files={len(manifest.files)}",
        file=sys.stderr,
    )

    only_dbs = set(args.database)
    only_tables = set(args.table)
    grouped = group_metadata(manifest)
    data_bytes = backup_data_sizes(manifest)
    errors = 0

    for db in sorted(grouped):
        if db in SKIP_DATABASES:
            continue
        if only_dbs and db not in only_dbs:
            continue
        db_info, table_infos = grouped[db]
        print(f"\n######## database {db} ########", file=sys.stderr)

        if db_info is not None:
            try:
                sql = load_rewritten(s3, args, manifest, db_info, "database")
                if sql:
                    run_sql(args, sql, f"database {db}")
            except Exception as e:
                errors += 1
                print(f"ERROR database {db}: {redact_secrets(str(e), args)}", file=sys.stderr)
                if not args.continue_on_error:
                    raise
                continue

        loaded: list[tuple[str, str]] = []
        for info in table_infos:
            _db, table = logical_names(info.name)
            if only_tables and table not in only_tables:
                continue
            if args.skip_empty_tables and data_bytes.get((db, table), 0) == 0:
                print(f"skip empty {db}.{table} (no data files in backup)", file=sys.stderr)
                continue
            try:
                sql = load_rewritten(s3, args, manifest, info, "table")
            except Exception as e:
                errors += 1
                print(f"ERROR fetch {db}.{table}: {redact_secrets(str(e), args)}", file=sys.stderr)
                if not args.continue_on_error:
                    raise
                continue
            if sql:
                loaded.append((table, sql))

        loaded.sort(key=lambda row: (apply_order("table", row[1]), row[0]))
        for table, sql in loaded:
            label = f"{db}.{table}"
            try:
                run_sql(args, sql, f"table {label}")
                q = restore_table_sql(args, db, table)
                run_sql(args, q, f"RESTORE TABLE {label}")
                check_restored_size(args, db, table, data_bytes.get((db, table), 0), sql)
            except Exception as e:
                errors += 1
                print(f"ERROR {label}: {redact_secrets(str(e), args)}", file=sys.stderr)
                if not args.continue_on_error:
                    raise

    if errors:
        print(f"Finished with {errors} error(s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
