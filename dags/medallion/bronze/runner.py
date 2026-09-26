"""Jalankan ingestion bronze untuk SATU tabel dengan dlt.

Alur (target=bigquery):
    SQL Server --(dlt sql_table, pyarrow)--> parquet/jsonl.gz di GCS (data lake, disimpan permanen)
               --(BigQuery load job dari GCS)--> tabel bronze di BigQuery (append/replace/merge)

Mode window:
    * full        -> load: full, tabel di-replace tiap run
    * incremental -> pakai state dlt (last_value) dikurangi lookback (lag). Run terlewat tidak bikin bolong.
    * backfill    -> window [start, end) eksplisit (param DAG / Airflow backfill / CLI). State TIDAK diubah.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from medallion.config import (
    INGESTED_AT,
    SKIP_EXIT_CODE,
    ConfigError,
    Platform,
    Source,
    Table,
    load_platform,
    load_source,
    normalize_identifier,
)

log = logging.getLogger("medallion.bronze")

TARGETS = ("bigquery", "local", "duckdb")

# Default config dlt (bisa di-override lewat env var dengan nama yang sama)
DLT_ENV_DEFAULTS = {
    "RUNTIME__DLTHUB_TELEMETRY": "false",
    # hapus file lokal setelah sukses load (GCS tetap menyimpan salinannya)
    "LOAD__DELETE_COMPLETED_JOBS": "true",
    # tambahkan kolom _dlt_load_id juga untuk data arrow (lineage ke tabel _dlt_loads & file di GCS)
    "NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID": "true",
    # rotasi file tiap ~256MB (BigQuery membatasi file JSON gzip maks 4GB per file)
    "DATA_WRITER__FILE_MAX_BYTES": str(256 * 1024 * 1024),
}


class SkipTable(Exception):
    """Tabel sengaja dilewati (Airflow: task jadi 'skipped', CLI: exit 0)."""


@dataclass(frozen=True)
class Window:
    mode: str  # full | incremental | backfill
    start: Any = None
    end: Any = None
    reason: str = ""


# ---------------------------------------------------------------------------
# Window resolution (pure function -> mudah di-unit test)
# ---------------------------------------------------------------------------
def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() in ("", "None", "null"))


def _run_type(run_ctx: dict[str, Any]) -> str:
    # Airflow 3 merender "backfill" / "scheduled" / "manual" / "asset_triggered"
    value = str(run_ctx.get("run_type") or "").strip().lower()
    return value.rsplit(".", 1)[-1]


def _to_cursor_value(moment: datetime, table: Table, tz: str) -> Any:
    """Konversi datetime (aware/naive) ke tipe cursor yang sama dengan initial_value."""
    sample = table.initial_value
    zone = ZoneInfo(tz)
    if isinstance(sample, datetime):
        if sample.tzinfo is None:
            # kolom datetime/datetime2 SQL Server = jam dinding sumber (naive)
            return moment.astimezone(zone).replace(tzinfo=None) if moment.tzinfo else moment
        aware = moment if moment.tzinfo else moment.replace(tzinfo=zone)
        return aware.astimezone(timezone.utc)
    if isinstance(sample, date):
        return (moment.astimezone(zone) if moment.tzinfo else moment).date()
    raise ConfigError(
        f"{table.table}: backfill window hanya untuk cursor datetime/date (initial_value={sample!r})"
    )


def _parse_bound(text: str, table: Table, tz: str, *, is_end: bool) -> Any:
    raw = text.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        day = date.fromisoformat(raw)
        if is_end:  # tanggal end inklusif -> batas eksklusif = besoknya 00:00
            day += timedelta(days=1)
        moment = datetime(day.year, day.month, day.day)
    else:
        moment = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return _to_cursor_value(moment, table, tz)


def backfill_requested(run_ctx: dict[str, Any] | None) -> bool:
    run_ctx = run_ctx or {}
    return (
        not _blank(run_ctx.get("backfill_start"))
        or not _blank(run_ctx.get("backfill_end"))
        or _run_type(run_ctx) == "backfill"
    )


def resolve_window(table: Table, source: Source, run_ctx: dict[str, Any] | None) -> Window:
    run_ctx = run_ctx or {}
    if not table.is_incremental:
        if backfill_requested(run_ctx):
            # window tidak berarti apa-apa untuk load full; jangan replace berulang tiap run backfill
            raise SkipTable(f"{table.table}: load full tidak ikut backfill (sudah di-load penuh tiap run terjadwal)")
        return Window("full", reason="load full -> replace")

    start_raw, end_raw = run_ctx.get("backfill_start"), run_ctx.get("backfill_end")
    if not _blank(start_raw) or not _blank(end_raw):
        if _blank(start_raw) or _blank(end_raw):
            raise ConfigError("Backfill butuh backfill_start DAN backfill_end")
        start = _parse_bound(str(start_raw), table, source.source_timezone, is_end=False)
        end = _parse_bound(str(end_raw), table, source.source_timezone, is_end=True)
        reason = "param backfill_start/backfill_end"
    elif _run_type(run_ctx) == "backfill":
        dis, die = run_ctx.get("data_interval_start"), run_ctx.get("data_interval_end")
        if _blank(dis) or _blank(die):
            raise ConfigError("Run backfill tanpa data interval")
        start = _to_cursor_value(datetime.fromisoformat(str(dis)), table, source.source_timezone)
        end = _to_cursor_value(datetime.fromisoformat(str(die)), table, source.source_timezone)
        reason = "Airflow backfill (data interval)"
    else:
        return Window("incremental", reason="state dlt + lookback")

    if start >= end:
        raise ConfigError(f"Window backfill kosong/terbalik: {start} >= {end}")
    return Window("backfill", start=start, end=end, reason=reason)


def lag_for(table: Table) -> float | None:
    """lookback -> parameter ``lag`` dlt (detik untuk datetime, hari untuk date, unit untuk angka)."""
    sample = table.initial_value
    if isinstance(sample, datetime):
        seconds = table.lookback_hours * 3600
        return seconds or None
    if isinstance(sample, date):
        days = math.ceil(table.lookback_hours / 24) if table.lookback_hours else 0
        return days or None
    return table.lookback_units or None


# ---------------------------------------------------------------------------
# dlt building blocks
# ---------------------------------------------------------------------------
class _QuietOwnHints(logging.Filter):
    """dlt mengeluarkan WARNING untuk hint kolom yang bukan kolom asli tabel sumber
    (_ingested_at & nama cluster snake_case yang kita tambahkan sendiri). Hint-nya tetap
    dipakai untuk CREATE TABLE BigQuery, jadi warning ini aman dan hanya bikin bingung."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "do not match any reflected source columns" not in record.getMessage()


def _apply_dlt_env_defaults() -> None:
    for key, value in DLT_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)
    dlt_logger = logging.getLogger("dlt")
    if not any(isinstance(f, _QuietOwnHints) for f in dlt_logger.filters):
        dlt_logger.addFilter(_QuietOwnHints())


def _add_ingested_at(ts: datetime):
    """Tambah kolom _ingested_at (UTC, waktu run). Dipakai untuk partisi bronze & incremental silver."""

    def _map(item: Any) -> Any:
        import pyarrow as pa

        if isinstance(item, pa.Table):
            if INGESTED_AT in item.column_names:
                return item
            return item.append_column(INGESTED_AT, pa.array([ts] * item.num_rows, type=pa.timestamp("us", tz="UTC")))
        if hasattr(item, "assign") and hasattr(item, "columns"):  # pandas DataFrame
            return item.assign(**{INGESTED_AT: ts})
        if isinstance(item, dict):
            item[INGESTED_AT] = ts
        return item

    return _map


def build_incremental(table: Table, window: Window):
    import dlt

    if window.mode == "backfill":
        return dlt.sources.incremental(
            table.cursor,
            initial_value=window.start,
            end_value=window.end,
            range_start="closed",
            range_end="open",
            on_cursor_value_missing=table.on_cursor_null,
        )
    return dlt.sources.incremental(
        table.cursor,
        initial_value=table.initial_value,
        lag=lag_for(table),
        on_cursor_value_missing=table.on_cursor_null,
    )


def build_resource(source: Source, table: Table, window: Window, engine: Any, target: str, run_started_at: datetime):
    from dlt.sources.sql_database import sql_table

    from medallion.bronze.adapters import mssql_query_adapter, mssql_type_adapter, nullable_table_adapter

    is_mssql = source.type == "mssql"
    kwargs: dict[str, Any] = dict(
        credentials=engine,
        table=table.db_table,
        schema=table.db_schema,
        backend=table.backend,
        chunk_size=table.chunk_size,
        reflection_level="full",
        table_adapter_callback=nullable_table_adapter,
        type_adapter_callback=mssql_type_adapter if is_mssql else None,
        query_adapter_callback=mssql_query_adapter if is_mssql else None,
        included_columns=list(table.included_columns) or None,
        excluded_columns=list(table.excluded_columns) or None,
        write_disposition=table.write_disposition,
    )
    if table.primary_key:
        kwargs["primary_key"] = list(table.primary_key)
    if table.is_incremental:
        kwargs["incremental"] = build_incremental(table, window)

    resource = sql_table(**kwargs)
    resource.apply_hints(table_name=table.target_table)
    resource.add_map(_add_ingested_at(run_started_at))

    if target == "bigquery" and (table.partition_by or table.cluster_by):
        from dlt.destinations.adapters import bigquery_adapter

        from dlt.destinations.impl.bigquery.bigquery_adapter import CLUSTER_COLUMNS_HINT

        # Hint kolom pakai nama ASLI (harus cocok dengan kolom hasil reflection sql_table).
        resource = bigquery_adapter(
            resource,
            partition=table.partition_by,
            cluster=list(table.cluster_by) or None,
        )
        if table.cluster_by:
            # ...tapi hint table-level CLUSTER BY TIDAK dinormalisasi dlt -> "CustomerID" bikin
            # CREATE TABLE gagal (kolom BigQuery-nya customer_id). Timpa dengan nama snake_case.
            resource.apply_hints(
                additional_table_hints={CLUSTER_COLUMNS_HINT: [normalize_identifier(c) for c in table.cluster_by]}
            )
    return resource


def check_null_cursor(engine: Any, table: Table) -> None:
    """``on_cursor_null: raise``: gagal kalau ada baris dengan cursor NULL.

    Tanpa cek ini baris tsb hilang diam-diam: query dlt ``WHERE cursor >= :last_value``
    tidak pernah mengembalikan NULL.
    """
    import sqlalchemy as sa

    cursor_col = sa.column(table.cursor)
    stmt = sa.select(sa.func.count()).select_from(
        sa.table(table.db_table, cursor_col, schema=table.db_schema)
    ).where(cursor_col.is_(None))
    with engine.connect() as conn:
        nulls = conn.execute(stmt).scalar() or 0
    if nulls:
        raise ConfigError(
            f"{table.table}: {nulls} baris punya {table.cursor} NULL dan tidak akan pernah ter-load incremental. "
            "Set on_cursor_null: include (ikut diambil tiap run) atau exclude (sengaja diabaikan) di YAML."
        )


def local_dir() -> Path:
    return Path(os.environ.get("MEDALLION_LOCAL_DIR", ".local")).resolve()


def build_pipeline(source: Source, table: Table, platform: Platform, target: str):
    import dlt

    if target == "bigquery":
        destination = dlt.destinations.bigquery(
            location=platform.bq_location,
            project_id=platform.bq_project,
            # default dlt = True (file lama per tabel DIHAPUS sebelum load). Kita mau data lake -> simpan.
            truncate_tables_on_staging_destination_before_load=not platform.keep_lake_files,
        )
        staging = dlt.destinations.filesystem(bucket_url=platform.lake_bucket_url, layout=platform.lake_layout)
        pipeline_name = table.pipeline_name
    elif target == "local":
        destination = dlt.destinations.filesystem(
            bucket_url=(local_dir() / "lake").as_uri(), layout=platform.lake_layout
        )
        staging = None
        pipeline_name = f"{table.pipeline_name}__local"  # state terpisah dari production!
    elif target == "duckdb":
        local_dir().mkdir(parents=True, exist_ok=True)
        destination = dlt.destinations.duckdb(str(local_dir() / "bronze.duckdb"))
        staging = None
        pipeline_name = f"{table.pipeline_name}__duckdb"
    else:
        raise ConfigError(f"target harus salah satu {TARGETS}")

    return dlt.pipeline(
        pipeline_name=pipeline_name,
        pipelines_dir=os.environ.get("MEDALLION_DLT_PIPELINES_DIR") or None,
        destination=destination,
        staging=staging,
        dataset_name=source.bronze_dataset,
        progress=dlt.progress.log(log_period=60, logger=sys.stdout, dump_system_stats=False),
    )


def _last_cursor_value(pipeline: Any, table: Table) -> str | None:
    try:
        resources = pipeline.state["sources"][pipeline.default_schema_name]["resources"]
        for res_state in resources.values():
            inc = (res_state.get("incremental") or {}).get(table.cursor)
            if inc:
                return str(inc.get("last_value"))
    except Exception:  # pragma: no cover - hanya untuk ringkasan
        return None
    return None


# ---------------------------------------------------------------------------
# Public entrypoints
# ---------------------------------------------------------------------------
def run_table(
    source_name: str,
    table_name: str,
    *,
    run_ctx: dict[str, Any] | None = None,
    db_conn: dict[str, Any] | None = None,
    target: str | None = None,
    refresh: str | None = None,
) -> dict[str, Any]:
    import sqlalchemy as sa

    from medallion.bronze.connections import resolve_credentials

    _apply_dlt_env_defaults()
    target = target or os.environ.get("MEDALLION_TARGET", "bigquery")
    platform = load_platform()
    source = load_source(source_name)
    table = source.get_table(table_name)
    window = resolve_window(table, source, run_ctx)
    run_started_at = datetime.now(timezone.utc)

    log.info(
        "bronze %s.%s -> %s.%s | target=%s mode=%s (%s) start=%s end=%s lag=%s",
        source.name, table.table, source.bronze_dataset, table.target_table, target,
        window.mode, window.reason, window.start, window.end,
        lag_for(table) if window.mode == "incremental" else None,
    )

    engine = sa.create_engine(resolve_credentials(source, db_conn), pool_pre_ping=True)
    started = time.monotonic()
    try:
        if window.mode == "incremental" and table.on_cursor_null == "raise":
            check_null_cursor(engine, table)
        pipeline = build_pipeline(source, table, platform, target)
        if pipeline.has_pending_data:
            # Run sebelumnya gagal setelah extract. pipeline.run(data) dlt akan me-load paket lama
            # lalu BERHENTI tanpa extract data baru -> selesaikan paket lama dulu secara eksplisit.
            # (Kalau dataset tujuan belum pernah dibuat, dlt membuang paket & state lokal lalu mulai
            # dari awal -> juga aman karena memang belum ada yang ter-load.)
            log.warning("Ada load package tertunda dari run sebelumnya -> diselesaikan dulu")
            pipeline.run()
        resource = build_resource(source, table, window, engine, target, run_started_at)
        load_info = pipeline.run(resource, loader_file_format=source.file_format, refresh=refresh)
    finally:
        engine.dispose()

    row_counts = pipeline.last_trace.last_normalize_info.row_counts if pipeline.last_trace.last_normalize_info else {}
    rows = int(row_counts.get(table.target_table, 0))
    summary = {
        "source": source.name,
        "table": table.table,
        "target": target,
        "pipeline": pipeline.pipeline_name,
        "destination_table": (
            f"{platform.bq_project}.{source.bronze_dataset}.{table.target_table}"
            if target == "bigquery" else f"{source.bronze_dataset}.{table.target_table}"
        ),
        "lake_prefix": {
            "bigquery": f"{platform.lake_bucket_url}/{source.bronze_dataset}/{table.target_table}/",
            "local": str(local_dir() / "lake" / source.bronze_dataset / table.target_table),
        }.get(target),
        "write_disposition": table.write_disposition,
        "mode": window.mode,
        "window_start": None if window.start is None else str(window.start),
        "window_end": None if window.end is None else str(window.end),
        "rows": rows,
        "load_ids": list(load_info.loads_ids),
        "last_cursor_value": _last_cursor_value(pipeline, table) if table.is_incremental else None,
        "ingested_at": run_started_at.isoformat(),
        "duration_s": round(time.monotonic() - started, 1),
    }
    log.info("selesai: %s", json.dumps(summary, default=str))
    print(load_info)
    return summary


def run_from_airflow(
    source_name: str, table_name: str, run_ctx: dict[str, Any], db_conn: dict[str, Any]
) -> dict[str, Any]:
    """Dipanggil dari ExternalPythonOperator (lihat medallion/airflow/bronze_factory.py)."""
    # timestamp sudah ditambahkan Airflow di log task -> format ringkas saja
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(levelname)s %(name)s: %(message)s")
    only = run_ctx.get("only_tables")
    if isinstance(only, str):
        only = json.loads(only) if only.strip() not in ("", "None", "null") else []
    if only:
        source = load_source(source_name)
        table = source.get_table(table_name)
        wanted = {str(t).strip().lower() for t in only}
        if not wanted & {table.table.lower(), table.target_table.lower(), table.db_table.lower()}:
            print(f"Tabel {table.table} tidak ada di param 'tables' {sorted(wanted)} -> skip")
            sys.exit(SKIP_EXIT_CODE)
    try:
        return run_table(source_name, table_name, run_ctx=run_ctx, db_conn=db_conn)
    except SkipTable as exc:
        print(f"{exc} -> skip")
        sys.exit(SKIP_EXIT_CODE)
