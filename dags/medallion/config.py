"""Loader + validator untuk semua file YAML di ``dags/configs/``.

PENTING: modul ini hanya boleh import stdlib + PyYAML (tidak boleh airflow / dlt),
karena dipakai di dua tempat:

* DAG processor Airflow  -> untuk generate DAG dari YAML
* venv dlt (/opt/dlt-venv) -> untuk menjalankan ingestion satu tabel

Semua error konfigurasi dilempar sebagai ``ConfigError`` dengan pesan yang jelas,
supaya kalau ada typo di YAML langsung kelihatan sebagai "DAG import error" di UI Airflow.
"""

from __future__ import annotations

import difflib
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import yaml

PACKAGE_DIR = Path(__file__).resolve().parent
DAGS_DIR = PACKAGE_DIR.parent


def config_dir() -> Path:
    return Path(os.environ.get("MEDALLION_CONFIG_DIR", DAGS_DIR / "configs"))


def sql_dir() -> Path:
    return Path(os.environ.get("MEDALLION_SQL_DIR", DAGS_DIR / "sql"))


class ConfigError(ValueError):
    """Konfigurasi YAML tidak valid."""


# ---------------------------------------------------------------------------
# Normalisasi nama (harus IDENTIK dengan naming convention "snake_case" milik dlt,
# karena nama tabel/kolom di BigQuery dibuat oleh dlt). Ada unit test yang
# membandingkan fungsi ini dengan implementasi dlt asli.
# ---------------------------------------------------------------------------
_BREAK_1 = re.compile(r"([^_])([A-Z][a-z]+)")
_BREAK_2 = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM = re.compile(r"[^a-zA-Z\d_]+")
_UNDERSCORES = re.compile(r"__+")
_LEADING_DIGITS = re.compile(r"^\d+")
_REDUCE_ALPHABET = str.maketrans("+-*@|", "x_xal")


def normalize_identifier(identifier: str) -> str:
    """``CustomerID`` -> ``customer_id``, ``SaldoStokProdukToko`` -> ``saldo_stok_produk_toko``."""
    ident = (identifier or "").strip()
    if not ident:
        raise ConfigError("Identifier tidak boleh kosong")
    ident = ident.translate(_REDUCE_ALPHABET)
    ident = _NON_ALNUM.sub("_", ident)
    ident = _BREAK_1.sub(r"\1_\2", ident)
    ident = _BREAK_2.sub(r"\1_\2", ident).lower()
    if _LEADING_DIGITS.match(ident):
        ident = "_" + ident
    stripped = ident.rstrip("_")
    ident = stripped + "x" * (len(ident) - len(stripped))
    return _UNDERSCORES.sub("_", ident)


def normalize_path(*parts: str) -> str:
    """Sama dengan ``NamingConvention.normalize_path`` dlt: ``__`` adalah separator."""
    joined = "__".join(parts)
    return "__".join(normalize_identifier(p) for p in joined.split("__") if p.strip())


def label_value(value: str) -> str:
    """Nilai label BigQuery: lowercase, [a-z0-9_-], maks 63 karakter."""
    return re.sub(r"[^a-z0-9_-]+", "_", str(value).lower())[:63]


# ---------------------------------------------------------------------------
# Helper parsing
# ---------------------------------------------------------------------------
def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - pesan dari PyYAML sudah jelas
        raise ConfigError(f"{path.name}: YAML tidak valid -> {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name}: isi file harus berupa mapping (key: value)")
    return data


def _check_keys(data: dict[str, Any], allowed: Iterable[str], where: str) -> None:
    allowed = set(allowed)
    unknown = sorted(set(data) - allowed)
    if unknown:
        hints = []
        for key in unknown:
            close = difflib.get_close_matches(key, sorted(allowed), n=1)
            hints.append(f"'{key}'" + (f" (maksudnya '{close[0]}'?)" if close else ""))
        raise ConfigError(f"{where}: key tidak dikenal: {', '.join(hints)}")


def _require(data: dict[str, Any], key: str, where: str) -> Any:
    value = data.get(key)
    if value is None or value == "":
        raise ConfigError(f"{where}: '{key}' wajib diisi")
    return value


def _as_list(value: Any, where: str, key: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    raise ConfigError(f"{where}: '{key}' harus string atau list")


def _as_date(value: Any, where: str, key: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ConfigError(f"{where}: '{key}' harus format YYYY-MM-DD, dapat {value!r}") from exc


def parse_cursor_value(value: Any, where: str = "initial_value") -> Any:
    """Parse ``initial_value`` cursor incremental.

    * int                        -> cursor numerik (mis. IDENTITY / rowid)
    * "2026-09-01"               -> date (untuk kolom DATE)
    * "2026-09-01T00:00:00"      -> datetime naive  (kolom datetime/datetime2 SQL Server)
    * "2026-09-01T00:00:00+07:00"-> datetime aware  (kolom datetimeoffset)
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ConfigError(f"{where}: boolean tidak bisa jadi cursor")
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return date.fromisoformat(text)
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConfigError(f"{where}: tidak bisa parse {value!r} (pakai ISO 8601)") from exc


# ---------------------------------------------------------------------------
# Platform (configs/platform.yml)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Platform:
    bq_project: str
    bq_location: str
    gcp_conn_id: str
    lake_bucket_url: str
    lake_layout: str
    keep_lake_files: bool
    timezone: str
    dlt_python: str
    owner: str
    retries: int
    retry_delay_minutes: int
    email_on_failure: tuple[str, ...]


_PLATFORM_KEYS = {"gcp", "lake", "runtime", "defaults"}


def _parse_platform(data: dict[str, Any]) -> Platform:
    where = "platform.yml"
    _check_keys(data, _PLATFORM_KEYS, where)
    gcp = data.get("gcp") or {}
    lake = data.get("lake") or {}
    runtime = data.get("runtime") or {}
    defaults = data.get("defaults") or {}
    _check_keys(gcp, {"bq_project", "bq_location", "gcp_conn_id"}, f"{where} > gcp")
    _check_keys(lake, {"bucket_url", "layout", "keep_files"}, f"{where} > lake")
    _check_keys(runtime, {"timezone", "dlt_python"}, f"{where} > runtime")
    _check_keys(defaults, {"owner", "retries", "retry_delay_minutes", "email_on_failure"}, f"{where} > defaults")

    bucket_url = str(_require(lake, "bucket_url", f"{where} > lake")).rstrip("/")
    if not re.match(r"^(gs|file)://", bucket_url):
        raise ConfigError(f"{where} > lake.bucket_url harus diawali gs:// (atau file:// untuk test lokal)")
    layout = str(lake.get("layout") or "{table_name}/{YYYY}/{MM}/{DD}/{load_id}.{file_id}.{ext}")
    for placeholder in ("{table_name}", "{load_id}", "{file_id}", "{ext}"):
        if placeholder not in layout:
            raise ConfigError(f"{where} > lake.layout wajib mengandung {placeholder}")

    return Platform(
        bq_project=str(_require(gcp, "bq_project", f"{where} > gcp")),
        bq_location=str(_require(gcp, "bq_location", f"{where} > gcp")),
        gcp_conn_id=str(gcp.get("gcp_conn_id") or "google_cloud_default"),
        lake_bucket_url=bucket_url,
        lake_layout=layout,
        keep_lake_files=bool(lake.get("keep_files", True)),
        timezone=str(runtime.get("timezone") or "Asia/Jakarta"),
        # env var menang, supaya test lokal bisa pakai python lain tanpa ubah YAML
        dlt_python=os.environ.get("MEDALLION_DLT_PYTHON") or str(runtime.get("dlt_python") or "/opt/dlt-venv/bin/python"),
        owner=str(defaults.get("owner") or "data-engineering"),
        retries=int(defaults.get("retries", 2)),
        retry_delay_minutes=int(defaults.get("retry_delay_minutes", 5)),
        email_on_failure=tuple(_as_list(defaults.get("email_on_failure"), where, "email_on_failure")),
    )


# ---------------------------------------------------------------------------
# Source + tabel bronze (configs/sources/*.yml)
# ---------------------------------------------------------------------------
LOAD_TYPES = ("incremental", "full")
WRITE_DISPOSITIONS = ("append", "replace", "merge")
FILE_FORMATS = ("parquet", "jsonl")
BACKENDS = ("pyarrow", "sqlalchemy", "pandas", "connectorx")
SOURCE_TYPES = ("mssql", "postgres", "sqlalchemy")
TABLE_NAMINGS = ("schema__table", "table")
INGESTED_AT = "_ingested_at"
SKIP_EXIT_CODE = 99  # exit code venv dlt -> task Airflow "skipped" (ExternalPythonOperator.skip_on_exit_code)

_TABLE_KEYS = {
    "table", "load", "cursor", "primary_key", "write_disposition", "lookback_hours",
    "lookback_units", "initial_value", "target_table", "cluster_by", "partition_by",
    "included_columns", "excluded_columns", "chunk_size", "backend", "timeout_minutes",
    "enabled", "description", "on_cursor_null",
}
CURSOR_NULL_POLICIES = ("raise", "include", "exclude")
_TABLE_DEFAULT_KEYS = {
    "lookback_hours", "lookback_units", "initial_value", "chunk_size", "backend",
    "timeout_minutes", "write_disposition",
}
_SOURCE_KEYS = {
    "name", "description", "type", "airflow_conn_id", "source_timezone", "schedule",
    "start_date", "max_active_tasks", "bronze_dataset", "file_format", "table_naming",
    "tags", "defaults", "tables", "enabled", "owner", "pool",
}


@dataclass(frozen=True)
class Table:
    source_name: str
    table: str  # nama asli di DB, mis. "dbo.Customer"
    db_schema: str | None
    db_table: str
    load: str
    write_disposition: str
    cursor: str | None
    primary_key: tuple[str, ...]
    lookback_hours: float
    lookback_units: float
    initial_value: Any
    target_table: str  # nama tabel di BigQuery (sudah dinormalisasi), mis. "dbo__customer"
    cluster_by: tuple[str, ...]
    partition_by: str | None
    included_columns: tuple[str, ...]
    excluded_columns: tuple[str, ...]
    chunk_size: int
    backend: str
    timeout_minutes: int
    description: str
    on_cursor_null: str = "raise"

    @property
    def task_id(self) -> str:
        return self.target_table

    @property
    def pipeline_name(self) -> str:
        # 1 pipeline dlt per tabel -> state & schema terisolasi, aman jalan paralel
        return f"{self.source_name}__{self.target_table}"

    @property
    def is_incremental(self) -> bool:
        return self.load == "incremental"


@dataclass(frozen=True)
class Source:
    name: str
    description: str
    type: str
    airflow_conn_id: str
    source_timezone: str
    schedule: str | None
    start_date: date
    max_active_tasks: int
    bronze_dataset: str
    file_format: str
    tags: tuple[str, ...]
    owner: str | None
    tables: tuple[Table, ...] = field(default_factory=tuple)
    pool: str | None = None

    @property
    def dag_id(self) -> str:
        return f"bronze__{self.name}"

    def get_table(self, name: str) -> Table:
        wanted = name.strip().lower()
        for tbl in self.tables:
            if wanted in (tbl.table.lower(), tbl.target_table.lower(), tbl.db_table.lower()):
                return tbl
        raise ConfigError(
            f"Tabel '{name}' tidak ada (atau enabled: false) di source '{self.name}'. "
            f"Tersedia: {', '.join(t.table for t in self.tables) or '-'}"
        )


def _parse_table(raw: dict[str, Any], src: dict[str, Any], defaults: dict[str, Any], idx: int) -> Table | None:
    where = f"sources/{src['name']}.yml > tables[{idx}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: harus mapping")
    if raw.get("table"):
        where += f" ({raw['table']})"
    _check_keys(raw, _TABLE_KEYS, where)
    if raw.get("enabled", True) is False:
        return None

    table_name = str(_require(raw, "table", where)).strip()
    where = f"sources/{src['name']}.yml > {table_name}"
    if "." in table_name:
        db_schema, db_table = table_name.split(".", 1)
    else:
        db_schema, db_table = None, table_name

    def opt(key: str, fallback: Any = None) -> Any:
        return raw.get(key, defaults.get(key, fallback))

    load = str(_require(raw, "load", where)).lower()
    if load not in LOAD_TYPES:
        raise ConfigError(f"{where}: load harus salah satu {LOAD_TYPES}")

    if load == "incremental":
        write_disposition = raw.get("write_disposition") or defaults.get("write_disposition") or "append"
    else:
        write_disposition = raw.get("write_disposition") or "replace"
    if write_disposition not in WRITE_DISPOSITIONS:
        raise ConfigError(f"{where}: write_disposition harus salah satu {WRITE_DISPOSITIONS}")

    primary_key = tuple(_as_list(raw.get("primary_key"), where, "primary_key"))
    cursor = raw.get("cursor")
    initial_value = parse_cursor_value(opt("initial_value"), f"{where} > initial_value")

    if load == "incremental":
        if not cursor:
            raise ConfigError(f"{where}: 'cursor' wajib untuk load incremental (kolom updated/modified)")
        if initial_value is None:
            raise ConfigError(f"{where}: 'initial_value' wajib untuk load incremental (isi di table atau defaults)")
    elif cursor:
        raise ConfigError(f"{where}: 'cursor' hanya dipakai untuk load incremental")
    if write_disposition == "merge" and not primary_key:
        raise ConfigError(f"{where}: write_disposition merge butuh 'primary_key'")
    if load == "full" and write_disposition != "replace":
        raise ConfigError(f"{where}: load full hanya boleh write_disposition replace")

    naming = src.get("table_naming") or "schema__table"
    if raw.get("target_table"):
        target = normalize_path(str(raw["target_table"]))
    elif naming == "table" or not db_schema:
        target = normalize_path(db_table)
    else:
        target = normalize_path(db_schema, db_table)

    cluster_by = tuple(_as_list(raw.get("cluster_by"), where, "cluster_by")) or primary_key[:4]
    if len(cluster_by) > 4:
        raise ConfigError(f"{where}: BigQuery maksimal 4 kolom cluster")

    backend = str(opt("backend", "pyarrow"))
    if backend not in BACKENDS:
        raise ConfigError(f"{where}: backend harus salah satu {BACKENDS}")

    on_cursor_null = str(raw.get("on_cursor_null") or "raise")
    if on_cursor_null not in CURSOR_NULL_POLICIES:
        raise ConfigError(f"{where}: on_cursor_null harus salah satu {CURSOR_NULL_POLICIES}")

    included = tuple(_as_list(raw.get("included_columns"), where, "included_columns"))
    if included:
        must_have = ([str(cursor)] if cursor else []) + list(primary_key)
        missing = [c for c in must_have if c not in included]
        if missing:
            raise ConfigError(f"{where}: included_columns wajib memuat cursor & primary_key: {missing}")

    return Table(
        source_name=src["name"],
        table=table_name,
        db_schema=db_schema,
        db_table=db_table,
        load=load,
        write_disposition=write_disposition,
        cursor=str(cursor) if cursor else None,
        primary_key=primary_key,
        lookback_hours=float(opt("lookback_hours", 0) or 0),
        lookback_units=float(opt("lookback_units", 0) or 0),
        initial_value=initial_value,
        target_table=target,
        cluster_by=cluster_by,
        partition_by=raw.get("partition_by", INGESTED_AT) or None,
        included_columns=included,
        excluded_columns=tuple(_as_list(raw.get("excluded_columns"), where, "excluded_columns")),
        chunk_size=int(opt("chunk_size", 50_000)),
        backend=backend,
        timeout_minutes=int(opt("timeout_minutes", 120)),
        description=str(raw.get("description") or ""),
        on_cursor_null=on_cursor_null,
    )


def _parse_source(data: dict[str, Any], file_name: str) -> Source | None:
    where = f"sources/{file_name}"
    _check_keys(data, _SOURCE_KEYS, where)
    if data.get("enabled", True) is False:
        return None
    name = str(_require(data, "name", where))
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise ConfigError(f"{where}: name harus snake_case huruf kecil (a-z, 0-9, _), dapat {name!r}")
    if Path(file_name).stem != name:
        raise ConfigError(f"{where}: nama file harus sama dengan name -> ganti jadi sources/{name}.yml")
    src_type = str(data.get("type") or "mssql")
    if src_type not in SOURCE_TYPES:
        raise ConfigError(f"{where}: type harus salah satu {SOURCE_TYPES}")
    file_format = str(data.get("file_format") or "parquet")
    if file_format not in FILE_FORMATS:
        raise ConfigError(f"{where}: file_format harus salah satu {FILE_FORMATS}")
    naming = str(data.get("table_naming") or "schema__table")
    if naming not in TABLE_NAMINGS:
        raise ConfigError(f"{where}: table_naming harus salah satu {TABLE_NAMINGS}")

    defaults = data.get("defaults") or {}
    _check_keys(defaults, _TABLE_DEFAULT_KEYS, f"{where} > defaults")
    raw_tables = data.get("tables") or []
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ConfigError(f"{where}: 'tables' wajib berupa list dan tidak boleh kosong")

    src = {"name": name, "table_naming": naming}
    parsed = [_parse_table(raw, src, defaults, i) for i, raw in enumerate(raw_tables)]
    tables = [t for t in parsed if t is not None]
    seen: dict[str, str] = {}
    for tbl in tables:
        if tbl.target_table in seen:
            raise ConfigError(
                f"{where}: '{tbl.table}' dan '{seen[tbl.target_table]}' sama-sama jadi tabel "
                f"'{tbl.target_table}' di BigQuery. Isi 'target_table' salah satunya."
            )
        seen[tbl.target_table] = tbl.table

    return Source(
        name=name,
        description=str(data.get("description") or ""),
        type=src_type,
        airflow_conn_id=str(data.get("airflow_conn_id") or f"{src_type}_{name}"),
        source_timezone=str(data.get("source_timezone") or "Asia/Jakarta"),
        schedule=data.get("schedule"),
        start_date=_as_date(data.get("start_date") or "2026-01-01", where, "start_date"),
        max_active_tasks=int(data.get("max_active_tasks", 4)),
        bronze_dataset=normalize_identifier(str(data.get("bronze_dataset") or f"bronze_{name}")),
        file_format=file_format,
        tags=tuple(_as_list(data.get("tags"), where, "tags")),
        owner=data.get("owner"),
        tables=tuple(tables),
        pool=data.get("pool"),
    )


# ---------------------------------------------------------------------------
# Transform silver/gold (configs/transforms/*.yml)
# ---------------------------------------------------------------------------
LAYERS = ("silver", "gold")
_REF_RE = re.compile(r"""\bref\(\s*['"]([^'"]+)['"]\s*\)""")
_SOURCE_RE = re.compile(r"""\bsource\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""")
_TRANSFORM_KEYS = {
    "dag_id", "description", "schedule", "start_date", "tags", "datasets", "models",
    "max_active_tasks", "lookback_days", "enabled", "owner",
}
_MODEL_KEYS = {"sql", "table", "timeout_minutes", "labels", "description"}


@dataclass(frozen=True)
class Model:
    id: str  # "silver.customer"
    layer: str
    name: str
    dataset: str
    table: str
    sql_file: str  # relatif ke dags/sql, pakai "/" (untuk Jinja include)
    dag_id: str
    refs: tuple[str, ...]
    sources: tuple[tuple[str, str], ...]
    timeout_minutes: int
    labels: tuple[tuple[str, str], ...]
    description: str

    @property
    def task_id(self) -> str:
        return f"{self.layer}__{self.name}"


@dataclass(frozen=True)
class TransformDag:
    dag_id: str
    description: str
    schedule: str  # "assets" atau cron
    start_date: date
    tags: tuple[str, ...]
    max_active_tasks: int
    lookback_days: int
    owner: str | None
    models: tuple[Model, ...]


def _parse_transform(data: dict[str, Any], file_name: str) -> TransformDag | None:
    where = f"transforms/{file_name}"
    _check_keys(data, _TRANSFORM_KEYS, where)
    if data.get("enabled", True) is False:
        return None
    dag_id = str(data.get("dag_id") or f"transform__{Path(file_name).stem}")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", dag_id):
        raise ConfigError(f"{where}: dag_id hanya boleh huruf, angka, _ . -")
    datasets = data.get("datasets") or {}
    _check_keys(datasets, set(LAYERS), f"{where} > datasets")
    raw_models = data.get("models") or []
    if not raw_models:
        raise ConfigError(f"{where}: 'models' tidak boleh kosong")

    models: list[Model] = []
    for idx, raw in enumerate(raw_models):
        raw = {"sql": raw} if isinstance(raw, str) else raw
        mwhere = f"{where} > models[{idx}]"
        if not isinstance(raw, dict):
            raise ConfigError(f"{mwhere}: harus string path SQL atau mapping")
        _check_keys(raw, _MODEL_KEYS, mwhere)
        sql_rel = str(_require(raw, "sql", mwhere)).replace("\\", "/").lstrip("/")
        sql_path = sql_dir() / sql_rel
        if not sql_path.is_file():
            raise ConfigError(f"{mwhere}: file SQL tidak ditemukan: sql/{sql_rel}")
        layer = sql_rel.split("/", 1)[0]
        if layer not in LAYERS:
            raise ConfigError(f"{mwhere}: file SQL harus di bawah sql/silver/ atau sql/gold/, dapat {sql_rel}")
        if layer not in datasets:
            raise ConfigError(f"{where}: datasets.{layer} wajib diisi (dipakai model {sql_rel})")
        name = normalize_identifier(Path(sql_rel).stem)
        text = sql_path.read_text(encoding="utf-8")
        labels = raw.get("labels") or {}
        models.append(
            Model(
                id=f"{layer}.{name}",
                layer=layer,
                name=name,
                dataset=normalize_identifier(str(datasets[layer])),
                table=normalize_identifier(str(raw.get("table") or name)),
                sql_file=sql_rel,
                dag_id=dag_id,
                refs=tuple(dict.fromkeys(_REF_RE.findall(text))),
                sources=tuple(dict.fromkeys(_SOURCE_RE.findall(text))),
                timeout_minutes=int(raw.get("timeout_minutes", 60)),
                labels=tuple((label_value(k), label_value(v)) for k, v in labels.items()),
                description=str(raw.get("description") or ""),
            )
        )

    ids = [m.id for m in models]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise ConfigError(f"{where}: model duplikat {dupes} (nama file SQL harus unik per layer, seperti dbt)")

    schedule = str(data.get("schedule") or "assets")
    return TransformDag(
        dag_id=dag_id,
        description=str(data.get("description") or ""),
        schedule=schedule,
        start_date=_as_date(data.get("start_date") or "2026-01-01", where, "start_date"),
        tags=tuple(_as_list(data.get("tags"), where, "tags")),
        max_active_tasks=int(data.get("max_active_tasks", 8)),
        lookback_days=int(data.get("lookback_days", 3)),
        owner=data.get("owner"),
        models=tuple(models),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def _yaml_files(folder: Path) -> list[Path]:
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.iterdir() if p.suffix in (".yml", ".yaml") and not p.name.startswith("_"))


@lru_cache(maxsize=8)
def _load_platform_cached(path: str) -> Platform:
    return _parse_platform(_read_yaml(Path(path)))


def load_platform() -> Platform:
    path = config_dir() / "platform.yml"
    if not path.is_file():
        raise ConfigError(f"File {path} tidak ditemukan")
    return _load_platform_cached(str(path))


def _config_file(kind: str, key: str) -> Path:
    for suffix in (".yml", ".yaml"):
        path = config_dir() / kind / f"{key}{suffix}"
        if path.is_file():
            return path
    raise ConfigError(f"File configs/{kind}/{key}.yml tidak ditemukan")


def load_source(name: str) -> Source:
    """Load SATU source (dipakai DAG bronze & runner) -> error di YAML lain tidak ikut berpengaruh."""
    path = _config_file("sources", name)
    src = _parse_source(_read_yaml(path), path.name)
    if src is None:
        raise ConfigError(f"Source '{name}' enabled: false")
    return src


def load_transform(key: str) -> TransformDag:
    path = _config_file("transforms", key)
    dag = _parse_transform(_read_yaml(path), path.name)
    if dag is None:
        raise ConfigError(f"Transform '{key}' enabled: false")
    return dag


def _load_all(kind: str, parser, strict: bool) -> tuple[list[Any], dict[str, str]]:
    items, errors = [], {}
    for path in _yaml_files(config_dir() / kind):
        try:
            item = parser(_read_yaml(path), path.name)
        except ConfigError as exc:
            if strict:
                raise
            errors[f"{kind}/{path.name}"] = str(exc)
            continue
        if item is not None:
            items.append(item)
    return items, errors


def load_sources(strict: bool = True) -> list[Source] | tuple[list[Source], dict[str, str]]:
    """strict=False -> (sources_valid, {file: error}) supaya 1 YAML rusak tidak menjatuhkan semua DAG."""
    sources, errors = _load_all("sources", _parse_source, strict)
    return sources if strict else (sources, errors)


def load_transforms(strict: bool = True) -> list[TransformDag] | tuple[list[TransformDag], dict[str, str]]:
    dags, errors = _load_all("transforms", _parse_transform, strict)
    seen_dag: dict[str, bool] = {}
    seen_model: dict[str, str] = {}
    valid = []
    for dag in dags:
        problem = None
        if dag.dag_id in seen_dag:
            problem = f"dag_id '{dag.dag_id}' duplikat"
        for model in dag.models:
            if model.id in seen_model:
                problem = f"model '{model.id}' sudah didefinisikan di DAG {seen_model[model.id]}"
        if problem:
            if strict:
                raise ConfigError(problem)
            errors[f"transforms/{dag.dag_id}"] = problem
            continue
        seen_dag[dag.dag_id] = True
        seen_model.update({m.id: dag.dag_id for m in dag.models})
        valid.append(dag)
    return valid if strict else (valid, errors)


def bq_uri(project: str, dataset: str, table: str) -> str:
    """Format URI Asset BigQuery yang divalidasi provider google: bigquery://project/dataset/table."""
    return f"bigquery://{project}/{dataset}/{table}"
