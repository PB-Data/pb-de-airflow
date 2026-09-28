"""Baca YAML di dags/configs. Tanpa import Airflow/dlt: dipakai DAG, validate, dan test."""

from pathlib import Path

import yaml

DAGS_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = DAGS_DIR / "configs"

PLATFORM_KEYS = {"gcp": {"bq_project", "bq_location"}, "lake": {"bucket_url"}, "runtime": {"timezone", "dlt_python"}}
SOURCE_KEYS = {"conn_id", "database", "schema", "dataset", "schedule", "file_format", "tables"}
TABLE_KEYS = {"table", "cursor", "initial_value", "lookback_hours", "chunk_size", "excluded_columns"}
TRANSFORM_KEYS = {"schedule", "models"}


def _read(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: isi YAML harus berupa mapping")
    return data


def _check_keys(data: dict, allowed: set, required: set, where: str) -> None:
    if unknown := set(data) - allowed:
        raise ValueError(f"{where}: key tidak dikenal {sorted(unknown)} (boleh: {sorted(allowed)})")
    if missing := required - set(data):
        raise ValueError(f"{where}: key wajib belum diisi {sorted(missing)}")


def load_platform() -> dict:
    path = CONFIG_DIR / "platform.yml"
    platform = _read(path)
    _check_keys(platform, set(PLATFORM_KEYS), set(PLATFORM_KEYS), str(path))
    for section, keys in PLATFORM_KEYS.items():
        _check_keys(platform[section], keys, keys, f"{path} {section}")
    return platform


def load_source(name: str) -> dict:
    path = CONFIG_DIR / "sources" / f"{name}.yml"
    src = _read(path)
    _check_keys(src, SOURCE_KEYS, SOURCE_KEYS - {"file_format"}, str(path))
    src.setdefault("file_format", "parquet")
    if src["file_format"] not in ("parquet", "jsonl"):
        raise ValueError(f"{path}: file_format harus parquet atau jsonl")
    if not src["tables"]:
        raise ValueError(f"{path}: tables kosong")
    for t in src["tables"]:
        where = f"{path} tabel {t.get('table')}"
        _check_keys(t, TABLE_KEYS, {"table"}, where)
        if "cursor" not in t and {"initial_value", "lookback_hours"} & set(t):
            raise ValueError(f"{where}: initial_value/lookback_hours hanya untuk tabel ber-cursor")
    return {"name": name, **src}


def load_transform(domain: str) -> dict:
    path = CONFIG_DIR / "transforms" / f"{domain}.yml"
    tr = _read(path)
    _check_keys(tr, TRANSFORM_KEYS, {"models"}, str(path))
    for model in tr["models"]:
        parts = model.split("/")
        if len(parts) != 3 or parts[0] not in ("silver", "gold") or parts[1] != domain or not model.endswith(".sql"):
            raise ValueError(f"{path}: model {model} harus silver/{domain}/<nama>.sql atau gold/{domain}/<nama>.sql")
    return {"name": domain, **tr}


def names(kind: str) -> list[str]:
    """Nama semua config di configs/<kind>/ (sources atau transforms)."""
    return sorted(p.stem for p in (CONFIG_DIR / kind).glob("*.yml"))
