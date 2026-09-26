"""Bangun URL SQLAlchemy dari Airflow Connection (dikirim via template) atau dlt secrets."""

from __future__ import annotations

import json
from typing import Any

from medallion.config import ConfigError, Source

MSSQL_DEFAULT_QUERY = {
    "driver": "ODBC Driver 18 for SQL Server",
    # Koneksi via public IP -> WAJIB terenkripsi. Sertifikat Cloud SQL ditandatangani CA
    # per-instance (bukan CA publik), jadi default TrustServerCertificate=yes.
    # Hardening: pasang server-ca.pem di image lalu set TrustServerCertificate=no.
    "Encrypt": "yes",
    "TrustServerCertificate": "yes",
}
POSTGRES_DEFAULT_QUERY = {"sslmode": "require"}


def _clean(value: Any) -> Any:
    """Template Airflow merender None sebagai string 'None'."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in ("", "None", "null"):
        return None
    return value


def _parse_extra(extra: Any) -> dict[str, Any]:
    extra = _clean(extra)
    if extra is None:
        return {}
    if isinstance(extra, dict):
        return dict(extra)
    try:
        parsed = json.loads(extra)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Extra connection harus JSON object, dapat: {extra[:80]!r}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError("Extra connection harus JSON object")
    return parsed


def build_url(source_type: str, conn: dict[str, Any]) -> Any:
    """``conn`` punya key ala Airflow Connection: host, port, login, password, schema (=database), extra."""
    from sqlalchemy.engine import URL

    extra = _parse_extra(conn.get("extra"))
    raw_url = _clean(conn.get("sqlalchemy_url")) or extra.pop("sqlalchemy_url", None)
    if raw_url:  # escape hatch: URL SQLAlchemy lengkap (dipakai juga untuk test)
        return str(raw_url)

    host = _clean(conn.get("host"))
    if not host:
        raise ConfigError("Connection source tidak punya host")
    port = _clean(conn.get("port"))
    port = int(port) if port is not None else None
    login = _clean(conn.get("login")) or _clean(conn.get("username"))
    password = _clean(conn.get("password"))
    database = _clean(conn.get("database")) or _clean(conn.get("schema"))

    if source_type in ("mssql", "postgres") and not database:
        # tanpa database, SQLAlchemy+pyodbc diam-diam membuat DSN string -> error "Data source name not found"
        raise ConfigError("Connection source tidak punya database (isi field 'Schema' di Airflow Connection)")
    if source_type == "mssql":
        query = dict(MSSQL_DEFAULT_QUERY)
        query.update({k: str(v) for k, v in extra.items()})
        return URL.create(
            "mssql+pyodbc", username=login, password=password, host=host,
            port=port or 1433, database=database, query=query,
        )
    if source_type == "postgres":
        query = dict(POSTGRES_DEFAULT_QUERY)
        query.update({k: str(v) for k, v in extra.items()})
        return URL.create(
            "postgresql+psycopg2", username=login, password=password, host=host,
            port=port or 5432, database=database, query=query,
        )
    raise ConfigError(f"type '{source_type}' butuh 'sqlalchemy_url' di extra connection")


def resolve_credentials(source: Source, db_conn: dict[str, Any] | None) -> Any:
    """Prioritas: Airflow Connection (dari DAG) -> dlt secrets / env (untuk CLI lokal)."""
    if db_conn and (_clean(db_conn.get("host")) or _clean(db_conn.get("extra")) or _clean(db_conn.get("sqlalchemy_url"))):
        return build_url(source.type, db_conn)

    import dlt

    key = f"sources.{source.name}.credentials"
    value = dlt.secrets.get(key)
    if value is None:
        env_key = key.upper().replace(".", "__")
        raise ConfigError(
            f"Kredensial source '{source.name}' tidak ditemukan. Di Airflow: buat Connection "
            f"'{source.airflow_conn_id}'. Di lokal: isi {key} di ~/.dlt/secrets.toml atau env {env_key}."
        )
    if isinstance(value, str):
        return value
    return build_url(source.type, dict(value))
