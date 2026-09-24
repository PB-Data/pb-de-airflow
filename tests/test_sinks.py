"""Uji bagian alur GCS -> BigQuery yang tidak butuh jaringan maupun GCP.

Mengimpor dlt dan google-cloud-bigquery (keduanya ada di requirements), tapi
tidak membuat koneksi apa pun.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from dlt.common.metrics import LoadJobMetrics
from google.cloud import bigquery

from pb_dwh.config.models import ConfigError, SourceConfig, TableConfig
from pb_dwh.sinks.bigquery import buat_load_config
from pb_dwh.sinks.gcs import file_hasil, raw_bucket_url


def _job(tabel: str, url: str | None) -> LoadJobMetrics:
    return LoadJobMetrics(
        job_id=f"{tabel}.abc",
        file_path=f"/tmp/{tabel}.parquet",
        table_name=tabel,
        started_at=datetime(2026, 9, 24),
        finished_at=datetime(2026, 9, 24),
        state="completed",
        remote_url=url,
    )


def test_file_hasil_hanya_parquet_milik_tabel_itu() -> None:
    dasar = "gs://pb-datalake-raw/dc_sby"
    info = SimpleNamespace(
        metrics={
            "1727150000.1": [
                {
                    "job_metrics": {
                        "a": _job("AIMHeader", f"{dasar}/AIMHeader/2026-09-24/1727150000.1.b.parquet"),
                        "b": _job("AIMHeader", f"{dasar}/AIMHeader/2026-09-24/1727150000.1.a.parquet"),
                        "c": _job("AIMDetail", f"{dasar}/AIMDetail/2026-09-24/1727150000.1.c.parquet"),
                        "d": _job("_dlt_loads", f"{dasar}/_dlt_loads/1727150000.1.jsonl"),
                        "e": _job("AIMHeader", None),
                    }
                }
            ]
        }
    )
    assert file_hasil(info, "AIMHeader") == [
        f"{dasar}/AIMHeader/2026-09-24/1727150000.1.a.parquet",
        f"{dasar}/AIMHeader/2026-09-24/1727150000.1.b.parquet",
    ]


def test_file_hasil_kosong_kalau_tidak_ada_data() -> None:
    assert file_hasil(SimpleNamespace(metrics={}), "AIMHeader") == []


def _cfg(raw_prefix: str = "") -> SourceConfig:
    return SourceConfig(
        name="dc_sby",
        env_prefix="MSSQL",
        schema="dbo",
        bq_dataset="bronze_uji",
        tables=(TableConfig(name="AIMHeader", write_disposition="replace"),),
        raw_prefix=raw_prefix,
    )


def test_raw_bucket_url(monkeypatch) -> None:
    monkeypatch.setenv("GCS_STAGING_BUCKET", "gs://pb-datalake-raw/")
    assert raw_bucket_url(_cfg()) == "gs://pb-datalake-raw"
    assert raw_bucket_url(_cfg("dlt")) == "gs://pb-datalake-raw/dlt"


def test_raw_bucket_url_tanpa_env_ditolak(monkeypatch) -> None:
    monkeypatch.delenv("GCS_STAGING_BUCKET", raising=False)
    with pytest.raises(ConfigError, match="GCS_STAGING_BUCKET"):
        raw_bucket_url(_cfg())


def test_replace_mempertahankan_skema_ddl() -> None:
    # WRITE_TRUNCATE biasa mengganti skema dengan skema file dan membuang
    # PRIMARY KEY; tabel bronze dibuat manual, jadi harus WRITE_TRUNCATE_DATA.
    c = buat_load_config(TableConfig(name="t", write_disposition="replace"))
    assert c.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE_DATA
    assert c.create_disposition == bigquery.CreateDisposition.CREATE_NEVER
    assert c.source_format == bigquery.SourceFormat.PARQUET


def test_append() -> None:
    c = buat_load_config(TableConfig(name="t", write_disposition="append"))
    assert c.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert c.create_disposition == bigquery.CreateDisposition.CREATE_NEVER


def test_dev_boleh_membuat_tabel() -> None:
    c = buat_load_config(TableConfig(name="t", write_disposition="replace"), dev_mode=True)
    assert c.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert c.create_disposition == bigquery.CreateDisposition.CREATE_IF_NEEDED


def test_merge_ditolak_di_tahap_bigquery() -> None:
    with pytest.raises(ConfigError, match="merge"):
        buat_load_config(TableConfig(name="t", write_disposition="merge", primary_key=("id",)))
