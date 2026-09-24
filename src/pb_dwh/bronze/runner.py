"""Menjalankan ingestion bronze untuk satu source.

Fungsi di sini dipakai dua pihak dengan jalur kode yang sama persis:

* DAG Airflow, lewat `dags/_factory.py`
* CLI untuk testing lokal, lewat `python -m pb_dwh.cli`

Disengaja sama supaya apa yang berhasil di laptop berperilaku sama di VM.
"""

from __future__ import annotations

import logging
from typing import Sequence

from pb_dwh.config.models import SourceConfig
from pb_dwh.sinks.bigquery import build_pipeline
from pb_dwh.sources.mssql import build_source

logger = logging.getLogger(__name__)


def run_source(
    cfg: SourceConfig,
    table_names: Sequence[str] | None = None,
    pipelines_dir: str | None = None,
    dev_mode: bool = False,
):
    """Extract seluruh (atau sebagian) tabel source, lalu muat ke BigQuery."""
    source = build_source(cfg, table_names=table_names)
    pipeline = build_pipeline(cfg, pipelines_dir=pipelines_dir, dev_mode=dev_mode)

    daftar = table_names or cfg.table_names
    logger.info("Mulai ingestion '%s': %d tabel", cfg.name, len(list(daftar)))

    info = pipeline.run(source, loader_file_format=cfg.loader_file_format)

    logger.info("Selesai: %s", info)
    return info
