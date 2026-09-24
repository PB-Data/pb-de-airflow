"""Titik masuk DAG ingestion bronze.

Satu file YAML di `config/sources/` menghasilkan satu DAG. File ini hanya
menjahitnya; logikanya ada di `_factory.py`.

Jangan tambah kode di sini untuk menambah tabel. Tambah di config.
"""

from __future__ import annotations

import logging
from pathlib import Path

from _factory import build_broken_dag, build_ingestion_dag

from pb_dwh.config.loader import config_dir, load_source_config

logger = logging.getLogger(__name__)

folder = config_dir() / "sources"
berkas = sorted(p for p in folder.glob("*.y*ml") if p.is_file()) if folder.exists() else []

if not berkas:
    logger.warning("Tidak ada file config di %s; tidak ada DAG ingestion yang dibuat.", folder)

for path in berkas:
    # Satu config rusak tidak boleh menjatuhkan DAG source lain. Yang rusak
    # tetap muncul di UI sebagai DAG yang gagal, lengkap dengan pesannya.
    try:
        cfg = load_source_config(path)
        dag = build_ingestion_dag(cfg)
    except Exception as exc:  # noqa: BLE001 - sengaja menangkap semua
        dag = build_broken_dag(path.name, exc)

    globals()[dag.dag_id] = dag
