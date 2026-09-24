"""Titik masuk DAG ingestion bronze.

Satu file YAML di `config/sources/` menghasilkan satu DAG. File ini hanya
menjahitnya; logikanya ada di `_factory.py`.

Jangan tambah kode di sini untuk menambah tabel. Tambah di config.

JANGAN hapus kata "airflow" di docstring ini. Dengan dag_discovery_safe_mode
(default), DAG processor airflow melewati file .py yang tidak memuat kata
"airflow" -- tanpa error, tanpa import error, DAG-nya diam-diam tidak pernah
muncul. File ini tidak mengimpor airflow secara langsung (semuanya lewat
_factory.py), jadi kalimat inilah yang membuatnya dibaca.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Airflow 3 tidak menaruh folder dags di sys.path saat DAG processor mem-parse
# file (terbukti di VM: "No module named '_factory'"). Ditambahkan sendiri
# supaya _factory.py di sebelah file ini bisa diimpor, apa pun versinya.
_FOLDER_DAGS = str(Path(__file__).resolve().parent)
if _FOLDER_DAGS not in sys.path:
    sys.path.insert(0, _FOLDER_DAGS)

from _factory import build_broken_dag, build_ingestion_dag  # noqa: E402

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
