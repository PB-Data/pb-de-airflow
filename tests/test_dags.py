"""Penjaga file di dags/ yang tidak butuh Airflow terpasang.

Airflow (dag_discovery_safe_mode, default) melewati file .py yang tidak memuat
kata "airflow" serta "dag" atau "asset", tanpa error apa pun. Pernah terjadi:
bronze_ingest.py tidak mengimpor airflow secara langsung, sehingga DAG
ingestion tidak pernah muncul di UI.
"""

from __future__ import annotations

from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"

# File yang memang tidak mendefinisikan DAG (modul pembantu) diberi awalan _.
FILE_DAG = sorted(p for p in DAGS.glob("*.py") if not p.name.startswith("_"))


def test_ada_file_dag() -> None:
    assert FILE_DAG, f"tidak ada file DAG di {DAGS}"


@pytest.mark.parametrize("path", FILE_DAG, ids=lambda p: p.name)
def test_file_dag_lolos_heuristik_airflow(path: Path) -> None:
    # Sama dengan airflow.utils.file.might_contain_dag_via_default_heuristic.
    isi = path.read_bytes().lower()
    assert b"airflow" in isi, (
        f"{path.name} tidak memuat kata 'airflow', jadi dilewati DAG processor. "
        "Tambahkan kata itu di docstring."
    )
    assert b"dag" in isi or b"asset" in isi, f"{path.name} tidak memuat kata 'dag'."
