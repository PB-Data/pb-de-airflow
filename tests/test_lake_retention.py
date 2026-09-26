"""Kenapa `lake.keep_files: true` penting: default dlt MENGHAPUS file staging lama per tabel.

Pakai destination `dummy` bawaan dlt + staging filesystem lokal: jalur kode staging-nya sama
dengan BigQuery + GCS (keduanya memakai flag truncate_tables_on_staging_destination_before_load).
"""

import glob
import os

import pytest

dlt = pytest.importorskip("dlt")


def _run_three_times(tmp_path, truncate: bool, disposition: str) -> int:
    base = tmp_path / f"stage_{truncate}_{disposition}"
    for i in range(3):
        pipe = dlt.pipeline(
            pipeline_name=f"retention_{truncate}_{disposition}",
            pipelines_dir=str(tmp_path / "pipelines"),
            destination=dlt.destinations.dummy(
                loader_file_format="parquet", completed_prob=1.0,
                truncate_tables_on_staging_destination_before_load=truncate,
            ),
            staging=dlt.destinations.filesystem(
                bucket_url=base.as_uri(), layout="{table_name}/{YYYY}/{MM}/{DD}/{load_id}.{file_id}.{ext}"
            ),
            dataset_name="bronze_x",
        )
        pipe.run([{"id": 1, "v": i}], table_name="t", write_disposition=disposition, loader_file_format="parquet")
    return len(glob.glob(os.path.join(base, "bronze_x", "t", "*", "*", "*", "*.parquet")))


@pytest.mark.parametrize("disposition", ["append", "replace"])
def test_keep_files_menyimpan_semua_file(tmp_path, disposition):
    assert _run_three_times(tmp_path, truncate=False, disposition=disposition) == 3


@pytest.mark.parametrize("disposition", ["append", "replace"])
def test_default_dlt_menghapus_file_lama(tmp_path, disposition):
    assert _run_three_times(tmp_path, truncate=True, disposition=disposition) == 1


def test_pipeline_bigquery_memakai_keep_files(config_dir, monkeypatch):
    from medallion.bronze.runner import build_pipeline
    from medallion.config import load_platform, load_source

    (config_dir / "sources" / "s.yml").write_text(
        "name: s\ntype: mssql\ntables:\n  - {table: dbo.Product, load: full}\n"
    )
    monkeypatch.setenv("MEDALLION_DLT_PIPELINES_DIR", str(config_dir.parent / "pipelines"))
    platform, source = load_platform(), load_source("s")
    pipe = build_pipeline(source, source.tables[0], platform, "bigquery")
    params = pipe.destination.config_params
    assert params["truncate_tables_on_staging_destination_before_load"] is False
    assert params["location"] == "asia-southeast2" and params["project_id"] == "test-project"
    assert pipe.staging.config_params["layout"].startswith("{table_name}/{YYYY}/{MM}/{DD}/")
