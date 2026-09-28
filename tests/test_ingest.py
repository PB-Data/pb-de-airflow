"""Ingest end-to-end tanpa GCP: sqlite -> dummy destination (jalur staging sama dengan BigQuery) + lake lokal."""

import sqlite3

import pytest

dlt = pytest.importorskip("dlt")
pq = pytest.importorskip("pyarrow.parquet")

from medallion.ingest import run  # noqa: E402


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    db = tmp_path / "src.db"
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE AIMHeader (nomorAIM TEXT, StatusProses TEXT)")
        c.execute("INSERT INTO AIMHeader VALUES ('A1', 'baru')")
        c.execute("CREATE TABLE SalesOrder (OrderID INTEGER, Qty INTEGER, ModifiedDate DATETIME)")
        c.executemany("INSERT INTO SalesOrder VALUES (?, ?, ?)", [(1, 5, "2024-01-01 10:00:00"), (2, 7, "2024-01-02 10:00:00")])
    return tmp_path, f"sqlite:///{db.as_posix()}"


def job(tmp_path, table, **extra):
    return {
        "table": table, "schema": "main", "database": "-", "dataset": "bronze_test", "file_format": "parquet",
        "pipeline_name": f"test__{table}", "bq_project": "-", "bq_location": "-",
        "bucket_url": (tmp_path / "lake").as_uri(), **extra,
    }


def lake_files(tmp_path, table):
    return sorted((tmp_path / "lake" / "bronze_test" / table).rglob("*.parquet"))


def dummy(**kw):
    return dlt.destinations.dummy(**{"completed_prob": 1.0, "loader_file_format": "parquet", **kw})


def test_full_refresh_menyimpan_file_lama_di_lake(env):
    tmp_path, cred = env
    run(job(tmp_path, "AIMHeader"), cred, destination=dummy())
    run(job(tmp_path, "AIMHeader"), cred, destination=dummy())

    files = lake_files(tmp_path, "AIMHeader")
    assert len(files) == 2  # file run pertama tidak dihapus (default dlt: dihapus)
    table = pq.read_table(files[-1])
    assert {"nomorAIM", "StatusProses", "_ingested_at", "_dlt_load_id"} <= set(table.column_names)
    assert str(table.schema.field("_ingested_at").type) == "timestamp[us, tz=UTC]"


def test_incremental_membaca_ulang_lookback(env):
    tmp_path, cred = env
    j = job(tmp_path, "SalesOrder", cursor="ModifiedDate", initial_value="2024-01-01", lookback_hours=25)
    run(j, cred, destination=dummy())
    with sqlite3.connect(tmp_path / "src.db") as c:
        c.execute("INSERT INTO SalesOrder VALUES (3, 1, '2024-01-02 12:00:00')")
    run(j, cred, destination=dummy())

    first, second = (pq.read_table(f).column("OrderID").to_pylist() for f in lake_files(tmp_path, "SalesOrder"))
    assert sorted(first) == [1, 2]
    # last_value 2024-01-02 10:00 - 25 jam = 2024-01-01 09:00: baris 1 & 2 terbaca ulang, 3 baru
    assert sorted(second) == [1, 2, 3]


def test_paket_gagal_diselesaikan_lalu_extract_baru(env):
    tmp_path, cred = env
    j = job(tmp_path, "SalesOrder", cursor="ModifiedDate")
    with pytest.raises(Exception):
        run(j, cred, destination=dummy(completed_prob=0.0, fail_prob=1.0))
    with sqlite3.connect(tmp_path / "src.db") as c:
        c.execute("INSERT INTO SalesOrder VALUES (3, 1, '2024-01-03 10:00:00')")
    run(j, cred, destination=dummy())

    rows = [r for f in lake_files(tmp_path, "SalesOrder") for r in pq.read_table(f).column("OrderID").to_pylist()]
    assert 3 in rows  # tanpa penanganan paket tertunda, run kedua hanya me-load paket lama
