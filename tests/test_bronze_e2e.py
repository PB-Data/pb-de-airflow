"""End-to-end bronze dengan dlt: SQLite (source) -> folder lokal / DuckDB (pengganti GCS/BigQuery).

Butuh: pip install -r requirements-dev.txt
"""

import glob
import os
from datetime import datetime

import pytest

pytest.importorskip("dlt")
pytest.importorskip("duckdb")
sa = pytest.importorskip("sqlalchemy")

from medallion.bronze.runner import run_table  # noqa: E402

SOURCE = """
name: sqlite_test
type: sqlalchemy
table_naming: table
schedule: "0 1 * * *"
defaults: {lookback_hours: 24, initial_value: "2026-09-01T00:00:00"}
tables:
  - {table: Customer, load: incremental, cursor: ModifiedDate, primary_key: [CustomerID]}
  - {table: Product, load: full, primary_key: [ProductID]}
"""


@pytest.fixture
def env(config_dir, tmp_path, monkeypatch):
    (config_dir / "sources" / "sqlite_test.yml").write_text(SOURCE)
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'src.db'}")
    meta = sa.MetaData()
    customer = sa.Table("Customer", meta, sa.Column("CustomerID", sa.Integer, primary_key=True),
                        sa.Column("City", sa.String(50)), sa.Column("ModifiedDate", sa.DateTime, nullable=False))
    product = sa.Table("Product", meta, sa.Column("ProductID", sa.Integer, primary_key=True),
                       sa.Column("Price", sa.Numeric(19, 4)))
    meta.create_all(engine)
    with engine.begin() as con:
        con.execute(customer.insert(), [
            {"CustomerID": 1, "City": "Semarang", "ModifiedDate": datetime(2026, 9, 20, 9)},
            {"CustomerID": 2, "City": "Jakarta", "ModifiedDate": datetime(2026, 9, 21, 13)},
            {"CustomerID": 3, "City": "Depok", "ModifiedDate": datetime(2026, 9, 22, 16)},
        ])
        con.execute(product.insert(), [{"ProductID": 10, "Price": 250000}, {"ProductID": 11, "Price": 65000}])
    monkeypatch.setenv("SOURCES__SQLITE_TEST__CREDENTIALS", str(engine.url))
    monkeypatch.setenv("MEDALLION_DLT_PIPELINES_DIR", str(tmp_path / "pipelines"))
    monkeypatch.setenv("MEDALLION_LOCAL_DIR", str(tmp_path / "local"))
    return engine, customer


def test_incremental_lookback_dan_backfill(env):
    engine, customer = env
    first = run_table("sqlite_test", "Customer", target="duckdb")
    assert (first["mode"], first["rows"]) == ("incremental", 3)
    assert first["last_cursor_value"].startswith("2026-09-22 16:00")

    # lookback 24 jam dari 22 Sep 16:00 -> baca ulang >= 21 Sep 16:00 (customer 3) + data baru
    with engine.begin() as con:
        con.execute(customer.update().where(customer.c.CustomerID == 1).values(City="Semarang Barat", ModifiedDate=datetime(2026, 9, 23, 8)))
    second = run_table("sqlite_test", "Customer", target="duckdb")
    assert second["rows"] == 2

    # backfill window 21 Sep (inklusif) -> hanya customer 2, state incremental tidak berubah
    backfill = run_table("sqlite_test", "Customer", target="duckdb",
                         run_ctx={"backfill_start": "2026-09-21", "backfill_end": "2026-09-21"})
    assert (backfill["mode"], backfill["rows"]) == ("backfill", 1)
    assert backfill["last_cursor_value"] == second["last_cursor_value"]

    import duckdb
    con = duckdb.connect(os.path.join(os.environ["MEDALLION_LOCAL_DIR"], "bronze.duckdb"), read_only=True)
    cols = {r[0] for r in con.sql("select column_name from information_schema.columns where table_name='customer'").fetchall()}
    assert {"customer_id", "city", "modified_date", "_ingested_at", "_dlt_load_id"} <= cols
    assert con.sql("select count(*) from bronze_sqlite_test.customer").fetchone()[0] == 6  # append-only


def test_target_local_layout_lake(env):
    for _ in range(2):
        inc = run_table("sqlite_test", "Customer", target="local")
        full = run_table("sqlite_test", "Product", target="local")
    assert (full["mode"], full["rows"]) == ("full", 2)
    pattern = ("*", "*", "*", "*.parquet")  # <tabel>/<YYYY>/<MM>/<DD>/<load_id>.<file_id>.parquet
    # incremental (append): file tiap run menumpuk
    assert len(glob.glob(os.path.join(inc["lake_prefix"], *pattern))) == 2
    # NB target=local memakai filesystem sebagai destination: replace = hanya snapshot terakhir.
    # Di production (BigQuery + staging GCS, keep_files: true) snapshot lama TETAP disimpan,
    # lihat test_lake_retention.py
    assert len(glob.glob(os.path.join(full["lake_prefix"], *pattern))) == 1


def test_cursor_null_dicek(env, config_dir):
    engine, customer = env
    from medallion.config import ConfigError

    with engine.begin() as con:  # kolom ModifiedDate NOT NULL di tabel contoh -> buat tabel baru yg nullable
        con.exec_driver_sql('CREATE TABLE "Visit" ("VisitID" INTEGER PRIMARY KEY, "UpdatedAt" TIMESTAMP)')
        con.exec_driver_sql("""INSERT INTO "Visit" VALUES (1, '2026-09-20 10:00:00.000000'), (2, NULL)""")
    yml = config_dir / "sources" / "sqlite_test.yml"
    yml.write_text(yml.read_text() + "  - {table: Visit, load: incremental, cursor: UpdatedAt, primary_key: [VisitID]}\n")
    with pytest.raises(ConfigError, match="NULL"):
        run_table("sqlite_test", "Visit", target="duckdb")

    yml.write_text(yml.read_text().replace("primary_key: [VisitID]}", "primary_key: [VisitID], on_cursor_null: include}"))
    assert run_table("sqlite_test", "Visit", target="duckdb")["rows"] == 2


def test_paket_tertunda_diselesaikan_dulu(env):
    """Run sebelumnya gagal setelah extract -> paket lama di-load DAN window run ini tetap di-extract."""
    import datetime as dt

    import duckdb

    from medallion.bronze.runner import build_pipeline, build_resource, resolve_window
    from medallion.config import load_platform, load_source

    engine, _ = env
    assert run_table("sqlite_test", "Customer", target="duckdb")["rows"] == 3  # load sukses pertama

    # simulasi crash: extract + normalize (lookback 24 jam -> 1 baris: customer 3), TANPA load
    source = load_source("sqlite_test")
    table = source.get_table("Customer")
    pipe = build_pipeline(source, table, load_platform(), "duckdb")
    res = build_resource(source, table, resolve_window(table, source, {}), engine, "duckdb", dt.datetime.now(dt.timezone.utc))
    pipe.extract(res)
    pipe.normalize()
    assert pipe.has_pending_data

    result = run_table("sqlite_test", "Customer", target="duckdb",
                       run_ctx={"backfill_start": "2026-09-21", "backfill_end": "2026-09-21"})
    assert (result["mode"], result["rows"]) == ("backfill", 1)  # window baru tetap di-extract (customer 2)
    con = duckdb.connect(os.path.join(os.environ["MEDALLION_LOCAL_DIR"], "bronze.duckdb"), read_only=True)
    assert con.sql("select count(*) from bronze_sqlite_test.customer").fetchone()[0] == 3 + 1 + 1
    assert con.sql("select count(*) from bronze_sqlite_test._dlt_loads").fetchone()[0] == 3
