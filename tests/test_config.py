"""Test validasi YAML, normalisasi nama, dan logika window incremental/backfill."""

from datetime import date, datetime, timezone

import pytest

from medallion.config import ConfigError, load_source, normalize_path
from medallion.bronze.runner import SkipTable, lag_for, resolve_window

SOURCE_YML = """
name: {name}
type: mssql
schedule: "0 1 * * *"
defaults:
  lookback_hours: 48
  initial_value: "2026-09-01T00:00:00"
tables:
{tables}
"""


def write_source(cfg, tables: str, name: str = "src_a", file_name: str | None = None):
    (cfg / "sources" / f"{file_name or name}.yml").write_text(SOURCE_YML.format(name=name, tables=tables))


# ------------------------------------------------------------------ naming
IDENTIFIERS = [
    "dbo__Customer", "dbo__SalesOrderHeader", "dbo__SaldoStokProdukToko", "sales__OrderDetail2024",
    "dbo__tblABC_Item", "dbo__T_Trx_Header", "dbo__Kode-Barang", "dbo__2024Sales", "CustomerID",
    "ModifiedDate", "orderID", "ABCTable", "dbo__tbl__double", "dbo__Name With Space", "x_",
]


@pytest.mark.parametrize("ident", IDENTIFIERS)
def test_naming_sama_dengan_dlt(ident):
    naming = pytest.importorskip("dlt.common.normalizers.naming.snake_case")
    assert normalize_path(ident) == naming.NamingConvention().normalize_path(ident)


# ------------------------------------------------------------------ validasi YAML
def test_typo_key_dikasih_saran(config_dir):
    write_source(config_dir, "  - {table: dbo.Customer, load: incremental, cursor: ModifiedDate, primay_key: [CustomerID]}")
    with pytest.raises(ConfigError, match="maksudnya 'primary_key'"):
        load_source("src_a")


def test_incremental_wajib_cursor(config_dir):
    write_source(config_dir, "  - {table: dbo.Customer, load: incremental}")
    with pytest.raises(ConfigError, match="cursor"):
        load_source("src_a")


def test_merge_wajib_primary_key(config_dir):
    write_source(config_dir, "  - {table: dbo.Customer, load: incremental, cursor: ModifiedDate, write_disposition: merge}")
    with pytest.raises(ConfigError, match="primary_key"):
        load_source("src_a")


def test_nama_file_harus_sama_dengan_name(config_dir):
    write_source(config_dir, "  - {table: dbo.Product, load: full}", name="src_a", file_name="beda")
    with pytest.raises(ConfigError, match="nama file"):
        load_source("beda")


def test_tabel_bentrok_nama_target(config_dir):
    write_source(config_dir, "  - {table: dbo.Customer, load: full}\n  - {table: dbo.customer, load: full}")
    with pytest.raises(ConfigError, match="target_table"):
        load_source("src_a")


def test_default_dan_nama_target(config_dir):
    write_source(
        config_dir,
        "  - {table: dbo.SalesOrderHeader, load: incremental, cursor: ModifiedDate, primary_key: [SalesOrderID]}\n"
        "  - {table: dbo.Product, load: full, primary_key: [ProductID]}\n"
        "  - {table: dbo.Old, load: full, enabled: false}",
    )
    src = load_source("src_a")
    assert [t.target_table for t in src.tables] == ["dbo__sales_order_header", "dbo__product"]
    header, product = src.tables
    assert (header.write_disposition, product.write_disposition) == ("append", "replace")
    assert header.cluster_by == ("SalesOrderID",)
    assert header.initial_value == datetime(2026, 9, 1)
    assert header.pipeline_name == "src_a__dbo__sales_order_header"


# ------------------------------------------------------------------ window
@pytest.fixture
def src(config_dir):
    write_source(
        config_dir,
        "  - {table: dbo.Customer, load: incremental, cursor: ModifiedDate}\n"
        "  - {table: dbo.Daily, load: incremental, cursor: TxDate, initial_value: '2026-09-01', lookback_hours: 49}\n"
        "  - {table: dbo.Offset, load: incremental, cursor: TxAt, initial_value: '2026-09-01T00:00:00+07:00'}\n"
        "  - {table: dbo.Log, load: incremental, cursor: LogID, initial_value: 0, lookback_units: 100}\n"
        "  - {table: dbo.Product, load: full}",
    )
    return load_source("src_a")


def test_window_incremental_default(src):
    w = resolve_window(src.get_table("dbo.Customer"), src, {"run_type": "scheduled"})
    assert w.mode == "incremental" and w.start is None


def test_window_full(src):
    assert resolve_window(src.get_table("dbo.Product"), src, {"run_type": "scheduled"}).mode == "full"


@pytest.mark.parametrize("ctx", [
    {"backfill_start": "2026-09-01", "backfill_end": "2026-09-02"},
    {"run_type": "backfill", "data_interval_start": "2026-09-01T18:00:00+00:00", "data_interval_end": "2026-09-02T18:00:00+00:00"},
])
def test_tabel_full_diskip_saat_backfill(src, ctx):
    with pytest.raises(SkipTable):
        resolve_window(src.get_table("dbo.Product"), src, ctx)


def test_backfill_param_end_inklusif(src):
    w = resolve_window(src.get_table("dbo.Customer"), src, {"backfill_start": "2026-09-01", "backfill_end": "2026-09-02"})
    assert (w.mode, w.start, w.end) == ("backfill", datetime(2026, 9, 1), datetime(2026, 9, 3))


def test_backfill_param_template_kosong_dianggap_tidak_ada(src):
    # Airflow merender param None sebagai string "None"
    w = resolve_window(src.get_table("dbo.Customer"), src, {"backfill_start": "None", "backfill_end": "None"})
    assert w.mode == "incremental"


def test_backfill_airflow_interval_dikonversi_ke_wib(src):
    ctx = {"run_type": "DagRunType.BACKFILL", "data_interval_start": "2026-09-22T18:00:00+00:00",
           "data_interval_end": "2026-09-23T18:00:00+00:00"}
    w = resolve_window(src.get_table("dbo.Customer"), src, ctx)
    assert (w.start, w.end) == (datetime(2026, 9, 23, 1), datetime(2026, 9, 24, 1))


def test_backfill_cursor_date_dan_aware(src):
    ctx = {"backfill_start": "2026-09-01", "backfill_end": "2026-09-01"}
    daily = resolve_window(src.get_table("dbo.Daily"), src, ctx)
    assert (daily.start, daily.end) == (date(2026, 9, 1), date(2026, 9, 2))
    aware = resolve_window(src.get_table("dbo.Offset"), src, ctx)
    assert aware.start == datetime(2026, 8, 31, 17, tzinfo=timezone.utc)


def test_backfill_tidak_valid(src):
    with pytest.raises(ConfigError):
        resolve_window(src.get_table("dbo.Customer"), src, {"backfill_start": "2026-09-01"})
    with pytest.raises(ConfigError):
        resolve_window(src.get_table("dbo.Customer"), src, {"backfill_start": "2026-09-05", "backfill_end": "2026-09-01"})
    with pytest.raises(ConfigError, match="datetime/date"):
        resolve_window(src.get_table("dbo.Log"), src, {"backfill_start": "2026-09-01", "backfill_end": "2026-09-02"})


def test_lag(src):
    assert lag_for(src.get_table("dbo.Customer")) == 48 * 3600  # detik
    assert lag_for(src.get_table("dbo.Daily")) == 3  # hari (dibulatkan ke atas)
    assert lag_for(src.get_table("dbo.Log")) == 100  # unit cursor angka
