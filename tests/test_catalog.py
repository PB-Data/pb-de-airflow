"""Test katalog model silver/gold: render SQL, dependency ref()/source(), cycle."""


import pytest

from medallion.catalog import Catalog, render_model_sql
from medallion.config import ConfigError


@pytest.fixture
def main_catalog(monkeypatch):
    monkeypatch.delenv("MEDALLION_CONFIG_DIR", raising=False)
    monkeypatch.delenv("MEDALLION_SQL_DIR", raising=False)
    return Catalog.load(strict=True)


def test_config_contoh_valid(main_catalog):
    assert "sqlserver_main" in main_catalog.sources
    assert "gold.daily_sales" in main_catalog.models


@pytest.mark.parametrize("full_refresh", [False, True])
def test_semua_model_bisa_dirender(main_catalog, full_refresh):
    for model_id in main_catalog.models:
        sql = render_model_sql(main_catalog, model_id, {"full_refresh": full_refresh})
        assert "{{" not in sql and "{%" not in sql
        assert "`smi-dwh." in sql


def test_ref_source_dan_this(main_catalog):
    sql = render_model_sql(main_catalog, "silver.customer")
    assert "FROM `smi-dwh.bronze_sqlserver_main.dbo__customer`" in sql
    assert "MERGE `smi-dwh.silver_sales.customer` T" in sql
    assert "DATETIME(modified_date, 'UTC')" in sql
    gold = render_model_sql(main_catalog, "gold.daily_sales", {"lookback_days": 7})
    assert "`smi-dwh.silver_sales.sales_order`" in gold and "INTERVAL 7 DAY" in gold


def test_dependency_dan_asset(main_catalog):
    dag = main_catalog.transforms["transform__sales"]
    edges = set(main_catalog.internal_edges(dag))
    assert ("silver.sales_order", "gold.daily_sales") in edges
    assets = dict(main_catalog.external_assets(dag))
    assert assets["bronze.sqlserver_main.dbo__customer"] == "bigquery://smi-dwh/bronze_sqlserver_main/dbo__customer"
    assert len(assets) == 4


def _transform(config_dir, models: dict[str, str]):
    sql_root = config_dir.parent / "sql"
    for rel, body in models.items():
        path = sql_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    (config_dir / "transforms" / "t.yml").write_text(
        "datasets: {silver: silver_t, gold: gold_t}\nmodels:\n" + "".join(f"  - {m}\n" for m in models)
    )


def test_cycle_terdeteksi(config_dir):
    _transform(config_dir, {
        "silver/a.sql": "SELECT * FROM {{ ref('silver.b') }}",
        "silver/b.sql": "SELECT * FROM {{ ref('silver.a') }}",
    })
    with pytest.raises(ConfigError, match="melingkar"):
        Catalog.load(strict=True)


def test_ref_tidak_ada(config_dir):
    _transform(config_dir, {"gold/x.sql": "SELECT * FROM {{ ref('silver.tidak_ada') }}"})
    with pytest.raises(ConfigError, match="tidak ditemukan"):
        Catalog.load(strict=True)


def test_model_duplikat_dalam_satu_yaml(config_dir):
    _transform(config_dir, {"silver/a/customer.sql": "SELECT 1", "silver/b/customer.sql": "SELECT 2"})
    with pytest.raises(ConfigError, match="duplikat"):
        Catalog.load(strict=True)


def test_validate_terima_kutip_tunggal(config_dir, tmp_path, monkeypatch):
    import medallion.validate as validate

    (config_dir / "sources" / "s1.yml").write_text("name: s1\ntables:\n  - {table: dbo.P, load: full}\n")
    dags = tmp_path / "dags_stub"
    dags.mkdir()
    (dags / "bronze__s1.py").write_text("# airflow dag\ndag = build_bronze_dag('s1')\n")
    monkeypatch.setattr(validate, "DAGS_DIR", dags)
    errors: list[str] = []
    validate._check_stub_files(errors)
    assert errors == []
    (dags / "bronze__s1.py").write_text("dag = build_bronze_dag('lain')\n")
    validate._check_stub_files(errors)
    assert errors and "sources/s1.yml" in errors[0]
