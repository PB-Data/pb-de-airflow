"""Uji pembacaan dan validasi config.

Semua test di sini sengaja tidak menyentuh database, jaringan, dlt, maupun
Airflow. Jadi bisa dijalankan di laptop mana pun dengan:

    pip install -r requirements-dev.txt
    pytest -q
"""

from __future__ import annotations

import textwrap
from datetime import date, datetime
from pathlib import Path

import pytest

from pb_dwh.config.loader import load_all_source_configs, load_source_config
from pb_dwh.config.models import ConfigError, TableConfig

SOURCE_MINIMAL = """
source:
  name: uji
  env_prefix: UJI
  schema: dbo
  bq_dataset: bronze_uji

tables:
  - name: pesanan
    primary_key: [pesanan_id]
    cursor: modified_at
    lookback_days: 2
"""


@pytest.fixture
def folder_config(tmp_path: Path, monkeypatch) -> Path:
    """Folder config sementara, lengkap dengan settings.yml."""
    (tmp_path / "sources").mkdir()
    (tmp_path / "settings.yml").write_text(
        textwrap.dedent(
            """
            defaults:
              write_disposition: merge
              chunk_size: 12345
            pipeline:
              backend: pyarrow
            airflow:
              schedule: "30 2 * * *"
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PB_DWH_CONFIG_DIR", str(tmp_path))
    return tmp_path


def tulis_source(folder: Path, isi: str, nama: str = "uji.yml") -> Path:
    path = folder / "sources" / nama
    path.write_text(textwrap.dedent(isi), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Pembacaan
# ---------------------------------------------------------------------------


def test_membaca_source_minimal(folder_config: Path) -> None:
    cfg = load_source_config(tulis_source(folder_config, SOURCE_MINIMAL))

    assert cfg.name == "uji"
    assert cfg.dag_id == "bronze_ingest_uji"
    assert cfg.pipeline_name == "bronze_uji"
    assert cfg.table_names == ["pesanan"]

    tabel = cfg.table("pesanan")
    assert tabel.primary_key == ("pesanan_id",)
    assert tabel.write_disposition == "merge"


def test_database_opsional(folder_config: Path) -> None:
    # Tanpa `database`, nama database nanti dibaca dari {env_prefix}_DATABASE.
    cfg = load_source_config(tulis_source(folder_config, SOURCE_MINIMAL))
    assert cfg.database == ""

    dengan_database = SOURCE_MINIMAL.replace(
        "  env_prefix: UJI\n", "  env_prefix: UJI\n  database: PB_DC_SBY\n"
    )
    cfg = load_source_config(tulis_source(folder_config, dengan_database))
    assert cfg.database == "PB_DC_SBY"


def test_settings_yml_jadi_default(folder_config: Path) -> None:
    cfg = load_source_config(tulis_source(folder_config, SOURCE_MINIMAL))

    # chunk_size tidak ditulis di file source, jadi diambil dari settings.yml.
    assert cfg.table("pesanan").chunk_size == 12345
    assert cfg.schedule == "30 2 * * *"


def test_setelan_tabel_menang_atas_defaults(folder_config: Path) -> None:
    cfg = load_source_config(
        tulis_source(
            folder_config,
            """
            source:
              name: uji
              env_prefix: UJI
              schema: dbo
              bq_dataset: bronze_uji
            defaults:
              chunk_size: 500
            tables:
              - name: kecil
                primary_key: [id]
              - name: besar
                chunk_size: 10
                primary_key: [id]
            """,
        )
    )

    assert cfg.table("kecil").chunk_size == 500
    assert cfg.table("besar").chunk_size == 10


def test_primary_key_boleh_string_tunggal(folder_config: Path) -> None:
    cfg = load_source_config(
        tulis_source(
            folder_config,
            """
            source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
            tables:
              - name: t
                primary_key: id
            """,
        )
    )
    assert cfg.table("t").primary_key == ("id",)


def test_initial_value_tanggal_jadi_datetime(folder_config: Path) -> None:
    cfg = load_source_config(
        tulis_source(
            folder_config,
            """
            source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
            tables:
              - name: t
                primary_key: [id]
                cursor: modified_at
                initial_value: 2024-01-01
            """,
        )
    )
    assert cfg.table("t").initial_value == datetime(2024, 1, 1)


def test_start_date_terbaca_sebagai_date(folder_config: Path) -> None:
    cfg = load_source_config(
        tulis_source(
            folder_config,
            """
            source:
              name: uji
              env_prefix: UJI
              schema: dbo
              bq_dataset: b
              start_date: 2025-06-15
            tables:
              - name: t
                primary_key: [id]
            """,
        )
    )
    assert cfg.start_date == date(2025, 6, 15)


# ---------------------------------------------------------------------------
# Validasi: yang penting adalah kesalahan ini TIDAK lolos diam-diam
# ---------------------------------------------------------------------------


def test_merge_tanpa_primary_key_ditolak(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="primary_key"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    write_disposition: merge
                """,
            )
        )


def test_replace_dengan_cursor_ditolak(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="replace"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    write_disposition: replace
                    cursor: modified_at
                """,
            )
        )


def test_lookback_tanpa_cursor_ditolak(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="lookback_days butuh cursor"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    primary_key: [id]
                    lookback_days: 3
                """,
            )
        )


def test_cursor_integer_dengan_lookback_days_ditolak(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="lookback_days"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    primary_key: [id]
                    cursor: id
                    cursor_type: integer
                    lookback_days: 3
                """,
            )
        )


def test_primary_key_di_exclude_columns_ditolak(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="Primary key"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    primary_key: [id]
                    exclude_columns: [id]
                """,
            )
        )


def test_nama_tabel_duplikat_ditolak(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="lebih dari sekali"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    primary_key: [id]
                  - name: t
                    primary_key: [id]
                """,
            )
        )


def test_kunci_salah_ketik_ditolak(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="tidak dikenal"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    primary_key: [id]
                    lookbackdays: 3
                """,
            )
        )


def test_schema_per_tabel_memberi_pesan_yang_menuntun(folder_config: Path) -> None:
    with pytest.raises(ConfigError, match="file config baru"):
        load_source_config(
            tulis_source(
                folder_config,
                """
                source: {name: uji, env_prefix: UJI, schema: dbo, bq_dataset: b}
                tables:
                  - name: t
                    primary_key: [id]
                    schema: sales
                """,
            )
        )


def test_source_name_kembar_antar_file_ditolak(folder_config: Path) -> None:
    tulis_source(folder_config, SOURCE_MINIMAL, "a.yml")
    tulis_source(folder_config, SOURCE_MINIMAL, "b.yml")

    with pytest.raises(ConfigError, match="lebih dari satu file"):
        load_all_source_configs(folder_config / "sources")


# ---------------------------------------------------------------------------
# Perhitungan lookback
# ---------------------------------------------------------------------------


def test_lag_datetime_dihitung_dalam_detik() -> None:
    tabel = TableConfig(name="t", cursor="modified_at", lookback_days=2)
    assert tabel.incremental_lag == 2 * 86_400


def test_lag_integer_apa_adanya() -> None:
    tabel = TableConfig(name="t", cursor="id", cursor_type="integer", lookback=1000)
    assert tabel.incremental_lag == 1000


def test_tanpa_lookback_lag_none() -> None:
    assert TableConfig(name="t", cursor="modified_at").incremental_lag is None


def test_tanpa_cursor_lag_none() -> None:
    assert TableConfig(name="t", lookback_days=5).incremental_lag is None


# ---------------------------------------------------------------------------
# Config yang benar-benar dipakai di repo ini harus selalu sah
# ---------------------------------------------------------------------------


def test_config_repo_sah() -> None:
    """Penjaga: `config/sources/*.yml` yang di-commit tidak boleh rusak.

    Tanpa test ini, config yang salah baru ketahuan setelah DAG di-deploy.
    """
    akar = Path(__file__).resolve().parents[1]
    konfigurasi = load_all_source_configs(akar / "config" / "sources")
    assert konfigurasi, "tidak ada config source di repo"


def test_config_contoh_sah() -> None:
    """File di `config/examples/` disalin orang jadi config sungguhan.

    Tidak dibaca Airflow, jadi kalau rusak tidak ada yang sadar sampai
    salinannya ditolak.
    """
    akar = Path(__file__).resolve().parents[1]
    assert load_all_source_configs(akar / "config" / "examples")
