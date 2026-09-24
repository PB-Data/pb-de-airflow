"""CLI untuk testing lokal tanpa menyentuh Airflow.

    python -m pb_dwh.cli validate
    python -m pb_dwh.cli list --source erp_mssql
    python -m pb_dwh.cli check-connection --source erp_mssql
    python -m pb_dwh.cli run --source erp_mssql --table sales_order --dev

`validate` dan `list` tidak menyentuh jaringan sama sekali, jadi aman
dijalankan di mana pun, termasuk di CI.
"""

from __future__ import annotations

import argparse
import logging
import sys

from pb_dwh.bronze.runner import run_source
from pb_dwh.config.loader import (
    config_dir,
    load_all_source_configs,
    load_source_config,
)
from pb_dwh.config.models import ConfigError, SourceConfig


def _siapkan_log(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def _cari_source(nama: str) -> SourceConfig:
    folder = config_dir() / "sources"
    for ekstensi in ("yml", "yaml"):
        kandidat = folder / f"{nama}.{ekstensi}"
        if kandidat.exists():
            return load_source_config(kandidat)

    tersedia = ", ".join(sorted(c.name for c in load_all_source_configs())) or "(kosong)"
    raise ConfigError(
        f"Source '{nama}' tidak ditemukan di {folder}. Yang tersedia: {tersedia}"
    )


def cmd_validate(args: argparse.Namespace) -> int:
    konfigurasi = load_all_source_configs()
    total = 0
    for cfg in konfigurasi:
        print(f"OK  {cfg.source_file:<24} source={cfg.name:<16} schema={cfg.schema:<10} "
              f"dataset={cfg.bq_dataset:<16} tabel={len(cfg.tables)}")
        total += len(cfg.tables)
    print(f"\n{len(konfigurasi)} source, {total} tabel. Semua config sah.")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    daftar = [_cari_source(args.source)] if args.source else load_all_source_configs()

    for cfg in daftar:
        print(f"\n# {cfg.source_file}  (dag_id: {cfg.dag_id})")
        print(f"{'tabel':<28} {'disposition':<12} {'cursor':<20} {'lookback':<10} primary_key")
        print("-" * 96)
        for t in cfg.tables:
            if not t.cursor:
                lookback = "-"
            elif t.cursor_type == "datetime":
                lookback = f"{t.lookback_days}d"
            else:
                lookback = str(t.lookback)
            print(
                f"{t.name:<28} {t.write_disposition:<12} {(t.cursor or '-'):<20} "
                f"{lookback:<10} {', '.join(t.primary_key) or '-'}"
            )
    return 0


def cmd_check_connection(args: argparse.Namespace) -> int:
    """Buka koneksi ke SQL Server dan hitung tabel yang kelihatan.

    Dipisah dari `run` supaya saat ada masalah, jelas apakah yang salah itu
    kredensial, driver ODBC, jaringan, atau justru pipeline-nya.
    """
    import sqlalchemy as sa

    from pb_dwh.sources.mssql import build_connection_url

    cfg = _cari_source(args.source)
    url = build_connection_url(cfg)

    # render_as_string() tanpa argumen menyembunyikan password.
    print(f"Menghubungi: {url.render_as_string()}")

    engine = sa.create_engine(url)
    try:
        with engine.connect() as conn:
            versi = conn.exec_driver_sql("SELECT @@VERSION").scalar()
            jumlah = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?",
                (cfg.schema,),
            ).scalar()
    finally:
        engine.dispose()

    print(f"\nTersambung.\n{str(versi).splitlines()[0]}")
    print(f"Schema '{cfg.schema}' berisi {jumlah} tabel/view.")

    hilang = []
    engine = sa.create_engine(url)
    try:
        with engine.connect() as conn:
            for t in cfg.tables:
                ada = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                    (cfg.schema, t.name),
                ).scalar()
                if not ada:
                    hilang.append(t.name)
    finally:
        engine.dispose()

    if hilang:
        print(f"\nPERINGATAN: tabel di config tapi tidak ada di source: {', '.join(hilang)}")
        return 1

    print(f"Semua {len(cfg.tables)} tabel di config ditemukan di source.")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    cfg = _cari_source(args.source)
    info = run_source(
        cfg,
        table_names=args.table or None,
        pipelines_dir=args.pipelines_dir,
        dev_mode=args.dev,
    )
    print(info)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pb_dwh.cli",
        description="Perkakas lokal untuk pipeline ingestion bronze.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="log level DEBUG")

    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("validate", help="periksa semua config YAML (tanpa koneksi DB)")
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("list", help="tampilkan daftar tabel beserta setelannya")
    p.add_argument("--source", help="nama source; kosongkan untuk semua")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("check-connection", help="uji koneksi ODBC ke SQL Server")
    p.add_argument("--source", required=True)
    p.set_defaults(func=cmd_check_connection)

    p = sub.add_parser("run", help="jalankan ingestion")
    p.add_argument("--source", required=True)
    p.add_argument(
        "--table",
        action="append",
        help="jalankan tabel tertentu saja; boleh diulang. Kosongkan untuk semua.",
    )
    p.add_argument(
        "--dev",
        action="store_true",
        help="tulis ke dataset bersufiks supaya tidak menimpa bronze asli",
    )
    p.add_argument("--pipelines-dir", help="folder kerja dlt; default ikut DLT_DATA_DIR")
    p.set_defaults(func=cmd_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _siapkan_log(args.verbose)
    try:
        return args.func(args)
    except ConfigError as exc:
        # Config salah itu kesalahan manusia yang wajar, bukan crash.
        # Tampilkan pesannya saja, tanpa traceback yang menakutkan.
        print(f"\nConfig bermasalah:\n  {exc}\n", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
