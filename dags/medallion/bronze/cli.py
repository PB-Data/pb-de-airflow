"""CLI untuk jalankan / debug ingestion bronze tanpa Airflow.

Jalankan dari folder ``dags/`` (supaya package ``medallion`` ketemu):

    # lihat semua source & tabel
    python -m medallion.bronze.cli list

    # coba 1 tabel ke folder lokal (.local/lake, parquet) -> TIDAK menyentuh GCS/BigQuery
    python -m medallion.bronze.cli run sqlserver_main dbo.Customer --target local

    # backfill window tertentu langsung ke BigQuery (tanggal end inklusif, jam WIB)
    python -m medallion.bronze.cli run sqlserver_main dbo.SalesOrder --start 2024-01-01 --end 2024-01-31

Kredensial DB untuk CLI diambil dari dlt secrets (~/.dlt/secrets.toml):

    [sources.sqlserver_main.credentials]
    host = "34.101.xx.xx"
    port = 1433
    login = "dlt_reader"
    password = "..."
    database = "NamaDatabase"
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from medallion.bronze.runner import TARGETS, SkipTable, lag_for, resolve_window, run_table
from medallion.config import ConfigError, load_platform, load_sources


def _cmd_list(args: argparse.Namespace) -> int:
    platform = load_platform()
    print(f"BigQuery project={platform.bq_project} location={platform.bq_location} lake={platform.lake_bucket_url}")
    for src in load_sources():
        if args.source and src.name != args.source:
            continue
        print(f"\n[{src.name}] type={src.type} conn={src.airflow_conn_id} dataset={src.bronze_dataset} "
              f"schedule={src.schedule!r} format={src.file_format}")
        for t in src.tables:
            extra = f" cursor={t.cursor} lag={lag_for(t)} initial={t.initial_value}" if t.is_incremental else ""
            print(f"  - {t.table:40} -> {t.target_table:35} {t.load:11} {t.write_disposition:7}{extra}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    run_ctx = {"run_type": "manual", "backfill_start": args.start, "backfill_end": args.end}
    if args.plan:
        from medallion.config import load_source

        src = load_source(args.source)
        tbl = src.get_table(args.table)
        try:
            window = resolve_window(tbl, src, run_ctx).__dict__
        except SkipTable as exc:
            window = {"mode": "skip", "reason": str(exc)}
        print(json.dumps({"table": tbl.table, "target_table": tbl.target_table, "window": window}, default=str, indent=2))
        return 0
    if args.refresh and not args.yes:
        answer = input(f"refresh={args.refresh} akan MENGHAPUS tabel/state '{args.table}' di target {args.target}. Ketik 'ya': ")
        if answer.strip().lower() != "ya":
            print("batal")
            return 1
    try:
        summary = run_table(args.source, args.table, run_ctx=run_ctx, target=args.target, refresh=args.refresh)
    except SkipTable as exc:
        print(f"SKIP: {exc}")
        return 0
    print(json.dumps(summary, indent=2, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m medallion.bronze.cli", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="daftar source & tabel dari configs/sources")
    p_list.add_argument("--source")
    p_list.set_defaults(func=_cmd_list)

    p_run = sub.add_parser("run", help="jalankan ingestion 1 tabel")
    p_run.add_argument("source")
    p_run.add_argument("table", help="nama seperti di YAML, mis. dbo.Customer")
    p_run.add_argument("--target", choices=TARGETS, default="bigquery")
    p_run.add_argument("--start", help="backfill start (YYYY-MM-DD atau ISO datetime, jam sumber)")
    p_run.add_argument("--end", help="backfill end (YYYY-MM-DD = inklusif)")
    p_run.add_argument("--plan", action="store_true", help="hanya tampilkan window, tidak menjalankan")
    p_run.add_argument("--refresh", choices=["drop_resources", "drop_data"],
                       help="reset tabel+state (drop_resources) atau data saja (drop_data). HATI-HATI.")
    p_run.add_argument("--yes", action="store_true", help="skip konfirmasi --refresh")
    p_run.set_defaults(func=_cmd_run)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
