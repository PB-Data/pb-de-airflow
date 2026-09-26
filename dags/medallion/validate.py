"""Validasi semua config + SQL sebelum deploy (dan setelah sync di VM).

Dari folder ``dags/``:

    python -m medallion.validate            # config YAML, ref()/source(), render SQL, syntax SQL
    python -m medallion.validate --dagbag   # + import semua file DAG pakai Airflow (di container Airflow)

Exit code 0 = aman, 1 = ada error (deploy script berhenti).
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

from medallion.catalog import Catalog, render_model_sql
import yaml

from medallion.config import DAGS_DIR, ConfigError, config_dir, load_sources

_CONTROL_FLOW = re.compile(r"^\s*(IF\b.*\bTHEN|ELSE|ELSEIF\b.*\bTHEN|END\s+IF\s*;?)\s*$", re.I | re.M)


def _check_stub_files(errors: list[str]) -> None:
    """Tiap YAML source/transform (enabled) harus punya file DAG kecil di dags/."""
    stubs = "\n".join(p.read_text(encoding="utf-8") for p in DAGS_DIR.glob("*.py"))
    wanted = [(f'build_bronze_dag("{s.name}")', f"sources/{s.name}.yml", f"bronze__{s.name}.py") for s in load_sources()]
    for path in sorted((config_dir() / "transforms").glob("*.y*ml")):
        if path.name.startswith("_") or (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("enabled", True) is False:
            continue
        wanted.append((f'build_transform_dag("{path.stem}")', f"transforms/{path.name}", f"transform__{path.stem}.py"))
    for call, cfg, suggested in wanted:
        func, arg = call.split("(", 1)
        pattern = re.escape(func) + r"\(\s*['\"]" + re.escape(arg.strip('")')) + r"['\"]\s*\)"
        if not re.search(pattern, stubs):
            example = "bronze__sqlserver_main.py" if call.startswith("build_bronze") else "transform__sales.py"
            errors.append(
                f"{cfg} belum punya file DAG. Copy dags/{example} jadi dags/{suggested} "
                f"lalu ganti baris terakhirnya jadi: dag = {call}"
            )


def _check_sql_syntax(sql: str, where: str, warnings: list[str], errors: list[str]) -> None:
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:  # pragma: no cover
        warnings.append("sqlglot tidak terpasang -> cek syntax SQL dilewati (pip install sqlglot)")
        return
    logging.getLogger("sqlglot").setLevel(logging.ERROR)
    try:
        statements = [s for s in sqlglot.parse(_CONTROL_FLOW.sub("", sql), read="bigquery") if s is not None]
    except sqlglot.errors.ParseError as exc:
        errors.append(f"{where}: syntax SQL error -> {str(exc).splitlines()[0]}")
        return
    for stmt in statements:
        if isinstance(stmt, exp.Command):
            warnings.append(f"{where}: statement tidak bisa dicek sqlglot: {stmt.sql()[:60]}...")


def _check_dagbag(errors: list[str]) -> None:
    import inspect

    try:
        from airflow.dag_processing.dagbag import DagBag  # Airflow >= 3.2
    except ImportError:
        from airflow.models.dagbag import DagBag  # Airflow 3.0 / 3.1

    kwargs = {"dag_folder": str(DAGS_DIR)}
    if "include_examples" in inspect.signature(DagBag.__init__).parameters:
        kwargs["include_examples"] = False  # Airflow 3.0/3.1: jangan ikut parse example DAG
    bag = DagBag(**kwargs)
    ours = str(DAGS_DIR.resolve())
    for file, err in bag.import_errors.items():
        if str(Path(file).resolve()).startswith(ours):
            errors.append(f"DAG import error {Path(file).name}:\n{err}")
    for dag_id, dag in sorted(bag.dags.items()):
        if str(Path(dag.fileloc).resolve()).startswith(ours):
            print(f"  DAG {dag_id}: {len(dag.tasks)} task")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m medallion.validate")
    parser.add_argument("--dagbag", action="store_true", help="import file DAG dengan Airflow (butuh airflow)")
    args = parser.parse_args(argv)

    errors: list[str] = []
    warnings: list[str] = []
    try:
        catalog = Catalog.load(strict=True)
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}")
        return 1

    print(f"Config OK: {len(catalog.sources)} source, {len(catalog.transforms)} DAG transform, {len(catalog.models)} model")
    for src in catalog.sources.values():
        print(f"  bronze__{src.name}: {len(src.tables)} tabel -> {catalog.platform.bq_project}.{src.bronze_dataset}")
    _check_stub_files(errors)

    for model_id, model in sorted(catalog.models.items()):
        for full_refresh in (False, True):
            try:
                sql = render_model_sql(catalog, model_id, {"full_refresh": full_refresh})
            except Exception as exc:  # jinja error, ref tidak ada, dll
                errors.append(f"sql/{model.sql_file}: gagal render -> {exc}")
                break
            _check_sql_syntax(sql, f"sql/{model.sql_file} (full_refresh={full_refresh})", warnings, errors)

    if args.dagbag:
        _check_dagbag(errors)

    for msg in dict.fromkeys(warnings):
        print(f"WARNING: {msg}")
    for msg in errors:
        print(f"ERROR: {msg}")
    print("VALIDASI GAGAL" if errors else "VALIDASI OK")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
