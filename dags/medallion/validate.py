"""Cek config & SQL sebelum deploy:  cd dags && python -m medallion.validate [--dagbag]

--dagbag  parse semua DAG dengan Airflow. Butuh Airflow terpasang; scripts/deploy.sh
          menjalankannya di container VM sebelum DAG baru disalin ke folder DAG Airflow.
"""

import sys

from medallion.config import DAGS_DIR, load_platform, load_source, load_transform, names


def check() -> list[str]:
    errors = []
    try:
        load_platform()
    except (ValueError, OSError) as e:
        errors.append(str(e))
    sources = names("sources")
    for name in sources:
        try:
            load_source(name)
        except ValueError as e:
            errors.append(str(e))
        if not (DAGS_DIR / f"bronze__{name}.py").exists():
            errors.append(f"sources/{name}.yml: file DAG dags/bronze__{name}.py belum ada")
    for domain in names("transforms"):
        try:
            tr = load_transform(domain)
        except ValueError as e:
            errors.append(str(e))
            continue
        if not (DAGS_DIR / f"transform__{domain}.py").exists():
            errors.append(f"transforms/{domain}.yml: file DAG dags/transform__{domain}.py belum ada")
        if isinstance(tr.get("schedule"), list):
            errors += [f"transforms/{domain}.yml: source {s} tidak ada" for s in tr["schedule"] if s not in sources]
        for model in tr["models"]:
            path = DAGS_DIR / "sql" / model
            layer, _, file = model.split("/")
            target = f"{layer}_{domain}.{file.removesuffix('.sql')}"
            if not path.exists():
                errors.append(f"transforms/{domain}.yml: file sql/{model} tidak ada")
            elif target not in path.read_text(encoding="utf-8"):
                errors.append(f"sql/{model}: tidak menulis ke tabel {target}")
    return errors


def dagbag_errors() -> list[str]:
    from airflow.dag_processing.dagbag import BundleDagBag

    bag = BundleDagBag(dag_folder=str(DAGS_DIR), bundle_path=DAGS_DIR, bundle_name="dags-folder")
    errors = [f"{file}: {err}" for file, err in bag.import_errors.items()]
    expected = {f"bronze__{n}" for n in names("sources")} | {f"transform__{d}" for d in names("transforms")}
    if missing := expected - set(bag.dags):
        errors.append(f"DAG tidak ter-load: {sorted(missing)}")
    errors += [f"DAG {dag_id} tidak punya task" for dag_id, dag in bag.dags.items() if not dag.task_dict]
    return errors


if __name__ == "__main__":
    errors = check()
    if "--dagbag" in sys.argv:
        errors += dagbag_errors()
    for e in errors:
        print("GAGAL:", e)
    print("VALIDASI GAGAL" if errors else "VALIDASI OK")
    sys.exit(1 if errors else 0)
