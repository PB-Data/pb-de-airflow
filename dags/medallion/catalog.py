"""Katalog semua tabel (bronze + model silver/gold) dan macro SQL ala dbt.

Dipakai oleh DAG transform, CLI render SQL, dan test. Pure python (tanpa airflow/dlt).

Macro yang tersedia di file SQL (``dags/sql/**.sql``):

* ``{{ this }}``                                   -> tabel model ini, mis. `smi-dwh.silver_sales.customer`
* ``{{ ref('silver.customer') }}``                 -> tabel model lain (dependency otomatis)
* ``{{ source('sqlserver_main', 'dbo.Customer') }}`` -> tabel bronze hasil dlt
* ``{{ as_local_datetime('modified_date') }}``     -> DATETIME jam dinding sumber (WIB)
* ``{{ as_utc_timestamp('modified_date') }}``      -> TIMESTAMP instant yang benar
* ``{{ params.lookback_days }}`` / ``{{ params.full_refresh }}`` -> param DAG

Nama-nama ini sengaja mirip dbt supaya migrasi ke dbt nanti tinggal copy file SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from typing import Any, Iterable

from medallion.config import (
    ConfigError,
    Model,
    Platform,
    Source,
    Table,
    TransformDag,
    bq_uri,
    load_platform,
    load_sources,
    load_transforms,
    sql_dir,
)


@dataclass(frozen=True)
class Relation:
    project: str
    dataset: str
    table: str

    def __str__(self) -> str:  # dipakai saat dirender di SQL
        return f"`{self.project}.{self.dataset}.{self.table}`"

    @property
    def fqn(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    @property
    def uri(self) -> str:
        return bq_uri(self.project, self.dataset, self.table)


def bronze_asset_name(table: Table) -> str:
    return f"bronze.{table.source_name}.{table.target_table}"


class Catalog:
    def __init__(
        self,
        platform: Platform,
        sources: list[Source],
        transforms: list[TransformDag],
        errors: dict[str, str] | None = None,
    ):
        self.platform = platform
        self.sources = {s.name: s for s in sources}
        self.transforms = {t.dag_id: t for t in transforms}
        self.models: dict[str, Model] = {m.id: m for t in transforms for m in t.models}
        self.errors = errors or {}

    @classmethod
    def load(cls, strict: bool = True) -> "Catalog":
        """strict=False dipakai DAG: YAML lain yang rusak tidak ikut menjatuhkan DAG ini."""
        platform = load_platform()
        if strict:
            catalog = cls(platform, load_sources(), load_transforms())
            catalog.validate()
            return catalog
        sources, src_errors = load_sources(strict=False)
        transforms, trf_errors = load_transforms(strict=False)
        return cls(platform, sources, transforms, {**src_errors, **trf_errors})

    def _broken_hint(self) -> str:
        if not self.errors:
            return ""
        return " | Config lain yang sedang error: " + "; ".join(f"{k}: {v}" for k, v in self.errors.items())

    # ------------------------------------------------------------------ lookup
    def bronze_relation(self, table: Table) -> Relation:
        src = self.sources[table.source_name]
        return Relation(self.platform.bq_project, src.bronze_dataset, table.target_table)

    def find_bronze(self, source_name: str, table_name: str) -> Table:
        if source_name not in self.sources:
            raise ConfigError(
                f"source('{source_name}', '{table_name}'): source tidak dikenal. "
                f"Tersedia: {sorted(self.sources)}{self._broken_hint()}"
            )
        return self.sources[source_name].get_table(table_name)

    def model_relation(self, model: Model) -> Relation:
        return Relation(self.platform.bq_project, model.dataset, model.table)

    def find_model(self, name: str, *, context: str = "") -> Model:
        if name in self.models:
            return self.models[name]
        # izinkan nama pendek kalau unik: ref('customer')
        matches = [m for m in self.models.values() if m.name == name]
        if len(matches) == 1:
            return matches[0]
        where = f" (di {context})" if context else ""
        if len(matches) > 1:
            raise ConfigError(f"ref('{name}'){where} ambigu, pakai 'layer.nama': {[m.id for m in matches]}")
        raise ConfigError(
            f"ref('{name}'){where} tidak ditemukan. Model tersedia: {sorted(self.models)}{self._broken_hint()}"
        )

    # -------------------------------------------------------------- dependency
    def model_upstream_models(self, model: Model) -> list[Model]:
        return [self.find_model(r, context=model.sql_file) for r in model.refs]

    def model_upstream_bronze(self, model: Model) -> list[Table]:
        return [self.find_bronze(s, t) for s, t in model.sources]

    def validate(self, dag_ids: Iterable[str] | None = None) -> None:
        """Cek semua ref()/source() bisa di-resolve dan tidak ada dependency melingkar."""
        if dag_ids is None:
            roots = list(self.models.values())
        else:
            roots = [m for d in dag_ids for m in self.transforms[d].models]
        graph: dict[str, set[str]] = {}
        todo = list(roots)
        while todo:
            model = todo.pop()
            if model.id in graph:
                continue
            ups = self.model_upstream_models(model)
            if model.id in {u.id for u in ups}:
                raise ConfigError(f"{model.sql_file}: model tidak boleh ref ke dirinya sendiri")
            self.model_upstream_bronze(model)  # validasi source() ada
            graph[model.id] = {u.id for u in ups}
            todo.extend(ups)
        try:
            list(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise ConfigError(f"Dependency model melingkar (cycle): {' -> '.join(exc.args[1])}") from exc

    def external_assets(self, dag: TransformDag) -> list[tuple[str, str]]:
        """Asset (name, uri) di luar DAG ini yang jadi pemicu DAG transform (schedule: assets)."""
        own = {m.id for m in dag.models}
        assets: dict[str, str] = {}
        for model in dag.models:
            for tbl in self.model_upstream_bronze(model):
                assets[bronze_asset_name(tbl)] = self.bronze_relation(tbl).uri
            for up in self.model_upstream_models(model):
                if up.id not in own:
                    assets[up.id] = self.model_relation(up).uri
        return sorted(assets.items())

    def internal_edges(self, dag: TransformDag) -> list[tuple[str, str]]:
        own = {m.id for m in dag.models}
        edges = []
        for model in dag.models:
            for up in self.model_upstream_models(model):
                if up.id in own:
                    edges.append((up.id, model.id))
        return edges

    # ------------------------------------------------------------------ macros
    def jinja_macros(self) -> dict[str, Any]:
        tz = self.platform.timezone

        def ref(name: str) -> Relation:
            return self.model_relation(self.find_model(name))

        def source(source_name: str, table_name: str) -> Relation:
            return self.bronze_relation(self.find_bronze(source_name, table_name))

        def relation(model_id: str) -> Relation:
            return self.model_relation(self.models[model_id])

        def as_local_datetime(column: str) -> str:
            # dlt menyimpan datetime SQL Server (tanpa timezone) sebagai TIMESTAMP berlabel UTC.
            # Nilainya sebenarnya jam dinding sumber -> ambil balik sebagai DATETIME.
            return f"DATETIME({column}, 'UTC')"

        def as_utc_timestamp(column: str, source_timezone: str = tz) -> str:
            # jam dinding sumber (mis. WIB) -> instant TIMESTAMP yang benar
            return f"TIMESTAMP(DATETIME({column}, 'UTC'), '{source_timezone}')"

        return {
            "ref": ref,
            "source": source,
            "relation": relation,
            "as_local_datetime": as_local_datetime,
            "as_utc_timestamp": as_utc_timestamp,
            "tz": tz,  # {{ tz }} -> 'Asia/Jakarta', mis. CURRENT_DATE('{{ tz }}')
        }

    @staticmethod
    def model_query_template(model: Model) -> str:
        """String template untuk BigQueryInsertJobOperator (dirender Airflow saat task jalan)."""
        return "{% set this = relation('" + model.id + "') %}{% include '" + model.sql_file + "' %}"


def render_model_sql(catalog: Catalog, model_id: str, params: dict[str, Any] | None = None) -> str:
    """Render SQL model persis seperti di Airflow (untuk preview / debug / test)."""
    import jinja2

    model = catalog.find_model(model_id)
    dag = catalog.transforms[model.dag_id]
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader([str(sql_dir())]),
        undefined=jinja2.StrictUndefined,
    )
    env.globals.update(catalog.jinja_macros())
    merged: dict[str, Any] = {"lookback_days": dag.lookback_days, "full_refresh": False}
    merged.update(params or {})
    return env.from_string(Catalog.model_query_template(model)).render(params=merged)
