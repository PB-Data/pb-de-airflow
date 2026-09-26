"""Adapter reflection/query SQLAlchemy untuk dlt ``sql_table``.

* ``nullable_table_adapter``  -> semua kolom bronze dibuat NULLABLE (bronze harus toleran:
  kolom NOT NULL di sumber yang suatu saat jadi NULL tidak bikin load gagal).
* ``mssql_type_adapter``      -> perbaiki tipe SQL Server yang tidak dikenali dlt
  (MONEY/SMALLMONEY tidak punya tipe -> precision bisa beda-beda antar chunk).
* ``mssql_query_adapter``     -> CAST kolom SQL_VARIANT ke NVARCHAR supaya konsisten.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

_MSSQL_NUMERIC_OVERRIDES = {
    "MONEY": (19, 4),
    "SMALLMONEY": (10, 4),
}
_MSSQL_CAST_TO_STRING = {"SQL_VARIANT"}


def nullable_table_adapter(table: sa.Table) -> None:
    for column in table.columns:
        column.nullable = True


def mssql_type_adapter(sql_type: Any) -> Any:
    """Dipanggil dlt per kolom. WAJIB return tipe (bukan None) kalau tidak diubah."""
    name = type(sql_type).__name__.upper()
    if name in _MSSQL_NUMERIC_OVERRIDES:
        precision, scale = _MSSQL_NUMERIC_OVERRIDES[name]
        return sa.Numeric(precision, scale)
    if name in _MSSQL_CAST_TO_STRING:
        return sa.String()
    return sql_type


def mssql_query_adapter(query: Any, table: sa.Table) -> Any:
    variant_names = {c.name for c in table.columns if type(c.type).__name__.upper() in _MSSQL_CAST_TO_STRING}
    if not variant_names:
        return query
    selected = [
        sa.cast(c, sa.NVARCHAR(4000)).label(c.name) if c.name in variant_names else c
        for c in query.selected_columns
    ]
    return query.with_only_columns(*selected, maintain_column_froms=True)
