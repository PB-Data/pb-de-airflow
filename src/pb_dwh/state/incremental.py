"""Menerjemahkan setelan cursor di config menjadi objek incremental dlt."""

from __future__ import annotations

import logging

import dlt

from pb_dwh.config.models import TableConfig

logger = logging.getLogger(__name__)


def build_incremental(table: TableConfig):
    """Bikin `dlt.sources.incremental` dari config tabel, atau None.

    Tentang lookback: dipakai parameter `lag` bawaan dlt, bukan menghitung
    tanggal sendiri. dlt yang menggeser `last_value` mundur sebelum menyusun
    query, dan filternya dikirim sebagai WHERE ke SQL Server, bukan disaring di
    Python. Untuk tabel puluhan juta baris, perbedaannya besar.

    Lookback memang membuat baris yang sama terambil ulang di beberapa run. Itu
    disengaja: kombinasi `write_disposition: merge` dan `primary_key` membuat
    baris berulang ditimpa, bukan diduplikasi.
    """
    if not table.cursor:
        return None

    lag = table.incremental_lag

    incremental = dlt.sources.incremental(
        cursor_path=table.cursor,
        initial_value=table.initial_value,
        lag=lag,
        last_value_func=max,
        row_order="asc",
    )

    if lag:
        satuan = "hari" if table.cursor_type == "datetime" else "nilai cursor"
        besaran = table.lookback_days if table.cursor_type == "datetime" else table.lookback
        logger.info(
            "Tabel %s: incremental pada '%s' dengan lookback %s %s",
            table.name,
            table.cursor,
            besaran,
            satuan,
        )
    else:
        logger.info(
            "Tabel %s: incremental pada '%s' tanpa lookback", table.name, table.cursor
        )

    return incremental
