"""Penyesuaian tipe data SQL Server sebelum masuk Parquet dan BigQuery.

Ada dua callback yang dipakai `sql_database`:

* `type_adapter_callback(sql_type)` menukar tipe SQLAlchemy satu per satu.
  Dipakai untuk tipe yang membuat PyArrow gagal membangun schema.
* `table_adapter_callback(table)` menerima objek Table dan boleh memodifikasinya.
  Dipakai untuk membuang kolom, karena `sql_database()` tidak punya parameter
  `excluded_columns` (yang punya cuma `sql_table()`).

Catatan jujur soal isi file ini: sebagian penanganan di bawah bersifat
defensif, bukan karena kami sudah melihatnya gagal. Setiap aturan diberi
keterangan mana yang pasti bermasalah dan mana yang sekadar jaga-jaga, supaya
nanti gampang dicabut kalau ternyata tidak perlu.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterable, Mapping

import sqlalchemy as sa

logger = logging.getLogger(__name__)

# BigQuery NUMERIC menampung precision 38 / scale 9.
# Di atas itu BigQuery butuh BIGNUMERIC (precision 76 / scale 38).
BQ_NUMERIC_PRECISION = 38
BQ_NUMERIC_SCALE = 9

# Parquet decimal128 tidak bisa menyimpan precision di atas 38. SQL Server
# sendiri membatasi DECIMAL di precision 38, jadi batas ini praktis tidak
# pernah tersentuh dari sisi source; tetap dicek supaya kalau suatu saat
# source-nya bukan SQL Server, gagalnya jelas.
PARQUET_MAX_PRECISION = 38


def build_type_adapter(
    warn_on_bignumeric: bool = True,
) -> Callable[[Any], Any]:
    """Bikin callback penyesuai tipe.

    Mengembalikan tipe SQLAlchemy baru, atau None supaya dlt menebak tipe dari
    datanya sendiri.
    """

    def adapt(sql_type: Any) -> Any:
        # --- uniqueidentifier ------------------------------------------------
        # dlt versi sekarang sudah memetakan UNIQUEIDENTIFIER ke text sendiri.
        # Baris ini defensif: memaksa String(36) membuat schema PyArrow
        # deterministik dan tidak bergantung pada driver ODBC mengembalikan
        # objek uuid.UUID atau str.
        nama_tipe = type(sql_type).__name__.upper()
        if nama_tipe == "UNIQUEIDENTIFIER":
            return sa.String(36)

        # --- decimal / numeric ----------------------------------------------
        # sa.Float ikut turunan sa.Numeric, jadi harus dikecualikan lebih dulu.
        if isinstance(sql_type, sa.Numeric) and not isinstance(sql_type, sa.Float):
            precision = sql_type.precision
            scale = sql_type.scale or 0

            # INI yang benar-benar bikin gagal: PyArrow tidak bisa membangun
            # decimal128 tanpa tahu precision-nya. Terjadi pada kolom DECIMAL
            # yang dideklarasikan tanpa presisi eksplisit.
            if precision is None:
                logger.warning(
                    "Kolom decimal tanpa precision; dipaksa ke NUMERIC(%s, %s). "
                    "Kalau nilainya butuh lebih, tulis column_overrides di config.",
                    BQ_NUMERIC_PRECISION,
                    BQ_NUMERIC_SCALE,
                )
                return sa.Numeric(precision=BQ_NUMERIC_PRECISION, scale=BQ_NUMERIC_SCALE)

            if precision > PARQUET_MAX_PRECISION:
                raise ValueError(
                    f"Kolom decimal dengan precision {precision} tidak muat di Parquet "
                    f"(maksimum {PARQUET_MAX_PRECISION}). Pakai column_overrides untuk "
                    "memetakannya ke text, lalu CAST di layer silver."
                )

            # Muat di NUMERIC biasa: biarkan apa adanya.
            if scale <= BQ_NUMERIC_SCALE:
                return sql_type

            # Precision masih aman tapi scale melebihi NUMERIC. dlt akan
            # memilih BIGNUMERIC berdasarkan precision/scale ini. Tipe tidak
            # diubah, hanya dicatat supaya tim tahu kenapa kolomnya jadi
            # BIGNUMERIC di BigQuery.
            if warn_on_bignumeric:
                logger.info(
                    "Decimal(%s, %s) melebihi NUMERIC BigQuery (scale > %s); "
                    "kolom akan mendarat sebagai BIGNUMERIC.",
                    precision,
                    scale,
                    BQ_NUMERIC_SCALE,
                )
            return sql_type

        # Tipe lain dibiarkan. Khususnya datetime2: presisinya 100 nanodetik,
        # sedangkan BigQuery TIMESTAMP hanya mikrodetik, jadi digit terakhir
        # terpotong. Itu diterima apa adanya dan dicatat di README, bukan
        # diakali di sini.
        return None

    return adapt


def build_table_adapter(
    exclude_per_table: Mapping[str, Iterable[str]],
) -> Callable[[Any], Any]:
    """Bikin callback pembuang kolom, per nama tabel.

    `exclude_per_table` dipetakan dari config: {nama_tabel: [kolom, ...]}.
    Kolom yang tidak ditemukan diabaikan dengan peringatan, bukan error, supaya
    kolom yang sudah terlanjur di-drop di source tidak menghentikan seluruh run.
    """
    dibuang = {nama: set(kolom) for nama, kolom in exclude_per_table.items() if kolom}

    def adapt(table: Any) -> Any:
        target = dibuang.get(table.name)
        if not target:
            return table

        ada = {c.name for c in table.columns}
        tidak_ketemu = target - ada
        if tidak_ketemu:
            logger.warning(
                "Tabel %s: kolom di exclude_columns tidak ada di source dan diabaikan: %s",
                table.name,
                ", ".join(sorted(tidak_ketemu)),
            )

        for kolom in list(table.columns):
            if kolom.name in target:
                table._columns.remove(kolom)

        return table

    return adapt
