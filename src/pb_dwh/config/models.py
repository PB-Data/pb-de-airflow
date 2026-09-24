"""Model konfigurasi tabel dan source, beserta validasinya.

Tujuan file ini: semua salah tulis di YAML ketahuan DI SINI, dengan pesan yang
menyebut nama file dan nama tabelnya. Bukan meledak di tengah run setelah 40
menit extract, dan bukan diam-diam menghasilkan data yang salah.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Mapping

SECONDS_PER_DAY = 86_400

WRITE_DISPOSITIONS = ("merge", "replace", "append")
CURSOR_TYPES = ("datetime", "integer")
REFLECTION_LEVELS = ("minimal", "full", "full_with_precision")


class ConfigError(ValueError):
    """Config YAML tidak valid.

    Pesannya ditulis untuk dibaca manusia, bukan untuk di-parse program.
    """


@dataclass(frozen=True)
class TableConfig:
    """Satu tabel di dalam satu source."""

    name: str
    primary_key: tuple[str, ...] = ()
    cursor: str | None = None
    cursor_type: str = "datetime"
    lookback_days: int = 0
    lookback: int = 0
    initial_value: Any = None
    write_disposition: str = "merge"
    chunk_size: int = 50_000
    reflection_level: str = "minimal"
    exclude_columns: tuple[str, ...] = ()
    column_overrides: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @property
    def incremental_lag(self) -> float | None:
        """Lookback window dalam satuan yang dimengerti `dlt.sources.incremental`.

        Untuk cursor datetime, `lag` dihitung dalam detik. Untuk cursor integer,
        `lag` dalam satuan nilai cursor itu sendiri (mis. mundur 1000 id).

        Mengembalikan None kalau tidak ada lookback: dlt memperlakukan lag=None
        sebagai "tidak usah mundur".
        """
        if not self.cursor:
            return None
        if self.cursor_type == "datetime":
            return self.lookback_days * SECONDS_PER_DAY or None
        return self.lookback or None


@dataclass(frozen=True)
class SourceConfig:
    """Satu database source = satu file YAML = satu schema = satu DAG."""

    name: str
    env_prefix: str
    schema: str
    bq_dataset: str
    tables: tuple[TableConfig, ...]
    backend: str = "pyarrow"
    loader_file_format: str = "parquet"
    staging_prefix: str = "dlt-staging"
    source_file: str = "<memory>"

    # Setelan Airflow. Ada di sini supaya jadwal sebuah source bisa diubah
    # tanpa menyentuh file Python sama sekali.
    schedule: str = "0 1 * * *"
    start_date: date = date(2026, 1, 1)
    pool: str = "dlt_ingest"
    task_timeout_hours: int = 4
    retries: int = 1

    @property
    def pipeline_name(self) -> str:
        return f"bronze_{self.name}"

    @property
    def dag_id(self) -> str:
        return f"bronze_ingest_{self.name}"

    @property
    def table_names(self) -> list[str]:
        return [t.name for t in self.tables]

    def table(self, name: str) -> TableConfig:
        for t in self.tables:
            if t.name == name:
                return t
        terdaftar = ", ".join(self.table_names) or "(kosong)"
        raise ConfigError(
            f"Tabel '{name}' tidak ada di {self.source_file}. Yang terdaftar: {terdaftar}"
        )


# ---------------------------------------------------------------------------
# Validasi
# ---------------------------------------------------------------------------


def fail(source_file: str, table: str | None, message: str) -> None:
    lokasi = source_file if table is None else f"{source_file}, tabel '{table}'"
    raise ConfigError(f"[{lokasi}] {message}")


def as_column_tuple(
    value: Any, source_file: str, table: str, field_name: str
) -> tuple[str, ...]:
    """Terima string tunggal maupun list.

    `primary_key: order_id` sama sahnya dengan `primary_key: [order_id]`. Ini
    kesalahan tulis yang paling sering, tidak perlu dihukum dengan error.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        for item in value:
            if not isinstance(item, str) or not item.strip():
                fail(
                    source_file,
                    table,
                    f"{field_name} berisi nilai yang bukan nama kolom: {item!r}",
                )
        return tuple(value)
    fail(
        source_file,
        table,
        f"{field_name} harus nama kolom atau list nama kolom, bukan {type(value).__name__}",
    )
    return ()  # tidak tercapai; fail() selalu melempar


def normalise_initial_value(
    value: Any, cursor_type: str, table_name: str, source_file: str
) -> Any:
    """YAML mem-parse `2024-01-01` jadi objek `date`.

    dlt butuh `datetime` untuk dibandingkan dengan kolom datetime2, jadi
    konversinya dilakukan di sini sekali, bukan di setiap pemakaian.
    """
    if value is None:
        return None

    if cursor_type == "datetime":
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                fail(
                    source_file,
                    table_name,
                    f"initial_value '{value}' bukan tanggal ISO yang sah. "
                    "Contoh yang benar: 2024-01-01 atau 2024-01-01T00:00:00",
                )
        fail(
            source_file,
            table_name,
            f"initial_value untuk cursor_type datetime harus tanggal, bukan {type(value).__name__}",
        )
    else:
        if isinstance(value, bool) or not isinstance(value, int):
            fail(
                source_file,
                table_name,
                f"initial_value untuk cursor_type integer harus angka bulat, bukan {value!r}",
            )
    return value


def validate_table(table: TableConfig, source_file: str) -> None:
    """Aturan yang kalau dilanggar akan merusak data secara diam-diam."""
    t = table.name

    if not t or not t.strip():
        fail(source_file, None, "ada tabel tanpa 'name'")

    if table.write_disposition not in WRITE_DISPOSITIONS:
        fail(
            source_file,
            t,
            f"write_disposition '{table.write_disposition}' tidak dikenal. "
            f"Pilih salah satu: {', '.join(WRITE_DISPOSITIONS)}",
        )

    if table.reflection_level not in REFLECTION_LEVELS:
        fail(
            source_file,
            t,
            f"reflection_level '{table.reflection_level}' tidak dikenal. "
            f"Pilih salah satu: {', '.join(REFLECTION_LEVELS)}",
        )

    if table.cursor_type not in CURSOR_TYPES:
        fail(
            source_file,
            t,
            f"cursor_type '{table.cursor_type}' tidak dikenal. "
            f"Pilih salah satu: {', '.join(CURSOR_TYPES)}",
        )

    # merge tanpa primary key: dlt tidak tahu baris mana yang harus ditimpa,
    # hasilnya duplikat yang menumpuk tiap hari tanpa satu pun pesan error.
    if table.write_disposition == "merge" and not table.primary_key:
        fail(
            source_file,
            t,
            "write_disposition 'merge' wajib punya primary_key. Tanpa itu baris "
            "akan menumpuk jadi duplikat tanpa pesan error.",
        )

    if table.write_disposition == "replace" and table.cursor:
        fail(
            source_file,
            t,
            "write_disposition 'replace' tidak boleh dipakai bersama cursor. "
            "'replace' menulis ulang seluruh tabel sehingga incremental jadi sia-sia. "
            "Pilih salah satu: hapus cursor, atau ganti ke merge.",
        )

    if table.lookback_days and not table.cursor:
        fail(
            source_file,
            t,
            "lookback_days butuh cursor. Tentukan kolom cursor-nya, atau hapus lookback_days.",
        )

    if table.lookback and not table.cursor:
        fail(
            source_file,
            t,
            "lookback butuh cursor. Tentukan kolom cursor-nya, atau hapus lookback.",
        )

    if table.cursor_type == "integer" and table.lookback_days:
        fail(
            source_file,
            t,
            "cursor_type 'integer' tidak mengenal lookback_days (satuannya hari). "
            "Pakai 'lookback' yang satuannya nilai cursor.",
        )

    if table.cursor_type == "datetime" and table.lookback:
        fail(
            source_file,
            t,
            "cursor_type 'datetime' memakai 'lookback_days', bukan 'lookback'.",
        )

    if table.lookback_days < 0 or table.lookback < 0:
        fail(source_file, t, "lookback tidak boleh negatif.")

    if table.chunk_size <= 0:
        fail(source_file, t, "chunk_size harus lebih besar dari 0.")

    tumpang_tindih = set(table.exclude_columns) & set(table.primary_key)
    if tumpang_tindih:
        fail(
            source_file,
            t,
            f"kolom {sorted(tumpang_tindih)} ada di exclude_columns sekaligus primary_key. "
            "Primary key tidak boleh dibuang karena merge akan gagal.",
        )

    if table.cursor and table.cursor in table.exclude_columns:
        fail(
            source_file,
            t,
            f"kolom cursor '{table.cursor}' tidak boleh masuk exclude_columns.",
        )

    for kolom, hint in table.column_overrides.items():
        if not isinstance(hint, dict):
            fail(
                source_file,
                t,
                f"column_overrides.{kolom} harus berupa mapping. "
                "Contoh: {data_type: decimal, precision: 38, scale: 9}",
            )
        if "data_type" not in hint:
            fail(source_file, t, f"column_overrides.{kolom} wajib punya 'data_type'.")


def validate_source(source: SourceConfig) -> None:
    f = source.source_file

    if not source.name or not source.name.replace("_", "").isalnum():
        fail(
            f,
            None,
            f"source.name '{source.name}' harus huruf/angka/underscore saja "
            "karena dipakai sebagai dag_id.",
        )

    if not source.env_prefix:
        fail(
            f,
            None,
            "source.env_prefix wajib diisi (mis. MSSQL). Dipakai untuk membaca "
            "MSSQL_HOST, MSSQL_USER, dan seterusnya.",
        )

    if not source.schema:
        fail(f, None, "source.schema wajib diisi (mis. dbo).")

    if not source.bq_dataset:
        fail(f, None, "source.bq_dataset wajib diisi; ini dataset tujuan di BigQuery.")

    if len(source.schedule.split()) != 5:
        fail(
            f,
            None,
            f"source.schedule '{source.schedule}' bukan ekspresi cron 5 bagian. "
            "Contoh: '0 1 * * *' untuk setiap hari pukul 01:00.",
        )

    if source.task_timeout_hours <= 0:
        fail(f, None, "source.task_timeout_hours harus lebih besar dari 0.")

    if source.retries < 0:
        fail(f, None, "source.retries tidak boleh negatif.")

    if not source.tables:
        fail(f, None, "tidak ada tabel sama sekali di 'tables:'.")

    hitung: dict[str, int] = {}
    for t in source.tables:
        hitung[t.name] = hitung.get(t.name, 0) + 1
    duplikat = sorted(n for n, c in hitung.items() if c > 1)
    if duplikat:
        fail(f, None, f"nama tabel muncul lebih dari sekali: {', '.join(duplikat)}")

    for t in source.tables:
        validate_table(t, f)
