import pytest

from medallion import config
from medallion.validate import check


def test_config_repo_valid():
    assert check() == []


def test_key_salah_ditolak(tmp_path, monkeypatch):
    (tmp_path / "sources").mkdir()
    (tmp_path / "sources" / "x.yml").write_text(
        "conn_id: c\ndatabase: d\nschema: dbo\ndataset: ds\nschedule: null\n"
        "tables:\n  - table: T\n    cursr: ModifiedDate\n"
    )
    monkeypatch.setattr(config, "CONFIG_DIR", tmp_path)
    with pytest.raises(ValueError, match="cursr"):
        config.load_source("x")
