import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"
FIXTURES = ROOT / "tests" / "fixtures"
sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture
def config_dir(tmp_path, monkeypatch):
    """Folder config kosong (platform.yml minimal) untuk test yang butuh YAML custom."""
    cfg = tmp_path / "configs"
    (cfg / "sources").mkdir(parents=True)
    (cfg / "transforms").mkdir()
    (cfg / "platform.yml").write_text(
        "gcp: {bq_project: test-project, bq_location: asia-southeast2}\n"
        f"lake: {{bucket_url: '{(tmp_path / 'lake').as_uri()}'}}\n"
        "runtime: {timezone: Asia/Jakarta}\n"
    )
    sql = tmp_path / "sql"
    (sql / "silver").mkdir(parents=True)
    (sql / "gold").mkdir()
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(cfg))
    monkeypatch.setenv("MEDALLION_SQL_DIR", str(sql))
    return cfg
