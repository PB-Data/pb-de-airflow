"""Supaya `pytest` menemukan paket `pb_dwh` tanpa perlu `pip install`.

Di dalam container hal ini diurus oleh PYTHONPATH yang di-set di
docker-compose.yml. Di laptop, file ini yang mengurusnya.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
