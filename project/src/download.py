from pathlib import Path
import shutil
from logger import log

BASE_DIR = Path(__file__).resolve().parent.parent

SOURCE_DIR = BASE_DIR / "source_files"
RAW_DIR = BASE_DIR / "data" / "raw"

RAW_DIR.mkdir(parents=True, exist_ok=True)


for file in SOURCE_DIR.glob("*.csv"):
    shutil.copy(file, RAW_DIR / file.name)

log("Raw files prepared.")