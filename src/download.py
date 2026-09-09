import urllib.request
import urllib.error
from pathlib import Path

from logger import log


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

RAW_DIR.mkdir(parents=True, exist_ok=True)

GITHUB_OWNER = "anjaaivanovic"
GITHUB_REPO = "OVKP"
RELEASE_TAG = "data"

BASE_URL = (
    f"https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}"
    f"/releases/download/{RELEASE_TAG}"
)

FILES = [
    "UNSW-NB15_1.csv",
    "UNSW-NB15_2.csv",
    "UNSW-NB15_3.csv",
    "UNSW-NB15_4.csv",
    "NUSW-NB15_features.csv",
]


def download_file(filename: str):
    target_path = RAW_DIR / filename
    url = f"{BASE_URL}/{filename}"

    log(f"Downloading {filename} from {url}...")

    try:
        urllib.request.urlretrieve(url, target_path)
    except urllib.error.HTTPError as e:
        log(f"ERROR: could not download {filename} (HTTP {e.code}). Check the release tag/asset name.")
        raise
    except urllib.error.URLError as e:
        log(f"ERROR: network issue while downloading {filename}: {e.reason}")
        raise

    size_mb = target_path.stat().st_size / (1024 * 1024)
    log(f"Saved {filename} ({size_mb:.1f} MB) to {target_path}")


for filename in FILES:
    download_file(filename)

log("Raw files prepared.")