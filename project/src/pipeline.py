import subprocess
import sys
from pathlib import Path

from logger import log


BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"
RESULTS_DIR = BASE_DIR / "results"

STEPS = [
    ("Downloading data", SRC_DIR / "download.py"),
    ("Validation and cleaning", SRC_DIR / "transform.py"),
    ("Running SQL analysis", SRC_DIR / "analysis.py"),
]


def run_step(description: str, script_path: Path):
    log(f"{description}...")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(SRC_DIR),
    )

    if result.returncode != 0:
        log(f"ERROR in step '{description}' (exit code {result.returncode}). Stopping pipeline.")
        sys.exit(result.returncode)

    log(f"{description} - done.")


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    for description, script_path in STEPS:
        run_step(description, script_path)

    log(f"Results written to {RESULTS_DIR}")


if __name__ == "__main__":
    main()