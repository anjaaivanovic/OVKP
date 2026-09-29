# Network Traffic Anomaly Detection

Automated data pipeline for processing and analyzing the UNSW-NB15 dataset of network traffic and security incidents. 
The pipeline loads raw CSV files, cleans and transforms the data using PySpark, runs SQL analysis, and produces visualizations that help identify suspicious patterns in the traffic (e.g. IP addresses with the most suspicious connections, port/protocol combinations typical of Reconnaissance attacks, the distribution of attacks over time...).

Dataset: [UNSW-NB15 dataset](https://research.unsw.edu.au/projects/unsw-nb15-dataset)

> **Note:** the original download link points to SharePoint, which requires authentication and is not suitable for automated downloading from code (e.g. in a GitHub Actions environment). 
> Because of this, and because the raw CSV files exceed GitHub's 100MB per-file limit for regular commits, the dataset files are instead attached as assets to a [GitHub Release](https://github.com/anjaaivanovic/OVKP/releases/tag/data) in this repository. 
> `download.py` fetches them directly from those stable, publicly accessible release URLs into `data/raw/`.

## Repository structure

```
project/
├── data/
│   ├── raw/              # downloaded raw CSV files
│   └── processed/        # cleaned data in Parquet format
├── notebooks/
│   └── analysis.ipynb     # visualization of results
├── src/
│   ├── logger.py         # log helper
│   ├── download.py       # downloads raw data from GitHub Releases
│   ├── transform.py      # cleaning and transformation (PySpark)
│   ├── analysis.py       # Spark SQL analysis
│   └── pipeline.py       # orchestrates all steps
├── results/
│   └── *.parquet         # SQL analysis results
├── .github/
│   └── workflows/
│       └── pipeline.yml  # GitHub Actions workflow
├── requirements.txt
└── README.md
```

## Instructions for running

### 1. Environment (recommended: GitHub Codespace)

Open the repository in a GitHub Codespace (or locally, with Python 3.11+ and Java 17 installed).

Install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Running the whole pipeline

```bash
python src/pipeline.py
```

This command runs `download.py` -> `transform.py` -> `analysis.py` and prints the log of each step to the console, in the following format:

```
[2024-01-01 08:00] Downloading data...
[2024-01-01 08:02] Validation and cleaning...
[2024-01-01 08:05] Running SQL analysis...
[2024-01-01 08:08] Results written to results/
```

Results (Parquet files) are written to `results/`.

### 3. Running the steps individually (optional)

```bash
python src/download.py
python src/transform.py
python src/analysis.py
```

### 4. Visualization

```bash
jupyter lab notebooks/analiza.ipynb
```

Run all cells of the notebook. The notebook loads the results from `results/` and displays the charts.

### 5. Running through GitHub Actions

In the **Actions** tab, select the **Network Traffic Anomaly Detection Pipeline** workflow and run it manually (**Run workflow**). Once finished, the results are available for download as an Actions Artifact (`results`).

## Description of the pipeline stages

### `download.py` — Data download

Downloads the CSV files from this repository's [GitHub Release](https://github.com/anjaaivanovic/OVKP/releases/tag/data) assets into `data/raw/`. 
It is kept separate from the rest of the pipeline so it can easily be pointed at a different source in the future, should the original dataset become available without authentication.

### `transform.py` — Validation and cleaning

- Loads column metadata (`NUSW-NB15_features.csv`) and uses it to cast columns to the appropriate types.
- Prints the number of null values per column, as well as the row count before/after cleaning.
- Removes rows with invalid values (negative connection duration, null IP addresses).
- Adds new columns:
  - `traffic_volume`: `low` / `medium` / `high`, based on total number of bytes
  - `time_bucket`: `business_hours` / `off_hours` / `night`, based on traffic timestamp
- Fills missing `attack_cat` values with `"Normal"` and explicitly casts `Label` to `int`.
- Writes the cleaned data as Parquet, partitioned by `attack_cat`, into `data/processed/`.

### `analysis.py` — Spark SQL analysis

Registers the cleaned data as a temporary SQL view (`traffic`) and runs 7 queries, each of which writes a separate Parquet file into `results/`:

| # | Query | File |
|---|------|------|
| 1 | Top 10 source IP addresses by number of suspicious connections (Label=1) | `top_suspicious_ips.parquet` |
| 2 | Most common protocol per attack category (window function `ROW_NUMBER`) | `top_protocol_per_attack_cat.parquet` |
| 3 | Attack count over time — rolling count per hour (window function `avg` over the last 3 windows) | `attacks_over_time.parquet` |
| 4 | Port + protocol combinations most commonly associated with Reconnaissance attacks | `reconnaissance_port_proto.parquet` |
| 5 | Ratio of normal to attack traffic by hour of day | `normal_vs_attack_by_hour.parquet` |
| 6 | Traffic category distribution (for pie chart) | `attack_cat_distribution.parquet` |
| 7 | Attack intensity by hour and day of week (for heatmap) | `attack_intensity_by_hour_day.parquet` |

### `pipeline.py` — Orchestration

Sequentially runs `download.py` → `transform.py` → `analysis.py` as steps, logging each one in a standard format (`[timestamp] description`). If a step fails (non-zero exit code), the pipeline stops immediately instead of silently continuing to the next step with invalid data.

### `notebooks/analysis.ipynb` — Visualization

Loads all 7 results from `results/` and displays:

- Bar chart: top 10 IP addresses by number of suspicious connections
- Table: most common protocol per attack category
- Line chart: attack count over time, with rolling average
- Table: riskiest port/protocol combinations (Reconnaissance)
- Grouped bar chart: ratio of normal to attack traffic by hour
- Pie chart: traffic category distribution
- Heatmap: attack intensity by hour and day of week

### `.github/workflows/pipeline.yml` — Automation (CI)

GitHub Actions workflow that is triggered manually (`workflow_dispatch`), installs dependencies from `requirements.txt`, runs `src/pipeline.py`, and saves the contents of `results/` as an Actions Artifact, available for download from the workflow run page.