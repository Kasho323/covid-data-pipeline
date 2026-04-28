# COVID Data Pipeline

[![CI](https://github.com/Kasho323/covid-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Kasho323/covid-data-pipeline/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

End-to-end data pipeline for UK COVID-19 case data: ingest from CSV or the official UK Coronavirus API, clean and filter, persist to SQLite, summarize, and visualize. CLI-driven with an optional Tkinter UI.

This repository was extracted from a larger coursework project at the University of Warwick and reworked as a standalone showcase of pragmatic data engineering.

---

## What it does

1. **Load** — read records from a local CSV or pull live data from the UK Coronavirus API.
2. **Clean** — coerce types, drop malformed dates, handle missing values, clamp negatives.
3. **Filter** — by country, by date range, with consistent CLI flags.
4. **Summarize** — mean / min / max statistics, top-N countries, time-series trends, grouped aggregates.
5. **Persist** — write filtered rows to SQLite with full CRUD (`insert`, `list`, `update`, `delete`).
6. **Output** — JSON, CSV, matplotlib chart, or interactive Tkinter UI.

---

## Why this design

A few non-trivial decisions worth surfacing:

- **SQLite over Postgres.** The dataset fits in memory; a file-based store keeps the project zero-infra and reproducible. The schema (countries / metrics / observations / activity_logs) is normalised so it could be lifted to Postgres unchanged if the data outgrew the box.
- **List-of-dicts over DataFrame as the core record format.** Pandas is used at the boundaries (visualisation) but not as the canonical type, so the pipeline runs even if pandas is missing — visualisation just degrades gracefully with a warning.
- **CLI as the primary interface.** Every operation (load, filter, persist, list, update) is a flag combination on a single entry point, which makes integration testing straightforward. The Tkinter UI is a thin wrapper.

---

## Quick start

```bash
pip install -r requirements.txt

# Default run — reads test_data.csv, prints summary, persists to SQLite
python -m covid_pipeline.main

# Filter by country and preview as a table
python -m covid_pipeline.main --country UK --table

# Pull live data from the UK Coronavirus API
python -m covid_pipeline.main --source uk-covid --api-area-name England --api-limit 50

# Visualise and save a chart
python -m covid_pipeline.main --country UK --visualize --plot-output uk_cases.png

# SQLite CRUD examples
python -m covid_pipeline.main --db-action list --db-limit 10
python -m covid_pipeline.main --db-action update --record-id 1 --new-value 42
python -m covid_pipeline.main --db-action delete --record-id 1

# Launch the GUI
python -m covid_pipeline.main --ui
```

---

## Database schema

Normalised dimensional design:

- `countries` — `id` PK, `code` UNIQUE (e.g., "UK"), `name`
- `metrics` — `id` PK, `metric_name` UNIQUE (e.g., `newCasesByPublishDate`), `alias`, `unit`
- `observations` — fact table: `id`, `country_id` FK, `metric_id` FK, `date`, `metric_value`, `source`, `raw_json`, `created_at`
- `activity_logs` — optional audit trail of pipeline actions

This keeps `observations` narrow and lets country / metric attributes evolve without rewriting fact rows.

---

## Tests

```bash
pytest -q
```

CI runs lint (ruff) and tests on Python 3.11 and 3.12. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

---

## Project layout

```
covid-data-pipeline/
├── covid_pipeline/
│   ├── __init__.py
│   ├── main.py          # pipeline + CLI + visualisation + SQLite + UI
│   ├── test_main.py     # pytest suite
│   └── test_data.csv    # default dataset
├── .github/workflows/ci.yml
├── requirements.txt
└── LICENSE
```

---

## License

MIT — see [LICENSE](LICENSE).
