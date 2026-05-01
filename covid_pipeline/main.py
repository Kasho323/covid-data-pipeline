"""Utilities for the PAI individual assignment Task 1 pipeline."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import requests


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATASET = "test_data.csv"
UK_COVID_API_ENDPOINT = "https://api.coronavirus.data.gov.uk/v1/data"
DEFAULT_DB_PATH = PROJECT_ROOT / "covid_data.sqlite3"
DEFAULT_DB_TABLE = "observations"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
)


def _resolve_dataset(filepath: str) -> Path:
    """Return a Path to the dataset, trying both CWD and module directory."""
    candidate = Path(filepath)
    if candidate.exists():
        return candidate
    fallback = PROJECT_ROOT / filepath
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"Dataset '{filepath}' not found. Looked in '{candidate}' and '{fallback}'.")


def _coerce_value(value: Any) -> Any:
    """Best-effort conversion of CSV text to int/float, else trimmed string."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace(",", "")
    try:
        return int(normalized)
    except ValueError:
        pass
    try:
        return float(normalized)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text)
            return parsed.date() if "T" not in text else parsed
        except ValueError:
            return text


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def serialize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {key: _serialize_value(val) for key, val in record.items()}


def load_data(filepath: str) -> List[Dict[str, Any]]:
    """Load CSV data into a list of dicts with numeric coercion where possible."""

    resolved = _resolve_dataset(filepath)
    rows: List[Dict[str, Any]] = []
    with resolved.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for record in reader:
            rows.append({field: _coerce_value(value) for field, value in record.items()})
    return rows


def clean_data(
    data: List[Dict[str, Any]],
    missing_policy: str = "drop",
    fill_value: Any = 0,
    clamp_negative: bool = True,
) -> List[Dict[str, Any]]:
    """Clean records by handling missing/invalid values.

    missing_policy options:
        - drop: remove rows containing None values
        - fill-zero: numeric fields -> 0, text -> "UNKNOWN"
        - fill-value: replace missing with `fill_value`
    clamp_negative: when True, negative numeric values become 0.
    """

    valid_policies = {"drop", "fill-zero", "fill-value"}
    if missing_policy not in valid_policies:
        raise ValueError(f"Unsupported missing_policy '{missing_policy}'. Expected one of {sorted(valid_policies)}.")

    text_like_fields = {"country", "date", "source"}

    cleaned: List[Dict[str, Any]] = []
    for rec in data:
        record = {}
        missing_keys = []
        for key, value in rec.items():
            current = value
            if isinstance(current, str):
                stripped = current.strip()
                current = stripped if stripped else None
            if isinstance(current, (int, float)) and clamp_negative and current < 0:
                current = 0
            if current is None:
                missing_keys.append(key)
            record[key] = current

        if missing_keys:
            if missing_policy == "drop":
                continue
            elif missing_policy == "fill-zero":
                for key in missing_keys:
                    record[key] = "UNKNOWN" if key.lower() in text_like_fields else 0
            elif missing_policy == "fill-value":
                for key in missing_keys:
                    record[key] = fill_value
        cleaned.append(record)
    return cleaned


def filter_by_country(data: List[Dict[str, Any]], country: str) -> List[Dict[str, Any]]:
    """Filter records by `country` field equality."""
    return [rec for rec in data if rec.get("country") == country]


def _ensure_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return None
    return None


def apply_filters(
    data: List[Dict[str, Any]],
    *,
    country: str | None,
    date_field: str,
    date_from: date | None,
    date_to: date | None,
    min_value: float | None,
    max_value: float | None,
    stats_field: str,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for rec in data:
        if country and rec.get("country") != country:
            continue

        if date_field:
            date_value = _ensure_date(rec.get(date_field))
            if date_from and (not date_value or date_value < date_from):
                continue
            if date_to and (not date_value or date_value > date_to):
                continue

        value = rec.get(stats_field)
        if min_value is not None and (not isinstance(value, (int, float)) or value < min_value):
            continue
        if max_value is not None and (not isinstance(value, (int, float)) or value > max_value):
            continue

        filtered.append(rec)
    return filtered


def parse_date_arg(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        raise SystemExit(f"Invalid date value '{value}'. Use YYYY-MM-DD format.")


def summary_stats(data: List[Dict[str, Any]], key: str) -> Dict[str, Any]:
    """Compute mean, min, max for numeric values under `key`. Empty input -> zeros."""
    values = [rec.get(key) for rec in data if isinstance(rec.get(key), (int, float))]
    if not values:
        return {"mean": 0, "min": 0, "max": 0}
    mean = sum(values) / len(values)
    if abs(mean - int(mean)) < 1e-9:
        mean = int(mean)
    return {"mean": mean, "min": min(values), "max": max(values)}


def analyze_transactions(
    file_path: str | None,
    stats_field: str,
    country: str | None = None,
    rows: List[Dict[str, Any]] | None = None,
    *,
    missing_policy: str = "drop",
    fill_value: Any = 0,
    clamp_negative: bool = True,
    date_field: str = "date",
    date_from: date | None = None,
    date_to: date | None = None,
    min_value: float | None = None,
    max_value: float | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """Run the standard pipeline: load -> clean -> optional filter -> stats.

    Provide either `file_path` (for CSV) or a pre-loaded `rows` list (e.g. API data).
    """

    if rows is not None:
        raw = rows
    else:
        if not file_path:
            raise ValueError("file_path is required when rows are not provided")
        raw = load_data(file_path)
    cleaned = clean_data(
        raw,
        missing_policy=missing_policy,
        fill_value=fill_value,
        clamp_negative=clamp_negative,
    )
    filtered = apply_filters(
        cleaned,
        country=country,
        date_field=date_field,
        date_from=date_from,
        date_to=date_to,
        min_value=min_value,
        max_value=max_value,
        stats_field=stats_field,
    )
    stats = summary_stats(filtered, stats_field)
    return raw, cleaned, filtered, stats


def load_data_from_uk_covid(
    area_type: str = "nation",
    area_name: str | None = "England",
    metric: str = "newCasesByPublishDate",
    metric_alias: str = "cases",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Fetch recent COVID-19 statistics from the UK government API.

    The API docs are publicly available at https://coronavirus.data.gov.uk/details/developers
    and require no API key. The response is normalised to match the local CSV schema, so the
    returned dictionaries contain at least `country`, `date`, and `metric_alias` keys.
    """

    if limit <= 0:
        return []

    filters = [f"areaType={area_type}"]
    if area_name:
        filters.append(f"areaName={area_name}")

    structure = json.dumps({
        "date": "date",
        "country": "areaName",
        metric_alias: metric,
    })

    params = {
        "filters": ";".join(filters),
        "structure": structure,
        "page": 1,
        "format": "json",
    }

    rows: List[Dict[str, Any]] = []
    while len(rows) < limit:
        response = requests.get(UK_COVID_API_ENDPOINT, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", [])
        if not data:
            break
        rows.extend(data)
        pagination = payload.get("pagination", {})
        if not pagination.get("next"):
            break
        params["page"] += 1

    normalized: List[Dict[str, Any]] = []
    for entry in rows[:limit]:
        normalized.append(
            {
                "country": entry.get("country"),
                metric_alias: _coerce_value(entry.get(metric_alias)),
                "date": entry.get("date"),
            }
        )
    return normalized


def _row_value(row: sqlite3.Row, key: str) -> Any:
    """Safely read a value from sqlite3.Row.

    sqlite3.Row supports mapping-style indexing but not dict.get in all Python versions.
    """

    return row[key] if key in row.keys() else None


def load_data_from_db(
    *,
    db_path: Path | str,
    table: str,
    stats_field: str,
    date_field: str = "date",
) -> List[Dict[str, Any]]:
    """Load records from SQLite (multi-table schema) and shape for the pipeline.

    Joins countries/metrics to return the expected fields: country code, date, stats_field value.
    Filters on metric_name/alias matching stats_field.
    """

    conn, table_name = init_db(db_path, table)
    conn.row_factory = sqlite3.Row
    query = f"""
        SELECT o.date, o.metric_value, o.source, o.raw_json, c.code AS country, m.metric_name, m.alias
        FROM {table_name} o
        JOIN countries c ON o.country_id = c.id
        JOIN metrics m ON o.metric_id = m.id
        WHERE m.metric_name = ? OR m.alias = ?
    """
    with closing(conn):
        rows = conn.execute(query, (stats_field, stats_field)).fetchall()

    shaped: List[Dict[str, Any]] = []
    for row in rows:
        shaped.append(
            {
                "country": row["country"],
                date_field: _row_value(row, "date"),
                stats_field: _row_value(row, "metric_value"),
                "source": _row_value(row, "source"),
                "raw_json": _row_value(row, "raw_json"),
            }
        )
    return shaped


def aggregate_top_countries(
    data: List[Dict[str, Any]], field: str, limit: int
) -> List[Tuple[str, float]]:
    """Return top-N countries by aggregated numeric value."""

    if limit <= 0:
        return []
    totals: Dict[str, float] = {}
    for row in data:
        country = row.get("country")
        value = row.get(field)
        if country is None or not isinstance(value, (int, float)):
            continue
        totals[country] = totals.get(country, 0.0) + float(value)
    ordered = sorted(totals.items(), key=lambda item: item[1], reverse=True)
    return ordered[:limit]


def trend_over_time(
    data: List[Dict[str, Any]],
    date_field: str,
    metric: str,
) -> List[Tuple[str, float]]:
    trend_map: Dict[str, float] = {}
    for rec in data:
        dt = _ensure_date(rec.get(date_field))
        if not dt:
            continue
        value = rec.get(metric)
        if not isinstance(value, (int, float)):
            continue
        key = dt.isoformat()
        trend_map[key] = trend_map.get(key, 0.0) + float(value)
    return sorted(trend_map.items(), key=lambda item: item[0])


def group_by_field(
    data: List[Dict[str, Any]],
    group_field: str,
    metric: str,
) -> List[Tuple[str, Dict[str, float]]]:
    aggregates: Dict[str, Dict[str, float]] = {}
    for rec in data:
        group_key = rec.get(group_field)
        if group_key is None:
            continue
        value = rec.get(metric)
        if not isinstance(value, (int, float)):
            continue
        bucket = aggregates.setdefault(group_key, {"sum": 0.0, "count": 0.0})
        bucket["sum"] += float(value)
        bucket["count"] += 1
    for bucket in aggregates.values():
        bucket["avg"] = bucket["sum"] / bucket["count"] if bucket["count"] else 0.0
    return sorted(aggregates.items(), key=lambda item: item[1]["sum"], reverse=True)


def _build_dataframe(rows: List[Dict[str, Any]]) -> "pd.DataFrame":  # noqa: F821
    import pandas as pd

    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame([serialize_record(rec) for rec in rows])
    return frame


def create_visualizations(
    filtered: List[Dict[str, Any]],
    stats_field: str,
    date_field: str,
    plot_output: str | None,
) -> None:
    if not filtered:
        logging.warning("No data to visualize.")
        return

    try:
        import pandas as pd
        import matplotlib.pyplot as plt
    except ImportError as exc:
        logging.error("Visualization requires pandas and matplotlib: %s", exc)
        return

    df = _build_dataframe(filtered)
    if df.empty:
        logging.warning("No data to visualize.")
        return

    if stats_field not in df.columns:
        logging.warning("Field '%s' not found; skipping visualization.", stats_field)
        return

    has_dates = date_field in df.columns
    if has_dates:
        df[date_field] = pd.to_datetime(df[date_field], errors="coerce")
        timeline_series = (
            df.dropna(subset=[date_field])
            .groupby(date_field)[stats_field]
            .sum()
            .sort_index()
        )
    else:
        timeline_series = None

    print("\nPandas describe() output:\n")
    describe_table = df[[stats_field]].describe(include="all")
    print(describe_table.to_string())

    if timeline_series is None or timeline_series.empty:
        logging.warning("Timeline is empty; skipping plot.")
        return

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(timeline_series.index, timeline_series.values, marker="o", linestyle="-", color="#0066cc")
    ax.set_title(f"Timeline for {stats_field}")
    ax.set_xlabel(date_field)
    ax.set_ylabel(stats_field)
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()

    if plot_output:
        output_path = Path(plot_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, bbox_inches="tight")
        print(f"Visualization saved to {output_path}")
    else:
        plt.show()
    plt.close(fig)


def launch_simple_ui(
    filtered: List[Dict[str, Any]],
    stats_field: str,
    date_field: str,
) -> None:
    if not filtered:
        print("No data available to display in the UI.")
        return

    try:
        import pandas as pd
        import tkinter as tk
        from tkinter import ttk, filedialog, messagebox
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    except ImportError as exc:
        logging.error("UI mode requires pandas and matplotlib: %s", exc)
        return

    df = _build_dataframe(filtered)
    if df.empty:
        print("No data available for the UI window.")
        return

    has_date_column = date_field in df.columns
    if has_date_column:
        df[date_field] = pd.to_datetime(df[date_field], errors="coerce")

    root = tk.Tk()
    root.title("PAI Task 1 Visualizer")
    root.geometry("900x600")

    describe_table = df[[stats_field]].describe(include="all")
    column_stats = describe_table.get(stats_field)
    mean_value = float(column_stats.get("mean", 0)) if column_stats is not None else 0
    count_value = float(column_stats.get("count", 0)) if column_stats is not None else 0
    summary_label = ttk.Label(
        root,
        text=f"Count: {count_value:.0f}    Mean: {mean_value:.2f}",
        font=("Segoe UI", 11, "bold"),
    )
    summary_label.pack(pady=8)

    columns = list(df.columns)
    tree_frame = ttk.Frame(root)
    tree_frame.pack(fill=tk.BOTH, expand=True, padx=10)
    tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=12)
    for col in columns:
        tree.heading(col, text=col)
        tree.column(col, width=max(100, len(col) * 12))
    scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
    tree.configure(yscrollcommand=scroll.set)
    tree.grid(row=0, column=0, sticky="nsew")
    scroll.grid(row=0, column=1, sticky="ns")
    tree_frame.columnconfigure(0, weight=1)
    tree_frame.rowconfigure(0, weight=1)

    for _, row in df.head(200).iterrows():
        tree.insert("", tk.END, values=list(row.values))

    fig, ax = plt.subplots(figsize=(6, 3))
    timeline = None
    if has_date_column:
        timeline = (
            df.dropna(subset=[date_field])
            .groupby(date_field)[stats_field]
            .sum()
            .sort_index()
        )

    if timeline is not None and not timeline.empty:
        ax.plot(timeline.index, timeline.values, marker="o", color="#00897b")
        ax.set_title(f"Timeline for {stats_field}")
        ax.set_xlabel(date_field)
        ax.set_ylabel(stats_field)
        ax.grid(True, linestyle="--", alpha=0.3)
    else:
        ax.text(0.5, 0.5, "No timeline data", ha="center", va="center")

    canvas = FigureCanvasTkAgg(fig, master=root)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=False, pady=10)

    def _save_chart() -> None:
        path = filedialog.asksaveasfilename(
            title="Save chart",
            defaultextension=".png",
            filetypes=[("PNG", "*.png"), ("All files", "*.*")],
        )
        if not path:
            return
        fig.savefig(path, bbox_inches="tight")
        messagebox.showinfo("Task 1 Visualizer", f"Chart saved to {path}")

    ttk.Button(root, text="Save chart as PNG", command=_save_chart).pack(pady=5)

    def _on_close() -> None:
        plt.close(fig)
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)
    root.mainloop()


def format_table(rows: Sequence[Sequence[Any]], headers: Sequence[str]) -> str:
    """Render a simple ASCII table for CLI display."""

    if not rows:
        return "No rows to display."

    str_rows = [[str(cell) for cell in row] for row in rows]
    widths = [len(str(header)) for header in headers]
    for row in str_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def _fmt_line(values: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(values))

    header_line = _fmt_line([str(h) for h in headers])
    divider = "-+-".join("-" * width for width in widths)
    body = "\n".join(_fmt_line(row) for row in str_rows)
    return "\n".join([header_line, divider, body])


def save_results(
    path: str,
    fmt: str | None,
    filtered: List[Dict[str, Any]],
    stats: Dict[str, Any],
    top_countries: List[Tuple[str, float]],
    meta: Dict[str, Any],
) -> Path:
    """Persist results as JSON (summary + data) or CSV (filtered rows)."""

    output_path = Path(path)
    resolved_fmt = (fmt or output_path.suffix.lstrip(".")).lower()
    if not resolved_fmt:
        raise ValueError("Unable to infer output format; please use .json/.csv or pass --output-format.")

    if resolved_fmt == "json":
        payload = {
            "metadata": meta,
            "summary": stats,
            "top_countries": top_countries,
            "records": [serialize_record(rec) for rec in filtered],
        }
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    elif resolved_fmt == "csv":
        if not filtered:
            output_path.write_text("", encoding="utf-8")
            return output_path
        fieldnames = sorted({key for row in filtered for key in row.keys()})
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(filtered)
    else:
        raise ValueError(f"Unsupported output format '{resolved_fmt}'. Use json or csv.")
    return output_path


def _sanitize_identifier(identifier: str) -> str:
    if not identifier or not identifier.replace("_", "").isalnum():
        raise ValueError("Table name must be alphanumeric with optional underscores.")
    return identifier


def init_db(db_path: Path | str, table: str) -> Tuple[sqlite3.Connection, str]:
    """Initialize multi-table schema (countries, metrics, observations, activity_logs)."""

    table_name = _sanitize_identifier(table)
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_name TEXT UNIQUE,
            alias TEXT,
            unit TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            metric_id INTEGER NOT NULL,
            date TEXT,
            metric_value REAL,
            source TEXT,
            raw_json TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(country_id) REFERENCES countries(id),
            FOREIGN KEY(metric_id) REFERENCES metrics(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT,
            detail TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    return conn, table_name


def _normalize_country_code(code: str | None) -> str:
    if not code:
        return "UNKNOWN"
    normalized = str(code).strip()
    return normalized.upper() if normalized else "UNKNOWN"


def _upsert_country(conn: sqlite3.Connection, code: str, name: str | None = None) -> int:
    country_code = _normalize_country_code(code)
    country_name = name or country_code
    conn.execute(
        "INSERT OR IGNORE INTO countries (code, name) VALUES (?, ?)",
        (country_code, country_name),
    )
    row = conn.execute("SELECT id FROM countries WHERE code = ?", (country_code,)).fetchone()
    return int(row[0])


def _upsert_metric(
    conn: sqlite3.Connection,
    metric_name: str,
    *,
    alias: str | None = None,
    unit: str | None = None,
) -> int:
    name = metric_name.strip()
    alias_val = alias.strip() if alias else name
    conn.execute(
        "INSERT OR IGNORE INTO metrics (metric_name, alias, unit) VALUES (?, ?, ?)",
        (name, alias_val, unit),
    )
    row = conn.execute("SELECT id FROM metrics WHERE metric_name = ?", (name,)).fetchone()
    return int(row[0])


def insert_records_into_db(
    records: List[Dict[str, Any]],
    *,
    db_path: Path | str,
    table: str,
    stats_field: str,
    source_label: str,
) -> int:
    if not records:
        return 0
    conn, table_name = init_db(db_path, table)
    with closing(conn):
        metric_id = _upsert_metric(conn, stats_field, alias=stats_field)
        insert_rows = []
        for rec in records:
            serialized = serialize_record(rec)
            country_code = serialized.get("country")
            country_id = _upsert_country(conn, country_code, country_code)
            metric_value = rec.get(stats_field)
            metric_numeric = float(metric_value) if isinstance(metric_value, (int, float)) else None
            insert_rows.append(
                (
                    country_id,
                    metric_id,
                    serialized.get("date"),
                    metric_numeric,
                    source_label,
                    json.dumps(serialized),
                )
            )
        conn.executemany(
            f"""
            INSERT INTO {table_name} (country_id, metric_id, date, metric_value, source, raw_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
        conn.commit()
    return len(insert_rows)


def list_db_records(
    *, db_path: Path | str, table: str, limit: int
) -> List[Dict[str, Any]]:
    conn, table_name = init_db(db_path, table)
    conn.row_factory = sqlite3.Row
    query = f"""
        SELECT o.id, c.code AS country, o.date, m.metric_name, o.metric_value, o.source, o.created_at
        FROM {table_name} o
        JOIN countries c ON o.country_id = c.id
        JOIN metrics m ON o.metric_id = m.id
        ORDER BY o.id DESC
        LIMIT ?
    """
    with closing(conn):
        rows = [dict(row) for row in conn.execute(query, (limit,)).fetchall()]
    return rows


def update_db_record(
    *, db_path: Path | str, table: str, record_id: int, new_value: float
) -> int:
    conn, table_name = init_db(db_path, table)
    with closing(conn):
        cursor = conn.execute(
            f"UPDATE {table_name} SET metric_value = ? WHERE id = ?",
            (new_value, record_id),
        )
        conn.commit()
        return cursor.rowcount


def delete_db_record(
    *, db_path: Path | str, table: str, record_id: int
) -> int:
    conn, table_name = init_db(db_path, table)
    with closing(conn):
        cursor = conn.execute(
            f"DELETE FROM {table_name} WHERE id = ?",
            (record_id,),
        )
        conn.commit()
        return cursor.rowcount


def handle_db_action(
    action: str,
    *,
    records: List[Dict[str, Any]] | None,
    db_path: Path | str,
    table: str,
    stats_field: str,
    source_label: str,
    limit: int,
    record_id: int | None,
    new_value: float | None,
) -> None:
    if action == "skip":
        return

    if action == "insert":
        if not records:
            logging.warning("No records available to insert into the database.")
            return
        inserted = insert_records_into_db(
            records,
            db_path=db_path,
            table=table,
            stats_field=stats_field,
            source_label=source_label,
        )
        logging.info("Inserted %s records into %s", inserted, table)
    elif action == "list":
        rows = list_db_records(db_path=db_path, table=table, limit=limit)
        if not rows:
            print("Database is empty.")
            return
        headers = list(rows[0].keys())
        table_rows = [[row.get(h, "") for h in headers] for row in rows]
        print(_format_section("Database preview", format_table(table_rows, headers)))
    elif action == "update":
        if record_id is None or new_value is None:
            raise SystemExit("--record-id and --new-value are required for update action.")
        updated = update_db_record(db_path=db_path, table=table, record_id=record_id, new_value=new_value)
        logging.info("Updated %s record(s)", updated)
    elif action == "delete":
        if record_id is None:
            raise SystemExit("--record-id is required for delete action.")
        deleted = delete_db_record(db_path=db_path, table=table, record_id=record_id)
        logging.info("Deleted %s record(s)", deleted)
    else:
        raise SystemExit(f"Unsupported db action '{action}'")


def interactive_menu(
    filtered: List[Dict[str, Any]],
    trend: List[Tuple[str, float]],
    grouping: List[Tuple[str, Dict[str, float]]],
) -> None:
    if not filtered:
        print("No data available for interactive session.")
        return
    while True:
        print(
            "\nInteractive menu:\n"
            "1. View first 5 records\n"
            "2. View timeline trend\n"
            "3. View grouped statistics\n"
            "q. Quit\n"
        )
        choice = input("Select an option: ").strip().lower()
        if choice == "1":
            headers = list(filtered[0].keys())
            rows = [[row.get(h, "") for h in headers] for row in filtered[:5]]
            print(format_table(rows, headers))
        elif choice == "2":
            if not trend:
                print("No trend data available.")
            else:
                headers = ["Date", "Cumulative"]
                rows = [[date_label, value] for date_label, value in trend]
                print(format_table(rows, headers))
        elif choice == "3":
            if not grouping:
                print("No grouped statistics available.")
            else:
                headers = ["Group", "Sum", "Count", "Average"]
                rows = [
                    [group, round(stats["sum"], 2), int(stats["count"]), round(stats["avg"], 2)]
                    for group, stats in grouping
                ]
                print(format_table(rows, headers))
        elif choice == "q":
            break
        else:
            print("Invalid selection, please try again.")


def _format_section(title: str, payload: Any) -> str:
    return f"\n{title}\n" + "-" * len(title) + f"\n{payload}"


def run_cli() -> None:
    """Entry point for Task 1 demo/CLI usage."""

    parser = argparse.ArgumentParser(
        description="PAI Task 1 pipeline: load, clean, filter, summarize supermarket data."
    )
    parser.add_argument(
        "--source",
        choices=["file", "uk-covid", "db"],
        default="file",
        help="Choose data source: local CSV ('file'), UK COVID-19 API ('uk-covid'), or SQLite ('db').",
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_DATASET,
        help="Path to the CSV file (defaults to test_data.csv in this repo).",
    )
    parser.add_argument(
        "--country",
        help="Optional country code filter (e.g. UK). If omitted, stats use all cleaned rows.",
    )
    parser.add_argument(
        "--stats-field",
        default="cases",
        help="Numeric field to summarize (default: cases).",
    )
    parser.add_argument(
        "--missing-policy",
        choices=["drop", "fill-zero", "fill-value"],
        default="drop",
        help="Strategy for handling missing values during cleaning.",
    )
    parser.add_argument(
        "--fill-value",
        default="0",
        help="Fallback value when --missing-policy=fill-value.",
    )
    parser.add_argument(
        "--allow-negative",
        action="store_true",
        help="Allow negative numeric values instead of clamping to zero.",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Print raw and cleaned datasets for quick inspection.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=0,
        help="Show top N countries by the stats field after cleaning (0 disables).",
    )
    parser.add_argument(
        "--table",
        action="store_true",
        help="Display a formatted table preview of up to 10 filtered rows.",
    )
    parser.add_argument(
        "--output",
        help="Optional path to save filtered data and summary (json/csv).",
    )
    parser.add_argument(
        "--output-format",
        choices=["json", "csv"],
        help="Force output format when --output has no extension.",
    )
    parser.add_argument(
        "--date-field",
        default="date",
        help="Field name containing date information for filtering/trends.",
    )
    parser.add_argument(
        "--date-from",
        help="Lower bound (inclusive) for date filtering, format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--date-to",
        help="Upper bound (inclusive) for date filtering, format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--min-value",
        type=float,
        help="Minimum numeric value for stats field after cleaning.",
    )
    parser.add_argument(
        "--max-value",
        type=float,
        help="Maximum numeric value for stats field after cleaning.",
    )
    parser.add_argument(
        "--group-field",
        default="country",
        help="Field used for grouped summaries (set blank to disable).",
    )
    parser.add_argument(
        "--api-area-type",
        default="nation",
        help="(API mode) areaType filter, e.g. nation, region, utla.",
    )
    parser.add_argument(
        "--api-area-name",
        default="England",
        help="(API mode) areaName filter; leave blank to include all areas.",
    )
    parser.add_argument(
        "--api-metric",
        default="newCasesByPublishDate",
        help="(API mode) metric name to fetch from the dataset.",
    )
    parser.add_argument(
        "--api-limit",
        type=int,
        default=100,
        help="(API mode) maximum rows to download (latest first).",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database file path (default: covid_data.sqlite3).",
    )
    parser.add_argument(
        "--db-table",
        default=DEFAULT_DB_TABLE,
        help="Database table for storing records.",
    )
    parser.add_argument(
        "--db-action",
        choices=["insert", "list", "update", "delete", "skip"],
        default="insert",
        help="Database operation to perform after processing.",
    )
    parser.add_argument(
        "--db-limit",
        type=int,
        default=10,
        help="Maximum rows to display when db-action=list.",
    )
    parser.add_argument(
        "--record-id",
        type=int,
        help="Record identifier for db-action update/delete.",
    )
    parser.add_argument(
        "--new-value",
        type=float,
        help="New numeric value for db-action=update.",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip database operations even if db-action is set.",
    )
    parser.add_argument(
        "--db-only",
        action="store_true",
        help="Perform the chosen database action without running the data pipeline.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Open a simple interactive menu after the summary is displayed.",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate a pandas summary and matplotlib chart for the filtered data.",
    )
    parser.add_argument(
        "--plot-output",
        help="Optional destination for saving the matplotlib chart when --visualize is used.",
    )
    parser.add_argument(
        "--ui",
        action="store_true",
        help="Launch a minimal Tkinter UI to browse rows and the generated chart.",
    )
    parser.add_argument(
        "--log-file",
        help="Optional log file to append detailed runtime information.",
    )
    args = parser.parse_args()

    if args.log_file:
        logging.getLogger().addHandler(logging.FileHandler(args.log_file))

    if args.db_only:
        handle_db_action(
            args.db_action,
            records=None,
            db_path=args.db_path,
            table=args.db_table,
            stats_field=args.stats_field,
            source_label=args.source,
            limit=args.db_limit,
            record_id=args.record_id,
            new_value=args.new_value,
        )
        return

    fill_value: Any = args.fill_value
    try:
        fill_value = float(fill_value)
    except ValueError:
        pass

    date_from = parse_date_arg(args.date_from)
    date_to = parse_date_arg(args.date_to)

    if args.source == "file":
        raw, cleaned, filtered, stats = analyze_transactions(
            args.file,
            args.stats_field,
            args.country,
            missing_policy=args.missing_policy,
            fill_value=fill_value,
            clamp_negative=not args.allow_negative,
            date_field=args.date_field,
            date_from=date_from,
            date_to=date_to,
            min_value=args.min_value,
            max_value=args.max_value,
        )
    elif args.source == "db":
        db_rows = load_data_from_db(
            db_path=args.db_path,
            table=args.db_table,
            stats_field=args.stats_field,
            date_field=args.date_field,
        )
        if not db_rows:
            raise SystemExit("Database returned no matching data. Insert data first or adjust stats-field.")
        raw, cleaned, filtered, stats = analyze_transactions(
            file_path=None,
            stats_field=args.stats_field,
            country=args.country,
            rows=db_rows,
            missing_policy=args.missing_policy,
            fill_value=fill_value,
            clamp_negative=not args.allow_negative,
            date_field=args.date_field,
            date_from=date_from,
            date_to=date_to,
            min_value=args.min_value,
            max_value=args.max_value,
        )
    else:
        try:
            api_rows = load_data_from_uk_covid(
                area_type=args.api_area_type,
                area_name=args.api_area_name or None,
                metric=args.api_metric,
                metric_alias=args.stats_field,
                limit=args.api_limit,
            )
        except requests.RequestException as exc:
            raise SystemExit(f"Failed to fetch data from UK COVID API: {exc}") from exc

        if not api_rows:
            raise SystemExit("API returned no data. Adjust filters or try again later.")

        raw, cleaned, filtered, stats = analyze_transactions(
            file_path=None,
            stats_field=args.stats_field,
            country=args.country,
            rows=api_rows,
            missing_policy=args.missing_policy,
            fill_value=fill_value,
            clamp_negative=not args.allow_negative,
            date_field=args.date_field,
            date_from=date_from,
            date_to=date_to,
            min_value=args.min_value,
            max_value=args.max_value,
        )
    top_countries = aggregate_top_countries(cleaned, args.stats_field, args.top_n)
    timeline = trend_over_time(filtered, args.date_field, args.stats_field)
    grouping = group_by_field(filtered, args.group_field, args.stats_field) if args.group_field else []

    output_sections = []
    if args.demo:
        output_sections.append(_format_section("Raw data", raw))
        output_sections.append(_format_section("Cleaned data", cleaned))
        if args.country:
            output_sections.append(
                _format_section(f"Filtered data (country={args.country})", filtered)
            )
    else:
        output_sections.append(
            _format_section("Records analysed", f"Raw={len(raw)}, Clean={len(cleaned)}, Filtered={len(filtered)}")
        )

    if args.table and filtered:
        headers = list(filtered[0].keys())
        preview_rows = [[row.get(h, "") for h in headers] for row in filtered[:10]]
        output_sections.append(
            _format_section("Filtered data (preview)", format_table(preview_rows, headers))
        )

    output_sections.append(_format_section(f"Summary for '{args.stats_field}'", stats))

    if top_countries:
        headers = ["Country", args.stats_field]
        rows = [[country, value] for country, value in top_countries]
        output_sections.append(
            _format_section(
                f"Top {len(rows)} countries by '{args.stats_field}'",
                format_table(rows, headers),
            )
        )
    if timeline:
        headers = ["Date", args.stats_field]
        rows = [[day, value] for day, value in timeline]
        output_sections.append(_format_section("Timeline Trend", format_table(rows, headers)))

    if grouping:
        headers = [args.group_field or "Group", "Sum", "Count", "Avg"]
        rows = [
            [group, round(stats_dict["sum"], 2), int(stats_dict["count"]), round(stats_dict["avg"], 2)]
            for group, stats_dict in grouping
        ]
        output_sections.append(_format_section("Grouped Statistics", format_table(rows, headers)))

    print("\n=== PAI Task 1 Results ===")
    print("\n".join(output_sections))
    print("\n=== End ===")

    if args.output:
        meta = {
            "data_source": args.source,
            "country_filter": args.country,
            "stats_field": args.stats_field,
        }
        if args.source == "file":
            meta["source_file"] = str(args.file)
        elif args.source == "db":
            meta.update(
                {
                    "db_path": args.db_path,
                    "db_table": args.db_table,
                }
            )
        else:
            meta.update(
                {
                    "api_endpoint": UK_COVID_API_ENDPOINT,
                    "api_area_type": args.api_area_type,
                    "api_area_name": args.api_area_name,
                    "api_metric": args.api_metric,
                    "api_limit": args.api_limit,
                }
            )
        saved_path = save_results(args.output, args.output_format, filtered, stats, top_countries, meta)
        print(f"Results saved to {saved_path}")

    if not args.skip_db:
        handle_db_action(
            args.db_action,
            records=filtered,
            db_path=args.db_path,
            table=args.db_table,
            stats_field=args.stats_field,
            source_label=args.source,
            limit=args.db_limit,
            record_id=args.record_id,
            new_value=args.new_value,
        )

    if args.visualize:
        create_visualizations(filtered, args.stats_field, args.date_field, args.plot_output)

    if args.ui:
        launch_simple_ui(filtered, args.stats_field, args.date_field)

    if args.interactive:
        interactive_menu(filtered, timeline, grouping)


if __name__ == "__main__":
    run_cli()


