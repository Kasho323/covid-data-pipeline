# Test cases for main application
import json
from datetime import date

import pytest

from covid_pipeline import main


def test_load_data_success():
    data = main.load_data("test_data.csv")
    assert isinstance(data, list)
    assert len(data) > 0


def test_load_data_parses_numeric_and_strings(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "country,cases,population\nUK,10.5,\"1,234\"\nFR,foo,\n",
        encoding="utf-8",
    )
    data = main.load_data(str(csv_path))
    assert data[0]["cases"] == 10.5
    assert data[0]["population"] == 1234
    assert data[1]["cases"] == "foo"
    assert data[1]["population"] is None


def test_load_data_missing_file_raises(tmp_path):
    missing = tmp_path / "absent.csv"
    with pytest.raises(FileNotFoundError):
        main.load_data(str(missing))


def test_clean_data_removes_missing():
    raw_data = [
        {"country": "UK", "cases": 100},
        {"country": "FR", "cases": None}
    ]
    clean = main.clean_data(raw_data)
    assert len(clean) == 1
    assert clean[0]["country"] == "UK"


def test_clean_data_fill_zero():
    raw_data = [
        {"country": "UK", "cases": None},
    ]
    clean = main.clean_data(raw_data, missing_policy="fill-zero")
    assert clean[0]["cases"] == 0




def test_clean_data_fill_zero_uses_unknown_for_text_fields():
    raw_data = [{"country": None, "cases": None, "date": None}]
    clean = main.clean_data(raw_data, missing_policy="fill-zero")
    assert clean[0]["country"] == "UNKNOWN"
    assert clean[0]["date"] == "UNKNOWN"
    assert clean[0]["cases"] == 0

def test_filter_by_country():
    data = [
        {"country": "UK", "cases": 100},
        {"country": "FR", "cases": 50}
    ]
    result = main.filter_by_country(data, "UK")
    assert len(result) == 1
    assert result[0]["country"] == "UK"


def test_summary_stats():
    data = [
        {"cases": 10},
        {"cases": 20},
        {"cases": 30}
    ]
    summary = main.summary_stats(data, "cases")
    assert summary["mean"] == 20
    assert summary["min"] == 10
    assert summary["max"] == 30


def test_summary_stats_handles_empty_numeric_list():
    data = [{"cases": None}]
    summary = main.summary_stats(data, "cases")
    assert summary == {"mean": 0, "min": 0, "max": 0}


def test_top_countries_returns_sorted_limit():
    data = [
        {"country": "UK", "cases": 10},
        {"country": "FR", "cases": 7},
        {"country": "UK", "cases": 5},
        {"country": "DE", "cases": None},
    ]
    result = main.aggregate_top_countries(data, "cases", 2)
    assert result == [("UK", 15.0), ("FR", 7.0)]


def test_save_results_json(tmp_path):
    filtered = [{"country": "UK", "cases": 10}]
    stats = {"mean": 10, "min": 10, "max": 10}
    top = [("UK", 10.0)]
    meta = {"source_file": "x.csv", "country_filter": None, "stats_field": "cases"}
    output = tmp_path / "report.json"
    saved_path = main.save_results(str(output), None, filtered, stats, top, meta)
    payload = json.loads(saved_path.read_text(encoding="utf-8"))
    assert payload["summary"]["mean"] == 10
    assert payload["top_countries"] == [["UK", 10.0]]


def test_save_results_csv(tmp_path):
    filtered = [
        {"country": "UK", "cases": 10},
        {"country": "FR", "cases": 5},
    ]
    stats = {"mean": 7.5, "min": 5, "max": 10}
    top = [("UK", 10.0), ("FR", 5.0)]
    meta = {"source_file": "x.csv", "country_filter": None, "stats_field": "cases"}
    output = tmp_path / "report.csv"
    saved_path = main.save_results(str(output), "csv", filtered, stats, top, meta)
    lines = saved_path.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0] == "cases,country"
    assert "UK" in lines[1]


def test_load_data_from_uk_covid(monkeypatch):
    payload = {
        "data": [
            {"country": "England", "cases": "12", "date": "2024-01-01"},
            {"country": "England", "cases": "11", "date": "2023-12-31"},
        ],
        "pagination": {"next": None},
    }

    class DummyResp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self._data

    def fake_get(url, params=None, timeout=None):
        assert url == main.UK_COVID_API_ENDPOINT
        assert "filters" in params
        return DummyResp(payload)

    monkeypatch.setattr(main.requests, "get", fake_get)

    rows = main.load_data_from_uk_covid(limit=1)
    assert len(rows) == 1
    assert rows[0]["country"] == "England"
    assert rows[0]["cases"] == 12


def test_analyze_transactions_with_rows():
    rows = [{"country": "UK", "cases": 5}]
    raw, cleaned, filtered, stats = main.analyze_transactions(None, "cases", None, rows=rows)
    assert len(raw) == 1
    assert stats["max"] == 5


def test_apply_filters_date_range():
    data = [
        {"country": "UK", "cases": 5, "date": "2024-01-01"},
        {"country": "UK", "cases": 10, "date": "2024-01-05"},
    ]
    filtered = main.apply_filters(
        data,
        country=None,
        date_field="date",
        date_from=date(2024, 1, 2),
        date_to=None,
        min_value=None,
        max_value=None,
        stats_field="cases",
    )
    assert len(filtered) == 1
    assert filtered[0]["cases"] == 10


def test_trend_and_grouping():
    data = [
        {"country": "UK", "cases": 5, "date": "2024-01-01"},
        {"country": "UK", "cases": 7, "date": "2024-01-01"},
        {"country": "FR", "cases": 3, "date": "2024-01-02"},
    ]
    trend = main.trend_over_time(data, "date", "cases")
    assert trend[0] == ("2024-01-01", 12.0)
    grouping = main.group_by_field(data, "country", "cases")
    assert grouping[0][0] == "UK"
    assert grouping[0][1]["sum"] == 12.0


def test_db_crud_flow(tmp_path):
    db_path = tmp_path / "task.sqlite"
    records = [{"country": "UK", "cases": 5, "date": "2024-01-01"}]
    inserted = main.insert_records_into_db(
        records,
        db_path=db_path,
        table="obs",
        stats_field="cases",
        source_label="test",
    )
    assert inserted == 1
    rows = main.list_db_records(db_path=db_path, table="obs", limit=5)
    assert len(rows) == 1
    rec_id = rows[0]["id"]
    main.update_db_record(db_path=db_path, table="obs", record_id=rec_id, new_value=10.0)
    rows = main.list_db_records(db_path=db_path, table="obs", limit=5)
    assert rows[0]["metric_value"] == 10.0
    main.delete_db_record(db_path=db_path, table="obs", record_id=rec_id)
    rows = main.list_db_records(db_path=db_path, table="obs", limit=5)
    assert rows == []



def test_clean_data_invalid_policy_raises():
    with pytest.raises(ValueError):
        main.clean_data([{"country": "UK", "cases": None}], missing_policy="bad-policy")


def test_load_data_from_db_round_trip(tmp_path):
    db_path = tmp_path / "task.sqlite"
    records = [{"country": "UK", "cases": 5, "date": "2024-01-01"}]
    main.insert_records_into_db(
        records,
        db_path=db_path,
        table="obs",
        stats_field="cases",
        source_label="test",
    )
    loaded = main.load_data_from_db(db_path=db_path, table="obs", stats_field="cases")
    assert loaded == [{
        "country": "UK",
        "date": "2024-01-01",
        "cases": 5.0,
        "source": "test",
        "raw_json": '{"country": "UK", "cases": 5, "date": "2024-01-01"}',
    }]
