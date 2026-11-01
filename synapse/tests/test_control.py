from datetime import datetime, timezone
import json
from pathlib import Path
import pytest
from mobility.control import prepare_sources, is_due, resolve_source

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 11, 1, 8, 10, tzinfo=timezone.utc)


def source():
    return json.loads((ROOT / "synapse/config/pipeline_control.json").read_text())[1]


def test_all_seed_metadata_is_valid():
    rows = json.loads((ROOT / "synapse/config/pipeline_control.json").read_text())
    assert len(prepare_sources(rows)) == 5


def test_utc_cron_buckets_and_force():
    row = source()
    assert is_due(row, NOW)
    row["last_success_at"] = "2026-11-01T08:06:00Z"
    assert is_due(row, NOW)
    row["last_success_at"] = "2026-11-01T08:10:00Z"
    assert not is_due(row, NOW)
    assert is_due(row, NOW, force=True)
    row["is_active"] = False
    assert not is_due(row, NOW, force=True)


def test_daily_schedule_waits_for_0200_utc():
    row = {**source(), "schedule_cron": "0 2 * * *", "last_success_at": "2026-10-31T05:00:00Z"}
    assert not is_due(row, NOW.replace(hour=1))
    assert is_due(row, NOW)


def test_historical_watermark_overlap_and_archive_lag():
    row = {
        **source(),
        "api_url": "https://example.org?start={start_date}&end={end_date}",
        "last_watermark_value": "2026-10-25",
    }
    assert resolve_source(row, NOW)["api_url"] == "https://example.org?start=2026-10-25&end=2026-10-30"


@pytest.mark.parametrize(
    "change", [{"schedule_cron": "* * * * *"}, {"api_url": "http://unsafe"}, {"target_folder": "../secret"}]
)
def test_invalid_control_metadata_is_rejected(change):
    with pytest.raises(ValueError):
        prepare_sources([{**source(), **change}])


def test_duplicate_control_keys_are_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        prepare_sources([source(), source()])
