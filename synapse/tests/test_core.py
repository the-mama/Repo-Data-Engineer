import json
from datetime import datetime, timezone
from pathlib import Path
import pytest
from mobility.core import (
    utc_timestamp,
    gtfs_arrival,
    delay_minutes,
    deduplicate,
    weather_condition,
    weather_impact,
    normalize,
    validate,
    decode_response,
    haversine_km,
)

ROOT = Path(__file__).resolve().parents[2]


def test_timestamp_offsets_and_epoch():
    assert utc_timestamp("2026-01-01T09:00:00+01:00") == utc_timestamp("2026-01-01T08:00:00Z")
    assert utc_timestamp(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert utc_timestamp("2026-01-01T08:00:00", "Europe/London").hour == 8


@pytest.mark.parametrize("value", ["2026-10-25T01:30:00", "2026-03-29T01:30:00"])
def test_ambiguous_and_nonexistent_local_time(value):
    with pytest.raises(ValueError):
        utc_timestamp(value, "Europe/London")


def test_service_day_over_24_hours():
    assert gtfs_arrival("20261031", "25:10:00", "Europe/London") == utc_timestamp("2026-11-01T01:10:00Z")


def test_gtfs_dst_uses_noon_origin():
    assert gtfs_arrival("20260329", "03:00:00", "Europe/London") == utc_timestamp("2026-03-29T02:00:00Z")


@pytest.mark.parametrize("time", ["12:99:00", "-1:00:00"])
def test_invalid_service_time(time):
    with pytest.raises(ValueError):
        gtfs_arrival("20261031", time, "Europe/London")


def test_delay_and_nulls():
    assert delay_minutes("2026-10-31T08:00Z", "2026-10-31T08:05Z") == 5
    assert delay_minutes("2026-10-31T08:00Z", "2026-10-31T07:58Z") == -2
    assert delay_minutes(None, None) is None


def test_deduplication_corrections_and_stability():
    first = {
        "trip_id": "01",
        "vehicle_id": "V",
        "timestamp": "2026-10-31T08:00Z",
        "latitude": 1,
        "ingestion_timestamp": "2026-10-31T08:01Z",
    }
    corrected = {**first, "latitude": 2, "ingestion_timestamp": "2026-10-31T08:02Z"}
    keys = ["trip_id", "vehicle_id", "timestamp"]
    assert deduplicate([first, corrected, first], keys) == [corrected]
    assert deduplicate([first, corrected], keys) == deduplicate([corrected, first], keys)


def test_weather_enrichment_and_index():
    assert [weather_condition(c) for c in [0, 3, 45, 61, 75, 95, 999]] == [
        "clear",
        "cloudy",
        "fog",
        "rain",
        "snow",
        "storm",
        "unknown",
    ]
    assert weather_impact([10, 20], [5, 10]) == 2
    assert weather_impact([5], []) is None
    assert weather_impact([5], [0]) is None


def test_dq_collects_all_errors():
    row = {"latitude": 91, "longitude": "bad", "timestamp": "not-a-date", "delay_minutes": "NaN"}
    errors = validate("vehicle_positions", row)
    assert {
        "missing:trip_id",
        "missing:vehicle_id",
        "range:latitude",
        "numeric:longitude",
        "invalid:timestamp",
        "range:delay_minutes",
    } <= set(errors)
    assert "range:delay_minutes" in validate("vehicle_positions", {**row, "delay_minutes": 181}, 180)


def test_numeric_and_timestamp_boundaries():
    now = utc_timestamp("2026-11-01T00:00Z")
    row = {
        "trip_id": "T",
        "vehicle_id": "V",
        "timestamp": "2026-10-31T08:00Z",
        "latitude": -90,
        "longitude": 180,
    }
    assert validate("vehicle_positions", row, now=now) == []
    assert "range:timestamp" in validate(
        "vehicle_positions", {**row, "timestamp": "2099-01-01T00:00Z"}, now=now
    )


def test_updates_preserve_prediction_and_cancelled():
    data = (ROOT / "samples/gtfs_realtime_sample.json").read_bytes()
    raw = list(decode_response(data, "trip_updates", "json"))
    rows = [r for name, record in raw for r in normalize(name, record, "sample")]
    assert rows[0]["arrival_time_kind"] == "observed"
    assert rows[1]["arrival_time_kind"] == "prediction"
    assert rows[2]["cancellation_flag"] and rows[2]["stop_sequence"] == "*"


def test_protobuf_decoder():
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1793434800
    item = feed.entity.add()
    item.id = "v1"
    item.vehicle.trip.trip_id = "T1"
    item.vehicle.vehicle.id = "V1"
    item.vehicle.timestamp = 1793434800
    item.vehicle.position.latitude = 51.5
    item.vehicle.position.longitude = -0.1
    records = list(decode_response(feed.SerializeToString(), "vehicle_positions", "protobuf"))
    assert list(normalize(*records[0], "sample"))[0]["vehicle_id"] == "V1"


def test_differential_is_explicitly_rejected():
    with pytest.raises(ValueError, match="DIFFERENTIAL"):
        list(
            decode_response(
                json.dumps({"header": {"incrementality": "DIFFERENTIAL"}}).encode(), "trip_updates", "json"
            )
        )


def test_weather_json_unrolls_hours():
    rows = [
        r
        for entity, raw in decode_response(
            (ROOT / "samples/weather_sample.json").read_bytes(), "weather", "json"
        )
        for r in normalize(entity, raw, "sample")
    ]
    assert len(rows) == 2 and rows[0]["weather_condition"] == "rain"
    assert rows[0]["timestamp"].endswith("+00:00")


def test_distance_is_km():
    assert haversine_km(0, 0, 0, 1) == pytest.approx(111.195, abs=0.01)


def test_offline_pipeline(tmp_path):
    from mobility.local import run

    result = run(ROOT, tmp_path)
    assert result["quarantined_records"] == 1
    assert len(result["facts"]) == 4
    assert result["facts"][0]["delay_minutes"] == 5
    assert result["facts"][1]["actual_arrival_time"] is None
    assert sum(r["cancellation_flag"] for r in result["facts"]) == 2


def test_mandatory_fields_and_schedule_validation():
    assert "missing:latitude" in validate(
        "vehicle_positions", {"trip_id": "T", "vehicle_id": "V", "timestamp": "2026-10-31T08:00Z"}
    )
    assert "invalid:agency_timezone" in validate(
        "agency",
        {
            "agency_id": "A",
            "agency_name": "Transit",
            "agency_url": "https://example.org",
            "agency_timezone": "Invalid/Zone",
        },
    )
    errors = validate(
        "stop_times",
        {
            "trip_id": "T",
            "stop_sequence": "1",
            "stop_id": "S",
            "arrival_time": "25:90:00",
            "departure_time": "25:10:00",
        },
    )
    assert "invalid:arrival_time" in errors
