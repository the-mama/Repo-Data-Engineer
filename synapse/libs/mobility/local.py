"""Offline proof of ingestion, normalization, quarantine and KPI arithmetic."""

import argparse
import json
import zipfile
import io
from pathlib import Path
from mobility.core import decode_response, normalize, validate, deduplicate, gtfs_arrival, delay_minutes


def run(root, output):
    output.mkdir(parents=True, exist_ok=True)
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as z:
        for file in sorted((root / "samples/gtfs_static_sample").glob("*.txt")):
            z.writestr(file.name, file.read_bytes())
    bronze, silver, quarantine = [], {}, []
    sources = [
        (archive.getvalue(), "static", "gtfs_zip"),
        ((root / "samples/gtfs_realtime_sample.json").read_bytes(), "trip_updates", "json"),
        ((root / "samples/vehicle_positions_sample.json").read_bytes(), "vehicle_positions", "json"),
        ((root / "samples/weather_sample.json").read_bytes(), "weather", "json"),
    ]
    for content, entity, fmt in sources:
        for name, raw in decode_response(content, entity, fmt):
            bronze.append({"entity": name, "payload": raw})
            for row in normalize(name, raw, "sample"):
                errors = validate(name, row)
                if errors:
                    quarantine.append({**row, "failure_reason": errors})
                else:
                    silver.setdefault(name, []).append(row)
    from mobility.core import KEYS

    silver = {name: deduplicate(rows, KEYS[name]) for name, rows in silver.items()}
    facts = []
    for update in silver["trip_updates"]:
        for stop in silver["stop_times"]:
            if stop["trip_id"] != update["trip_id"] or update["stop_sequence"] not in (
                "*",
                stop["stop_sequence"],
            ):
                continue
            scheduled = gtfs_arrival(update["service_date"], stop["arrival_time"], "Europe/London")
            actual = update["reported_arrival_time"] if update["arrival_time_kind"] == "observed" else None
            facts.append(
                {
                    "trip_id": update["trip_id"],
                    "stop_id": stop["stop_id"],
                    "scheduled_arrival_time": scheduled.isoformat(),
                    "actual_arrival_time": actual,
                    "delay_minutes": delay_minutes(scheduled, actual),
                    "cancellation_flag": update["cancellation_flag"],
                }
            )
    result = {
        "bronze_records": len(bronze),
        "silver_records": sum(map(len, silver.values())),
        "quarantined_records": len(quarantine),
        "facts": facts,
    }
    (output / "offline_result.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=Path(".local"))
    args = parser.parse_args()
    print(json.dumps(run(args.root, args.output), indent=2))
