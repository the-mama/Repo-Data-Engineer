"""Pure transformations shared by local tests and Spark workers."""

import csv
import hashlib
import io
import json
import math
import zipfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

STATIC_KEYS = {
    "agency": ["agency_id"],
    "routes": ["route_id"],
    "stops": ["stop_id"],
    "trips": ["trip_id"],
    "stop_times": ["trip_id", "stop_sequence"],
    "calendar": ["service_id"],
    "calendar_dates": ["service_id", "date"],
}
KEYS = {
    **STATIC_KEYS,
    "vehicle_positions": ["trip_id", "vehicle_id", "timestamp"],
    "trip_updates": ["trip_id", "service_date", "stop_sequence", "timestamp"],
    "weather": ["location_id", "timestamp", "weather_kind"],
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def utc_timestamp(value, source_timezone="UTC"):
    if value is None or value == "":
        raise ValueError("missing timestamp")
    if isinstance(value, (int, float)) or str(value).isdigit():
        dt = datetime.fromtimestamp(float(value), timezone.utc)
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            zone = ZoneInfo(source_timezone)
            # Ambiguous/nonexistent local wall times must carry an explicit offset.
            if dt.replace(tzinfo=zone, fold=0).utcoffset() != dt.replace(tzinfo=zone, fold=1).utcoffset():
                raise ValueError("ambiguous or nonexistent local timestamp; provide offset")
            dt = dt.replace(tzinfo=zone)
    return dt.astimezone(timezone.utc)


def gtfs_arrival(service_date, service_time, agency_timezone):
    """GTFS time uses local noon minus 12 hours as the service-day origin (DST safe)."""
    h, m, s = map(int, service_time.split(":"))
    if h < 0 or not 0 <= m < 60 or not 0 <= s < 60:
        raise ValueError("invalid GTFS time")
    day = datetime.strptime(service_date.replace("-", ""), "%Y%m%d")
    noon = day.replace(hour=12, tzinfo=ZoneInfo(agency_timezone)).astimezone(timezone.utc)
    return noon - timedelta(hours=12) + timedelta(hours=h, minutes=m, seconds=s)


def delay_minutes(scheduled, actual):
    if scheduled is None or actual is None:
        return None
    return (utc_timestamp(actual) - utc_timestamp(scheduled)).total_seconds() / 60


def weather_condition(code):
    code = int(code)
    if code in (0, 1):
        return "clear"
    if code in (2, 3):
        return "cloudy"
    if code in (45, 48):
        return "fog"
    if code in (71, 73, 75, 77, 85, 86):
        return "snow"
    if code in (95, 96, 99):
        return "storm"
    if code in (51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82):
        return "rain"
    return "unknown"


def weather_impact(poor_delays, clear_delays):
    """Ratio of means; undefined for missing groups or nonpositive baseline."""
    if not poor_delays or not clear_delays:
        return None
    baseline = sum(clear_delays) / len(clear_delays)
    return (sum(poor_delays) / len(poor_delays)) / baseline if baseline > 0 else None


def haversine_km(lat1, lon1, lat2, lon2):
    a, b = math.radians(lat1), math.radians(lat2)
    dlat, dlon = b - a, math.radians(lon2 - lon1)
    v = math.sin(dlat / 2) ** 2 + math.cos(a) * math.cos(b) * math.sin(dlon / 2) ** 2
    return 6371.0088 * 2 * math.asin(min(1, math.sqrt(v)))


def deduplicate(records, keys):
    """Stable winner even when correction timestamps tie."""
    winners = {}
    for record in records:
        key = tuple(record.get(k) for k in keys)
        rank = (str(record.get("ingestion_timestamp", "")), digest(record))
        if key not in winners or rank > winners[key][0]:
            winners[key] = (rank, record)
    return [winners[k][1] for k in sorted(winners, key=str)]


REQUIRED = {
    "agency": ["agency_name", "agency_url", "agency_timezone"],
    "routes": ["route_type"],
    "stops": ["stop_name", "latitude", "longitude"],
    "trips": ["route_id", "service_id"],
    "stop_times": ["stop_id", "arrival_time", "departure_time"],
    "calendar": [
        "start_date",
        "end_date",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ],
    "vehicle_positions": ["latitude", "longitude"],
    "weather": ["latitude", "longitude", "weather_code"],
}


def validate(entity, row, max_delay_minutes=180, now=None):
    errors = [f"missing:{k}" for k in KEYS[entity] + REQUIRED.get(entity, []) if row.get(k) in (None, "")]
    if entity == "agency" and row.get("agency_timezone"):
        try:
            ZoneInfo(row["agency_timezone"])
        except (KeyError, ValueError):
            errors.append("invalid:agency_timezone")
    if entity == "routes" and not (row.get("route_short_name") or row.get("route_long_name")):
        errors.append("missing:route_name")
    if entity == "calendar":
        for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
            if str(row.get(day)) not in ("0", "1"):
                errors.append(f"invalid:{day}")
    for field in ("service_date", "start_date", "end_date", "date"):
        if row.get(field):
            try:
                datetime.strptime(str(row[field]), "%Y%m%d")
            except ValueError:
                errors.append(f"invalid:{field}")
    if entity == "stop_times":
        for field in ("arrival_time", "departure_time"):
            if row.get(field):
                try:
                    gtfs_arrival("20260101", row[field], "UTC")
                except (ValueError, TypeError):
                    errors.append(f"invalid:{field}")
    for key, lo, hi in [("latitude", -90, 90), ("longitude", -180, 180)]:
        if row.get(key) is not None:
            try:
                value = float(row[key])
                if not math.isfinite(value) or not lo <= value <= hi:
                    errors.append(f"range:{key}")
            except (TypeError, ValueError):
                errors.append(f"numeric:{key}")
    if row.get("delay_minutes") is not None:
        try:
            delay = float(row["delay_minutes"])
            if not math.isfinite(delay) or abs(delay) > max_delay_minutes:
                errors.append("range:delay_minutes")
        except (TypeError, ValueError):
            errors.append("numeric:delay_minutes")
    if row.get("timestamp") is not None:
        try:
            timestamp = utc_timestamp(row["timestamp"])
            # Forecasts may extend 16 days into the future; historical data is allowed.
            if (
                not datetime(2000, 1, 1, tzinfo=timezone.utc)
                <= timestamp
                <= (now or datetime.now(timezone.utc)) + timedelta(days=17)
            ):
                errors.append("range:timestamp")
        except (ValueError, TypeError, OverflowError, OSError):
            errors.append("invalid:timestamp")
    return errors


def decode_response(content, entity, content_format):
    """Yield (entity, raw record); ZIP/protobuf bytes are never silently parsed as JSON."""
    if content_format == "gtfs_zip":
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            for name in archive.namelist():
                table = name.rsplit("/", 1)[-1].removesuffix(".txt")
                if table in STATIC_KEYS:
                    for row in csv.DictReader(io.StringIO(archive.read(name).decode("utf-8-sig"))):
                        yield table, row
        return
    if content_format == "protobuf":
        from google.protobuf.json_format import MessageToDict
        from google.transit import gtfs_realtime_pb2

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(content)
        data = MessageToDict(feed, preserving_proto_field_name=True)
    else:
        data = json.loads(content)
    if entity == "weather":
        hourly = data["hourly"]
        for i, timestamp in enumerate(hourly["time"]):
            yield (
                entity,
                {
                    "timestamp": timestamp,
                    "latitude": data["latitude"],
                    "longitude": data["longitude"],
                    **{key: values[i] for key, values in hourly.items() if key != "time"},
                },
            )
    else:
        header = data.get("header", {})
        if header.get("incrementality") in ("DIFFERENTIAL", 1):
            raise ValueError("DIFFERENTIAL feeds require stateful deletion handling; FULL_DATASET required")
        for record in data.get("entity", []):
            if record.get("is_deleted", record.get("isDeleted", False)):
                raise ValueError("deleted realtime entity is unsupported in FULL_DATASET")
            # Support canonical protobuf JSON camelCase and preserving_proto_field_name.
            record = json.loads(json.dumps(record).replace('"tripUpdate"', '"trip_update"'))
            yield entity, {**record, "feed_timestamp": header.get("timestamp")}


def normalize(entity, raw, source, weather_kind="forecast"):
    """Return flat records. Unknown raw fields remain available in bronze payload."""

    def snake(obj):
        import re

        if isinstance(obj, dict):
            return {re.sub(r"(?<!^)(?=[A-Z])", "_", k).lower(): snake(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [snake(x) for x in obj]
        return obj

    raw = snake(raw)
    if entity in STATIC_KEYS:
        row = dict(raw)
        if entity == "agency" and not row.get("agency_id"):
            row["agency_id"] = "default"  # GTFS permits omission for a single agency.
        for key in ("stop_lat", "stop_lon"):
            if key in row:
                row[{"stop_lat": "latitude", "stop_lon": "longitude"}[key]] = float(row[key])
        yield row
    elif entity == "weather":
        row = dict(raw)
        row["timestamp"] = utc_timestamp(raw["timestamp"]).isoformat()
        row["location_id"] = f"{float(raw['latitude']):.3f},{float(raw['longitude']):.3f}"
        row["weather_condition"] = weather_condition(raw["weather_code"])
        row["weather_kind"] = weather_kind
        yield row
    elif entity == "vehicle_positions":
        v = raw["vehicle"]
        trip, position = v["trip"], v["position"]
        yield {
            "trip_id": trip.get("trip_id"),
            "route_id": trip.get("route_id"),
            "vehicle_id": v.get("vehicle", {}).get("id"),
            "service_date": trip.get("start_date"),
            "timestamp": utc_timestamp(v.get("timestamp", raw.get("feed_timestamp"))).isoformat(),
            "latitude": position.get("latitude"),
            "longitude": position.get("longitude"),
        }
    elif entity == "trip_updates":
        update = raw["trip_update"]
        trip = update["trip"]
        cancelled = trip.get("schedule_relationship") in ("CANCELED", 3)
        stops = update.get("stop_time_update", [])
        if cancelled:
            stops = [{"stop_sequence": "*"}]  # Expanded against static stops in Gold.
        for stop in stops:
            arrival = stop.get("arrival", {})
            timestamp = utc_timestamp(update.get("timestamp", raw.get("feed_timestamp")))
            yield {
                "trip_id": trip.get("trip_id"),
                "route_id": trip.get("route_id"),
                "service_date": trip.get("start_date"),
                "stop_id": stop.get("stop_id"),
                "stop_sequence": str(stop.get("stop_sequence", "")),
                "timestamp": timestamp.isoformat(),
                "reported_arrival_time": utc_timestamp(arrival["time"]).isoformat()
                if arrival.get("time")
                else None,
                "reported_delay_seconds": arrival.get("delay"),
                "arrival_time_kind": "observed" if arrival.get("observed") is True else "prediction",
                "cancellation_flag": cancelled,
                "skipped_flag": stop.get("schedule_relationship") in ("SKIPPED", 1),
                "delay_minutes": float(arrival["delay"]) / 60 if arrival.get("delay") is not None else None,
            }
    else:
        raise ValueError(f"unknown entity: {entity}")
