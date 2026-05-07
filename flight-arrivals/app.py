import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import json
import requests
import boto3
from boto3.dynamodb.conditions import Key
from chalice import Chalice

app = Chalice(app_name="flight-arrivals")

dynamodb = boto3.resource("dynamodb")
s3_identity = boto3.client("sts")
s3_client = boto3.client("s3")

TABLE_NAME = os.getenv("TABLE_NAME", "flight_arrivals")
PLOT_BUCKET = os.getenv("PLOT_BUCKET", "")
PROJECT_NAME = os.getenv("PROJECT_NAME", "flight-arrivals")
PROJECT_DESCRIPTION = (
    "Tracks recent flight arrivals at IAD and LHR; provides latest sample, a trend summary, and a plot URL."
)

# AirLabs config
AIRLABS_API_BASE = "https://airlabs.co/api/v9"
AIRLABS_SECRET_NAME = os.getenv("AIRLABS_SECRET_NAME", "airlabs-api-key")
secrets_client = boto3.client("secretsmanager")


def _get_airlabs_api_key() -> Optional[str]:
    try:
        resp = secrets_client.get_secret_value(SecretId=AIRLABS_SECRET_NAME)
        s = resp.get("SecretString", "")
        try:
            d = json.loads(s or "{}")
            return d.get("api_key") or d.get("AIRLABS_API_KEY") or d.get("key") or s
        except Exception:
            return s
    except Exception as e:
        app.log.error(f"Failed to read AirLabs secret: {e}")
        return None


def _fetch_airlabs_range(airport: str, start_dt, end_dt) -> List[Dict]:
    """Fetch flights from AirLabs for airport between start_dt and end_dt.

    Uses unix timestamps 'from' and 'to' as query params; falls back to single call if provider ignores range.
    Returns list of items matching our DynamoDB schema (airport, airline, flight_number, arrival_time).
    """
    api_key = _get_airlabs_api_key()
    if not api_key:
        return []

    try:
        params = {
            "api_key": api_key,
            "arr_iata": airport,
            "limit": 1000,
            "from": int(start_dt.timestamp()),
            "to": int(end_dt.timestamp()),
        }
        r = requests.get(f"{AIRLABS_API_BASE}/flights", params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        flights = data.get("response") or []
        # If provider ignored range and returned empty, fall back to broad call and filter locally
        if not flights:
            r2 = requests.get(f"{AIRLABS_API_BASE}/flights", params={"api_key": api_key, "arr_iata": airport, "limit": 1000}, timeout=15)
            r2.raise_for_status()
            flights = r2.json().get("response") or []
        items = []
        for f in flights:
            updated = f.get("updated")
            if not updated:
                continue
            # filter to requested window
            if updated < int(start_dt.timestamp()) or updated > int(end_dt.timestamp()):
                continue
            at = datetime.fromtimestamp(updated, tz=timezone.utc).isoformat()
            items.append({
                "airport": airport,
                "airline": f.get("airline_iata") or f.get("airline_icao") or "UNK",
                "flight_number": f.get("flight_iata") or f.get("flight_icao") or "",
                "arrival_time": at,
            })
        return items
    except Exception as e:
        app.log.error(f"AirLabs historical fetch failed for {airport}: {e}")
        return []

RESOURCE_NAMES = ["current", "trend", "plot"]


def _table():
    return dynamodb.Table(TABLE_NAME)


def _normalize_airport(airport: Optional[str]) -> str:
    value = (airport or "ALL").strip().upper()
    return value if value in {"IAD", "LHR", "ALL"} else "ALL"


def _parse_iso(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _query_airport_items(airport: str, cutoff: Optional[datetime] = None, limit: Optional[int] = None) -> List[Dict]:
    query_kwargs = {
        "KeyConditionExpression": Key("airport").eq(airport),
        "ScanIndexForward": False,
    }

    if cutoff is not None:
        query_kwargs["KeyConditionExpression"] = Key("airport").eq(airport) & Key("arrival_time").gte(
            cutoff.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        )

    if limit is not None:
        query_kwargs["Limit"] = limit

    response = _table().query(**query_kwargs)
    return response.get("Items", [])


def _all_airports() -> List[str]:
    return ["IAD", "LHR"]


def _collect_items(airport: str, cutoff: Optional[datetime] = None, limit_per_airport: Optional[int] = None) -> List[Dict]:
    airports = _all_airports() if airport == "ALL" else [airport]
    items: List[Dict] = []
    for code in airports:
        items.extend(_query_airport_items(code, cutoff=cutoff, limit=limit_per_airport))
    items.sort(key=lambda item: item.get("arrival_time", ""), reverse=True)
    return items


def _latest_summary(items: List[Dict]) -> str:
    if not items:
        return "No arrival records are available yet."

    latest = items[0]
    airport = latest.get("airport", "unknown airport")
    airline = latest.get("airline", "UNK")
    flight_number = latest.get("flight_number", "unknown flight")
    arrival_time = latest.get("arrival_time", "unknown time")
    return f"Latest arrival for {airport} is {flight_number} ({airline}) at {arrival_time}."


def _trend_summary(items: List[Dict], window_hours: int) -> str:
    if not items:
        return f"No arrivals recorded in the last {window_hours} hours."

    counts = Counter(item.get("airline", "UNK") or "UNK" for item in items)
    top_airline, top_count = counts.most_common(1)[0]
    airport_counts = Counter(item.get("airport", "UNK") for item in items)
    airport_text = ", ".join(f"{airport}: {count}" for airport, count in sorted(airport_counts.items()))
    return (
        f"In the last {window_hours} hours, {len(items)} arrivals were recorded. "
        f"Top airline: {top_airline} with {top_count} arrivals. "
        f"Airport split: {airport_text}."
    )


def _build_svg(items: List[Dict], airport: str, window_hours: int) -> bytes:
    # Build an hourly time-series SVG line chart for the top airlines
    # Prepare time bins (hourly) from cutoff (now - window_hours) to now
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=window_hours)
    # Create list of hourly bin boundaries
    hours = []
    t = start.replace(minute=0, second=0, microsecond=0)
    while t <= now:
        hours.append(t)
        t = t + timedelta(hours=1)

    # Select top airlines overall to plot (limit to 5 for clarity)
    airline_counts = Counter(item.get("airline", "UNK") or "UNK" for item in items)
    top_airlines = [a for a, _ in airline_counts.most_common(5)]
    if not top_airlines:
        top_airlines = ["UNK"]

    # Initialize series per airline
    series = {airline: [0] * (len(hours)) for airline in top_airlines}

    # Populate counts into hourly bins
    for item in items:
        at = _parse_iso(item.get("arrival_time", ""))
        if not at:
            continue
        # ignore items outside window
        if at < start or at > now:
            continue
        # find nearest hour index
        idx = int((at - hours[0]).total_seconds() // 3600)
        airline = item.get("airline", "UNK") or "UNK"
        if airline in series:
            # protect index bounds
            if 0 <= idx < len(hours):
                series[airline][idx] += 1

    # SVG canvas settings
    width = 900
    height = 360
    margin_left = 60
    margin_right = 20
    margin_top = 30
    margin_bottom = 70

    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    # Determine max y for scaling
    max_y = max((max(vals) if vals else 0) for vals in series.values())
    max_y = max(max_y, 1)

    # Color palette
    colors = ["#2a6fdb", "#e76f51", "#2a9d8f", "#f4a261", "#8d99ae"]

    svg_parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    svg_parts.append('<style>text{font-family:Arial, sans-serif; font-size:12px}</style>')
    svg_parts.append(f'<text x="10" y="18">Arrivals time series for {airport} (last {window_hours}h)</text>')

    # Draw axes
    svg_parts.append(f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top+plot_h}" stroke="#333"/>')
    svg_parts.append(f'<line x1="{margin_left}" y1="{margin_top+plot_h}" x2="{margin_left+plot_w}" y2="{margin_top+plot_h}" stroke="#333"/>')

    # Y-axis labels and grid
    y_steps = 5
    for i in range(y_steps + 1):
        y_val = int(max_y * i / y_steps)
        y_pos = margin_top + plot_h - (plot_h * i / y_steps)
        svg_parts.append(f'<line x1="{margin_left}" y1="{y_pos}" x2="{margin_left+plot_w}" y2="{y_pos}" stroke="#eee"/>')
        svg_parts.append(f'<text x="{margin_left-8}" y="{y_pos+4}" text-anchor="end">{y_val}</text>')

    # X-axis labels (every Nth hour to avoid crowding)
    step = max(1, len(hours) // 8)
    for i, ts in enumerate(hours):
        x = margin_left + (plot_w * i / max(1, len(hours)-1))
        if i % step == 0 or i == len(hours)-1:
            label = ts.strftime('%m-%d %H:%M')
            svg_parts.append(f'<text x="{x}" y="{margin_top+plot_h+18}" text-anchor="middle">{label}</text>')
            svg_parts.append(f'<line x1="{x}" y1="{margin_top+plot_h}" x2="{x}" y2="{margin_top+plot_h+4}" stroke="#333"/>')

    # Plot lines for each airline
    for idx, (airline, vals) in enumerate(series.items()):
        points = []
        for i, v in enumerate(vals):
            x = margin_left + (plot_w * i / max(1, len(hours)-1))
            y = margin_top + plot_h - (v / max_y) * plot_h
            points.append(f'{x},{y}')
        color = colors[idx % len(colors)]
        svg_parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{" ".join(points)}" />')
        # draw circles at points
        for p in points:
            px, py = p.split(',')
            svg_parts.append(f'<circle cx="{px}" cy="{py}" r="2" fill="{color}" />')

    # Legend
    legend_x = margin_left
    legend_y = margin_top + plot_h + 40
    for idx, airline in enumerate(series.keys()):
        color = colors[idx % len(colors)]
        lx = legend_x + idx * 140
        svg_parts.append(f'<rect x="{lx}" y="{legend_y-12}" width="12" height="12" fill="{color}"/>')
        svg_parts.append(f'<text x="{lx+18}" y="{legend_y-2}">{airline}</text>')

    svg_parts.append('</svg>')
    svg = "\n".join(svg_parts)
    return svg.encode('utf-8')


def _store_plot(image_bytes: bytes, airport: str, window_hours: int) -> str:
    # Store SVG in S3 and return a presigned URL
    bucket_name = PLOT_BUCKET or f"{PROJECT_NAME}-plots-{s3_identity.get_caller_identity()['Account']}-{os.getenv('AWS_REGION', 'us-east-1')}"
    key = f"dp3/flight-arrivals/{airport.lower()}/latest.svg"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=image_bytes,
        ContentType="image/svg+xml",
        CacheControl="no-cache",
    )
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=3600,
    )


def _strip_svg_wrapper(svg_text: str) -> str:
    """Remove outer <svg ...> wrapper and return inner markup."""
    start = svg_text.find('>')
    end = svg_text.rfind('</svg>')
    if start == -1 or end == -1:
        return svg_text
    return svg_text[start+1:end]


def _build_side_by_side_svg(items_map: Dict[str, List[Dict]], window_hours: int) -> bytes:
    """Build a side-by-side SVG combining each airport's chart horizontally.

    This reuses the existing `_build_svg` for each airport and embeds the inner
    SVG fragments inside a larger outer SVG positioned side-by-side.
    """
    svgs = []
    # generate per-airport svgs and strip their outer wrappers
    for airport, items in items_map.items():
        inner = _build_svg(items, airport, window_hours).decode('utf-8')
        inner_fragment = _strip_svg_wrapper(inner)
        svgs.append(inner_fragment)

    # assume each inner svg uses same width/height as _build_svg (900x360)
    single_w = 900
    single_h = 360
    total_w = single_w * len(svgs)

    outer = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_w}" height="{single_h}">']
    # include a minimal style once
    outer.append('<style>text{font-family:Arial, sans-serif; font-size:12px}</style>')
    for i, frag in enumerate(svgs):
        tx = i * single_w
        outer.append(f'<g transform="translate({tx},0)">')
        outer.append(frag)
        outer.append('</g>')
    outer.append('</svg>')
    return "\n".join(outer).encode('utf-8')


@app.route("/", methods=["GET"])
def index():
    return {"about": PROJECT_DESCRIPTION, "resources": RESOURCE_NAMES}


@app.route("/current", methods=["GET"])
def current():
    airport = _normalize_airport(app.current_request.query_params.get("airport") if app.current_request.query_params else None)
    # If requesting ALL, return latest per airport
    if airport == "ALL":
        results = {}
        for code in _all_airports():
            items = _collect_items(code, limit_per_airport=1)
            results[code] = _latest_summary(items)
        return {"response": results}
    else:
        items = _collect_items(airport, limit_per_airport=1)
        return {"response": _latest_summary(items)}


@app.route("/trend", methods=["GET"])
def trend():
    query_params = app.current_request.query_params or {}
    airport = _normalize_airport(query_params.get("airport"))
    window_hours = int(query_params.get("window_hours", "24"))
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=window_hours)

    # Prefer DynamoDB historical data (records we've already ingested). If empty, fall back to AirLabs.
    if airport == "ALL":
        combined_items = []
        per_airport = {}
        for code in _all_airports():
            items = _collect_items(code, cutoff=start_dt)
            if not items:
                # fallback to AirLabs
                items = _fetch_airlabs_range(code, start_dt, end_dt)
            per_airport[code] = {
                "summary": _trend_summary(items, window_hours),
                "count": len(items),
            }
            combined_items.extend(items)
        return {"response": {"combined": _trend_summary(combined_items, window_hours), "per_airport": per_airport}}
    else:
        items = _collect_items(airport, cutoff=start_dt)
        if not items:
            items = _fetch_airlabs_range(airport, start_dt, end_dt)
        return {"response": _trend_summary(items, window_hours)}


@app.route("/plot", methods=["GET"])
def plot():
    query_params = app.current_request.query_params or {}
    airport = _normalize_airport(query_params.get("airport"))
    window_hours = int(query_params.get("window_hours", "24"))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    # If requesting ALL, generate a separate plot per airport and return mapping
    if airport == "ALL":
        # Build per-airport svgs and a combined side-by-side SVG; return both
        items_map = {}
        per_urls = {}
        for code in _all_airports():
            items = _collect_items(code, cutoff=cutoff)
            if not items:
                items = _fetch_airlabs_range(code, cutoff, datetime.now(timezone.utc))
            items_map[code] = items
            img = _build_svg(items, code, window_hours)
            per_urls[code] = _store_plot(img, code, window_hours)

        combined_bytes = _build_side_by_side_svg(items_map, window_hours)
        combined_url = _store_plot(combined_bytes, "all", window_hours)
        return {"response": {"combined": combined_url, "per_airport": per_urls}}
    else:
        items = _collect_items(airport, cutoff=cutoff)
        if not items:
            items = _fetch_airlabs_range(airport, cutoff, datetime.now(timezone.utc))
        image_bytes = _build_svg(items, airport, window_hours)
        plot_url = _store_plot(image_bytes, airport, window_hours)
        return {"response": plot_url}
