import os
import re
import math
import time
import asyncio
from typing import Any

import httpx
from mcp.server import MCPServer
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


# ============================================================
# HIDDEN SIGNAL LIVE V3.0 — ZYLA ONLY
# ============================================================

VERSION = "V3.1-ZYLA-DQ"

ZYLA_API_KEY = (os.environ.get("ZYLA_API_KEY") or "").strip()
ZYLA_BASE_URL = (
    "https://zylalabs.com/api/12518/"
    "flashscore+-+live+api"
)

mcp = MCPServer("Hidden Signal Live")


@mcp.custom_route("/", methods=["GET", "HEAD"])
async def health_root(request: Request) -> Response:
    """Render health-check endpoint."""
    return JSONResponse({
        "status": "ok",
        "service": "Hidden Signal Live",
        "version": VERSION,
    })


# ============================================================
# SETTINGS
# ============================================================

STRONG_THRESHOLD = 75.0
MEDIUM_THRESHOLD = 62.0

DEFAULT_PREFILTER_LIMIT = 8
DEFAULT_DEEP_LIMIT = 4
MAX_PREFILTER_LIMIT = 14
MAX_DEEP_LIMIT = 6

LIVE_CACHE_TTL = 10
STATS_CACHE_TTL = 20
DETAILS_CACHE_TTL = 20
SUMMARY_CACHE_TTL = 25
ODDS_CACHE_TTL = 20

REPEAT_SIGNAL_DELTA = 4.0

_CACHE: dict[str, dict[str, Any]] = {}
_LAST_SIGNAL_STATE: dict[str, dict[str, float]] = {}


# ============================================================
# GENERIC HELPERS
# ============================================================

def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default

    if isinstance(value, bool):
        return float(value)

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        match = re.search(r"-?\d+(?:[.,]\d+)?", value)
        if not match:
            return default
        try:
            return float(match.group(0).replace(",", "."))
        except Exception:
            return default

    return default


def safe_int(value: Any, default: int = 0) -> int:
    return int(round(safe_float(value, float(default))))


def normalize_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def unwrap(payload: Any) -> Any:
    if isinstance(payload, dict) and "api_response" in payload:
        return payload["api_response"]
    return payload


def response_nonempty(payload: Any) -> bool:
    return unwrap(payload) not in (None, "", [], {})


def http_status(payload: Any):
    if not isinstance(payload, dict):
        return None
    return payload.get("diagnostic", {}).get("http_status")


def cache_get(key: str):
    item = _CACHE.get(key)

    if not item:
        return None

    if time.time() >= item["expires_at"]:
        _CACHE.pop(key, None)
        return None

    return item["value"]


def cache_set(key: str, value: Any, ttl: int):
    _CACHE[key] = {
        "value": value,
        "expires_at": time.time() + ttl,
    }


# ============================================================
# ZYLA HTTP
# ============================================================

async def zyla_get(
    endpoint_id: int,
    endpoint_slug: str,
    params: dict | None = None,
    *,
    cache_key: str | None = None,
    cache_ttl: int = 0,
):
    if not ZYLA_API_KEY:
        return {
            "source": "zyla",
            "error": "ZYLA_API_KEY is not configured",
            "diagnostic": {
                "key_loaded": False,
                "http_status": None,
            },
        }

    if cache_key and cache_ttl > 0:
        cached = cache_get(cache_key)
        if cached is not None:
            result = dict(cached)
            result["cache_hit"] = True
            return result

    url = f"{ZYLA_BASE_URL}/{endpoint_id}/{endpoint_slug}"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {ZYLA_API_KEY}"},
                params=params or {},
            )

        try:
            data = response.json()
        except Exception:
            data = {"raw_response": response.text}

        result = {
            "source": "zyla",
            "diagnostic": {
                "key_loaded": True,
                "http_status": response.status_code,
                "endpoint_id": endpoint_id,
            },
            "api_response": data,
            "cache_hit": False,
        }

        if (
            cache_key
            and cache_ttl > 0
            and response.status_code == 200
        ):
            cache_set(cache_key, result, cache_ttl)

        return result

    except Exception as exc:
        return {
            "source": "zyla",
            "error": str(exc),
            "diagnostic": {
                "key_loaded": bool(ZYLA_API_KEY),
                "http_status": None,
                "endpoint_id": endpoint_id,
            },
        }


async def zyla_live(*, fresh: bool = False):
    return await zyla_get(
        23856,
        "get+live+matches",
        {"sport_id": 1},
        cache_key=None if fresh else "live",
        cache_ttl=0 if fresh else LIVE_CACHE_TTL,
    )


async def zyla_details(match_id: str):
    return await zyla_get(
        23859,
        "get+match+details",
        {"match_id": match_id},
        cache_key=f"details:{match_id}",
        cache_ttl=DETAILS_CACHE_TTL,
    )


async def zyla_summary(match_id: str):
    return await zyla_get(
        23860,
        "get+match+summary",
        {"match_id": match_id},
        cache_key=f"summary:{match_id}",
        cache_ttl=SUMMARY_CACHE_TTL,
    )


async def zyla_stats(match_id: str):
    return await zyla_get(
        23861,
        "get+match+stats",
        {"match_id": match_id},
        cache_key=f"stats:{match_id}",
        cache_ttl=STATS_CACHE_TTL,
    )


async def zyla_odds(match_id: str):
    return await zyla_get(
        23865,
        "get+match+odds",
        {"match_id": match_id},
        cache_key=f"odds:{match_id}",
        cache_ttl=ODDS_CACHE_TTL,
    )


# ============================================================
# EXACT ZYLA LIVE PARSER
# Official structure:
# [
#   {
#     "name": "...",
#     "matches": [
#       {
#         "match_id": "...",
#         "match_status": {"live_time": "29'"},
#         "home_team": {"name": "...", "red_cards": 0},
#         "away_team": {"name": "...", "red_cards": 0},
#         "scores": {"home": 1, "away": 0}
#       }
#     ]
#   }
# ]
# ============================================================

def parse_live_minute(match: dict) -> int:
    status = match.get("match_status")

    if isinstance(status, dict):
        live_time = status.get("live_time")

        # Handles "29'", "45+2", "98'", etc.
        minute = safe_int(live_time)

        if minute > 0:
            return int(clamp(minute, 0, 130))

        stage = str(status.get("stage") or "").lower()

        if "half time" in stage:
            return 45

    return 0


def parse_scores_object(value: Any) -> tuple[int, int] | None:
    if not isinstance(value, dict):
        return None

    if "home" in value and "away" in value:
        return safe_int(value.get("home")), safe_int(value.get("away"))

    return None


def parse_live_match(match: dict, tournament: dict | None = None) -> dict | None:
    if not isinstance(match, dict):
        return None

    match_id = match.get("match_id")

    if not match_id:
        return None

    home_obj = match.get("home_team")
    away_obj = match.get("away_team")
    scores = parse_scores_object(match.get("scores")) or (0, 0)

    home_name = (
        str(home_obj.get("name") or "").strip()
        if isinstance(home_obj, dict)
        else ""
    )
    away_name = (
        str(away_obj.get("name") or "").strip()
        if isinstance(away_obj, dict)
        else ""
    )

    red_home = (
        safe_int(home_obj.get("red_cards"))
        if isinstance(home_obj, dict)
        else 0
    )
    red_away = (
        safe_int(away_obj.get("red_cards"))
        if isinstance(away_obj, dict)
        else 0
    )

    return {
        "match_id": str(match_id),
        "home": home_name,
        "away": away_name,
        "minute": parse_live_minute(match),
        "score_home": scores[0],
        "score_away": scores[1],
        "red_home": red_home,
        "red_away": red_away,
        "tournament": (
            str((tournament or {}).get("name") or "").strip()
        ),
    }


def extract_live_candidates(payload: Any) -> list[dict]:
    raw = unwrap(payload)
    output = []
    seen = set()

    # Exact documented structure first.
    if isinstance(raw, list):
        for tournament in raw:
            if not isinstance(tournament, dict):
                continue

            matches = tournament.get("matches")

            if not isinstance(matches, list):
                continue

            for match in matches:
                parsed = parse_live_match(match, tournament)

                if not parsed:
                    continue

                match_id = parsed["match_id"]

                if match_id in seen:
                    continue

                seen.add(match_id)
                output.append(parsed)

    return output


def find_live_candidate(
    candidates: list[dict],
    match_id: str,
) -> dict | None:
    target = str(match_id)

    for candidate in candidates:
        if str(candidate.get("match_id")) == target:
            return candidate

    return None


# ============================================================
# EXACT ZYLA DETAILS PARSER
# ============================================================

def parse_details(payload: Any) -> dict:
    raw = unwrap(payload)

    if not isinstance(raw, dict):
        return {
            "home": "",
            "away": "",
            "minute": 0,
            "score_home": 0,
            "score_away": 0,
            "score_present": False,
        }

    home_obj = raw.get("home_team")
    away_obj = raw.get("away_team")

    home = (
        str(home_obj.get("name") or "").strip()
        if isinstance(home_obj, dict)
        else ""
    )
    away = (
        str(away_obj.get("name") or "").strip()
        if isinstance(away_obj, dict)
        else ""
    )

    scores_obj = raw.get("scores")
    scores = parse_scores_object(scores_obj)
    score_present = scores is not None

    if scores is None:
        scores = (0, 0)

    status = raw.get("match_status")
    minute = 0

    if isinstance(status, dict):
        minute = safe_int(status.get("live_time"))

        if minute <= 0:
            stage = str(status.get("stage") or "").lower()
            if "half time" in stage:
                minute = 45

    return {
        "home": home,
        "away": away,
        "minute": int(clamp(minute, 0, 130)),
        "score_home": scores[0],
        "score_away": scores[1],
        "score_present": score_present,
    }


# ============================================================
# EXACT ZYLA STATS PARSER
# Official structure:
# {
#   "match": [
#     {"name":"Expected goals (xG)","home_team":1.47,"away_team":0.43},
#     ...
#   ]
# }
# ============================================================

STAT_ALIASES = {
    "xg": (
        "expected goals (xg)",
        "expected goals",
        "xg",
    ),
    "shots": (
        "total shots",
        "shots",
    ),
    "shots_on_target": (
        "shots on target",
        "shots on goal",
    ),
    "shots_in_box": (
        "shots inside the box",
        "shots inside box",
        "shots in box",
    ),
    "touches_in_box": (
        "touches in opposition box",
        "touches in opponent box",
        "touches in box",
    ),
    "corners": (
        "corner kicks",
        "corners",
    ),
    "possession": (
        "ball possession",
        "possession",
    ),
    "xa": (
        "expected assists (xa)",
        "expected assists",
        "xa",
    ),
    "fouls": (
        "fouls",
        "fouls committed",
    ),
    "red_cards": (
        "red cards",
        "red card",
    ),
}


def stat_name_matches(name: str, aliases: tuple[str, ...]) -> bool:
    normalized = normalize_key(name)

    for alias in aliases:
        alias_normalized = normalize_key(alias)

        if normalized == alias_normalized:
            return True

        if alias_normalized in normalized:
            return True

    return False


def parse_stats_rows(payload: Any) -> list[dict]:
    raw = unwrap(payload)

    if not isinstance(raw, dict):
        return []

    rows = raw.get("match")

    if not isinstance(rows, list):
        return []

    return [
        row
        for row in rows
        if isinstance(row, dict)
    ]


def get_stat_entry(
    rows: list[dict],
    aliases: tuple[str, ...],
) -> dict:
    """
    Distinguish a real 0:0 statistic from a missing statistic.
    "present=True" means Zyla actually supplied the row.
    """
    for row in rows:
        name = row.get("name")

        if not isinstance(name, str):
            continue

        if not stat_name_matches(name, aliases):
            continue

        return {
            "present": True,
            "home": safe_float(row.get("home_team")),
            "away": safe_float(row.get("away_team")),
            "raw_name": name,
        }

    return {
        "present": False,
        "home": 0.0,
        "away": 0.0,
        "raw_name": None,
    }


def build_data_quality(availability: dict[str, bool]) -> dict:
    """Rate completeness without treating missing advanced stats as zero."""
    weights = {
        "shots": 20,
        "shots_on_target": 20,
        "xg": 20,
        "touches_in_box": 15,
        "shots_in_box": 10,
        "corners": 8,
        "possession": 4,
        "xa": 3,
    }

    score = sum(
        weight
        for name, weight in weights.items()
        if availability.get(name, False)
    )

    basic_ok = (
        availability.get("shots", False)
        and availability.get("shots_on_target", False)
    )

    advanced_count = sum(
        1
        for name in ("xg", "touches_in_box", "shots_in_box")
        if availability.get(name, False)
    )

    # Strong signals require basic attacking data plus at least
    # one advanced chance-quality metric.
    strong_eligible = basic_ok and advanced_count >= 1 and score >= 55

    if strong_eligible and score >= 75:
        level = "HIGH"
    elif basic_ok and score >= 45:
        level = "MEDIUM"
    else:
        level = "LOW"

    missing = [
        name
        for name in weights
        if not availability.get(name, False)
    ]

    return {
        "score": int(score),
        "level": level,
        "basic_ok": basic_ok,
        "advanced_count": advanced_count,
        "strong_eligible": strong_eligible,
        "missing": missing,
    }


def parse_stats(payload: Any) -> dict:
    rows = parse_stats_rows(payload)

    output = {}
    availability = {}

    for metric_name, aliases in STAT_ALIASES.items():
        entry = get_stat_entry(rows, aliases)
        home = entry["home"]
        away = entry["away"]

        availability[metric_name] = entry["present"]

        output[metric_name] = {
            "home": home,
            "away": away,
            "total": home + away,
            "present": entry["present"],
        }

    # Parser health is about whether the response can be interpreted,
    # not whether xG happens to equal zero.
    parser_ok = bool(rows) and (
        availability.get("shots", False)
        or availability.get("shots_on_target", False)
        or availability.get("xg", False)
    )

    data_quality = build_data_quality(availability)

    return {
        "rows_count": len(rows),
        "parser_ok": parser_ok,
        "availability": availability,
        "data_quality": data_quality,
        **output,
    }


# ============================================================
# NORMALIZATION + SCORE SAFETY
# ============================================================

def normalize_match(
    live_candidate: dict | None,
    details_payload: Any,
    stats_payload: Any,
) -> dict:
    live_candidate = live_candidate or {}
    details = parse_details(details_payload)
    stats = parse_stats(stats_payload)

    has_live = bool(live_candidate)

    if has_live:
        home = live_candidate.get("home") or details["home"]
        away = live_candidate.get("away") or details["away"]

        score_home = safe_int(live_candidate.get("score_home"))
        score_away = safe_int(live_candidate.get("score_away"))
        minute = safe_int(live_candidate.get("minute"))

        red_home = safe_int(live_candidate.get("red_home"))
        red_away = safe_int(live_candidate.get("red_away"))

    else:
        home = details["home"]
        away = details["away"]

        score_home = details["score_home"]
        score_away = details["score_away"]
        minute = details["minute"]

        red_home = safe_int(stats["red_cards"]["home"])
        red_away = safe_int(stats["red_cards"]["away"])

    details_can_verify_score = bool(details["score_present"])

    score_conflict = (
        has_live
        and details_can_verify_score
        and (
            score_home != details["score_home"]
            or score_away != details["score_away"]
        )
    )

    score_sync = {
        "ok": not score_conflict,
        "conflict": score_conflict,
        "live_score": (
            {"home": score_home, "away": score_away}
            if has_live
            else None
        ),
        "details_score": (
            {
                "home": details["score_home"],
                "away": details["score_away"],
            }
            if details_can_verify_score
            else None
        ),
        "details_verified": details_can_verify_score,
        "authoritative_source": (
            "fresh_live_list"
            if has_live
            else "match_details"
        ),
    }

    parser_ok = stats["parser_ok"]

    return {
        "home": home,
        "away": away,
        "minute": minute,
        "score": {
            "home": score_home,
            "away": score_away,
            "total": score_home + score_away,
        },
        "score_sync": score_sync,
        "parser_ok": parser_ok,
        "parser_warning": (
            None
            if parser_ok
            else "Zyla stats response cannot be safely normalized"
        ),
        "data_quality": stats["data_quality"],
        "availability": stats["availability"],
        "xg": stats["xg"],
        "shots": stats["shots"],
        "shots_on_target": stats["shots_on_target"],
        "shots_in_box": stats["shots_in_box"],
        "touches_in_box": stats["touches_in_box"],
        "corners": stats["corners"],
        "possession": stats["possession"],
        "xa": stats["xa"],
        "fouls": stats["fouls"],
        "red_cards": {
            "home": red_home,
            "away": red_away,
            "total": red_home + red_away,
        },
    }


# ============================================================
# ODDS SNAPSHOT
# ============================================================

def parse_odds_snapshot(payload: Any) -> dict:
    raw = unwrap(payload)

    if raw in (None, "", [], {}):
        return {
            "available": False,
            "bookmakers_count": 0,
        }

    bookmakers = set()

    def visit(value: Any):
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = normalize_key(key)

                if normalized in {
                    "bookmaker",
                    "bookmaker name",
                } and isinstance(child, str):
                    bookmakers.add(child.strip())

                visit(child)

        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(raw)

    return {
        "available": True,
        "bookmakers_count": len(bookmakers),
        "bookmakers": sorted(bookmakers)[:8],
    }


# ============================================================
# SIGNAL MODEL
# Percent values are heuristic ranking estimates, not
# calibrated betting probabilities.
# ============================================================

def pressure_index(metrics: dict) -> float:
    minute = max(metrics["minute"], 1)

    xg_rate = metrics["xg"]["total"] * 90 / minute
    shots_rate = metrics["shots"]["total"] * 90 / minute
    sot_rate = metrics["shots_on_target"]["total"] * 90 / minute
    box_rate = metrics["shots_in_box"]["total"] * 90 / minute
    touches_rate = metrics["touches_in_box"]["total"] * 90 / minute
    corners_rate = metrics["corners"]["total"] * 90 / minute

    value = (
        clamp(xg_rate / 3.2, 0, 1) * 0.30
        + clamp(shots_rate / 25.0, 0, 1) * 0.15
        + clamp(sot_rate / 9.0, 0, 1) * 0.20
        + clamp(box_rate / 15.0, 0, 1) * 0.10
        + clamp(touches_rate / 45.0, 0, 1) * 0.15
        + clamp(corners_rate / 11.0, 0, 1) * 0.10
    )

    return round(value * 100, 1)


def estimated_goal_rate_90(metrics: dict) -> float:
    minute = max(metrics["minute"], 1)

    observed_xg_rate = metrics["xg"]["total"] * 90 / minute

    shots = metrics["shots"]["total"]
    sot = metrics["shots_on_target"]["total"]

    shot_proxy_xg = (
        max(sot, 0) * 0.20
        + max(shots - sot, 0) * 0.04
    )

    shot_proxy_rate = shot_proxy_xg * 90 / minute

    pressure = pressure_index(metrics) / 100

    # Blend league-like baseline with observed match information.
    rate_90 = (
        2.60 * 0.45
        + observed_xg_rate * 0.35
        + shot_proxy_rate * 0.20
    )

    pressure_multiplier = 0.80 + pressure * 0.65

    red_diff = abs(
        metrics["red_cards"]["home"]
        - metrics["red_cards"]["away"]
    )

    if red_diff:
        pressure_multiplier *= 1.08

    return clamp(
        rate_90 * pressure_multiplier,
        0.60,
        6.20,
    )


def goal_probability_for_minutes(
    metrics: dict,
    minutes: float,
) -> float:
    minutes = max(minutes, 0)

    rate_90 = estimated_goal_rate_90(metrics)
    expected_goals = rate_90 * minutes / 90

    probability = 1 - math.exp(-expected_goals)

    return clamp(probability * 100, 1, 94)


def team_attack_share(metrics: dict, side: str) -> float:
    other = "away" if side == "home" else "home"

    xg_side = metrics["xg"][side]
    xg_other = metrics["xg"][other]

    shots_side = metrics["shots"][side]
    shots_other = metrics["shots"][other]

    sot_side = metrics["shots_on_target"][side]
    sot_other = metrics["shots_on_target"][other]

    touches_side = metrics["touches_in_box"][side]
    touches_other = metrics["touches_in_box"][other]

    side_score = (
        xg_side * 3.0
        + shots_side * 0.18
        + sot_side * 0.55
        + touches_side * 0.05
        + 0.40
    )

    other_score = (
        xg_other * 3.0
        + shots_other * 0.18
        + sot_other * 0.55
        + touches_other * 0.05
        + 0.40
    )

    if side == "home":
        side_score += (
            metrics["red_cards"]["away"]
            - metrics["red_cards"]["home"]
        ) * 0.70
    else:
        side_score += (
            metrics["red_cards"]["home"]
            - metrics["red_cards"]["away"]
        ) * 0.70

    side_score = max(side_score, 0.05)
    other_score = max(other_score, 0.05)

    return side_score / (side_score + other_score)


def classify(value: float) -> tuple[str, str]:
    if value >= STRONG_THRESHOLD:
        return "🟢", "ENTER"

    if value >= MEDIUM_THRESHOLD:
        return "🟡", "WAIT"

    return "🔴", "SKIP"


def make_signal(
    market: str,
    selection: str,
    value: float,
    reasons: list[str],
    *,
    risk: str = "medium",
) -> dict:
    color, decision = classify(value)

    return {
        "market": market,
        "selection": selection,
        "probability": round(value, 1),
        "signal": color,
        "decision": decision,
        "risk": risk,
        "reasons": reasons[:5],
    }


def format_metric_reason(metrics: dict, key: str, label: str) -> str:
    metric = metrics[key]

    if not metric.get("present", False):
        return f"{label} missing"

    return f"{label} {metric['home']:.2f}-{metric['away']:.2f}"


def apply_data_quality_guard(
    signal: dict,
    metrics: dict,
) -> dict:
    """
    Missing advanced data must reduce confidence, never behave like 0.00.
    Strong ENTER is blocked when the data package is incomplete.
    """
    item = dict(signal)
    dq = metrics["data_quality"]
    market = item.get("market")

    probability = float(item.get("probability", 0))
    reasons = list(item.get("reasons", []))

    # General completeness penalty.
    if dq["level"] == "MEDIUM":
        probability -= 5.0
        reasons.append(f"data quality {dq['score']}/100")
    elif dq["level"] == "LOW":
        probability -= 12.0
        reasons.append(f"low data quality {dq['score']}/100")

    # Conservative unders are especially dangerous if chance-quality
    # metrics are missing: absence of xG is NOT xG=0.
    if market == "OVER_UNDER" and str(item.get("selection", "")).startswith("Under"):
        if not metrics["availability"].get("xg", False):
            probability = min(probability, 69.0)
            reasons.append("xG unavailable: under cannot be strong")

        if not metrics["availability"].get("touches_in_box", False):
            probability = min(probability, 72.0)
            reasons.append("box touches unavailable")

    # Team goal / BTTS needs at least one advanced attacking metric.
    if market in {"TEAM_GOAL", "BTTS"}:
        if metrics["data_quality"]["advanced_count"] == 0:
            probability = min(probability, 69.0)
            reasons.append("no advanced attack metric available")

    probability = clamp(probability, 1, 94)
    color, decision = classify(probability)

    # Hard fail-closed rule for ENTER.
    if decision == "ENTER" and not dq["strong_eligible"]:
        decision = "WAIT"
        color = "🟡"
        probability = min(probability, 74.0)
        reasons.append("strong signal blocked by data-quality guard")

    item["probability"] = round(probability, 1)
    item["signal"] = color
    item["decision"] = decision
    item["data_quality_score"] = dq["score"]
    item["data_quality_level"] = dq["level"]
    item["reasons"] = reasons[:6]

    return item


def build_signals(metrics: dict) -> list[dict]:
    if not metrics["parser_ok"]:
        return []

    minute = metrics["minute"]

    if minute <= 0 or minute >= 100:
        return []

    remaining = max(0, 90 - minute)
    score_total = metrics["score"]["total"]

    pressure = pressure_index(metrics)

    common = [
        f"pressure {pressure}/100",
        format_metric_reason(metrics, "xg", "xG"),
        f"shots {metrics['shots']['home']:.0f}-{metrics['shots']['away']:.0f}",
        f"SOT {metrics['shots_on_target']['home']:.0f}-{metrics['shots_on_target']['away']:.0f}",
        format_metric_reason(metrics, "touches_in_box", "box touches"),
    ]

    signals = []

    # Short-horizon live goal.
    p5 = goal_probability_for_minutes(metrics, 5)
    p10 = goal_probability_for_minutes(metrics, 10)

    signals.append(
        make_signal(
            "GOAL_NEXT_5",
            "Goal in next 5 minutes",
            p5,
            common,
            risk="high",
        )
    )

    signals.append(
        make_signal(
            "GOAL_NEXT_10",
            "Goal in next 10 minutes",
            p10,
            common,
            risk="high",
        )
    )

    # Goal before HT.
    if minute < 45:
        to_ht = max(1, 45 - minute)

        signals.append(
            make_signal(
                "GOAL_BEFORE_HALFTIME",
                "Goal before halftime",
                goal_probability_for_minutes(metrics, to_ht),
                common + [f"{to_ht} min to HT"],
            )
        )

    # Goal before FT / live over current total + 0.5.
    if remaining > 0:
        p_ft = goal_probability_for_minutes(metrics, remaining)

        signals.append(
            make_signal(
                "GOAL_BEFORE_FULLTIME",
                "At least one more goal",
                p_ft,
                common + [f"{remaining} min to FT"],
            )
        )

        signals.append(
            make_signal(
                "OVER_UNDER",
                f"Over {score_total + 0.5:.1f}",
                p_ft,
                common,
            )
        )

        signals.append(
            make_signal(
                "OVER_UNDER",
                f"Under {score_total + 0.5:.1f}",
                100 - p_ft,
                [
                    f"current total {score_total}",
                    f"minute {minute}",
                    f"another-goal estimate {p_ft:.1f}%",
                ],
            )
        )

        # Team to score.
        home_share = team_attack_share(metrics, "home")
        away_share = 1 - home_share

        home_goal = clamp(
            p_ft * (0.45 + home_share),
            1,
            92,
        )
        away_goal = clamp(
            p_ft * (0.45 + away_share),
            1,
            92,
        )

        signals.append(
            make_signal(
                "TEAM_GOAL",
                f"{metrics['home'] or 'Home'} to score",
                home_goal,
                [
                    f"home share {home_share * 100:.0f}%",
                    f"home xG {metrics['xg']['home']:.2f}",
                    f"home shots {metrics['shots']['home']:.0f}",
                    f"home SOT {metrics['shots_on_target']['home']:.0f}",
                ],
            )
        )

        signals.append(
            make_signal(
                "TEAM_GOAL",
                f"{metrics['away'] or 'Away'} to score",
                away_goal,
                [
                    f"away share {away_share * 100:.0f}%",
                    f"away xG {metrics['xg']['away']:.2f}",
                    f"away shots {metrics['shots']['away']:.0f}",
                    f"away SOT {metrics['shots_on_target']['away']:.0f}",
                ],
            )
        )

    # BTTS only if it is not already settled.
    home_scored = metrics["score"]["home"] > 0
    away_scored = metrics["score"]["away"] > 0

    if not (home_scored and away_scored) and remaining > 0:
        p_ft = goal_probability_for_minutes(metrics, remaining)

        if home_scored:
            p_btts = clamp(
                p_ft * (0.55 + team_attack_share(metrics, "away")),
                1,
                90,
            )
        elif away_scored:
            p_btts = clamp(
                p_ft * (0.55 + team_attack_share(metrics, "home")),
                1,
                90,
            )
        else:
            weakest = min(
                team_attack_share(metrics, "home"),
                team_attack_share(metrics, "away"),
            )
            p_btts = clamp(
                p_ft * weakest * 1.20,
                1,
                82,
            )

        signals.append(
            make_signal(
                "BTTS",
                "Both teams to score - YES",
                p_btts,
                [
                    f"score {metrics['score']['home']}-{metrics['score']['away']}",
                    f"xG {metrics['xg']['home']:.2f}-{metrics['xg']['away']:.2f}",
                ],
            )
        )

    signals = [
        apply_data_quality_guard(signal, metrics)
        for signal in signals
    ]

    signals.sort(
        key=lambda item: item["probability"],
        reverse=True,
    )

    return signals


def downgrade_score_conflicts(
    signals: list[dict],
) -> list[dict]:
    output = []

    for signal in signals:
        item = dict(signal)

        if item.get("decision") == "ENTER":
            item["decision"] = "WAIT"
            item["signal"] = "🟡"
            item["risk"] = "high"

            reasons = list(item.get("reasons", []))
            reasons.append("live/details score conflict")
            item["reasons"] = reasons[:5]

        output.append(item)

    return output


def mark_new_signals(
    match_id: str,
    signals: list[dict],
    *,
    commit: bool,
) -> list[dict]:
    state = _LAST_SIGNAL_STATE.setdefault(match_id, {})
    output = []

    for signal in signals:
        item = dict(signal)
        key = f"{item['market']}::{item['selection']}"
        probability = float(item["probability"])

        old = state.get(key)

        new_or_changed = (
            old is None
            or abs(probability - old) >= REPEAT_SIGNAL_DELTA
        )

        item["new_or_changed"] = new_or_changed

        if (
            commit
            and item.get("decision") == "ENTER"
        ):
            state[key] = probability

        output.append(item)

    return output


# ============================================================
# SINGLE MATCH ANALYSIS
# ============================================================

async def analyze_match_internal(
    match_id: str,
    *,
    live_candidate: dict | None = None,
    refresh_live: bool,
    include_details: bool,
    include_summary: bool,
    include_odds: bool,
    mark_repeats: bool,
):
    fresh_live = None

    if refresh_live:
        fresh_live = await zyla_live(fresh=True)

        if http_status(fresh_live) == 200:
            candidates = extract_live_candidates(fresh_live)
            fresh_candidate = find_live_candidate(candidates, match_id)

            if fresh_candidate is not None:
                live_candidate = fresh_candidate

    tasks = [zyla_stats(match_id)]
    names = ["stats"]

    if include_details:
        tasks.append(zyla_details(match_id))
        names.append("details")

    if include_summary:
        tasks.append(zyla_summary(match_id))
        names.append("summary")

    if include_odds:
        tasks.append(zyla_odds(match_id))
        names.append("odds")

    results = await asyncio.gather(*tasks)
    mapped = dict(zip(names, results))

    stats_payload = mapped.get("stats", {})
    details_payload = mapped.get("details", {})
    summary_payload = mapped.get("summary", {})
    odds_payload = mapped.get("odds", {})

    normalized = normalize_match(
        live_candidate,
        details_payload,
        stats_payload,
    )

    signals = build_signals(normalized)

    if normalized["score_sync"]["conflict"]:
        signals = downgrade_score_conflicts(signals)

    if mark_repeats:
        signals = mark_new_signals(
            match_id,
            signals,
            commit=True,
        )
    else:
        signals = [
            {**signal, "new_or_changed": True}
            for signal in signals
        ]

    strong = [
        signal
        for signal in signals
        if signal["decision"] == "ENTER"
        and signal.get("new_or_changed", True)
    ]

    return {
        "source": "hidden-signal-v3",
        "version": VERSION,
        "match_id": match_id,
        "match": {
            "home": normalized["home"],
            "away": normalized["away"],
            "minute": normalized["minute"],
            "score": normalized["score"],
        },
        "parser_ok": normalized["parser_ok"],
        "parser_warning": normalized["parser_warning"],
        "data_quality": normalized["data_quality"],
        "availability": normalized["availability"],
        "score_sync": normalized["score_sync"],
        "metrics": {
            "xg": normalized["xg"],
            "shots": normalized["shots"],
            "shots_on_target": normalized["shots_on_target"],
            "shots_in_box": normalized["shots_in_box"],
            "touches_in_box": normalized["touches_in_box"],
            "corners": normalized["corners"],
            "possession": normalized["possession"],
            "red_cards": normalized["red_cards"],
        },
        "pressure": pressure_index(normalized) if normalized["parser_ok"] else None,
        "odds_snapshot": parse_odds_snapshot(odds_payload),
        "top_candidate": signals[0] if signals else None,
        "top_3_signals": signals[:3],
        "strong_signals": strong,
        "all_signals": signals,
        "diagnostic": {
            "fresh_live_http": http_status(fresh_live) if fresh_live else None,
            "stats_http": http_status(stats_payload),
            "details_http": http_status(details_payload) if details_payload else None,
            "summary_http": http_status(summary_payload) if summary_payload else None,
            "odds_http": http_status(odds_payload) if odds_payload else None,
        },
    }


@mcp.tool()
async def analyze_zyla_match(match_id: str):
    """
    Full direct Hidden Signal analysis for one live match.

    It always refreshes the live list first, uses that score/minute as
    authoritative, checks match details for conflicts, parses statistics,
    and returns Top-3 plus strong signals.
    """
    return await analyze_match_internal(
        match_id,
        refresh_live=True,
        include_details=True,
        include_summary=True,
        include_odds=True,
        mark_repeats=True,
    )


# ============================================================
# SCANNER
# ============================================================

def prefilter_score(candidate: dict) -> float:
    minute = candidate.get("minute", 0)
    total_goals = (
        candidate.get("score_home", 0)
        + candidate.get("score_away", 0)
    )

    score = 0.0

    if 8 <= minute <= 88:
        score += 40

    if 25 <= minute < 45:
        score += 12

    if 55 <= minute <= 85:
        score += 18

    if total_goals <= 4:
        score += 10

    if candidate.get("home") and candidate.get("away"):
        score += 10

    if candidate.get("red_home") != candidate.get("red_away"):
        score += 8

    return score


@mcp.tool()
async def scan_zyla_live(
    prefilter_limit: int = DEFAULT_PREFILTER_LIMIT,
    deep_limit: int = DEFAULT_DEEP_LIMIT,
):
    """
    Efficient full live scan.

    Uses one exact fresh live list for the whole scan. The same parsed
    live score/minute is passed into every candidate analysis, so the
    scanner cannot silently replace a 1:0 or 3:1 live score with 0:0.
    """

    prefilter_limit = int(
        clamp(prefilter_limit, 1, MAX_PREFILTER_LIMIT)
    )
    deep_limit = int(
        clamp(deep_limit, 1, MAX_DEEP_LIMIT)
    )

    live = await zyla_live(fresh=True)

    if http_status(live) != 200:
        return {
            "source": "hidden-signal-v3",
            "version": VERSION,
            "status": "ZYLA_ERROR",
            "live_matches_found": 0,
            "prefiltered_matches": 0,
            "cheap_analyzed": 0,
            "fully_analyzed": 0,
            "parser_failures": 0,
            "score_conflicts": 0,
            "quality_blocked": 0,
            "strong_signals": [],
            "top_candidates": [],
            "estimated_api_calls_this_scan": 1,
        }

    candidates = extract_live_candidates(live)

    candidates.sort(
        key=prefilter_score,
        reverse=True,
    )

    selected = candidates[:prefilter_limit]

    semaphore = asyncio.Semaphore(4)

    async def cheap(candidate: dict):
        async with semaphore:
            try:
                return await analyze_match_internal(
                    candidate["match_id"],
                    live_candidate=candidate,
                    refresh_live=False,
                    include_details=False,
                    include_summary=False,
                    include_odds=False,
                    mark_repeats=False,
                )
            except Exception as exc:
                return {
                    "match_id": candidate["match_id"],
                    "error": str(exc),
                }

    cheap_results = await asyncio.gather(
        *(cheap(candidate) for candidate in selected)
    )

    parser_failures = sum(
        1
        for item in cheap_results
        if isinstance(item, dict)
        and not item.get("error")
        and item.get("parser_ok") is False
    )

    quality_blocked = sum(
        1
        for item in cheap_results
        if isinstance(item, dict)
        and not item.get("error")
        and item.get("parser_ok") is True
        and not item.get("data_quality", {}).get("strong_eligible", False)
    )

    valid = [
        item
        for item in cheap_results
        if isinstance(item, dict)
        and not item.get("error")
        and item.get("parser_ok") is True
        and item.get("top_candidate")
    ]

    valid.sort(
        key=lambda item: item["top_candidate"]["probability"],
        reverse=True,
    )

    deep_seed = valid[:deep_limit]

    async def deep(item: dict):
        async with semaphore:
            candidate = find_live_candidate(
                selected,
                item["match_id"],
            )

            try:
                return await analyze_match_internal(
                    item["match_id"],
                    live_candidate=candidate,
                    refresh_live=False,
                    include_details=True,
                    include_summary=True,
                    include_odds=True,
                    mark_repeats=False,
                )
            except Exception as exc:
                return {
                    "match_id": item["match_id"],
                    "error": str(exc),
                }

    deep_results = (
        await asyncio.gather(
            *(deep(item) for item in deep_seed)
        )
        if deep_seed
        else []
    )

    final_by_id = {
        item["match_id"]: item
        for item in valid
    }

    for item in deep_results:
        if (
            isinstance(item, dict)
            and item.get("match_id")
            and not item.get("error")
        ):
            final_by_id[item["match_id"]] = item

    final_results = list(final_by_id.values())

    top_candidates = []
    strong_pool = []

    for analysis in final_results:
        match = analysis["match"]
        top = analysis.get("top_candidate")

        if top:
            top_candidates.append({
                "match_id": analysis["match_id"],
                "home": match["home"],
                "away": match["away"],
                "minute": match["minute"],
                "score": match["score"],
                "score_sync_ok": analysis["score_sync"]["ok"],
                "data_quality": analysis.get("data_quality"),
                "market": top["market"],
                "selection": top["selection"],
                "probability": top["probability"],
                "decision": top["decision"],
                "reasons": top["reasons"],
            })

        for signal in analysis.get("all_signals", []):
            if signal.get("decision") != "ENTER":
                continue

            strong_pool.append({
                "match_id": analysis["match_id"],
                "home": match["home"],
                "away": match["away"],
                "minute": match["minute"],
                "score": match["score"],
                "score_sync_ok": analysis["score_sync"]["ok"],
                "data_quality": analysis.get("data_quality"),
                "market": signal["market"],
                "selection": signal["selection"],
                "probability": signal["probability"],
                "risk": signal["risk"],
                "reasons": signal["reasons"],
            })

    # Apply repeat guard only ONCE, after the scan has finished.
    strong_signals = []

    for item in strong_pool:
        match_id = item["match_id"]
        key = f"{item['market']}::{item['selection']}"
        probability = float(item["probability"])

        state = _LAST_SIGNAL_STATE.setdefault(match_id, {})
        old = state.get(key)

        new_or_changed = (
            old is None
            or abs(probability - old) >= REPEAT_SIGNAL_DELTA
        )

        if not new_or_changed:
            continue

        state[key] = probability

        strong_signals.append({
            **item,
            "decision": "ENTER",
            "new_or_changed": True,
        })

    top_candidates.sort(
        key=lambda item: item["probability"],
        reverse=True,
    )

    strong_signals.sort(
        key=lambda item: item["probability"],
        reverse=True,
    )

    fully_analyzed = sum(
        1
        for item in deep_results
        if isinstance(item, dict)
        and not item.get("error")
    )

    score_conflicts = sum(
        1
        for item in deep_results
        if isinstance(item, dict)
        and item.get("score_sync", {}).get("conflict") is True
    )

    api_estimate = (
        1
        + len(selected)
        + fully_analyzed * 3
    )

    return {
        "source": "hidden-signal-v3",
        "version": VERSION,
        "status": "OK",
        "live_http_status": 200,
        "live_matches_found": len(candidates),
        "prefiltered_matches": len(selected),
        "cheap_analyzed": len(valid),
        "fully_analyzed": fully_analyzed,
        "parser_failures": parser_failures,
        "score_conflicts": score_conflicts,
        "quality_blocked": quality_blocked,
        "strong_signal_threshold": STRONG_THRESHOLD,
        "strong_signals": strong_signals,
        "top_candidates": top_candidates[:5],
        "estimated_api_calls_this_scan": api_estimate,
        "note": (
            "The scanner uses the exact scores object from the same fresh "
            "Zyla live-list response for all candidates. Repeat filtering is "
            "applied only after the scan is complete."
        ),
    }


# ============================================================
# DEBUG + STATUS
# ============================================================

@mcp.tool()
async def debug_zyla_match(match_id: str):
    """
    Small diagnostic for one match. No API key is exposed.
    """
    live, details, stats = await asyncio.gather(
        zyla_live(fresh=True),
        zyla_details(match_id),
        zyla_stats(match_id),
    )

    candidates = extract_live_candidates(live)
    candidate = find_live_candidate(candidates, match_id)

    normalized = normalize_match(
        candidate,
        details,
        stats,
    )

    return {
        "version": VERSION,
        "match_id": match_id,
        "live_candidate": candidate,
        "details": parse_details(details),
        "parser_ok": normalized["parser_ok"],
        "data_quality": normalized["data_quality"],
        "availability": normalized["availability"],
        "score_sync": normalized["score_sync"],
        "normalized": {
            "minute": normalized["minute"],
            "score": normalized["score"],
            "xg": normalized["xg"],
            "shots": normalized["shots"],
            "shots_on_target": normalized["shots_on_target"],
            "touches_in_box": normalized["touches_in_box"],
            "corners": normalized["corners"],
            "red_cards": normalized["red_cards"],
        },
        "http": {
            "live": http_status(live),
            "details": http_status(details),
            "stats": http_status(stats),
        },
    }


@mcp.tool()
async def hidden_signal_status():
    """Hidden Signal V3 health and configuration."""
    return {
        "service": "Hidden Signal Live",
        "version": VERSION,
        "provider": "Zyla FlashScore only",
        "zyla_key_loaded": bool(ZYLA_API_KEY),
        "strong_signal_threshold": STRONG_THRESHOLD,
        "medium_signal_threshold": MEDIUM_THRESHOLD,
        "scanner": {
            "default_prefilter_limit": DEFAULT_PREFILTER_LIMIT,
            "default_deep_limit": DEFAULT_DEEP_LIMIT,
            "max_prefilter_limit": MAX_PREFILTER_LIMIT,
            "max_deep_limit": MAX_DEEP_LIMIT,
        },
        "safety": {
            "exact_live_scores_parser": True,
            "fresh_live_before_direct_analysis": True,
            "same_live_snapshot_for_full_scan": True,
            "score_conflict_downgrades_enter": True,
            "parser_fail_closed": True,
            "repeat_guard_after_scan_only": True,
            "missing_stats_are_not_zero": True,
            "data_quality_guard": True,
            "strong_enter_requires_advanced_metric": True,
        },
        "model_type": "heuristic-v3.1-not-calibrated",
        "important_note": (
            "Signal percentages are heuristic ranking estimates, not "
            "calibrated statistical probabilities."
        ),
    }


# ============================================================
# RAW TOOLS — useful for manual verification
# ============================================================

@mcp.tool()
async def get_zyla_live_matches():
    """Get current Zyla football live list."""
    return await zyla_live(fresh=True)


@mcp.tool()
async def get_zyla_match_details(match_id: str):
    """Get Zyla details for one match."""
    return await zyla_details(match_id)


@mcp.tool()
async def get_zyla_match_stats(match_id: str):
    """Get Zyla statistics for one match."""
    return await zyla_stats(match_id)


@mcp.tool()
async def get_zyla_match_summary(match_id: str):
    """Get Zyla match summary/events."""
    return await zyla_summary(match_id)


@mcp.tool()
async def get_zyla_match_odds(match_id: str):
    """Get Zyla match odds."""
    return await zyla_odds(match_id)


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=port,
    )
