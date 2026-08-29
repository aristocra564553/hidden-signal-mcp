import os
import re
import math
import time
import asyncio
from typing import Any

import httpx
from mcp.server import MCPServer


# ============================================================
# HIDDEN SIGNAL LIVE V2.2 — ZYLA ONLY
# ============================================================

VERSION = "V2.2-ZYLA"

ZYLA_API_KEY = (os.environ.get("ZYLA_API_KEY") or "").strip()
ZYLA_BASE_URL = (
    "https://zylalabs.com/api/12518/"
    "flashscore+-+live+api"
)

mcp = MCPServer("Hidden Signal Live")


# ============================================================
# SETTINGS
# ============================================================

STRONG_SIGNAL_THRESHOLD = 75.0
MEDIUM_SIGNAL_THRESHOLD = 62.0

DEFAULT_PREFILTER_LIMIT = 8
DEFAULT_DEEP_LIMIT = 4
MAX_PREFILTER_LIMIT = 14
MAX_DEEP_LIMIT = 6

LIVE_CACHE_TTL = 12
STATS_CACHE_TTL = 22
DETAILS_CACHE_TTL = 25
SUMMARY_CACHE_TTL = 30
ODDS_CACHE_TTL = 22

REPEAT_SIGNAL_DELTA = 4.0

SIGNALS = {
    "goal_next_5": True,
    "goal_next_10": True,
    "goal_before_halftime": True,
    "goal_before_fulltime": True,
    "over_under": True,
    "btts": True,
    "team_goal": True,
    "match_result": True,
}

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


def normalize_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)

    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def unwrap(payload: Any):
    if isinstance(payload, dict) and "api_response" in payload:
        return payload.get("api_response")
    return payload


def first_value_by_keys(payload: Any, keys: list[str], default=None):
    wanted = {normalize_key(k) for k in keys}

    for node in walk(payload):
        for key, value in node.items():
            if normalize_key(key) in wanted and value not in (None, "", [], {}):
                return value

    return default


def parse_pair_string(value: Any) -> tuple[float, float] | None:
    if not isinstance(value, str):
        return None

    match = re.search(
        r"(-?\d+(?:[.,]\d+)?)\s*[:\-–—]\s*(-?\d+(?:[.,]\d+)?)",
        value,
    )

    if not match:
        return None

    return (
        float(match.group(1).replace(",", ".")),
        float(match.group(2).replace(",", ".")),
    )


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


def http_status(payload: Any):
    if not isinstance(payload, dict):
        return None
    return payload.get("diagnostic", {}).get("http_status")


def response_nonempty(payload: Any) -> bool:
    value = unwrap(payload)
    return value not in (None, "", [], {})


# ============================================================
# ZYLA REQUESTS
# ============================================================

async def zyla_get(
    endpoint_id: int,
    endpoint_slug: str,
    params: dict | None = None,
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
            result = dict(cached) if isinstance(cached, dict) else cached

            if isinstance(result, dict):
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


async def zyla_live(use_cache: bool = True):
    return await zyla_get(
        23856,
        "get+live+matches",
        {"sport_id": 1},
        cache_key="live" if use_cache else None,
        cache_ttl=LIVE_CACHE_TTL if use_cache else 0,
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
# RAW ZYLA TOOLS
# ============================================================

@mcp.tool()
async def get_zyla_live_matches():
    """Get all football matches currently live from Zyla."""
    return await zyla_live(use_cache=False)


@mcp.tool()
async def get_zyla_match_details(match_id: str):
    """Get Zyla match details."""
    return await zyla_details(match_id)


@mcp.tool()
async def get_zyla_match_stats(match_id: str):
    """Get Zyla live statistics including xG when available."""
    return await zyla_stats(match_id)


@mcp.tool()
async def get_zyla_match_summary(match_id: str):
    """Get goals, cards, substitutions and match events."""
    return await zyla_summary(match_id)


@mcp.tool()
async def get_zyla_match_odds(match_id: str):
    """Get Zyla live bookmaker odds."""
    return await zyla_odds(match_id)


# ============================================================
# LIVE LIST PARSER
# ============================================================

def extract_team_name(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()

    if isinstance(value, dict):
        for key in (
            "name",
            "team_name",
            "teamName",
            "participant_name",
            "participantName",
        ):
            if key in value and isinstance(value[key], str):
                return value[key].strip()

    return ""


def parse_team_names(payload: Any) -> tuple[str, str]:
    home = ""
    away = ""

    for node in walk(payload):
        for key, value in node.items():
            normalized = normalize_key(key)

            if not home and normalized in {
                "home",
                "home team",
                "home team name",
                "home name",
                "team home",
                "participant home",
            }:
                home = extract_team_name(value)

            if not away and normalized in {
                "away",
                "away team",
                "away team name",
                "away name",
                "team away",
                "participant away",
            }:
                away = extract_team_name(value)

    return home, away


def parse_score(payload: Any) -> tuple[int, int]:
    home = first_value_by_keys(
        payload,
        [
            "home_score",
            "score_home",
            "home goals",
            "homeGoals",
            "homeScore",
            "home score",
        ],
    )

    away = first_value_by_keys(
        payload,
        [
            "away_score",
            "score_away",
            "away goals",
            "awayGoals",
            "awayScore",
            "away score",
        ],
    )

    if home is not None and away is not None:
        return int(safe_float(home)), int(safe_float(away))

    for node in walk(payload):
        for key, value in node.items():
            if normalize_key(key) in {
                "score",
                "current score",
                "result",
                "current result",
                "live score",
            }:
                pair = parse_pair_string(value)

                if pair:
                    return int(pair[0]), int(pair[1])

                if isinstance(value, dict):
                    h = first_value_by_keys(
                        value,
                        ["home", "home_score", "homeScore"],
                    )
                    a = first_value_by_keys(
                        value,
                        ["away", "away_score", "awayScore"],
                    )

                    if h is not None and a is not None:
                        return int(safe_float(h)), int(safe_float(a))

    return 0, 0


def parse_minute(payload: Any) -> int:
    value = first_value_by_keys(
        payload,
        [
            "minute",
            "live_time",
            "live time",
            "time",
            "elapsed",
            "match minute",
            "match_minute",
            "stage_time",
        ],
    )

    return int(clamp(safe_float(value), 0, 130))


def parse_live_red_cards(payload: Any) -> tuple[int, int]:
    home = first_value_by_keys(
        payload,
        [
            "home_red_cards",
            "home red cards",
            "homeRedCards",
            "home reds",
        ],
    )

    away = first_value_by_keys(
        payload,
        [
            "away_red_cards",
            "away red cards",
            "awayRedCards",
            "away reds",
        ],
    )

    if home is not None or away is not None:
        return int(safe_float(home)), int(safe_float(away))

    return 0, 0


def extract_live_candidates(payload: Any) -> list[dict]:
    raw = unwrap(payload)
    results = []
    seen = set()

    for node in walk(raw):
        match_id = None

        for key in ("match_id", "matchId", "event_id", "eventId"):
            if key in node and node.get(key) not in (None, ""):
                match_id = str(node.get(key))
                break

        if not match_id or match_id in seen:
            continue

        home, away = parse_team_names(node)
        minute = parse_minute(node)
        score_home, score_away = parse_score(node)
        red_home, red_away = parse_live_red_cards(node)

        if not (
            (home and away)
            or minute > 0
            or first_value_by_keys(
                node,
                ["status", "stage", "live_time", "score"],
            ) is not None
        ):
            continue

        seen.add(match_id)

        results.append({
            "match_id": match_id,
            "home": home,
            "away": away,
            "minute": minute,
            "score_home": score_home,
            "score_away": score_away,
            "red_home": red_home,
            "red_away": red_away,
        })

    return results


def find_live_candidate(
    live_candidates: list[dict],
    match_id: str,
):
    for candidate in live_candidates:
        if str(candidate.get("match_id")) == str(match_id):
            return candidate

    return None


# ============================================================
# ROBUST ZYLA STAT PARSER V2.2
# ============================================================

LABEL_KEYS = (
    "name",
    "type",
    "title",
    "label",
    "stat",
    "stat_name",
    "statName",
    "statistic",
    "statistic_name",
    "statisticName",
    "key",
)

HOME_VALUE_KEYS = (
    "home",
    "home_team",
    "homeTeam",
    "home_value",
    "homeValue",
    "value_home",
    "valueHome",
    "home_stat",
    "homeStat",
    "participant_1",
    "participant1",
    "team1",
)

AWAY_VALUE_KEYS = (
    "away",
    "away_team",
    "awayTeam",
    "away_value",
    "awayValue",
    "value_away",
    "valueAway",
    "away_stat",
    "awayStat",
    "participant_2",
    "participant2",
    "team2",
)


def clean_stat_value(value: Any) -> float:
    if value is None:
        return 0.0

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        text = value.strip()

        if not text or text in ("-", "—", "N/A", "null"):
            return 0.0

        text = text.replace("%", "").replace(",", ".")

        match = re.search(r"-?\d+(?:\.\d+)?", text)

        if match:
            try:
                return float(match.group(0))
            except Exception:
                return 0.0

    return 0.0


def label_matches(label: str, aliases: list[str]) -> bool:
    label = normalize_key(label)

    if not label:
        return False

    for alias in [normalize_key(a) for a in aliases]:
        if label == alias:
            return True
        if alias in label:
            return True
        if label in alias:
            return True

    return False


def direct_pair_from_dict(
    node: dict,
) -> tuple[float, float] | None:
    home_value = None
    away_value = None

    for key in HOME_VALUE_KEYS:
        if key in node and not isinstance(node[key], (dict, list)):
            home_value = node[key]
            break

    for key in AWAY_VALUE_KEYS:
        if key in node and not isinstance(node[key], (dict, list)):
            away_value = node[key]
            break

    if home_value is not None and away_value is not None:
        return (
            clean_stat_value(home_value),
            clean_stat_value(away_value),
        )

    for container_key in ("value", "values", "data", "stats"):
        value = node.get(container_key)

        if isinstance(value, dict):
            nested_home = None
            nested_away = None

            for key in HOME_VALUE_KEYS:
                if key in value:
                    nested_home = value[key]
                    break

            for key in AWAY_VALUE_KEYS:
                if key in value:
                    nested_away = value[key]
                    break

            if nested_home is not None and nested_away is not None:
                return (
                    clean_stat_value(nested_home),
                    clean_stat_value(nested_away),
                )

        if isinstance(value, list) and len(value) >= 2:
            if (
                not isinstance(value[0], (dict, list))
                and not isinstance(value[1], (dict, list))
            ):
                return (
                    clean_stat_value(value[0]),
                    clean_stat_value(value[1]),
                )

        if isinstance(value, str):
            pair = parse_pair_string(value)
            if pair:
                return pair

    return None


def find_metric_pair(
    payload: Any,
    aliases: list[str],
) -> tuple[float, float] | None:
    # Format 1: {"name": "...", "home_team": "...", "away_team": "..."}
    for node in walk(payload):
        if not isinstance(node, dict):
            continue

        label = None

        for key in LABEL_KEYS:
            if key in node and isinstance(node.get(key), str):
                label = node.get(key)
                break

        if label and label_matches(label, aliases):
            pair = direct_pair_from_dict(node)
            if pair is not None:
                return pair

    # Format 2: {"Total shots": {"home_team": 6, "away_team": 9}}
    for node in walk(payload):
        if not isinstance(node, dict):
            continue

        for key, value in node.items():
            if not label_matches(str(key), aliases):
                continue

            if isinstance(value, dict):
                pair = direct_pair_from_dict(value)
                if pair is not None:
                    return pair

            if isinstance(value, list) and len(value) >= 2:
                if (
                    not isinstance(value[0], (dict, list))
                    and not isinstance(value[1], (dict, list))
                ):
                    return (
                        clean_stat_value(value[0]),
                        clean_stat_value(value[1]),
                    )

            if isinstance(value, str):
                pair = parse_pair_string(value)
                if pair:
                    return pair

    return None


def metric(
    payload: Any,
    aliases: list[str],
) -> tuple[float, float]:
    pair = find_metric_pair(payload, aliases)
    return pair if pair is not None else (0.0, 0.0)


def parse_red_cards(
    stats_raw: Any,
    summary_raw: Any,
) -> tuple[int, int]:
    pair = find_metric_pair(
        stats_raw,
        ["red cards", "red card", "reds"],
    )

    if pair:
        return int(pair[0]), int(pair[1])

    pair = find_metric_pair(
        summary_raw,
        ["red cards", "red card", "reds"],
    )

    if pair:
        return int(pair[0]), int(pair[1])

    return 0, 0


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_match(
    live_candidate: dict | None,
    details_payload: Any,
    stats_payload: Any,
    summary_payload: Any,
    odds_payload: Any,
) -> dict:
    live_candidate = live_candidate or {}

    details_raw = unwrap(details_payload)
    stats_raw = unwrap(stats_payload)
    summary_raw = unwrap(summary_payload)
    odds_raw = unwrap(odds_payload)

    combined = {
        "details": details_raw,
        "stats": stats_raw,
        "summary": summary_raw,
    }

    details_home, details_away = parse_team_names(combined)

    home = live_candidate.get("home") or details_home
    away = live_candidate.get("away") or details_away

    # Live list is authoritative for current score/minute when available.
    live_score = (
        int(live_candidate.get("score_home", 0)),
        int(live_candidate.get("score_away", 0)),
    )

    details_score = parse_score(combined)

    if live_candidate:
        score_home, score_away = live_score
    else:
        score_home, score_away = details_score

    minute = (
        int(live_candidate.get("minute", 0))
        if live_candidate
        else 0
    ) or parse_minute(combined)

    xg = metric(
        stats_raw,
        ["xg", "expected goals", "expected goals xg"],
    )

    shots = metric(
        stats_raw,
        ["shots", "total shots", "shots total", "total attempts"],
    )

    shots_on_target = metric(
        stats_raw,
        ["shots on target", "shots on goal", "on target"],
    )

    shots_in_box = metric(
        stats_raw,
        [
            "shots inside box",
            "shots inside the box",
            "shots in box",
            "inside box",
        ],
    )

    touches_in_box = metric(
        stats_raw,
        [
            "touches in opposition box",
            "touches in opponent box",
            "touches in box",
            "box touches",
        ],
    )

    corners = metric(
        stats_raw,
        ["corners", "corner kicks"],
    )

    possession = metric(
        stats_raw,
        ["possession", "ball possession"],
    )

    xa = metric(
        stats_raw,
        ["xa", "expected assists"],
    )

    fouls = metric(
        stats_raw,
        ["fouls", "fouls committed"],
    )

    red_home, red_away = parse_red_cards(
        stats_raw,
        summary_raw,
    )

    if red_home == 0 and red_away == 0 and live_candidate:
        red_home = int(live_candidate.get("red_home", 0))
        red_away = int(live_candidate.get("red_away", 0))

    core_total = (
        xg[0] + xg[1]
        + shots[0] + shots[1]
        + shots_on_target[0] + shots_on_target[1]
        + touches_in_box[0] + touches_in_box[1]
        + corners[0] + corners[1]
    )

    stats_have_data = response_nonempty(stats_payload)

    parser_ok = not (
        stats_have_data
        and core_total == 0
    )

    score_conflict = (
        bool(live_candidate)
        and details_score != live_score
    )

    return {
        "home": home,
        "away": away,
        "minute": minute,
        "score": {
            "home": score_home,
            "away": score_away,
            "total": score_home + score_away,
        },
        "score_sync": {
            "ok": not score_conflict,
            "conflict": score_conflict,
            "live_score": {
                "home": live_score[0],
                "away": live_score[1],
            } if live_candidate else None,
            "details_score": {
                "home": details_score[0],
                "away": details_score[1],
            },
        },
        "xg": {
            "home": xg[0],
            "away": xg[1],
            "total": xg[0] + xg[1],
        },
        "shots": {
            "home": shots[0],
            "away": shots[1],
            "total": shots[0] + shots[1],
        },
        "shots_on_target": {
            "home": shots_on_target[0],
            "away": shots_on_target[1],
            "total": shots_on_target[0] + shots_on_target[1],
        },
        "shots_in_box": {
            "home": shots_in_box[0],
            "away": shots_in_box[1],
            "total": shots_in_box[0] + shots_in_box[1],
        },
        "touches_in_box": {
            "home": touches_in_box[0],
            "away": touches_in_box[1],
            "total": touches_in_box[0] + touches_in_box[1],
        },
        "corners": {
            "home": corners[0],
            "away": corners[1],
            "total": corners[0] + corners[1],
        },
        "possession": {
            "home": possession[0],
            "away": possession[1],
        },
        "xa": {
            "home": xa[0],
            "away": xa[1],
            "total": xa[0] + xa[1],
        },
        "fouls": {
            "home": fouls[0],
            "away": fouls[1],
            "total": fouls[0] + fouls[1],
        },
        "red_cards": {
            "home": red_home,
            "away": red_away,
        },
        "events_available": response_nonempty(summary_payload),
        "odds_available": response_nonempty(odds_payload),
        "parser_ok": parser_ok,
        "parser_warning": (
            None
            if parser_ok
            else "Zyla stats are non-empty but normalized core metrics are all zero"
        ),
    }


# ============================================================
# ODDS SNAPSHOT
# ============================================================

def parse_odds_snapshot(payload: Any) -> dict:
    raw = unwrap(payload)

    bookmakers = set()
    markets = []

    for node in walk(raw):
        name = first_value_by_keys(
            node,
            ["bookmaker", "bookmaker_name", "name"],
        )

        if isinstance(name, str) and len(name) <= 80:
            lower = name.lower()

            if any(
                token in lower
                for token in (
                    "bet",
                    "1xbet",
                    "unibet",
                    "betfair",
                    "365",
                    "pinnacle",
                )
            ):
                bookmakers.add(name.strip())

        market = first_value_by_keys(
            node,
            [
                "betting_type",
                "betting type",
                "market",
                "market_name",
                "market name",
            ],
        )

        if isinstance(market, str):
            markets.append(market.strip())

    return {
        "bookmakers_count": len(bookmakers),
        "bookmakers": sorted(bookmakers)[:8],
        "markets_sample": list(dict.fromkeys(markets))[:8],
    }


# ============================================================
# HEURISTIC MODEL
# ============================================================

def build_pressure(metrics: dict) -> dict:
    minute = max(metrics["minute"], 1)

    xg_rate = metrics["xg"]["total"] * 90 / minute
    shots_rate = metrics["shots"]["total"] * 90 / minute
    sot_rate = metrics["shots_on_target"]["total"] * 90 / minute
    box_rate = metrics["shots_in_box"]["total"] * 90 / minute
    touches_rate = metrics["touches_in_box"]["total"] * 90 / minute
    corner_rate = metrics["corners"]["total"] * 90 / minute

    score = (
        clamp(xg_rate / 3.0, 0, 1) * 0.30
        + clamp(shots_rate / 24.0, 0, 1) * 0.13
        + clamp(sot_rate / 9.0, 0, 1) * 0.20
        + clamp(box_rate / 15.0, 0, 1) * 0.13
        + clamp(touches_rate / 45.0, 0, 1) * 0.14
        + clamp(corner_rate / 11.0, 0, 1) * 0.10
    )

    return {
        "score_0_100": round(score * 100, 1),
        "xg_rate_90": round(xg_rate, 2),
        "shots_rate_90": round(shots_rate, 2),
        "sot_rate_90": round(sot_rate, 2),
        "box_shots_rate_90": round(box_rate, 2),
        "touches_box_rate_90": round(touches_rate, 2),
        "corners_rate_90": round(corner_rate, 2),
    }


def team_attack_strength(metrics: dict, side: str) -> float:
    minute = max(metrics["minute"], 1)

    xg = metrics["xg"][side] * 90 / minute
    shots = metrics["shots"][side] * 90 / minute
    sot = metrics["shots_on_target"][side] * 90 / minute
    box = metrics["shots_in_box"][side] * 90 / minute
    touches = metrics["touches_in_box"][side] * 90 / minute
    corners = metrics["corners"][side] * 90 / minute

    score = (
        clamp(xg / 1.8, 0, 1) * 0.34
        + clamp(shots / 14.0, 0, 1) * 0.12
        + clamp(sot / 5.0, 0, 1) * 0.22
        + clamp(box / 9.0, 0, 1) * 0.12
        + clamp(touches / 28.0, 0, 1) * 0.12
        + clamp(corners / 6.0, 0, 1) * 0.08
    )

    reds = metrics["red_cards"]

    if side == "home":
        score += 0.08 * max(0, reds["away"] - reds["home"])
        score -= 0.10 * max(0, reds["home"] - reds["away"])
    else:
        score += 0.08 * max(0, reds["home"] - reds["away"])
        score -= 0.10 * max(0, reds["away"] - reds["home"])

    return clamp(score, 0, 1)


def logistic(value: float) -> float:
    return 1 / (1 + math.exp(-clamp(value, -20, 20)))


def probability_goal_next_5(metrics: dict, pressure: dict) -> float:
    pressure_factor = pressure["score_0_100"] / 100

    logit = -2.05 + 2.35 * pressure_factor

    minute = metrics["minute"]

    if 65 <= minute <= 88:
        logit += 0.18

    if metrics["score"]["total"] == 0 and minute < 25:
        logit -= 0.10

    red_diff = abs(
        metrics["red_cards"]["home"]
        - metrics["red_cards"]["away"]
    )

    logit += 0.20 * min(red_diff, 1)

    return clamp(logistic(logit) * 100, 5, 92)


def probability_from_interval(
    p5: float,
    minutes: float,
) -> float:
    p5 = clamp(p5 / 100, 0.001, 0.95)
    intervals = max(minutes / 5, 0)

    return clamp(
        (1 - ((1 - p5) ** intervals)) * 100,
        1,
        98,
    )


def classify(probability: float) -> tuple[str, str]:
    if probability >= STRONG_SIGNAL_THRESHOLD:
        return "🟢", "ENTER"

    if probability >= MEDIUM_SIGNAL_THRESHOLD:
        return "🟡", "WAIT"

    return "🔴", "SKIP"


def make_signal(
    market: str,
    selection: str,
    probability: float,
    reasons: list[str],
    risk: str = "medium",
):
    color, decision = classify(probability)

    return {
        "market": market,
        "selection": selection,
        "probability": round(probability, 1),
        "signal": color,
        "decision": decision,
        "risk": risk,
        "reasons": reasons[:5],
    }


def result_probabilities(metrics: dict) -> dict:
    home_attack = team_attack_strength(metrics, "home")
    away_attack = team_attack_strength(metrics, "away")

    score_diff = (
        metrics["score"]["home"]
        - metrics["score"]["away"]
    )

    time_factor = clamp(metrics["minute"] / 90, 0, 1)

    home_raw = math.exp(
        clamp(
            0.20
            + 1.7 * (home_attack - away_attack)
            + 1.25 * score_diff * time_factor,
            -6,
            6,
        )
    )

    away_raw = math.exp(
        clamp(
            1.7 * (away_attack - home_attack)
            - 1.25 * score_diff * time_factor,
            -6,
            6,
        )
    )

    draw_raw = 1.15

    if score_diff == 0:
        draw_raw += 0.75 * time_factor
    else:
        draw_raw -= 0.35 * time_factor

    draw_raw = max(0.15, draw_raw)

    total = home_raw + draw_raw + away_raw

    return {
        "home": home_raw / total * 100,
        "draw": draw_raw / total * 100,
        "away": away_raw / total * 100,
    }


def build_signals(metrics: dict) -> list[dict]:
    # Fail closed if parser is bad.
    if not metrics["parser_ok"]:
        return []

    minute = metrics["minute"]
    total_goals = metrics["score"]["total"]

    pressure = build_pressure(metrics)

    home_attack = team_attack_strength(metrics, "home")
    away_attack = team_attack_strength(metrics, "away")

    p5 = probability_goal_next_5(metrics, pressure)
    p10 = probability_from_interval(p5, 10)

    common = [
        f"pressure {pressure['score_0_100']}/100",
        f"xG {metrics['xg']['home']:.2f}-{metrics['xg']['away']:.2f}",
        f"shots {metrics['shots']['home']:.0f}-{metrics['shots']['away']:.0f}",
        f"SOT {metrics['shots_on_target']['home']:.0f}-{metrics['shots_on_target']['away']:.0f}",
        f"box touches {metrics['touches_in_box']['home']:.0f}-{metrics['touches_in_box']['away']:.0f}",
    ]

    signals = []

    if SIGNALS["goal_next_5"]:
        signals.append(
            make_signal(
                "GOAL_NEXT_5",
                "Goal in next 5 minutes",
                p5,
                common,
                "high" if p5 < STRONG_SIGNAL_THRESHOLD else "medium",
            )
        )

    if SIGNALS["goal_next_10"]:
        signals.append(
            make_signal(
                "GOAL_NEXT_10",
                "Goal in next 10 minutes",
                p10,
                common,
            )
        )

    if SIGNALS["goal_before_halftime"] and 1 <= minute < 45:
        remaining = max(1, 45 - minute)

        signals.append(
            make_signal(
                "GOAL_BEFORE_HALFTIME",
                "Goal before halftime",
                probability_from_interval(p5, remaining),
                common + [f"{remaining} min to HT"],
            )
        )

    if SIGNALS["goal_before_fulltime"] and 1 <= minute < 90:
        remaining = max(1, 90 - minute)

        signals.append(
            make_signal(
                "GOAL_BEFORE_FULLTIME",
                "At least one more goal",
                probability_from_interval(p5, remaining),
                common + [f"{remaining} min to FT"],
            )
        )

    # This is an "another goal" line: current total + 0.5.
    if SIGNALS["over_under"] and minute < 90:
        line = total_goals + 0.5
        p_over = probability_from_interval(
            p5,
            max(1, 90 - minute),
        )
        p_under = 100 - p_over

        signals.append(
            make_signal(
                "OVER_UNDER",
                f"Over {line:.1f}",
                p_over,
                common,
            )
        )

        signals.append(
            make_signal(
                "OVER_UNDER",
                f"Under {line:.1f}",
                p_under,
                [
                    f"current total {total_goals}",
                    f"minute {minute}",
                    f"another-goal estimate {p_over:.1f}%",
                ],
            )
        )

    if SIGNALS["team_goal"] and minute < 90:
        remaining = max(1, 90 - minute)
        p_any = probability_from_interval(p5, remaining)

        total_attack = max(home_attack + away_attack, 0.05)

        home_share = home_attack / total_attack
        away_share = away_attack / total_attack

        home_p = clamp(
            p_any * (0.35 + 0.85 * home_share),
            1,
            96,
        )

        away_p = clamp(
            p_any * (0.35 + 0.85 * away_share),
            1,
            96,
        )

        signals.append(
            make_signal(
                "TEAM_GOAL",
                f"{metrics['home'] or 'Home'} to score",
                home_p,
                [
                    f"home attack {home_attack * 100:.0f}/100",
                    f"home xG {metrics['xg']['home']:.2f}",
                    f"home shots {metrics['shots']['home']:.0f}",
                    f"home box touches {metrics['touches_in_box']['home']:.0f}",
                ],
            )
        )

        signals.append(
            make_signal(
                "TEAM_GOAL",
                f"{metrics['away'] or 'Away'} to score",
                away_p,
                [
                    f"away attack {away_attack * 100:.0f}/100",
                    f"away xG {metrics['xg']['away']:.2f}",
                    f"away shots {metrics['shots']['away']:.0f}",
                    f"away box touches {metrics['touches_in_box']['away']:.0f}",
                ],
            )
        )

    if SIGNALS["btts"]:
        home_scored = metrics["score"]["home"] > 0
        away_scored = metrics["score"]["away"] > 0

        # Do not emit a settled BTTS market as a new signal.
        if not (home_scored and away_scored):
            remaining = max(1, 90 - minute)
            p_any = probability_from_interval(p5, remaining)

            if home_scored:
                p_btts = clamp(
                    p_any * (0.50 + away_attack),
                    1,
                    95,
                )
            elif away_scored:
                p_btts = clamp(
                    p_any * (0.50 + home_attack),
                    1,
                    95,
                )
            else:
                p_btts = clamp(
                    p_any
                    * min(home_attack, away_attack)
                    * 1.15,
                    1,
                    88,
                )

            signals.append(
                make_signal(
                    "BTTS",
                    "Both teams to score - YES",
                    p_btts,
                    [
                        f"score {metrics['score']['home']}-{metrics['score']['away']}",
                        f"home attack {home_attack * 100:.0f}/100",
                        f"away attack {away_attack * 100:.0f}/100",
                        f"xG {metrics['xg']['home']:.2f}-{metrics['xg']['away']:.2f}",
                    ],
                )
            )

    if SIGNALS["match_result"]:
        rp = result_probabilities(metrics)

        signals.append(
            make_signal(
                "MATCH_RESULT",
                f"{metrics['home'] or 'Home'} win",
                rp["home"],
                [
                    f"score {metrics['score']['home']}-{metrics['score']['away']}",
                    f"home attack {home_attack * 100:.0f}/100",
                    f"away attack {away_attack * 100:.0f}/100",
                ],
            )
        )

        signals.append(
            make_signal(
                "MATCH_RESULT",
                "Draw",
                rp["draw"],
                [
                    f"score {metrics['score']['home']}-{metrics['score']['away']}",
                    f"minute {minute}",
                ],
            )
        )

        signals.append(
            make_signal(
                "MATCH_RESULT",
                f"{metrics['away'] or 'Away'} win",
                rp["away"],
                [
                    f"score {metrics['score']['home']}-{metrics['score']['away']}",
                    f"home attack {home_attack * 100:.0f}/100",
                    f"away attack {away_attack * 100:.0f}/100",
                ],
            )
        )

    signals.sort(
        key=lambda item: item["probability"],
        reverse=True,
    )

    return signals


def apply_repeat_guard(
    match_id: str,
    signals: list[dict],
):
    state = _LAST_SIGNAL_STATE.setdefault(match_id, {})
    output = []

    for item in signals:
        key = f"{item['market']}::{item['selection']}"
        probability = float(item["probability"])
        old = state.get(key)

        changed = (
            old is None
            or abs(probability - old) >= REPEAT_SIGNAL_DELTA
        )

        item = dict(item)
        item["new_or_changed"] = changed

        if item["decision"] == "ENTER":
            state[key] = probability

        output.append(item)

    return output


def downgrade_for_score_conflict(
    signals: list[dict],
) -> list[dict]:
    guarded = []

    for signal in signals:
        signal = dict(signal)

        if signal.get("decision") == "ENTER":
            signal["decision"] = "WAIT"
            signal["signal"] = "🟡"
            signal["risk"] = "high"

            reasons = list(signal.get("reasons", []))
            reasons.append(
                "score conflict between fresh live list and match details"
            )
            signal["reasons"] = reasons[:5]

        guarded.append(signal)

    return guarded


# ============================================================
# MATCH ANALYSIS
# ============================================================

async def analyze_match_internal(
    match_id: str,
    live_candidate: dict | None = None,
    refresh_live: bool = True,
    include_details: bool = True,
    include_summary: bool = True,
    include_odds: bool = True,
):
    fresh_live = None

    # Direct single-match analysis must refresh score from live list.
    if refresh_live:
        fresh_live = await zyla_live(use_cache=False)
        candidates = extract_live_candidates(fresh_live)

        fresh_candidate = find_live_candidate(
            candidates,
            match_id,
        )

        if fresh_candidate is not None:
            live_candidate = fresh_candidate

    elif live_candidate is None:
        cached_live = await zyla_live(use_cache=True)
        candidates = extract_live_candidates(cached_live)
        live_candidate = find_live_candidate(candidates, match_id)

    tasks = [
        zyla_stats(match_id),
    ]
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

    stats = mapped.get("stats", {})
    details = mapped.get("details", {})
    summary = mapped.get("summary", {})
    odds = mapped.get("odds", {})

    normalized = normalize_match(
        live_candidate,
        details,
        stats,
        summary,
        odds,
    )

    pressure = (
        build_pressure(normalized)
        if normalized["parser_ok"]
        else None
    )

    signals = apply_repeat_guard(
        match_id,
        build_signals(normalized),
    )

    if normalized["score_sync"]["conflict"]:
        signals = downgrade_for_score_conflict(signals)

    strong = [
        item
        for item in signals
        if item["decision"] == "ENTER"
        and item.get("new_or_changed", True)
    ]

    return {
        "source": "hidden-signal-v2.2",
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
        "pressure": pressure,
        "odds_snapshot": parse_odds_snapshot(odds),
        "top_candidate": signals[0] if signals else None,
        "top_3_signals": signals[:3],
        "strong_signals": strong,
        "all_signals": signals,
        "diagnostic": {
            "fresh_live_http": http_status(fresh_live) if fresh_live else None,
            "stats_http": http_status(stats),
            "details_http": http_status(details) if details else None,
            "summary_http": http_status(summary) if summary else None,
            "odds_http": http_status(odds) if odds else None,
        },
    }


@mcp.tool()
async def analyze_zyla_match(match_id: str):
    """
    Full Hidden Signal analysis of one Zyla live match.

    Always refreshes the live score first, normalizes statistics,
    checks score synchronization, and returns Top-3 signals.
    """
    return await analyze_match_internal(
        match_id,
        refresh_live=True,
        include_details=True,
        include_summary=True,
        include_odds=True,
    )


# ============================================================
# SCANNER
# ============================================================

def live_prefilter_score(candidate: dict) -> float:
    minute = candidate.get("minute", 0)

    total_goals = (
        candidate.get("score_home", 0)
        + candidate.get("score_away", 0)
    )

    score = 0.0

    if 8 <= minute <= 88:
        score += 40

    if 30 <= minute < 45:
        score += 18
    elif 55 <= minute <= 85:
        score += 22

    if total_goals <= 4:
        score += 12

    if candidate.get("home") and candidate.get("away"):
        score += 10

    if (
        candidate.get("red_home", 0)
        != candidate.get("red_away", 0)
    ):
        score += 8

    return score


@mcp.tool()
async def scan_zyla_live(
    prefilter_limit: int = DEFAULT_PREFILTER_LIMIT,
    deep_limit: int = DEFAULT_DEEP_LIMIT,
):
    """
    Efficient Zyla-only live scanner.

    1) One fresh live-list request.
    2) Stats for prefiltered matches.
    3) Details + events + odds only for best deep candidates.
    4) Compact output with Top-5 and all strong 75%+ signals.
    """

    prefilter_limit = int(
        clamp(
            prefilter_limit,
            1,
            MAX_PREFILTER_LIMIT,
        )
    )

    deep_limit = int(
        clamp(
            deep_limit,
            1,
            MAX_DEEP_LIMIT,
        )
    )

    live = await zyla_live(use_cache=False)
    live_http = http_status(live)

    if live_http != 200:
        return {
            "source": "hidden-signal-v2.2",
            "version": VERSION,
            "status": "ZYLA_ERROR",
            "live_http_status": live_http,
            "live_matches_found": 0,
            "prefiltered_matches": 0,
            "cheap_analyzed": 0,
            "fully_analyzed": 0,
            "parser_failures": 0,
            "score_conflicts": 0,
            "strong_signals": [],
            "top_candidates": [],
            "estimated_api_calls_this_scan": 1,
            "error": unwrap(live),
        }

    candidates = extract_live_candidates(live)
    total_live = len(candidates)

    if not candidates:
        return {
            "source": "hidden-signal-v2.2",
            "version": VERSION,
            "status": "NO_LIVE_MATCHES_OR_PARSE_FAILED",
            "live_http_status": 200,
            "live_matches_found": 0,
            "prefiltered_matches": 0,
            "cheap_analyzed": 0,
            "fully_analyzed": 0,
            "parser_failures": 0,
            "score_conflicts": 0,
            "strong_signals": [],
            "top_candidates": [],
            "estimated_api_calls_this_scan": 1,
        }

    candidates.sort(
        key=live_prefilter_score,
        reverse=True,
    )

    selected = candidates[:prefilter_limit]

    semaphore = asyncio.Semaphore(3)

    async def cheap(candidate):
        async with semaphore:
            try:
                return await analyze_match_internal(
                    candidate["match_id"],
                    live_candidate=candidate,
                    refresh_live=False,
                    include_details=False,
                    include_summary=False,
                    include_odds=False,
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

    valid = [
        item
        for item in cheap_results
        if isinstance(item, dict)
        and not item.get("error")
        and item.get("parser_ok") is True
        and item.get("top_candidate")
    ]

    valid.sort(
        key=lambda item: (
            item.get("top_candidate", {})
            .get("probability", 0)
        ),
        reverse=True,
    )

    deep_seed = valid[:deep_limit]

    async def deep(item):
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
    strong_signals = []

    for analysis in final_results:
        match = analysis.get("match", {})
        top = analysis.get("top_candidate")

        if top:
            top_candidates.append({
                "match_id": analysis["match_id"],
                "home": match.get("home"),
                "away": match.get("away"),
                "minute": match.get("minute"),
                "score": match.get("score"),
                "score_sync_ok": (
                    analysis.get("score_sync", {})
                    .get("ok")
                ),
                "market": top.get("market"),
                "selection": top.get("selection"),
                "probability": top.get("probability"),
                "decision": top.get("decision"),
                "reasons": top.get("reasons"),
            })

        for signal in analysis.get("strong_signals", []):
            strong_signals.append({
                "match_id": analysis["match_id"],
                "home": match.get("home"),
                "away": match.get("away"),
                "minute": match.get("minute"),
                "score": match.get("score"),
                "score_sync_ok": (
                    analysis.get("score_sync", {})
                    .get("ok")
                ),
                "market": signal.get("market"),
                "selection": signal.get("selection"),
                "probability": signal.get("probability"),
                "risk": signal.get("risk"),
                "decision": signal.get("decision"),
                "reasons": signal.get("reasons"),
            })

    top_candidates.sort(
        key=lambda item: item.get("probability", 0),
        reverse=True,
    )

    strong_signals.sort(
        key=lambda item: item.get("probability", 0),
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

    # 1 live list + one stats call per selected match
    # + details/summary/odds for fully analyzed deep matches.
    api_estimate = (
        1
        + len(selected)
        + fully_analyzed * 3
    )

    return {
        "source": "hidden-signal-v2.2",
        "version": VERSION,
        "status": "OK",
        "live_http_status": 200,
        "live_matches_found": total_live,
        "prefiltered_matches": len(selected),
        "cheap_analyzed": len(valid),
        "fully_analyzed": fully_analyzed,
        "parser_failures": parser_failures,
        "score_conflicts": score_conflicts,
        "strong_signal_threshold": STRONG_SIGNAL_THRESHOLD,
        "strong_signals": strong_signals,
        "top_candidates": top_candidates[:5],
        "estimated_api_calls_this_scan": api_estimate,
        "note": (
            "Score conflicts are automatically downgraded to WAIT. "
            "Parser failures are excluded from signals."
        ),
    }


# ============================================================
# DEBUG
# ============================================================

@mcp.tool()
async def debug_zyla_parser(match_id: str):
    """
    Debug parser and score synchronization for one match.
    Does not expose the API key.
    """

    live = await zyla_live(use_cache=False)
    candidates = extract_live_candidates(live)
    candidate = find_live_candidate(candidates, match_id)

    stats, details = await asyncio.gather(
        zyla_stats(match_id),
        zyla_details(match_id),
    )

    normalized = normalize_match(
        candidate,
        details,
        stats,
        {},
        {},
    )

    raw_stats = unwrap(stats)
    samples = []

    for node in walk(raw_stats):
        if len(samples) >= 12:
            break

        label = None

        for key in LABEL_KEYS:
            if key in node and isinstance(node.get(key), str):
                label = node.get(key)
                break

        if label:
            samples.append({
                "label": label,
                "keys": list(node.keys())[:12],
            })

    return {
        "version": VERSION,
        "match_id": match_id,
        "live_candidate": candidate,
        "parser_ok": normalized["parser_ok"],
        "parser_warning": normalized["parser_warning"],
        "score_sync": normalized["score_sync"],
        "normalized": {
            "score": normalized["score"],
            "minute": normalized["minute"],
            "xg": normalized["xg"],
            "shots": normalized["shots"],
            "shots_on_target": normalized["shots_on_target"],
            "shots_in_box": normalized["shots_in_box"],
            "touches_in_box": normalized["touches_in_box"],
            "corners": normalized["corners"],
            "possession": normalized["possession"],
            "red_cards": normalized["red_cards"],
        },
        "stats_http": http_status(stats),
        "details_http": http_status(details),
        "raw_structure_samples": samples,
    }


# ============================================================
# STATUS
# ============================================================

@mcp.tool()
async def hidden_signal_status():
    """Check Hidden Signal V2.2 status and configuration."""

    return {
        "service": "Hidden Signal Live",
        "version": VERSION,
        "provider": "Zyla FlashScore only",
        "zyla_key_loaded": bool(ZYLA_API_KEY),
        "strong_signal_threshold": STRONG_SIGNAL_THRESHOLD,
        "medium_signal_threshold": MEDIUM_SIGNAL_THRESHOLD,
        "signals_enabled": SIGNALS,
        "scanner": {
            "default_prefilter_limit": DEFAULT_PREFILTER_LIMIT,
            "default_deep_limit": DEFAULT_DEEP_LIMIT,
            "max_prefilter_limit": MAX_PREFILTER_LIMIT,
            "max_deep_limit": MAX_DEEP_LIMIT,
        },
        "cache_ttl_seconds": {
            "live": LIVE_CACHE_TTL,
            "stats": STATS_CACHE_TTL,
            "details": DETAILS_CACHE_TTL,
            "summary": SUMMARY_CACHE_TTL,
            "odds": ODDS_CACHE_TTL,
        },
        "safety": {
            "parser_fail_closed": True,
            "score_conflict_blocks_enter": True,
            "fresh_score_before_direct_analysis": True,
        },
        "model_type": "heuristic-v2.2-not-calibrated",
        "note": (
            "Signal percentages are heuristic ranking estimates. "
            "They should be calibrated on real historical outcomes "
            "before being treated as statistical probabilities."
        ),
    }


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
