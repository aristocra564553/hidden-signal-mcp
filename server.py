import os
import math
import time
import asyncio
import httpx
from typing import Any
from mcp.server import MCPServer


# ============================================================
# HIDDEN SIGNAL LIVE — ZYLA + API-FOOTBALL
# ============================================================

API_KEY = (os.environ.get("API_FOOTBALL_KEY") or "").strip()
ZYLA_API_KEY = (os.environ.get("ZYLA_API_KEY") or "").strip()

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

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
MAX_SCAN_MATCHES = 8

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

# Повторный сигнал не считается новым, пока его вероятность
# не изменилась хотя бы на это количество процентных пунктов.
REPEAT_SIGNAL_DELTA = 4.0

# Scanner cache (seconds)
ZYLA_CACHE_TTL = 30
_ZYLA_CACHE: dict[str, dict[str, Any]] = {}

# Простая память процесса Render. После перезапуска сервера очищается.
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
        text = value.strip().replace("%", "").replace(",", ".")
        if not text:
            return default

        # Берём первое число из строки вроде "45+2", "0.64 xG", "9 shots".
        match = re_search_number(text)
        if match is None:
            return default

        try:
            return float(match)
        except Exception:
            return default

    return default


def re_search_number(text: str):
    import re
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    return m.group(0) if m else None


def norm_key(value: Any) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("_", " ")
        .replace("-", " ")
        .replace("  ", " ")
    )


def normalize_name(value: Any) -> str:
    return str(value or "").strip()


def _dedupe(value: Any):
    """Убирает точные дубли из списков рекурсивно."""
    if isinstance(value, dict):
        return {k: _dedupe(v) for k, v in value.items()}

    if isinstance(value, list):
        result = []
        seen = set()

        for item in value:
            item = _dedupe(item)

            try:
                marker = json.dumps(item, ensure_ascii=False, sort_keys=True)
            except Exception:
                marker = repr(item)

            if marker not in seen:
                seen.add(marker)
                result.append(item)

        return result

    return value


def walk(value: Any):
    """Рекурсивно проходит по dict/list."""
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from walk(item)

    elif isinstance(value, list):
        for item in value:
            yield from walk(item)


def unwrap_api_response(payload: Any):
    if not isinstance(payload, dict):
        return payload

    if "api_response" in payload:
        return payload.get("api_response")

    if "data" in payload:
        return payload.get("data")

    return payload


def first_value_by_keys(payload: Any, keys: list[str], default=None):
    wanted = {norm_key(k) for k in keys}

    for node in walk(payload):
        for key, value in node.items():
            if norm_key(key) in wanted and value not in (None, "", [], {}):
                return value

    return default


def collect_values_by_keys(payload: Any, keys: list[str]) -> list[Any]:
    wanted = {norm_key(k) for k in keys}
    out = []

    for node in walk(payload):
        for key, value in node.items():
            if norm_key(key) in wanted:
                out.append(value)

    return out


def find_stat_pair(payload: Any, aliases: list[str]) -> tuple[float, float] | None:
    """
    Ищет статистику в наиболее распространённых форматах:
    1) {"type": "Expected Goals (xG)", "home": 0.45, "away": 0.10}
    2) {"name": "Shots", "home": "9", "away": "5"}
    3) {"stat": "Corners", "home": 4, "away": 1}
    4) {"key": "xG", "value": {"home": 0.45, "away": 0.10}}
    """
    wanted = {norm_key(a) for a in aliases}
    label_keys = ("type", "name", "stat", "title", "label", "key")

    for node in walk(payload):
        label = None

        for lk in label_keys:
            if lk in node:
                label = norm_key(node.get(lk))
                break

        if label and (
            label in wanted
            or any(alias in label for alias in wanted)
            or any(label in alias for alias in wanted)
        ):
            home = None
            away = None

            for hk in ("home", "home_value", "value_home", "homeValue"):
                if hk in node:
                    home = node.get(hk)
                    break

            for ak in ("away", "away_value", "value_away", "awayValue"):
                if ak in node:
                    away = node.get(ak)
                    break

            if home is not None and away is not None:
                return safe_float(home), safe_float(away)

            value = node.get("value")
            if isinstance(value, dict):
                h = first_value_by_keys(value, ["home", "home_value", "homeValue"])
                a = first_value_by_keys(value, ["away", "away_value", "awayValue"])
                if h is not None and a is not None:
                    return safe_float(h), safe_float(a)

    return None


def find_team_names(payload: Any) -> tuple[str, str]:
    home = ""
    away = ""

    home_keys = [
        "home_name", "home team", "home_team_name", "homeTeamName",
        "home", "team_home"
    ]
    away_keys = [
        "away_name", "away team", "away_team_name", "awayTeamName",
        "away", "team_away"
    ]

    # Сначала ищем очевидные строковые поля.
    for node in walk(payload):
        for key, value in node.items():
            nk = norm_key(key)

            if not home and nk in {norm_key(k) for k in home_keys}:
                if isinstance(value, str):
                    home = normalize_name(value)
                elif isinstance(value, dict):
                    name = first_value_by_keys(value, ["name", "team_name", "team"])
                    if isinstance(name, str):
                        home = normalize_name(name)

            if not away and nk in {norm_key(k) for k in away_keys}:
                if isinstance(value, str):
                    away = normalize_name(value)
                elif isinstance(value, dict):
                    name = first_value_by_keys(value, ["name", "team_name", "team"])
                    if isinstance(name, str):
                        away = normalize_name(name)

    return home, away


def parse_score(payload: Any) -> tuple[int, int]:
    pair = find_stat_pair(payload, ["score", "current score"])
    if pair:
        return int(pair[0]), int(pair[1])

    # Распространённые поля.
    home = first_value_by_keys(
        payload,
        ["home_score", "score_home", "home goals", "homeGoals"],
    )
    away = first_value_by_keys(
        payload,
        ["away_score", "score_away", "away goals", "awayGoals"],
    )

    if home is not None and away is not None:
        return int(safe_float(home)), int(safe_float(away))

    # Строка "1:0" / "1-0"
    score_text = first_value_by_keys(
        payload,
        ["score", "current_score", "current score"],
    )

    if isinstance(score_text, str):
        import re
        m = re.search(r"(\d+)\s*[:\-]\s*(\d+)", score_text)
        if m:
            return int(m.group(1)), int(m.group(2))

    return 0, 0


def parse_minute(payload: Any) -> int:
    minute = first_value_by_keys(
        payload,
        [
            "minute", "live_time", "live time", "time",
            "match_minute", "match minute", "elapsed"
        ],
    )

    if minute is None:
        return 0

    text = str(minute)
    num = safe_float(text, 0.0)
    return int(clamp(num, 0, 130))


def parse_red_cards(payload: Any) -> tuple[int, int]:
    pair = find_stat_pair(payload, ["red cards", "red card", "reds"])
    if pair:
        return int(pair[0]), int(pair[1])

    home = first_value_by_keys(payload, ["home_red_cards", "home reds"])
    away = first_value_by_keys(payload, ["away_red_cards", "away reds"])

    return int(safe_float(home)), int(safe_float(away))


# ============================================================
# HTTP REQUESTS
# ============================================================

async def api_get(endpoint: str, params: dict | None = None):
    if not API_KEY:
        return {
            "source": "api-football",
            "error": "API_FOOTBALL_KEY is not configured",
            "diagnostic": {"key_loaded": False},
        }

    headers = {"x-apisports-key": API_KEY}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{API_FOOTBALL_BASE_URL}{endpoint}",
                headers=headers,
                params=params or {},
            )

        try:
            data = response.json()
        except Exception:
            data = {"raw_response": response.text}

        return {
            "source": "api-football",
            "diagnostic": {
                "key_loaded": True,
                "http_status": response.status_code,
            },
            "api_response": data,
        }

    except Exception as exc:
        return {
            "source": "api-football",
            "error": str(exc),
        }


async def zyla_get(
    endpoint_id: int,
    endpoint_slug: str,
    params: dict | None = None,
):
    if not ZYLA_API_KEY:
        return {
            "source": "zyla-flashscore",
            "error": "ZYLA_API_KEY is not configured",
            "diagnostic": {"key_loaded": False},
        }

    url = f"{ZYLA_BASE_URL}/{endpoint_id}/{endpoint_slug}"

    headers = {
        "Authorization": f"Bearer {ZYLA_API_KEY}",
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                url,
                headers=headers,
                params=params or {},
            )

        try:
            data = response.json()
        except Exception:
            data = {"raw_response": response.text}

        return {
            "source": "zyla-flashscore",
            "diagnostic": {
                "key_loaded": True,
                "http_status": response.status_code,
                "endpoint_id": endpoint_id,
            },
            "api_response": data,
        }

    except Exception as exc:
        return {
            "source": "zyla-flashscore",
            "error": str(exc),
        }



async def zyla_get_cached(
    endpoint_id: int,
    endpoint_slug: str,
    params: dict | None = None,
    cache_ttl: int = ZYLA_CACHE_TTL,
):
    """
    Small in-memory cache for scanner calls.
    Render restart clears it automatically.
    """
    params = params or {}
    cache_key = f"{endpoint_id}:{endpoint_slug}:{json.dumps(params, sort_keys=True, ensure_ascii=False)}"
    now = time.time()

    cached = _ZYLA_CACHE.get(cache_key)
    if cached and now < cached["expires_at"]:
        value = cached["value"]
        if isinstance(value, dict):
            value = dict(value)
            value["cache_hit"] = True
        return value

    value = await zyla_get(endpoint_id, endpoint_slug, params)

    if (
        isinstance(value, dict)
        and value.get("diagnostic", {}).get("http_status") == 200
    ):
        _ZYLA_CACHE[cache_key] = {
            "expires_at": now + max(1, int(cache_ttl)),
            "value": value,
        }

    return value


# ============================================================
# ZYLA RAW TOOLS
# ============================================================

@mcp.tool()
async def get_zyla_live_matches():
    """Get all live football matches from Zyla FlashScore."""
    return await zyla_get(
        23856,
        "get+live+matches",
        {"sport_id": 1},
    )


@mcp.tool()
async def get_zyla_match_details(match_id: str):
    """Get Zyla match details."""
    return await zyla_get(
        23859,
        "get+match+details",
        {"match_id": match_id},
    )


@mcp.tool()
async def get_zyla_match_summary(match_id: str):
    """Get Zyla goals, cards, substitutions and events."""
    return await zyla_get(
        23860,
        "get+match+summary",
        {"match_id": match_id},
    )


@mcp.tool()
async def get_zyla_match_stats(match_id: str):
    """Get Zyla live match statistics including xG when available."""
    return await zyla_get(
        23861,
        "get+match+stats",
        {"match_id": match_id},
    )


@mcp.tool()
async def get_zyla_match_odds(match_id: str):
    """Get available Zyla odds for a match."""
    return await zyla_get(
        23865,
        "get+match+odds",
        {"match_id": match_id},
    )


# ============================================================
# HIDDEN SIGNAL DATA PARSER
# ============================================================

def parse_match_metrics(details: Any, stats: Any, events: Any, odds: Any) -> dict:
    d = unwrap_api_response(details)
    s = unwrap_api_response(stats)
    e = unwrap_api_response(events)
    o = unwrap_api_response(odds)

    combined = {
        "details": d,
        "stats": s,
        "events": e,
        "odds": o,
    }

    home_name, away_name = find_team_names(d)
    if not home_name or not away_name:
        h2, a2 = find_team_names(combined)
        home_name = home_name or h2
        away_name = away_name or a2

    score_home, score_away = parse_score(d)
    minute = parse_minute(d)

    xg = find_stat_pair(s, ["expected goals", "expected goals xg", "xg"])
    shots = find_stat_pair(s, ["shots", "total shots", "shots total"])
    sot = find_stat_pair(s, ["shots on target", "shots on goal", "on target"])
    box_shots = find_stat_pair(
        s,
        ["shots inside box", "shots insidebox", "inside box", "shots in box"]
    )
    touches_box = find_stat_pair(
        s,
        ["touches in opposition box", "touches in box", "box touches"]
    )
    corners = find_stat_pair(s, ["corners", "corner kicks"])
    possession = find_stat_pair(s, ["possession", "ball possession"])
    xa = find_stat_pair(s, ["expected assists", "xa"])
    fouls = find_stat_pair(s, ["fouls", "fouls committed"])
    red_cards = parse_red_cards(combined)

    def pair_or_zero(value):
        return value if value is not None else (0.0, 0.0)

    xg = pair_or_zero(xg)
    shots = pair_or_zero(shots)
    sot = pair_or_zero(sot)
    box_shots = pair_or_zero(box_shots)
    touches_box = pair_or_zero(touches_box)
    corners = pair_or_zero(corners)
    possession = pair_or_zero(possession)
    xa = pair_or_zero(xa)
    fouls = pair_or_zero(fouls)

    return {
        "home": home_name,
        "away": away_name,
        "minute": minute,
        "score": {
            "home": score_home,
            "away": score_away,
            "total": score_home + score_away,
        },
        "xg": {"home": xg[0], "away": xg[1], "total": xg[0] + xg[1]},
        "shots": {
            "home": shots[0],
            "away": shots[1],
            "total": shots[0] + shots[1],
        },
        "shots_on_target": {
            "home": sot[0],
            "away": sot[1],
            "total": sot[0] + sot[1],
        },
        "shots_in_box": {
            "home": box_shots[0],
            "away": box_shots[1],
            "total": box_shots[0] + box_shots[1],
        },
        "touches_in_box": {
            "home": touches_box[0],
            "away": touches_box[1],
            "total": touches_box[0] + touches_box[1],
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
            "home": red_cards[0],
            "away": red_cards[1],
        },
        "events_available": bool(e),
        "odds_available": bool(o),
    }


# ============================================================
# HIDDEN SIGNAL V1 HEURISTIC ENGINE
# ============================================================

def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-clamp(x, -20, 20)))


def build_pressure(metrics: dict) -> dict:
    minute = max(metrics["minute"], 1)

    xg_total = metrics["xg"]["total"]
    shots_total = metrics["shots"]["total"]
    sot_total = metrics["shots_on_target"]["total"]
    box_total = metrics["shots_in_box"]["total"]
    touches_total = metrics["touches_in_box"]["total"]
    corners_total = metrics["corners"]["total"]

    xg_rate = xg_total * 90 / minute
    shots_rate = shots_total * 90 / minute
    sot_rate = sot_total * 90 / minute
    box_rate = box_total * 90 / minute
    touches_rate = touches_total * 90 / minute
    corners_rate = corners_total * 90 / minute

    # Нормализованные компоненты 0..1.
    c_xg = clamp(xg_rate / 3.0, 0, 1)
    c_shots = clamp(shots_rate / 24.0, 0, 1)
    c_sot = clamp(sot_rate / 9.0, 0, 1)
    c_box = clamp(box_rate / 15.0, 0, 1)
    c_touch = clamp(touches_rate / 45.0, 0, 1)
    c_corner = clamp(corners_rate / 11.0, 0, 1)

    pressure = (
        c_xg * 0.30
        + c_shots * 0.13
        + c_sot * 0.20
        + c_box * 0.13
        + c_touch * 0.14
        + c_corner * 0.10
    )

    return {
        "score_0_100": round(pressure * 100, 1),
        "xg_rate_90": round(xg_rate, 2),
        "shots_rate_90": round(shots_rate, 2),
        "sot_rate_90": round(sot_rate, 2),
        "box_shots_rate_90": round(box_rate, 2),
        "touches_box_rate_90": round(touches_rate, 2),
        "corners_rate_90": round(corners_rate, 2),
    }


def team_attack_strength(metrics: dict, side: str) -> float:
    minute = max(metrics["minute"], 1)

    xg = metrics["xg"][side] * 90 / minute
    shots = metrics["shots"][side] * 90 / minute
    sot = metrics["shots_on_target"][side] * 90 / minute
    box_shots = metrics["shots_in_box"][side] * 90 / minute
    touches = metrics["touches_in_box"][side] * 90 / minute
    corners = metrics["corners"][side] * 90 / minute

    score = (
        clamp(xg / 1.8, 0, 1) * 0.34
        + clamp(shots / 14.0, 0, 1) * 0.12
        + clamp(sot / 5.0, 0, 1) * 0.22
        + clamp(box_shots / 9.0, 0, 1) * 0.12
        + clamp(touches / 28.0, 0, 1) * 0.12
        + clamp(corners / 6.0, 0, 1) * 0.08
    )

    red_home = metrics["red_cards"]["home"]
    red_away = metrics["red_cards"]["away"]

    if side == "home":
        score += 0.08 * max(0, red_away - red_home)
        score -= 0.10 * max(0, red_home - red_away)
    else:
        score += 0.08 * max(0, red_home - red_away)
        score -= 0.10 * max(0, red_away - red_home)

    return clamp(score, 0, 1)


def probability_goal_next_5(metrics: dict, pressure: dict) -> float:
    p = pressure["score_0_100"] / 100.0
    minute = metrics["minute"]
    total_goals = metrics["score"]["total"]

    # Базовый live hazard + интенсивность.
    logit = -2.05 + 2.35 * p

    # Последняя треть матча чаще открывается.
    if 65 <= minute <= 88:
        logit += 0.18

    # Нулевой счёт в начале немного снижает немедленный hazard.
    if total_goals == 0 and minute < 25:
        logit -= 0.10

    # Красная карточка создаёт дополнительную дисперсию.
    red_diff = abs(
        metrics["red_cards"]["home"] - metrics["red_cards"]["away"]
    )
    logit += 0.20 * min(red_diff, 1)

    return clamp(logistic(logit) * 100, 5, 92)


def probability_from_interval(p5: float, minutes: float) -> float:
    p5 = clamp(p5 / 100.0, 0.001, 0.95)
    intervals = max(minutes / 5.0, 0.0)
    probability = 1 - ((1 - p5) ** intervals)
    return clamp(probability * 100, 1, 98)


def result_probabilities(metrics: dict) -> dict:
    home_strength = team_attack_strength(metrics, "home")
    away_strength = team_attack_strength(metrics, "away")

    score_home = metrics["score"]["home"]
    score_away = metrics["score"]["away"]
    minute = metrics["minute"]

    score_diff = score_home - score_away
    time_factor = clamp(minute / 90.0, 0, 1)

    home_logit = (
        0.20
        + 1.7 * (home_strength - away_strength)
        + 1.25 * score_diff * time_factor
    )
    away_logit = (
        0.00
        + 1.7 * (away_strength - home_strength)
        - 1.25 * score_diff * time_factor
    )

    home_raw = math.exp(clamp(home_logit, -6, 6))
    away_raw = math.exp(clamp(away_logit, -6, 6))

    draw_base = 1.15
    if score_diff == 0:
        draw_base += 0.75 * time_factor
    else:
        draw_base -= 0.35 * time_factor

    draw_raw = max(0.15, draw_base)

    total = home_raw + draw_raw + away_raw

    return {
        "home": round(home_raw / total * 100, 1),
        "draw": round(draw_raw / total * 100, 1),
        "away": round(away_raw / total * 100, 1),
    }


def classify_probability(probability: float) -> tuple[str, str]:
    if probability >= STRONG_SIGNAL_THRESHOLD:
        return "🟢", "ENTER"

    if probability >= MEDIUM_SIGNAL_THRESHOLD:
        return "🟡", "WAIT"

    return "🔴", "SKIP"


def signal_item(
    market: str,
    selection: str,
    probability: float,
    reasons: list[str],
    risk: str = "medium",
) -> dict:
    color, decision = classify_probability(probability)

    return {
        "market": market,
        "selection": selection,
        "probability": round(probability, 1),
        "signal": color,
        "decision": decision,
        "risk": risk,
        "reasons": reasons[:5],
    }


def build_signals(metrics: dict) -> list[dict]:
    pressure = build_pressure(metrics)

    minute = metrics["minute"]
    score_home = metrics["score"]["home"]
    score_away = metrics["score"]["away"]
    total_goals = metrics["score"]["total"]

    home_attack = team_attack_strength(metrics, "home")
    away_attack = team_attack_strength(metrics, "away")

    p5 = probability_goal_next_5(metrics, pressure)
    p10 = probability_from_interval(p5, 10)

    reasons_common = [
        f"pressure {pressure['score_0_100']}/100",
        f"xG {metrics['xg']['home']:.2f}-{metrics['xg']['away']:.2f}",
        f"shots {metrics['shots']['home']:.0f}-{metrics['shots']['away']:.0f}",
        f"SOT {metrics['shots_on_target']['home']:.0f}-{metrics['shots_on_target']['away']:.0f}",
        f"box touches {metrics['touches_in_box']['home']:.0f}-{metrics['touches_in_box']['away']:.0f}",
    ]

    signals: list[dict] = []

    if SIGNALS["goal_next_5"]:
        signals.append(
            signal_item(
                "GOAL_NEXT_5",
                "Goal in next 5 minutes",
                p5,
                reasons_common,
                "high" if p5 < STRONG_SIGNAL_THRESHOLD else "medium",
            )
        )

    if SIGNALS["goal_next_10"]:
        signals.append(
            signal_item(
                "GOAL_NEXT_10",
                "Goal in next 10 minutes",
                p10,
                reasons_common,
            )
        )

    if SIGNALS["goal_before_halftime"] and 1 <= minute < 45:
        remaining = max(1, 45 - minute)
        p_ht = probability_from_interval(p5, remaining)

        signals.append(
            signal_item(
                "GOAL_BEFORE_HALFTIME",
                "Goal before halftime",
                p_ht,
                reasons_common + [f"{remaining} min to HT"],
            )
        )

    if SIGNALS["goal_before_fulltime"] and 1 <= minute < 90:
        remaining = max(1, 90 - minute)
        p_ft = probability_from_interval(p5, remaining)

        signals.append(
            signal_item(
                "GOAL_BEFORE_FULLTIME",
                "At least one more goal",
                p_ft,
                reasons_common + [f"{remaining} min to FT"],
            )
        )

    if SIGNALS["over_under"]:
        # Динамические линии относительно текущего счёта.
        over_line = total_goals + 0.5
        p_over = probability_from_interval(p5, max(1, 90 - minute))
        p_under = 100 - p_over

        signals.append(
            signal_item(
                "OVER_UNDER",
                f"Over {over_line:.1f}",
                p_over,
                reasons_common,
            )
        )

        signals.append(
            signal_item(
                "OVER_UNDER",
                f"Under {over_line:.1f}",
                p_under,
                [
                    f"goal probability to FT {p_over:.1f}%",
                    f"current total {total_goals}",
                    f"minute {minute}",
                ],
            )
        )

    if SIGNALS["team_goal"]:
        if minute < 90:
            remaining = max(1, 90 - minute)

            # Распределяем общий hazard по силе атак.
            total_attack = max(home_attack + away_attack, 0.05)

            home_share = home_attack / total_attack
            away_share = away_attack / total_attack

            p_any = probability_from_interval(p5, remaining)
            p_home_goal = clamp(
                p_any * (0.35 + 0.85 * home_share),
                1,
                96,
            )
            p_away_goal = clamp(
                p_any * (0.35 + 0.85 * away_share),
                1,
                96,
            )

            signals.append(
                signal_item(
                    "TEAM_GOAL",
                    f"{metrics['home'] or 'Home'} to score",
                    p_home_goal,
                    [
                        f"home attack {home_attack * 100:.0f}/100",
                        f"home xG {metrics['xg']['home']:.2f}",
                        f"home shots {metrics['shots']['home']:.0f}",
                        f"home box touches {metrics['touches_in_box']['home']:.0f}",
                    ],
                )
            )

            signals.append(
                signal_item(
                    "TEAM_GOAL",
                    f"{metrics['away'] or 'Away'} to score",
                    p_away_goal,
                    [
                        f"away attack {away_attack * 100:.0f}/100",
                        f"away xG {metrics['xg']['away']:.2f}",
                        f"away shots {metrics['shots']['away']:.0f}",
                        f"away box touches {metrics['touches_in_box']['away']:.0f}",
                    ],
                )
            )

    if SIGNALS["btts"]:
        home_scored = score_home > 0
        away_scored = score_away > 0

        if home_scored and away_scored:
            p_btts = 100.0
        elif home_scored and not away_scored:
            remaining = max(1, 90 - minute)
            p_any = probability_from_interval(p5, remaining)
            p_btts = clamp(p_any * (0.50 + away_attack), 1, 95)
        elif away_scored and not home_scored:
            remaining = max(1, 90 - minute)
            p_any = probability_from_interval(p5, remaining)
            p_btts = clamp(p_any * (0.50 + home_attack), 1, 95)
        else:
            remaining = max(1, 90 - minute)
            p_any = probability_from_interval(p5, remaining)
            p_btts = clamp(
                p_any * min(home_attack, away_attack) * 1.15,
                1,
                88,
            )

        signals.append(
            signal_item(
                "BTTS",
                "Both teams to score - YES",
                p_btts,
                [
                    f"score {score_home}-{score_away}",
                    f"home attack {home_attack * 100:.0f}/100",
                    f"away attack {away_attack * 100:.0f}/100",
                    f"xG {metrics['xg']['home']:.2f}-{metrics['xg']['away']:.2f}",
                ],
            )
        )

    if SIGNALS["match_result"]:
        probs = result_probabilities(metrics)

        home_name = metrics["home"] or "Home"
        away_name = metrics["away"] or "Away"

        signals.append(
            signal_item(
                "MATCH_RESULT",
                f"{home_name} win",
                probs["home"],
                [
                    f"score {score_home}-{score_away}",
                    f"home attack {home_attack * 100:.0f}/100",
                    f"away attack {away_attack * 100:.0f}/100",
                ],
            )
        )

        signals.append(
            signal_item(
                "MATCH_RESULT",
                "Draw",
                probs["draw"],
                [
                    f"score {score_home}-{score_away}",
                    f"minute {minute}",
                ],
            )
        )

        signals.append(
            signal_item(
                "MATCH_RESULT",
                f"{away_name} win",
                probs["away"],
                [
                    f"score {score_home}-{score_away}",
                    f"home attack {home_attack * 100:.0f}/100",
                    f"away attack {away_attack * 100:.0f}/100",
                ],
            )
        )

    # Самые сильные наверх.
    signals.sort(key=lambda x: x["probability"], reverse=True)

    return signals


def apply_repeat_guard(match_id: str, signals: list[dict]) -> list[dict]:
    previous = _LAST_SIGNAL_STATE.setdefault(match_id, {})
    output = []

    for item in signals:
        key = f"{item['market']}::{item['selection']}"
        p = float(item["probability"])
        last = previous.get(key)

        is_new = (
            last is None
            or abs(p - last) >= REPEAT_SIGNAL_DELTA
        )

        item = dict(item)
        item["new_or_changed"] = is_new

        if item["decision"] == "ENTER":
            previous[key] = p

        output.append(item)

    return output


def compact_recommendation(signals: list[dict]) -> dict:
    strong = [
        s for s in signals
        if s["decision"] == "ENTER"
        and s.get("new_or_changed", True)
    ]

    if strong:
        top = strong[0]
        return {
            "status": "🟢 STRONG SIGNAL",
            "market": top["market"],
            "selection": top["selection"],
            "probability": top["probability"],
            "risk": top["risk"],
            "decision": "ENTER",
            "reasons": top["reasons"],
        }

    waiting = [s for s in signals if s["decision"] == "WAIT"]

    if waiting:
        top = waiting[0]
        return {
            "status": "🟡 NO STRONG SIGNAL",
            "market": top["market"],
            "selection": top["selection"],
            "probability": top["probability"],
            "risk": top["risk"],
            "decision": "WAIT",
            "reasons": top["reasons"],
        }

    return {
        "status": "🔴 SKIP",
        "decision": "SKIP",
        "probability": 0,
        "reasons": ["No enabled market reached the signal threshold"],
    }


# ============================================================
# MAIN MATCH ANALYZER
# ============================================================

async def _collect_zyla_match(match_id: str):
    details, stats, summary, odds = await asyncio.gather(
        zyla_get(
            23859,
            "get+match+details",
            {"match_id": match_id},
        ),
        zyla_get(
            23861,
            "get+match+stats",
            {"match_id": match_id},
        ),
        zyla_get(
            23860,
            "get+match+summary",
            {"match_id": match_id},
        ),
        zyla_get(
            23865,
            "get+match+odds",
            {"match_id": match_id},
        ),
    )

    return details, stats, summary, odds


@mcp.tool()
async def analyze_zyla_match(match_id: str):
    """
    Full Hidden Signal analysis for one Zyla live match.

    Collects details + stats/xG + events + odds,
    cleans duplicate data, calculates enabled signals,
    and returns ENTER / WAIT / SKIP.
    """

    details, stats, summary, odds = await _collect_zyla_match(match_id)

    metrics = parse_match_metrics(
        details,
        stats,
        summary,
        odds,
    )

    pressure = build_pressure(metrics)
    signals = build_signals(metrics)
    signals = apply_repeat_guard(match_id, signals)
    recommendation = compact_recommendation(signals)

    strong_signals = [
        s for s in signals
        if s["decision"] == "ENTER"
        and s.get("new_or_changed", True)
    ]

    return _dedupe({
        "source": "hidden-signal-v1",
        "model_type": "heuristic-v1-not-calibrated",
        "match_id": match_id,
        "match": {
            "home": metrics["home"],
            "away": metrics["away"],
            "minute": metrics["minute"],
            "score": metrics["score"],
        },
        "metrics": metrics,
        "pressure": pressure,
        "recommendation": recommendation,
        "strong_signals": strong_signals,
        "all_signals": signals,
        "raw": {
            "details": details,
            "statistics": stats,
            "events": summary,
            "odds": odds,
        },
    })


# ============================================================
# OPTIMIZED LIVE SCANNER
# ============================================================

def _compact_candidate_from_analysis(analysis: dict) -> dict:
    match = analysis.get("match", {})
    signals = analysis.get("all_signals", [])
    top = signals[0] if signals else None

    return {
        "match_id": analysis.get("match_id"),
        "home": match.get("home"),
        "away": match.get("away"),
        "minute": match.get("minute"),
        "score": match.get("score"),
        "top_signal": top,
        "pressure": analysis.get("pressure"),
    }


async def _light_analyze_zyla_match(match_id: str) -> dict:
    """
    Cheap stage for the scanner:
    only Details + Stats (2 calls, often less with cache).
    No giant raw payload is returned.
    """
    details, stats = await asyncio.gather(
        zyla_get_cached(
            23859,
            "get+match+details",
            {"match_id": match_id},
            cache_ttl=30,
        ),
        zyla_get_cached(
            23861,
            "get+match+stats",
            {"match_id": match_id},
            cache_ttl=30,
        ),
    )

    metrics = parse_match_metrics(
        details,
        stats,
        {},
        {},
    )

    pressure = build_pressure(metrics)
    signals = build_signals(metrics)

    return {
        "match_id": match_id,
        "match": {
            "home": metrics["home"],
            "away": metrics["away"],
            "minute": metrics["minute"],
            "score": metrics["score"],
        },
        "metrics": {
            "xg": metrics["xg"],
            "shots": metrics["shots"],
            "shots_on_target": metrics["shots_on_target"],
            "shots_in_box": metrics["shots_in_box"],
            "touches_in_box": metrics["touches_in_box"],
            "corners": metrics["corners"],
            "possession": metrics["possession"],
            "red_cards": metrics["red_cards"],
        },
        "pressure": pressure,
        "all_signals": signals,
        "diagnostic": {
            "details_http": details.get("diagnostic", {}).get("http_status")
            if isinstance(details, dict) else None,
            "stats_http": stats.get("diagnostic", {}).get("http_status")
            if isinstance(stats, dict) else None,
        },
    }


async def _deep_confirm_zyla_match(match_id: str) -> dict:
    """
    Deep confirmation only for genuinely strong candidates:
    Events + Odds (2 extra calls, often less with cache).
    """
    summary, odds = await asyncio.gather(
        zyla_get_cached(
            23860,
            "get+match+summary",
            {"match_id": match_id},
            cache_ttl=45,
        ),
        zyla_get_cached(
            23865,
            "get+match+odds",
            {"match_id": match_id},
            cache_ttl=30,
        ),
    )

    return {
        "events_http": summary.get("diagnostic", {}).get("http_status")
        if isinstance(summary, dict) else None,
        "odds_http": odds.get("diagnostic", {}).get("http_status")
        if isinstance(odds, dict) else None,
        "events_available": bool(unwrap_api_response(summary)),
        "odds_available": bool(unwrap_api_response(odds)),
    }


@mcp.tool()
async def scan_zyla_live(max_matches: int = MAX_SCAN_MATCHES):
    """
    Economical Hidden Signal scanner.

    1. One request gets all live football matches.
    2. At most max_matches candidates get Details + Stats.
    3. Events + Odds are requested only for matches that already have
       at least one 75%+ signal.
    4. Returns compact JSON only: counters, strong signals and top-5.
    """

    max_matches = int(clamp(max_matches, 1, 12))

    live = await zyla_get_cached(
        23856,
        "get+live+matches",
        {"sport_id": 1},
        cache_ttl=20,
    )

    live_http = (
        live.get("diagnostic", {}).get("http_status")
        if isinstance(live, dict) else None
    )

    if live_http != 200:
        return {
            "source": "hidden-signal-v1.1",
            "status": "ZYLA_LIVE_ERROR",
            "live_http_status": live_http,
            "live_matches_found": 0,
            "matches_light_analyzed": 0,
            "fully_analyzed": 0,
            "strong_signal_threshold": STRONG_SIGNAL_THRESHOLD,
            "strong_signals": [],
            "top_candidates": [],
            "zyla_error": (
                live.get("api_response")
                if isinstance(live, dict)
                else str(live)
            ),
        }

    all_candidates = extract_live_match_candidates(live)
    total_live_found = len(all_candidates)

    if not all_candidates:
        return {
            "source": "hidden-signal-v1.1",
            "status": "NO_LIVE_MATCHES_OR_PARSE_FAILED",
            "live_http_status": 200,
            "live_matches_found": 0,
            "matches_light_analyzed": 0,
            "fully_analyzed": 0,
            "strong_signal_threshold": STRONG_SIGNAL_THRESHOLD,
            "strong_signals": [],
            "top_candidates": [],
        }

    # Prefer useful live windows and matches whose minute parsed correctly.
    def pre_score(item: dict):
        minute = int(item.get("minute") or 0)
        score_home, score_away = item.get("score") or (0, 0)
        total_goals = int(score_home) + int(score_away)

        value = 0
        if 8 <= minute <= 88:
            value += 50
        if 30 <= minute < 45:
            value += 12
        if 55 <= minute <= 85:
            value += 18
        if total_goals <= 3:
            value += 8
        if item.get("home") and item.get("away"):
            value += 10
        return value

    all_candidates.sort(key=pre_score, reverse=True)
    selected = all_candidates[:max_matches]

    semaphore = asyncio.Semaphore(3)

    async def run_light(item):
        async with semaphore:
            try:
                return await _light_analyze_zyla_match(item["match_id"])
            except Exception as exc:
                return {
                    "match_id": item["match_id"],
                    "error": str(exc),
                }

    light_results = await asyncio.gather(
        *(run_light(item) for item in selected)
    )

    valid = [
        item for item in light_results
        if isinstance(item, dict)
        and "error" not in item
        and item.get("all_signals")
    ]

    # Sort by best signal even if below 75%.
    valid.sort(
        key=lambda x: x.get("all_signals", [{}])[0].get("probability", 0),
        reverse=True,
    )

    strong_match_ids = []
    strong_signals = []

    for analysis in valid:
        match = analysis.get("match", {})

        for signal in analysis.get("all_signals", []):
            if signal.get("decision") != "ENTER":
                continue

            strong_match_ids.append(analysis["match_id"])
            strong_signals.append({
                "match_id": analysis.get("match_id"),
                "home": match.get("home"),
                "away": match.get("away"),
                "minute": match.get("minute"),
                "score": match.get("score"),
                "market": signal.get("market"),
                "selection": signal.get("selection"),
                "probability": signal.get("probability"),
                "risk": signal.get("risk"),
                "decision": signal.get("decision"),
                "reasons": signal.get("reasons"),
            })

    # Deep confirmation only for unique matches with a strong signal.
    unique_strong_ids = list(dict.fromkeys(strong_match_ids))[:4]

    async def run_deep(match_id):
        async with semaphore:
            try:
                confirmation = await _deep_confirm_zyla_match(match_id)
                return match_id, confirmation
            except Exception as exc:
                return match_id, {"error": str(exc)}

    deep_pairs = await asyncio.gather(
        *(run_deep(match_id) for match_id in unique_strong_ids)
    ) if unique_strong_ids else []

    confirmations = dict(deep_pairs)

    for signal in strong_signals:
        signal["deep_confirmation"] = confirmations.get(
            signal["match_id"],
            {"not_requested": True},
        )

    # Repeat guard is applied only to strong signals that are about to surface.
    final_strong = []
    for signal in strong_signals:
        match_id = signal["match_id"]
        key = f"{signal['market']}::{signal['selection']}"
        p = float(signal["probability"])

        state = _LAST_SIGNAL_STATE.setdefault(match_id, {})
        previous = state.get(key)

        is_new = (
            previous is None
            or abs(p - previous) >= REPEAT_SIGNAL_DELTA
        )

        signal["new_or_changed"] = is_new

        if is_new:
            state[key] = p
            final_strong.append(signal)

    final_strong.sort(
        key=lambda x: x.get("probability", 0),
        reverse=True,
    )

    top_candidates = []

    for analysis in valid[:5]:
        match = analysis.get("match", {})
        top = analysis.get("all_signals", [{}])[0]

        top_candidates.append({
            "match_id": analysis.get("match_id"),
            "home": match.get("home"),
            "away": match.get("away"),
            "minute": match.get("minute"),
            "score": match.get("score"),
            "market": top.get("market"),
            "selection": top.get("selection"),
            "probability": top.get("probability"),
            "decision": top.get("decision"),
            "reasons": top.get("reasons"),
        })

    return {
        "source": "hidden-signal-v1.1",
        "status": "OK",
        "model_type": "heuristic-v1-not-calibrated",
        "live_http_status": 200,
        "live_matches_found": total_live_found,
        "matches_selected_for_stats": len(selected),
        "matches_light_analyzed": len(valid),
        "fully_analyzed": len(confirmations),
        "strong_signal_threshold": STRONG_SIGNAL_THRESHOLD,
        "strong_signals": final_strong,
        "top_candidates": top_candidates,
        "estimated_max_api_calls_without_cache": (
            1
            + len(selected) * 2
            + len(unique_strong_ids) * 2
        ),
        "cache_ttl_seconds": ZYLA_CACHE_TTL,
        "note": (
            "Scanner returns compact JSON only. "
            "Events and odds are fetched only for 75%+ candidates."
        ),
    }


# ============================================================
# STATUS
# ============================================================

@mcp.tool()
async def hidden_signal_status():
    """Check Hidden Signal configuration and enabled markets."""

    return {
        "service": "Hidden Signal Live",
        "version": "V1.1",
        "zyla_key_loaded": bool(ZYLA_API_KEY),
        "api_football_key_loaded": bool(API_KEY),
        "strong_signal_threshold": STRONG_SIGNAL_THRESHOLD,
        "medium_signal_threshold": MEDIUM_SIGNAL_THRESHOLD,
        "max_scan_matches": MAX_SCAN_MATCHES,
        "signals": SIGNALS,
        "model_type": "heuristic-v1-not-calibrated",
        "scanner_mode": "optimized-compact-cache",
        "note": (
            "Percentages are heuristic estimates. "
            "They must be calibrated against real results before "
            "being treated as statistical probabilities."
        ),
    }


# ============================================================
# API-FOOTBALL FALLBACK TOOLS
# ============================================================

@mcp.tool()
async def get_live_matches():
    """Get all API-Football matches that are live right now."""
    return await api_get(
        "/fixtures",
        {"live": "all"},
    )


@mcp.tool()
async def get_fixture_details(fixture_id: int):
    """Get fixture details from API-Football."""
    return await api_get(
        "/fixtures",
        {"id": fixture_id},
    )


@mcp.tool()
async def get_fixture_statistics(fixture_id: int):
    """Get fixture live statistics from API-Football."""
    return await api_get(
        "/fixtures/statistics",
        {"fixture": fixture_id},
    )


@mcp.tool()
async def get_fixture_events(fixture_id: int):
    """Get goals, cards, substitutions and VAR events."""
    return await api_get(
        "/fixtures/events",
        {"fixture": fixture_id},
    )


@mcp.tool()
async def get_fixture_lineups(fixture_id: int):
    """Get lineups and formations."""
    return await api_get(
        "/fixtures/lineups",
        {"fixture": fixture_id},
    )


@mcp.tool()
async def get_head_to_head(
    team1_id: int,
    team2_id: int,
    last: int = 10,
):
    """Get recent head-to-head matches."""
    return await api_get(
        "/fixtures/headtohead",
        {
            "h2h": f"{team1_id}-{team2_id}",
            "last": last,
        },
    )


@mcp.tool()
async def get_team_last_matches(
    team_id: int,
    season: int,
    last: int = 10,
):
    """Get latest matches for a team."""
    return await api_get(
        "/fixtures",
        {
            "team": team_id,
            "season": season,
            "last": last,
        },
    )


@mcp.tool()
async def get_injuries(fixture_id: int):
    """Get injury information."""
    return await api_get(
        "/injuries",
        {"fixture": fixture_id},
    )


@mcp.tool()
async def get_prematch_odds(fixture_id: int):
    """Get pre-match bookmaker odds."""
    return await api_get(
        "/odds",
        {"fixture": fixture_id},
    )


@mcp.tool()
async def get_live_odds(fixture_id: int):
    """Get live bookmaker odds."""
    return await api_get(
        "/odds/live",
        {"fixture": fixture_id},
    )


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
