
import os
import json
import math
import time
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

VERSION = "V4.0-FOOTBALL-REACTOR"
MODEL_TYPE = "heuristic-v4.0-not-calibrated"

ZYLA_API_KEY = os.getenv("ZYLA_API_KEY", "").strip()
ZYLA_BASE = "https://zylalabs.com/api/12518/flashscore+-+live+api"

ENDPOINTS = {
    "live": (23856, "get+live+matches"),
    "details": (23859, "get+match+details"),
    "summary": (23860, "get+match+summary"),
    "stats": (23861, "get+match+stats"),
    "lineups": (23862, "get+match+lineups"),
    "player_stats": (23863, "get+match+player+stats"),
    "odds": (23865, "get+match+odds"),
    "h2h": (23866, "get+h2h"),
}

STRONG_THRESHOLD = 75.0
EARLY_MINUTE_HARD = 8
EARLY_MINUTE_SOFT = 15
STALE_SECONDS = 90

mcp = FastMCP("Hidden Signal Live")


# -------------------------
# Utility
# -------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))

def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace("%", "").replace(",", ".")
        if not s or s.lower() in {"n/a", "null", "-", "none"}:
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None

def round1(v: Any) -> Any:
    try:
        return round(float(v), 1)
    except Exception:
        return v

def endpoint_url(name: str) -> str:
    eid, slug = ENDPOINTS[name]
    return f"{ZYLA_BASE}/{eid}/{slug}"

def headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {ZYLA_API_KEY}"}

def score_obj(home: Any, away: Any) -> Dict[str, int]:
    h = int(safe_float(home) or 0)
    a = int(safe_float(away) or 0)
    return {"home": h, "away": a, "total": h + a}

def normalize_name(s: Any) -> str:
    return str(s or "").strip()

def weighted_mean(items: List[Tuple[float, float]]) -> float:
    valid = [(float(v), float(w)) for v, w in items if v is not None and w > 0]
    if not valid:
        return 0.0
    sw = sum(w for _, w in valid)
    return sum(v * w for v, w in valid) / sw if sw else 0.0


# -------------------------
# Lightweight journal
# -------------------------

SIGNAL_LOG_PATH = os.getenv("SIGNAL_LOG_PATH", "/tmp/hidden_signal_v4_signals.jsonl")
_REPEAT_MEMORY: Dict[str, Dict[str, Any]] = {}

def log_event(payload: Dict[str, Any]) -> None:
    record = {"logged_at": now_iso(), **payload}
    try:
        with open(SIGNAL_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass

def repeat_key(match_id: str, market: str, selection: str) -> str:
    return f"{match_id}|{market}|{selection}"

def is_new_or_changed(match_id: str, signal: Dict[str, Any]) -> bool:
    key = repeat_key(match_id, signal.get("market", ""), signal.get("selection", ""))
    current = {
        "probability": round1(signal.get("probability", 0)),
        "decision": signal.get("decision"),
    }
    previous = _REPEAT_MEMORY.get(key)
    changed = (
        previous is None
        or previous.get("decision") != current["decision"]
        or abs(float(previous.get("probability", 0)) - float(current["probability"])) >= 3.0
    )
    _REPEAT_MEMORY[key] = current
    return changed


# -------------------------
# Zyla client
# -------------------------

class ZylaClient:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(timeout=18.0, headers=headers())

    async def close(self) -> None:
        await self.client.aclose()

    async def get(self, name: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not ZYLA_API_KEY:
            return {"ok": False, "status": 0, "data": None, "error": "ZYLA_API_KEY missing"}
        try:
            r = await self.client.get(endpoint_url(name), params=params or {})
            data: Any
            try:
                data = r.json()
            except Exception:
                data = r.text
            return {
                "ok": 200 <= r.status_code < 300,
                "status": r.status_code,
                "data": data,
                "error": None if 200 <= r.status_code < 300 else str(data)[:500],
            }
        except Exception as e:
            return {"ok": False, "status": 0, "data": None, "error": repr(e)}

    async def live(self) -> Dict[str, Any]:
        return await self.get("live", {"sport_id": 1})

    async def details(self, match_id: str) -> Dict[str, Any]:
        return await self.get("details", {"match_id": match_id})

    async def summary(self, match_id: str) -> Dict[str, Any]:
        return await self.get("summary", {"match_id": match_id})

    async def stats(self, match_id: str) -> Dict[str, Any]:
        return await self.get("stats", {"match_id": match_id})

    async def lineups(self, match_id: str) -> Dict[str, Any]:
        return await self.get("lineups", {"match_id": match_id})

    async def player_stats(self, match_id: str) -> Dict[str, Any]:
        return await self.get("player_stats", {"match_id": match_id})

    async def odds(self, match_id: str) -> Dict[str, Any]:
        return await self.get("odds", {"match_id": match_id})

    async def h2h(self, match_id: str) -> Dict[str, Any]:
        return await self.get("h2h", {"match_id": match_id})


# -------------------------
# Live parsing
# -------------------------

def flatten_live(data: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(data, list):
        return out
    for block in data:
        if not isinstance(block, dict):
            continue
        tournament = block.get("name")
        tournament_id = block.get("tournament_id")
        for m in block.get("matches", []) or []:
            if not isinstance(m, dict):
                continue
            st = m.get("match_status") or {}
            scores = m.get("scores") or {}
            ht = m.get("home_team") or {}
            at = m.get("away_team") or {}

            live_time = st.get("live_time")
            minute = None
            if isinstance(live_time, (int, float)):
                minute = int(live_time)
            elif isinstance(live_time, str):
                digits = "".join(ch for ch in live_time if ch.isdigit())
                if digits:
                    minute = int(digits[:3])
            stage = str(st.get("stage") or "")
            if minute is None:
                if "Half Time" in stage:
                    minute = 45
                elif "Finished" in stage:
                    minute = 90
                else:
                    minute = 0

            out.append({
                "match_id": m.get("match_id"),
                "tournament_id": tournament_id,
                "tournament": tournament,
                "home": ht.get("name"),
                "away": at.get("name"),
                "home_team_id": ht.get("team_id"),
                "away_team_id": at.get("team_id"),
                "minute": minute,
                "stage": stage,
                "is_in_progress": bool(st.get("is_in_progress")),
                "score": score_obj(scores.get("home"), scores.get("away")),
                "red_cards": {
                    "home": int(safe_float(ht.get("red_cards")) or 0),
                    "away": int(safe_float(at.get("red_cards")) or 0),
                },
                "live_odds_1x2": m.get("odds") or {},
                "raw": m,
            })
    return out

def pick_live(live_matches: List[Dict[str, Any]], match_id: str) -> Optional[Dict[str, Any]]:
    for m in live_matches:
        if str(m.get("match_id")) == str(match_id):
            return m
    return None


# -------------------------
# Statistics parser
# -------------------------

STAT_ALIASES = {
    "xg": ["Expected goals (xG)", "Expected goals", "xG"],
    "shots": ["Total shots", "Shots"],
    "shots_on_target": ["Shots on target", "Shots on Target"],
    "shots_in_box": ["Shots inside the box", "Shots in the box"],
    "touches_in_box": ["Touches in opposition box", "Touches in the box", "Touches in penalty area"],
    "corners": ["Corner kicks", "Corners"],
    "possession": ["Ball possession", "Possession"],
    "xa": ["Expected assists (xA)", "Expected assists", "xA"],
    "fouls": ["Fouls", "Fouls committed"],
    "big_chances": ["Big chances", "Big Chances"],
    "dangerous_attacks": ["Dangerous attacks", "Dangerous Attacks"],
}

def stats_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        for key in ("match", "stats", "statistics"):
            rows = data.get(key)
            if isinstance(rows, list):
                return [r for r in rows if isinstance(r, dict)]
        # sometimes nested
        for v in data.values():
            if isinstance(v, dict):
                rows = v.get("match")
                if isinstance(rows, list):
                    return [r for r in rows if isinstance(r, dict)]
    if isinstance(data, list):
        # direct stat rows
        if all(isinstance(x, dict) and "name" in x for x in data):
            return data
    return []

def find_stat(rows: List[Dict[str, Any]], aliases: List[str]) -> Dict[str, Any]:
    alias_norm = {a.strip().lower() for a in aliases}
    for r in rows:
        n = str(r.get("name") or "").strip().lower()
        if n in alias_norm:
            h = safe_float(r.get("home_team"))
            a = safe_float(r.get("away_team"))
            return {
                "home": h,
                "away": a,
                "total": (h + a) if h is not None and a is not None else None,
                "present": h is not None and a is not None,
            }
    return {"home": None, "away": None, "total": None, "present": False}

def parse_metrics(stats_data: Any, live: Dict[str, Any]) -> Dict[str, Any]:
    rows = stats_rows(stats_data)
    metrics = {k: find_stat(rows, aliases) for k, aliases in STAT_ALIASES.items()}
    metrics["red_cards"] = {
        "home": live.get("red_cards", {}).get("home", 0),
        "away": live.get("red_cards", {}).get("away", 0),
        "total": live.get("red_cards", {}).get("home", 0) + live.get("red_cards", {}).get("away", 0),
        "present": True,
    }
    return {"rows": rows, "metrics": metrics}


# -------------------------
# Data quality
# -------------------------

QUALITY_WEIGHTS = {
    "shots": 18,
    "shots_on_target": 18,
    "xg": 20,
    "touches_in_box": 14,
    "shots_in_box": 10,
    "corners": 6,
    "possession": 4,
    "xa": 3,
    "big_chances": 4,
    "dangerous_attacks": 3,
}

def quality_guard(metrics: Dict[str, Any]) -> Dict[str, Any]:
    score = 0
    missing = []
    for k, w in QUALITY_WEIGHTS.items():
        present = bool((metrics.get(k) or {}).get("present"))
        if present:
            score += w
        else:
            missing.append(k)
    score = int(min(100, score))
    basic_ok = bool(metrics["shots"]["present"] and metrics["shots_on_target"]["present"])
    advanced_keys = ["xg", "touches_in_box", "shots_in_box", "big_chances"]
    advanced_count = sum(1 for k in advanced_keys if metrics.get(k, {}).get("present"))
    strong_eligible = basic_ok and advanced_count >= 1 and score >= 55
    level = "HIGH" if strong_eligible and score >= 75 else ("MEDIUM" if basic_ok and score >= 45 else "LOW")
    return {
        "score": score,
        "level": level,
        "basic_ok": basic_ok,
        "advanced_count": advanced_count,
        "strong_eligible": strong_eligible,
        "missing": missing,
    }


# -------------------------
# Lineups & player context
# -------------------------

ATTACK_FORMATION_BONUS = {
    "3-4-3": 5.0, "4-3-3": 4.0, "4-2-3-1": 2.5, "3-4-2-1": 2.0,
    "4-4-2": 1.5, "3-5-2": 1.5, "4-1-4-1": 0.5,
    "5-4-1": -2.5, "5-3-2": -1.5,
}

def parse_lineups(data: Any) -> Dict[str, Any]:
    sides = {"home": {}, "away": {}}
    available = False
    if isinstance(data, list):
        for side in data:
            if not isinstance(side, dict):
                continue
            s = str(side.get("side") or "").lower()
            if s not in sides:
                continue
            formation = side.get("formation") or side.get("predictedFormation")
            starters = side.get("startingLineups") or side.get("predictedLineups") or []
            bench = side.get("substitutes") or side.get("bench") or []
            sides[s] = {
                "formation": formation,
                "formation_attack_modifier": ATTACK_FORMATION_BONUS.get(str(formation), 0.0),
                "starters_count": len(starters) if isinstance(starters, list) else 0,
                "bench_count": len(bench) if isinstance(bench, list) else 0,
                "starters": [
                    {
                        "name": p.get("name"),
                        "number": p.get("number"),
                        "player_id": p.get("player_id"),
                    }
                    for p in (starters if isinstance(starters, list) else [])
                    if isinstance(p, dict)
                ][:15],
            }
            available = True
    return {"available": available, **sides}

def parse_player_stats(data: Any) -> Dict[str, Any]:
    # Schema differs by competition. Preserve summaries without inventing fields.
    result = {"available": False, "home": [], "away": [], "raw_type": type(data).__name__}
    if not isinstance(data, (list, dict)):
        return result

    candidates = data if isinstance(data, list) else data.get("players") or data.get("playerStats") or []
    if not isinstance(candidates, list):
        return result

    for p in candidates[:80]:
        if not isinstance(p, dict):
            continue
        side = str(p.get("side") or p.get("team_side") or "").lower()
        target = result["home"] if side == "home" else result["away"] if side == "away" else None
        compact = {
            "name": p.get("name") or p.get("player_name"),
            "player_id": p.get("player_id"),
            "rating": safe_float(p.get("rating")),
            "shots": safe_float(p.get("shots") or p.get("total_shots")),
            "shots_on_target": safe_float(p.get("shots_on_target")),
            "xg": safe_float(p.get("xg") or p.get("expected_goals")),
            "xa": safe_float(p.get("xa") or p.get("expected_assists")),
            "goals": safe_float(p.get("goals")),
            "assists": safe_float(p.get("assists")),
        }
        if target is not None:
            target.append(compact)
            result["available"] = True
    return result


# -------------------------
# Momentum / pressure
# -------------------------

def total_val(metrics: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = metrics.get(key, {}).get("total")
    return float(v) if v is not None else default

def side_val(metrics: Dict[str, Any], key: str, side: str, default: float = 0.0) -> float:
    v = metrics.get(key, {}).get(side)
    return float(v) if v is not None else default

def pressure_score(metrics: Dict[str, Any], minute: int) -> Dict[str, float]:
    # normalized live attacking pressure, intentionally heuristic
    minute = max(1, minute)
    pace = min(1.6, 90.0 / minute)

    def side(side_name: str) -> float:
        xg = side_val(metrics, "xg", side_name)
        shots = side_val(metrics, "shots", side_name)
        sot = side_val(metrics, "shots_on_target", side_name)
        sib = side_val(metrics, "shots_in_box", side_name)
        tib = side_val(metrics, "touches_in_box", side_name)
        corners = side_val(metrics, "corners", side_name)
        big = side_val(metrics, "big_chances", side_name)
        da = side_val(metrics, "dangerous_attacks", side_name)
        raw = (
            xg * 18
            + shots * 1.35
            + sot * 4.2
            + sib * 1.8
            + tib * 0.65
            + corners * 1.0
            + big * 6.0
            + da * 0.18
        ) * pace
        return clamp(raw, 0, 100)

    h = side("home")
    a = side("away")
    total = clamp((h + a) * 0.62, 0, 100)
    return {"home": round1(h), "away": round1(a), "total": round1(total)}


# -------------------------
# Probability components
# -------------------------

def baseline_goal_probability(minute: int, total_goals: int) -> float:
    remaining = max(0, 96 - minute)
    # broad prior, not calibrated
    rate = 1.55 / 90.0
    if total_goals >= 4:
        rate *= 0.90
    p = 1 - math.exp(-rate * remaining)
    return clamp(p * 100)

def live_goal_probability(live: Dict[str, Any], metrics: Dict[str, Any], pressure: Dict[str, float]) -> float:
    minute = int(live["minute"])
    prior = baseline_goal_probability(minute, live["score"]["total"])
    xg_total = total_val(metrics, "xg")
    shots = total_val(metrics, "shots")
    sot = total_val(metrics, "shots_on_target")
    tib = total_val(metrics, "touches_in_box")
    big = total_val(metrics, "big_chances")
    corners = total_val(metrics, "corners")

    evidence = 0.0
    if metrics["xg"]["present"]:
        expected_pace_xg = xg_total * (90.0 / max(15, minute))
        evidence += (expected_pace_xg - 2.2) * 7.5
    if metrics["shots"]["present"]:
        expected_shots = shots * (90.0 / max(15, minute))
        evidence += (expected_shots - 20) * 0.48
    if metrics["shots_on_target"]["present"]:
        expected_sot = sot * (90.0 / max(15, minute))
        evidence += (expected_sot - 7) * 1.8
    if metrics["touches_in_box"]["present"]:
        expected_tib = tib * (90.0 / max(15, minute))
        evidence += (expected_tib - 30) * 0.18
    if metrics["big_chances"]["present"]:
        evidence += big * 2.4
    if metrics["corners"]["present"]:
        evidence += min(6, corners * 0.35)
    evidence += (pressure["total"] - 50) * 0.27

    reds = metrics["red_cards"]
    if reds["home"] + reds["away"] > 0:
        evidence += 4.0

    return clamp(prior + evidence, 3, 97)

def team_share(metrics: Dict[str, Any], pressure: Dict[str, float], side: str) -> float:
    other = "away" if side == "home" else "home"
    xg_s = side_val(metrics, "xg", side)
    xg_o = side_val(metrics, "xg", other)
    sot_s = side_val(metrics, "shots_on_target", side)
    sot_o = side_val(metrics, "shots_on_target", other)
    tib_s = side_val(metrics, "touches_in_box", side)
    tib_o = side_val(metrics, "touches_in_box", other)

    ratios = []
    if metrics["xg"]["present"]:
        ratios.append((xg_s + 0.12) / (xg_s + xg_o + 0.24))
    if metrics["shots_on_target"]["present"]:
        ratios.append((sot_s + 0.6) / (sot_s + sot_o + 1.2))
    if metrics["touches_in_box"]["present"]:
        ratios.append((tib_s + 2.0) / (tib_s + tib_o + 4.0))
    ps = pressure[side]
    po = pressure[other]
    ratios.append((ps + 10.0) / (ps + po + 20.0))
    return clamp(sum(ratios) / len(ratios) * 100, 15, 85)

def team_goal_probability(goal_p: float, share: float) -> float:
    # chance this team supplies at least one future goal
    s = share / 100.0
    p_any = goal_p / 100.0
    p = p_any * (0.60 + 0.82 * s)
    return clamp(p * 100, 2, 95)

def next_window_probability(goal_ft: float, minute: int, window: int) -> float:
    remain = max(1, 96 - minute)
    hazard = -math.log(max(0.001, 1 - goal_ft / 100.0)) / remain
    p = 1 - math.exp(-hazard * min(window, remain))
    return clamp(p * 100, 1, 85)


# -------------------------
# Guards
# -------------------------

def early_match_guard(prob: float, minute: int, market: str, metrics: Dict[str, Any]) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    p = float(prob)

    if minute <= EARLY_MINUTE_HARD:
        cap = 72.0
        if market in {"TEAM_GOAL", "GOAL_BEFORE_FULLTIME", "OVER_UNDER"}:
            p = min(p, cap)
            reasons.append(f"early match hard cap ≤{cap:.0f}% through {EARLY_MINUTE_HARD}'")

    elif minute <= EARLY_MINUTE_SOFT:
        sample_strength = 0.0
        xg = total_val(metrics, "xg")
        shots = total_val(metrics, "shots")
        sot = total_val(metrics, "shots_on_target")
        tib = total_val(metrics, "touches_in_box")
        if xg >= 0.45: sample_strength += 1
        if shots >= 7: sample_strength += 1
        if sot >= 3: sample_strength += 1
        if tib >= 10: sample_strength += 1
        cap = 80.0 if sample_strength >= 3 else 74.0
        if market in {"TEAM_GOAL", "GOAL_BEFORE_FULLTIME", "OVER_UNDER"} and p > cap:
            p = cap
            reasons.append(f"early sample cap ≤{cap:.0f}% at {minute}'")

    return p, reasons

def small_sample_guard(prob: float, minute: int, metrics: Dict[str, Any]) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    p = float(prob)
    shots = total_val(metrics, "shots")
    sot = total_val(metrics, "shots_on_target")
    xg = total_val(metrics, "xg")
    if minute < 25 and shots < 5 and sot < 2 and xg < 0.25:
        if p > 70:
            p = 70.0
            reasons.append("small-sample cap: low shot/xG volume")
    return p, reasons

def quality_probability_guard(
    prob: float, market: str, q: Dict[str, Any], metrics: Dict[str, Any]
) -> Tuple[float, List[str], bool]:
    p = float(prob)
    reasons: List[str] = []
    blocked = False
    if q["level"] == "MEDIUM":
        p -= 5
        reasons.append("medium data quality penalty")
    elif q["level"] == "LOW":
        p -= 12
        reasons.append("low data quality penalty")

    if market == "UNDER" and not metrics["xg"]["present"]:
        p = min(p, 69)
        reasons.append("xG unavailable for under")
    if market in {"TEAM_GOAL", "BTTS"} and q["advanced_count"] == 0:
        p = min(p, 69)
        reasons.append("advanced attacking metrics unavailable")

    if p >= STRONG_THRESHOLD and not q["strong_eligible"]:
        p = min(p, 69)
        blocked = True
        reasons.append("Data Quality Guard blocked ENTER")
    return clamp(p), reasons, blocked

def red_card_guard(prob: float, live: Dict[str, Any], market: str) -> Tuple[float, List[str]]:
    reds = live.get("red_cards") or {"home": 0, "away": 0}
    reasons = []
    p = prob
    if reds.get("home", 0) or reds.get("away", 0):
        # Do not blindly treat red card as more goals for all markets
        if market in {"BTTS", "TEAM_GOAL"}:
            p -= 3
            reasons.append("red-card uncertainty penalty")
    return clamp(p), reasons

def decision_for(p: float, q: Dict[str, Any]) -> str:
    if p >= STRONG_THRESHOLD and q["strong_eligible"]:
        return "ENTER"
    if p >= 62:
        return "WAIT"
    return "SKIP"


# -------------------------
# Odds parser (best effort)
# -------------------------

def compact_odds(data: Any) -> Dict[str, Any]:
    if data is None:
        return {"available": False, "bookmakers_count": 0, "bookmakers": []}
    books = []
    if isinstance(data, list):
        for x in data[:12]:
            if isinstance(x, dict):
                books.append(x)
    elif isinstance(data, dict):
        candidates = data.get("bookmakers") or data.get("odds") or data.get("data")
        if isinstance(candidates, list):
            books = [x for x in candidates[:12] if isinstance(x, dict)]
    return {
        "available": bool(books),
        "bookmakers_count": len(books),
        "bookmakers": books[:5],
    }


# -------------------------
# H2H / context
# -------------------------

def h2h_context(data: Any) -> Dict[str, Any]:
    # H2H gets deliberately low weight. We return evidence, not false precision.
    matches = []
    if isinstance(data, list):
        matches = [x for x in data if isinstance(x, dict)]
    elif isinstance(data, dict):
        for key in ("matches", "h2h", "data"):
            if isinstance(data.get(key), list):
                matches = [x for x in data[key] if isinstance(x, dict)]
                break
    return {
        "available": bool(matches),
        "sample_count": len(matches),
        "weight_policy": "LOW",
        "note": "H2H is context only; it cannot create an ENTER by itself.",
    }


# -------------------------
# Signal engine
# -------------------------

def apply_guards(
    raw_p: float,
    minute: int,
    market_class: str,
    q: Dict[str, Any],
    metrics: Dict[str, Any],
    live: Dict[str, Any],
) -> Tuple[float, List[str], bool]:
    p = raw_p
    reasons: List[str] = []
    blocked = False

    p, r = early_match_guard(p, minute, market_class, metrics)
    reasons += r
    p, r = small_sample_guard(p, minute, metrics)
    reasons += r
    p, r, b = quality_probability_guard(p, market_class, q, metrics)
    reasons += r
    blocked = blocked or b
    p, r = red_card_guard(p, live, market_class)
    reasons += r
    return round1(clamp(p)), reasons, blocked

def signal_obj(
    market: str,
    selection: str,
    raw_p: float,
    guarded_p: float,
    q: Dict[str, Any],
    reasons: List[str],
    guard_reasons: List[str],
    risk: str = "medium",
    blocked: bool = False,
) -> Dict[str, Any]:
    decision = decision_for(guarded_p, q)
    return {
        "market": market,
        "selection": selection,
        "raw_probability": round1(raw_p),
        "probability": round1(guarded_p),
        "signal": "🟢" if decision == "ENTER" else ("🟡" if decision == "WAIT" else "🔴"),
        "decision": decision,
        "risk": risk,
        "reasons": reasons,
        "guard_reasons": guard_reasons,
        "data_quality_blocked": blocked,
        "data_quality_score": q["score"],
        "data_quality_level": q["level"],
    }

def build_signals(
    live: Dict[str, Any],
    metrics: Dict[str, Any],
    q: Dict[str, Any],
    pressure: Dict[str, float],
    lineups: Dict[str, Any],
) -> List[Dict[str, Any]]:
    minute = int(live["minute"])
    score = live["score"]
    goal_raw = live_goal_probability(live, metrics, pressure)

    # lineup formation influence kept small
    formation_mod = 0.0
    if lineups.get("available"):
        formation_mod += float(lineups.get("home", {}).get("formation_attack_modifier") or 0)
        formation_mod += float(lineups.get("away", {}).get("formation_attack_modifier") or 0)
        formation_mod *= 0.35
    goal_raw = clamp(goal_raw + formation_mod)

    hshare = team_share(metrics, pressure, "home")
    ashare = 100 - hshare
    home_raw = team_goal_probability(goal_raw, hshare)
    away_raw = team_goal_probability(goal_raw, ashare)

    reasons_goal = [
        f"pressure {pressure['total']}/100",
        f"xG {side_val(metrics,'xg','home'):.2f}-{side_val(metrics,'xg','away'):.2f}" if metrics["xg"]["present"] else "xG missing",
        f"shots {side_val(metrics,'shots','home'):.0f}-{side_val(metrics,'shots','away'):.0f}" if metrics["shots"]["present"] else "shots missing",
        f"SOT {side_val(metrics,'shots_on_target','home'):.0f}-{side_val(metrics,'shots_on_target','away'):.0f}" if metrics["shots_on_target"]["present"] else "SOT missing",
        f"box touches {side_val(metrics,'touches_in_box','home'):.0f}-{side_val(metrics,'touches_in_box','away'):.0f}" if metrics["touches_in_box"]["present"] else "box touches missing",
    ]

    signals: List[Dict[str, Any]] = []

    # Goal before FT
    gp, gr, blocked = apply_guards(goal_raw, minute, "GOAL_BEFORE_FULLTIME", q, metrics, live)
    signals.append(signal_obj(
        "GOAL_BEFORE_FULLTIME", "At least one more goal", goal_raw, gp, q,
        reasons_goal, gr, blocked=blocked
    ))

    # O/U current + 0.5
    over_line = score["total"] + 0.5
    op, ogr, blocked = apply_guards(goal_raw, minute, "OVER_UNDER", q, metrics, live)
    signals.append(signal_obj(
        "OVER_UNDER", f"Over {over_line:.1f}", goal_raw, op, q,
        reasons_goal, ogr, blocked=blocked
    ))
    under_raw = 100 - goal_raw
    up, ugr, blocked = apply_guards(under_raw, minute, "UNDER", q, metrics, live)
    signals.append(signal_obj(
        "OVER_UNDER", f"Under {over_line:.1f}", under_raw, up, q,
        [f"current total {score['total']}", f"minute {minute}", f"another-goal raw {goal_raw:.1f}%"],
        ugr, blocked=blocked
    ))

    # team goals
    for side, share, raw in [("home", hshare, home_raw), ("away", ashare, away_raw)]:
        team_name = live["home"] if side == "home" else live["away"]
        p, guards, blocked = apply_guards(raw, minute, "TEAM_GOAL", q, metrics, live)
        signals.append(signal_obj(
            "TEAM_GOAL", f"{team_name} to score", raw, p, q,
            [
                f"{side} share {share:.0f}%",
                f"{side} xG {side_val(metrics,'xg',side):.2f}" if metrics["xg"]["present"] else f"{side} xG missing",
                f"{side} shots {side_val(metrics,'shots',side):.0f}" if metrics["shots"]["present"] else f"{side} shots missing",
                f"{side} SOT {side_val(metrics,'shots_on_target',side):.0f}" if metrics["shots_on_target"]["present"] else f"{side} SOT missing",
            ],
            guards,
            blocked=blocked
        ))

    # next windows
    for window in (5, 10):
        raw = next_window_probability(goal_raw, minute, window)
        p, guards, blocked = apply_guards(raw, minute, f"GOAL_NEXT_{window}", q, metrics, live)
        signals.append(signal_obj(
            f"GOAL_NEXT_{window}", f"Goal in next {window} minutes", raw, p, q,
            reasons_goal, guards, risk="high", blocked=blocked
        ))

    # first half goal only if applicable
    if minute < 45:
        remain_to_ht = max(1, 47 - minute)
        raw = next_window_probability(goal_raw, minute, remain_to_ht)
        p, guards, blocked = apply_guards(raw, minute, "GOAL_BEFORE_HALFTIME", q, metrics, live)
        signals.append(signal_obj(
            "GOAL_BEFORE_HALFTIME", "At least one goal before half-time", raw, p, q,
            reasons_goal + [f"{remain_to_ht} min model window to HT"],
            guards, blocked=blocked
        ))

    # BTTS YES only meaningful if neither/both state considered
    if score["home"] > 0 and score["away"] > 0:
        btts_raw = 100.0
    elif score["home"] > 0:
        btts_raw = away_raw
    elif score["away"] > 0:
        btts_raw = home_raw
    else:
        # both still need to score: strong dependence penalty
        btts_raw = (home_raw / 100.0) * (away_raw / 100.0) * 100.0 * 0.82

    p, guards, blocked = apply_guards(btts_raw, minute, "BTTS", q, metrics, live)
    signals.append(signal_obj(
        "BTTS", "Both teams to score - YES", btts_raw, p, q,
        [f"score {score['home']}-{score['away']}", f"team goal raw {home_raw:.1f}%/{away_raw:.1f}%"],
        guards, blocked=blocked
    ))

    # Sort strongest first
    signals.sort(key=lambda x: float(x["probability"]), reverse=True)
    return signals


# -------------------------
# Consensus & correlation
# -------------------------

def consensus_report(
    live: Dict[str, Any],
    metrics: Dict[str, Any],
    q: Dict[str, Any],
    pressure: Dict[str, float],
    lineups: Dict[str, Any],
    odds: Dict[str, Any],
    h2h: Dict[str, Any],
    top_signal: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    votes = []
    if q["score"] >= 75:
        votes.append(("data_quality", 1, "high-quality live data"))
    else:
        votes.append(("data_quality", 0, f"quality {q['score']}/100"))

    if pressure["total"] >= 65:
        votes.append(("pressure", 1, f"pressure {pressure['total']}"))
    elif pressure["total"] <= 40:
        votes.append(("pressure", -1, f"pressure {pressure['total']}"))
    else:
        votes.append(("pressure", 0, f"pressure {pressure['total']}"))

    if metrics["xg"]["present"]:
        minute = max(15, int(live["minute"]))
        pace = total_val(metrics, "xg") * 90 / minute
        votes.append(("xg_pace", 1 if pace >= 2.4 else (-1 if pace < 1.3 else 0), f"xG pace {pace:.2f}/90"))
    else:
        votes.append(("xg_pace", 0, "xG missing"))

    if metrics["shots_on_target"]["present"]:
        minute = max(15, int(live["minute"]))
        pace = total_val(metrics, "shots_on_target") * 90 / minute
        votes.append(("sot_pace", 1 if pace >= 8 else (-1 if pace < 4 else 0), f"SOT pace {pace:.1f}/90"))

    if lineups.get("available"):
        mod = (
            float(lineups.get("home", {}).get("formation_attack_modifier") or 0)
            + float(lineups.get("away", {}).get("formation_attack_modifier") or 0)
        )
        votes.append(("lineup_shape", 1 if mod >= 4 else (-1 if mod <= -3 else 0), f"formation modifier {mod:+.1f}"))
    else:
        votes.append(("lineup_shape", 0, "lineups unavailable"))

    votes.append(("odds", 0, "odds captured" if odds.get("available") else "odds unavailable"))
    votes.append(("h2h", 0, "context only" if h2h.get("available") else "H2H unavailable"))

    positive = sum(1 for _, v, _ in votes if v > 0)
    negative = sum(1 for _, v, _ in votes if v < 0)
    confidence = clamp(50 + positive * 8 - negative * 10)
    return {
        "positive_votes": positive,
        "negative_votes": negative,
        "total_modules": len(votes),
        "consensus_score": round1(confidence),
        "modules": [{"module": n, "vote": v, "note": note} for n, v, note in votes],
        "top_signal": top_signal.get("selection") if top_signal else None,
    }

def correlation_groups(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Prevent treating correlated markets as independent recommendations.
    groups: Dict[str, List[Dict[str, Any]]] = {
        "future_goal_cluster": [],
        "team_goal_cluster": [],
        "btts_cluster": [],
        "under_cluster": [],
    }
    for s in signals:
        m = s["market"]
        sel = s["selection"]
        if m in {"GOAL_BEFORE_FULLTIME", "GOAL_BEFORE_HALFTIME", "GOAL_NEXT_5", "GOAL_NEXT_10"} or (m == "OVER_UNDER" and sel.startswith("Over")):
            groups["future_goal_cluster"].append(s)
        elif m == "TEAM_GOAL":
            groups["team_goal_cluster"].append(s)
        elif m == "BTTS":
            groups["btts_cluster"].append(s)
        elif m == "OVER_UNDER" and sel.startswith("Under"):
            groups["under_cluster"].append(s)

    result = []
    for name, ss in groups.items():
        if not ss:
            continue
        best = max(ss, key=lambda x: float(x["probability"]))
        result.append({
            "group": name,
            "signals_count": len(ss),
            "best_market": best["market"],
            "best_selection": best["selection"],
            "best_probability": best["probability"],
            "decision": best["decision"],
            "note": "Signals inside this group are correlated and must not be counted as independent evidence.",
        })
    return result


# -------------------------
# Match analyzer
# -------------------------

async def analyze_match_internal(match_id: str, exact_live: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    client = ZylaClient()
    api_calls = 0
    try:
        if exact_live is None:
            live_resp = await client.live()
            api_calls += 1
            matches = flatten_live(live_resp.get("data"))
            live = pick_live(matches, match_id)
            live_http = live_resp["status"]
        else:
            live = exact_live
            live_http = 200

        if not live:
            return {
                "source": "football-reactor-v4",
                "version": VERSION,
                "match_id": match_id,
                "status": "NOT_LIVE_OR_NOT_FOUND",
                "api_calls": api_calls,
            }

        # Parallel deep fetch
        details_r, summary_r, stats_r, lineups_r, player_r, odds_r, h2h_r = await asyncio.gather(
            client.details(match_id),
            client.summary(match_id),
            client.stats(match_id),
            client.lineups(match_id),
            client.player_stats(match_id),
            client.odds(match_id),
            client.h2h(match_id),
        )
        api_calls += 7

        parsed = parse_metrics(stats_r.get("data"), live)
        metrics = parsed["metrics"]
        q = quality_guard(metrics)
        lineups = parse_lineups(lineups_r.get("data"))
        players = parse_player_stats(player_r.get("data"))
        odds = compact_odds(odds_r.get("data"))
        h2h = h2h_context(h2h_r.get("data"))
        pressure = pressure_score(metrics, int(live["minute"]))
        signals = build_signals(live, metrics, q, pressure, lineups)

        for s in signals:
            s["new_or_changed"] = is_new_or_changed(match_id, s)

        strong = [s for s in signals if s["decision"] == "ENTER"]
        top3 = signals[:3]
        corr = correlation_groups(signals)
        consensus = consensus_report(live, metrics, q, pressure, lineups, odds, h2h, top3[0] if top3 else None)

        # Details score sync, fail closed on a detectable conflict
        details_score = None
        details_data = details_r.get("data")
        if isinstance(details_data, dict):
            sc = details_data.get("scores") or details_data.get("score") or {}
            if isinstance(sc, dict):
                dh = sc.get("home") if sc.get("home") is not None else sc.get("home_total")
                da = sc.get("away") if sc.get("away") is not None else sc.get("away_total")
                if dh is not None and da is not None:
                    details_score = score_obj(dh, da)

        score_sync_ok = True
        if details_score is not None:
            score_sync_ok = (
                details_score["home"] == live["score"]["home"]
                and details_score["away"] == live["score"]["away"]
            )

        if not score_sync_ok:
            for s in signals:
                if s["decision"] == "ENTER":
                    s["decision"] = "WAIT"
                    s["signal"] = "🟡"
                    s["guard_reasons"].append("Score Sync Guard blocked ENTER")
            strong = []

        quality_blocked_signals = [
            s for s in signals
            if s.get("data_quality_blocked")
        ]

        report = {
            "source": "football-reactor-v4",
            "version": VERSION,
            "model_type": MODEL_TYPE,
            "match_id": match_id,
            "status": "OK",
            "match": {
                "home": live["home"],
                "away": live["away"],
                "tournament": live.get("tournament"),
                "minute": live["minute"],
                "stage": live.get("stage"),
                "score": live["score"],
                "red_cards": live.get("red_cards"),
            },
            "parser_ok": len(parsed["rows"]) > 0,
            "score_sync": {
                "ok": score_sync_ok,
                "live_score": live["score"],
                "details_score": details_score,
                "authoritative_source": "fresh_live_list",
            },
            "data_quality": q,
            "availability": {k: bool(v.get("present")) for k, v in metrics.items() if isinstance(v, dict) and "present" in v},
            "metrics": metrics,
            "pressure": pressure,
            "lineups": lineups,
            "player_stats": players,
            "odds_snapshot": odds,
            "h2h_context": h2h,
            "consensus": consensus,
            "correlation_groups": corr,
            "top_candidate": top3[0] if top3 else None,
            "top_3_signals": top3,
            "strong_signals": strong,
            "quality_blocked_signals": quality_blocked_signals,
            "all_signals": signals,
            "diagnostic": {
                "fresh_live_http": live_http,
                "details_http": details_r["status"],
                "summary_http": summary_r["status"],
                "stats_http": stats_r["status"],
                "stats_rows_count": len(parsed["rows"]),
                "lineups_http": lineups_r["status"],
                "player_stats_http": player_r["status"],
                "odds_http": odds_r["status"],
                "h2h_http": h2h_r["status"],
                "estimated_api_calls": api_calls,
            },
        }

        for s in strong:
            log_event({
                "type": "strong_signal",
                "match_id": match_id,
                "home": live["home"],
                "away": live["away"],
                "minute": live["minute"],
                "score": live["score"],
                "signal": s,
                "data_quality": q,
                "consensus": consensus,
            })

        return report
    finally:
        await client.close()


# -------------------------
# Scanner
# -------------------------

def cheap_rank(m: Dict[str, Any]) -> float:
    minute = int(m.get("minute") or 0)
    total = int(m.get("score", {}).get("total") or 0)
    score = 0.0
    if 8 <= minute <= 88:
        score += 20
    if 20 <= minute <= 80:
        score += 15
    if total <= 3:
        score += 8
    if m.get("red_cards", {}).get("home") or m.get("red_cards", {}).get("away"):
        score += 5
    # Favor matches with valid ids and live state
    if m.get("match_id"):
        score += 10
    if m.get("is_in_progress"):
        score += 10
    return score

@mcp.tool()
async def hidden_signal_status() -> Dict[str, Any]:
    return {
        "service": "Hidden Signal Live",
        "version": VERSION,
        "model_type": MODEL_TYPE,
        "zyla_key_configured": bool(ZYLA_API_KEY),
        "strong_signal_threshold": STRONG_THRESHOLD,
        "guards": [
            "score_sync_guard",
            "data_quality_guard",
            "early_match_guard",
            "small_sample_guard",
            "red_card_uncertainty_guard",
            "correlation_guard",
        ],
        "modules": [
            "live_core",
            "stats_parser",
            "lineups",
            "player_stats",
            "odds_snapshot",
            "h2h_context",
            "pressure_engine",
            "market_engine",
            "consensus_engine",
            "signal_journal",
        ],
        "notes": [
            "Probabilities are heuristic ranking estimates, not calibrated true probabilities.",
            "H2H has low weight and cannot create ENTER by itself.",
            "Correlated signals are grouped and should not be counted as independent.",
            "Signal journal is local unless SIGNAL_LOG_PATH points to persistent storage.",
        ],
    }

@mcp.tool()
async def get_zyla_live_matches() -> Dict[str, Any]:
    client = ZylaClient()
    try:
        r = await client.live()
        matches = flatten_live(r.get("data"))
        return {
            "source": "football-reactor-v4",
            "version": VERSION,
            "http_status": r["status"],
            "live_matches_found": len(matches),
            "matches": [
                {
                    "match_id": m["match_id"],
                    "tournament": m["tournament"],
                    "home": m["home"],
                    "away": m["away"],
                    "minute": m["minute"],
                    "stage": m["stage"],
                    "score": m["score"],
                    "red_cards": m["red_cards"],
                    "live_odds_1x2": m["live_odds_1x2"],
                }
                for m in matches
            ],
        }
    finally:
        await client.close()

@mcp.tool()
async def analyze_zyla_match(match_id: str) -> Dict[str, Any]:
    return await analyze_match_internal(match_id)

@mcp.tool()
async def reactor_match_report(match_id: str) -> Dict[str, Any]:
    """Full V4 reactor report for one live football match."""
    return await analyze_match_internal(match_id)

@mcp.tool()
async def scan_zyla_live(prefilter_limit: int = 12, deep_limit: int = 6) -> Dict[str, Any]:
    client = ZylaClient()
    try:
        live_r = await client.live()
        matches = flatten_live(live_r.get("data"))
    finally:
        await client.close()

    ranked = sorted(matches, key=cheap_rank, reverse=True)
    pre = ranked[:max(1, int(prefilter_limit))]
    deep = pre[:max(1, int(deep_limit))]

    reports: List[Dict[str, Any]] = []
    # Analyze sequentially to avoid hammering provider; stable for free tiers.
    for m in deep:
        try:
            rep = await analyze_match_internal(str(m["match_id"]), exact_live=m)
            reports.append(rep)
        except Exception as e:
            reports.append({
                "status": "ERROR",
                "match_id": m.get("match_id"),
                "home": m.get("home"),
                "away": m.get("away"),
                "error": repr(e),
            })

    parser_failures = []
    score_conflicts = []
    quality_blocked = []
    analysis_errors = []
    strong_signals = []
    top_candidates = []

    for rep in reports:
        if rep.get("status") != "OK":
            analysis_errors.append(rep)
            continue
        if not rep.get("parser_ok"):
            parser_failures.append({
                "match_id": rep.get("match_id"),
                "match": rep.get("match"),
                "diagnostic": rep.get("diagnostic"),
                "missing": rep.get("data_quality", {}).get("missing"),
            })
        if not rep.get("score_sync", {}).get("ok", True):
            score_conflicts.append({
                "match_id": rep.get("match_id"),
                "match": rep.get("match"),
                "score_sync": rep.get("score_sync"),
            })
        for s in rep.get("quality_blocked_signals", []):
            quality_blocked.append({
                "match_id": rep.get("match_id"),
                "home": rep.get("match", {}).get("home"),
                "away": rep.get("match", {}).get("away"),
                "minute": rep.get("match", {}).get("minute"),
                "score": rep.get("match", {}).get("score"),
                "signal": s,
                "data_quality": rep.get("data_quality"),
            })
        for s in rep.get("strong_signals", []):
            if s.get("new_or_changed"):
                strong_signals.append({
                    "match_id": rep.get("match_id"),
                    "home": rep.get("match", {}).get("home"),
                    "away": rep.get("match", {}).get("away"),
                    "minute": rep.get("match", {}).get("minute"),
                    "score": rep.get("match", {}).get("score"),
                    "data_quality": rep.get("data_quality"),
                    "consensus": rep.get("consensus"),
                    **s,
                })
        if rep.get("top_candidate"):
            top_candidates.append({
                "match_id": rep.get("match_id"),
                "home": rep.get("match", {}).get("home"),
                "away": rep.get("match", {}).get("away"),
                "minute": rep.get("match", {}).get("minute"),
                "score": rep.get("match", {}).get("score"),
                "data_quality": rep.get("data_quality"),
                "consensus": rep.get("consensus"),
                **rep["top_candidate"],
            })

    top_candidates.sort(key=lambda x: float(x.get("probability") or 0), reverse=True)
    strong_signals.sort(key=lambda x: float(x.get("probability") or 0), reverse=True)

    return {
        "source": "football-reactor-v4",
        "version": VERSION,
        "status": "OK" if live_r.get("ok") else "LIVE_FETCH_ERROR",
        "live_http_status": live_r.get("status"),
        "live_matches_found": len(matches),
        "prefiltered_matches": len(pre),
        "cheap_analyzed": len(pre),
        "fully_analyzed": len([r for r in reports if r.get("status") == "OK"]),
        "parser_failures": len(parser_failures),
        "score_conflicts": len(score_conflicts),
        "quality_blocked": len(quality_blocked),
        "parser_failure_details": parser_failures,
        "analysis_errors": analysis_errors,
        "quality_blocked_signals": quality_blocked,
        "strong_signal_threshold": STRONG_THRESHOLD,
        "strong_signals": strong_signals,
        "top_candidates": top_candidates[:8],
        "estimated_api_calls_this_scan": 1 + len(deep) * 7,
        "reactor_notes": [
            "Fresh live list is authoritative for score/minute.",
            "Lineups and match player stats are fetched for deep-analyzed matches.",
            "Early-match and small-sample guards prevent 90%+ inflation in the opening minutes.",
            "Correlated markets are grouped inside each match report.",
            "Probabilities are not calibrated until enough logged outcomes are collected.",
        ],
    }

@mcp.tool()
async def get_signal_log(limit: int = 50) -> Dict[str, Any]:
    limit = max(1, min(int(limit), 500))
    rows = []
    try:
        with open(SIGNAL_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    return {
        "version": VERSION,
        "path": SIGNAL_LOG_PATH,
        "persistent_storage_warning": "On Render free instances /tmp is ephemeral unless persistent storage/external DB is configured.",
        "count_returned": min(limit, len(rows)),
        "events": rows[-limit:],
    }


# -------------------------
# Health
# -------------------------

@mcp.custom_route("/", methods=["GET", "HEAD"])
async def health_root(request: Request) -> Response:
    return JSONResponse({
        "status": "ok",
        "service": "Hidden Signal Live",
        "version": VERSION,
        "model_type": MODEL_TYPE,
    })

if __name__ == "__main__":
    if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http")
