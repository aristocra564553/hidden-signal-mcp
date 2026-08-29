
import os
import json
import math
import re
import time
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

VERSION = "V4.6-WARM-REACTOR"
MODEL_TYPE = "heuristic-v4.3-anti-stale-next-goal-not-calibrated"

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
    "team_details": (23875, "get+team+details"),
    "team_results": (23876, "get+team+results"),
    "team_fixtures": (23877, "get+team+fixtures"),
    "team_squad": (23878, "get+team+squad"),
    "tournament_details": (23881, "get+tournament+details"),
    "tournament_form": (23885, "get+tournament+standings+form"),
    "tournament_over_under": (23887, "get+tournament+standings+over+under"),
}

STRONG_THRESHOLD = 75.0
VISIBLE_SIGNAL_MIN = 65.0
EARLY_MINUTE_HARD = 8
EARLY_MINUTE_SOFT = 15
STALE_SECONDS = 90
GOAL_COOLDOWN_SECONDS = 150
FINAL_MINUTE_DRIFT_MAX = 4

mcp = FastMCP(
    "Hidden Signal Live",
    transport_security=TransportSecuritySettings(
        allowed_hosts=[
            "hidden-signal-mcp.onrender.com",
            "hidden-signal-mcp.onrender.com:*",
            "localhost",
            "localhost:*",
            "127.0.0.1",
            "127.0.0.1:*",
        ],
    ),
)


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
_MATCH_STATE_MEMORY: Dict[str, Dict[str, Any]] = {}
_GOAL_COOLDOWN_UNTIL: Dict[str, float] = {}

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


def register_live_state(match: Dict[str, Any]) -> Dict[str, Any]:
    """Track score changes between scans and activate a short post-goal cooldown."""
    match_id = str(match.get("match_id") or "")
    now_ts = time.time()
    score = match.get("score") or {"home": 0, "away": 0, "total": 0}
    minute = int(match.get("minute") or 0)
    previous = _MATCH_STATE_MEMORY.get(match_id)
    goal_detected = False
    if previous:
        prev_score = previous.get("score") or {}
        if (
            int(score.get("home", 0)) != int(prev_score.get("home", 0))
            or int(score.get("away", 0)) != int(prev_score.get("away", 0))
        ):
            goal_detected = True
            _GOAL_COOLDOWN_UNTIL[match_id] = now_ts + GOAL_COOLDOWN_SECONDS
            log_event({
                "type": "goal_state_change",
                "match_id": match_id,
                "previous_score": prev_score,
                "new_score": score,
                "minute": minute,
                "cooldown_seconds": GOAL_COOLDOWN_SECONDS,
            })
    _MATCH_STATE_MEMORY[match_id] = {
        "score": dict(score),
        "minute": minute,
        "seen_at": now_ts,
    }
    cooldown_until = float(_GOAL_COOLDOWN_UNTIL.get(match_id, 0.0))
    remaining = max(0, int(round(cooldown_until - now_ts)))
    if remaining <= 0:
        _GOAL_COOLDOWN_UNTIL.pop(match_id, None)
    return {
        "goal_detected": goal_detected,
        "cooldown_active": remaining > 0,
        "cooldown_remaining_seconds": remaining,
    }


def score_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return (
        int((a or {}).get("home", 0)) == int((b or {}).get("home", 0))
        and int((a or {}).get("away", 0)) == int((b or {}).get("away", 0))
    )


def final_freshness_check(
    analyzed_match: Dict[str, Any],
    fresh_match: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Fail closed if the match changed while analysis was running."""
    if fresh_match is None:
        return {
            "ok": False,
            "reason": "MATCH_NOT_IN_FINAL_LIVE_LIST",
            "score_changed": False,
            "minute_drift": None,
            "cooldown_active": False,
        }

    state = register_live_state(fresh_match)
    analyzed_score = analyzed_match.get("score") or {}
    fresh_score = fresh_match.get("score") or {}
    analyzed_minute = int(analyzed_match.get("minute") or 0)
    fresh_minute = int(fresh_match.get("minute") or 0)
    minute_drift = fresh_minute - analyzed_minute

    if not score_equal(analyzed_score, fresh_score):
        _GOAL_COOLDOWN_UNTIL[str(fresh_match.get("match_id") or "")] = time.time() + GOAL_COOLDOWN_SECONDS
        return {
            "ok": False,
            "reason": "STALE_AFTER_GOAL",
            "score_changed": True,
            "analyzed_score": analyzed_score,
            "fresh_score": fresh_score,
            "analyzed_minute": analyzed_minute,
            "fresh_minute": fresh_minute,
            "minute_drift": minute_drift,
            "cooldown_active": True,
            "cooldown_remaining_seconds": GOAL_COOLDOWN_SECONDS,
        }

    if not fresh_match.get("is_in_progress"):
        return {
            "ok": False,
            "reason": "MATCH_NOT_IN_PROGRESS",
            "score_changed": False,
            "minute_drift": minute_drift,
            "cooldown_active": state["cooldown_active"],
            "cooldown_remaining_seconds": state["cooldown_remaining_seconds"],
        }

    if not fresh_match.get("minute_valid", True):
        return {
            "ok": False,
            "reason": "INVALID_FINAL_MINUTE",
            "score_changed": False,
            "minute_drift": minute_drift,
            "cooldown_active": state["cooldown_active"],
            "cooldown_remaining_seconds": state["cooldown_remaining_seconds"],
        }

    if minute_drift > FINAL_MINUTE_DRIFT_MAX:
        return {
            "ok": False,
            "reason": "FINAL_SNAPSHOT_TOO_OLD",
            "score_changed": False,
            "minute_drift": minute_drift,
            "cooldown_active": state["cooldown_active"],
            "cooldown_remaining_seconds": state["cooldown_remaining_seconds"],
        }

    if state["cooldown_active"]:
        return {
            "ok": False,
            "reason": "POST_GOAL_COOLDOWN",
            "score_changed": False,
            "minute_drift": minute_drift,
            "cooldown_active": True,
            "cooldown_remaining_seconds": state["cooldown_remaining_seconds"],
        }

    return {
        "ok": True,
        "reason": "FRESH",
        "score_changed": False,
        "minute_drift": minute_drift,
        "fresh_score": fresh_score,
        "fresh_minute": fresh_minute,
        "cooldown_active": False,
        "cooldown_remaining_seconds": 0,
    }


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

    async def team_details(self, team_url: str) -> Dict[str, Any]:
        return await self.get("team_details", {"team_url": team_url})

    async def team_results(self, team_id: str, page: int = 1) -> Dict[str, Any]:
        return await self.get("team_results", {"team_id": team_id, "page": max(1, int(page))})

    async def team_fixtures(self, team_id: str, page: int = 1) -> Dict[str, Any]:
        return await self.get("team_fixtures", {"team_id": team_id, "page": max(1, int(page))})

    async def team_squad(self, team_url: str) -> Dict[str, Any]:
        return await self.get("team_squad", {"team_url": team_url})

    async def tournament_details(self, tournament_stage_id: str) -> Dict[str, Any]:
        return await self.get("tournament_details", {"tournament_stage_id": tournament_stage_id})

    async def tournament_form(self, tournament_id: str, tournament_stage_id: str, type_: str = "overall") -> Dict[str, Any]:
        return await self.get("tournament_form", {
            "tournament_id": tournament_id,
            "tournament_stage_id": tournament_stage_id,
            "type": type_,
        })

    async def tournament_over_under(self, tournament_id: str, tournament_stage_id: str, type_: str = "overall", sub_type: str = "2.5") -> Dict[str, Any]:
        return await self.get("tournament_over_under", {
            "tournament_id": tournament_id,
            "tournament_stage_id": tournament_stage_id,
            "type": type_,
            "sub_type": sub_type,
        })


# -------------------------
# Live parsing
# -------------------------

def parse_live_minute(live_time: Any, stage: str) -> Tuple[int, bool, str]:
    """
    Parse FlashScore-style live minute safely.
    Handles values like 67, "67'", "90+9'", "Half Time".
    Returns: minute, valid, diagnostic.
    """
    stage_l = str(stage or "").lower().strip()

    if isinstance(live_time, (int, float)):
        minute = int(live_time)
    elif isinstance(live_time, str):
        text = live_time.strip()
        # Prefer explicit added-time form: 45+2 / 90+9.
        m = re.search(r"(?<!\d)(\d{1,3})\s*\+\s*(\d{1,2})(?!\d)", text)
        if m:
            minute = int(m.group(1)) + int(m.group(2))
        else:
            m = re.search(r"(?<!\d)(\d{1,3})(?!\d)", text)
            minute = int(m.group(1)) if m else None
    else:
        minute = None

    if minute is None:
        if "half time" in stage_l:
            return 45, True, "stage_half_time"
        if "finished" in stage_l:
            return 90, True, "stage_finished"
        return 0, False, "minute_missing"

    # Football can legitimately go beyond 90 in added time / extra time,
    # but values such as 909 are parser corruption and must fail closed.
    if minute < 0 or minute > 130:
        return 0, False, f"minute_out_of_range:{minute}"

    return minute, True, "ok"

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
            stage = str(st.get("stage") or "")
            minute, minute_valid, minute_diag = parse_live_minute(live_time, stage)

            out.append({
                "match_id": m.get("match_id"),
                "tournament_id": tournament_id,
                "tournament": tournament,
                "home": ht.get("name"),
                "away": at.get("name"),
                "home_team_id": ht.get("team_id"),
                "away_team_id": at.get("team_id"),
                "minute": minute,
                "minute_valid": minute_valid,
                "minute_diagnostic": minute_diag,
                "raw_live_time": live_time,
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
    shots_total = total_val(metrics, "shots")
    sot_total = total_val(metrics, "shots_on_target")
    robust_basic_sample = basic_ok and shots_total >= 10 and sot_total >= 3

    # Two ways to become strong-eligible:
    # 1) advanced attacking metric is present, or
    # 2) basic shot sample is already large enough to be informative.
    strong_eligible = basic_ok and score >= 45 and (advanced_count >= 1 or robust_basic_sample)
    level = "HIGH" if strong_eligible and score >= 75 else ("MEDIUM" if basic_ok and score >= 45 else "LOW")
    return {
        "score": score,
        "level": level,
        "basic_ok": basic_ok,
        "advanced_count": advanced_count,
        "robust_basic_sample": robust_basic_sample,
        "eligibility_path": "advanced" if (basic_ok and advanced_count >= 1 and score >= 45) else ("robust_basic" if robust_basic_sample and score >= 45 else "blocked"),
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

def live_impulse_probability(
    base_goal_p: float,
    live: Dict[str, Any],
    metrics: Dict[str, Any],
    pressure: Dict[str, float],
) -> float:
    """
    Aggressive live-reading layer. It does NOT bypass early/small-sample/red-card guards.
    It intentionally ignores missing-field penalties, so incomplete feeds can still
    surface as WATCH candidates without becoming SAFE ENTER automatically.
    """
    p = float(base_goal_p)
    minute = max(1, int(live.get("minute") or 1))

    # High pressure should matter in live play, but avoid runaway inflation.
    p += max(0.0, pressure["total"] - 62.0) * 0.34

    if metrics["xg"]["present"]:
        xg_total = total_val(metrics, "xg")
        xg_pace = xg_total * 90.0 / max(20, minute)
        p += max(0.0, xg_pace - 2.0) * 2.2

    if metrics["shots_on_target"]["present"]:
        sot = total_val(metrics, "shots_on_target")
        sot_pace = sot * 90.0 / max(20, minute)
        p += max(0.0, sot_pace - 6.0) * 0.8

    if metrics["shots_in_box"]["present"]:
        sib = total_val(metrics, "shots_in_box")
        p += min(5.0, sib * 0.30)

    if metrics["touches_in_box"]["present"]:
        tib = total_val(metrics, "touches_in_box")
        p += min(5.0, tib * 0.10)

    # Classic Live: pressure is evidence, never a near-guarantee.
    # The final DQ cap is applied in signal_obj as well.
    return clamp(p, 2, 90)


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
        p -= 3
        reasons.append("medium data quality penalty")
    elif q["level"] == "LOW":
        p -= 9
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

def apply_live_guards(
    raw_p: float,
    minute: int,
    market_class: str,
    metrics: Dict[str, Any],
    live: Dict[str, Any],
) -> Tuple[float, List[str]]:
    p = float(raw_p)
    reasons: List[str] = []
    p, r = early_match_guard(p, minute, market_class, metrics)
    reasons += r
    p, r = small_sample_guard(p, minute, metrics)
    reasons += r
    p, r = red_card_guard(p, live, market_class)
    reasons += r
    return round1(clamp(p)), reasons


def live_decision_for(p: float, q: Dict[str, Any], market: str = "") -> str:
    # LIVE is intentionally stricter than V4.2: completeness alone is not enough.
    dq = float(q.get("score") or 0)
    advanced = int(q.get("advanced_count") or 0)
    robust = bool(q.get("robust_basic_sample"))
    next_goal_market = market.startswith("NEXT_GOAL") or market.startswith("GOAL_NEXT_")

    if next_goal_market:
        if p >= 80 and q.get("basic_ok") and dq >= 65 and (advanced >= 1 or robust):
            return "LIVE_ENTER"
        if p >= 72 and q.get("basic_ok") and dq >= 50:
            return "WATCH"
        return "PASS"

    if p >= 80 and q.get("basic_ok") and dq >= 60 and (advanced >= 1 or robust):
        return "LIVE_ENTER"
    if p >= 72 and q.get("basic_ok") and dq >= 45:
        return "WATCH"
    return "PASS"


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
    live_p: Optional[float] = None,
    live_guard_reasons: Optional[List[str]] = None,
) -> Dict[str, Any]:
    decision = decision_for(guarded_p, q)
    if live_p is None:
        live_p = raw_p
    # Classic Live quality ceiling: incomplete feeds cannot produce 90-100% LIVE claims.
    dq_score = float(q.get("score") or 0)
    if dq_score < 50:
        live_p = min(float(live_p), 69.0)
    elif dq_score < 65:
        live_p = min(float(live_p), 74.0)
    elif dq_score < 75:
        live_p = min(float(live_p), 82.0)
    else:
        live_p = min(float(live_p), 90.0)

    live_decision = live_decision_for(float(live_p), q, market)
    return {
        "market": market,
        "selection": selection,
        "raw_probability": round1(raw_p),
        "probability": round1(guarded_p),
        "safe_probability": round1(guarded_p),
        "safe_decision": decision,
        "live_probability": round1(live_p),
        "live_decision": live_decision,
        "signal": "🟢" if decision == "ENTER" else ("🟡" if decision == "WAIT" else "🔴"),
        "decision": decision,
        "risk": risk,
        "reasons": reasons,
        "guard_reasons": guard_reasons,
        "live_guard_reasons": live_guard_reasons or [],
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

    def dual_values(raw: float, market_class: str) -> Tuple[float, List[str], bool, float, List[str]]:
        safe_p, safe_reasons, blocked = apply_guards(raw, minute, market_class, q, metrics, live)
        live_base = raw
        if market_class in {"GOAL_BEFORE_FULLTIME", "OVER_UNDER", "TEAM_GOAL"}:
            # Live impulse is strongest for broad goal/team-goal markets.
            if market_class == "GOAL_BEFORE_FULLTIME" or market_class == "OVER_UNDER":
                live_base = live_impulse_probability(raw, live, metrics, pressure)
            else:
                # Team-goal markets inherit only part of the global live impulse.
                global_live = live_impulse_probability(goal_raw, live, metrics, pressure)
                live_base = clamp(raw + max(0.0, global_live - goal_raw) * 0.65)
        live_p, live_reasons = apply_live_guards(live_base, minute, market_class, metrics, live)
        return safe_p, safe_reasons, blocked, live_p, live_reasons

    # Goal before FT
    gp, gr, blocked, glive, glr = dual_values(goal_raw, "GOAL_BEFORE_FULLTIME")
    signals.append(signal_obj(
        "GOAL_BEFORE_FULLTIME", "At least one more goal", goal_raw, gp, q,
        reasons_goal, gr, blocked=blocked, live_p=glive, live_guard_reasons=glr
    ))

    # O/U current + 0.5
    over_line = score["total"] + 0.5
    op, ogr, blocked, olive, olr = dual_values(goal_raw, "OVER_UNDER")
    signals.append(signal_obj(
        "OVER_UNDER", f"Over {over_line:.1f}", goal_raw, op, q,
        reasons_goal, ogr, blocked=blocked, live_p=olive, live_guard_reasons=olr
    ))
    under_raw = 100 - goal_raw
    up, ugr, blocked, ulive, ulr = dual_values(under_raw, "UNDER")
    signals.append(signal_obj(
        "OVER_UNDER", f"Under {over_line:.1f}", under_raw, up, q,
        [f"current total {score['total']}", f"minute {minute}", f"another-goal raw {goal_raw:.1f}%"],
        ugr, blocked=blocked, live_p=ulive, live_guard_reasons=ulr
    ))

    # team goals
    for side, share, raw in [("home", hshare, home_raw), ("away", ashare, away_raw)]:
        team_name = live["home"] if side == "home" else live["away"]
        p, guards, blocked, tlive, tlr = dual_values(raw, "TEAM_GOAL")
        signals.append(signal_obj(
            "TEAM_GOAL", f"{team_name} to score another goal" if score[side] > 0 else f"{team_name} to score", raw, p, q,
            [
                f"{side} share {share:.0f}%",
                f"{side} xG {side_val(metrics,'xg',side):.2f}" if metrics["xg"]["present"] else f"{side} xG missing",
                f"{side} shots {side_val(metrics,'shots',side):.0f}" if metrics["shots"]["present"] else f"{side} shots missing",
                f"{side} SOT {side_val(metrics,'shots_on_target',side):.0f}" if metrics["shots_on_target"]["present"] else f"{side} SOT missing",
            ],
            guards,
            blocked=blocked, live_p=tlive, live_guard_reasons=tlr
        ))

    # nearest-goal windows. 3' is ultra-fast; 5'/10' are progressively broader.
    for window in (3, 5, 10):
        raw = next_window_probability(goal_raw, minute, window)
        p, guards, blocked, wlive, wlr = dual_values(raw, f"GOAL_NEXT_{window}")
        signals.append(signal_obj(
            f"GOAL_NEXT_{window}", f"Any goal in next {window} minutes", raw, p, q,
            reasons_goal + [f"nearest-goal window {window}m"], guards,
            risk="high" if window <= 5 else "medium",
            blocked=blocked, live_p=wlive, live_guard_reasons=wlr
        ))

        # Team-specific next-goal estimate is unconditional: it includes the chance
        # that there is no goal in the window, so it cannot exceed ANY-GOAL probability.
        for side, share in (("home", hshare), ("away", ashare)):
            team_name = live["home"] if side == "home" else live["away"]
            team_raw = clamp(raw * (share / 100.0) * 1.08, 1, raw)
            tp, tgr, tblocked, tlive, tlr = dual_values(team_raw, f"NEXT_GOAL_TEAM_{window}")
            signals.append(signal_obj(
                f"NEXT_GOAL_TEAM_{window}",
                f"{team_name} next goal within {window} minutes",
                team_raw, tp, q,
                [
                    f"any-goal window raw {raw:.1f}%",
                    f"{team_name} attacking share {share:.0f}%",
                    f"pressure {pressure[side]}/100",
                ],
                tgr,
                risk="high",
                blocked=tblocked,
                live_p=min(tlive, wlive),
                live_guard_reasons=tlr,
            ))

    # first half goal only if applicable
    if minute < 45:
        remain_to_ht = max(1, 47 - minute)
        raw = next_window_probability(goal_raw, minute, remain_to_ht)
        p, guards, blocked, hlive, hlr = dual_values(raw, "GOAL_BEFORE_HALFTIME")
        signals.append(signal_obj(
            "GOAL_BEFORE_HALFTIME", "At least one goal before half-time", raw, p, q,
            reasons_goal + [f"{remain_to_ht} min model window to HT"],
            guards, blocked=blocked, live_p=hlive, live_guard_reasons=hlr
        ))

    # BTTS YES must never be emitted after it is already settled.
    if not (score["home"] > 0 and score["away"] > 0):
        if score["home"] > 0:
            btts_raw = away_raw
        elif score["away"] > 0:
            btts_raw = home_raw
        else:
            # both still need to score: strong dependence penalty
            btts_raw = (home_raw / 100.0) * (away_raw / 100.0) * 100.0 * 0.82

        p, guards, blocked, blive, blr = dual_values(btts_raw, "BTTS")
        signals.append(signal_obj(
            "BTTS", "Both teams to score - YES", btts_raw, p, q,
            [f"score {score['home']}-{score['away']}", f"team goal raw {home_raw:.1f}%/{away_raw:.1f}%"],
            guards, blocked=blocked, live_p=blive, live_guard_reasons=blr
        ))

    # Classic Live phase guard.
    # Half-time is a pause, not "goal now". Finished/not-started phases are never actionable.
    stage_l = str(live.get("stage") or "").lower()
    is_half_time = ("half time" in stage_l) or ("halftime" in stage_l)
    is_dead_phase = any(x in stage_l for x in ("finished", "after extra time", "penalties", "not started", "cancelled", "postponed"))

    for s in signals:
        market = str(s.get("market") or "")
        nearest = market.startswith("GOAL_NEXT_") or market.startswith("NEXT_GOAL_TEAM_")
        if is_half_time and nearest:
            s["safe_decision"] = "WAIT"
            s["decision"] = "WAIT"
            s["live_decision"] = "PASS"
            s["signal"] = "🟡"
            s.setdefault("guard_reasons", []).append("HALF_TIME_BLOCK: wait for second half to start")
            s.setdefault("live_guard_reasons", []).append("HALF_TIME_BLOCK")
        if is_dead_phase:
            s["safe_decision"] = "SKIP"
            s["decision"] = "SKIP"
            s["live_decision"] = "PASS"
            s["signal"] = "🔴"
            s.setdefault("guard_reasons", []).append("NON_ACTIONABLE_MATCH_PHASE")

    # Sort strongest first
    signals.sort(key=lambda x: float(x["probability"]), reverse=True)
    return signals


def nearest_goal_assessment(signals: List[Dict[str, Any]], live: Dict[str, Any], q: Dict[str, Any], pressure: Dict[str, float]) -> Dict[str, Any]:
    windows: Dict[str, Any] = {}
    for w in (3, 5, 10):
        any_sig = next((s for s in signals if s.get("market") == f"GOAL_NEXT_{w}"), None)
        team_sigs = [s for s in signals if s.get("market") == f"NEXT_GOAL_TEAM_{w}"]
        team_sigs.sort(key=lambda x: float(x.get("live_probability") or 0), reverse=True)
        windows[str(w)] = {"any_goal": any_sig, "best_team": team_sigs[0] if team_sigs else None}

    home_p = float(pressure.get("home") or 0)
    away_p = float(pressure.get("away") or 0)
    diff = home_p - away_p
    if diff >= 10:
        likely_side = live.get("home")
        side_confidence = min(85.0, 50.0 + abs(diff) * 0.45)
    elif diff <= -10:
        likely_side = live.get("away")
        side_confidence = min(85.0, 50.0 + abs(diff) * 0.45)
    else:
        likely_side = "unclear"
        side_confidence = 50.0

    stage_l = str(live.get("stage") or "").lower()
    half_time = ("half time" in stage_l) or ("halftime" in stage_l)

    # Main Classic Live candidates are ONLY "any goal in next 5/10".
    # Team direction is shown separately and never masquerades as a timed-goal probability.
    actionable = [
        s for s in signals
        if s.get("market") in {"GOAL_NEXT_5", "GOAL_NEXT_10"}
        and (s.get("safe_decision") == "ENTER" or s.get("live_decision") == "LIVE_ENTER")
    ]
    actionable.sort(
        key=lambda x: (
            1 if x.get("safe_decision") == "ENTER" else 0,
            float(x.get("safe_probability") or 0),
            float(x.get("live_probability") or 0),
        ),
        reverse=True,
    )

    return {
        "minute": live.get("minute"),
        "score": live.get("score"),
        "stage": live.get("stage"),
        "likely_next_goal_side": likely_side,
        "next_goal_side_confidence": round1(side_confidence),
        "data_quality_score": q.get("score"),
        "data_quality_level": q.get("level"),
        "windows": windows,
        "best_nearest_goal_signal": None if half_time else (actionable[0] if actionable else None),
        "classic_live_enter": bool(actionable) and not half_time,
        "policy": "Main list: only fresh GOAL_NEXT_5/10 ENTER. Team direction is separate. HT/WAIT/SKIP never appear as a bet.",
    }


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



def parse_team_history(data: Any, team_id: str, limit: int = 10) -> Dict[str, Any]:
    """Compact recent-results context. Historical data is confirmation only."""
    matches: List[Dict[str, Any]] = []
    if isinstance(data, list):
        blocks = data
    elif isinstance(data, dict):
        blocks = data.get("data") if isinstance(data.get("data"), list) else [data]
    else:
        blocks = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        rows = block.get("matches")
        if isinstance(rows, list):
            matches.extend([m for m in rows if isinstance(m, dict)])
    matches = sorted(matches, key=lambda m: int(safe_float(m.get("timestamp")) or 0), reverse=True)[:max(1, limit)]
    gf = ga = btts = over25 = wins = draws = losses = 0
    used = 0
    for m in matches:
        ht = m.get("home_team") or {}; at = m.get("away_team") or {}; sc = m.get("scores") or {}
        h = safe_float(sc.get("home")); a = safe_float(sc.get("away"))
        if h is None or a is None:
            continue
        is_home = str(ht.get("team_id")) == str(team_id)
        is_away = str(at.get("team_id")) == str(team_id)
        if not (is_home or is_away):
            continue
        team_g = int(h if is_home else a); opp_g = int(a if is_home else h)
        gf += team_g; ga += opp_g; used += 1
        btts += int(team_g > 0 and opp_g > 0)
        over25 += int(team_g + opp_g >= 3)
        if team_g > opp_g: wins += 1
        elif team_g == opp_g: draws += 1
        else: losses += 1
    if not used:
        return {"available": False, "matches": 0}
    return {
        "available": True, "matches": used,
        "goals_for_avg": round(gf / used, 2),
        "goals_against_avg": round(ga / used, 2),
        "total_goals_avg": round((gf + ga) / used, 2),
        "btts_rate": round(100 * btts / used, 1),
        "over_2_5_rate": round(100 * over25 / used, 1),
        "wins": wins, "draws": draws, "losses": losses,
    }

def historical_confirmation(home_hist: Dict[str, Any], away_hist: Dict[str, Any]) -> Dict[str, Any]:
    if not home_hist.get("available") or not away_hist.get("available"):
        return {"available": False, "weight": "LOW", "note": "History incomplete; no model override."}
    goal_env = (float(home_hist.get("total_goals_avg", 0)) + float(away_hist.get("total_goals_avg", 0))) / 2
    btts = (float(home_hist.get("btts_rate", 0)) + float(away_hist.get("btts_rate", 0))) / 2
    over25 = (float(home_hist.get("over_2_5_rate", 0)) + float(away_hist.get("over_2_5_rate", 0))) / 2
    return {
        "available": True, "weight": "LOW",
        "goal_environment_avg": round(goal_env, 2),
        "btts_blend_rate": round(btts, 1),
        "over_2_5_blend_rate": round(over25, 1),
        "goal_context": "HIGH" if goal_env >= 3.0 else "MEDIUM" if goal_env >= 2.4 else "LOW",
        "note": "Historical confirmation has low weight and cannot create SAFE ENTER by itself.",
    }

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
                "source": "football-reactor-v4.6",
                "version": VERSION,
                "match_id": match_id,
                "status": "NOT_LIVE_OR_NOT_FOUND",
                "api_calls": api_calls,
            }

        if not live.get("minute_valid", True):
            return {
                "source": "football-reactor-v4.6",
                "version": VERSION,
                "match_id": match_id,
                "status": "INVALID_LIVE_MINUTE",
                "match": {
                    "home": live.get("home"),
                    "away": live.get("away"),
                    "minute": live.get("minute"),
                    "raw_live_time": live.get("raw_live_time"),
                    "minute_diagnostic": live.get("minute_diagnostic"),
                    "score": live.get("score"),
                },
                "api_calls": api_calls,
                "reason": "Live minute failed sanity validation; analysis blocked fail-closed.",
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
        nearest_goal = nearest_goal_assessment(signals, live, q, pressure)

        # Historical context is fetched only for meaningful candidates, keeping quota under control.
        top_pre_context = signals[0] if signals else None
        history_context = {"available": False, "reason": "not_requested_for_weak_candidate"}
        history_http = {"home": None, "away": None}
        if top_pre_context and float(top_pre_context.get("probability") or 0) >= 60.0:
            home_id = live.get("home_team_id")
            away_id = live.get("away_team_id")
            if home_id and away_id:
                home_hist_r, away_hist_r = await asyncio.gather(
                    client.team_results(str(home_id), 1),
                    client.team_results(str(away_id), 1),
                )
                api_calls += 2
                history_http = {"home": home_hist_r.get("status"), "away": away_hist_r.get("status")}
                home_hist = parse_team_history(home_hist_r.get("data"), str(home_id))
                away_hist = parse_team_history(away_hist_r.get("data"), str(away_id))
                history_context = {
                    "home": home_hist,
                    "away": away_hist,
                    "confirmation": historical_confirmation(home_hist, away_hist),
                }

        for s in signals:
            s["new_or_changed"] = is_new_or_changed(match_id, s)

        strong = [s for s in signals if s["decision"] == "ENTER"]
        live_watch = [s for s in signals if s.get("live_decision") in {"LIVE_ENTER", "WATCH"}]
        live_watch.sort(key=lambda x: float(x.get("live_probability") or 0), reverse=True)
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
            "source": "football-reactor-v4.6",
            "version": VERSION,
            "model_type": MODEL_TYPE,
            "match_id": match_id,
            "status": "OK",
            "match": {
                "home": live["home"],
                "away": live["away"],
                "tournament": live.get("tournament"),
                "minute": live["minute"],
                "minute_valid": live.get("minute_valid", True),
                "minute_diagnostic": live.get("minute_diagnostic"),
                "raw_live_time": live.get("raw_live_time"),
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
            "historical_context": history_context,
            "consensus": consensus,
            "correlation_groups": corr,
            "nearest_goal": nearest_goal,
            "top_candidate": top3[0] if top3 else None,
            "top_3_signals": top3,
            "strong_signals": strong,
            "live_watchlist": live_watch[:5],
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
                "team_history_http": history_http,
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
        "visible_signal_min": VISIBLE_SIGNAL_MIN,
        "guards": [
            "score_sync_guard",
            "data_quality_guard",
            "early_match_guard",
            "small_sample_guard",
            "red_card_uncertainty_guard",
            "correlation_guard",
            "final_freshness_guard",
            "post_goal_cooldown_guard",
            "settled_market_guard",
            "next_goal_quality_guard",
        ],
        "modules": [
            "live_core",
            "stats_parser",
            "lineups",
            "player_stats",
            "odds_snapshot",
            "h2h_context",
            "team_recent_results_context",
            "team_fixtures_endpoint",
            "team_details_endpoint",
            "team_squad_endpoint",
            "tournament_form_endpoint",
            "tournament_over_under_endpoint",
            "pressure_engine",
            "market_engine",
            "dual_safe_live_engine",
            "consensus_engine",
            "signal_journal",
            "nearest_goal_engine",
            "anti_stale_final_refresh",
        ],
        "notes": [
            "Probabilities are heuristic ranking estimates, not calibrated true probabilities.",
            "H2H and historical team context have low weight and cannot create ENTER by themselves.",
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
            "source": "football-reactor-v4.6",
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
                    "minute_valid": m.get("minute_valid", True),
                    "minute_diagnostic": m.get("minute_diagnostic"),
                    "raw_live_time": m.get("raw_live_time"),
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
    """Full V4.3 reactor report for one live football match."""
    return await analyze_match_internal(match_id)

@mcp.tool()
async def scan_zyla_live(prefilter_limit: int = 12, deep_limit: int = 6) -> Dict[str, Any]:
    client = ZylaClient()
    try:
        live_r = await client.live()
        matches = flatten_live(live_r.get("data"))
    finally:
        await client.close()

    invalid_minute_matches = [
        m for m in matches if not m.get("minute_valid", True)
    ]
    valid_matches = [
        m for m in matches if m.get("minute_valid", True)
    ]
    ranked = sorted(valid_matches, key=cheap_rank, reverse=True)
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

    # Final live refresh: no ENTER/WATCH leaves the scanner without a last score/minute check.
    final_client = ZylaClient()
    try:
        final_live_r = await final_client.live()
        final_matches = flatten_live(final_live_r.get("data"))
    finally:
        await final_client.close()
    final_map = {str(m.get("match_id")): m for m in final_matches if m.get("match_id")}

    stale_blocked_matches = []
    cooldown_blocked_matches = []
    for rep in reports:
        if rep.get("status") != "OK":
            continue
        match_id = str(rep.get("match_id") or "")
        freshness = final_freshness_check(rep.get("match") or {}, final_map.get(match_id))
        rep["final_freshness"] = freshness
        if not freshness.get("ok"):
            reason = freshness.get("reason")
            rep["strong_signals"] = []
            rep["live_watchlist"] = []
            rep["top_candidate"] = None
            if isinstance(rep.get("nearest_goal"), dict):
                rep["nearest_goal"]["best_nearest_goal_signal"] = None
                rep["nearest_goal"]["blocked_by_freshness"] = reason
            if reason in {"STALE_AFTER_GOAL", "FINAL_SNAPSHOT_TOO_OLD", "MATCH_NOT_IN_FINAL_LIVE_LIST", "MATCH_NOT_IN_PROGRESS", "INVALID_FINAL_MINUTE"}:
                stale_blocked_matches.append({
                    "match_id": match_id,
                    "home": rep.get("match", {}).get("home"),
                    "away": rep.get("match", {}).get("away"),
                    "reason": reason,
                    "freshness": freshness,
                })
            if reason == "POST_GOAL_COOLDOWN":
                cooldown_blocked_matches.append({
                    "match_id": match_id,
                    "home": rep.get("match", {}).get("home"),
                    "away": rep.get("match", {}).get("away"),
                    "reason": reason,
                    "freshness": freshness,
                })

    parser_failures = []
    score_conflicts = []
    quality_blocked = []
    analysis_errors = []
    strong_signals = []
    live_watchlist = []
    nearest_goal_signals = []
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
        for s in rep.get("live_watchlist", []):
            live_watchlist.append({
                "match_id": rep.get("match_id"),
                "home": rep.get("match", {}).get("home"),
                "away": rep.get("match", {}).get("away"),
                "minute": rep.get("match", {}).get("minute"),
                "score": rep.get("match", {}).get("score"),
                "data_quality": rep.get("data_quality"),
                "pressure": rep.get("pressure"),
                **s,
            })
        ng = rep.get("nearest_goal") or {}
        ng_best = ng.get("best_nearest_goal_signal") if isinstance(ng, dict) else None
        if (ng_best and rep.get("final_freshness", {}).get("ok")
                and (ng_best.get("safe_decision") == "ENTER" or ng_best.get("live_decision") == "LIVE_ENTER")):
            nearest_goal_signals.append({
                "match_id": rep.get("match_id"),
                "home": rep.get("match", {}).get("home"),
                "away": rep.get("match", {}).get("away"),
                "minute": rep.get("final_freshness", {}).get("fresh_minute", rep.get("match", {}).get("minute")),
                "score": rep.get("final_freshness", {}).get("fresh_score", rep.get("match", {}).get("score")),
                "data_quality": rep.get("data_quality"),
                "pressure": rep.get("pressure"),
                "likely_next_goal_side": ng.get("likely_next_goal_side"),
                **ng_best,
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
    live_watchlist.sort(key=lambda x: float(x.get("live_probability") or 0), reverse=True)
    nearest_goal_signals.sort(
        key=lambda x: (
            1 if x.get("safe_decision") == "ENTER" else 0,
            float(x.get("safe_probability") or 0),
            float(x.get("live_probability") or 0),
        ),
        reverse=True,
    )

    return {
        "source": "football-reactor-v4.6",
        "version": VERSION,
        "status": "OK" if live_r.get("ok") else "LIVE_FETCH_ERROR",
        "live_http_status": live_r.get("status"),
        "live_matches_found": len(matches),
        "valid_live_matches": len(valid_matches),
        "invalid_minute_matches": [
            {
                "match_id": m.get("match_id"),
                "home": m.get("home"),
                "away": m.get("away"),
                "raw_live_time": m.get("raw_live_time"),
                "minute_diagnostic": m.get("minute_diagnostic"),
                "score": m.get("score"),
            }
            for m in invalid_minute_matches
        ],
        "prefiltered_matches": len(pre),
        "cheap_analyzed": len(pre),
        "fully_analyzed": len([r for r in reports if r.get("status") == "OK"]),
        "parser_failures": len(parser_failures),
        "score_conflicts": len(score_conflicts),
        "quality_blocked": len(quality_blocked),
        "parser_failure_details": parser_failures,
        "analysis_errors": analysis_errors,
        "quality_blocked_signals": quality_blocked,
        "final_live_http_status": final_live_r.get("status"),
        "stale_blocked": len(stale_blocked_matches),
        "stale_blocked_matches": stale_blocked_matches,
        "post_goal_cooldown_blocked": len(cooldown_blocked_matches),
        "post_goal_cooldown_matches": cooldown_blocked_matches,
        "strong_signal_threshold": STRONG_THRESHOLD,
        "visible_signal_min": VISIBLE_SIGNAL_MIN,
        "strong_signals": strong_signals,
        "nearest_goal_signals": nearest_goal_signals[:10],
        "best_nearest_goal_signal": nearest_goal_signals[0] if nearest_goal_signals else None,
        "live_watchlist": live_watchlist[:10],
        "top_candidates": top_candidates[:8],
        "estimated_api_calls_this_scan": 2 + sum(int(r.get("diagnostic", {}).get("estimated_api_calls") or 0) for r in reports if isinstance(r, dict)),
        "classic_live_policy": {
            "main_output": "nearest goal only",
            "show_markets": ["GOAL_NEXT_5", "GOAL_NEXT_10"],
            "hide_from_main": ["WAIT", "SKIP", "TEAM_GOAL", "BTTS", "HALF_TIME"],
            "safe_enter_threshold": 75,
            "live_enter_threshold": 80,
            "final_freshness_required": True,
            "post_goal_cooldown_seconds": POST_GOAL_COOLDOWN_SECONDS,
        },
        "reactor_notes": [
            "Fresh live list is authoritative for score/minute.",
            "Lineups and match player stats are fetched for deep-analyzed matches.",
            "Early-match and small-sample guards prevent 90%+ inflation in the opening minutes.",
            "SAFE probability applies data-quality gating; LIVE probability surfaces pressure-driven WATCH candidates without bypassing sample guards.",
            "Invalid live minutes are rejected before deep analysis.",
            "Correlated markets are grouped inside each match report.",
            "A final live refresh blocks stale signals after a goal or material minute drift.",
            "A short post-goal cooldown blocks signals while provider stats catch up.",
            "Already-settled markets such as BTTS YES after both teams scored are never emitted.",
            "Nearest-goal engine ranks ANY GOAL and likely scoring side for 3/5/10-minute windows.",
            "Probabilities are not calibrated until enough logged outcomes are collected.",
        ],
    }

@mcp.tool()
async def scan_next_goal(prefilter_limit: int = 12, deep_limit: int = 6) -> Dict[str, Any]:
    """Run the full scanner and return only fresh nearest-goal candidates."""
    result = await scan_zyla_live(prefilter_limit=prefilter_limit, deep_limit=deep_limit)
    return {
        "source": result.get("source"),
        "version": result.get("version"),
        "status": result.get("status"),
        "live_matches_found": result.get("live_matches_found"),
        "fully_analyzed": result.get("fully_analyzed"),
        "final_live_http_status": result.get("final_live_http_status"),
        "stale_blocked": result.get("stale_blocked"),
        "post_goal_cooldown_blocked": result.get("post_goal_cooldown_blocked"),
        "parser_failures": result.get("parser_failures"),
        "quality_blocked": result.get("quality_blocked"),
        "best_nearest_goal_signal": result.get("best_nearest_goal_signal"),
        "nearest_goal_signals": result.get("nearest_goal_signals", []),
        "policy": {
            "safe_enter": "SAFE >=75 with data-quality eligibility",
            "live_enter": "LIVE >=80 with DQ>=65, basic live stats and sufficient sample",
            "freshness": "final score/minute rechecked immediately before output",
            "cooldown": f"{GOAL_COOLDOWN_SECONDS}s after detected score change",
        },
    }


@mcp.tool()
async def get_zyla_team_context(team_id: str, page: int = 1) -> Dict[str, Any]:
    """Recent results and upcoming fixtures for a team. Uses team_id from live/results data."""
    client = ZylaClient()
    try:
        results_r, fixtures_r = await asyncio.gather(
            client.team_results(team_id, page),
            client.team_fixtures(team_id, page),
        )
        return {
            "source": "football-reactor-v4.6", "version": VERSION, "team_id": team_id,
            "recent_results_http": results_r.get("status"),
            "fixtures_http": fixtures_r.get("status"),
            "recent_summary": parse_team_history(results_r.get("data"), team_id),
            "recent_results": results_r.get("data"),
            "fixtures": fixtures_r.get("data"),
        }
    finally:
        await client.close()

@mcp.tool()
async def get_zyla_team_profile(team_url: str, include_squad: bool = False) -> Dict[str, Any]:
    """Team details and optionally squad. team_url must come from Zyla, do not invent it."""
    client = ZylaClient()
    try:
        if include_squad:
            details_r, squad_r = await asyncio.gather(client.team_details(team_url), client.team_squad(team_url))
        else:
            details_r = await client.team_details(team_url); squad_r = {"status": None, "data": None}
        return {
            "source": "football-reactor-v4.6", "version": VERSION, "team_url": team_url,
            "details_http": details_r.get("status"), "details": details_r.get("data"),
            "squad_http": squad_r.get("status"), "squad": squad_r.get("data"),
        }
    finally:
        await client.close()

@mcp.tool()
async def get_zyla_tournament_context(tournament_id: str, tournament_stage_id: str, type_: str = "overall", sub_type: str = "2.5") -> Dict[str, Any]:
    """Tournament details, current form and over/under table. IDs must come from Zyla."""
    client = ZylaClient()
    try:
        details_r, form_r, ou_r = await asyncio.gather(
            client.tournament_details(tournament_stage_id),
            client.tournament_form(tournament_id, tournament_stage_id, type_),
            client.tournament_over_under(tournament_id, tournament_stage_id, type_, sub_type),
        )
        return {
            "source": "football-reactor-v4.6", "version": VERSION,
            "tournament_id": tournament_id, "tournament_stage_id": tournament_stage_id,
            "details_http": details_r.get("status"), "details": details_r.get("data"),
            "form_http": form_r.get("status"), "form": form_r.get("data"),
            "over_under_http": ou_r.get("status"), "over_under": ou_r.get("data"),
        }
    finally:
        await client.close()



# -------------------------
# V4.6 Warm Reactor — clear human-facing structure
# -------------------------

def _phase_bucket(match: Dict[str, Any]) -> str:
    minute = int(match.get("minute") or 0)
    stage = str(match.get("stage") or "").lower()
    if "half time" in stage or "halftime" in stage:
        return "HALF_TIME"
    return "FIRST_HALF" if minute <= 45 else "SECOND_HALF"


def _metric_pair(metrics: Dict[str, Any], key: str):
    m = metrics.get(key) or {}
    if not isinstance(m, dict) or not m.get("present"):
        return None
    try:
        return float(m.get("home") or 0), float(m.get("away") or 0)
    except Exception:
        return None


def _human_pressure(value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        return "неизвестно"
    if v >= 80:
        return "очень высокое"
    if v >= 60:
        return "высокое"
    if v >= 40:
        return "среднее"
    return "низкое"


def _effective_probability(sig: Optional[Dict[str, Any]]) -> float:
    if not sig:
        return 0.0
    safe = float(sig.get("safe_probability") or sig.get("probability") or 0)
    live = float(sig.get("live_probability") or 0)
    # LIVE is useful, but it cannot fully replace SAFE.
    return round1(clamp(safe * 0.70 + live * 0.30 if live else safe, 0, 99))


def _signal_band(p: float) -> Dict[str, str]:
    p = float(p or 0)
    if p >= 80:
        return {"level": "STRONG", "emoji": "🟢", "label": "сильный"}
    if p >= 75:
        return {"level": "GOOD", "emoji": "🟢", "label": "хороший"}
    if p >= 70:
        return {"level": "MODERATE", "emoji": "🟡", "label": "умеренный"}
    if p >= 65:
        return {"level": "EARLY", "emoji": "🟠", "label": "ранний / рискованный"}
    return {"level": "HIDDEN", "emoji": "🔴", "label": "слабый"}


def _signal_view(sig: Optional[Dict[str, Any]], q: Dict[str, Any]):
    if not sig or sig.get("data_quality_blocked"):
        return None
    p = _effective_probability(sig)
    if p < VISIBLE_SIGNAL_MIN:
        return None
    band = _signal_band(p)
    return {
        "market": sig.get("market"),
        "selection": sig.get("selection"),
        "probability": p,
        "safe": sig.get("safe_probability"),
        "live": sig.get("live_probability"),
        "strength": band["level"],
        "strength_label": band["label"],
        "emoji": band["emoji"],
        "decision": "ENTER" if p >= 75 and q.get("strong_eligible") else "SIGNAL",
        "risk": "средний" if p >= 75 else ("повышенный" if p >= 70 else "высокий"),
    }


def _best_signal(signals: List[Dict[str, Any]], markets: set):
    xs = [s for s in signals if s.get("market") in markets and not s.get("data_quality_blocked")]
    xs.sort(key=_effective_probability, reverse=True)
    return xs[0] if xs else None


def _reasoning(metrics: Dict[str, Any], match: Dict[str, Any], pressure: Dict[str, Any], likely_side: str) -> Dict[str, Any]:
    facts = []
    for key, label, integer in [
        ("xg", "xG", False),
        ("shots", "удары", True),
        ("shots_on_target", "в створ", True),
        ("touches_in_box", "касания в штрафной", True),
        ("shots_in_box", "удары из штрафной", True),
        ("big_chances", "большие моменты", True),
        ("corners", "угловые", True),
    ]:
        pair = _metric_pair(metrics, key)
        if pair:
            a, b = pair
            facts.append(f"{label} {int(a)}–{int(b)}" if integer else f"{label} {round1(a)}–{round1(b)}")

    total_pressure = float(pressure.get("total") or 0)
    strengthens = []
    sot = _metric_pair(metrics, "shots_on_target")
    xg = _metric_pair(metrics, "xg")
    box = _metric_pair(metrics, "touches_in_box")
    if sot and sum(sot) >= 3:
        strengthens.append("есть реальные удары в створ")
    if xg and sum(xg) >= 0.8:
        strengthens.append("xG подтверждает моменты")
    if box and sum(box) >= 12:
        strengthens.append("команды регулярно доходят до штрафной")
    if total_pressure >= 60:
        strengthens.append("темп и давление высокие")
    if not strengthens:
        strengthens.append("для усиления нужен рост xG/створа/касаний в штрафной")

    breaks = []
    minute = int(match.get("minute") or 0)
    if minute <= 15:
        breaks.append("матч ещё ранний, выборка маленькая")
    if total_pressure < 40:
        breaks.append("темп может просесть")
    rc = match.get("red_cards") or {}
    if rc.get("home") or rc.get("away"):
        breaks.append("удаление меняет рисунок игры")
    if not breaks:
        breaks.append("резкое падение темпа, длинная пауза или тактическое закрытие матча")

    return {
        "facts": facts[:6],
        "who_is_closer": likely_side if likely_side and likely_side != "unclear" else "явного перевеса нет",
        "pressure": _human_pressure(total_pressure),
        "why": "; ".join(facts[:5]) if facts else f"давление {_human_pressure(total_pressure)}",
        "what_strengthens": "; ".join(strengthens[:3]),
        "what_can_break": "; ".join(breaks[:3]),
    }


def _outcome_estimate(match: Dict[str, Any], pressure: Dict[str, Any]) -> Dict[str, Any]:
    import math
    score = match.get("score") or {}
    h = int(score.get("home") or 0)
    a = int(score.get("away") or 0)
    minute = int(match.get("minute") or 0)
    remain = max(1, 95 - minute)

    score_edge = (h - a) * (1.15 + max(0, minute - 45) / 55.0)
    pressure_edge = (float(pressure.get("home") or 0) - float(pressure.get("away") or 0)) / 55.0
    time_factor = max(0.35, min(1.0, remain / 55.0))
    home_logit = score_edge + pressure_edge * time_factor
    away_logit = -score_edge - pressure_edge * time_factor
    draw_logit = 0.55 - abs(h - a) * 0.9 + (0.75 if h == a else 0) + (0.5 if minute >= 70 and h == a else 0)

    vals = [math.exp(home_logit), math.exp(draw_logit), math.exp(away_logit)]
    total = sum(vals) or 1.0
    probs = [v / total * 100 for v in vals]
    ph, px, pa = [round1(p) for p in probs]
    return {
        "P1": ph,
        "X": px,
        "P2": pa,
        "note": "наша эвристическая оценка исхода для разбора, не калиброванная вероятность",
    }


def structured_market_report(rep: Dict[str, Any]) -> Dict[str, Any]:
    match = rep.get("match") or {}
    metrics = rep.get("metrics") or {}
    pressure = rep.get("pressure") or {}
    q = rep.get("data_quality") or {}
    signals = rep.get("all_signals") or []
    nearest = rep.get("nearest_goal") or {}
    phase = _phase_bucket(match)

    first_half_sig = _best_signal(signals, {"GOAL_BEFORE_HALFTIME"}) if phase == "FIRST_HALF" else None
    second_half_sig = _best_signal(signals, {"GOAL_BEFORE_FULLTIME", "OVER_UNDER"}) if phase == "SECOND_HALF" else None
    btts_sig = _best_signal(signals, {"BTTS"})
    n5_sig = _best_signal(signals, {"GOAL_NEXT_5"})
    n10_sig = _best_signal(signals, {"GOAL_NEXT_10"})

    fh = _signal_view(first_half_sig, q)
    sh = _signal_view(second_half_sig, q)
    n5 = _signal_view(n5_sig, q)
    n10 = _signal_view(n10_sig, q)

    score = match.get("score") or {}
    if int(score.get("home") or 0) > 0 and int(score.get("away") or 0) > 0:
        btts = {"status": "ALREADY_WON", "text": "ОЗ уже сыграно"}
    else:
        btts = _signal_view(btts_sig, q)

    likely = nearest.get("likely_next_goal_side") or "unclear"
    reasoning = _reasoning(metrics, match, pressure, likely)

    options = []
    for title, view in [
        ("Гол в 1-м тайме", fh),
        ("Гол во 2-м тайме / до конца", sh),
        ("ОЗ — Да", btts if isinstance(btts, dict) and btts.get("status") != "ALREADY_WON" else None),
        ("Гол в ближайшие 5 минут", n5),
        ("Гол в ближайшие 10 минут", n10),
    ]:
        if isinstance(view, dict) and view.get("probability") is not None:
            options.append({"option": title, **view})
    options.sort(key=lambda x: float(x.get("probability") or 0), reverse=True)
    best = options[0] if options else None

    if phase == "HALF_TIME":
        decision = "Перерыв. Ближайший гол не берём до старта 2-го тайма."
    elif best:
        decision = f"{best['emoji']} Лучший вариант: {best['option']} — {best['probability']}%."
    else:
        decision = "⛔ Сигналов от 65% сейчас нет. Ждём."

    return {
        "match": f"{match.get('home')} — {match.get('away')}",
        "minute": match.get("minute"),
        "score": match.get("score"),
        "phase": phase,
        "FIRST_HALF": fh if phase == "FIRST_HALF" else ("завершён" if phase == "SECOND_HALF" else "перерыв"),
        "SECOND_HALF": sh if phase == "SECOND_HALF" else ("ждём начало 2-го тайма" if phase == "HALF_TIME" else None),
        "BTTS": btts,
        "NEAREST_GOAL": {
            "5_min": n5,
            "10_min": n10,
            "likely_team": likely,
            "likely_team_confidence": nearest.get("next_goal_side_confidence"),
        },
        "OUTCOMES_1X2": _outcome_estimate(match, pressure),
        "OUR_REASONING": reasoning,
        "BEST_OPTION": best,
        "decision": decision,
        "visible_signal_min": VISIBLE_SIGNAL_MIN,
        "has_visible_signal": bool(options),
    }


async def _final_refresh_for_report(rep: Dict[str, Any]) -> Dict[str, Any]:
    if rep.get("status") != "OK":
        return rep
    client = ZylaClient()
    try:
        live_r = await client.live()
        final_matches = flatten_live(live_r.get("data"))
    finally:
        await client.close()
    final_map = {str(m.get("match_id")): m for m in final_matches if m.get("match_id")}
    rep["final_freshness"] = final_freshness_check(
        rep.get("match") or {},
        final_map.get(str(rep.get("match_id") or ""))
    )
    return rep


@mcp.tool()
async def structured_live_report(match_id: str) -> Dict[str, Any]:
    """Readable one-match report: 1H / 2H / BTTS / nearest goal / 1X2 / our reasoning."""
    rep = await reactor_match_report(match_id)
    rep = await _final_refresh_for_report(rep)
    if rep.get("error") or rep.get("status") != "OK":
        return rep
    freshness = rep.get("final_freshness") or {}
    if not freshness.get("ok"):
        return {
            "source": "hidden-signal-v4.6",
            "version": VERSION,
            "blocked": True,
            "reason": freshness.get("reason"),
            "message": "Сигнал скрыт: финальная проверка лайва не пройдена.",
            "final_freshness": freshness,
        }
    return {
        "source": "hidden-signal-v4.6",
        "version": VERSION,
        "report": structured_market_report(rep),
        "technical": {
            "parser_ok": rep.get("parser_ok"),
            "score_sync": rep.get("score_sync"),
            "final_freshness": freshness,
            "data_quality": rep.get("data_quality"),
        },
    }


@mcp.tool()
async def scan_structured_live(limit: int = 10) -> Dict[str, Any]:
    """Main V4.6 scan: only 65-99% candidates, clearly split by market."""
    client = ZylaClient()
    try:
        live_r = await client.live()
        matches = flatten_live(live_r.get("data"))
    finally:
        await client.close()

    valid = []
    for m in matches:
        if not m.get("minute_valid", True):
            continue
        stage = str(m.get("stage") or "").lower()
        if any(x in stage for x in ("finished", "cancelled", "postponed", "not started")):
            continue
        valid.append(m)

    valid.sort(key=cheap_rank, reverse=True)
    deep = valid[:max(1, min(int(limit), 12))]

    reports = []
    for m in deep:
        try:
            reports.append(await analyze_match_internal(str(m["match_id"]), exact_live=m))
        except Exception as e:
            reports.append({"status": "ERROR", "match_id": m.get("match_id"), "error": repr(e)})

    final_client = ZylaClient()
    try:
        final_r = await final_client.live()
        final_matches = flatten_live(final_r.get("data"))
    finally:
        await final_client.close()
    final_map = {str(m.get("match_id")): m for m in final_matches if m.get("match_id")}

    sections = {"FIRST_HALF": [], "SECOND_HALF": [], "BTTS": [], "NEAREST_GOAL": []}
    freshness_blocked = []
    parser_failures = 0
    quality_blocked = 0

    for rep in reports:
        if rep.get("status") != "OK":
            continue
        if not rep.get("parser_ok"):
            parser_failures += 1

        freshness = final_freshness_check(rep.get("match") or {}, final_map.get(str(rep.get("match_id") or "")))
        if not freshness.get("ok"):
            freshness_blocked.append({
                "match_id": rep.get("match_id"),
                "match": f"{rep.get('match',{}).get('home')} — {rep.get('match',{}).get('away')}",
                "reason": freshness.get("reason"),
            })
            continue

        q = rep.get("data_quality") or {}
        if not q.get("basic_ok"):
            quality_blocked += 1
            continue

        view = structured_market_report(rep)
        common = {
            "match_id": rep.get("match_id"),
            "match": view.get("match"),
            "minute": view.get("minute"),
            "score": view.get("score"),
            "OUR_REASONING": view.get("OUR_REASONING"),
            "OUTCOMES_1X2": view.get("OUTCOMES_1X2"),
            "BEST_OPTION": view.get("BEST_OPTION"),
            "decision": view.get("decision"),
        }

        fh = view.get("FIRST_HALF")
        sh = view.get("SECOND_HALF")
        oz = view.get("BTTS")
        ng = view.get("NEAREST_GOAL") or {}

        if isinstance(fh, dict) and float(fh.get("probability") or 0) >= VISIBLE_SIGNAL_MIN:
            sections["FIRST_HALF"].append({**common, "signal": fh})
        if isinstance(sh, dict) and float(sh.get("probability") or 0) >= VISIBLE_SIGNAL_MIN:
            sections["SECOND_HALF"].append({**common, "signal": sh})
        if isinstance(oz, dict) and oz.get("status") != "ALREADY_WON" and float(oz.get("probability") or 0) >= VISIBLE_SIGNAL_MIN:
            sections["BTTS"].append({**common, "signal": oz})

        n5 = ng.get("5_min")
        n10 = ng.get("10_min")
        if ((isinstance(n5, dict) and float(n5.get("probability") or 0) >= VISIBLE_SIGNAL_MIN) or
            (isinstance(n10, dict) and float(n10.get("probability") or 0) >= VISIBLE_SIGNAL_MIN)):
            sections["NEAREST_GOAL"].append({
                **common,
                "goal_5_min": n5,
                "goal_10_min": n10,
                "likely_team": ng.get("likely_team"),
                "likely_team_confidence": ng.get("likely_team_confidence"),
            })

    def score_item(item):
        vals = []
        if isinstance(item.get("signal"), dict):
            vals.append(float(item["signal"].get("probability") or 0))
        for k in ("goal_5_min", "goal_10_min", "BEST_OPTION"):
            if isinstance(item.get(k), dict):
                vals.append(float(item[k].get("probability") or 0))
        return max(vals or [0])

    for arr in sections.values():
        arr.sort(key=score_item, reverse=True)

    total = sum(len(v) for v in sections.values())
    return {
        "source": "hidden-signal-v4.6",
        "version": VERSION,
        "model_type": MODEL_TYPE,
        "live_matches_found": len(matches),
        "deep_checked": len(reports),
        "visible_range": "65-99%",
        "FIRST_HALF_TOP": sections["FIRST_HALF"][:5],
        "sections": sections,
        "signals_found": total,
        "freshness_blocked": freshness_blocked,
        "parser_failures": parser_failures,
        "quality_blocked": quality_blocked,
        "message": "⛔ Сигналов от 65% сейчас нет. Ждём." if total == 0 else "Показаны сигналы 65-99%, отдельно по рынкам.",
        "display_policy": {
            "65-69": "🟠 ранний / рискованный",
            "70-74": "🟡 умеренный",
            "75-79": "🟢 хороший",
            "80-99": "🟢 сильный",
            "below_65": "не показывать",
            "sections": ["1-й тайм", "2-й тайм", "ОЗ", "ближайший гол"],
            "human_reasoning": True,
            "outcome_discussion": True,
            "final_freshness_required": True,
        },
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
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http")
