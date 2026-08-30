
import os
import json
import hashlib
import math
import re
import time
import asyncio
import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

VERSION = "V5.7.5m-DUAL-HALF-DEDICATED-BUDGET-FINAL"
MODEL_TYPE = "heuristic-v5.7.5m-dual-half-dedicated-budget-not-calibrated"

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
# Lightweight journal + V5.7.5f persistent state resolver
# -------------------------

def _v575f_probe_writable_dir(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        probe = os.path.join(path, ".hidden_signal_write_probe")
        with open(probe, "a", encoding="utf-8") as f:
            f.write("")
        try:
            os.remove(probe)
        except Exception:
            pass
        return True
    except Exception:
        return False


def _v575f_resolve_state_dir() -> Tuple[str, str, bool]:
    # Best option: explicitly mount a persistent Render disk and point this env
    # variable at it, e.g. HIDDEN_SIGNAL_STATE_DIR=/var/data/hidden_signal.
    explicit = (
        os.getenv("HIDDEN_SIGNAL_STATE_DIR", "").strip()
        or os.getenv("PERSISTENT_STORAGE_DIR", "").strip()
        or os.getenv("RENDER_DISK_PATH", "").strip()
    )
    candidates: List[Tuple[str, str, bool]] = []
    if explicit:
        candidates.append((explicit, "ENV_CONFIGURED", not explicit.startswith("/tmp")))

    # Common persistent-disk mount locations. Auto-detect them only when the
    # mount root already exists; never create /var/data or /data ourselves and
    # then incorrectly label an ordinary filesystem path as persistent.
    if os.path.isdir("/var/data"):
        candidates.append(("/var/data/hidden_signal", "AUTO_VAR_DATA", True))
    if os.path.isdir("/data"):
        candidates.append(("/data/hidden_signal", "AUTO_DATA", True))
    candidates.append(("/tmp/hidden_signal", "TMP_FALLBACK", False))

    seen = set()
    for path, mode, persistent in candidates:
        path = os.path.abspath(path)
        if path in seen:
            continue
        seen.add(path)
        if _v575f_probe_writable_dir(path):
            return path, mode, persistent

    # Extremely defensive last resort. Existing write helpers remain fail-soft.
    return "/tmp", "TMP_LAST_RESORT", False


def _v575f_configured_path(env_name: str, filename: str) -> str:
    raw = os.getenv(env_name, "").strip()
    if raw:
        try:
            parent = os.path.dirname(os.path.abspath(raw)) or "."
            if _v575f_probe_writable_dir(parent):
                return os.path.abspath(raw)
        except Exception:
            pass
    return os.path.join(STATE_STORAGE_DIR, filename)


STATE_STORAGE_DIR, STATE_STORAGE_MODE, STATE_STORAGE_PERSISTENT = _v575f_resolve_state_dir()
STATE_STORAGE_WARNING = (
    None
    if STATE_STORAGE_PERSISTENT
    else (
        "State is using temporary storage. Attach a persistent disk and set "
        "HIDDEN_SIGNAL_STATE_DIR (for example /var/data/hidden_signal) so scan history, "
        "adaptive stable_count, decision journal and self-learning survive restarts/redeploys."
    )
)

SIGNAL_LOG_PATH = _v575f_configured_path("SIGNAL_LOG_PATH", "signals.jsonl")
SCAN_STATE_PATH = _v575f_configured_path("SCAN_STATE_PATH", "scan_state.json")
SCAN_HISTORY_PATH = _v575f_configured_path("SCAN_HISTORY_PATH", "scan_history.json")
DECISION_JOURNAL_PATH = _v575f_configured_path("DECISION_JOURNAL_PATH", "decision_journal.json")
LEARNING_STATE_PATH = _v575f_configured_path("LEARNING_STATE_PATH", "learning_state.json")
FEED_GUARD_STATE_PATH = _v575f_configured_path("FEED_GUARD_STATE_PATH", "feed_guard_state.json")
DEEP_ROTATION_QUEUE_PATH = _v575f_configured_path("DEEP_ROTATION_QUEUE_PATH", "deep_rotation_queue.json")

def _v575j_state_storage_health() -> Dict[str, Any]:
    """Atomic write/read probe. Persistent=True only means a non-/tmp state path is configured/detected."""
    probe = os.path.join(STATE_STORAGE_DIR, ".v575j_state_health")
    token = f"{os.getpid()}-{time.time_ns()}"
    writable = False
    roundtrip_ok = False
    try:
        tmp = probe + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(token)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, probe)
        writable = True
        with open(probe, "r", encoding="utf-8") as f:
            roundtrip_ok = f.read() == token
    except Exception:
        writable = False
        roundtrip_ok = False
    finally:
        for p in (probe, probe + ".tmp"):
            try:
                os.remove(p)
            except Exception:
                pass
    return {
        "directory": STATE_STORAGE_DIR,
        "mode": STATE_STORAGE_MODE,
        "persistent": STATE_STORAGE_PERSISTENT,
        "writable": writable,
        "atomic_roundtrip_ok": roundtrip_ok,
        "restart_survival_expected": bool(STATE_STORAGE_PERSISTENT and writable and roundtrip_ok),
        "warning": STATE_STORAGE_WARNING,
    }

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


# ===== V5.7.3 QUOTA GUARD =====
PROVIDER_CACHE_TTL = float(os.environ.get("PROVIDER_CACHE_TTL", "15"))
PROVIDER_LIVE_CACHE_TTL = float(os.environ.get("PROVIDER_LIVE_CACHE_TTL", "8"))
PROVIDER_MIN_GAP = float(os.environ.get("PROVIDER_MIN_GAP", "0.45"))
PROVIDER_MAX_CALLS_PER_MINUTE = int(os.environ.get("PROVIDER_MAX_CALLS_PER_MINUTE", "24"))
PROVIDER_MAX_CALLS_PER_SCAN = int(os.environ.get("PROVIDER_MAX_CALLS_PER_SCAN", "14"))
PROVIDER_FINAL_SNAPSHOT_RESERVE = int(os.environ.get("PROVIDER_FINAL_SNAPSHOT_RESERVE", "1"))
PROVIDER_TARGETED_VERIFY_RESERVE = int(os.environ.get("PROVIDER_TARGETED_VERIFY_RESERVE", "1"))

# V5.7.5k: proactive budget planner. The balanced mode keeps the V5.7.5j
# rotation logic, while focus="first_half" spends every available normal
# provider call on 5'–43' first halves first and uses a stats-first strategy.
V575J_ENRICHMENT_RESERVE = max(0, int(os.environ.get("V575J_ENRICHMENT_RESERVE", "3")))
V575J_FIRST_HALF_DEEP_SHARE = max(0.0, min(0.80, float(os.environ.get("V575J_FIRST_HALF_DEEP_SHARE", "0.55"))))
V575J_ROTATION_KEEP_SECONDS = max(300, int(os.environ.get("V575J_ROTATION_KEEP_SECONDS", "1800")))
V575J_ROTATION_MAX = max(20, int(os.environ.get("V575J_ROTATION_MAX", "100")))
V575J_ROTATION_BOOST_PER_DEFER = max(1.0, float(os.environ.get("V575J_ROTATION_BOOST_PER_DEFER", "8")))
V575J_ROTATION_BOOST_CAP = max(8.0, float(os.environ.get("V575J_ROTATION_BOOST_CAP", "32")))
# V5.7.5l1: dedicated first-half window is intentionally FIXED.
# Do not let stale Render env vars from older 10-40 builds silently change it.
V575K_FIRST_HALF_START_MINUTE = 5
V575K_FIRST_HALF_END_MINUTE = 43
V575K_FIRST_HALF_ENRICH_THRESHOLD = max(65.0, min(90.0, float(os.environ.get("V575K_FIRST_HALF_ENRICH_THRESHOLD", "70"))))
V575K_FIRST_HALF_ENRICH_RESERVE = max(0, min(3, int(os.environ.get("V575K_FIRST_HALF_ENRICH_RESERVE", "0"))))
# V5.7.5m: fixed dedicated second-half window. 90 includes normal late regulation;
# explicit extra-time/penalty stages are rejected by the phase detector.
V575M_SECOND_HALF_START_MINUTE = 46
V575M_SECOND_HALF_END_MINUTE = 90
V575M_SECOND_HALF_ENRICH_THRESHOLD = max(65.0, min(90.0, float(os.environ.get("V575M_SECOND_HALF_ENRICH_THRESHOLD", "70"))))
V575M_SECOND_HALF_ENRICH_RESERVE = max(0, min(3, int(os.environ.get("V575M_SECOND_HALF_ENRICH_RESERVE", "0"))))
V575K_ROLLING_MINUTE_FINAL_RESERVE = max(1, int(os.environ.get("V575K_ROLLING_MINUTE_FINAL_RESERVE", "1")))
V575K_ROLLING_MINUTE_TARGETED_RESERVE = max(0, int(os.environ.get("V575K_ROLLING_MINUTE_TARGETED_RESERVE", "1")))
_SCAN_FOCUS = {"mode": "balanced"}

_PROVIDER_CACHE: Dict[str, Dict[str, Any]] = {}
_PROVIDER_CALL_TIMES: List[float] = []
_PROVIDER_LAST_CALL_AT = 0.0
_PROVIDER_LOCK = asyncio.Lock()
_SCAN_BUDGET = {"active": False, "used": 0, "limit": PROVIDER_MAX_CALLS_PER_SCAN}
_SCAN_ENRICHMENT_BUDGET = {"active": False, "used": 0, "limit": V575J_ENRICHMENT_RESERVE}
_SCAN_CORE_RESERVATION = {"active": False, "remaining": 0, "initial": 0}

def _v573_cache_key(name: str, params: Optional[Dict[str, Any]]) -> str:
    raw = json.dumps({"name": name, "params": params or {}}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def _v573_cache_get(name: str, params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    key = _v573_cache_key(name, params)
    row = _PROVIDER_CACHE.get(key)
    if not row:
        return None
    ttl = PROVIDER_LIVE_CACHE_TTL if name == "live" else PROVIDER_CACHE_TTL
    age = _v572_epoch() - float(row.get("at") or 0)
    if age > ttl:
        _PROVIDER_CACHE.pop(key, None)
        return None
    data = dict(row.get("response") or {})
    data["cache_hit"] = True
    data["cache_age_seconds"] = round(age, 2)
    return data

def _v573_cache_put(name: str, params: Optional[Dict[str, Any]], response: Dict[str, Any]) -> None:
    if not response.get("ok"):
        return
    key = _v573_cache_key(name, params)
    clean = dict(response)
    clean["cache_hit"] = False
    _PROVIDER_CACHE[key] = {"at": _v572_epoch(), "response": clean}
    if len(_PROVIDER_CACHE) > 300:
        oldest = sorted(_PROVIDER_CACHE.items(), key=lambda kv: kv[1].get("at", 0))[:80]
        for k, _ in oldest:
            _PROVIDER_CACHE.pop(k, None)

def _v573_trim_calls(now: float) -> None:
    global _PROVIDER_CALL_TIMES
    _PROVIDER_CALL_TIMES = [t for t in _PROVIDER_CALL_TIMES if now - t < 60.0]

def _v573_scan_budget_start() -> None:
    _SCAN_BUDGET["active"] = True
    _SCAN_BUDGET["used"] = 0
    _SCAN_BUDGET["limit"] = PROVIDER_MAX_CALLS_PER_SCAN
    _SCAN_ENRICHMENT_BUDGET["active"] = True
    _SCAN_ENRICHMENT_BUDGET["used"] = 0
    _SCAN_ENRICHMENT_BUDGET["limit"] = V575J_ENRICHMENT_RESERVE
    _SCAN_CORE_RESERVATION["active"] = True
    _SCAN_CORE_RESERVATION["remaining"] = 0
    _SCAN_CORE_RESERVATION["initial"] = 0

def _v575j_enrichment_status() -> Dict[str, Any]:
    used = int(_SCAN_ENRICHMENT_BUDGET.get("used") or 0)
    limit = int(_SCAN_ENRICHMENT_BUDGET.get("limit") or 0)
    return {
        "active": bool(_SCAN_ENRICHMENT_BUDGET.get("active")),
        "used": used,
        "limit": limit,
        "remaining": max(0, limit - used),
    }

def _v575j_core_reservation_status() -> Dict[str, Any]:
    return {
        "active": bool(_SCAN_CORE_RESERVATION.get("active")),
        "initial": int(_SCAN_CORE_RESERVATION.get("initial") or 0),
        "remaining": int(_SCAN_CORE_RESERVATION.get("remaining") or 0),
    }

def _v575j_set_core_reservation(slots: int) -> None:
    slots = max(0, int(slots))
    _SCAN_CORE_RESERVATION["active"] = True
    _SCAN_CORE_RESERVATION["initial"] = slots
    _SCAN_CORE_RESERVATION["remaining"] = slots

async def _v575j_release_core_reservation(slots: int = 1) -> None:
    slots = max(0, int(slots))
    if slots <= 0:
        return
    async with _PROVIDER_LOCK:
        remaining = int(_SCAN_CORE_RESERVATION.get("remaining") or 0)
        _SCAN_CORE_RESERVATION["remaining"] = max(0, remaining - slots)

async def _v575j_reserve_enrichment(cost: int) -> bool:
    cost = max(0, int(cost))
    if cost == 0:
        return True
    async with _PROVIDER_LOCK:
        if not _SCAN_ENRICHMENT_BUDGET.get("active"):
            return _v575b_normal_budget_remaining() >= cost if "_v575b_normal_budget_remaining" in globals() else True
        used = int(_SCAN_ENRICHMENT_BUDGET.get("used") or 0)
        limit = int(_SCAN_ENRICHMENT_BUDGET.get("limit") or 0)
        if used + cost > limit:
            return False
        # Also respect actual provider budget AND core-stat slots that were
        # already admitted but have not yet reached the provider gate. This is
        # the stats-first guarantee: enrichment starts only after those core
        # reservations can no longer be stolen by a fast coroutine.
        st = _v573_scan_budget_status()
        normal_remaining = max(0, int(st.get("normal_call_ceiling") or 0) - int(st.get("used") or 0))
        core_remaining = int(_SCAN_CORE_RESERVATION.get("remaining") or 0)
        optional_capacity = max(0, normal_remaining - core_remaining)
        if optional_capacity < cost:
            return False
        _SCAN_ENRICHMENT_BUDGET["used"] = used + cost
        return True

def _v573_scan_budget_status() -> Dict[str, Any]:
    used = int(_SCAN_BUDGET.get("used") or 0)
    limit = int(_SCAN_BUDGET.get("limit") or PROVIDER_MAX_CALLS_PER_SCAN)
    final_reserve = max(0, int(PROVIDER_FINAL_SNAPSHOT_RESERVE))
    targeted_reserve = max(0, int(PROVIDER_TARGETED_VERIFY_RESERVE))
    normal_ceiling = max(0, limit - final_reserve - targeted_reserve)
    return {
        "active": bool(_SCAN_BUDGET.get("active")),
        "used": used,
        "limit": limit,
        "remaining": max(0, limit-used),
        "normal_call_ceiling": normal_ceiling,
        "final_snapshot_reserve": final_reserve,
        "targeted_verify_reserve": targeted_reserve,
    }

async def _v573_provider_gate(
    name: str,
    params: Optional[Dict[str, Any]],
    purpose: str = "normal",
) -> Dict[str, Any]:
    global _PROVIDER_LAST_CALL_AT

    rs = _v572_rate_status()
    if rs.get("active"):
        return {"allowed": False, "reason": "RATE_LIMIT_COOLDOWN", "rate_limit": rs}

    # V5.7.5b: admission, budget reservation, rolling-minute rate check and
    # request spacing are serialized under one lock. No coroutine can observe
    # an old `used` value and then increment after another coroutine has spent it.
    async with _PROVIDER_LOCK:
        now = _v572_epoch()
        _v573_trim_calls(now)

        if len(_PROVIDER_CALL_TIMES) >= PROVIDER_MAX_CALLS_PER_MINUTE:
            wait = max(1.0, 60.0 - (now - min(_PROVIDER_CALL_TIMES)))
            return {
                "allowed": False,
                "reason": "LOCAL_RATE_LIMIT",
                "retry_after": round(wait, 1),
                "calls_last_60s": len(_PROVIDER_CALL_TIMES),
                "limit_per_minute": PROVIDER_MAX_CALLS_PER_MINUTE,
            }

        if _SCAN_BUDGET.get("active"):
            used = int(_SCAN_BUDGET.get("used") or 0)
            limit = int(_SCAN_BUDGET.get("limit") or PROVIDER_MAX_CALLS_PER_SCAN)
            final_reserve = max(0, int(PROVIDER_FINAL_SNAPSHOT_RESERVE))
            targeted_reserve = max(0, int(PROVIDER_TARGETED_VERIFY_RESERVE))
            normal_ceiling = max(0, limit - final_reserve - targeted_reserve)
            final_ceiling = max(0, limit - targeted_reserve)

            if purpose in {"normal", "core_stats", "enrichment", "optional_context"} and used >= normal_ceiling:
                return {
                    "allowed": False,
                    "reason": "SCAN_BUDGET_RESERVED_FOR_FINAL_GUARDS",
                    "budget": _v573_scan_budget_status(),
                }

            # Optional calls may never steal budget reserved for core stats that
            # have already been admitted by the deep planner but have not yet
            # reached the provider gate. This closes the concurrency race where
            # one fast visible match could consume 3 enrichment calls first.
            if purpose in {"enrichment", "optional_context"}:
                core_remaining = int(_SCAN_CORE_RESERVATION.get("remaining") or 0)
                optional_ceiling = max(0, normal_ceiling - core_remaining)
                if used >= optional_ceiling:
                    return {
                        "allowed": False,
                        "reason": "DEEP_CORE_RESERVE_PROTECTED",
                        "core_reservation": _v575j_core_reservation_status(),
                        "budget": _v573_scan_budget_status(),
                    }

            if purpose == "core_stats" and _SCAN_CORE_RESERVATION.get("active"):
                remaining = int(_SCAN_CORE_RESERVATION.get("remaining") or 0)
                if remaining > 0:
                    _SCAN_CORE_RESERVATION["remaining"] = remaining - 1

            if purpose == "final_snapshot" and used >= final_ceiling:
                return {
                    "allowed": False,
                    "reason": "FINAL_SNAPSHOT_RESERVE_EXHAUSTED",
                    "budget": _v573_scan_budget_status(),
                }
            if purpose == "targeted_verify" and used >= limit:
                return {
                    "allowed": False,
                    "reason": "TARGETED_VERIFY_RESERVE_EXHAUSTED",
                    "budget": _v573_scan_budget_status(),
                }

        gap = now - _PROVIDER_LAST_CALL_AT
        if gap < PROVIDER_MIN_GAP:
            await asyncio.sleep(PROVIDER_MIN_GAP - gap)
            now = _v572_epoch()

        # Atomic reservation happens BEFORE releasing the lock.
        _PROVIDER_LAST_CALL_AT = now
        _PROVIDER_CALL_TIMES.append(now)
        if _SCAN_BUDGET.get("active"):
            _SCAN_BUDGET["used"] = int(_SCAN_BUDGET.get("used") or 0) + 1

        return {
            "allowed": True,
            "purpose": purpose,
            "budget": _v573_scan_budget_status(),
        }


class ZylaClient:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(timeout=18.0, headers=headers())

    async def close(self) -> None:
        await self.client.aclose()

    async def get(
        self,
        name: str,
        params: Optional[Dict[str, Any]] = None,
        force_refresh: bool = False,
        purpose: str = "normal",
    ) -> Dict[str, Any]:
        if not ZYLA_API_KEY:
            return {"ok": False, "status": 0, "data": None, "error": "ZYLA_API_KEY missing"}

        if not force_refresh:
            cached = _v573_cache_get(name, params)
            if cached is not None:
                if purpose == "core_stats":
                    await _v575j_release_core_reservation(1)
                return cached

        # V5.7.2 shared quota protection. The live guard itself is allowed
        # to make the recovery probe; deep endpoints are blocked in cooldown.
        if name != "live" and "_v572_rate_status" in globals():
            rs = _v572_rate_status()
            if rs.get("active"):
                return {
                    "ok": False,
                    "status": 429,
                    "data": None,
                    "headers": {},
                    "retry_after": rs.get("remaining_seconds"),
                    "error": "RATE_LIMIT_COOLDOWN",
                    "rate_limit": rs,
                }
        gate = await _v573_provider_gate(name, params, purpose=purpose)
        if not gate.get("allowed"):
            if purpose == "core_stats":
                await _v575j_release_core_reservation(1)
            return {
                "ok": False,
                "status": 429 if gate.get("reason") in {"RATE_LIMIT_COOLDOWN", "LOCAL_RATE_LIMIT"} else 0,
                "data": None,
                "headers": {},
                "retry_after": gate.get("retry_after"),
                "error": gate.get("reason"),
                "quota_guard": gate,
            }

        try:
            r = await self.client.get(endpoint_url(name), params=params or {})
            data: Any
            try:
                data = r.json()
            except Exception:
                data = r.text

            if r.status_code == 429 and "_v572_activate_cooldown" in globals():
                _v572_activate_cooldown(r.headers.get("retry-after"))

            response = {
                "ok": 200 <= r.status_code < 300,
                "status": r.status_code,
                "data": data,
                "headers": dict(r.headers),
                "retry_after": r.headers.get("retry-after"),
                "error": None if 200 <= r.status_code < 300 else str(data)[:500],
            }
            _v573_cache_put(name, params, response)
            return response
        except Exception as e:
            return {"ok": False, "status": 0, "data": None, "error": repr(e)}

    async def live(
        self,
        force_refresh: bool = False,
        purpose: str = "normal",
    ) -> Dict[str, Any]:
        return await self.get(
            "live",
            {"sport_id": 1},
            force_refresh=force_refresh,
            purpose=purpose,
        )

    async def details(self, match_id: str, purpose: str = "normal") -> Dict[str, Any]:
        return await self.get("details", {"match_id": match_id}, purpose=purpose)

    async def summary(self, match_id: str) -> Dict[str, Any]:
        return await self.get("summary", {"match_id": match_id})

    async def stats(self, match_id: str, purpose: str = "normal") -> Dict[str, Any]:
        return await self.get("stats", {"match_id": match_id}, purpose=purpose)

    async def lineups(self, match_id: str, purpose: str = "normal") -> Dict[str, Any]:
        return await self.get("lineups", {"match_id": match_id}, purpose=purpose)

    async def player_stats(self, match_id: str, purpose: str = "normal") -> Dict[str, Any]:
        return await self.get("player_stats", {"match_id": match_id}, purpose=purpose)

    async def odds(self, match_id: str, purpose: str = "normal") -> Dict[str, Any]:
        return await self.get("odds", {"match_id": match_id}, purpose=purpose)

    async def h2h(self, match_id: str, purpose: str = "normal") -> Dict[str, Any]:
        return await self.get("h2h", {"match_id": match_id}, purpose=purpose)

    async def team_details(self, team_url: str) -> Dict[str, Any]:
        return await self.get("team_details", {"team_url": team_url})

    async def team_results(self, team_id: str, page: int = 1, purpose: str = "normal") -> Dict[str, Any]:
        return await self.get("team_results", {"team_id": team_id, "page": max(1, int(page))}, purpose=purpose)

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

def _v575b_not_requested(reason: str = "NOT_REQUESTED_BY_PRIORITY") -> Dict[str, Any]:
    return {
        "ok": False,
        "status": 0,
        "data": None,
        "error": reason,
        "cache_hit": False,
    }


def _v575b_real_call_count(*responses: Dict[str, Any]) -> int:
    """
    Diagnostic only: count requests that appear to have reached provider.
    Cached responses and locally blocked requests do not count.
    """
    total = 0
    for r in responses:
        if not isinstance(r, dict):
            continue
        if r.get("cache_hit"):
            continue
        if r.get("quota_guard"):
            continue
        status = int(r.get("status") or 0)
        if status > 0:
            total += 1
    return total


def _v575b_normal_budget_remaining() -> int:
    st = _v573_scan_budget_status()
    used = int(st.get("used") or 0)
    ceiling = int(st.get("normal_call_ceiling") or 0)
    return max(0, ceiling - used)


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
                "source": "football-reactor-v5",
                "version": VERSION,
                "match_id": match_id,
                "status": "NOT_LIVE_OR_NOT_FOUND",
                "api_calls": api_calls,
            }

        if not live.get("minute_valid", True):
            return {
                "source": "football-reactor-v5",
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

        # V5.7.5b PRIORITY ANALYSIS
        # Tier 1: fetch only live stats for every selected match. This is the
        # main signal-producing endpoint and lets the scan cover more matches
        # without spending 7 requests on weak candidates.
        details_r = _v575b_not_requested()
        summary_r = _v575b_not_requested()
        lineups_r = _v575b_not_requested()
        player_r = _v575b_not_requested()
        odds_r = _v575b_not_requested()
        h2h_r = _v575b_not_requested()

        stats_r = await client.stats(match_id, purpose="core_stats")
        api_calls += _v575b_real_call_count(stats_r)

        # If the core stats request was locally blocked because the normal
        # budget is reserved for final guards, stop cleanly instead of building
        # a fake analysis from missing data.
        if (
            not stats_r.get("ok")
            and stats_r.get("error") in {
                "SCAN_BUDGET_RESERVED_FOR_FINAL_GUARDS",
                "SCAN_API_BUDGET_EXHAUSTED",
                "LOCAL_RATE_LIMIT",
                "RATE_LIMIT_COOLDOWN",
            }
        ):
            return {
                "source": "football-reactor-v5",
                "version": VERSION,
                "model_type": MODEL_TYPE,
                "match_id": match_id,
                "status": "CORE_STATS_GUARD_BLOCKED",
                "error": stats_r.get("error"),
                "match": {
                    "home": live.get("home"),
                    "away": live.get("away"),
                    "minute": live.get("minute"),
                    "score": live.get("score"),
                },
                "diagnostic": {
                    "stats_http": stats_r.get("status"),
                    "scan_budget": _v573_scan_budget_status(),
                    "analysis_tier": "CORE_BLOCKED",
                    "estimated_api_calls": api_calls,
                },
            }

        parsed = parse_metrics(stats_r.get("data"), live)
        metrics = parsed["metrics"]
        q = quality_guard(metrics)
        pressure = pressure_score(metrics, int(live["minute"]))

        # Preliminary model intentionally uses no lineup modifier. A 65%+
        # candidate earns enrichment; weak matches consume no extra endpoints.
        lineups = parse_lineups(None)
        players = parse_player_stats(None)
        odds = compact_odds(None)
        h2h = h2h_context(None)
        signals = build_signals(live, metrics, q, pressure, lineups)

        preliminary_best = max(
            [
                max(
                    float(s.get("probability") or 0),
                    float(s.get("live_probability") or 0),
                )
                for s in signals
            ] or [0.0]
        )

        analysis_tier = "CORE_STATS_ONLY"

        # Tier 2: only a visible 65%+ candidate gets freshness/lineup/market
        # context. These endpoints may run concurrently, but the atomic budget
        # gate guarantees they cannot consume the final reserves.
        current_focus = _v575k_focus_mode()
        if current_focus == "first_half":
            enrichment_threshold = V575K_FIRST_HALF_ENRICH_THRESHOLD
        elif current_focus == "second_half":
            enrichment_threshold = V575M_SECOND_HALF_ENRICH_THRESHOLD
        else:
            enrichment_threshold = VISIBLE_SIGNAL_MIN
        if preliminary_best >= enrichment_threshold:
            # V5.7.5j: reserve all three Tier-2 calls atomically before starting
            # them. This prevents two concurrent visible candidates from eating
            # the core-stat slots of matches that were already admitted to deep.
            if await _v575j_reserve_enrichment(3):
                details_r, lineups_r, odds_r = await asyncio.gather(
                    client.details(match_id, purpose="enrichment"),
                    client.lineups(match_id, purpose="enrichment"),
                    client.odds(match_id, purpose="enrichment"),
                )
                api_calls += _v575b_real_call_count(details_r, lineups_r, odds_r)

                lineups = parse_lineups(lineups_r.get("data"))
                odds = compact_odds(odds_r.get("data"))

                # Rebuild because lineup formation can make a small bounded change.
                signals = build_signals(live, metrics, q, pressure, lineups)
                analysis_tier = "VISIBLE_THRESHOLD_ENRICHED"
            else:
                # Core stats remain valid; only optional context is skipped.
                analysis_tier = "VISIBLE_THRESHOLD_CORE_ONLY_ENRICHMENT_RESERVED"

        enriched_best = max(
            [
                max(
                    float(s.get("probability") or 0),
                    float(s.get("live_probability") or 0),
                )
                for s in signals
            ] or [0.0]
        )

        # Tier 3: player/H2H context is descriptive, not required to create
        # the signal. Spend it only on a very strong candidate and only if
        # at least two normal-budget slots remain.
        if (
            not _v575m_is_dedicated_half_focus()
            and enriched_best >= 78.0
            and _v575b_normal_budget_remaining() >= 2
        ):
            player_r, h2h_r = await asyncio.gather(
                client.player_stats(match_id, purpose="optional_context"),
                client.h2h(match_id, purpose="optional_context"),
            )
            api_calls += _v575b_real_call_count(player_r, h2h_r)
            players = parse_player_stats(player_r.get("data"))
            h2h = h2h_context(h2h_r.get("data"))
            analysis_tier = "STRONG_CONTEXT_ENRICHED"

        nearest_goal = nearest_goal_assessment(signals, live, q, pressure)

        # Historical context has low model weight. It is now reserved only for
        # exceptional candidates and only when two normal slots remain.
        top_pre_context = signals[0] if signals else None
        history_context = {"available": False, "reason": "not_requested_by_priority_budget"}
        history_http = {"home": None, "away": None}
        if (
            not _v575m_is_dedicated_half_focus()
            and top_pre_context
            and max(
                float(top_pre_context.get("probability") or 0),
                float(top_pre_context.get("live_probability") or 0),
            ) >= 82.0
            and _v575b_normal_budget_remaining() >= 2
        ):
            home_id = live.get("home_team_id")
            away_id = live.get("away_team_id")
            if home_id and away_id:
                home_hist_r, away_hist_r = await asyncio.gather(
                    client.team_results(str(home_id), 1, purpose="optional_context"),
                    client.team_results(str(away_id), 1, purpose="optional_context"),
                )
                api_calls += _v575b_real_call_count(home_hist_r, away_hist_r)
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
            "source": "football-reactor-v5",
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
                "analysis_tier": analysis_tier,
                "preliminary_best_probability": round(preliminary_best, 1),
                "enriched_best_probability": round(enriched_best, 1),
                "details_http": details_r.get("status"),
                "summary_http": summary_r.get("status"),
                "stats_http": stats_r.get("status"),
                "stats_rows_count": len(parsed["rows"]),
                "lineups_http": lineups_r.get("status"),
                "player_stats_http": player_r.get("status"),
                "odds_http": odds_r.get("status"),
                "h2h_http": h2h_r.get("status"),
                "team_history_http": history_http,
                "estimated_api_calls": api_calls,
                "scan_budget_at_report": _v573_scan_budget_status(),
                "enrichment_budget_at_report": _v575j_enrichment_status(),
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
            "targeted_score_minute_sync_guard",
            "score_conflict_recalculation",
            "adaptive_now_stability_guard",
            "persistent_state_guard",
            "diagnostics_budget_skip_separation",
            "proactive_deep_budget_guard",
            "rolling_minute_budget_guard",
            "first_half_stats_first_guard",
            "second_half_stats_first_guard",
            "dedicated_half_budget_isolation_guard",
            "first_half_unilateral_pressure_guard",
            "first_half_low_dq_analysis_only_guard",
            "rotation_queue_guard",
            "persistent_state_health_guard",
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
            "our_analysis_engine",
            "goal_hunter_old_style",
            "smart_live_scout",
            "two_stage_live_collection",
            "short_term_scan_memory",
            "momentum_delta_engine",
            "rising_pressure_detection",
            "soft_freshness_guard",
            "team_name_final_snapshot_fallback",
            "memory_before_freshness_filter",
            "rising_radar_55_64",
            "human_market_comparison",
            "first_half_goal_section",
            "second_half_goal_section",
            "take_now_soon_later",
            "timed_entry_guidance",
            "emerging_goal_detector",
            "dedicated_first_half_engine",
            "first_half_pressure_fallback_seed",
            "first_half_unilateral_pressure_engine",
            "multi_scan_pressure_trend",
            "false_pressure_detector",
            "market_comparison_engine",
            "our_thinking_2",
            "signal_hysteresis",
            "two_scan_confirmation",
            "entry_expiry_window",
            "market_conflict_detector",
            "context_risk_layer",
            "priority_score",
            "decision_journal",
            "bounded_self_learning",
            "auto_outcome_resolution",
            "market_specific_calibration",
            "sample_gated_tuning",
            "automatic_rollback",
            "adaptive_take_now",
            "adaptive_final_state_persistence",
            "persistent_state_resolver",
            "diagnostics_budget_skip_separation",
            "phase_quota_selection",
            "dynamic_deep_budget_planner",
            "rolling_60s_budget_planner",
            "first_half_focus_mode",
            "dedicated_first_half_scan_endpoint",
            "dedicated_second_half_scan_endpoint",
            "first_half_window_5_43",
            "second_half_window_46_90",
            "first_half_all_available_core_budget",
            "second_half_all_available_core_budget",
            "first_half_stats_first_pipeline",
            "second_half_stats_first_pipeline",
            "deferred_match_rotation_queue",
            "first_half_deep_slot_reservation",
            "atomic_tier2_enrichment_reserve",
            "persistent_rotation_queue",
            "tournament_diversity_prefilter",
            "goal_chain_board",
            "quick_screenshot_goal_hunter",
            "final_freshness_after_deep_scan",
            "screenshot_quick_bridge",
            "phase_balanced_live_collector",
        ],
        "state_storage": {
            "directory": STATE_STORAGE_DIR,
            "mode": STATE_STORAGE_MODE,
            "persistent": STATE_STORAGE_PERSISTENT,
            "warning": STATE_STORAGE_WARNING,
            "health": _v575j_state_storage_health(),
            "rotation_queue_path": DEEP_ROTATION_QUEUE_PATH,
        },
        "deep_budget_policy": {
            "max_calls_per_scan": PROVIDER_MAX_CALLS_PER_SCAN,
            "final_snapshot_reserve": PROVIDER_FINAL_SNAPSHOT_RESERVE,
            "targeted_verify_reserve": PROVIDER_TARGETED_VERIFY_RESERVE,
            "tier2_enrichment_reserve_balanced": V575J_ENRICHMENT_RESERVE,
            "tier2_enrichment_reserve_first_half": V575K_FIRST_HALF_ENRICH_RESERVE,
            "tier2_enrichment_reserve_second_half": V575M_SECOND_HALF_ENRICH_RESERVE,
            "first_half_deep_share_balanced": V575J_FIRST_HALF_DEEP_SHARE,
            "first_half_focus_share": 1.0,
            "second_half_focus_share": 1.0,
            "first_half_window": [V575K_FIRST_HALF_START_MINUTE, V575K_FIRST_HALF_END_MINUTE],
            "second_half_window": [V575M_SECOND_HALF_START_MINUTE, V575M_SECOND_HALF_END_MINUTE],
            "first_half_enrich_threshold": V575K_FIRST_HALF_ENRICH_THRESHOLD,
            "second_half_enrich_threshold": V575M_SECOND_HALF_ENRICH_THRESHOLD,
            "dedicated_first_half_endpoint": "scan_first_half_live",
            "dedicated_second_half_endpoint": "scan_second_half_live",
            "first_half_budget_mode": "ALL_AVAILABLE_NORMAL_CALLS_TO_FIRST_HALF_CORE_STATS",
            "second_half_budget_mode": "ALL_AVAILABLE_NORMAL_CALLS_TO_SECOND_HALF_CORE_STATS",
            "rolling_minute_aware": True,
            "rotation_queue_enabled": True,
        },
        "notes": [
            "Probabilities are heuristic ranking estimates, not calibrated true probabilities.",
            "H2H and historical team context have low weight and cannot create ENTER by themselves.",
            "Correlated signals are grouped and should not be counted as independent.",
            "Persistent state uses HIDDEN_SIGNAL_STATE_DIR/PERSISTENT_STORAGE_DIR/RENDER_DISK_PATH when configured, then common disk mounts, then /tmp only as fallback.",
        ],
    }

@mcp.tool()
async def get_zyla_live_matches() -> Dict[str, Any]:
    client = ZylaClient()
    try:
        r = await client.live()
        matches = flatten_live(r.get("data"))
        return {
            "source": "football-reactor-v5",
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
    """Full V4.7 thinking reactor report for one live football match."""
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
        "source": "football-reactor-v5",
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
            "source": "football-reactor-v5", "version": VERSION, "team_id": team_id,
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
            "source": "football-reactor-v5", "version": VERSION, "team_url": team_url,
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
            "source": "football-reactor-v5", "version": VERSION,
            "tournament_id": tournament_id, "tournament_stage_id": tournament_stage_id,
            "details_http": details_r.get("status"), "details": details_r.get("data"),
            "form_http": form_r.get("status"), "form": form_r.get("data"),
            "over_under_http": ou_r.get("status"), "over_under": ou_r.get("data"),
        }
    finally:
        await client.close()




# -------------------------
# V4.7 Thinking Reactor — our football reasoning layer
# -------------------------

def _norm_team_name(name: Any) -> str:
    s = str(name or "").lower().strip()
    s = re.sub(r"[^a-zа-яё0-9]+", " ", s, flags=re.I)
    tokens = [t for t in s.split() if t not in {"fc", "fk", "cf", "sc", "club", "football", "фк"}]
    return " ".join(tokens)


def _name_similarity(a: Any, b: Any) -> float:
    a1, b1 = _norm_team_name(a), _norm_team_name(b)
    if not a1 or not b1:
        return 0.0
    if a1 == b1:
        return 1.0
    sa, sb = set(a1.split()), set(b1.split())
    overlap = len(sa & sb) / max(1, len(sa | sb))
    contains = 0.9 if a1 in b1 or b1 in a1 else 0.0
    return max(overlap, contains)


def _match_similarity(live: Dict[str, Any], home: str, away: str) -> float:
    direct = (_name_similarity(live.get("home"), home) + _name_similarity(live.get("away"), away)) / 2
    swapped = (_name_similarity(live.get("home"), away) + _name_similarity(live.get("away"), home)) / 2
    return max(direct, swapped * 0.72)


def _metric_present(metrics: Dict[str, Any], key: str) -> bool:
    return bool((metrics.get(key) or {}).get("present"))


def _pair(metrics: Dict[str, Any], key: str) -> Tuple[float, float]:
    m = metrics.get(key) or {}
    return float(m.get("home") or 0), float(m.get("away") or 0)


def _game_picture(metrics: Dict[str, Any], match: Dict[str, Any], pressure: Dict[str, Any]) -> Dict[str, Any]:
    """Interpret the match, not just list statistics."""
    score = match.get("score") or {}
    h_score, a_score = int(score.get("home") or 0), int(score.get("away") or 0)
    minute = int(match.get("minute") or 0)

    h_attack = a_attack = 0.0
    weights = {
        "xg": 30.0,
        "shots_on_target": 24.0,
        "touches_in_box": 18.0,
        "shots_in_box": 12.0,
        "shots": 8.0,
        "big_chances": 8.0,
    }
    for key, weight in weights.items():
        if _metric_present(metrics, key):
            h, a = _pair(metrics, key)
            total = h + a
            if total > 0:
                h_attack += weight * h / total
                a_attack += weight * a / total

    h_attack += float(pressure.get("home") or 0) * 0.22
    a_attack += float(pressure.get("away") or 0) * 0.22
    diff = h_attack - a_attack

    if diff >= 15:
        closer = match.get("home")
        control = "хозяева заметно ближе к голу"
    elif diff <= -15:
        closer = match.get("away")
        control = "гости заметно ближе к голу"
    elif diff >= 6:
        closer = match.get("home")
        control = "небольшой перевес хозяев"
    elif diff <= -6:
        closer = match.get("away")
        control = "небольшой перевес гостей"
    else:
        closer = "неясно"
        control = "игра достаточно равная"

    xg = _pair(metrics, "xg") if _metric_present(metrics, "xg") else None
    sot = _pair(metrics, "shots_on_target") if _metric_present(metrics, "shots_on_target") else None
    score_story = "счёт примерно соответствует игре"
    if xg:
        expected_edge = xg[0] - xg[1]
        actual_edge = h_score - a_score
        if actual_edge <= -2 and expected_edge > -0.35:
            score_story = "счёт выглядит жёстче, чем сама игра — хозяева создали больше, чем показывает табло"
        elif actual_edge >= 2 and expected_edge < 0.35:
            score_story = "счёт выглядит крупнее, чем преимущество по моментам"
        elif h_score == a_score and abs(expected_edge) >= 0.55:
            score_story = "равный счёт немного обманывает: по моментам есть заметный перевес"

    intensity = _human_pressure(float(pressure.get("total") or 0))
    if sot and sum(sot) >= 7 and minute <= 70:
        rhythm = "матч живой: команды регулярно доводят атаки до створа"
    elif _metric_present(metrics, "touches_in_box") and sum(_pair(metrics, "touches_in_box")) >= 24:
        rhythm = "много игры идёт через штрафную — матч не выглядит закрытым"
    elif float(pressure.get("total") or 0) >= 65:
        rhythm = "давление высокое, голевой эпизод может появиться быстро"
    else:
        rhythm = "темп пока не даёт ощущения обязательного скорого гола"

    return {
        "closer_team": closer,
        "control": control,
        "score_story": score_story,
        "rhythm": rhythm,
        "intensity": intensity,
        "home_attack_index": round1(h_attack),
        "away_attack_index": round1(a_attack),
    }


def _market_family(option: str) -> str:
    s = str(option or "").lower()
    if "1-м тайме" in s:
        return "FIRST_HALF"
    if "2-м тайме" in s or "до конца" in s:
        return "FULLTIME_GOAL"
    if "оз" in s:
        return "BTTS"
    if "5 минут" in s:
        return "NEXT_5"
    if "10 минут" in s:
        return "NEXT_10"
    if "гол" in s:
        return "GOAL"
    return "OTHER"


def _thinking_engine(view: Dict[str, Any], rep: Dict[str, Any]) -> Dict[str, Any]:
    """Compare options against each other, like our old live discussion."""
    match = rep.get("match") or {}
    metrics = rep.get("metrics") or {}
    pressure = rep.get("pressure") or {}
    phase = view.get("phase")
    picture = _game_picture(metrics, match, pressure)

    candidates = []
    fh = view.get("FIRST_HALF")
    sh = view.get("SECOND_HALF")
    oz = view.get("BTTS")
    ng = view.get("NEAREST_GOAL") or {}
    if isinstance(fh, dict) and fh.get("probability") is not None:
        candidates.append({"name": "гол в 1-м тайме", **fh})
    if isinstance(sh, dict) and sh.get("probability") is not None:
        candidates.append({"name": "ещё гол во 2-м тайме / до конца", **sh})
    if isinstance(oz, dict) and oz.get("status") != "ALREADY_WON" and oz.get("probability") is not None:
        candidates.append({"name": "ОЗ — Да", **oz})
    if isinstance(ng.get("5_min"), dict):
        candidates.append({"name": "гол в ближайшие 5 минут", **ng["5_min"]})
    if isinstance(ng.get("10_min"), dict):
        candidates.append({"name": "гол в ближайшие 10 минут", **ng["10_min"]})

    # Penalize ultra-short windows unless pressure/sample really support them.
    for c in candidates:
        p = float(c.get("probability") or 0)
        fam = _market_family(c["name"])
        adjusted = p
        if fam == "NEXT_5":
            adjusted -= 6
        elif fam == "NEXT_10":
            adjusted -= 2
        if phase == "HALF_TIME":
            adjusted = -1
        if int(match.get("minute") or 0) <= 12:
            adjusted -= 5
        c["_choice_score"] = round1(adjusted)

    candidates.sort(key=lambda x: x.get("_choice_score", 0), reverse=True)
    best = candidates[0] if candidates and candidates[0]["_choice_score"] >= VISIBLE_SIGNAL_MIN - 4 else None
    alternative = candidates[1] if len(candidates) > 1 and candidates[1]["_choice_score"] >= VISIBLE_SIGNAL_MIN - 4 else None

    rejected = []
    for c in candidates[1:]:
        fam = _market_family(c["name"])
        if fam == "NEXT_5":
            why = "слишком короткое окно: даже при хорошем давлении дисперсия выше"
        elif best and abs(float(best.get("probability") or 0) - float(c.get("probability") or 0)) < 4:
            why = "вариант близкий, но основной рынок лучше совпадает с общей картиной игры"
        else:
            why = "по совокупности цифр и рисунка игры основной вариант выглядит чище"
        rejected.append({"option": c["name"], "probability": c.get("probability"), "why_not": why})

    if phase == "HALF_TIME":
        verdict = "WAIT"
        action = "Перерыв: не подтверждаем live-вход вслепую. Смотрим первые 2–5 минут второго тайма."
    elif best and float(best.get("probability") or 0) >= 75:
        verdict = "ENTER_CANDIDATE"
        action = f"Главный вариант — {best['name']} ({best['probability']}%). Перед входом сверяем, что давление не исчезло."
    elif best:
        verdict = "WATCH"
        action = f"Сигнал есть, но пока наблюдаем: {best['name']} ({best['probability']}%). Нужен ещё один импульс."
    else:
        verdict = "PASS"
        action = "Сильного варианта от 65% нет — не натягиваем."

    return {
        "what_i_see": [
            picture["control"],
            picture["score_story"],
            picture["rhythm"],
        ],
        "who_is_closer": picture["closer_team"],
        "game_picture": picture,
        "best_option": None if not best else {
            "market": best["name"],
            "probability": best.get("probability"),
            "risk": best.get("risk"),
            "reason": "лучше всего совпадает с текущей статистикой, фазой матча и направлением давления",
        },
        "alternative": None if not alternative else {
            "market": alternative["name"],
            "probability": alternative.get("probability"),
        },
        "why_not_others": rejected[:4],
        "verdict": verdict,
        "our_action": action,
        "next_confirmation": (
            "Рост xG, ещё 1–2 удара/створа, новые касания в штрафной или сохранение давления."
            if phase != "HALF_TIME"
            else "После старта 2Т ждём 2–5 минут свежей картины: створ, xG, штрафная, темп."
        ),
        "warning": "Это эвристический live-разбор, а не гарантированная или калиброванная вероятность.",
    }


def _screenshot_metrics(
    xg_home=None, xg_away=None, shots_home=None, shots_away=None,
    sot_home=None, sot_away=None, box_home=None, box_away=None,
    corners_home=None, corners_away=None, possession_home=None, possession_away=None,
    dangerous_home=None, dangerous_away=None, big_home=None, big_away=None,
) -> Dict[str, Any]:
    def metric(h, a):
        hp, ap = safe_float(h), safe_float(a)
        return {
            "home": hp, "away": ap,
            "total": (hp + ap) if hp is not None and ap is not None else None,
            "present": hp is not None and ap is not None,
        }
    return {
        "xg": metric(xg_home, xg_away),
        "shots": metric(shots_home, shots_away),
        "shots_on_target": metric(sot_home, sot_away),
        "touches_in_box": metric(box_home, box_away),
        "shots_in_box": metric(None, None),
        "corners": metric(corners_home, corners_away),
        "possession": metric(possession_home, possession_away),
        "xa": metric(None, None),
        "fouls": metric(None, None),
        "big_chances": metric(big_home, big_away),
        "dangerous_attacks": metric(dangerous_home, dangerous_away),
        "red_cards": metric(0, 0),
    }


def _quick_screenshot_report(live: Dict[str, Any], metrics: Dict[str, Any], source_note: str) -> Dict[str, Any]:
    q = quality_guard(metrics)
    pressure = pressure_score(metrics, int(live.get("minute") or 0))
    signals = build_signals(live, metrics, q, pressure, {"available": False})
    nearest = nearest_goal_assessment(signals, live, q, pressure)
    rep = {
        "status": "OK",
        "match_id": live.get("match_id"),
        "match": live,
        "metrics": metrics,
        "pressure": pressure,
        "data_quality": q,
        "all_signals": signals,
        "nearest_goal": nearest,
    }
    view = structured_market_report(rep)
    view["OUR_ANALYSIS_ENGINE"] = _thinking_engine(view, rep)
    view["source_note"] = source_note
    return view


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

    result = {
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
    result["OUR_ANALYSIS_ENGINE"] = _thinking_engine(result, rep)
    return result


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
            "source": "hidden-signal-v5.7",
            "version": VERSION,
            "blocked": True,
            "reason": freshness.get("reason"),
            "message": "Сигнал скрыт: финальная проверка лайва не пройдена.",
            "final_freshness": freshness,
        }
    return {
        "source": "hidden-signal-v5.7",
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
    """Main V4.7 structured scan: only 65-99% candidates, clearly split by market."""
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
        "source": "hidden-signal-v5.7",
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
async def analyze_screenshot_quick(
    home: str,
    away: str,
    minute: int,
    score_home: int,
    score_away: int,
    xg_home: Optional[float] = None,
    xg_away: Optional[float] = None,
    shots_home: Optional[float] = None,
    shots_away: Optional[float] = None,
    sot_home: Optional[float] = None,
    sot_away: Optional[float] = None,
    box_home: Optional[float] = None,
    box_away: Optional[float] = None,
    corners_home: Optional[float] = None,
    corners_away: Optional[float] = None,
    possession_home: Optional[float] = None,
    possession_away: Optional[float] = None,
    dangerous_home: Optional[float] = None,
    dangerous_away: Optional[float] = None,
    big_home: Optional[float] = None,
    big_away: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Fast screenshot bridge.
    ChatGPT reads the screenshot, sends visible numbers here, and V4.7 immediately
    cross-checks the match against the current live list. Provider score/minute win
    when a confident live match is found; screenshot stats fill the fast model.
    """
    started = time.time()
    client = ZylaClient()
    try:
        live_r = await client.live()
        matches = flatten_live(live_r.get("data"))
    finally:
        await client.close()

    best_match = None
    best_similarity = 0.0
    for m in matches:
        sim = _match_similarity(m, home, away)
        if sim > best_similarity:
            best_similarity, best_match = sim, m

    screenshot_score = score_obj(score_home, score_away)
    mismatch = []
    if best_match and best_similarity >= 0.58:
        live = dict(best_match)
        if live.get("score") != screenshot_score:
            mismatch.append({
                "type": "score",
                "screenshot": screenshot_score,
                "live": live.get("score"),
            })
        if abs(int(live.get("minute") or 0) - int(minute or 0)) > 4:
            mismatch.append({
                "type": "minute",
                "screenshot": minute,
                "live": live.get("minute"),
            })
        source_note = "Скрин сопоставлен со свежим live. Счёт/минута провайдера используются как контроль свежести."
    else:
        live = {
            "match_id": None,
            "home": home,
            "away": away,
            "minute": max(0, min(int(minute or 0), 130)),
            "minute_valid": 0 <= int(minute or 0) <= 130,
            "stage": "Screenshot",
            "score": screenshot_score,
            "red_cards": {"home": 0, "away": 0},
            "live_odds_1x2": {},
        }
        source_note = "Live-сопоставление не найдено уверенно. Быстрый разбор сделан только по данным со скрина."

    metrics = _screenshot_metrics(
        xg_home, xg_away, shots_home, shots_away,
        sot_home, sot_away, box_home, box_away,
        corners_home, corners_away, possession_home, possession_away,
        dangerous_home, dangerous_away, big_home, big_away,
    )
    report = _quick_screenshot_report(live, metrics, source_note)
    return {
        "source": "hidden-signal-v5.7.1-screenshot",
        "version": VERSION,
        "latency_ms": int((time.time() - started) * 1000),
        "live_match_found": bool(best_match and best_similarity >= 0.58),
        "match_similarity": round1(best_similarity * 100),
        "freshness_mismatch": mismatch,
        "report": report,
        "answer_style": {
            "fast": True,
            "human_reasoning_first": True,
            "show_65_99_only": True,
            "compare_markets": True,
            "do_not_force_signal": True,
        },
    }


@mcp.tool()
async def scan_thinking_live(limit: int = 12, concurrency: int = 2) -> Dict[str, Any]:
    """
    Improved V4.7 live collector.
    One live snapshot -> cheap prefilter -> limited concurrent deep analyses ->
    one final freshness snapshot -> human reasoning for every surviving candidate.
    """
    started = time.time()
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

    # Broader phase-aware prefilter: preserve 1H and 2H opportunities.
    first_half = sorted([m for m in valid if int(m.get("minute") or 0) <= 45], key=cheap_rank, reverse=True)
    second_half = sorted([m for m in valid if int(m.get("minute") or 0) > 45], key=cheap_rank, reverse=True)
    target_n = max(1, min(int(limit), 14))
    selected = []
    while len(selected) < target_n and (first_half or second_half):
        if first_half:
            selected.append(first_half.pop(0))
            if len(selected) >= target_n:
                break
        if second_half:
            selected.append(second_half.pop(0))

    sem = asyncio.Semaphore(max(1, min(int(concurrency), 3)))
    async def run_one(m):
        async with sem:
            try:
                return await analyze_match_internal(str(m["match_id"]), exact_live=m)
            except Exception as e:
                return {"status": "ERROR", "match_id": m.get("match_id"), "error": repr(e)}

    reports = await asyncio.gather(*(run_one(m) for m in selected))

    final_client = ZylaClient()
    try:
        final_r = await final_client.live()
        final_matches = flatten_live(final_r.get("data"))
    finally:
        await final_client.close()
    final_map = {str(m.get("match_id")): m for m in final_matches if m.get("match_id")}

    candidates = []
    blocked = []
    parser_failures = 0
    quality_blocked = 0

    for rep in reports:
        if rep.get("status") != "OK":
            parser_failures += 1
            continue
        freshness = final_freshness_check(rep.get("match") or {}, final_map.get(str(rep.get("match_id") or "")))
        if not freshness.get("ok"):
            blocked.append({"match_id": rep.get("match_id"), "reason": freshness.get("reason")})
            continue
        q = rep.get("data_quality") or {}
        if not q.get("basic_ok"):
            quality_blocked += 1
            continue

        view = structured_market_report(rep)
        thinking = view.get("OUR_ANALYSIS_ENGINE") or {}
        best = thinking.get("best_option")
        if best and float(best.get("probability") or 0) >= VISIBLE_SIGNAL_MIN:
            candidates.append({
                "match_id": rep.get("match_id"),
                "match": view.get("match"),
                "minute": view.get("minute"),
                "score": view.get("score"),
                "FIRST_HALF": view.get("FIRST_HALF"),
                "SECOND_HALF": view.get("SECOND_HALF"),
                "BTTS": view.get("BTTS"),
                "NEAREST_GOAL": view.get("NEAREST_GOAL"),
                "OUTCOMES_1X2": view.get("OUTCOMES_1X2"),
                "OUR_REASONING": view.get("OUR_REASONING"),
                "OUR_ANALYSIS_ENGINE": thinking,
            })

    candidates.sort(
        key=lambda x: float(((x.get("OUR_ANALYSIS_ENGINE") or {}).get("best_option") or {}).get("probability") or 0),
        reverse=True,
    )

    return {
        "source": "hidden-signal-v5.7",
        "version": VERSION,
        "live_snapshot_at": now_iso(),
        "live_matches_found": len(matches),
        "selected_for_deep_analysis": len(selected),
        "surviving_candidates": len(candidates),
        "latency_ms": int((time.time() - started) * 1000),
        "candidates": candidates,
        "freshness_blocked": blocked,
        "parser_failures": parser_failures,
        "quality_blocked": quality_blocked,
        "message": "Сильных вариантов от 65% сейчас нет — пропускаем." if not candidates else "Есть кандидаты. Сначала читаем OUR_ANALYSIS_ENGINE, а не просто самый высокий процент.",
    }



# -------------------------
# V5.0 Goal Hunter — restore the original "goal after goal" workflow
# -------------------------

def _goal_market_priority(name: str) -> int:
    s = str(name or "").lower()
    if "1-м тайме" in s:
        return 100
    if "до конца" in s or "ещё гол" in s:
        return 92
    if "10 минут" in s:
        return 88
    if "5 минут" in s:
        return 84
    if "оз" in s:
        return 78
    if "команды" in s or "забь" in s:
        return 74
    return 50


def _classic_goal_options(rep: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build a human goal-board from all available goal-related markets."""
    q = rep.get("data_quality") or {}
    signals = rep.get("all_signals") or []
    minute = int((rep.get("match") or {}).get("minute") or 0)
    stage = str((rep.get("match") or {}).get("stage") or "").lower()
    half_time = "half time" in stage or "halftime" in stage

    out = []
    seen = set()
    for s in signals:
        market = str(s.get("market") or "")
        if market not in {
            "GOAL_BEFORE_FULLTIME", "GOAL_BEFORE_HALFTIME",
            "GOAL_NEXT_5", "GOAL_NEXT_10", "TEAM_GOAL", "BTTS", "OVER_UNDER"
        }:
            continue

        selection = str(s.get("selection") or "")
        # Keep only OVER side from O/U in the goal-hunter board.
        if market == "OVER_UNDER" and not selection.lower().startswith("over"):
            continue

        p = _effective_probability(s)
        if p < VISIBLE_SIGNAL_MIN:
            continue
        if s.get("data_quality_blocked"):
            continue

        if market == "GOAL_BEFORE_HALFTIME":
            title = "Гол в 1-м тайме"
        elif market == "GOAL_BEFORE_FULLTIME":
            title = "Ещё минимум 1 гол до конца"
        elif market == "GOAL_NEXT_5":
            title = "Гол в ближайшие 5 минут"
        elif market == "GOAL_NEXT_10":
            title = "Гол в ближайшие 10 минут"
        elif market == "BTTS":
            title = "ОЗ — Да"
        elif market == "TEAM_GOAL":
            title = selection
        else:
            title = selection

        key = (market, title)
        if key in seen:
            continue
        seen.add(key)
        band = _signal_band(p)

        decision = "ENTER" if p >= 75 and q.get("strong_eligible") else ("WATCH" if p >= 65 else "PASS")
        if half_time and market in {"GOAL_NEXT_5", "GOAL_NEXT_10"}:
            decision = "WAIT"

        out.append({
            "market": market,
            "title": title,
            "probability": p,
            "safe": s.get("safe_probability"),
            "live": s.get("live_probability"),
            "emoji": band["emoji"],
            "strength": band["label"],
            "risk": "средний" if p >= 75 else ("повышенный" if p >= 70 else "высокий"),
            "decision": decision,
            "priority": _goal_market_priority(title),
        })

    out.sort(key=lambda x: (x["probability"], x["priority"]), reverse=True)
    return out


def _old_style_reasoning(rep: Dict[str, Any], board: List[Dict[str, Any]]) -> Dict[str, Any]:
    match = rep.get("match") or {}
    metrics = rep.get("metrics") or {}
    pressure = rep.get("pressure") or {}
    picture = _game_picture(metrics, match, pressure)
    score = match.get("score") or {}
    minute = int(match.get("minute") or 0)

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

    best = board[0] if board else None
    backup = board[1] if len(board) > 1 else None

    reasons_for = []
    reasons_against = []

    if float(pressure.get("total") or 0) >= 60:
        reasons_for.append("давление высокое")
    if _metric_present(metrics, "shots_on_target") and sum(_pair(metrics, "shots_on_target")) >= 4:
        reasons_for.append("матч даёт реальные удары в створ")
    if _metric_present(metrics, "xg") and sum(_pair(metrics, "xg")) >= 0.9:
        reasons_for.append("xG подтверждает голевые моменты")
    if _metric_present(metrics, "touches_in_box") and sum(_pair(metrics, "touches_in_box")) >= 14:
        reasons_for.append("команды регулярно доходят до штрафной")

    if minute <= 12:
        reasons_against.append("ранняя минута — выборка ещё маленькая")
    if float(pressure.get("total") or 0) < 40:
        reasons_against.append("темп пока недостаточный")
    stage = str(match.get("stage") or "").lower()
    if "half time" in stage or "halftime" in stage:
        reasons_against.append("перерыв — следующий live-импульс ещё не подтверждён")
    if not reasons_against:
        reasons_against.append("главный риск — резкое падение темпа или закрытие игры")

    if best:
        if best["decision"] == "ENTER":
            voice = f"Мне нравится {best['title']}: {best['probability']}%. По игре это сейчас самый чистый вариант."
        elif best["decision"] in {"WATCH", "WAIT"}:
            voice = f"Сигнал есть по {best['title']} ({best['probability']}%), но сейчас я бы дождался подтверждения."
        else:
            voice = "Сигнал не дотягивает — не натягиваем."
    else:
        voice = "Сейчас голевой рынок не даёт честных 65% — пропускаем."

    return {
        "what_i_see": [
            picture.get("control"),
            picture.get("score_story"),
            picture.get("rhythm"),
        ],
        "facts": facts[:7],
        "who_is_closer": picture.get("closer_team"),
        "our_voice": voice,
        "best": best,
        "backup": backup,
        "for_signal": reasons_for[:4],
        "against_signal": reasons_against[:3],
        "what_confirms": "Ещё один створ/опасный момент, рост xG или серия заходов в штрафную.",
        "what_breaks": "Падение темпа, длинная пауза, удаление, тактическое закрытие матча или свежий гол до входа.",
    }


def _goal_hunter_view(rep: Dict[str, Any]) -> Dict[str, Any]:
    board = _classic_goal_options(rep)
    reasoning = _old_style_reasoning(rep, board)
    match = rep.get("match") or {}
    score = match.get("score") or {}

    # Special handling: if BTTS is actually "one remaining team must score", explain it.
    for item in board:
        if item["market"] == "BTTS":
            if int(score.get("home") or 0) == 0 and int(score.get("away") or 0) > 0:
                item["meaning_now"] = f"Для ОЗ нужен гол {match.get('home')}"
            elif int(score.get("away") or 0) == 0 and int(score.get("home") or 0) > 0:
                item["meaning_now"] = f"Для ОЗ нужен гол {match.get('away')}"

    return {
        "match": f"{match.get('home')} — {match.get('away')}",
        "minute": match.get("minute"),
        "score": score,
        "stage": match.get("stage"),
        "goal_board_65_99": board,
        "OUR_THINKING": reasoning,
        "OUTCOMES_1X2": _outcome_estimate(match, rep.get("pressure") or {}),
        "final_decision": (
            "PASS — не натягиваем."
            if not board else
            f"{board[0]['decision']} — {board[0]['title']} — {board[0]['probability']}%"
        ),
    }



def _odds_balance_score(m: Dict[str, Any]) -> float:
    """Cheap prefilter signal from 1X2 odds: balanced/live games tend to stay competitive."""
    odds = m.get("live_odds_1x2") or {}
    vals = []
    for k in ("1", "X", "2"):
        v = safe_float(odds.get(k))
        if v and v > 1:
            vals.append(v)
    if len(vals) < 2:
        return 0.0
    spread = max(vals) - min(vals)
    if spread <= 1.5:
        return 10.0
    if spread <= 3.0:
        return 6.0
    if spread <= 6.0:
        return 2.0
    return -2.0


def smart_scout_rank(m: Dict[str, Any]) -> float:
    """
    Better live prefilter.
    It does NOT predict a goal by itself; it only decides which matches deserve expensive deep analysis.
    """
    minute = int(m.get("minute") or 0)
    score = m.get("score") or {}
    h = int(score.get("home") or 0)
    a = int(score.get("away") or 0)
    total = h + a
    diff = abs(h - a)
    stage = str(m.get("stage") or "").lower()

    s = 0.0

    # Best live windows for goal hunting.
    if 16 <= minute <= 43:
        s += 34
    elif 8 <= minute <= 15:
        s += 20
    elif 46 <= minute <= 72:
        s += 32
    elif 73 <= minute <= 84:
        s += 26
    elif 85 <= minute <= 91:
        s += 12
    elif minute < 8:
        s -= 12

    # Halftime should not consume deep-analysis quota ahead of active play.
    if "half time" in stage or "halftime" in stage:
        s -= 18

    # Score states that often keep motivation alive.
    if total == 0:
        s += 14
    elif total == 1:
        s += 18
    elif total == 2 and diff <= 1:
        s += 12
    elif total <= 3 and diff <= 1:
        s += 8
    elif diff >= 3:
        s -= 10

    # Competitive scoreline > already dead game.
    if diff == 0:
        s += 10
    elif diff == 1:
        s += 8
    elif diff == 2:
        s -= 2

    # Red card can create pressure, but also destabilizes model assumptions.
    rc = m.get("red_cards") or {}
    reds = int(rc.get("home") or 0) + int(rc.get("away") or 0)
    if reds == 1:
        s += 3
    elif reds >= 2:
        s -= 5

    s += _odds_balance_score(m)

    if m.get("match_id"):
        s += 5
    if m.get("is_in_progress"):
        s += 7

    # Small bonus for known tournament metadata.
    if m.get("tournament"):
        s += 2

    return round1(s)


def _scout_bucket(m: Dict[str, Any]) -> str:
    minute = int(m.get("minute") or 0)
    stage = str(m.get("stage") or "").lower()
    if "half time" in stage or "halftime" in stage:
        return "HT"
    if minute <= 15:
        return "EARLY_1H"
    if minute <= 45:
        return "PRIME_1H"
    if minute <= 72:
        return "PRIME_2H"
    if minute <= 84:
        return "LATE_2H"
    return "VERY_LATE"


def _diversified_scout_selection(matches: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """
    Do not waste the whole quota on similar matches or one league.
    Prefer prime 1H/2H windows, then broaden.
    """
    ranked = sorted(matches, key=smart_scout_rank, reverse=True)
    target = max(1, min(int(limit), 20))

    quotas = {
        "PRIME_1H": max(3, target // 3),
        "PRIME_2H": max(3, target // 3),
        "LATE_2H": max(2, target // 5),
        "EARLY_1H": max(1, target // 8),
        "VERY_LATE": max(1, target // 8),
        "HT": 1,
    }

    selected = []
    per_bucket = {k: 0 for k in quotas}
    per_tournament = {}

    # Pass 1: balanced, diversified.
    for m in ranked:
        if len(selected) >= target:
            break
        bucket = _scout_bucket(m)
        tid = str(m.get("tournament_id") or m.get("tournament") or "")
        if per_bucket.get(bucket, 0) >= quotas.get(bucket, 0):
            continue
        if tid and per_tournament.get(tid, 0) >= 2:
            continue
        selected.append(m)
        per_bucket[bucket] = per_bucket.get(bucket, 0) + 1
        if tid:
            per_tournament[tid] = per_tournament.get(tid, 0) + 1

    # Pass 2: fill remaining slots by pure scout rank.
    ids = {str(x.get("match_id")) for x in selected}
    for m in ranked:
        if len(selected) >= target:
            break
        if str(m.get("match_id")) in ids:
            continue
        selected.append(m)
        ids.add(str(m.get("match_id")))

    return selected



# -------------------------
# V5.2 FINAL — scan memory + short-term momentum + two-stage live collection
# -------------------------

def _load_scan_state() -> Dict[str, Any]:
    try:
        with open(SCAN_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_scan_state(state: Dict[str, Any]) -> None:
    try:
        tmp = SCAN_STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, SCAN_STATE_PATH)
    except Exception:
        pass


def _metric_snapshot(metrics: Dict[str, Any], key: str) -> Optional[Dict[str, float]]:
    m = metrics.get(key) or {}
    if not isinstance(m, dict) or not m.get("present"):
        return None
    try:
        return {
            "home": float(m.get("home") or 0),
            "away": float(m.get("away") or 0),
        }
    except Exception:
        return None


def _build_scan_snapshot(rep: Dict[str, Any]) -> Dict[str, Any]:
    match = rep.get("match") or {}
    metrics = rep.get("metrics") or {}
    return {
        "ts": time.time(),
        "minute": int(match.get("minute") or 0),
        "score": match.get("score") or {},
        "xg": _metric_snapshot(metrics, "xg"),
        "shots": _metric_snapshot(metrics, "shots"),
        "shots_on_target": _metric_snapshot(metrics, "shots_on_target"),
        "touches_in_box": _metric_snapshot(metrics, "touches_in_box"),
        "shots_in_box": _metric_snapshot(metrics, "shots_in_box"),
        "big_chances": _metric_snapshot(metrics, "big_chances"),
        "corners": _metric_snapshot(metrics, "corners"),
        "dangerous_attacks": _metric_snapshot(metrics, "dangerous_attacks"),
        "pressure": rep.get("pressure") or {},
    }


def _delta_pair(now: Optional[Dict[str, float]], prev: Optional[Dict[str, float]]) -> Optional[Dict[str, float]]:
    if not now or not prev:
        return None
    return {
        "home": round1(now.get("home", 0) - prev.get("home", 0)),
        "away": round1(now.get("away", 0) - prev.get("away", 0)),
    }


def _momentum_from_history(rep: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compare current deep report with the previous scan.
    This is the missing 'is pressure rising right now?' layer.
    """
    current = _build_scan_snapshot(rep)
    if not prev:
        return {
            "available": False,
            "label": "нет предыдущего скана",
            "score": 0.0,
            "current": current,
        }

    dt_sec = max(1.0, current["ts"] - float(prev.get("ts") or current["ts"]))
    dmin = int(current.get("minute") or 0) - int(prev.get("minute") or 0)
    # Ignore stale history or impossible minute jumps.
    if dt_sec > 45 * 60 or dmin < 0 or dmin > 20:
        return {
            "available": False,
            "label": "предыдущий скан слишком старый",
            "score": 0.0,
            "current": current,
        }

    deltas = {}
    for k in ("xg", "shots", "shots_on_target", "touches_in_box", "shots_in_box", "big_chances", "corners"):
        deltas[k] = _delta_pair(current.get(k), prev.get(k))

    home_m = away_m = 0.0
    weights = {
        "xg": 34.0,
        "shots_on_target": 26.0,
        "touches_in_box": 18.0,
        "shots_in_box": 10.0,
        "shots": 6.0,
        "big_chances": 8.0,
        "corners": 4.0,
    }
    normalizers = {
        "xg": 0.45,
        "shots_on_target": 2.0,
        "touches_in_box": 6.0,
        "shots_in_box": 4.0,
        "shots": 5.0,
        "big_chances": 1.0,
        "corners": 2.0,
    }

    for key, weight in weights.items():
        d = deltas.get(key)
        if not d:
            continue
        norm = normalizers[key]
        home_m += weight * min(1.5, max(0.0, d["home"]) / norm)
        away_m += weight * min(1.5, max(0.0, d["away"]) / norm)

    # Pressure delta if available.
    cp = current.get("pressure") or {}
    pp = prev.get("pressure") or {}
    try:
        pdelta_h = float(cp.get("home") or 0) - float(pp.get("home") or 0)
        pdelta_a = float(cp.get("away") or 0) - float(pp.get("away") or 0)
        home_m += max(0.0, pdelta_h) * 0.35
        away_m += max(0.0, pdelta_a) * 0.35
    except Exception:
        pass

    total = home_m + away_m
    if total >= 75:
        label = "резкий рост голевого давления"
    elif total >= 45:
        label = "давление заметно растёт"
    elif total >= 22:
        label = "есть свежий атакующий импульс"
    else:
        label = "свежего ускорения почти нет"

    if home_m - away_m >= 12:
        rising_side = (rep.get("match") or {}).get("home")
    elif away_m - home_m >= 12:
        rising_side = (rep.get("match") or {}).get("away")
    else:
        rising_side = "обе/неясно"

    return {
        "available": True,
        "seconds_since_previous": int(dt_sec),
        "minutes_since_previous": dmin,
        "label": label,
        "rising_side": rising_side,
        "home_momentum": round1(home_m),
        "away_momentum": round1(away_m),
        "score": round1(min(99.0, total)),
        "deltas": deltas,
        "current": current,
    }


def _apply_momentum_to_goal_board(board: List[Dict[str, Any]], momentum: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Momentum is a modifier, not a replacement for the base model.
    Boost only when fresh attacking growth is observed; cap total display at 99.
    """
    if not momentum.get("available"):
        return board

    mscore = float(momentum.get("score") or 0)
    if mscore < 15:
        return board

    boost = 0.0
    if mscore >= 75:
        boost = 6.0
    elif mscore >= 45:
        boost = 4.0
    elif mscore >= 25:
        boost = 2.0

    updated = []
    for item in board:
        x = dict(item)
        market = x.get("market")
        local_boost = boost
        if market == "GOAL_NEXT_5":
            local_boost *= 1.10
        elif market == "GOAL_NEXT_10":
            local_boost *= 1.00
        elif market == "GOAL_BEFORE_FULLTIME":
            local_boost *= 0.75
        elif market == "BTTS":
            local_boost *= 0.55

        p = round1(clamp(float(x.get("probability") or 0) + local_boost, 0, 99))
        x["base_probability"] = x.get("probability")
        x["momentum_boost"] = round1(local_boost)
        x["probability"] = p
        band = _signal_band(p)
        x["emoji"] = band["emoji"]
        x["strength"] = band["label"]
        if x.get("decision") != "WAIT":
            x["decision"] = "ENTER" if p >= 75 else ("WATCH" if p >= 65 else "PASS")
        updated.append(x)

    updated.sort(key=lambda x: (x["probability"], x.get("priority", 0)), reverse=True)
    return updated


def _final_goal_hunter_view(rep: Dict[str, Any], momentum: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    view = _goal_hunter_view(rep)
    board = view.get("goal_board_65_99") or []

    if momentum:
        board = _apply_momentum_to_goal_board(board, momentum)
        view["goal_board_65_99"] = board
        view["SHORT_TERM_MOMENTUM"] = momentum

        # Rebuild human reasoning so the new best option is reflected.
        reasoning = _old_style_reasoning(rep, board)
        if momentum.get("available"):
            reasoning["fresh_momentum"] = {
                "label": momentum.get("label"),
                "rising_side": momentum.get("rising_side"),
                "score": momentum.get("score"),
                "deltas": momentum.get("deltas"),
            }
            if momentum.get("score", 0) >= 45:
                reasoning["our_voice"] += " Плюс есть свежий рост давления относительно прошлого скана."
        view["OUR_THINKING"] = reasoning
        view["final_decision"] = (
            "PASS — не натягиваем."
            if not board else
            f"{board[0]['decision']} — {board[0]['title']} — {board[0]['probability']}%"
        )

    return view


def _stage1_live_pool(matches: List[Dict[str, Any]], max_pool: int = 80) -> List[Dict[str, Any]]:
    """
    Stage 1: scan the whole live list cheaply and keep a broad pool.
    This avoids deep-calling random matches too early.
    """
    active = []
    for m in matches:
        if not m.get("minute_valid", True):
            continue
        stage = str(m.get("stage") or "").lower()
        if any(x in stage for x in ("finished", "cancelled", "postponed", "not started")):
            continue
        minute = int(m.get("minute") or 0)
        if minute <= 0 or minute > 100:
            continue
        active.append(m)

    ranked = sorted(active, key=smart_scout_rank, reverse=True)
    return ranked[:max(1, min(int(max_pool), 120))]


def _stage2_deep_selection(pool: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """
    Stage 2: diversified deep-analysis set.
    Use the smarter V5.1 selector on the already-cleaned pool.
    """
    return _diversified_scout_selection(pool, max(1, min(int(limit), 20)))


# ============================================================
# V5.7.5j — proactive deep budget + rotation queue
# ============================================================

def _v575j_load_rotation_queue() -> Dict[str, Dict[str, Any]]:
    try:
        with open(DEEP_ROTATION_QUEUE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _v575j_save_rotation_queue(queue: Dict[str, Dict[str, Any]]) -> None:
    try:
        rows = sorted(
            queue.items(),
            key=lambda kv: (
                int((kv[1] or {}).get("deferred_scans") or 0),
                float((kv[1] or {}).get("last_scout_score") or 0),
                float((kv[1] or {}).get("last_deferred_at") or 0),
            ),
            reverse=True,
        )[:V575J_ROTATION_MAX]
        payload = {k: v for k, v in rows}
        tmp = DEEP_ROTATION_QUEUE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, DEEP_ROTATION_QUEUE_PATH)
    except Exception:
        pass


def _v575j_prune_rotation_queue(
    queue: Dict[str, Dict[str, Any]],
    live_matches: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    now = time.time()
    live_ids = {str(m.get("match_id") or "") for m in live_matches if m.get("match_id")}
    out: Dict[str, Dict[str, Any]] = {}
    for mid, row in (queue or {}).items():
        if not mid or mid not in live_ids:
            continue
        age = now - float((row or {}).get("last_deferred_at") or now)
        if age > V575J_ROTATION_KEEP_SECONDS:
            continue
        out[mid] = dict(row or {})
    return out


def _v575j_is_priority_first_half(m: Dict[str, Any]) -> bool:
    """V5.7.5l: tolerant 1H detector for the dedicated 5'–43' scanner.

    Minute is the primary gate. Explicit second-half/HT/finished stages are rejected.
    Common first-half labels are accepted, and an in-progress fallback is allowed
    when the provider omits a clean stage label.
    """
    minute = int(m.get("minute") or 0)
    if not (V575K_FIRST_HALF_START_MINUTE <= minute <= V575K_FIRST_HALF_END_MINUTE):
        return False

    stage = re.sub(r"\s+", " ", str(m.get("stage") or "").strip().lower())
    explicit_block = (
        "2nd half", "second half", "2h", "half time", "halftime", "ht",
        "finished", "full time", "ft", "extra time", "penalties"
    )
    if any(token == stage or token in stage for token in explicit_block):
        return False

    explicit_first = ("1st half", "first half", "1h", "1. half", "1 half")
    if any(token == stage or token in stage for token in explicit_first):
        return True

    # Provider occasionally supplies a generic live stage. The 5'–43' minute
    # window plus in-progress status is a safe fallback for first-half focus.
    return bool(m.get("is_in_progress", True))


def _v575m_is_priority_second_half(m: Dict[str, Any]) -> bool:
    """V5.7.5m: tolerant regulation-time 2H detector for the dedicated scanner."""
    minute = int(m.get("minute") or 0)
    if not (V575M_SECOND_HALF_START_MINUTE <= minute <= V575M_SECOND_HALF_END_MINUTE):
        return False

    stage = re.sub(r"\s+", " ", str(m.get("stage") or "").strip().lower())
    explicit_block = (
        "1st half", "first half", "1h", "1. half", "1 half",
        "half time", "halftime", "ht", "finished", "full time", "ft",
        "extra time", "penalties"
    )
    if any(token == stage or token in stage for token in explicit_block):
        return False

    explicit_second = ("2nd half", "second half", "2h", "2. half", "2 half")
    if any(token == stage or token in stage for token in explicit_second):
        return True

    # Generic LIVE fallback is accepted only inside the fixed 46'–90' window.
    return bool(m.get("is_in_progress", True))


def _v575k_normalize_focus(focus: Optional[str]) -> str:
    raw = str(focus or "balanced").strip().lower().replace("-", "_")
    if raw in {"1h", "firsthalf", "first_half", "fh", "first", "1"}:
        return "first_half"
    if raw in {"2h", "secondhalf", "second_half", "sh", "second", "2"}:
        return "second_half"
    return "balanced"


def _v575m_is_dedicated_half_focus(focus: Optional[str] = None) -> bool:
    mode = _v575k_normalize_focus(focus if focus is not None else (_SCAN_FOCUS or {}).get("mode"))
    return mode in {"first_half", "second_half"}


def _v575k_focus_mode() -> str:
    return _v575k_normalize_focus((_SCAN_FOCUS or {}).get("mode"))


def _v575k_rolling_minute_status() -> Dict[str, Any]:
    now = _v572_epoch()
    _v573_trim_calls(now)
    used = len(_PROVIDER_CALL_TIMES)
    limit = max(1, int(PROVIDER_MAX_CALLS_PER_MINUTE))
    remaining = max(0, limit - used)
    final_reserve = min(remaining, max(1, int(V575K_ROLLING_MINUTE_FINAL_RESERVE)))
    after_final = max(0, remaining - final_reserve)
    targeted_reserve = min(after_final, max(0, int(V575K_ROLLING_MINUTE_TARGETED_RESERVE)))
    normal_remaining = max(0, remaining - final_reserve - targeted_reserve)
    oldest_age = None
    seconds_until_one_slot = 0.0
    if used >= limit and _PROVIDER_CALL_TIMES:
        oldest = min(_PROVIDER_CALL_TIMES)
        oldest_age = max(0.0, now - oldest)
        seconds_until_one_slot = max(0.0, 60.0 - oldest_age)
    return {
        "used": used,
        "limit": limit,
        "remaining": remaining,
        "final_snapshot_reserve": final_reserve,
        "targeted_verify_reserve": targeted_reserve,
        "normal_calls_available_now": normal_remaining,
        "seconds_until_one_slot_if_full": round(seconds_until_one_slot, 1),
        "oldest_call_age_seconds": None if oldest_age is None else round(oldest_age, 1),
    }


def _v575j_rotation_boost(m: Dict[str, Any], queue: Dict[str, Dict[str, Any]]) -> float:
    row = queue.get(str(m.get("match_id") or "")) or {}
    deferred = max(0, int(row.get("deferred_scans") or 0))
    return min(V575J_ROTATION_BOOST_CAP, deferred * V575J_ROTATION_BOOST_PER_DEFER)


def _v575j_budgeted_rank(m: Dict[str, Any], queue: Dict[str, Dict[str, Any]]) -> float:
    score = float(smart_scout_rank(m) or 0)
    score += _v575j_rotation_boost(m, queue)
    # First-half windows decay quickly, so a small urgency bonus is justified.
    if _v575j_is_priority_first_half(m):
        score += 6.0
    return score


def _v575j_select_with_rotation(
    pool: List[Dict[str, Any]],
    limit: int,
    queue: Dict[str, Dict[str, Any]],
    focus: str = "balanced",
) -> List[Dict[str, Any]]:
    target = max(0, min(int(limit), 20))
    if target <= 0:
        return []

    focus_mode = _v575k_normalize_focus(focus)
    eligible_pool = list(pool)
    if focus_mode == "first_half":
        eligible_pool = [m for m in pool if _v575j_is_priority_first_half(m)]
    elif focus_mode == "second_half":
        eligible_pool = [m for m in pool if _v575m_is_priority_second_half(m)]

    ranked = sorted(eligible_pool, key=lambda m: _v575j_budgeted_rank(m, queue), reverse=True)
    first_half_target = target if focus_mode == "first_half" else min(
        target,
        max(1, int(math.ceil(target * V575J_FIRST_HALF_DEEP_SHARE))),
    )
    second_half_target = target if focus_mode == "second_half" else 0

    selected: List[Dict[str, Any]] = []
    ids = set()
    tournament_counts: Dict[str, int] = {}

    def can_add(m: Dict[str, Any], tournament_cap: int = 2) -> bool:
        mid = str(m.get("match_id") or "")
        if not mid or mid in ids:
            return False
        tid = str(m.get("tournament_id") or m.get("tournament") or "")
        if tid and tournament_counts.get(tid, 0) >= tournament_cap:
            return False
        return True

    def add(m: Dict[str, Any]) -> None:
        mid = str(m.get("match_id") or "")
        selected.append(m)
        ids.add(mid)
        tid = str(m.get("tournament_id") or m.get("tournament") or "")
        if tid:
            tournament_counts[tid] = tournament_counts.get(tid, 0) + 1

    # Pass 1A: preserve balanced 1H quota; dedicated first-half mode makes it 100%.
    if focus_mode in {"balanced", "first_half"}:
        for m in ranked:
            if len(selected) >= first_half_target:
                break
            if not _v575j_is_priority_first_half(m) or not can_add(m):
                continue
            add(m)

    # Pass 1B: dedicated second-half mode -> 100% of safe deep slots are 46'–90'.
    if focus_mode == "second_half":
        for m in ranked:
            if len(selected) >= second_half_target:
                break
            if not _v575m_is_priority_second_half(m) or not can_add(m):
                continue
            add(m)

    # Pass 2: highest priority within the already focus-filtered pool.
    for m in ranked:
        if len(selected) >= target:
            break
        if not can_add(m):
            continue
        add(m)

    # Pass 3: if tournament caps prevented filling, relax diversity but not duplicates.
    for m in ranked:
        if len(selected) >= target:
            break
        mid = str(m.get("match_id") or "")
        if not mid or mid in ids:
            continue
        add(m)

    return selected


def _v575j_deep_budget_plan(requested_limit: int, focus: str = "balanced") -> Dict[str, Any]:
    focus_mode = _v575k_normalize_focus(focus)
    st = _v573_scan_budget_status()
    used = int(st.get("used") or 0)
    normal_ceiling = int(st.get("normal_call_ceiling") or 0)
    scan_normal_remaining = max(0, normal_ceiling - used)
    requested = max(0, min(int(requested_limit), 20))

    # V5.7.5k root fix: per-scan budget alone is insufficient. If the rolling
    # 60-second window is already near 24/24, admitting 18 deep matches merely
    # turns them into LOCAL_RATE_LIMIT failures. Plan against BOTH ceilings.
    rolling = _v575k_rolling_minute_status()
    rolling_normal_remaining = int(rolling.get("normal_calls_available_now") or 0)
    normal_remaining = min(scan_normal_remaining, rolling_normal_remaining)

    # In first-half focus, maximize coverage: core stats first. Expensive
    # details/lineups/odds are not pre-reserved; they may use only genuine
    # leftover capacity after admitted core calls. Balanced mode preserves j's
    # one-candidate Tier-2 reserve.
    if focus_mode == "first_half":
        desired_reserve = V575K_FIRST_HALF_ENRICH_RESERVE
    elif focus_mode == "second_half":
        desired_reserve = V575M_SECOND_HALF_ENRICH_RESERVE
    else:
        desired_reserve = V575J_ENRICHMENT_RESERVE
    reserve = min(desired_reserve, max(0, normal_remaining - 1))
    effective = min(requested, max(0, normal_remaining - reserve))
    if requested > 0 and normal_remaining > 0 and effective <= 0:
        effective = 1
        reserve = max(0, normal_remaining - effective)

    if rolling_normal_remaining < scan_normal_remaining:
        binding = "ROLLING_60S_LIMIT"
    elif scan_normal_remaining < rolling_normal_remaining:
        binding = "PER_SCAN_LIMIT"
    else:
        binding = "BOTH_OR_EQUAL"

    first_half_share = 1.0 if focus_mode == "first_half" else (0.0 if focus_mode == "second_half" else V575J_FIRST_HALF_DEEP_SHARE)
    second_half_share = 1.0 if focus_mode == "second_half" else 0.0
    return {
        "focus": focus_mode,
        "requested_limit": requested,
        "effective_core_slots": effective,
        "normal_budget_used_before_deep": used,
        "normal_call_ceiling": normal_ceiling,
        "scan_normal_calls_remaining_before_deep": scan_normal_remaining,
        "rolling_minute": rolling,
        "rolling_normal_calls_remaining_before_deep": rolling_normal_remaining,
        "effective_normal_calls_available": normal_remaining,
        "binding_constraint": binding,
        "enrichment_reserve": reserve,
        "expected_core_plus_enrichment_calls": effective + reserve,
        "first_half_share_target": first_half_share,
        "first_half_slots_target": min(
            effective,
            int(math.ceil(effective * first_half_share)) if effective else 0,
        ),
        "first_half_window": [V575K_FIRST_HALF_START_MINUTE, V575K_FIRST_HALF_END_MINUTE],
        "second_half_share_target": second_half_share,
        "second_half_slots_target": effective if focus_mode == "second_half" else 0,
        "second_half_window": [V575M_SECOND_HALF_START_MINUTE, V575M_SECOND_HALF_END_MINUTE],
        "first_half_enrich_threshold": V575K_FIRST_HALF_ENRICH_THRESHOLD,
        "second_half_enrich_threshold": V575M_SECOND_HALF_ENRICH_THRESHOLD,
        "final_snapshot_reserve": PROVIDER_FINAL_SNAPSHOT_RESERVE,
        "targeted_verify_reserve": PROVIDER_TARGETED_VERIFY_RESERVE,
        "proactive_budgeting": True,
        "rolling_minute_budgeting": True,
        "stats_first": focus_mode in {"first_half", "second_half"},
    }


def _v575j_mark_deferred(
    queue: Dict[str, Dict[str, Any]],
    m: Dict[str, Any],
    reason: str = "DEFERRED_BY_BUDGET",
) -> Dict[str, Any]:
    mid = str(m.get("match_id") or "")
    if not mid:
        return {}
    now = time.time()
    old = queue.get(mid) or {}
    row = {
        "match_id": mid,
        "home": m.get("home"),
        "away": m.get("away"),
        "tournament": m.get("tournament"),
        "bucket": _scout_bucket(m),
        "last_minute": m.get("minute"),
        "last_score": m.get("score"),
        "first_deferred_at": float(old.get("first_deferred_at") or now),
        "last_deferred_at": now,
        "deferred_scans": int(old.get("deferred_scans") or 0) + 1,
        "last_scout_score": float(smart_scout_rank(m) or 0),
        "priority_boost_next_scan": min(
            V575J_ROTATION_BOOST_CAP,
            (int(old.get("deferred_scans") or 0) + 1) * V575J_ROTATION_BOOST_PER_DEFER,
        ),
        "reason": reason,
    }
    queue[mid] = row
    return row


def _v575j_queue_item_public(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "match_id": row.get("match_id"),
        "match": f"{row.get('home')} — {row.get('away')}",
        "minute": row.get("last_minute"),
        "score": row.get("last_score"),
        "bucket": row.get("bucket"),
        "deferred_scans": row.get("deferred_scans"),
        "priority_boost_next_scan": row.get("priority_boost_next_scan"),
        "reason": row.get("reason"),
    }


# ============================================================
# V5.3 COMPLETE LIVE REACTOR
# Fixes:
# - Missing match from the second live list is NOT an automatic hard block.
# - Fuzzy/name fallback is used when match_id disappears from provider snapshot.
# - Every successfully parsed deep report is saved to scan memory BEFORE freshness filtering.
# - 55–64% rising radar is retained for the next scan, but never promoted to ENTER.
# - Freshness-unconfirmed candidates are downgraded to WATCH.
# - Strong score/minute conflicts remain hard blocks.
# - Human-readable "thinking" includes fresh deltas and why one market is preferred.
# ============================================================

RADAR_MIN = 55.0
VISIBLE_MIN = 65.0
ENTER_MIN = 75.0


def _v53_norm_name(value: Any) -> str:
    s = str(value or "").lower().strip()
    # Common harmless presentation differences.
    repl = {
        "women": "w", "woman": "w", "ladies": "w",
        "(w)": "w", " w ": " w ",
        "fc": "", "cf": "", "fk": "", "sc": "", "ac": "",
        "u-20": "u20", "u-21": "u21", "u-23": "u23",
    }
    for a, b in repl.items():
        s = s.replace(a, b)
    return "".join(ch for ch in s if ch.isalnum())


def _v53_name_similarity(a: Any, b: Any) -> float:
    a = _v53_norm_name(a)
    b = _v53_norm_name(b)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        return min(len(a), len(b)) / max(len(a), len(b))
    # Cheap character overlap; enough only as a fallback after exact id failed.
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _v53_find_final_match(analyzed_match: Dict[str, Any], match_id: str, final_matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    # 1. Exact provider ID.
    for m in final_matches:
        if str(m.get("match_id") or "") == str(match_id):
            return {"match": m, "method": "MATCH_ID", "confidence": 1.0}

    # 2. Team-name fallback. This handles volatile live-list snapshots.
    ah = analyzed_match.get("home")
    aa = analyzed_match.get("away")
    amin = int(analyzed_match.get("minute") or 0)
    best = None
    best_score = 0.0

    for m in final_matches:
        hs = _v53_name_similarity(ah, m.get("home"))
        aws = _v53_name_similarity(aa, m.get("away"))
        direct = (hs + aws) / 2.0

        # Also test reversed teams, but penalize heavily.
        rhs = _v53_name_similarity(ah, m.get("away"))
        raws = _v53_name_similarity(aa, m.get("home"))
        reverse = ((rhs + raws) / 2.0) - 0.20

        name_score = max(direct, reverse)
        mmin = int(m.get("minute") or 0)
        minute_penalty = min(0.30, abs(mmin - amin) * 0.015) if amin and mmin else 0.08
        score = name_score - minute_penalty

        if score > best_score:
            best_score = score
            best = m

    if best is not None and best_score >= 0.58:
        return {
            "match": best,
            "method": "TEAM_NAME_FALLBACK",
            "confidence": round1(min(0.99, best_score)),
        }

    return {"match": None, "method": "NOT_FOUND", "confidence": 0.0}


def _v53_freshness_check(
    analyzed_match: Dict[str, Any],
    final_match: Optional[Dict[str, Any]],
    resolution_method: str = "MATCH_ID",
    confidence: float = 1.0,
) -> Dict[str, Any]:
    """
    Three-state freshness:
      CONFIRMED   -> can ENTER if model otherwise permits.
      UNCONFIRMED -> visible, memory retained, ENTER downgraded to WATCH.
      BLOCKED     -> score conflict, invalid minute, not in progress, or excessive drift.
    """
    if final_match is None:
        return {
            "status": "UNCONFIRMED",
            "ok_for_display": True,
            "ok_for_enter": False,
            "reason": "FINAL_LIVE_SNAPSHOT_MISSING",
            "resolution_method": resolution_method,
            "confidence": confidence,
            "minute_drift": None,
            "score_changed": False,
        }

    analyzed_score = analyzed_match.get("score") or {}
    fresh_score = final_match.get("score") or {}
    analyzed_minute = int(analyzed_match.get("minute") or 0)
    fresh_minute = int(final_match.get("minute") or 0)
    drift = fresh_minute - analyzed_minute

    if not score_equal(analyzed_score, fresh_score):
        _GOAL_COOLDOWN_UNTIL[str(final_match.get("match_id") or "")] = time.time() + GOAL_COOLDOWN_SECONDS
        return {
            "status": "BLOCKED",
            "ok_for_display": False,
            "ok_for_enter": False,
            "reason": "STALE_AFTER_SCORE_CHANGE",
            "score_changed": True,
            "analyzed_score": analyzed_score,
            "fresh_score": fresh_score,
            "analyzed_minute": analyzed_minute,
            "fresh_minute": fresh_minute,
            "minute_drift": drift,
            "resolution_method": resolution_method,
            "confidence": confidence,
        }

    if not final_match.get("is_in_progress"):
        return {
            "status": "BLOCKED",
            "ok_for_display": False,
            "ok_for_enter": False,
            "reason": "MATCH_NOT_IN_PROGRESS",
            "score_changed": False,
            "minute_drift": drift,
            "resolution_method": resolution_method,
            "confidence": confidence,
        }

    if not final_match.get("minute_valid", True):
        return {
            "status": "BLOCKED",
            "ok_for_display": False,
            "ok_for_enter": False,
            "reason": "INVALID_FINAL_MINUTE",
            "score_changed": False,
            "minute_drift": drift,
            "resolution_method": resolution_method,
            "confidence": confidence,
        }

    # Negative drift can happen due to snapshot quirks; large positive drift means analysis is old.
    if drift > max(FINAL_MINUTE_DRIFT_MAX, 8):
        return {
            "status": "BLOCKED",
            "ok_for_display": False,
            "ok_for_enter": False,
            "reason": "FINAL_SNAPSHOT_TOO_OLD",
            "score_changed": False,
            "minute_drift": drift,
            "resolution_method": resolution_method,
            "confidence": confidence,
        }

    state = register_live_state(final_match)
    if state.get("cooldown_active"):
        return {
            "status": "UNCONFIRMED",
            "ok_for_display": True,
            "ok_for_enter": False,
            "reason": "POST_GOAL_COOLDOWN",
            "score_changed": False,
            "minute_drift": drift,
            "resolution_method": resolution_method,
            "confidence": confidence,
            "cooldown_remaining_seconds": state.get("cooldown_remaining_seconds"),
        }

    return {
        "status": "CONFIRMED",
        "ok_for_display": True,
        "ok_for_enter": True,
        "reason": "FRESH",
        "score_changed": False,
        "fresh_score": fresh_score,
        "fresh_minute": fresh_minute,
        "minute_drift": drift,
        "resolution_method": resolution_method,
        "confidence": confidence,
    }


def _v53_goal_options(rep: Dict[str, Any], min_probability: float = RADAR_MIN) -> List[Dict[str, Any]]:
    """Same goal universe as Goal Hunter, but supports a 55–64% pre-signal radar."""
    q = rep.get("data_quality") or {}
    signals = rep.get("all_signals") or []
    stage = str((rep.get("match") or {}).get("stage") or "").lower()
    half_time = "half time" in stage or "halftime" in stage

    out = []
    seen = set()
    allowed = {
        "GOAL_BEFORE_FULLTIME", "GOAL_BEFORE_HALFTIME",
        "GOAL_NEXT_5", "GOAL_NEXT_10", "TEAM_GOAL", "BTTS", "OVER_UNDER"
    }

    for s in signals:
        market = str(s.get("market") or "")
        if market not in allowed:
            continue

        selection = str(s.get("selection") or "")
        if market == "OVER_UNDER" and not selection.lower().startswith("over"):
            continue
        if s.get("data_quality_blocked"):
            continue

        p = float(_effective_probability(s) or 0)
        if p < float(min_probability):
            continue

        if market == "GOAL_BEFORE_HALFTIME":
            title = "Гол в 1-м тайме"
        elif market == "GOAL_BEFORE_FULLTIME":
            title = "Ещё минимум 1 гол до конца"
        elif market == "GOAL_NEXT_5":
            title = "Гол в ближайшие 5 минут"
        elif market == "GOAL_NEXT_10":
            title = "Гол в ближайшие 10 минут"
        elif market == "BTTS":
            title = "ОЗ — Да"
        elif market == "TEAM_GOAL":
            title = selection
        else:
            title = selection

        key = (market, title)
        if key in seen:
            continue
        seen.add(key)

        band = _signal_band(max(VISIBLE_MIN, p)) if p >= VISIBLE_MIN else {
            "emoji": "⚪", "label": "формируется"
        }

        if p >= ENTER_MIN and q.get("strong_eligible"):
            decision = "ENTER"
        elif p >= VISIBLE_MIN:
            decision = "WATCH"
        else:
            decision = "RADAR"

        if half_time and market in {"GOAL_NEXT_5", "GOAL_NEXT_10"}:
            decision = "WAIT"

        out.append({
            "market": market,
            "title": title,
            "probability": round1(p),
            "safe": s.get("safe_probability"),
            "live": s.get("live_probability"),
            "emoji": band["emoji"],
            "strength": band["label"],
            "decision": decision,
            "risk": (
                "средний" if p >= 75 else
                "повышенный" if p >= 65 else
                "наблюдение"
            ),
            "priority": _goal_market_priority(title),
        })

    out.sort(key=lambda x: (x["probability"], x["priority"]), reverse=True)
    return out


def _v53_apply_freshness_to_board(board: List[Dict[str, Any]], freshness: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    for item in board:
        x = dict(item)
        x["freshness"] = freshness.get("status")
        x["freshness_reason"] = freshness.get("reason")

        # Crucial safety rule:
        # missing from final list no longer deletes the candidate,
        # but it cannot be an ENTER until confirmed.
        if not freshness.get("ok_for_enter") and x.get("decision") == "ENTER":
            x["decision"] = "WATCH"
            x["freshness_downgrade"] = True
        result.append(x)
    return result


def _v53_human_view(
    rep: Dict[str, Any],
    momentum: Dict[str, Any],
    freshness: Dict[str, Any],
) -> Dict[str, Any]:
    match = rep.get("match") or {}
    score = match.get("score") or {}

    board55 = _v53_goal_options(rep, RADAR_MIN)
    board55 = _apply_momentum_to_goal_board(board55, momentum)
    board55 = _v53_apply_freshness_to_board(board55, freshness)

    visible = [x for x in board55 if float(x.get("probability") or 0) >= VISIBLE_MIN]
    radar = [x for x in board55 if RADAR_MIN <= float(x.get("probability") or 0) < VISIBLE_MIN]

    # Explain BTTS as the remaining team goal when applicable.
    for item in visible + radar:
        if item.get("market") == "BTTS":
            if int(score.get("home") or 0) == 0 and int(score.get("away") or 0) > 0:
                item["meaning_now"] = f"Для ОЗ нужен гол {match.get('home')}"
            elif int(score.get("away") or 0) == 0 and int(score.get("home") or 0) > 0:
                item["meaning_now"] = f"Для ОЗ нужен гол {match.get('away')}"

    reasoning = _old_style_reasoning(rep, visible if visible else radar)

    fresh_note = (
        "финальный live подтверждён"
        if freshness.get("status") == "CONFIRMED"
        else "финальный live не подтверждён — вход автоматически понижен до WATCH"
    )

    reasoning["freshness"] = fresh_note
    reasoning["freshness_method"] = freshness.get("resolution_method")
    reasoning["freshness_reason"] = freshness.get("reason")

    if momentum.get("available"):
        reasoning["fresh_momentum"] = {
            "label": momentum.get("label"),
            "rising_side": momentum.get("rising_side"),
            "score": momentum.get("score"),
            "minutes_since_previous": momentum.get("minutes_since_previous"),
            "deltas": momentum.get("deltas"),
        }

        if float(momentum.get("score") or 0) >= 45:
            reasoning["our_voice"] += (
                f" Свежая динамика подтверждает ускорение: {momentum.get('label')}; "
                f"ближе по импульсу — {momentum.get('rising_side')}."
            )
        elif float(momentum.get("score") or 0) < 22:
            reasoning["our_voice"] += " Но по сравнению с прошлым сканом свежего ускорения почти нет."

    best = visible[0] if visible else (radar[0] if radar else None)
    backup = visible[1] if len(visible) > 1 else None

    reasoning["best"] = best
    reasoning["backup"] = backup
    if best:
        if best["decision"] == "ENTER":
            reasoning["choice_between_markets"] = (
                f"Из доступных рынков сейчас первым выбрал бы: {best['title']} "
                f"({best['probability']}%)."
            )
        elif best["decision"] == "WATCH":
            reasoning["choice_between_markets"] = (
                f"Лучший вариант сейчас {best['title']} ({best['probability']}%), "
                "но подтверждения для входа пока недостаточно."
            )
        else:
            reasoning["choice_between_markets"] = (
                f"Пока только ранний радар: {best['title']} ({best['probability']}%)."
            )

    return {
        "match": f"{match.get('home')} — {match.get('away')}",
        "minute": match.get("minute"),
        "score": score,
        "stage": match.get("stage"),
        "freshness": freshness,
        "SHORT_TERM_MOMENTUM": momentum,
        "goal_board_65_99": visible,
        "RISING_RADAR_55_64": radar,
        "OUR_THINKING": reasoning,
        "OUTCOMES_1X2": _outcome_estimate(match, rep.get("pressure") or {}),
        "final_decision": (
            "PASS — пока даже радар ниже 55%."
            if not best else
            f"{best['decision']} — {best['title']} — {best['probability']}%"
        ),
    }


def _v53_report_strength(rep: Dict[str, Any], momentum: Optional[Dict[str, Any]] = None) -> float:
    opts = _v53_goal_options(rep, RADAR_MIN)
    p = float(opts[0].get("probability") or 0) if opts else 0.0
    m = float((momentum or {}).get("score") or 0)
    return round1(p + min(10.0, m * 0.08))


def _v53_compact_deep_summary(rep: Dict[str, Any], momentum: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    match = rep.get("match") or {}
    metrics = rep.get("metrics") or {}

    def pair(key):
        p = _metric_pair(metrics, key)
        if not p:
            return None
        return [round1(p[0]), round1(p[1])]

    opts = _v53_goal_options(rep, RADAR_MIN)
    best = opts[0] if opts else None

    return {
        "match_id": rep.get("match_id"),
        "match": f"{match.get('home')} — {match.get('away')}",
        "minute": match.get("minute"),
        "score": match.get("score"),
        "best_goal_option": best,
        "xg": pair("xg"),
        "shots": pair("shots"),
        "shots_on_target": pair("shots_on_target"),
        "touches_in_box": pair("touches_in_box"),
        "corners": pair("corners"),
        "momentum_score": float((momentum or {}).get("score") or 0),
        "data_quality": (rep.get("data_quality") or {}).get("score"),
    }


# ============================================================
# V5.4 TIMING SIGNALS
# Human split: FIRST HALF / SECOND HALF / BTTS / NEXT GOAL
# and action timing: TAKE_NOW / SOON / LATER / WATCH_ONLY
# ============================================================

def _v54_timing_action(item: Dict[str, Any], minute: int, stage: str, momentum: Dict[str, Any], freshness: Dict[str, Any]) -> Dict[str, Any]:
    p = float(item.get("probability") or 0)
    market = str(item.get("market") or "")
    decision = str(item.get("decision") or "")
    mscore = float(momentum.get("score") or 0)
    fresh = freshness.get("status") == "CONFIRMED"
    stage_l = str(stage or "").lower()
    halftime = "half time" in stage_l or "halftime" in stage_l

    action = "WATCH_ONLY"
    wait_for = None
    reason = ""

    if halftime:
        action = "LATER"
        wait_for = "дождаться начала 2-го тайма и первых 3–5 минут"
        reason = "На перерыве не входим вслепую."
    elif decision == "ENTER" and fresh:
        action = "TAKE_NOW"
        reason = "Сигнал уже подтверждён свежим live и моделью."
    elif p >= 75 and not fresh:
        action = "SOON"
        wait_for = "подтверждение свежего счёта/минуты"
        reason = "Процент высокий, но freshness ещё не подтверждён."
    elif 65 <= p < 75:
        if mscore >= 45:
            action = "SOON"
            wait_for = "ещё 1–2 атакующих подтверждения: створ/xG/штрафная"
            reason = "Сигнал уже хороший и давление растёт."
        else:
            action = "LATER"
            wait_for = "рост давления на следующем скане"
            reason = "Сигнал есть, но свежего ускорения пока мало."
    elif 55 <= p < 65:
        action = "LATER"
        wait_for = "переход выше 65% + рост давления"
        reason = "Это ранний радар, не вход."
    else:
        action = "WATCH_ONLY"
        reason = "Сигнал слишком слабый."

    # First-half timing should be stricter as the clock runs.
    if market == "GOAL_BEFORE_HALFTIME" and minute >= 40 and action == "LATER":
        action = "SOON"
        wait_for = "следующий опасный эпизод в ближайшие 1–2 минуты"
        reason = "До перерыва мало времени, ждать долго уже нельзя."

    if market in {"GOAL_NEXT_5", "GOAL_NEXT_10"} and p >= 75 and fresh and not halftime:
        action = "TAKE_NOW"
        reason = "Короткое окно: если брать, то только сейчас."

    return {
        "action": action,
        "wait_for": wait_for,
        "reason": reason,
    }


def _v54_market_section(view: Dict[str, Any]) -> Dict[str, Any]:
    minute = int(view.get("minute") or 0)
    stage = str(view.get("stage") or "")
    freshness = view.get("freshness") or {}
    momentum = view.get("SHORT_TERM_MOMENTUM") or {}
    board = list(view.get("goal_board_65_99") or [])
    radar = list(view.get("RISING_RADAR_55_64") or [])

    sections = {
        "FIRST_HALF_GOAL": [],
        "SECOND_HALF_OR_FT_GOAL": [],
        "BTTS": [],
        "NEXT_GOAL_5_10": [],
        "TEAM_GOAL": [],
        "RADAR_LATER": [],
    }

    for item in board:
        x = dict(item)
        x["timing"] = _v54_timing_action(x, minute, stage, momentum, freshness)
        market = x.get("market")
        if market == "GOAL_BEFORE_HALFTIME":
            sections["FIRST_HALF_GOAL"].append(x)
        elif market == "GOAL_BEFORE_FULLTIME":
            sections["SECOND_HALF_OR_FT_GOAL"].append(x)
        elif market == "BTTS":
            sections["BTTS"].append(x)
        elif market in {"GOAL_NEXT_5", "GOAL_NEXT_10"}:
            sections["NEXT_GOAL_5_10"].append(x)
        elif market == "TEAM_GOAL":
            sections["TEAM_GOAL"].append(x)
        else:
            sections["SECOND_HALF_OR_FT_GOAL"].append(x)

    for item in radar:
        x = dict(item)
        x["timing"] = _v54_timing_action(x, minute, stage, momentum, freshness)
        sections["RADAR_LATER"].append(x)

    for key in sections:
        sections[key].sort(key=lambda z: float(z.get("probability") or 0), reverse=True)

    # Best action across sections.
    flat = []
    for key, vals in sections.items():
        if key != "RADAR_LATER":
            flat.extend(vals)

    priority = {"TAKE_NOW": 4, "SOON": 3, "LATER": 2, "WATCH_ONLY": 1}
    flat.sort(
        key=lambda z: (
            priority.get((z.get("timing") or {}).get("action"), 0),
            float(z.get("probability") or 0)
        ),
        reverse=True
    )

    best = flat[0] if flat else None
    return {
        "sections": sections,
        "BEST_TIMING_ACTION": best,
    }


def _v54_attach_timing(view: Dict[str, Any]) -> Dict[str, Any]:
    timing = _v54_market_section(view)
    view["TIMING_SECTIONS"] = timing["sections"]
    view["BEST_TIMING_ACTION"] = timing["BEST_TIMING_ACTION"]

    best = timing["BEST_TIMING_ACTION"]
    if best:
        t = best.get("timing") or {}
        action = t.get("action")
        if action == "TAKE_NOW":
            human = f"БРАТЬ СЕЙЧАС: {best.get('title')} — {best.get('probability')}%"
        elif action == "SOON":
            human = f"СКОРО: {best.get('title')} — {best.get('probability')}%; ждём {t.get('wait_for')}"
        elif action == "LATER":
            human = f"ПОЗЖЕ: {best.get('title')} — {best.get('probability')}%; ждём {t.get('wait_for')}"
        else:
            human = "ТОЛЬКО НАБЛЮДЕНИЕ"
    else:
        human = "Сильного сигнала нет — пропускаем."

    view["WHEN_TO_ENTER"] = human
    return view


# ============================================================
# V5.5 SIGNAL FLOW
# Layer above V5.4 core:
# - emerging-goal detector
# - NOW / SOON / LATER state machine
# - dedicated first-half engine
# - 3-scan pressure trend
# - false-pressure detector
# - market comparison
# - OUR_THINKING 2.0
# ============================================================

FLOW_EMERGING_MIN = 55.0
FLOW_VISIBLE_MIN = 65.0
FLOW_ENTER_MIN = 75.0
FLOW_HISTORY_KEEP = 12
FIRST_HALF_DOMINANCE_DISPLAY_FLOOR = 60.0
FIRST_HALF_UNILATERAL_DISPLAY_FLOOR = 62.0
FIRST_HALF_LOW_DQ_ANALYSIS_CAP = 62.0
FLOW_TREND_KEEP = 4


def _v55_load_history() -> Dict[str, Any]:
    try:
        with open(SCAN_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _v55_save_history(history: Dict[str, Any]) -> None:
    try:
        tmp = SCAN_HISTORY_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False)
        os.replace(tmp, SCAN_HISTORY_PATH)
    except Exception:
        pass


def _v55_append_history(history: Dict[str, Any], mid: str, snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    seq = list(history.get(mid) or [])
    seq.append(snapshot)
    seq = seq[-FLOW_HISTORY_KEEP:]
    history[mid] = seq
    return seq


def _v55_pair_value(snap: Dict[str, Any], key: str, side: str) -> Optional[float]:
    obj = snap.get(key)
    if not isinstance(obj, dict):
        return None
    try:
        return float(obj.get(side))
    except Exception:
        return None


def _v55_series(history_seq: List[Dict[str, Any]], key: str, side: str) -> List[float]:
    vals = []
    for s in history_seq:
        v = _v55_pair_value(s, key, side)
        if v is not None:
            vals.append(round(v, 2))
    return vals


def _v55_trend(history_seq: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    3–4 scan trend. The important question is direction, not only cumulative totals.
    Longer history is retained for 5/10-minute first-half windows, but the legacy
    trend score intentionally remains a short 4-scan signal.
    """
    history_seq = list(history_seq or [])[-FLOW_TREND_KEEP:]
    if len(history_seq) < 2:
        return {
            "available": False,
            "label": "строим историю",
            "score": 0.0,
            "direction": "FLAT",
            "series": {},
        }

    metrics = ("xg", "shots_on_target", "touches_in_box", "shots_in_box", "shots", "corners")
    series = {
        key: {
            "home": _v55_series(history_seq, key, "home"),
            "away": _v55_series(history_seq, key, "away"),
        }
        for key in metrics
    }

    def delta(key, side):
        vals = series[key][side]
        return (vals[-1] - vals[0]) if len(vals) >= 2 else 0.0

    # Weighted recent growth across both teams.
    score_h = (
        max(0, delta("xg", "home")) * 40
        + max(0, delta("shots_on_target", "home")) * 11
        + max(0, delta("touches_in_box", "home")) * 2.2
        + max(0, delta("shots_in_box", "home")) * 3.2
        + max(0, delta("shots", "home")) * 1.2
        + max(0, delta("corners", "home")) * 1.5
    )
    score_a = (
        max(0, delta("xg", "away")) * 40
        + max(0, delta("shots_on_target", "away")) * 11
        + max(0, delta("touches_in_box", "away")) * 2.2
        + max(0, delta("shots_in_box", "away")) * 3.2
        + max(0, delta("shots", "away")) * 1.2
        + max(0, delta("corners", "away")) * 1.5
    )
    total = min(99.0, score_h + score_a)

    if total >= 60:
        label = "резко разгоняется"
        direction = "UP_FAST"
    elif total >= 32:
        label = "давление растёт"
        direction = "UP"
    elif total >= 14:
        label = "есть небольшой рост"
        direction = "UP_SLOW"
    else:
        label = "темп не растёт"
        direction = "FLAT"

    if score_h - score_a >= 12:
        side = "HOME"
    elif score_a - score_h >= 12:
        side = "AWAY"
    else:
        side = "BOTH_OR_UNCLEAR"

    return {
        "available": True,
        "samples": len(history_seq),
        "label": label,
        "direction": direction,
        "dominant_side": side,
        "home_score": round1(score_h),
        "away_score": round1(score_a),
        "score": round1(total),
        "series": series,
    }


def _v55_false_pressure(history_seq: List[Dict[str, Any]], rep: Dict[str, Any]) -> Dict[str, Any]:
    """
    Detect cumulative-stat traps in the short recent scan window.
    Do not compare against the oldest retained 10-minute-history snapshot.
    """
    history_seq = list(history_seq or [])[-FLOW_TREND_KEEP:]
    if len(history_seq) < 2:
        return {"detected": False, "penalty": 0.0, "reason": "недостаточно истории"}

    first, last = history_seq[0], history_seq[-1]

    def total_delta(key):
        a = first.get(key) or {}
        b = last.get(key) or {}
        try:
            return (
                float(b.get("home") or 0) + float(b.get("away") or 0)
                - float(a.get("home") or 0) - float(a.get("away") or 0)
            )
        except Exception:
            return 0.0

    dxg = total_delta("xg")
    dsot = total_delta("shots_on_target")
    dbox = total_delta("touches_in_box")
    dshots = total_delta("shots")

    metrics = rep.get("metrics") or {}
    shots = _metric_pair(metrics, "shots")
    total_shots = sum(shots) if shots else 0

    # Strong cumulative boxscore, dead recent interval.
    if total_shots >= 14 and dxg < 0.12 and dsot < 1 and dbox < 3 and dshots < 2:
        return {
            "detected": True,
            "penalty": 8.0,
            "reason": "много накопленной статистики, но последние минуты матч остыл",
        }

    # Moderate cooling.
    if total_shots >= 10 and dxg < 0.20 and dsot < 1 and dbox < 5:
        return {
            "detected": True,
            "penalty": 4.0,
            "reason": "накопленные цифры хорошие, но свежий импульс слабый",
        }

    return {"detected": False, "penalty": 0.0, "reason": "свежая активность не противоречит общей статистике"}


def _v55_first_half_engine(
    rep: Dict[str, Any],
    base_p: float,
    trend: Dict[str, Any],
    false_pressure: Dict[str, Any],
    history_seq: Optional[List[Dict[str, Any]]] = None,
    freshness: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """FIRST HALF ENGINE V3.1.2 integrated into Hidden Signal.

    Safety/integration fixes over V3.1:
    - can rescue a first-half candidate even when the legacy HT board was <55%;
    - data-quality caps are re-applied AFTER dominance/attack-chain boosts;
    - stale/unconfirmed freshness can never return TAKE_NOW/TAKE_SOON;
    - recent 5/10-minute pressure uses a longer persisted scan history;
    - low trend_score is not a hard requirement under real static dominance.
    """
    match = rep.get("match") or {}
    metrics = rep.get("metrics") or {}
    q = rep.get("data_quality") or {}
    freshness = freshness or {}
    minute = int(match.get("minute") or 0)
    stage = str(match.get("stage") or "").lower()

    # V5.7.5l2 keeps the dedicated first-half engine aligned with the scanner:
    # only 5'–43' are eligible. 44'+ is too late for a stable new entry.
    if minute < 5 or minute > 43 or "half time" in stage or "halftime" in stage:
        return {"active": False}

    def pair(key: str) -> Tuple[float, float]:
        row = metrics.get(key) or {}
        return float(row.get("home") or 0), float(row.get("away") or 0)

    xg_h, xg_a = pair("xg")
    sh_h, sh_a = pair("shots")
    sot_h, sot_a = pair("shots_on_target")
    box_h, box_a = pair("touches_in_box")
    sib_h, sib_a = pair("shots_in_box")
    cor_h, cor_a = pair("corners")
    big_h, big_a = pair("big_chances")
    da_h, da_a = pair("dangerous_attacks")
    poss_h, poss_a = pair("possession")

    def side_pressure(side: str) -> float:
        if side == "home":
            xg, shots, sot, box, sib, cor, big, da, poss = xg_h, sh_h, sot_h, box_h, sib_h, cor_h, big_h, da_h, poss_h
            oxg, osh, osot, obox, obig = xg_a, sh_a, sot_a, box_a, big_a
        else:
            xg, shots, sot, box, sib, cor, big, da, poss = xg_a, sh_a, sot_a, box_a, sib_a, cor_a, big_a, da_a, poss_a
            oxg, osh, osot, obox, obig = xg_h, sh_h, sot_h, box_h, big_h

        raw = (
            sot * 4.0 + shots * 1.05 + min(xg, 2.5) * 6.0
            + box * 0.38 + sib * 0.65 + da * 0.07 + cor * 0.85 + big * 3.6
        )
        raw += max(0.0, shots - osh) * 0.45
        raw += max(0.0, sot - osot) * 1.6
        raw += max(0.0, xg - oxg) * 2.8
        raw += max(0.0, box - obox) * 0.18
        raw += max(0.0, big - obig) * 1.4
        raw += max(0.0, poss - 50.0) * 0.035
        if minute >= 12:
            raw *= max(0.82, min(1.08, 30.0 / max(1, minute)))
        return round1(clamp(raw, 0, 100))

    pressure_home = float(side_pressure("home"))
    pressure_away = float(side_pressure("away"))

    def hist_pair(row: Dict[str, Any], key: str) -> Tuple[float, float]:
        obj = row.get(key) or {}
        return float(obj.get("home") or 0), float(obj.get("away") or 0)

    def recent_delta(minutes_back: int) -> Dict[str, Dict[str, float]]:
        seq = list(history_seq or [])
        if not seq:
            return {"available": False, "home": {}, "away": {}, "actual_minutes": None}
        current_min = minute
        eligible = []
        for row in seq:
            try:
                rm = int(row.get("minute") or 0)
            except Exception:
                continue
            if 0 < rm <= current_min - minutes_back:
                eligible.append(row)
        if not eligible:
            return {"available": False, "home": {}, "away": {}, "actual_minutes": None}
        old = max(eligible, key=lambda r: int(r.get("minute") or 0))
        old_min = int(old.get("minute") or 0)
        actual_minutes = max(1, current_min - old_min)
        # Sparse manual scans must not masquerade as an exact 5/10-minute window.
        max_reasonable = 7 if minutes_back <= 5 else 13
        if actual_minutes > max_reasonable:
            return {
                "available": False,
                "home": {},
                "away": {},
                "actual_minutes": actual_minutes,
                "reason": "SPARSE_HISTORY_WINDOW",
            }
        out = {
            "available": True,
            "home": {},
            "away": {},
            "actual_minutes": actual_minutes,
        }
        for key in (
            "xg", "shots", "shots_on_target", "touches_in_box",
            "corners", "big_chances", "dangerous_attacks",
        ):
            oh, oa = hist_pair(old, key)
            ch, ca = pair(key)
            out["home"][key] = max(0.0, ch - oh)
            out["away"][key] = max(0.0, ca - oa)
        return out

    def window_pressure(d: Dict[str, float]) -> float:
        return round1(clamp(
            float(d.get("shots_on_target") or 0) * 5.2
            + float(d.get("shots") or 0) * 1.25
            + min(float(d.get("xg") or 0), 1.5) * 7.5
            + float(d.get("touches_in_box") or 0) * 0.45
            + float(d.get("dangerous_attacks") or 0) * 0.08
            + float(d.get("corners") or 0) * 0.95
            + float(d.get("big_chances") or 0) * 2.5,
            0, 100,
        ))

    d5 = recent_delta(5)
    d10 = recent_delta(10)
    p5_h = window_pressure(d5["home"]) if d5.get("available") else 0.0
    p5_a = window_pressure(d5["away"]) if d5.get("available") else 0.0
    p10_h = window_pressure(d10["home"]) if d10.get("available") else 0.0
    p10_a = window_pressure(d10["away"]) if d10.get("available") else 0.0

    # Compare approximate 5-minute pace against the preceding portion of the
    # 10-minute window. If exact windows are unavailable, acceleration stays 0.
    if d5.get("available") and d10.get("available"):
        acc_h = round1(p5_h - max(0.0, p10_h - p5_h))
        acc_a = round1(p5_a - max(0.0, p10_a - p5_a))
    else:
        acc_h = 0.0
        acc_a = 0.0

    home_name = str(match.get("home") or "HOME")
    away_name = str(match.get("away") or "AWAY")
    if pressure_home >= pressure_away:
        leader_side, leader_name = "home", home_name
        leader_p, opp_p = pressure_home, pressure_away
        lshots, lsot, lxg, lbox, lbig = sh_h, sot_h, xg_h, box_h, big_h
        lp5, lacc = p5_h, acc_h
    else:
        leader_side, leader_name = "away", away_name
        leader_p, opp_p = pressure_away, pressure_home
        lshots, lsot, lxg, lbox, lbig = sh_a, sot_a, xg_a, box_a, big_a
        lp5, lacc = p5_a, acc_a

    pressure_gap = round1(leader_p - opp_p)
    trend_score = float(trend.get("score") or 0)

    # Snapshot-only dominance: catches a real one-sided attack even before
    # multi-scan trend history has had time to build. This is especially
    # important at 10'–30' when one team already owns the shots/SOT profile.
    opp_shots = sh_a if leader_side == "home" else sh_h
    opp_sot = sot_a if leader_side == "home" else sot_h
    total_shots = max(0.0, lshots + opp_shots)
    total_sot = max(0.0, lsot + opp_sot)
    shot_share = 0.5 if total_shots <= 0 else lshots / total_shots
    sot_share = 0.5 if total_sot <= 0 else lsot / total_sot
    unilateral_pressure_override = bool(
        (lsot >= 3 and lshots >= 4 and opp_sot <= 1 and pressure_gap >= 12)
        or (lsot >= 2 and lshots >= 5 and shot_share >= 0.72 and sot_share >= 0.72 and pressure_gap >= 10)
        or (minute <= 24 and lsot >= 3 and opp_sot == 0 and pressure_gap >= 10)
    )

    dominance_override = bool(
        (leader_p >= 30 and lsot >= 3 and pressure_gap >= 10)
        or (leader_p >= 25 and lsot >= 2 and lshots >= 6 and pressure_gap >= 10)
        or (lbig >= 2 and lsot >= 2 and pressure_gap >= 8)
        or (lp5 >= 14 and lacc >= 5 and pressure_gap >= 6)
        or unilateral_pressure_override
    )

    false_static = bool(
        ((poss_h if leader_side == "home" else poss_a) >= 62 and lshots <= 3 and lsot == 0 and lbox <= 4)
        or (lshots >= 7 and lsot <= 1 and lxg < 0.40)
    )
    false_detected = bool(false_pressure.get("detected")) or false_static
    false_penalty = float(false_pressure.get("penalty") or 0) + (5.0 if false_static else 0.0)

    chain_score = 0.0
    chain_components: List[str] = []
    if d5.get("available"):
        ld = d5[leader_side]
        if ld.get("shots_on_target", 0) >= 1:
            chain_score += 3.0; chain_components.append("створ за последние 5 минут")
        if ld.get("shots", 0) >= 3:
            chain_score += 2.0; chain_components.append("3+ удара за последние 5 минут")
        if ld.get("touches_in_box", 0) >= 4:
            chain_score += 2.0; chain_components.append("серия входов в штрафную")
        if ld.get("corners", 0) >= 2:
            chain_score += 1.5; chain_components.append("серия угловых")
        if ld.get("xg", 0) >= 0.20:
            chain_score += 2.5; chain_components.append("рост xG")
        if ld.get("dangerous_attacks", 0) >= 8:
            chain_score += 1.5; chain_components.append("серия опасных атак")
    attack_chain_active = chain_score >= 5.0

    # Start from the already-guarded native HT market estimate whenever present.
    # V5.7.5l2 no longer turns the engine off when the legacy HT signal is
    # missing. In that case it builds a conservative snapshot seed from the
    # remaining first-half time + live attacking evidence.
    seed_source = "LEGACY_HT" if float(base_p or 0) > 0 else "LIVE_PRESSURE_FALLBACK"
    if float(base_p or 0) > 0:
        p = float(base_p)
    else:
        remaining_seed = max(2, 45 - minute)
        total_xg = max(0.0, xg_h + xg_a)
        total_box = max(0.0, box_h + box_a)
        fallback_seed = (
            38.0
            + remaining_seed * 0.35
            + min(10.0, (sot_h + sot_a) * 3.0)
            + min(6.0, (sh_h + sh_a) * 0.50)
            + min(6.0, total_xg * 4.0)
            + min(4.0, total_box * 0.20)
            + min(6.0, leader_p * 0.10)
            + min(5.0, max(0.0, pressure_gap) * 0.12)
        )
        p = round1(clamp(fallback_seed, 30, 72))
    if 5 <= minute <= 9:
        # V5.7.5l: we now scan from 5', but keep a conservative early-sample
        # penalty so a tiny sample cannot create an aggressive first-half entry.
        window = "EARLY_FORMATION"; p -= 4
    elif 10 <= minute <= 19:
        window = "FORMATION"; p -= 2
    elif 20 <= minute <= 34:
        window = "PRIME"; p += 2
    elif 35 <= minute <= 42:
        window = "AGGRESSIVE"; p += 4
    else:
        window = "LAST_CHANCE"; p -= 2

    if trend_score >= 60:
        p += 5
    elif trend_score >= 32:
        p += 3
    elif trend_score < 14 and minute >= 25 and not dominance_override:
        p -= 2

    p += min(5.0, max(0.0, pressure_gap - 10.0) * 0.10)
    p += min(4.0, chain_score * 0.35)
    p += min(3.0, max(0.0, lacc) * 0.15)
    if dominance_override:
        p += 4.0
    if unilateral_pressure_override:
        # Extra bounded boost for a one-sided SOT/shots profile. This is not
        # enough by itself to bypass DQ or freshness action guards.
        p += 3.0
    p -= false_penalty
    p = round1(clamp(p, 0, 90))

    evidence = bool(
        (sh_h + sh_a) >= 4
        or (sot_h + sot_a) >= 1
        or (xg_h + xg_a) >= 0.25
        or leader_p >= 18
        or attack_chain_active
    )
    if not evidence:
        p = min(p, 59.0)

    # V3.1.2 rescue floor. A verified static-dominance pattern (for example
    # 6 shots + 3 SOT with a large pressure gap) must at least reach the
    # dedicated 60% first-half radar even when the legacy HT model is <55%.
    # This is display-only: DQ/freshness guards below can still cap/block it.
    dominance_floor_applied = False
    unilateral_floor_applied = False
    if evidence and unilateral_pressure_override and p < FIRST_HALF_UNILATERAL_DISPLAY_FLOOR:
        p = FIRST_HALF_UNILATERAL_DISPLAY_FLOOR
        dominance_floor_applied = True
        unilateral_floor_applied = True
    elif evidence and dominance_override and p < FIRST_HALF_DOMINANCE_DISPLAY_FLOOR:
        p = FIRST_HALF_DOMINANCE_DISPLAY_FLOOR
        dominance_floor_applied = True

    # Re-apply data-quality safety AFTER all V3.1 boosts so dominance cannot
    # accidentally bypass the existing Data Quality Guard.
    dq_score = float(q.get("score") or 0)
    strong_eligible = bool(q.get("strong_eligible"))
    basic_ok = bool(q.get("basic_ok", True))
    dq_block_reason = None
    dq_analysis_override = False
    if not basic_ok or dq_score < 45:
        # Never make a low-DQ snapshot actionable. But if the basic scoreboard
        # still contains a very strong one-sided shots/SOT pattern, keep it
        # visible as analysis-only instead of silently dropping below radar.
        if unilateral_pressure_override and lshots >= 4 and lsot >= 3:
            p = min(max(p, FIRST_HALF_DOMINANCE_DISPLAY_FLOOR), FIRST_HALF_LOW_DQ_ANALYSIS_CAP)
            dq_analysis_override = True
        else:
            p = min(p, 59.0)
        dq_block_reason = "DATA_QUALITY_TOO_LOW"
    elif dq_score < 65:
        p = min(p, 69.0)
        dq_block_reason = "DATA_QUALITY_WATCH_ONLY"
    elif not strong_eligible:
        p = min(p, 74.0)
        dq_block_reason = "DATA_QUALITY_NO_STRONG_ENTER"
    p = round1(p)

    remaining = max(1, 45 - minute)
    p_any = max(0.001, min(0.90, p / 100.0))
    hazard = -math.log(max(0.001, 1.0 - p_any)) / remaining
    p5 = round1(clamp((1.0 - math.exp(-hazard * min(5, remaining))) * 100, 0, 85))
    p10 = round1(clamp((1.0 - math.exp(-hazard * min(10, remaining))) * 100, 0, 90))

    h_strength = pressure_home + sot_h * 2.5 + xg_h * 4.5 + big_h * 2.8 + p5_h * 0.35
    a_strength = pressure_away + sot_a * 2.5 + xg_a * 4.5 + big_a * 2.8 + p5_a * 0.35
    denom = h_strength + a_strength
    h_share = 0.5 if denom <= 0 else max(0.10, min(0.90, h_strength / denom))
    a_share = 1.0 - h_share
    likely_scorer = home_name if h_share >= 0.60 else away_name if a_share >= 0.60 else "UNCLEAR"
    likely_share = round1(max(h_share, a_share) * 100)
    home_goal_p = round1(p * h_share)
    away_goal_p = round1(p * a_share)

    if p >= 75 and (trend_score >= 24 or dominance_override or attack_chain_active):
        state = "TAKE_NOW" if minute <= 42 else "PASS"
    elif p >= 65:
        state = "TAKE_SOON" if (trend_score >= 14 or dominance_override or attack_chain_active) else "EMERGING"
    elif p >= 60:
        state = "EMERGING"
    else:
        state = "PASS"

    if minute >= 43 and state in {"EMERGING", "TAKE_SOON"}:
        state = "TAKE_NOW" if p >= 80 and (trend_score >= 32 or dominance_override) else "PASS"

    # Any DQ cap means analysis-only / emerging. It must not become an action
    # instruction merely because static dominance added a bounded bonus.
    if dq_block_reason and state in {"TAKE_NOW", "TAKE_SOON"}:
        state = "EMERGING" if p >= 60 else "PASS"

    # Freshness is a hard action guard. We may display the analysis, but never
    # advertise an actionable first-half entry on unconfirmed/stale data.
    freshness_status = str(freshness.get("status") or "UNKNOWN")
    freshness_ok_for_enter = bool(freshness.get("ok_for_enter")) or freshness_status == "CONFIRMED"
    freshness_blocked = not freshness_ok_for_enter
    if freshness_blocked and state in {"TAKE_NOW", "TAKE_SOON"}:
        state = "EMERGING" if p >= 60 else "PASS"

    goal_window = (
        f"{minute+1}–{min(45, minute+5)}'" if p5 >= 35
        else f"{minute+3}–{min(45, minute+10)}'" if p10 >= 45
        else "NO_CLEAR_WINDOW"
    )

    reasons = [
        f"давление: {leader_name} {round1(leader_p)} vs {round1(opp_p)}; gap +{pressure_gap}",
        f"удары {round1(sh_h)}–{round1(sh_a)}, створ {round1(sot_h)}–{round1(sot_a)}, xG {round1(xg_h)}–{round1(xg_a)}",
    ]
    if dominance_override:
        reasons.append("STATIC_DOMINANCE_OVERRIDE: сильное доминирование, trend_score не обязателен")
    if unilateral_pressure_override:
        reasons.append(
            f"UNILATERAL_PRESSURE: shots share {round1(shot_share*100)}%, "
            f"SOT share {round1(sot_share*100)}%, opponent SOT {round1(opp_sot)}"
        )
    if dominance_floor_applied:
        floor_value = FIRST_HALF_UNILATERAL_DISPLAY_FLOOR if unilateral_floor_applied else FIRST_HALF_DOMINANCE_DISPLAY_FLOOR
        reasons.append(f"DOMINANCE_DISPLAY_FLOOR: минимум {floor_value:.0f}% для 1Т-радара")
    if dq_analysis_override:
        reasons.append("LOW_DQ_ANALYSIS_ONLY: давление видно, но вход заблокирован Data Quality Guard")
    if attack_chain_active:
        reasons.append(f"ATTACK_CHAIN {leader_name}: {round1(chain_score)}")
    if false_detected:
        reasons.append("FALSE_PRESSURE guard active")
    if dq_block_reason:
        reasons.append(f"{dq_block_reason}: DQ={round1(dq_score)}")
    if freshness_blocked:
        reasons.append(f"FRESHNESS_BLOCK: {freshness_status}")

    return {
        "active": True,
        "engine_version": "FIRST_HALF_ENGINE_V3.2-UNILATERAL-PRESSURE",
        "minute_window": window,
        "probability": p,
        "probability_goal_before_ht": p,
        "probability_goal_next_5": p5,
        "probability_goal_next_10": p10,
        "probability_home_goal_before_ht": home_goal_p,
        "probability_away_goal_before_ht": away_goal_p,
        "state": state,
        "confidence_band": "STRONG_75_PLUS" if p >= 75 else "GOOD_65_74" if p >= 65 else "WATCH_60_64" if p >= 60 else "BELOW_60",
        "goal_window": goal_window,
        "trend_score": round1(trend_score),
        "trend_required": not dominance_override,
        "dominance_override": dominance_override,
        "unilateral_pressure_override": unilateral_pressure_override,
        "shot_share": round1(shot_share * 100),
        "sot_share": round1(sot_share * 100),
        "seed_source": seed_source,
        "dominance_floor_applied": dominance_floor_applied,
        "unilateral_floor_applied": unilateral_floor_applied,
        "pressure_team": leader_name,
        "pressure_home": round1(pressure_home),
        "pressure_away": round1(pressure_away),
        "pressure_gap": pressure_gap,
        "pressure_last_5_home": round1(p5_h),
        "pressure_last_5_away": round1(p5_a),
        "pressure_last_10_home": round1(p10_h),
        "pressure_last_10_away": round1(p10_a),
        "pressure_acceleration_home": round1(acc_h),
        "pressure_acceleration_away": round1(acc_a),
        "recent_pressure_available": bool(d5.get("available") or d10.get("available")),
        "recent_5_actual_minutes": d5.get("actual_minutes"),
        "recent_10_actual_minutes": d10.get("actual_minutes"),
        "attack_chain_active": attack_chain_active,
        "attack_chain_team": leader_name if attack_chain_active else None,
        "attack_chain_score": round1(chain_score),
        "attack_chain_components": chain_components,
        "likely_scorer_side": likely_scorer,
        "likely_scorer_share": likely_share,
        "false_pressure": {
            "detected": false_detected,
            "penalty": round1(false_penalty),
            "legacy": false_pressure,
        },
        "attacking_evidence": evidence,
        "data_quality": {
            "score": round1(dq_score),
            "strong_eligible": strong_eligible,
            "block_reason": dq_block_reason,
            "analysis_only_override": dq_analysis_override,
        },
        "freshness": {
            "status": freshness_status,
            "ok_for_enter": freshness_ok_for_enter,
            "blocked": freshness_blocked,
            "reason": freshness.get("reason"),
        },
        "actionable": bool(state in {"TAKE_NOW", "TAKE_SOON"} and not dq_block_reason and not freshness_blocked),
        "blockers": [x for x in (dq_block_reason, ("FRESHNESS_UNCONFIRMED" if freshness_blocked else None)) if x],
        "reasons": reasons,
    }

def _v55_flow_state(
    item: Dict[str, Any],
    minute: int,
    stage: str,
    trend: Dict[str, Any],
    freshness: Dict[str, Any],
    false_pressure: Dict[str, Any],
) -> Dict[str, Any]:
    p = float(item.get("probability") or 0)
    trend_score = float(trend.get("score") or 0)
    fresh = freshness.get("status") == "CONFIRMED"
    halftime = "half time" in str(stage or "").lower() or "halftime" in str(stage or "").lower()
    penalty = float(false_pressure.get("penalty") or 0)

    adjusted = round1(clamp(p - penalty, 0, 99))

    if halftime:
        state = "TAKE_LATER"
        why = "перерыв: ждём начало 2-го тайма и первые 3–5 минут"
    elif adjusted >= 78 and fresh and trend_score >= 24:
        state = "TAKE_NOW"
        why = "высокий сигнал + свежесть + текущая динамика"
    elif adjusted >= 75 and fresh and trend_score < 24:
        state = "TAKE_SOON"
        why = "процент уже высокий, но нужен свежий атакующий толчок"
    elif adjusted >= 65 and trend_score >= 32:
        state = "TAKE_SOON"
        why = "сигнал ещё не максимальный, зато давление ускоряется"
    elif adjusted >= 65:
        state = "TAKE_LATER"
        why = "сигнал есть, но динамика пока недостаточна"
    elif adjusted >= 55 and trend_score >= 14:
        state = "EMERGING"
        why = "ранний сигнал и уже есть рост"
    elif adjusted >= 55:
        state = "RADAR"
        why = "процент формируется, но роста пока нет"
    else:
        state = "PASS"
        why = "ни процент, ни динамика не дают вход"

    if not fresh and state == "TAKE_NOW":
        state = "TAKE_SOON"
        why = "сильный сигнал, но финальная свежесть ещё не подтверждена"

    return {
        "state": state,
        "base_probability": round1(p),
        "adjusted_probability": adjusted,
        "trend_score": trend_score,
        "freshness": freshness.get("status"),
        "why": why,
        "false_pressure_penalty": penalty,
    }


def _v55_compare_markets(items: List[Dict[str, Any]], match: Dict[str, Any]) -> Dict[str, Any]:
    if not items:
        return {
            "best": None,
            "alternative": None,
            "why_best": "нет достаточно сильных рынков",
        }

    market_bonus = {
        "GOAL_BEFORE_FULLTIME": 3.0,
        "GOAL_BEFORE_HALFTIME": 2.0,
        "GOAL_NEXT_10": 1.0,
        "GOAL_NEXT_5": -1.0,
        "TEAM_GOAL": 0.0,
        "BTTS": -1.5,
    }

    ranked = []
    for x in items:
        y = dict(x)
        flow = y.get("FLOW") or {}
        score = float(flow.get("adjusted_probability") or y.get("probability") or 0)
        score += market_bonus.get(str(y.get("market") or ""), 0.0)
        if flow.get("state") == "TAKE_NOW":
            score += 5
        elif flow.get("state") == "TAKE_SOON":
            score += 3
        ranked.append((score, y))

    ranked.sort(key=lambda z: z[0], reverse=True)
    best = ranked[0][1]
    alt = ranked[1][1] if len(ranked) > 1 else None

    why = f"{best.get('title')} лучше сочетает вероятность, динамику и ширину условия."
    if best.get("market") == "GOAL_BEFORE_FULLTIME":
        why += " Общий гол не требует угадывать конкретную команду."
    elif best.get("market") == "TEAM_GOAL":
        why += " Направление давления достаточно выражено в сторону одной команды."
    elif best.get("market") == "BTTS":
        why += " ОЗ имеет смысл только если именно ещё не забившая команда реально создаёт."

    return {
        "best": best,
        "alternative": alt,
        "why_best": why,
    }


def _v55_thinking_2(
    rep: Dict[str, Any],
    items: List[Dict[str, Any]],
    trend: Dict[str, Any],
    false_pressure: Dict[str, Any],
    freshness: Dict[str, Any],
    first_half: Dict[str, Any],
) -> Dict[str, Any]:
    match = rep.get("match") or {}
    metrics = rep.get("metrics") or {}
    comparison = _v55_compare_markets(items, match)

    def metric_text(key, label):
        p = _metric_pair(metrics, key)
        if not p:
            return None
        return f"{label} {round1(p[0])}–{round1(p[1])}"

    facts = [
        metric_text("xg", "xG"),
        metric_text("shots", "удары"),
        metric_text("shots_on_target", "створ"),
        metric_text("touches_in_box", "штрафная"),
        metric_text("corners", "угловые"),
    ]
    facts = [x for x in facts if x]

    if trend.get("available"):
        trend_text = f"{trend.get('label')} ({trend.get('score')}%)"
    else:
        trend_text = "история ещё строится"

    if false_pressure.get("detected"):
        tempo_text = f"⚠️ {false_pressure.get('reason')}"
    else:
        tempo_text = "свежая динамика не выглядит ложным накопленным давлением"

    best = comparison.get("best")
    if best:
        flow = best.get("FLOW") or {}
        state = flow.get("state")
        if state == "TAKE_NOW":
            action = f"🟢 БРАТЬ СЕЙЧАС — {best.get('title')} {flow.get('adjusted_probability')}%"
        elif state == "TAKE_SOON":
            action = f"🟡 СКОРО — {best.get('title')} {flow.get('adjusted_probability')}%"
        elif state == "TAKE_LATER":
            action = f"🟠 ПОЗЖЕ — {best.get('title')} {flow.get('adjusted_probability')}%"
        elif state in {"EMERGING", "RADAR"}:
            action = f"👀 НАЗРЕВАЕТ — {best.get('title')} {flow.get('adjusted_probability')}%"
        else:
            action = "🔴 ПРОПУСКАЕМ"
    else:
        action = "🔴 ПРОПУСКАЕМ"

    return {
        "what_i_see": (
            f"{match.get('home')} — {match.get('away')}, "
            f"{match.get('minute')}′, счёт {(match.get('score') or {}).get('home')}:"
            f"{(match.get('score') or {}).get('away')}."
        ),
        "facts": facts,
        "pressure_trend": trend_text,
        "trend_series": trend.get("series"),
        "false_pressure_check": tempo_text,
        "first_half_engine": first_half if first_half.get("active") else None,
        "market_comparison": comparison,
        "freshness": freshness.get("status"),
        "OUR_ACTION": action,
        "what_confirms": (
            "рост xG, новый створ, серия касаний/ударов из штрафной или большой момент"
        ),
        "what_breaks": (
            "5–10 минут без роста метрик, изменение счёта до входа, красная/VAR или резкое падение темпа"
        ),
    }


def _v55_enrich_view(
    rep: Dict[str, Any],
    view: Dict[str, Any],
    history_seq: List[Dict[str, Any]],
    freshness: Dict[str, Any],
) -> Dict[str, Any]:
    trend = _v55_trend(history_seq)
    false_pressure = _v55_false_pressure(history_seq, rep)
    match = rep.get("match") or {}
    minute = int(match.get("minute") or 0)
    stage = str(match.get("stage") or "")

    all_items = []
    for x in list(view.get("goal_board_65_99") or []) + list(view.get("RISING_RADAR_55_64") or []):
        y = dict(x)
        y["FLOW"] = _v55_flow_state(y, minute, stage, trend, freshness, false_pressure)
        all_items.append(y)

    # Dedicated first-half score. V3.1.1 does NOT depend on the legacy 55%
    # radar being present: static dominance may legitimately rescue a base HT
    # estimate that was below the old radar threshold.
    ht_item = next((x for x in all_items if x.get("market") == "GOAL_BEFORE_HALFTIME"), None)
    raw_ht_signal = next(
        (x for x in list(rep.get("all_signals") or []) if x.get("market") == "GOAL_BEFORE_HALFTIME"),
        None,
    )
    ht_base_probability = (
        float((ht_item or {}).get("probability") or 0)
        if ht_item
        else float(_effective_probability(raw_ht_signal) or 0)
    )
    # V5.7.5l2: run the dedicated 1H engine for every live first-half match in
    # the 5'–43' window. If the legacy GOAL_BEFORE_HALFTIME market is missing,
    # the engine uses its conservative LIVE_PRESSURE_FALLBACK seed.
    first_half = _v55_first_half_engine(
        rep,
        ht_base_probability,
        trend,
        false_pressure,
        history_seq,
        freshness,
    )

    emerging = [
        x for x in all_items
        if (x.get("FLOW") or {}).get("state") in {"EMERGING", "RADAR"}
        and float((x.get("FLOW") or {}).get("adjusted_probability") or 0) >= 55
    ]
    later = [x for x in all_items if (x.get("FLOW") or {}).get("state") == "TAKE_LATER"]
    soon = [x for x in all_items if (x.get("FLOW") or {}).get("state") == "TAKE_SOON"]
    now = [x for x in all_items if (x.get("FLOW") or {}).get("state") == "TAKE_NOW"]

    # First-half emerging signal is surfaced even before 65%.
    first_half_emerging = []
    if first_half.get("active") and first_half.get("state") in {"EMERGING", "TAKE_SOON", "TAKE_NOW"}:
        first_half_emerging.append({
            "title": "Гол в 1-м тайме",
            "probability": first_half.get("probability"),
            "state": first_half.get("state"),
            "minute_window": first_half.get("minute_window"),
            "trend_score": first_half.get("trend_score"),
            "confidence_band": first_half.get("confidence_band"),
            "pressure_team": first_half.get("pressure_team"),
            "pressure_home": first_half.get("pressure_home"),
            "pressure_away": first_half.get("pressure_away"),
            "pressure_gap": first_half.get("pressure_gap"),
            "pressure_last_5_home": first_half.get("pressure_last_5_home"),
            "pressure_last_5_away": first_half.get("pressure_last_5_away"),
            "pressure_acceleration_home": first_half.get("pressure_acceleration_home"),
            "pressure_acceleration_away": first_half.get("pressure_acceleration_away"),
            "attack_chain_active": first_half.get("attack_chain_active"),
            "attack_chain_team": first_half.get("attack_chain_team"),
            "attack_chain_score": first_half.get("attack_chain_score"),
            "likely_scorer_side": first_half.get("likely_scorer_side"),
            "likely_scorer_share": first_half.get("likely_scorer_share"),
            "probability_goal_next_5": first_half.get("probability_goal_next_5"),
            "probability_goal_next_10": first_half.get("probability_goal_next_10"),
            "probability_home_goal_before_ht": first_half.get("probability_home_goal_before_ht"),
            "probability_away_goal_before_ht": first_half.get("probability_away_goal_before_ht"),
            "goal_window": first_half.get("goal_window"),
            "dominance_override": first_half.get("dominance_override"),
            "unilateral_pressure_override": first_half.get("unilateral_pressure_override"),
            "shot_share": first_half.get("shot_share"),
            "sot_share": first_half.get("sot_share"),
            "seed_source": first_half.get("seed_source"),
            "dominance_floor_applied": first_half.get("dominance_floor_applied"),
            "unilateral_floor_applied": first_half.get("unilateral_floor_applied"),
            "trend_required": first_half.get("trend_required"),
            "false_pressure": first_half.get("false_pressure"),
            "data_quality": first_half.get("data_quality"),
            "freshness": first_half.get("freshness"),
            "actionable": first_half.get("actionable"),
            "blockers": first_half.get("blockers"),
            "recent_pressure_available": first_half.get("recent_pressure_available"),
            "pressure_last_10_home": first_half.get("pressure_last_10_home"),
            "pressure_last_10_away": first_half.get("pressure_last_10_away"),
            "reasons": first_half.get("reasons"),
        })

    view["PRESSURE_TREND"] = trend
    view["FALSE_PRESSURE"] = false_pressure
    view["FIRST_HALF_ENGINE"] = first_half
    view["EMERGING_GOAL"] = emerging
    view["FLOW_TAKE_LATER"] = later
    view["FLOW_TAKE_SOON"] = soon
    view["FLOW_TAKE_NOW"] = now
    view["FIRST_HALF_EMERGING"] = first_half_emerging
    view["OUR_THINKING_2"] = _v55_thinking_2(
        rep, all_items, trend, false_pressure, freshness, first_half
    )

    best_action = view["OUR_THINKING_2"].get("OUR_ACTION")
    view["WHEN_TO_ENTER"] = best_action
    return view


# ============================================================
# V5.6 DECISION CONTROL
# Extra safety + timing layer above V5.5 Signal Flow.
# Keeps the core intact.
# ============================================================

V56_JOURNAL_KEEP = 8

# V5.7.5c Adaptive Bridge.
# Strong confirmed base ENTER signals must not disappear from the adaptive
# ladder only because trend history is still young.
ADAPTIVE_BRIDGE_SOON_PROB = float(os.environ.get("ADAPTIVE_BRIDGE_SOON_PROB", "90"))
ADAPTIVE_BRIDGE_SOON_TREND = float(os.environ.get("ADAPTIVE_BRIDGE_SOON_TREND", "15"))
ADAPTIVE_BRIDGE_NOW_PROB = float(os.environ.get("ADAPTIVE_BRIDGE_NOW_PROB", "96"))
ADAPTIVE_BRIDGE_NOW_TREND = float(os.environ.get("ADAPTIVE_BRIDGE_NOW_TREND", "24"))
ADAPTIVE_BRIDGE_MAX_FALSE_PRESSURE_FOR_SOON = float(
    os.environ.get("ADAPTIVE_BRIDGE_MAX_FALSE_PRESSURE_FOR_SOON", "4")
)



def _v56_load_journal() -> Dict[str, Any]:
    try:
        with open(DECISION_JOURNAL_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _v56_save_journal(data: Dict[str, Any]) -> None:
    try:
        tmp = DECISION_JOURNAL_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, DECISION_JOURNAL_PATH)
    except Exception:
        pass


V575C2_SCAN_AUDIT_KEY = "__SCAN_AUDIT__"


def _v575c2_signal_audit(item: Dict[str, Any]) -> Dict[str, Any]:
    flow = item.get("FLOW") or {}
    ctrl = item.get("DECISION_CONTROL") or {}
    bridge = item.get("ADAPTIVE_BRIDGE") or {}
    learning = item.get("SELF_LEARNING") or {}

    return {
        "key": _v56_signal_key(item),
        "market": item.get("market"),
        "title": item.get("title"),
        "base_decision": item.get("decision"),
        "base_probability": item.get("probability"),
        "adjusted_probability": flow.get("adjusted_probability"),
        "adaptive_probability": learning.get("learned_probability"),
        "flow_state": flow.get("state"),
        "state_before_learning": flow.get("state_before_learning"),
        "stable_count": ctrl.get("stable_count"),
        "previous_state": ctrl.get("previous_state"),
        "previous_probability": ctrl.get("previous_probability"),
        "decision_control_reason": ctrl.get("reason"),
        "priority_score": item.get("PRIORITY_SCORE"),
        "entry_expiry": item.get("ENTRY_EXPIRY"),
        "ADAPTIVE_BRIDGE": {
            "applied": bool(bridge.get("applied")),
            "target": bridge.get("target"),
            "reason": bridge.get("reason"),
            "hard_block": bridge.get("hard_block"),
            "trend_score": bridge.get("trend_score"),
            "false_pressure_penalty": bridge.get("false_pressure_penalty"),
            "context_risk": bridge.get("context_risk"),
            "market_conflict": bridge.get("market_conflict"),
            "freshness_status": bridge.get("freshness_status"),
            "freshness_reason": bridge.get("freshness_reason"),
            "stable_count": bridge.get("stable_count"),
        },
    }


def _v575c2_attach_match_audit(
    journal: Dict[str, Any],
    match_id: str,
    view: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Enrich the journal row that V5.6 just created for this match.
    This runs AFTER V5.7 adaptive control, so the saved row contains the
    fields needed to audit Bridge decisions without another live scan.
    """
    rows = list(journal.get(match_id) or [])
    if not rows:
        return {}

    adaptive_items = list(view.get("ADAPTIVE_ITEMS") or [])
    best = view.get("BEST_ADAPTIVE_SIGNAL") or {}
    best_bridge = best.get("ADAPTIVE_BRIDGE") or {}
    trend = view.get("PRESSURE_TREND") or {}
    false_pressure = view.get("FALSE_PRESSURE") or {}
    conflicts = view.get("MARKET_CONFLICTS") or {}
    context_risk = view.get("CONTEXT_RISK") or {}
    freshness = view.get("freshness") or view.get("FRESHNESS") or {}

    audit = {
        "trend_score": float(trend.get("score") or 0),
        "PRESSURE_TREND": trend,
        "FALSE_PRESSURE": false_pressure,
        "market_conflict": bool(conflicts.get("detected")),
        "MARKET_CONFLICTS": conflicts,
        "context_risk": context_risk,
        "freshness": freshness,
        "best_adaptive_state": (best.get("FLOW") or {}).get("state"),
        "best_adaptive_title": best.get("title"),
        "best_adaptive_probability": (
            (best.get("SELF_LEARNING") or {}).get("learned_probability")
            or (best.get("FLOW") or {}).get("adjusted_probability")
            or best.get("probability")
        ),
        "ADAPTIVE_BRIDGE": {
            "applied": bool(best_bridge.get("applied")),
            "target": best_bridge.get("target"),
            "reason": best_bridge.get("reason"),
            "hard_block": best_bridge.get("hard_block"),
        },
        "signals": [_v575c2_signal_audit(x) for x in adaptive_items],
    }

    rows[-1] = dict(rows[-1])
    rows[-1]["adaptive_audit"] = audit
    journal[match_id] = rows[-V56_JOURNAL_KEEP:]
    return audit


def _v575c2_compact_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
    best = c.get("BEST_ADAPTIVE_SIGNAL") or {}
    base = (c.get("goal_board_65_99") or [None])[0] or {}
    return {
        "match_id": c.get("match_id"),
        "match": c.get("match"),
        "minute": c.get("minute"),
        "score": c.get("score"),
        "base_market": base.get("market"),
        "base_title": base.get("title"),
        "base_probability": base.get("probability"),
        "base_decision": base.get("decision"),
        "trend_score": float((c.get("PRESSURE_TREND") or {}).get("score") or 0),
        "freshness": c.get("freshness"),
        "FALSE_PRESSURE": c.get("FALSE_PRESSURE"),
        "stable_count": ((best.get("DECISION_CONTROL") or {}).get("stable_count")),
        "market_conflict": bool((c.get("MARKET_CONFLICTS") or {}).get("detected")),
        "context_risk": c.get("CONTEXT_RISK"),
        "adaptive_state": (best.get("FLOW") or {}).get("state"),
        "ADAPTIVE_BRIDGE": best.get("ADAPTIVE_BRIDGE"),
    }


def _v575e_commit_adaptive_state(
    journal: Dict[str, Any],
    match_id: str,
    view: Dict[str, Any],
) -> None:
    """Persist the FINAL adaptive state into the latest decision-journal row.

    V5.6 writes the journal before V5.7 adaptive control runs. Without this
    post-adaptive commit, Bridge-created TAKE_SOON states are invisible to the
    next scan and TAKE_NOW stability can never accumulate.
    """
    rows = list(journal.get(match_id) or [])
    if not rows:
        return

    latest = rows[-1]
    signals = list(latest.get("signals") or [])
    by_key = {str(s.get("key") or ""): s for s in signals}

    for item in list(view.get("ADAPTIVE_ITEMS") or []):
        key = _v56_signal_key(item)
        saved = by_key.get(key)
        if not saved:
            continue
        final_state = str((item.get("FLOW") or {}).get("state") or "PASS")
        bridge = item.get("ADAPTIVE_BRIDGE") or {}
        final_stable = int(bridge.get("adaptive_stable_count") or 1)
        if final_state not in {"TAKE_SOON", "TAKE_NOW"}:
            final_stable = 1
        saved["adaptive_state"] = final_state
        saved["adaptive_stable_count"] = final_stable

    latest["signals"] = signals
    rows[-1] = latest
    journal[match_id] = rows[-V56_JOURNAL_KEEP:]


def _v575c2_append_scan_audit(
    journal: Dict[str, Any],
    adaptive_take_now: List[Dict[str, Any]],
    adaptive_take_soon: List[Dict[str, Any]],
    adaptive_take_later: List[Dict[str, Any]],
    adaptive_emerging: List[Dict[str, Any]],
    adaptive_bridge_applied: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    row = {
        "at": now_iso(),
        "ADAPTIVE_TAKE_NOW": [_v575c2_compact_candidate(x) for x in adaptive_take_now],
        "ADAPTIVE_TAKE_SOON": [_v575c2_compact_candidate(x) for x in adaptive_take_soon],
        "ADAPTIVE_TAKE_LATER": [_v575c2_compact_candidate(x) for x in adaptive_take_later],
        "ADAPTIVE_EMERGING": [_v575c2_compact_candidate(x) for x in adaptive_emerging],
        "ADAPTIVE_BRIDGE_APPLIED": [_v575c2_compact_candidate(x) for x in adaptive_bridge_applied],
        "ALL_BASE_CANDIDATES": [_v575c2_compact_candidate(x) for x in candidates],
    }
    rows = list(journal.get(V575C2_SCAN_AUDIT_KEY) or [])
    rows.append(row)
    journal[V575C2_SCAN_AUDIT_KEY] = rows[-V56_JOURNAL_KEEP:]
    return row


def _v56_signal_key(item: Dict[str, Any]) -> str:
    return f"{item.get('market')}::{item.get('title')}"


def _v56_previous_state(journal: Dict[str, Any], match_id: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    key = _v56_signal_key(item)
    rows = list(journal.get(match_id) or [])
    for row in reversed(rows):
        for s in row.get("signals") or []:
            if s.get("key") == key:
                return s
    return None


def _v56_transition_control(
    item: Dict[str, Any],
    journal: Dict[str, Any],
    match_id: str,
    trend: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Anti-whipsaw / hysteresis:
    a signal should normally walk through the ladder instead of teleporting.
    Extremely strong fresh acceleration can skip a step.
    """
    flow = dict(item.get("FLOW") or {})
    current = str(flow.get("state") or "PASS")
    adjusted = float(flow.get("adjusted_probability") or item.get("probability") or 0)
    trend_score = float(trend.get("score") or 0)
    prev = _v56_previous_state(journal, match_id, item)
    prev_state = str((prev or {}).get("state") or "PASS")
    prev_adaptive_state = str((prev or {}).get("adaptive_state") or "PASS")
    prev_adaptive_stable_count = int((prev or {}).get("adaptive_stable_count") or 0)
    prev_p = float((prev or {}).get("probability") or 0)

    order = {
        "PASS": 0,
        "RADAR": 1,
        "EMERGING": 2,
        "TAKE_LATER": 3,
        "TAKE_SOON": 4,
        "TAKE_NOW": 5,
    }

    controlled = current
    reason = "переход нормальный"

    # Prevent a one-scan jump from weak radar straight to NOW,
    # except when the move is very strong and fresh.
    if order.get(current, 0) - order.get(prev_state, 0) >= 3:
        if not (adjusted >= 85 and trend_score >= 60):
            controlled = "TAKE_SOON"
            reason = "слишком резкий скачок статуса; нужен ещё один подтверждающий скан"

    # Falling probability should not retain NOW automatically.
    if current == "TAKE_NOW" and prev and adjusted <= prev_p - 4:
        controlled = "TAKE_SOON"
        reason = "вероятность заметно откатилась относительно прошлого скана"

    # Two consecutive strong observations increase stability.
    stable_count = 1
    if prev and prev_state in {"TAKE_SOON", "TAKE_NOW"} and current in {"TAKE_SOON", "TAKE_NOW"}:
        stable_count = int((prev or {}).get("stable_count") or 1) + 1

    if controlled == "TAKE_NOW" and stable_count < 2 and adjusted < 85:
        controlled = "TAKE_SOON"
        reason = "для обычного TAKE NOW нужен второй сильный последовательный скан"

    return {
        "raw_state": current,
        "controlled_state": controlled,
        "previous_state": prev_state,
        "previous_adaptive_state": prev_adaptive_state,
        "previous_adaptive_stable_count": prev_adaptive_stable_count,
        "previous_probability": prev_p if prev else None,
        "stable_count": stable_count,
        "reason": reason,
    }


def _v56_entry_expiry(item: Dict[str, Any], minute: int) -> Dict[str, Any]:
    market = str(item.get("market") or "")
    flow = item.get("FLOW") or {}
    state = str(flow.get("state") or "")

    if state not in {"TAKE_NOW", "TAKE_SOON"}:
        return {
            "active": False,
            "expires_after_minute": None,
            "window_minutes": None,
        }

    if market == "GOAL_NEXT_5":
        window = 2
    elif market == "GOAL_NEXT_10":
        window = 3
    elif market == "GOAL_BEFORE_HALFTIME":
        window = 2 if minute >= 40 else 3
    else:
        window = 4

    return {
        "active": True,
        "window_minutes": window,
        "expires_after_minute": minute + window,
        "note": f"если картина не подтверждается до {minute + window}′ — пересканировать, старый вход не переносить",
    }


def _v56_market_conflicts(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect when several attractive markets are actually asking for different game scripts.
    """
    active = [
        x for x in items
        if (x.get("FLOW") or {}).get("state") in {"TAKE_NOW", "TAKE_SOON", "TAKE_LATER"}
    ]
    markets = {str(x.get("market") or "") for x in active}
    conflicts = []

    if "BTTS" in markets and "TEAM_GOAL" in markets:
        conflicts.append(
            "ОЗ и гол конкретной команды могут опираться на разные сценарии — не складываем их как независимые подтверждения."
        )

    if "GOAL_NEXT_5" in markets and "GOAL_BEFORE_FULLTIME" in markets:
        conflicts.append(
            "Ближайшие 5 минут — более узкое условие, чем гол до конца; высокий общий гол не гарантирует короткое окно."
        )

    team_goal_titles = [str(x.get("title") or "") for x in active if x.get("market") == "TEAM_GOAL"]
    if len(team_goal_titles) >= 2:
        conflicts.append(
            "Есть направления на обе команды — лучше рассматривать общий гол, если он тоже силён."
        )

    return {
        "detected": bool(conflicts),
        "items": conflicts,
    }


def _v56_context_risk(rep: Dict[str, Any], trend: Dict[str, Any]) -> Dict[str, Any]:
    match = rep.get("match") or {}
    minute = int(match.get("minute") or 0)
    score = match.get("score") or {}
    h = int(score.get("home") or 0)
    a = int(score.get("away") or 0)
    red = match.get("red_cards") or {}
    rh = int(red.get("home") or 0) if isinstance(red, dict) else 0
    ra = int(red.get("away") or 0) if isinstance(red, dict) else 0

    risk = 0
    reasons = []

    if abs(h - a) >= 3:
        risk += 18
        reasons.append("крупная разница в счёте может убить темп")
    elif abs(h - a) == 2 and minute >= 70:
        risk += 10
        reasons.append("при разнице в 2 мяча в концовке темп может стать рваным")

    if minute >= 86:
        risk += 10
        reasons.append("очень поздняя минута — маленькое окно и высокая дисперсия")

    if rh or ra:
        risk += 8
        reasons.append("красная карточка меняет обычный рисунок матча")

    if float(trend.get("score") or 0) < 14 and minute >= 25:
        risk += 6
        reasons.append("последние сканы не показывают роста давления")

    if risk >= 24:
        band = "HIGH"
    elif risk >= 12:
        band = "MEDIUM"
    else:
        band = "LOW"

    return {
        "score": min(40, risk),
        "band": band,
        "reasons": reasons,
    }


def _v56_priority_score(
    item: Dict[str, Any],
    trend: Dict[str, Any],
    context_risk: Dict[str, Any],
    false_pressure: Dict[str, Any],
) -> float:
    flow = item.get("FLOW") or {}
    p = float(flow.get("adjusted_probability") or item.get("probability") or 0)
    trend_score = float(trend.get("score") or 0)
    risk = float(context_risk.get("score") or 0)
    false_penalty = float(false_pressure.get("penalty") or 0)

    score = p + min(10, trend_score * 0.12) - risk * 0.35 - false_penalty * 0.5

    if flow.get("state") == "TAKE_NOW":
        score += 5
    elif flow.get("state") == "TAKE_SOON":
        score += 2

    return round1(clamp(score, 0, 99))


def _v56_apply_decision_control(
    rep: Dict[str, Any],
    view: Dict[str, Any],
    journal: Dict[str, Any],
    match_id: str,
) -> Dict[str, Any]:
    trend = view.get("PRESSURE_TREND") or {}
    false_pressure = view.get("FALSE_PRESSURE") or {}
    match = rep.get("match") or {}
    minute = int(match.get("minute") or 0)
    context_risk = _v56_context_risk(rep, trend)

    all_items = []
    for x in list(view.get("goal_board_65_99") or []) + list(view.get("RISING_RADAR_55_64") or []):
        y = dict(x)
        y["FLOW"] = dict(y.get("FLOW") or {})
        transition = _v56_transition_control(y, journal, match_id, trend)

        # Controlled state replaces the raw state for final decision presentation.
        y["FLOW"]["state_before_control"] = y["FLOW"].get("state")
        y["FLOW"]["state"] = transition["controlled_state"]
        y["DECISION_CONTROL"] = transition
        y["ENTRY_EXPIRY"] = _v56_entry_expiry(y, minute)
        y["CONTEXT_RISK"] = context_risk
        y["PRIORITY_SCORE"] = _v56_priority_score(y, trend, context_risk, false_pressure)
        all_items.append(y)

    conflicts = _v56_market_conflicts(all_items)

    rank = sorted(
        all_items,
        key=lambda z: (
            {"TAKE_NOW": 5, "TAKE_SOON": 4, "TAKE_LATER": 3, "EMERGING": 2, "RADAR": 1, "PASS": 0}.get(
                (z.get("FLOW") or {}).get("state"), 0
            ),
            float(z.get("PRIORITY_SCORE") or 0),
        ),
        reverse=True,
    )

    best = rank[0] if rank else None
    if best:
        state = (best.get("FLOW") or {}).get("state")
        expiry = best.get("ENTRY_EXPIRY") or {}
        if state == "TAKE_NOW":
            text = f"🟢 БРАТЬ СЕЙЧАС — {best.get('title')} {best.get('PRIORITY_SCORE')} priority"
        elif state == "TAKE_SOON":
            text = f"🟡 СКОРО — {best.get('title')} {best.get('PRIORITY_SCORE')} priority"
        elif state == "TAKE_LATER":
            text = f"🟠 ПОЗЖЕ — {best.get('title')}"
        elif state in {"EMERGING", "RADAR"}:
            text = f"👀 НАЗРЕВАЕТ — {best.get('title')}"
        else:
            text = "🔴 ПРОПУСКАЕМ"
        if expiry.get("active"):
            text += f"; окно до ~{expiry.get('expires_after_minute')}′"
    else:
        text = "🔴 ПРОПУСКАЕМ"

    # Record one compact journal row per scan.
    signals_for_journal = []
    for x in all_items:
        flow = x.get("FLOW") or {}
        ctrl = x.get("DECISION_CONTROL") or {}
        signals_for_journal.append({
            "key": _v56_signal_key(x),
            "state": flow.get("state"),
            "probability": flow.get("adjusted_probability") or x.get("probability"),
            "stable_count": ctrl.get("stable_count") or 1,
        })

    rows = list(journal.get(match_id) or [])
    rows.append({
        "at": now_iso(),
        "minute": minute,
        "score": match.get("score"),
        "signals": signals_for_journal,
    })
    journal[match_id] = rows[-V56_JOURNAL_KEEP:]

    view["DECISION_CONTROL_ITEMS"] = all_items
    view["MARKET_CONFLICTS"] = conflicts
    view["CONTEXT_RISK"] = context_risk
    view["BEST_CONTROLLED_SIGNAL"] = best
    view["WHEN_TO_ENTER_CONTROLLED"] = text
    view["WHY_NOW"] = (
        "процент + свежая динамика + последовательность сканов подтверждают вход"
        if best and (best.get("FLOW") or {}).get("state") == "TAKE_NOW"
        else None
    )
    view["WHY_NOT_NOW"] = (
        None
        if best and (best.get("FLOW") or {}).get("state") == "TAKE_NOW"
        else (
            ((best or {}).get("DECISION_CONTROL") or {}).get("reason")
            if best else "нет подходящего сигнала"
        )
    )
    return view


# ============================================================
# V5.7 BOUNDED SELF-LEARNING
#
# IMPORTANT:
# - Does NOT rewrite server.py.
# - Learns only bounded configuration parameters.
# - Auto-resolves only outcomes that can be inferred reliably
#   from later live score/minute snapshots.
# - Broad/ambiguous markets are observed but not used for
#   automatic tuning unless they resolve unambiguously.
# ============================================================

V57_MIN_RESOLVED = 30
V57_TUNE_EVERY = 10
V57_MAX_RECORDS_PER_MARKET = 120
V57_PENDING_KEEP = 250
V57_MARKET_DEFAULTS = {
    "GOAL_NEXT_5": {"probability_bias": 0.0, "enter_offset": 0.0, "soon_offset": 0.0, "trend_min": 24.0},
    "GOAL_NEXT_10": {"probability_bias": 0.0, "enter_offset": 0.0, "soon_offset": 0.0, "trend_min": 24.0},
    "GOAL_BEFORE_HALFTIME": {"probability_bias": 0.0, "enter_offset": 0.0, "soon_offset": 0.0, "trend_min": 26.0},
    "GOAL_BEFORE_FULLTIME": {"probability_bias": 0.0, "enter_offset": 0.0, "soon_offset": 0.0, "trend_min": 22.0},
    "TEAM_GOAL": {"probability_bias": 0.0, "enter_offset": 0.0, "soon_offset": 0.0, "trend_min": 25.0},
    "BTTS": {"probability_bias": 0.0, "enter_offset": 0.0, "soon_offset": 0.0, "trend_min": 26.0},
}


def _v57_default_learning_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at": now_iso(),
        "markets": {
            k: {
                **dict(v),
                "records": [],
                "resolved_count": 0,
                "wins": 0,
                "losses": 0,
                "last_tuned_at_resolved": 0,
                "config_version": 1,
                "last_change": None,
                "previous_config": None,
            }
            for k, v in V57_MARKET_DEFAULTS.items()
        },
        "pending": [],
        "resolved_total": 0,
        "learning_mode": "BOUNDED_AUTO_TUNING",
        "notes": [
            "source code is never rewritten automatically",
            "automatic tuning is bounded and sample-gated",
            "unresolved/ambiguous outcomes do not tune the model",
        ],
    }


def _v57_load_state() -> Dict[str, Any]:
    try:
        with open(LEARNING_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return _v57_default_learning_state()
    except Exception:
        return _v57_default_learning_state()

    defaults = _v57_default_learning_state()
    data.setdefault("markets", {})
    for market, base in defaults["markets"].items():
        cur = data["markets"].setdefault(market, {})
        for k, v in base.items():
            cur.setdefault(k, v)
    data.setdefault("pending", [])
    data.setdefault("resolved_total", 0)
    data.setdefault("learning_mode", "BOUNDED_AUTO_TUNING")
    return data


def _v57_save_state(state: Dict[str, Any]) -> None:
    try:
        state["updated_at"] = now_iso()
        tmp = LEARNING_STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, LEARNING_STATE_PATH)
    except Exception:
        pass


def _v57_market_cfg(state: Dict[str, Any], market: str) -> Dict[str, Any]:
    markets = state.setdefault("markets", {})
    if market not in markets:
        base = dict(V57_MARKET_DEFAULTS.get(market, {
            "probability_bias": 0.0,
            "enter_offset": 0.0,
            "soon_offset": 0.0,
            "trend_min": 24.0,
        }))
        markets[market] = {
            **base,
            "records": [],
            "resolved_count": 0,
            "wins": 0,
            "losses": 0,
            "last_tuned_at_resolved": 0,
            "config_version": 1,
            "last_change": None,
            "previous_config": None,
        }
    return markets[market]


def _v57_clamp_cfg(cfg: Dict[str, Any]) -> None:
    # Hard safety bounds: learning can refine, not reinvent the model.
    cfg["probability_bias"] = round1(clamp(float(cfg.get("probability_bias") or 0), -3.0, 3.0))
    cfg["enter_offset"] = round1(clamp(float(cfg.get("enter_offset") or 0), -2.0, 5.0))
    cfg["soon_offset"] = round1(clamp(float(cfg.get("soon_offset") or 0), -2.0, 4.0))
    cfg["trend_min"] = round1(clamp(float(cfg.get("trend_min") or 24), 18.0, 36.0))


def _v57_brier(records: List[Dict[str, Any]]) -> Optional[float]:
    vals = []
    for r in records:
        try:
            p = float(r.get("probability")) / 100.0
            y = 1.0 if int(r.get("outcome")) == 1 else 0.0
            vals.append((p - y) ** 2)
        except Exception:
            continue
    if not vals:
        return None
    return round(sum(vals) / len(vals), 4)


def _v57_learning_summary(cfg: Dict[str, Any]) -> Dict[str, Any]:
    records = list(cfg.get("records") or [])
    n = len(records)
    if not n:
        return {
            "resolved": 0,
            "hit_rate": None,
            "avg_model_score": None,
            "calibration_gap": None,
            "brier": None,
        }

    outcomes = [int(r.get("outcome") or 0) for r in records]
    probs = [float(r.get("probability") or 0) for r in records]
    hit = sum(outcomes) / n
    avgp = sum(probs) / n / 100.0
    return {
        "resolved": n,
        "hit_rate": round1(hit * 100),
        "avg_model_score": round1(avgp * 100),
        "calibration_gap": round1((hit - avgp) * 100),
        "brier": _v57_brier(records),
    }


def _v57_tune_market(state: Dict[str, Any], market: str) -> Optional[Dict[str, Any]]:
    cfg = _v57_market_cfg(state, market)
    records = list(cfg.get("records") or [])
    n = len(records)
    last_tuned = int(cfg.get("last_tuned_at_resolved") or 0)

    if n < V57_MIN_RESOLVED or n - last_tuned < V57_TUNE_EVERY:
        return None

    recent = records[-min(40, n):]
    summary = _v57_learning_summary({**cfg, "records": recent})
    gap = float(summary.get("calibration_gap") or 0)

    previous = {
        "probability_bias": float(cfg.get("probability_bias") or 0),
        "enter_offset": float(cfg.get("enter_offset") or 0),
        "soon_offset": float(cfg.get("soon_offset") or 0),
        "trend_min": float(cfg.get("trend_min") or 24),
    }
    cfg["previous_config"] = previous

    changes = []

    # Calibration-guided small steps only.
    # If realized hit rate is well below the model score, become stricter.
    if gap <= -12:
        cfg["probability_bias"] = previous["probability_bias"] - 0.5
        cfg["enter_offset"] = previous["enter_offset"] + 1.0
        cfg["soon_offset"] = previous["soon_offset"] + 0.5
        cfg["trend_min"] = previous["trend_min"] + 1.0
        changes.append("ужесточены пороги: фактический результат ниже модельной оценки")
    elif gap <= -6:
        cfg["probability_bias"] = previous["probability_bias"] - 0.3
        cfg["enter_offset"] = previous["enter_offset"] + 0.5
        cfg["trend_min"] = previous["trend_min"] + 0.5
        changes.append("слегка ужесточён вход")
    elif gap >= 12:
        cfg["probability_bias"] = previous["probability_bias"] + 0.5
        cfg["enter_offset"] = previous["enter_offset"] - 0.5
        cfg["soon_offset"] = previous["soon_offset"] - 0.5
        changes.append("слегка смягчены пороги: результат устойчиво выше модельной оценки")
    elif gap >= 6:
        cfg["probability_bias"] = previous["probability_bias"] + 0.3
        cfg["soon_offset"] = previous["soon_offset"] - 0.5
        changes.append("слегка смягчён переход в TAKE_SOON")
    else:
        changes.append("калибровка стабильна — параметры оставлены без изменения")

    _v57_clamp_cfg(cfg)
    cfg["last_tuned_at_resolved"] = n
    cfg["config_version"] = int(cfg.get("config_version") or 1) + 1
    cfg["last_change"] = {
        "at": now_iso(),
        "resolved": n,
        "market": market,
        "summary": summary,
        "changes": changes,
        "new_config": {
            "probability_bias": cfg["probability_bias"],
            "enter_offset": cfg["enter_offset"],
            "soon_offset": cfg["soon_offset"],
            "trend_min": cfg["trend_min"],
        },
    }
    return cfg["last_change"]


def _v57_maybe_rollback(state: Dict[str, Any], market: str) -> Optional[Dict[str, Any]]:
    """
    Conservative rollback:
    after a tune, wait for at least 10 newer outcomes.
    If recent Brier score is materially worse than the preceding window,
    restore the previous bounded config.
    """
    cfg = _v57_market_cfg(state, market)
    prev_cfg = cfg.get("previous_config")
    last_tuned = int(cfg.get("last_tuned_at_resolved") or 0)
    records = list(cfg.get("records") or [])

    if not prev_cfg or len(records) < last_tuned + 10 or last_tuned < 20:
        return None

    before = records[max(0, last_tuned - 20):last_tuned]
    after = records[last_tuned:min(len(records), last_tuned + 20)]
    if len(after) < 10:
        return None

    b_before = _v57_brier(before)
    b_after = _v57_brier(after)
    if b_before is None or b_after is None:
        return None

    # Lower Brier is better. Roll back only on clear degradation.
    if b_after > b_before + 0.08:
        for k in ("probability_bias", "enter_offset", "soon_offset", "trend_min"):
            cfg[k] = prev_cfg[k]
        _v57_clamp_cfg(cfg)
        cfg["previous_config"] = None
        cfg["config_version"] = int(cfg.get("config_version") or 1) + 1
        cfg["last_change"] = {
            "at": now_iso(),
            "market": market,
            "rollback": True,
            "reason": "качество после настройки ухудшилось",
            "brier_before": b_before,
            "brier_after": b_after,
            "restored_config": {
                "probability_bias": cfg["probability_bias"],
                "enter_offset": cfg["enter_offset"],
                "soon_offset": cfg["soon_offset"],
                "trend_min": cfg["trend_min"],
            },
        }
        return cfg["last_change"]
    return None


def _v57_pending_id(match_id: str, item: Dict[str, Any], minute: int) -> str:
    return f"{match_id}:{item.get('market')}:{item.get('title')}:{minute}"


def _v57_register_pending(
    state: Dict[str, Any],
    match_id: str,
    match: Dict[str, Any],
    item: Dict[str, Any],
) -> Optional[str]:
    flow = item.get("FLOW") or {}
    controlled = str(flow.get("state") or "")
    if controlled != "TAKE_NOW":
        return None

    market = str(item.get("market") or "")
    if market not in V57_MARKET_DEFAULTS:
        return None

    minute = int(match.get("minute") or 0)
    score = match.get("score") or {}
    sid = _v57_pending_id(match_id, item, minute)

    pending = state.setdefault("pending", [])
    if any(x.get("id") == sid for x in pending):
        return sid

    # Auto-learning is most reliable for these horizons.
    if market == "GOAL_NEXT_5":
        deadline = minute + 5
        resolution_mode = "SHORT_WINDOW"
    elif market == "GOAL_NEXT_10":
        deadline = minute + 10
        resolution_mode = "SHORT_WINDOW"
    elif market == "GOAL_BEFORE_HALFTIME" and minute <= 44:
        deadline = 45
        resolution_mode = "FIRST_HALF"
    elif market == "GOAL_BEFORE_FULLTIME":
        deadline = 96
        resolution_mode = "FULLTIME_GOAL"
    else:
        # Team goal / BTTS can be ambiguous without side-specific event resolution.
        # Keep them for audit, but do not auto-tune from them.
        deadline = None
        resolution_mode = "AUDIT_ONLY"

    pending.append({
        "id": sid,
        "created_at": now_iso(),
        "match_id": match_id,
        "market": market,
        "title": item.get("title"),
        "minute": minute,
        "deadline": deadline,
        "resolution_mode": resolution_mode,
        "score_home": int(score.get("home") or 0),
        "score_away": int(score.get("away") or 0),
        "score_total": int(score.get("home") or 0) + int(score.get("away") or 0),
        "probability": float(flow.get("adjusted_probability") or item.get("probability") or 0),
        "priority_score": float(item.get("PRIORITY_SCORE") or 0),
        "resolved": False,
    })
    state["pending"] = pending[-V57_PENDING_KEEP:]
    return sid


def _v57_record_resolution(
    state: Dict[str, Any],
    pending: Dict[str, Any],
    outcome: int,
    resolved_minute: int,
    reason: str,
) -> None:
    market = str(pending.get("market") or "")
    cfg = _v57_market_cfg(state, market)
    records = list(cfg.get("records") or [])
    records.append({
        "signal_id": pending.get("id"),
        "at": now_iso(),
        "minute": pending.get("minute"),
        "resolved_minute": resolved_minute,
        "probability": pending.get("probability"),
        "priority_score": pending.get("priority_score"),
        "outcome": int(outcome),
        "reason": reason,
    })
    cfg["records"] = records[-V57_MAX_RECORDS_PER_MARKET:]
    cfg["resolved_count"] = len(cfg["records"])
    cfg["wins"] = sum(int(x.get("outcome") or 0) for x in cfg["records"])
    cfg["losses"] = len(cfg["records"]) - cfg["wins"]
    state["resolved_total"] = int(state.get("resolved_total") or 0) + 1
    pending["resolved"] = True
    pending["outcome"] = int(outcome)
    pending["resolved_at"] = now_iso()
    pending["resolution_reason"] = reason


def _v57_resolve_pending_for_match(
    state: Dict[str, Any],
    match_id: str,
    match: Dict[str, Any],
) -> List[Dict[str, Any]]:
    score = match.get("score") or {}
    cur_total = int(score.get("home") or 0) + int(score.get("away") or 0)
    minute = int(match.get("minute") or 0)
    stage = str(match.get("stage") or "").lower()
    resolved = []

    for p in state.get("pending") or []:
        if p.get("resolved") or str(p.get("match_id")) != str(match_id):
            continue

        mode = p.get("resolution_mode")
        start_total = int(p.get("score_total") or 0)
        deadline = p.get("deadline")

        if mode == "AUDIT_ONLY":
            continue

        # A later goal is an unambiguous win for total-goal horizons.
        if cur_total > start_total:
            if mode == "FIRST_HALF":
                # Only auto-resolve as win while still in first half or at HT.
                if minute <= 45 or "half time" in stage or "halftime" in stage:
                    _v57_record_resolution(state, p, 1, minute, "гол появился до конца 1-го тайма")
                    resolved.append(p)
                # If already in 2H, exact timing is ambiguous -> leave unresolved.
            else:
                if deadline is None or minute <= int(deadline) + 1:
                    _v57_record_resolution(state, p, 1, minute, "счёт увеличился внутри отслеживаемого окна")
                    resolved.append(p)
                elif mode == "FULLTIME_GOAL":
                    _v57_record_resolution(state, p, 1, minute, "гол появился позже входа до конца матча")
                    resolved.append(p)
            continue

        # Loss only when the tracked window has clearly expired with unchanged score.
        if deadline is not None and minute > int(deadline):
            if mode in {"SHORT_WINDOW", "FIRST_HALF"}:
                _v57_record_resolution(state, p, 0, minute, "окно истекло без изменения счёта")
                resolved.append(p)
            elif mode == "FULLTIME_GOAL" and minute >= 96:
                _v57_record_resolution(state, p, 0, minute, "до 96-й минуты счёт не изменился")
                resolved.append(p)

    return resolved


def _v57_adaptive_control(
    state: Dict[str, Any],
    view: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Final bounded adaptive layer after V5.6.

    V5.7.5c adds ADAPTIVE BRIDGE:
    a very strong, fresh, base ENTER signal with a real trend is guaranteed
    at least TAKE_SOON instead of disappearing from the adaptive ladder.

    TAKE_NOW remains strict:
    - stronger trend,
    - two-scan stability,
    - no severe false-pressure,
    - no market-conflict,
    - low context risk.
    """
    trend = view.get("PRESSURE_TREND") or {}
    trend_score = float(trend.get("score") or 0)

    freshness_obj = view.get("freshness") or view.get("FRESHNESS") or {}
    if not isinstance(freshness_obj, dict):
        freshness_obj = {}
    freshness_status = str(freshness_obj.get("status") or "")
    freshness_reason = str(freshness_obj.get("reason") or "")

    false_pressure = view.get("FALSE_PRESSURE") or {}
    false_penalty = float(false_pressure.get("penalty") or 0)

    context_risk = view.get("CONTEXT_RISK") or {}
    context_risk_score = float(context_risk.get("score") or 0)
    context_risk_band = str(context_risk.get("band") or "LOW")

    conflicts = view.get("MARKET_CONFLICTS") or {}
    conflict_detected = bool(conflicts.get("detected"))

    hard_bridge_block = (
        freshness_status != "CONFIRMED"
        or freshness_reason in {
            "POST_GOAL_COOLDOWN",
            "SCORE_CONFLICT",
            "MATCH_FINISHED",
            "STALE_MINUTE",
            "FINAL_SCORE_CHANGED",
        }
        or context_risk_score >= 24
    )

    items = []
    bridge_items = []

    # V5.6 already produced controlled decision items.
    for x in list(view.get("DECISION_CONTROL_ITEMS") or []):
        y = dict(x)
        y["FLOW"] = dict(y.get("FLOW") or {})
        market = str(y.get("market") or "")
        cfg = _v57_market_cfg(state, market)

        p_raw = float(y["FLOW"].get("adjusted_probability") or y.get("probability") or 0)
        p_learned = round1(clamp(p_raw + float(cfg.get("probability_bias") or 0), 0, 99))
        enter_threshold = 78.0 + float(cfg.get("enter_offset") or 0)
        soon_threshold = 65.0 + float(cfg.get("soon_offset") or 0)
        trend_min = float(cfg.get("trend_min") or 24)

        cur = str(y["FLOW"].get("state") or "PASS")
        adaptive = cur
        reason = "адаптивный слой не менял решение"

        decision_control = y.get("DECISION_CONTROL") or {}
        stable = int(decision_control.get("stable_count") or 1)
        previous_adaptive_state = str(decision_control.get("previous_adaptive_state") or "PASS")
        previous_adaptive_stable_count = int(decision_control.get("previous_adaptive_stable_count") or 0)
        base_decision = str(y.get("decision") or "")

        # Existing bounded self-learning behavior.
        if cur == "TAKE_NOW" and p_learned < enter_threshold:
            adaptive = "TAKE_SOON"
            reason = "самообучение сделало порог TAKE NOW строже"
        elif cur == "TAKE_SOON":
            if p_learned >= enter_threshold and trend_score >= trend_min and stable >= 2:
                adaptive = "TAKE_NOW"
                reason = "история рынка + два сильных скана разрешили TAKE NOW"
            elif p_learned < soon_threshold:
                adaptive = "TAKE_LATER"
                reason = "адаптивный порог TAKE SOON пока не достигнут"
        elif cur == "TAKE_LATER":
            if p_learned >= soon_threshold and trend_score >= trend_min:
                adaptive = "TAKE_SOON"
                reason = "рынок исторически допускает более ранний переход при текущем тренде"
        elif cur in {"EMERGING", "RADAR"}:
            if p_learned >= soon_threshold and trend_score >= trend_min:
                adaptive = "TAKE_LATER"
                reason = "ранний сигнал усилился, но самообучение не позволяет перепрыгнуть сразу в вход"

        # ----------------------------------------------------
        # V5.7.5c ADAPTIVE BRIDGE
        # ----------------------------------------------------
        bridge_applied = False
        bridge_target = None
        bridge_reason = None

        bridge_soon_ok = (
            not hard_bridge_block
            and base_decision == "ENTER"
            and p_learned >= ADAPTIVE_BRIDGE_SOON_PROB
            and trend_score >= ADAPTIVE_BRIDGE_SOON_TREND
            and false_penalty <= ADAPTIVE_BRIDGE_MAX_FALSE_PRESSURE_FOR_SOON
        )

        # Guarantee at least TAKE_SOON for a genuinely strong confirmed signal.
        if bridge_soon_ok and adaptive not in {"TAKE_NOW", "TAKE_SOON"}:
            adaptive = "TAKE_SOON"
            bridge_applied = True
            bridge_target = "TAKE_SOON"
            bridge_reason = (
                "сильный CONFIRMED ENTER + реальный тренд: "
                "adaptive bridge не позволяет сигналу исчезнуть из входной лестницы"
            )
            reason = bridge_reason

        # V5.7.5e ADAPTIVE NOW STABILITY FIX
        # The old bridge required raw FLOW to already be TAKE_SOON/TAKE_NOW and
        # used only the pre-adaptive DECISION_CONTROL stable_count. A signal that
        # was promoted by Adaptive Bridge itself therefore kept returning with
        # stable_count=1 and could get stuck in TAKE_SOON forever.
        #
        # We now persist the FINAL adaptive state and use two consecutive final
        # adaptive strong observations as the stability proof for TAKE_NOW.
        adaptive_stable_count = 1
        if bridge_soon_ok and previous_adaptive_state in {"TAKE_SOON", "TAKE_NOW"}:
            adaptive_stable_count = max(2, previous_adaptive_stable_count + 1)
        elif adaptive in {"TAKE_SOON", "TAKE_NOW"} and stable >= 2:
            adaptive_stable_count = stable

        now_stability_ok = (
            stable >= 2
            or adaptive_stable_count >= 2
        )

        # TAKE_NOW bridge is intentionally stricter.
        # Moderate false pressure (penalty 4) can still be TAKE_SOON,
        # but cannot be promoted to TAKE_NOW by the bridge.
        bridge_now_ok = (
            bridge_soon_ok
            and p_learned >= ADAPTIVE_BRIDGE_NOW_PROB
            and trend_score >= ADAPTIVE_BRIDGE_NOW_TREND
            and now_stability_ok
            and false_penalty <= 0
            and not conflict_detected
            and context_risk_band == "LOW"
        )

        if bridge_now_ok and adaptive == "TAKE_SOON":
            adaptive = "TAKE_NOW"
            adaptive_stable_count = max(2, adaptive_stable_count)
            bridge_applied = True
            bridge_target = "TAKE_NOW"
            bridge_reason = (
                "очень сильный CONFIRMED ENTER + устойчивый тренд + второй финальный "
                "adaptive-сигнал + нет false pressure/market conflict"
            )
            reason = bridge_reason

        y["FLOW"]["state_before_learning"] = cur
        y["FLOW"]["state"] = adaptive
        y["ADAPTIVE_BRIDGE"] = {
            "applied": bridge_applied,
            "target": bridge_target,
            "reason": bridge_reason,
            "base_enter_required": True,
            "base_decision": base_decision,
            "freshness_status": freshness_status,
            "freshness_reason": freshness_reason,
            "trend_score": round1(trend_score),
            "false_pressure_penalty": round1(false_penalty),
            "context_risk": context_risk_band,
            "market_conflict": conflict_detected,
            "stable_count": stable,
            "previous_adaptive_state": previous_adaptive_state,
            "previous_adaptive_stable_count": previous_adaptive_stable_count,
            "adaptive_stable_count": adaptive_stable_count,
            "now_stability_ok": now_stability_ok,
            "soon_probability_threshold": ADAPTIVE_BRIDGE_SOON_PROB,
            "soon_trend_threshold": ADAPTIVE_BRIDGE_SOON_TREND,
            "now_probability_threshold": ADAPTIVE_BRIDGE_NOW_PROB,
            "now_trend_threshold": ADAPTIVE_BRIDGE_NOW_TREND,
            "hard_block": hard_bridge_block,
        }
        if bridge_applied:
            bridge_items.append({
                "title": y.get("title"),
                "market": y.get("market"),
                "probability": p_learned,
                "from_state": cur,
                "to_state": adaptive,
                "trend_score": round1(trend_score),
                "false_pressure_penalty": round1(false_penalty),
                "reason": bridge_reason,
            })

        y["SELF_LEARNING"] = {
            "market_config_version": cfg.get("config_version"),
            "raw_probability": round1(p_raw),
            "learned_probability": p_learned,
            "probability_bias": cfg.get("probability_bias"),
            "enter_threshold": round1(enter_threshold),
            "soon_threshold": round1(soon_threshold),
            "trend_min": round1(trend_min),
            "state_before_learning": cur,
            "adaptive_state": adaptive,
            "reason": reason,
            "samples": len(cfg.get("records") or []),
        }
        items.append(y)

    order = {"TAKE_NOW": 5, "TAKE_SOON": 4, "TAKE_LATER": 3, "EMERGING": 2, "RADAR": 1, "PASS": 0}
    items.sort(
        key=lambda z: (
            order.get((z.get("FLOW") or {}).get("state"), 0),
            float(z.get("PRIORITY_SCORE") or 0),
        ),
        reverse=True,
    )

    best = items[0] if items else None
    if best:
        state_name = (best.get("FLOW") or {}).get("state")
        sl = best.get("SELF_LEARNING") or {}
        if state_name == "TAKE_NOW":
            action = f"🟢 БРАТЬ СЕЙЧАС — {best.get('title')} ({sl.get('learned_probability')}%)"
        elif state_name == "TAKE_SOON":
            action = f"🟡 СКОРО — {best.get('title')} ({sl.get('learned_probability')}%)"
        elif state_name == "TAKE_LATER":
            action = f"🟠 ПОЗЖЕ — {best.get('title')} ({sl.get('learned_probability')}%)"
        elif state_name in {"EMERGING", "RADAR"}:
            action = f"👀 НАЗРЕВАЕТ — {best.get('title')} ({sl.get('learned_probability')}%)"
        else:
            action = "🔴 ПРОПУСКАЕМ"
    else:
        action = "🔴 ПРОПУСКАЕМ"

    view["ADAPTIVE_ITEMS"] = items
    view["ADAPTIVE_BRIDGE_ITEMS"] = bridge_items
    view["ADAPTIVE_BRIDGE_APPLIED"] = bool(bridge_items)
    view["BEST_ADAPTIVE_SIGNAL"] = best
    view["WHEN_TO_ENTER_ADAPTIVE"] = action
    return view


def _v57_learning_report(state: Dict[str, Any]) -> Dict[str, Any]:
    markets = {}
    for market in sorted(state.get("markets") or {}):
        cfg = _v57_market_cfg(state, market)
        markets[market] = {
            "summary": _v57_learning_summary(cfg),
            "config": {
                "probability_bias": cfg.get("probability_bias"),
                "enter_offset": cfg.get("enter_offset"),
                "soon_offset": cfg.get("soon_offset"),
                "trend_min": cfg.get("trend_min"),
                "config_version": cfg.get("config_version"),
            },
            "last_change": cfg.get("last_change"),
            "ready_to_tune": len(cfg.get("records") or []) >= V57_MIN_RESOLVED,
        }
    return {
        "learning_mode": state.get("learning_mode"),
        "resolved_total": state.get("resolved_total"),
        "pending_unresolved": sum(1 for x in state.get("pending") or [] if not x.get("resolved")),
        "minimum_samples_before_tuning": V57_MIN_RESOLVED,
        "markets": markets,
        "safety": {
            "rewrites_source_code": False,
            "bounded_probability_bias": "[-3,+3]",
            "bounded_enter_offset": "[-2,+5]",
            "rollback_enabled": True,
            "ambiguous_outcomes_tune_model": False,
        },
    }

# ===== V5.7.2 RATE LIMIT + LIVE FEED GUARD =====
# 429 is not retried aggressively. A shared cooldown prevents a single scan
# from hammering the provider through live/stats/odds/detail calls.

RATE_LIMIT_STATE_PATH = _v575f_configured_path(
    "RATE_LIMIT_STATE_PATH", "rate_limit_state.json"
)
FEED_GUARD_RETRIES = 3
FEED_EMPTY_200_MAX_ATTEMPTS = int(os.environ.get("FEED_EMPTY_200_MAX_ATTEMPTS", "2"))
FEED_GUARD_NORMAL_DELAYS = (1.0, 2.0)
RATE_LIMIT_BACKOFF = (15, 30, 60)
RATE_LIMIT_MAX_WAIT_INSIDE_SCAN = 18.0
RATE_LIMIT_DEFAULT_COOLDOWN = 60


def _v572_load_rate_state() -> Dict[str, Any]:
    try:
        with open(RATE_LIMIT_STATE_PATH, "r", encoding="utf-8") as f:
            x = json.load(f)
            return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def _v572_save_rate_state(state: Dict[str, Any]) -> None:
    try:
        tmp = RATE_LIMIT_STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, RATE_LIMIT_STATE_PATH)
    except Exception:
        pass


def _v572_epoch() -> float:
    return time.time()


def _v572_retry_after_seconds(value: Any, fallback: int) -> int:
    try:
        if value is not None:
            return max(1, min(300, int(float(str(value).strip()))))
    except Exception:
        pass
    return int(fallback)


def _v572_rate_status() -> Dict[str, Any]:
    st = _v572_load_rate_state()
    until = float(st.get("cooldown_until") or 0)
    remaining = max(0, int(round(until - _v572_epoch())))
    return {
        "active": remaining > 0,
        "remaining_seconds": remaining,
        "cooldown_until": until or None,
        "last_429_at": st.get("last_429_at"),
        "consecutive_429": int(st.get("consecutive_429") or 0),
    }


def _v572_activate_cooldown(retry_after: Any = None) -> Dict[str, Any]:
    st = _v572_load_rate_state()
    consecutive = int(st.get("consecutive_429") or 0) + 1
    fallback = RATE_LIMIT_BACKOFF[min(consecutive - 1, len(RATE_LIMIT_BACKOFF) - 1)]
    seconds = _v572_retry_after_seconds(retry_after, fallback)
    # Never shorten an already-active cooldown.
    until = max(float(st.get("cooldown_until") or 0), _v572_epoch() + seconds)
    st.update({
        "cooldown_until": until,
        "last_429_at": now_iso(),
        "consecutive_429": consecutive,
        "last_retry_after": retry_after,
        "last_cooldown_seconds": seconds,
    })
    _v572_save_rate_state(st)
    return _v572_rate_status()


def _v572_clear_rate_limit() -> None:
    st = _v572_load_rate_state()
    st["cooldown_until"] = 0
    st["consecutive_429"] = 0
    st["recovered_at"] = now_iso()
    _v572_save_rate_state(st)


def _v575a_load_feed_guard_state() -> Dict[str, Any]:
    try:
        with open(FEED_GUARD_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _v575a_save_feed_guard_state(state: Dict[str, Any]) -> None:
    try:
        tmp = FEED_GUARD_STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, FEED_GUARD_STATE_PATH)
    except Exception:
        pass


def _v571_previous_live_count() -> int:
    try:
        st = _v575a_load_feed_guard_state()
        return int(st.get("last_good_live_matches_found") or 0)
    except Exception:
        return 0


def _v571_store_live_count(n: int) -> None:
    # Store only a successful non-negative snapshot. Suspicious empty/collapse
    # responses never call this function, so they cannot erase the baseline.
    try:
        st = _v575a_load_feed_guard_state()
        st["last_good_live_matches_found"] = int(max(0, n))
        st["last_good_live_feed_seen_at"] = now_iso()
        _v575a_save_feed_guard_state(st)
    except Exception:
        pass


async def _v571_guarded_live_snapshot() -> Dict[str, Any]:
    prev = _v571_previous_live_count()
    attempts = []
    best_matches = []
    best_count = -1
    best_status = None

    # If another request recently hit 429, do not send more requests yet.
    pre = _v572_rate_status()
    if pre["active"]:
        return {
            "status": "RATE_LIMIT_COOLDOWN",
            "matches": [],
            "live_matches_found": 0,
            "previous_live_matches_found": prev,
            "attempts": [],
            "safe_for_signals": False,
            "safe_for_learning": False,
            "rate_limit": pre,
            "message": "Provider cooldown is active. No request was sent.",
        }

    for i in range(FEED_GUARD_RETRIES):
        client = ZylaClient()
        err = None
        try:
            r = await client.live()
            http_status = r.get("status")
            retry_after = r.get("retry_after")
            raw_data = r.get("data")

            # Special handling: 429 must not be hammered with rapid retries.
            if int(http_status or 0) == 429:
                rate = _v572_activate_cooldown(retry_after)
                attempts.append({
                    "attempt": i + 1,
                    "http_status": 429,
                    "live_matches_found": -1,
                    "retry_after": retry_after,
                    "rate_limit": rate,
                    "suspicious": True,
                })

                # Wait inside this scan only if the server explicitly asks for
                # a short enough delay. Otherwise stop and let the next scan retry.
                wait_s = int(rate.get("remaining_seconds") or 0)
                if wait_s <= RATE_LIMIT_MAX_WAIT_INSIDE_SCAN and i < FEED_GUARD_RETRIES - 1:
                    await asyncio.sleep(max(1, wait_s))
                    # Clear only the elapsed cooldown; next request may still return 429.
                    st = _v572_load_rate_state()
                    st["cooldown_until"] = 0
                    _v572_save_rate_state(st)
                    continue

                return {
                    "status": "RATE_LIMIT_COOLDOWN",
                    "matches": [],
                    "live_matches_found": 0,
                    "previous_live_matches_found": prev,
                    "attempts": attempts,
                    "safe_for_signals": False,
                    "safe_for_learning": False,
                    "rate_limit": rate,
                    "message": "Zyla returned HTTP 429. Scan stopped to protect the quota.",
                }

            structurally_valid = isinstance(raw_data, list)
            matches = flatten_live(raw_data) if structurally_valid else []
            n = len(matches) if structurally_valid else -1

        except Exception as e:
            matches = []
            n = -1
            http_status = None
            err = f"{type(e).__name__}: {str(e)[:220]}"
        finally:
            await client.close()

        empty_200 = (
            int(http_status or 0) == 200
            and structurally_valid
            and n == 0
        )
        suspicious = (
            n < 0
            or (http_status is not None and int(http_status) >= 400)
            or empty_200
            or (prev >= 20 and n < max(5, int(prev * 0.20)))
        )
        attempts.append({
            "attempt": i + 1,
            "http_status": http_status,
            "live_matches_found": n,
            "error": err,
            "empty_200": bool(locals().get("empty_200", False)),
            "suspicious": suspicious,
        })

        if n > best_count:
            best_count, best_matches, best_status = n, matches, http_status

        if not suspicious:
            _v572_clear_rate_limit()
            status = "LIVE_FEED_OK"
            if prev >= 20 and n < prev * 0.5:
                status = "LIVE_FEED_DEGRADED"
            _v571_store_live_count(n)
            return {
                "status": status,
                "matches": matches,
                "live_matches_found": n,
                "previous_live_matches_found": prev,
                "attempts": attempts,
                "safe_for_signals": True,
                "safe_for_learning": True,
                "rate_limit": _v572_rate_status(),
            }

        if empty_200 and (i + 1) >= max(1, FEED_EMPTY_200_MAX_ATTEMPTS):
            break

        if i < FEED_GUARD_RETRIES - 1:
            await asyncio.sleep(FEED_GUARD_NORMAL_DELAYS[min(i, len(FEED_GUARD_NORMAL_DELAYS)-1)])

    all_empty_200 = bool(attempts) and all(
        int(a.get("http_status") or 0) == 200
        and int(a.get("live_matches_found") or 0) == 0
        and a.get("empty_200")
        for a in attempts
    )

    return {
        "status": (
            "LIVE_FEED_EMPTY_200"
            if all_empty_200
            else ("LIVE_FEED_OUTAGE" if best_count <= 0 else "LIVE_FEED_DEGRADED")
        ),
        "matches": best_matches,
        "live_matches_found": max(0, best_count),
        "previous_live_matches_found": prev,
        "last_http_status": best_status,
        "attempts": attempts,
        "safe_for_signals": False,
        "safe_for_learning": False,
        "rate_limit": _v572_rate_status(),
        "message": "Live feed remained suspicious after retries. Signals and Self Learning are frozen.",
    }


# ===== V5.7.4 TARGETED FINAL MATCH VERIFY =====
# If a strong candidate disappears from the final whole-live snapshot,
# verify only that match_id via details/summary before downgrading freshness.
# This uses the existing provider quota guard and does NOT change probabilities.

FINAL_VERIFY_MIN_PROB = float(os.environ.get("FINAL_VERIFY_MIN_PROB", "65"))
FINAL_VERIFY_MAX_MATCHES = int(os.environ.get("FINAL_VERIFY_MAX_MATCHES", "3"))


def _v574_extract_score_minute(payload: Any) -> Dict[str, Any]:
    """
    Best-effort parser for targeted details/summary responses.
    Returns {"ok": bool, "home": int|None, "away": int|None, "minute": int|None, "in_progress": bool|None}
    """
    out = {"ok": False, "home": None, "away": None, "minute": None, "in_progress": None}
    if payload is None:
        return out

    # Normalize nested dict/list structures.
    nodes = []
    if isinstance(payload, dict):
        nodes.append(payload)
        for key in ("match", "data", "details", "event"):
            v = payload.get(key)
            if isinstance(v, dict):
                nodes.append(v)
            elif isinstance(v, list):
                nodes.extend([x for x in v if isinstance(x, dict)])
    elif isinstance(payload, list):
        nodes.extend([x for x in payload if isinstance(x, dict)])

    def as_int(v):
        try:
            if v is None or v == "":
                return None
            s = str(v).strip()
            # Accept 45+2 / 90+4
            if "+" in s:
                a, b = s.replace("'", "").split("+", 1)
                return int(float(a)) + int(float(b))
            return int(float(s.replace("'", "")))
        except Exception:
            return None

    for n in nodes:
        scores = n.get("scores") if isinstance(n.get("scores"), dict) else None
        if scores:
            h = as_int(scores.get("home"))
            a = as_int(scores.get("away"))
            if h is not None and a is not None:
                out["home"], out["away"] = h, a
                out["ok"] = True

        # Common alternate shapes.
        for hk, ak in (
            ("home_score", "away_score"),
            ("score_home", "score_away"),
            ("home", "away"),
        ):
            if out["home"] is None or out["away"] is None:
                h = as_int(n.get(hk))
                a = as_int(n.get(ak))
                if h is not None and a is not None:
                    out["home"], out["away"] = h, a
                    out["ok"] = True
                    break

        ms = n.get("match_status") if isinstance(n.get("match_status"), dict) else {}
        minute = (
            ms.get("live_time")
            or n.get("minute")
            or n.get("live_time")
            or n.get("time")
        )
        mi = parse_minute(minute) if "parse_minute" in globals() else as_int(minute)
        if mi is not None:
            out["minute"] = mi

        ip = ms.get("is_in_progress")
        if isinstance(ip, bool):
            out["in_progress"] = ip
        elif isinstance(n.get("is_in_progress"), bool):
            out["in_progress"] = n.get("is_in_progress")

    return out


async def _v574_verify_match_id(match_id: str) -> Dict[str, Any]:
    """
    One targeted verification for a candidate missing from final live list.
    Tries details first, then summary only if details cannot confirm.
    """
    client = ZylaClient()
    attempts = []
    try:
        for endpoint in ("details", "summary"):
            try:
                r = await client.get(endpoint, {"match_id": match_id}, purpose="targeted_verify")
            except Exception as e:
                attempts.append({"endpoint": endpoint, "ok": False, "error": f"{type(e).__name__}: {str(e)[:160]}"})
                continue

            parsed = _v574_extract_score_minute(r.get("data"))
            attempts.append({
                "endpoint": endpoint,
                "http_status": r.get("status"),
                "ok": bool(r.get("ok")),
                "parsed": parsed,
                "cache_hit": r.get("cache_hit", False),
            })
            if r.get("ok") and parsed.get("ok"):
                return {
                    "verified": True,
                    "endpoint": endpoint,
                    "http_status": r.get("status"),
                    "score_home": parsed.get("home"),
                    "score_away": parsed.get("away"),
                    "minute": parsed.get("minute"),
                    "in_progress": parsed.get("in_progress"),
                    "attempts": attempts,
                }

            # Stop immediately on quota/cooldown signals.
            if int(r.get("status") or 0) == 429 or r.get("error") in {"RATE_LIMIT_COOLDOWN", "LOCAL_RATE_LIMIT", "SCAN_API_BUDGET_EXHAUSTED"}:
                break
    finally:
        await client.close()

    return {"verified": False, "attempts": attempts}


def _v574_candidate_probability(view: Dict[str, Any]) -> float:
    vals = []
    for key in ("goal_board_65_99", "DECISION_CONTROL_ITEMS", "ADAPTIVE_ITEMS", "signals", "all_signals"):
        items = view.get(key)
        if isinstance(items, list):
            for x in items:
                if not isinstance(x, dict):
                    continue
                for pk in ("adaptive_probability", "adjusted_probability", "probability", "p", "percent"):
                    try:
                        if x.get(pk) is not None:
                            vals.append(float(x.get(pk)))
                    except Exception:
                        pass
    for pk in ("probability", "best_probability", "top_probability"):
        try:
            if view.get(pk) is not None:
                vals.append(float(view.get(pk)))
        except Exception:
            pass
    return max(vals) if vals else 0.0


async def _v574_targeted_freshness_recovery(
    analyzed_views: Optional[List[Dict[str, Any]]],
    final_live_matches: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    Recover only strong candidates that were otherwise marked
    FINAL_LIVE_SNAPSHOT_MISSING.
    """
    recovered = {}
    checked = []
    analyzed_views = analyzed_views if isinstance(analyzed_views, list) else []
    final_live_matches = final_live_matches if isinstance(final_live_matches, list) else []
    final_ids = {str(m.get("match_id")) for m in final_live_matches if isinstance(m, dict) and m.get("match_id")}

    candidates = []
    for v in analyzed_views:
        if not isinstance(v, dict):
            continue
        mid = v.get("match_id") or (v.get("match") or {}).get("match_id")
        if not mid or str(mid) in final_ids:
            continue
        p = _v574_candidate_probability(v)
        if p >= FINAL_VERIFY_MIN_PROB:
            candidates.append((p, str(mid), v))

    candidates.sort(reverse=True, key=lambda t: t[0])

    for p, mid, v in candidates[:FINAL_VERIFY_MAX_MATCHES]:
        ver = await _v574_verify_match_id(mid)
        item = {"match_id": mid, "probability": p, "verification": ver}
        checked.append(item)
        if not ver.get("verified"):
            continue

        # Compare with analyzed score; score conflict remains a hard block.
        match = v.get("match") if isinstance(v.get("match"), dict) else v
        old_h = match.get("score_home")
        old_a = match.get("score_away")
        if old_h is None or old_a is None:
            score = match.get("score")
            if isinstance(score, dict):
                old_h, old_a = score.get("home"), score.get("away")

        try:
            score_ok = (old_h is None or old_a is None) or (
                int(old_h) == int(ver.get("score_home")) and int(old_a) == int(ver.get("score_away"))
            )
        except Exception:
            score_ok = True

        if not score_ok:
            item["result"] = "BLOCKED_SCORE_CONFLICT"
            continue

        if ver.get("in_progress") is False:
            item["result"] = "BLOCKED_NOT_IN_PROGRESS"
            continue

        recovered[mid] = {
            "status": "CONFIRMED",
            "reason": "TARGETED_MATCH_VERIFY",
            "verified_endpoint": ver.get("endpoint"),
            "verified_http_status": ver.get("http_status"),
            "verified_score": [ver.get("score_home"), ver.get("score_away")],
            "verified_minute": ver.get("minute"),
        }
        item["result"] = "RECOVERED_CONFIRMED"

    return {"recovered": recovered, "checked": checked}


# ============================================================
# V5.7.5d — SCORE SYNC GUARD
# Every ENTER 75%+ / ADAPTIVE TAKE_SOON / TAKE_NOW must pass
# a targeted details->summary score+minute verification before
# it can be exposed as a strong signal.
# ============================================================

V575D_SCORE_SYNC_MIN_PROB = 75.0


def _v575d_score_sync_requirement(view: Dict[str, Any]) -> Dict[str, Any]:
    reasons: List[str] = []
    strongest_enter = 0.0

    for item in list(view.get("goal_board_65_99") or []):
        try:
            p = float(item.get("probability") or 0)
        except Exception:
            p = 0.0
        if item.get("decision") == "ENTER" and p >= V575D_SCORE_SYNC_MIN_PROB:
            strongest_enter = max(strongest_enter, p)

    if strongest_enter >= V575D_SCORE_SYNC_MIN_PROB:
        reasons.append(f"ENTER_{round1(strongest_enter)}")

    adaptive_states = []
    for item in list(view.get("ADAPTIVE_ITEMS") or []):
        state = str((item.get("FLOW") or {}).get("state") or "")
        if state in {"TAKE_SOON", "TAKE_NOW"}:
            adaptive_states.append(state)

    best_adaptive = view.get("BEST_ADAPTIVE_SIGNAL") or {}
    best_state = str((best_adaptive.get("FLOW") or {}).get("state") or "")
    if best_state in {"TAKE_SOON", "TAKE_NOW"}:
        adaptive_states.append(best_state)

    adaptive_states = sorted(set(adaptive_states))
    reasons.extend(adaptive_states)

    return {
        "required": bool(reasons),
        "reasons": reasons,
        "strongest_enter_probability": round1(strongest_enter),
        "adaptive_states": adaptive_states,
    }


def _v575d_targeted_live(
    rep: Dict[str, Any],
    verification: Dict[str, Any],
) -> Dict[str, Any]:
    match = rep.get("match") or {}
    old_score = match.get("score") or {}
    vh = verification.get("score_home")
    va = verification.get("score_away")
    vm = verification.get("minute")

    if vh is None:
        vh = old_score.get("home")
    if va is None:
        va = old_score.get("away")
    if vm is None:
        vm = match.get("minute")

    return {
        "match_id": str(rep.get("match_id") or match.get("match_id") or ""),
        "home": match.get("home"),
        "away": match.get("away"),
        "tournament": match.get("tournament"),
        "minute": int(vm or 0),
        "minute_valid": True,
        "raw_live_time": vm,
        "stage": match.get("stage"),
        "score": score_obj(vh, va),
        "red_cards": match.get("red_cards") or {"home": 0, "away": 0},
        "is_in_progress": verification.get("in_progress") is not False,
    }


def _v575d_recalculate_report_from_targeted(
    rep: Dict[str, Any],
    verification: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Recalculate the model with the already-fetched metrics/context, but with
    targeted details/summary score+minute as the only authoritative live state.
    No extra stats request is made here.
    """
    out = copy.deepcopy(rep)
    live = _v575d_targeted_live(out, verification)

    metrics = out.get("metrics") or {}
    q = out.get("data_quality") or {}
    lineups = out.get("lineups") or parse_lineups(None)
    odds = out.get("odds_snapshot") or compact_odds(None)
    h2h = out.get("h2h_context") or h2h_context(None)

    pressure = pressure_score(metrics, int(live.get("minute") or 0))
    signals = build_signals(live, metrics, q, pressure, lineups)

    for s in signals:
        s["new_or_changed"] = is_new_or_changed(str(out.get("match_id") or ""), s)

    strong = [s for s in signals if s.get("decision") == "ENTER"]
    live_watch = [s for s in signals if s.get("live_decision") in {"LIVE_ENTER", "WATCH"}]
    live_watch.sort(key=lambda x: float(x.get("live_probability") or 0), reverse=True)

    top3 = signals[:3]
    corr = correlation_groups(signals)
    consensus = consensus_report(
        live,
        metrics,
        q,
        pressure,
        lineups,
        odds,
        h2h,
        top3[0] if top3 else None,
    )

    out["match"] = {
        **(out.get("match") or {}),
        "home": live.get("home"),
        "away": live.get("away"),
        "tournament": live.get("tournament"),
        "minute": live.get("minute"),
        "minute_valid": True,
        "raw_live_time": live.get("raw_live_time"),
        "stage": live.get("stage"),
        "score": live.get("score"),
        "red_cards": live.get("red_cards"),
    }
    out["pressure"] = pressure
    out["consensus"] = consensus
    out["correlation_groups"] = corr
    out["nearest_goal"] = nearest_goal_assessment(signals, live, q, pressure)
    out["top_candidate"] = top3[0] if top3 else None
    out["top_3_signals"] = top3
    out["strong_signals"] = strong
    out["live_watchlist"] = live_watch[:5]
    out["quality_blocked_signals"] = [s for s in signals if s.get("data_quality_blocked")]
    out["all_signals"] = signals

    old_sync = out.get("score_sync") or {}
    out["score_sync"] = {
        **old_sync,
        "ok": True,
        "live_score": old_sync.get("live_score"),
        "details_score": live.get("score"),
        "authoritative_source": f"targeted_{verification.get('endpoint') or 'details_summary'}",
        "verified_minute": live.get("minute"),
        "recalculated": True,
    }
    out.setdefault("diagnostic", {})
    out["diagnostic"]["v575d_recalculated_from_targeted"] = True
    out["diagnostic"]["v575d_targeted_endpoint"] = verification.get("endpoint")
    return out


def _v575d_fail_closed_freshness(
    prior: Dict[str, Any],
    reason: str,
    verification: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        **(prior or {}),
        "status": "UNCONFIRMED",
        "ok_for_display": True,
        "ok_for_enter": False,
        "reason": reason,
        "score_changed": False,
        "targeted_verify": verification,
    }


def _v575d_guard_allows_strong(view: Dict[str, Any]) -> bool:
    guard = view.get("SCORE_SYNC_GUARD") or {}
    return str(guard.get("status") or "") == "VERIFIED"


V575F_BUDGET_SKIP_CODES = {
    "SCAN_BUDGET_RESERVED_FOR_FINAL_GUARDS",
    "FINAL_SNAPSHOT_RESERVE_EXHAUSTED",
    "TARGETED_VERIFY_RESERVE_EXHAUSTED",
    "SCAN_API_BUDGET_EXHAUSTED",
    "DEEP_CORE_RESERVE_PROTECTED",
    "LOCAL_RATE_LIMIT",
    "RATE_LIMIT_COOLDOWN",
}

def _v575f_is_budget_skip(error: Any) -> bool:
    s = str(error or "")
    return any(code in s for code in V575F_BUDGET_SKIP_CODES)


def _v575f_failure_item(rep: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "match_id": rep.get("match_id"),
        "error": rep.get("error") or rep.get("status"),
    }


@mcp.tool()
async def scan_final_live(limit: int = 18, max_pool: int = 80, concurrency: int = 2, focus: str = "balanced") -> Dict[str, Any]:
    """
    Hidden Signal V5.7.5m — main live scanner.

    focus modes: balanced / first_half / second_half.

    Flow:
      1) broad live snapshot
      2) smart diversified prefilter
      3) deep stats analysis
      4) SAVE memory for every valid deep report immediately
      5) compare with previous scan for rising pressure
      6) second live snapshot
      7) exact-id or team-name freshness resolution
      8) targeted Score Sync Guard for every ENTER 75%+ / TAKE_SOON / TAKE_NOW
      9) SCORE_CONFLICT kills stale ENTER and recalculates on targeted score+minute
      10) expose only score-synced strong entries; visible WATCH/radar remain available
    """
    started = time.time()
    focus_mode = _v575k_normalize_focus(focus)
    _SCAN_FOCUS["mode"] = focus_mode
    previous_state = _load_scan_state()
    scan_history = _v55_load_history()
    decision_journal = _v56_load_journal()
    learning_state = _v57_load_state()
    _v573_scan_budget_start()

    # ---------- First broad snapshot, protected by V5.7.1 Feed Guard ----------
    feed_guard = await _v571_guarded_live_snapshot()
    matches = feed_guard.get("matches") or []
    if not feed_guard.get("safe_for_signals"):
        return {
            "version": VERSION,
            "model_type": MODEL_TYPE,
            "FINAL_MATCH_VERIFY": targeted_final_verify if "targeted_final_verify" in locals() else {"recovered": {}, "checked": []},
        "LIVE_FEED_STATUS": feed_guard.get("status"),
            "LIVE_FEED_GUARD": feed_guard,
            "ADAPTIVE_TAKE_NOW": [],
            "ADAPTIVE_TAKE_SOON": [],
            "ADAPTIVE_TAKE_LATER": [],
            "ADAPTIVE_EMERGING": [],
            "FIRST_HALF_EMERGING": [],
            "PRESSURE_TREND_NOW": [],
            "SELF_LEARNING_FROZEN_THIS_SCAN": True,
            "SELF_LEARNING_REPORT": _v57_learning_report(learning_state),
            "message": "Источник live временно нестабилен. Старые сигналы не выдаются как свежие, а Self Learning на этом скане заморожен.",
        }

    pool = _stage1_live_pool(matches, max_pool=max_pool)

    # V5.7.5j: do not admit 18 matches when the provider budget can only
    # guarantee ~8 core-stat calls after final/targeted/enrichment reserves.
    rotation_queue = _v575j_prune_rotation_queue(_v575j_load_rotation_queue(), matches)
    rotation_queue_before = len(rotation_queue)
    deep_budget_plan = _v575j_deep_budget_plan(limit, focus=focus_mode)
    requested_candidates = _v575j_select_with_rotation(pool, deep_budget_plan["requested_limit"], rotation_queue, focus=focus_mode)
    chosen = _v575j_select_with_rotation(pool, deep_budget_plan["effective_core_slots"], rotation_queue, focus=focus_mode)
    _v575j_set_core_reservation(len(chosen))
    chosen_ids = {str(m.get("match_id") or "") for m in chosen}

    budget_deferred = []
    for m in requested_candidates:
        if str(m.get("match_id") or "") in chosen_ids:
            continue
        defer_reason = (
            "DEFERRED_BY_ROLLING_MINUTE_BUDGET"
            if deep_budget_plan.get("binding_constraint") == "ROLLING_60S_LIMIT"
            else "DEFERRED_BY_PROACTIVE_BUDGET"
        )
        row = _v575j_mark_deferred(rotation_queue, m, reason=defer_reason)
        if row:
            budget_deferred.append(_v575j_queue_item_public(row))
    _v575j_save_rotation_queue(rotation_queue)

    # ---------- Deep analysis ----------
    sem = asyncio.Semaphore(max(1, min(int(concurrency), 3)))

    async def analyze_one(m):
        async with sem:
            try:
                return await analyze_match_internal(str(m["match_id"]), exact_live=m)
            except Exception as e:
                return {
                    "status": "ERROR",
                    "match_id": m.get("match_id"),
                    "error": repr(e),
                }

    deep_reports = await asyncio.gather(*(analyze_one(m) for m in chosen))

    # ---------- IMPORTANT: build memory BEFORE freshness guard ----------
    new_state = {}
    momentum_map = {}
    parser_failures = []
    budget_skipped = []

    chosen_by_id = {str(m.get("match_id") or ""): m for m in chosen}
    ok_reports = []
    for rep in deep_reports:
        mid = str(rep.get("match_id") or "")
        if rep.get("status") != "OK":
            failure = _v575f_failure_item(rep)
            if _v575f_is_budget_skip(failure.get("error")):
                budget_skipped.append(failure)
                m = chosen_by_id.get(mid)
                if m:
                    _v575j_mark_deferred(rotation_queue, m, reason="DEFERRED_AFTER_RUNTIME_BUDGET_GUARD")
            else:
                parser_failures.append(failure)
                # It was actually attempted, so do not let rotation endlessly
                # retry a parser/data failure as if it had never been analyzed.
                rotation_queue.pop(mid, None)
            continue

        rotation_queue.pop(mid, None)
        prev = previous_state.get(mid)
        momentum = _momentum_from_history(rep, prev)
        momentum_map[mid] = momentum

        # Always retain this snapshot, even if final live confirmation later fails.
        snap = momentum.get("current") if momentum else _build_scan_snapshot(rep)
        new_state[mid] = snap
        _v55_append_history(scan_history, mid, snap)
        ok_reports.append(rep)

    _v575j_save_rotation_queue(rotation_queue)
    rotation_queue_after = len(rotation_queue)

    # Preserve recent history for matches not selected in this pass.
    now_ts = time.time()
    for mid, snap in previous_state.items():
        if mid in new_state:
            continue
        try:
            if now_ts - float(snap.get("ts") or 0) <= 45 * 60:
                new_state[mid] = snap
        except Exception:
            pass

    _save_scan_state(new_state)
    _v55_save_history(scan_history)

    # ---------- Final broad refresh ----------
    # V5.7.5 root-cause fix:
    # reserve budget for this call and bypass the short live cache so the final
    # snapshot is a real provider refresh, not a blocked/old pseudo-snapshot.
    final_budget_before = _v573_scan_budget_status()
    final_client = ZylaClient()
    try:
        final_r = await final_client.live(
            force_refresh=True,
            purpose="final_snapshot",
        )
        final_matches = flatten_live(final_r.get("data"))
    finally:
        await final_client.close()
    final_budget_after = _v573_scan_budget_status()

    final_http_status = int(final_r.get("status") or 0)
    final_error = final_r.get("error")
    final_cache_hit = bool(final_r.get("cache_hit", False))
    final_payload_is_list = isinstance(final_r.get("data"), list)

    if final_r.get("ok") and len(final_matches) > 0:
        final_snapshot_status = "FINAL_SNAPSHOT_OK"
    elif final_r.get("ok") and final_payload_is_list and len(final_matches) == 0:
        final_snapshot_status = (
            "FINAL_SNAPSHOT_DEGRADED_EMPTY_200"
            if len(matches) > 0
            else "FINAL_SNAPSHOT_EMPTY_200"
        )
    elif final_error in {
        "SCAN_BUDGET_RESERVED_FOR_FINAL_GUARDS",
        "FINAL_SNAPSHOT_RESERVE_EXHAUSTED",
        "SCAN_API_BUDGET_EXHAUSTED",
    }:
        final_snapshot_status = "FINAL_SNAPSHOT_GUARD_BLOCKED"
    elif final_http_status == 429:
        final_snapshot_status = "FINAL_SNAPSHOT_RATE_LIMITED"
    else:
        final_snapshot_status = "FINAL_SNAPSHOT_REQUEST_FAILED"

    targeted_final_verify = {"recovered": {}, "checked": []}
    targeted_checked_count = 0
    targeted_verification_cache: Dict[str, Dict[str, Any]] = {}

    score_sync_guard = {
        "version": VERSION,
        "component_origin": "V5.7.5d-SCORE-SYNC-GUARD",
        "checked": [],
        "conflicts": [],
        "recalculated": [],
        "unverified_blocked": [],
        "not_in_progress_blocked": [],
    }

    # ---------- Build results ----------
    candidates = []
    watch_radar = []
    hard_blocked = []
    freshness_unconfirmed = []
    quality_blocked = []
    analyzed_summaries = []

    for rep in ok_reports:
        mid = str(rep.get("match_id") or "")
        match = rep.get("match") or {}
        momentum = momentum_map.get(mid) or {"available": False, "score": 0.0}

        analyzed_summaries.append(_v53_compact_deep_summary(rep, momentum))

        q = rep.get("data_quality") or {}
        if not q.get("basic_ok"):
            quality_blocked.append({
                "match_id": mid,
                "match": f"{match.get('home')} — {match.get('away')}",
                "data_quality": q.get("score"),
                "missing": q.get("missing"),
            })
            continue

        resolved = _v53_find_final_match(match, mid, final_matches)
        freshness = _v53_freshness_check(
            match,
            resolved.get("match"),
            resolved.get("method"),
            float(resolved.get("confidence") or 0),
        )

        # V5.7.4b TARGETED RECOVERY:
        # First determine whether this unresolved report actually contains a
        # visible 65%+ goal candidate. Only then spend one targeted API check.
        if (
            resolved.get("match") is None
            and freshness.get("status") == "UNCONFIRMED"
            and targeted_checked_count < FINAL_VERIFY_MAX_MATCHES
        ):
            provisional_view = _v53_human_view(rep, momentum, freshness)
            provisional_visible = provisional_view.get("goal_board_65_99") or []
            provisional_best = 0.0
            for _sig in provisional_visible:
                try:
                    provisional_best = max(
                        provisional_best,
                        float(_sig.get("probability") or 0),
                    )
                except Exception:
                    pass

            if provisional_best >= FINAL_VERIFY_MIN_PROB:
                targeted_checked_count += 1
                verification = await _v574_verify_match_id(mid)
                targeted_verification_cache[mid] = verification
                check_item = {
                    "match_id": mid,
                    "match": f"{match.get('home')} — {match.get('away')}",
                    "provisional_probability": round(provisional_best, 1),
                    "verification": verification,
                    "result": "NOT_RECOVERED",
                }
                targeted_final_verify["checked"].append(check_item)

                if verification.get("verified"):
                    verified_h = verification.get("score_home")
                    verified_a = verification.get("score_away")
                    old_score = match.get("score") if isinstance(match.get("score"), dict) else {}
                    old_h = old_score.get("home")
                    old_a = old_score.get("away")

                    try:
                        score_ok = (
                            old_h is None or old_a is None
                            or (
                                int(old_h) == int(verified_h)
                                and int(old_a) == int(verified_a)
                            )
                        )
                    except Exception:
                        score_ok = False

                    still_live = verification.get("in_progress") is not False

                    if score_ok and still_live:
                        verified_minute = verification.get("minute")
                        if verified_minute is None:
                            verified_minute = int(match.get("minute") or 0)

                        synthetic_final = {
                            "match_id": mid,
                            "home": match.get("home"),
                            "away": match.get("away"),
                            "score": {
                                "home": verified_h,
                                "away": verified_a,
                            },
                            "minute": int(verified_minute or 0),
                            "minute_valid": True,
                            "is_in_progress": True,
                        }
                        resolved = {
                            "match": synthetic_final,
                            "method": "TARGETED_MATCH_VERIFY",
                            "confidence": 0.96,
                        }
                        freshness = _v53_freshness_check(
                            match,
                            resolved.get("match"),
                            resolved.get("method"),
                            float(resolved.get("confidence") or 0),
                        )
                        if freshness.get("status") == "CONFIRMED":
                            recovery = {
                                "status": "CONFIRMED",
                                "reason": "TARGETED_MATCH_VERIFY",
                                "verified_endpoint": verification.get("endpoint"),
                                "verified_http_status": verification.get("http_status"),
                                "verified_score": [verified_h, verified_a],
                                "verified_minute": verified_minute,
                            }
                            targeted_final_verify["recovered"][mid] = recovery
                            freshness["reason"] = "TARGETED_MATCH_VERIFY"
                            freshness["targeted_verify"] = recovery
                            check_item["result"] = "RECOVERED_CONFIRMED"
                    elif not score_ok:
                        check_item["result"] = "BLOCKED_SCORE_CONFLICT"
                    elif not still_live:
                        check_item["result"] = "BLOCKED_NOT_IN_PROGRESS"

        if freshness.get("status") == "BLOCKED":
            hard_blocked.append({
                "match_id": mid,
                "match": f"{match.get('home')} — {match.get('away')}",
                "reason": freshness.get("reason"),
                "freshness": freshness,
            })
            continue

        # ----------------------------------------------------
        # V5.7.5d SCORE SYNC GUARD
        # Probe the final ladder on COPY state first. This avoids writing
        # journal/self-learning side effects before score+minute verification.
        # ----------------------------------------------------
        probe_journal = copy.deepcopy(decision_journal)
        probe_learning = copy.deepcopy(learning_state)

        probe_view = _v54_attach_timing(_v53_human_view(rep, momentum, freshness))
        probe_view = _v55_enrich_view(
            rep,
            probe_view,
            list(scan_history.get(mid) or []),
            freshness,
        )
        probe_view = _v56_apply_decision_control(
            rep,
            probe_view,
            probe_journal,
            mid,
        )
        probe_view = _v57_adaptive_control(
            probe_learning,
            probe_view,
        )
        sync_requirement = _v575d_score_sync_requirement(probe_view)

        sync_meta = {
            "required": bool(sync_requirement.get("required")),
            "trigger_reasons": sync_requirement.get("reasons") or [],
            "status": "NOT_REQUIRED",
            "old_score": (rep.get("match") or {}).get("score"),
            "old_minute": (rep.get("match") or {}).get("minute"),
            "authoritative_source": None,
            "old_enter_blocked": False,
            "recalculated": False,
        }

        if sync_requirement.get("required"):
            verification = targeted_verification_cache.get(mid)
            if verification is None:
                verification = await _v574_verify_match_id(mid)
                targeted_verification_cache[mid] = verification

            sync_check = {
                "match_id": mid,
                "match": f"{match.get('home')} — {match.get('away')}",
                "trigger_reasons": sync_requirement.get("reasons") or [],
                "verification": verification,
                "result": "UNVERIFIED",
            }
            score_sync_guard["checked"].append(sync_check)

            if not verification.get("verified"):
                sync_meta.update({
                    "status": "UNVERIFIED",
                    "old_enter_blocked": True,
                    "verification": verification,
                })
                freshness = _v575d_fail_closed_freshness(
                    freshness,
                    "SCORE_SYNC_UNVERIFIED",
                    verification,
                )
                score_sync_guard["unverified_blocked"].append({
                    "match_id": mid,
                    "match": sync_check["match"],
                    "reason": "SCORE_SYNC_UNVERIFIED",
                })
                sync_check["result"] = "BLOCKED_UNVERIFIED"

            elif verification.get("in_progress") is False:
                sync_meta.update({
                    "status": "MATCH_NOT_IN_PROGRESS",
                    "old_enter_blocked": True,
                    "verification": verification,
                })
                score_sync_guard["not_in_progress_blocked"].append({
                    "match_id": mid,
                    "match": sync_check["match"],
                    "reason": "MATCH_NOT_IN_PROGRESS",
                })
                sync_check["result"] = "BLOCKED_NOT_IN_PROGRESS"
                hard_blocked.append({
                    "match_id": mid,
                    "match": sync_check["match"],
                    "reason": "MATCH_NOT_IN_PROGRESS",
                    "freshness": {
                        **freshness,
                        "status": "BLOCKED",
                        "ok_for_display": False,
                        "ok_for_enter": False,
                        "reason": "MATCH_NOT_IN_PROGRESS",
                    },
                    "SCORE_SYNC_GUARD": sync_meta,
                })
                continue

            else:
                targeted_live = _v575d_targeted_live(rep, verification)
                old_match = rep.get("match") or {}
                old_score = old_match.get("score") or {}
                new_score = targeted_live.get("score") or {}
                old_minute = int(old_match.get("minute") or 0)
                new_minute = int(targeted_live.get("minute") or old_minute)
                minute_drift = new_minute - old_minute
                score_conflict = not score_equal(old_score, new_score)
                minute_conflict = abs(minute_drift) > int(FINAL_MINUTE_DRIFT_MAX)

                sync_meta.update({
                    "verification": verification,
                    "verified_score": new_score,
                    "verified_minute": new_minute,
                    "minute_drift": minute_drift,
                    "authoritative_source": f"targeted_{verification.get('endpoint') or 'details_summary'}",
                })

                if score_conflict:
                    # Register the score change against the old state first.
                    # This activates the existing post-goal cooldown and kills
                    # the stale pre-goal ENTER deterministically.
                    _v53_freshness_check(
                        old_match,
                        targeted_live,
                        "TARGETED_SCORE_SYNC",
                        1.0,
                    )

                    sync_meta.update({
                        "status": "SCORE_CONFLICT",
                        "old_enter_blocked": True,
                        "recalculated": True,
                        "conflict_type": "SCORE",
                    })
                    score_sync_guard["conflicts"].append({
                        "match_id": mid,
                        "match": sync_check["match"],
                        "type": "SCORE_CONFLICT",
                        "old_score": old_score,
                        "fresh_score": new_score,
                        "old_minute": old_minute,
                        "fresh_minute": new_minute,
                    })

                    rep = _v575d_recalculate_report_from_targeted(rep, verification)
                    match = rep.get("match") or {}
                    momentum = _momentum_from_history(rep, previous_state.get(mid))
                    momentum_map[mid] = momentum

                    freshness = _v53_freshness_check(
                        rep.get("match") or {},
                        targeted_live,
                        "TARGETED_SCORE_SYNC",
                        1.0,
                    )
                    # Even if another freshness branch changes later, the old
                    # ENTER is never allowed to survive a detected score conflict.
                    if freshness.get("ok_for_enter"):
                        freshness = _v575d_fail_closed_freshness(
                            freshness,
                            "SCORE_CONFLICT_RECALCULATED",
                            verification,
                        )

                    score_sync_guard["recalculated"].append({
                        "match_id": mid,
                        "match": sync_check["match"],
                        "fresh_score": new_score,
                        "fresh_minute": new_minute,
                    })
                    sync_check["result"] = "SCORE_CONFLICT_RECALCULATED"

                elif minute_conflict:
                    # Targeted minute is authoritative, but a material time
                    # disagreement blocks a strong entry on this scan.
                    sync_meta.update({
                        "status": "MINUTE_CONFLICT",
                        "old_enter_blocked": True,
                        "recalculated": True,
                        "conflict_type": "MINUTE",
                    })
                    score_sync_guard["conflicts"].append({
                        "match_id": mid,
                        "match": sync_check["match"],
                        "type": "MINUTE_CONFLICT",
                        "score": new_score,
                        "old_minute": old_minute,
                        "fresh_minute": new_minute,
                    })

                    rep = _v575d_recalculate_report_from_targeted(rep, verification)
                    match = rep.get("match") or {}
                    momentum = _momentum_from_history(rep, previous_state.get(mid))
                    momentum_map[mid] = momentum
                    freshness = _v575d_fail_closed_freshness(
                        freshness,
                        "MINUTE_CONFLICT",
                        verification,
                    )
                    score_sync_guard["recalculated"].append({
                        "match_id": mid,
                        "match": sync_check["match"],
                        "fresh_score": new_score,
                        "fresh_minute": new_minute,
                    })
                    sync_check["result"] = "MINUTE_CONFLICT_RECALCULATED"

                else:
                    # Same score and acceptable minute drift: targeted state
                    # becomes authoritative. Recalculate on targeted minute so
                    # the shown minute and the model are synchronized.
                    rep = _v575d_recalculate_report_from_targeted(rep, verification)
                    match = rep.get("match") or {}
                    momentum = _momentum_from_history(rep, previous_state.get(mid))
                    momentum_map[mid] = momentum
                    freshness = _v53_freshness_check(
                        rep.get("match") or {},
                        targeted_live,
                        "TARGETED_SCORE_SYNC",
                        1.0,
                    )
                    if freshness.get("status") == "CONFIRMED":
                        freshness["reason"] = "TARGETED_SCORE_SYNC"
                        freshness["targeted_verify"] = {
                            "endpoint": verification.get("endpoint"),
                            "http_status": verification.get("http_status"),
                            "score": new_score,
                            "minute": new_minute,
                        }

                    sync_meta.update({
                        "status": "VERIFIED",
                        "recalculated": bool(minute_drift),
                    })
                    sync_check["result"] = "VERIFIED"

        # Resolve learning and write the real decision journal only after the
        # targeted Score Sync Guard has selected the authoritative live state.
        _v57_resolve_pending_for_match(
            learning_state,
            mid,
            rep.get("match") or {},
        )

        view = _v54_attach_timing(_v53_human_view(rep, momentum, freshness))
        view = _v55_enrich_view(
            rep,
            view,
            list(scan_history.get(mid) or []),
            freshness,
        )
        view = _v56_apply_decision_control(
            rep,
            view,
            decision_journal,
            mid,
        )
        view = _v57_adaptive_control(
            learning_state,
            view,
        )
        view["SCORE_SYNC_GUARD"] = sync_meta

        # Last fail-closed invariant: a real strong output may exist only after
        # targeted verification. Any accidental strong state without VERIFIED
        # is downgraded before audit/registration/output.
        final_sync_requirement = _v575d_score_sync_requirement(view)
        if final_sync_requirement.get("required") and not _v575d_guard_allows_strong(view):
            fail_reason = str(sync_meta.get("status") or "SCORE_SYNC_UNVERIFIED")
            view["ADAPTIVE_BRIDGE_APPLIED"] = False
            view["ADAPTIVE_BRIDGE_ITEMS"] = []
            for _item in list(view.get("goal_board_65_99") or []):
                if _item.get("decision") == "ENTER":
                    _item["decision"] = "WATCH"
                    _item["freshness_downgrade"] = True
                    _item["score_sync_guard_block"] = fail_reason
            for _item in list(view.get("DECISION_CONTROL_ITEMS") or []):
                _flow = _item.get("FLOW") or {}
                if _flow.get("state") in {"TAKE_SOON", "TAKE_NOW"}:
                    _flow["state"] = "TAKE_LATER"
                    _item["SCORE_SYNC_GUARD_BLOCK"] = fail_reason
            for _item in list(view.get("ADAPTIVE_ITEMS") or []):
                _flow = _item.get("FLOW") or {}
                if _flow.get("state") in {"TAKE_SOON", "TAKE_NOW"}:
                    _flow["state"] = "TAKE_LATER"
                    _item["SCORE_SYNC_GUARD_BLOCK"] = fail_reason

            # Refresh best pointers/text after fail-closed downgrade.
            _order = {"TAKE_NOW": 5, "TAKE_SOON": 4, "TAKE_LATER": 3, "EMERGING": 2, "RADAR": 1, "PASS": 0}
            _adaptive_items = list(view.get("ADAPTIVE_ITEMS") or [])
            _adaptive_items.sort(
                key=lambda z: (
                    _order.get((z.get("FLOW") or {}).get("state"), 0),
                    float(z.get("PRIORITY_SCORE") or 0),
                ),
                reverse=True,
            )
            view["ADAPTIVE_ITEMS"] = _adaptive_items
            view["BEST_ADAPTIVE_SIGNAL"] = _adaptive_items[0] if _adaptive_items else None
            _best = view.get("BEST_ADAPTIVE_SIGNAL") or {}
            _state = (_best.get("FLOW") or {}).get("state")
            if _state == "TAKE_LATER":
                view["WHEN_TO_ENTER_ADAPTIVE"] = f"🟠 ПОЗЖЕ — Score Sync Guard: {fail_reason}"
            elif _state in {"EMERGING", "RADAR"}:
                view["WHEN_TO_ENTER_ADAPTIVE"] = f"👀 НАЗРЕВАЕТ — Score Sync Guard: {fail_reason}"
            else:
                view["WHEN_TO_ENTER_ADAPTIVE"] = f"🔴 ПРОПУСКАЕМ — Score Sync Guard: {fail_reason}"

        _v575e_commit_adaptive_state(
            decision_journal,
            mid,
            view,
        )

        _v575c2_attach_match_audit(
            decision_journal,
            mid,
            view,
        )

        for adaptive_item in list(view.get("ADAPTIVE_ITEMS") or []):
            _v57_register_pending(
                learning_state,
                mid,
                rep.get("match") or {},
                adaptive_item,
            )
        visible = view.get("goal_board_65_99") or []
        radar = view.get("RISING_RADAR_55_64") or []

        if freshness.get("status") == "UNCONFIRMED":
            freshness_unconfirmed.append({
                "match_id": mid,
                "match": view.get("match"),
                "minute": view.get("minute"),
                "score": view.get("score"),
                "reason": freshness.get("reason"),
                "best": (visible or radar or [None])[0],
            })

        # 65–99 candidates are shown even when freshness is unconfirmed,
        # but ENTER has already been downgraded to WATCH in the view.
        if visible:
            best = visible[0]
            pseudo = {
                "market": best.get("market"),
                "selection": best.get("title"),
                "probability": best.get("probability"),
                "decision": best.get("decision"),
            }
            new_or_changed = is_new_or_changed(mid, pseudo)
            if momentum.get("available") and float(momentum.get("score") or 0) >= 45:
                new_or_changed = True

            candidates.append({
                "match_id": mid,
                **view,
                "new_or_changed": new_or_changed,
                "scout_score": smart_scout_rank(match),
            })
        elif radar or view.get("FIRST_HALF_EMERGING"):
            # Keep dedicated 1H 60%+ candidates even when every legacy board
            # market is below the old 55% radar threshold.
            watch_radar.append({
                "match_id": mid,
                **view,
                "scout_score": smart_scout_rank(match),
            })

    # ---------- Ranking ----------
    candidates.sort(
        key=lambda x: (
            1 if ((x.get("goal_board_65_99") or [{}])[0].get("decision") == "ENTER") else 0,
            float(((x.get("goal_board_65_99") or [{}])[0]).get("probability") or 0),
            float((x.get("SHORT_TERM_MOMENTUM") or {}).get("score") or 0),
        ),
        reverse=True,
    )

    watch_radar.sort(
        key=lambda x: (
            float((x.get("SHORT_TERM_MOMENTUM") or {}).get("score") or 0),
            float(((x.get("RISING_RADAR_55_64") or [{}])[0]).get("probability") or 0),
        ),
        reverse=True,
    )

    analyzed_summaries.sort(
        key=lambda x: (
            float((x.get("best_goal_option") or {}).get("probability") or 0),
            float(x.get("momentum_score") or 0),
        ),
        reverse=True,
    )

    enter_now = [
        c for c in candidates
        if (c.get("goal_board_65_99") or [{}])[0].get("decision") == "ENTER"
        and float(((c.get("goal_board_65_99") or [{}])[0]).get("probability") or 0) >= V575D_SCORE_SYNC_MIN_PROB
        and (c.get("freshness") or {}).get("status") == "CONFIRMED"
        and _v575d_guard_allows_strong(c)
    ]

    rising_now = [
        c for c in candidates
        if float((c.get("SHORT_TERM_MOMENTUM") or {}).get("score") or 0) >= 45
    ]

    radar_rising = [
        c for c in watch_radar
        if float((c.get("SHORT_TERM_MOMENTUM") or {}).get("score") or 0) >= 22
    ]


    # V5.7.5c2: journal save is deferred until global adaptive sections
    # are assembled, so one saved scan contains both per-match and scan-level audit.
    learning_changes = []
    learning_rollbacks = []
    for _market in list((learning_state.get("markets") or {}).keys()):
        _rb = _v57_maybe_rollback(learning_state, _market)
        if _rb:
            learning_rollbacks.append(_rb)
        _ch = _v57_tune_market(learning_state, _market)
        if _ch:
            learning_changes.append(_ch)
    _v57_save_state(learning_state)


    adaptive_take_now = []
    adaptive_take_soon = []
    adaptive_take_later = []
    adaptive_emerging = []
    adaptive_bridge_applied = []

    controlled_take_now = []
    controlled_take_soon = []
    controlled_take_later = []
    controlled_emerging = []

    emerging_goal_now = []
    first_half_emerging_now = []
    flow_take_now = []
    flow_take_soon = []
    flow_take_later = []
    pressure_trend_now = []

    for c in candidates:
        strong_sync_ok = _v575d_guard_allows_strong(c)
        adaptive_best = c.get("BEST_ADAPTIVE_SIGNAL") or {}
        adaptive_state = (adaptive_best.get("FLOW") or {}).get("state")
        if adaptive_state == "TAKE_NOW" and strong_sync_ok:
            adaptive_take_now.append(c)
        elif adaptive_state == "TAKE_SOON" and strong_sync_ok:
            adaptive_take_soon.append(c)
        elif adaptive_state == "TAKE_LATER":
            adaptive_take_later.append(c)
        elif adaptive_state in {"EMERGING", "RADAR"}:
            adaptive_emerging.append(c)

        if c.get("ADAPTIVE_BRIDGE_APPLIED") and strong_sync_ok:
            adaptive_bridge_applied.append(c)

        controlled_best = c.get("BEST_CONTROLLED_SIGNAL") or {}
        controlled_state = (controlled_best.get("FLOW") or {}).get("state")
        if controlled_state == "TAKE_NOW" and strong_sync_ok:
            controlled_take_now.append(c)
        elif controlled_state == "TAKE_SOON" and strong_sync_ok:
            controlled_take_soon.append(c)
        elif controlled_state == "TAKE_LATER":
            controlled_take_later.append(c)
        elif controlled_state in {"EMERGING", "RADAR"}:
            controlled_emerging.append(c)

        if c.get("EMERGING_GOAL"):
            emerging_goal_now.append(c)
        if c.get("FIRST_HALF_EMERGING"):
            first_half_emerging_now.append(c)
        if c.get("FLOW_TAKE_NOW") and strong_sync_ok:
            flow_take_now.append(c)
        if c.get("FLOW_TAKE_SOON") and strong_sync_ok:
            flow_take_soon.append(c)
        if c.get("FLOW_TAKE_LATER"):
            flow_take_later.append(c)
        if float((c.get("PRESSURE_TREND") or {}).get("score") or 0) >= 32:
            pressure_trend_now.append(c)

    # V5.7.5h: 60–64% dedicated first-half signals often live in watch_radar,
    # not in the 65+ candidates list. Surface them globally as requested.
    _fh_seen = {str(x.get("match_id") or "") for x in first_half_emerging_now}
    for c in watch_radar:
        mid = str(c.get("match_id") or "")
        if c.get("FIRST_HALF_EMERGING") and mid not in _fh_seen:
            first_half_emerging_now.append(c)
            _fh_seen.add(mid)

    first_half_now = []
    second_half_now = []
    btts_now = []
    next_goal_now = []
    take_now = []
    take_soon = []
    take_later = []

    for c in candidates:
        strong_sync_ok = _v575d_guard_allows_strong(c)
        ts = c.get("TIMING_SECTIONS") or {}
        if ts.get("FIRST_HALF_GOAL"):
            first_half_now.append(c)
        if ts.get("SECOND_HALF_OR_FT_GOAL"):
            second_half_now.append(c)
        if ts.get("BTTS"):
            btts_now.append(c)
        if ts.get("NEXT_GOAL_5_10"):
            next_goal_now.append(c)

        best_t = c.get("BEST_TIMING_ACTION") or {}
        action = (best_t.get("timing") or {}).get("action")
        if action == "TAKE_NOW" and strong_sync_ok:
            take_now.append(c)
        elif action == "SOON" and strong_sync_ok:
            take_soon.append(c)
        elif action == "LATER":
            take_later.append(c)

    adaptive_audit_log = _v575c2_append_scan_audit(
        decision_journal,
        adaptive_take_now,
        adaptive_take_soon,
        adaptive_take_later,
        adaptive_emerging,
        adaptive_bridge_applied,
        candidates,
    )
    _v56_save_journal(decision_journal)

    return {
        "source": "hidden-signal-v5.7-complete-live",
        "version": VERSION,
        "model_type": MODEL_TYPE,
        "live_snapshot_at": now_iso(),

        "live_matches_found": len(matches),
        "stage1_pool_size": len(pool),
        "SCAN_FOCUS": focus_mode,
        "requested_for_deep_analysis": deep_budget_plan.get("requested_limit"),
        "selected_for_deep_analysis": len(chosen),
        "effective_deep_analysis_limit": deep_budget_plan.get("effective_core_slots"),
        "successfully_analyzed": len(ok_reports),
        "DEEP_BUDGET_PLAN": deep_budget_plan,
        "DEEP_ROTATION_QUEUE": {
            "path": DEEP_ROTATION_QUEUE_PATH,
            "persistent_with_state": STATE_STORAGE_PERSISTENT,
            "size_before": rotation_queue_before,
            "size_after": rotation_queue_after,
            "deferred_this_scan": len(budget_deferred),
            "items": budget_deferred[:12],
        },

        # Main human sections:
        "ENTER_NOW": enter_now,
        "RISING_PRESSURE_NOW": rising_now,
        "ALL_65_99": candidates,
        "RISING_RADAR_55_64": radar_rising[:10],

        # V5.5 Signal Flow:
        "EMERGING_GOAL": emerging_goal_now,
        "FIRST_HALF_EMERGING": first_half_emerging_now,
        "PRESSURE_TREND_NOW": pressure_trend_now,
        "FLOW_TAKE_NOW": flow_take_now,
        "FLOW_TAKE_SOON": flow_take_soon,
        "FLOW_TAKE_LATER": flow_take_later,

        # V5.6 controlled final ladder:
        "CONTROLLED_TAKE_NOW": controlled_take_now,
        "CONTROLLED_TAKE_SOON": controlled_take_soon,
        "CONTROLLED_TAKE_LATER": controlled_take_later,
        "CONTROLLED_EMERGING": controlled_emerging,

        # V5.7 final adaptive ladder:
        "LIVE_FEED_STATUS": feed_guard.get("status"),
        "LIVE_FEED_GUARD": feed_guard,
        "SELF_LEARNING_FROZEN_THIS_SCAN": False,
        "ADAPTIVE_TAKE_NOW": adaptive_take_now,
        "ADAPTIVE_TAKE_SOON": adaptive_take_soon,
        "ADAPTIVE_TAKE_LATER": adaptive_take_later,
        "ADAPTIVE_EMERGING": adaptive_emerging,
        "ADAPTIVE_BRIDGE_APPLIED": adaptive_bridge_applied,
        "ADAPTIVE_AUDIT_LOG": adaptive_audit_log,
        "ADAPTIVE_BRIDGE_CONFIG": {
            "soon_probability": ADAPTIVE_BRIDGE_SOON_PROB,
            "soon_trend": ADAPTIVE_BRIDGE_SOON_TREND,
            "now_probability": ADAPTIVE_BRIDGE_NOW_PROB,
            "now_trend": ADAPTIVE_BRIDGE_NOW_TREND,
            "max_false_pressure_for_soon": ADAPTIVE_BRIDGE_MAX_FALSE_PRESSURE_FOR_SOON,
        },
        "SELF_LEARNING_REPORT": _v57_learning_report(learning_state),
        "SELF_LEARNING_CHANGES_THIS_SCAN": learning_changes,
        "SELF_LEARNING_ROLLBACKS_THIS_SCAN": learning_rollbacks,

        # V5.4 human timing split:
        "GOAL_FIRST_HALF": first_half_now,
        "GOAL_SECOND_HALF_OR_FT": second_half_now,
        "BTTS_NOW": btts_now,
        "NEXT_GOAL_5_10": next_goal_now,
        "TAKE_NOW": take_now,
        "TAKE_SOON": take_soon,
        "TAKE_LATER": take_later,

        # Useful fallback so a scan is never 'dry' without explanation:
        "TOP_ANALYZED": analyzed_summaries[:8],

        # Diagnostics:
        "freshness_unconfirmed": freshness_unconfirmed,
        "hard_freshness_blocked": hard_blocked,
        "quality_blocked": quality_blocked,
        "parser_failures": parser_failures,
        "budget_deferred": budget_deferred,
        "budget_deferred_count": len(budget_deferred),
        "budget_skipped": budget_skipped,
        "budget_skipped_count": len(budget_skipped),
        "enrichment_budget_end": _v575j_enrichment_status(),
        "core_reservation_end": _v575j_core_reservation_status(),
        "scan_memory_matches": len(new_state),
        "final_live_matches_found": len(final_matches),
        "FINAL_SNAPSHOT_STATUS": final_snapshot_status,
        "FINAL_SNAPSHOT_HTTP": final_http_status,
        "FINAL_SNAPSHOT_ERROR": final_error,
        "FINAL_SNAPSHOT_CACHE_HIT": final_cache_hit,
        "FINAL_SNAPSHOT_PAYLOAD_IS_LIST": final_payload_is_list,
        "FINAL_SNAPSHOT_BUDGET_BEFORE": final_budget_before,
        "FINAL_SNAPSHOT_BUDGET_AFTER": final_budget_after,
        "FINAL_MATCH_VERIFY": targeted_final_verify,
        "TARGETED_MATCH_VERIFY": targeted_final_verify.get("checked") or [],
        "SCORE_SYNC_GUARD": score_sync_guard,
        "scan_budget_end": _v573_scan_budget_status(),
        "SCAN_BUDGET_INVARIANT_OK": int(_SCAN_BUDGET.get("used") or 0) <= int(_SCAN_BUDGET.get("limit") or PROVIDER_MAX_CALLS_PER_SCAN),
        "calls_last_60s_end": len(_PROVIDER_CALL_TIMES),

        "stage1_top": [
            {
                "match_id": m.get("match_id"),
                "match": f"{m.get('home')} — {m.get('away')}",
                "minute": m.get("minute"),
                "score": m.get("score"),
                "bucket": _scout_bucket(m),
                "scout_score": smart_scout_rank(m),
                "tournament": m.get("tournament"),
            }
            for m in pool[:12]
        ],

        "latency_ms": int((time.time() - started) * 1000),

        "OUR_THINKING": (
            "Есть подтверждённые ENTER — сначала смотри ENTER_NOW."
            if enter_now else
            "ENTER сейчас нет, но проверь RISING_PRESSURE_NOW и RISING_RADAR_55_64. "
            "Отсутствие матча во втором live-снимке больше не удаляет его автоматически; "
            "такой сигнал остаётся WATCH до подтверждения."
        ),

        "message": (
            f"{VERSION}: ENTER {len(enter_now)}, 65–99% {len(candidates)}, "
            f"rising {len(rising_now)}, radar 55–64 {len(radar_rising)}, "
            f"memory {len(new_state)}, deep {len(ok_reports)}/{len(chosen)} of requested {deep_budget_plan.get('requested_limit')}, "
            f"deferred {len(budget_deferred)}, final_snapshot {final_snapshot_status}, "
            f"targeted_recovered {len(targeted_final_verify.get('recovered') or {})}."
        ),

        "mode": "BOUNDED_SELF_LEARNING_FLOW",
    }





@mcp.tool()
async def verify_live_match(match_id: str) -> Dict[str, Any]:
    """Targeted freshness check for one match_id using details/summary."""
    result = await _v574_verify_match_id(match_id)
    return {
        "version": VERSION,
        "match_id": match_id,
        "TARGETED_VERIFY": result,
        "rate_limit": _v572_rate_status(),
        "scan_budget": _v573_scan_budget_status(),
    }


@mcp.tool()
async def get_adaptive_audit_log(
    match_id: Optional[str] = None,
    limit: int = 5,
) -> Dict[str, Any]:
    """
    Read saved Adaptive/Bridge audit data without calling Zyla.
    If match_id is provided, returns that match's journal rows.
    Otherwise returns recent scan-level audit rows.
    """
    journal = _v56_load_journal()
    n = max(1, min(int(limit), V56_JOURNAL_KEEP))

    if match_id:
        rows = list(journal.get(str(match_id)) or [])
        return {
            "version": VERSION,
            "source": "local_decision_journal",
            "provider_calls": 0,
            "match_id": str(match_id),
            "rows": rows[-n:],
        }

    rows = list(journal.get(V575C2_SCAN_AUDIT_KEY) or [])
    return {
        "version": VERSION,
        "source": "local_decision_journal",
        "provider_calls": 0,
        "rows": rows[-n:],
    }


@mcp.tool()
async def get_provider_guard_status() -> Dict[str, Any]:
    """Show cooldown, local limiter, cache and per-scan provider budget."""
    now = _v572_epoch()
    _v573_trim_calls(now)
    return {
        "version": VERSION,
        "rate_limit": _v572_rate_status(),
        "calls_last_60s": len(_PROVIDER_CALL_TIMES),
        "max_calls_per_minute": PROVIDER_MAX_CALLS_PER_MINUTE,
        "min_gap_seconds": PROVIDER_MIN_GAP,
        "cache_ttl_seconds": PROVIDER_CACHE_TTL,
        "live_cache_ttl_seconds": PROVIDER_LIVE_CACHE_TTL,
        "max_calls_per_scan": PROVIDER_MAX_CALLS_PER_SCAN,
        "final_snapshot_reserve": PROVIDER_FINAL_SNAPSHOT_RESERVE,
        "targeted_verify_reserve": PROVIDER_TARGETED_VERIFY_RESERVE,
        "tier2_enrichment_reserve": V575J_ENRICHMENT_RESERVE,
        "first_half_enrichment_pre_reserve": V575K_FIRST_HALF_ENRICH_RESERVE,
        "second_half_enrichment_pre_reserve": V575M_SECOND_HALF_ENRICH_RESERVE,
        "first_half_deep_share_balanced": V575J_FIRST_HALF_DEEP_SHARE,
        "first_half_deep_share_dedicated": 1.0,
        "second_half_deep_share_dedicated": 1.0,
        "dedicated_first_half_endpoint": "scan_first_half_live",
        "dedicated_second_half_endpoint": "scan_second_half_live",
        "atomic_budget_guard": True,
        "proactive_deep_budgeting": True,
        "rotation_queue": {
            "enabled": True,
            "path": DEEP_ROTATION_QUEUE_PATH,
            "size": len(_v575j_load_rotation_queue()),
            "keep_seconds": V575J_ROTATION_KEEP_SECONDS,
            "max_items": V575J_ROTATION_MAX,
        },
        "adaptive_audit_log": {
            "enabled": True,
            "journal_path": DECISION_JOURNAL_PATH,
            "scan_rows_kept": V56_JOURNAL_KEEP,
            "provider_calls_to_read": 0,
        },
        "adaptive_bridge": {
            "enabled": True,
            "soon_probability": ADAPTIVE_BRIDGE_SOON_PROB,
            "soon_trend": ADAPTIVE_BRIDGE_SOON_TREND,
            "now_probability": ADAPTIVE_BRIDGE_NOW_PROB,
            "now_trend": ADAPTIVE_BRIDGE_NOW_TREND,
        },
        "priority_analysis": {
            "tier_1": "stats_only",
            "tier_2_threshold_balanced": VISIBLE_SIGNAL_MIN,
            "tier_2_threshold_first_half": V575K_FIRST_HALF_ENRICH_THRESHOLD,
            "tier_2_threshold_second_half": V575M_SECOND_HALF_ENRICH_THRESHOLD,
            "tier_2": "details+lineups+odds_if_budget",
            "tier_3_threshold": 78.0,
            "tier_3": "player_stats+h2h_if_budget_balanced_only",
            "first_half_window": [V575K_FIRST_HALF_START_MINUTE, V575K_FIRST_HALF_END_MINUTE],
            "second_half_window": [V575M_SECOND_HALF_START_MINUTE, V575M_SECOND_HALF_END_MINUTE],
            "focus_supported": ["balanced", "first_half", "second_half"],
        },
        "cache_entries": len(_PROVIDER_CACHE),
        "persistence": {
            "directory": STATE_STORAGE_DIR,
            "mode": STATE_STORAGE_MODE,
            "persistent": STATE_STORAGE_PERSISTENT,
            "warning": STATE_STORAGE_WARNING,
            "health": _v575j_state_storage_health(),
        },
        "persistence_paths": {
            "signal_log": SIGNAL_LOG_PATH,
            "scan_state": SCAN_STATE_PATH,
            "scan_history": SCAN_HISTORY_PATH,
            "decision_journal": DECISION_JOURNAL_PATH,
            "learning_state": LEARNING_STATE_PATH,
            "feed_guard_state": FEED_GUARD_STATE_PATH,
            "rate_limit_state": RATE_LIMIT_STATE_PATH,
            "deep_rotation_queue": DEEP_ROTATION_QUEUE_PATH,
        },
        "feed_guard_state": _v575a_load_feed_guard_state(),
        "scan_budget": _v573_scan_budget_status(),
        "enrichment_budget": _v575j_enrichment_status(),
        "core_reservation": _v575j_core_reservation_status(),
        "note": "Provider-side exhausted account quota cannot be bypassed; V5.7.5m dedicated half scanners spend all currently available normal deep-analysis calls only on the requested half (1H 5'–43' or 2H 46'–90'), while preserving final-snapshot and targeted Score Sync safety reserves. Deferred matches rotate instead of becoming parser failures.",
    }

@mcp.tool()
async def get_self_learning_report() -> Dict[str, Any]:
    """Show what V5.7 has learned without changing anything."""
    state = _v57_load_state()
    return {
        "version": VERSION,
        "model_type": MODEL_TYPE,
        **_v57_learning_report(state),
    }


@mcp.tool()
async def reset_self_learning() -> Dict[str, Any]:
    """Reset only adaptive learning data/config. Core model and other scan memory remain intact."""
    try:
        if os.path.exists(LEARNING_STATE_PATH):
            os.remove(LEARNING_STATE_PATH)
        state = _v57_default_learning_state()
        _v57_save_state(state)
        return {
            "ok": True,
            "version": VERSION,
            "message": "Self-learning state reset to safe defaults.",
        }
    except Exception as e:
        return {
            "ok": False,
            "version": VERSION,
            "error": str(e),
        }


@mcp.tool()
async def reset_scan_memory() -> Dict[str, Any]:
    """Clear short-term scan memory used for momentum comparison."""
    try:
        if os.path.exists(SCAN_STATE_PATH):
            os.remove(SCAN_STATE_PATH)
        if os.path.exists(SCAN_HISTORY_PATH):
            os.remove(SCAN_HISTORY_PATH)
        if os.path.exists(DECISION_JOURNAL_PATH):
            os.remove(DECISION_JOURNAL_PATH)
        if os.path.exists(LEARNING_STATE_PATH):
            os.remove(LEARNING_STATE_PATH)
        if os.path.exists(FEED_GUARD_STATE_PATH):
            os.remove(FEED_GUARD_STATE_PATH)
        return {"ok": True, "message": "Память предыдущего лайв-скана очищена."}
    except Exception as e:
        return {"ok": False, "error": repr(e)}

@mcp.tool()
async def scan_goal_hunter(limit: int = 16, concurrency: int = 2) -> Dict[str, Any]:
    """
    Main V5 scanner. Restores the original workflow:
    find where a goal is brewing -> compare all goal markets -> explain who is closer ->
    return only 65-99% candidates -> final live freshness check.
    """
    started = time.time()
    client = ZylaClient()
    try:
        live_r = await client.live()
        matches = flatten_live(live_r.get("data"))
    finally:
        await client.close()

    valid = []
    invalid_minute = 0
    for m in matches:
        if not m.get("minute_valid", True):
            invalid_minute += 1
            continue
        stage = str(m.get("stage") or "").lower()
        if any(x in stage for x in ("finished", "cancelled", "postponed", "not started")):
            continue
        valid.append(m)

    # V5.1 Smart Live Scout:
    # rank the whole live pool by goal-hunting usefulness, keep phase balance and league diversity.
    n = max(1, min(int(limit), 20))
    chosen = _diversified_scout_selection(valid, n)

    sem = asyncio.Semaphore(max(1, min(int(concurrency), 3)))

    async def analyze_one(m):
        async with sem:
            try:
                return await analyze_match_internal(str(m["match_id"]), exact_live=m)
            except Exception as e:
                return {"status": "ERROR", "match_id": m.get("match_id"), "error": repr(e)}

    deep_reports = await asyncio.gather(*(analyze_one(m) for m in chosen))

    # One final snapshot after all deep analysis: anti-stale / anti-goal-race guard.
    final_client = ZylaClient()
    try:
        final_r = await final_client.live()
        final_matches = flatten_live(final_r.get("data"))
    finally:
        await final_client.close()
    final_map = {str(m.get("match_id")): m for m in final_matches if m.get("match_id")}

    candidates = []
    parser_failures = []
    budget_skipped = []
    quality_blocked = []
    stale_blocked = []

    for rep in deep_reports:
        if rep.get("status") != "OK":
            failure = _v575f_failure_item(rep)
            if _v575f_is_budget_skip(failure.get("error")):
                budget_skipped.append(failure)
            else:
                parser_failures.append(failure)
            continue

        freshness = final_freshness_check(
            rep.get("match") or {},
            final_map.get(str(rep.get("match_id") or ""))
        )
        if not freshness.get("ok"):
            stale_blocked.append({
                "match_id": rep.get("match_id"),
                "match": f"{(rep.get('match') or {}).get('home')} — {(rep.get('match') or {}).get('away')}",
                "reason": freshness.get("reason"),
            })
            continue

        q = rep.get("data_quality") or {}
        if not q.get("basic_ok"):
            quality_blocked.append({
                "match_id": rep.get("match_id"),
                "match": f"{(rep.get('match') or {}).get('home')} — {(rep.get('match') or {}).get('away')}",
                "data_quality": q.get("score"),
                "missing": q.get("missing"),
            })
            continue

        view = _goal_hunter_view(rep)
        board = view.get("goal_board_65_99") or []
        if not board:
            continue

        # Suppress exact repeats unless probability/decision changed.
        best = board[0]
        pseudo = {
            "market": best.get("market"),
            "selection": best.get("title"),
            "probability": best.get("probability"),
            "decision": best.get("decision"),
        }
        fresh_signal = is_new_or_changed(str(rep.get("match_id")), pseudo)
        view["new_or_changed"] = fresh_signal
        candidates.append({
            "match_id": rep.get("match_id"),
            **view,
        })

        log_event({
            "event": "goal_hunter_candidate",
            "match_id": rep.get("match_id"),
            "match": view.get("match"),
            "minute": view.get("minute"),
            "score": view.get("score"),
            "best": best,
        })

    candidates.sort(
        key=lambda x: float(((x.get("goal_board_65_99") or [{}])[0]).get("probability") or 0),
        reverse=True
    )

    first_half = [c for c in candidates if int(c.get("minute") or 0) <= 45]
    second_half = [c for c in candidates if int(c.get("minute") or 0) > 45]
    enter = [
        c for c in candidates
        if (c.get("goal_board_65_99") or [{}])[0].get("decision") == "ENTER"
    ]

    return {
        "source": "hidden-signal-v5.7.1-goal-hunter",
        "version": VERSION,
        "live_snapshot_at": now_iso(),
        "live_matches_found": len(matches),
        "selected_for_deep_analysis": len(chosen),
        "smart_scout_top": [
            {
                "match_id": m.get("match_id"),
                "match": f"{m.get('home')} — {m.get('away')}",
                "minute": m.get("minute"),
                "score": m.get("score"),
                "bucket": _scout_bucket(m),
                "scout_score": smart_scout_rank(m),
                "tournament": m.get("tournament"),
            }
            for m in chosen[:10]
        ],
        "invalid_minute": invalid_minute,
        "parser_failures": parser_failures,
        "budget_skipped": budget_skipped,
        "budget_skipped_count": len(budget_skipped),
        "quality_blocked": quality_blocked,
        "stale_blocked": stale_blocked,
        "FIRST_HALF": first_half,
        "SECOND_HALF": second_half,
        "ENTER_NOW": enter,
        "ALL_65_99": candidates,
        "message": (
            "Сигналов 65-99% после умного отбора сейчас нет — пропускаем."
            if not candidates else
            "Нашёл голевые кандидаты. Смотри OUR_THINKING и лучший рынок, а не только цифру."
        ),
        "latency_ms": int((time.time() - started) * 1000),
        "mode": "OLD_STYLE_PLUS_SMART_LIVE_SCOUT",
    }


@mcp.tool()
async def analyze_goal_hunter_match(match_id: str) -> Dict[str, Any]:
    """Deep one-match version of the original goal-hunter workflow."""
    rep = await reactor_match_report(match_id)
    rep = await _final_refresh_for_report(rep)
    if rep.get("status") != "OK":
        return rep
    freshness = rep.get("final_freshness") or {}
    if not freshness.get("ok"):
        return {
            "source": "hidden-signal-v5.7",
            "version": VERSION,
            "blocked": True,
            "reason": freshness.get("reason"),
            "message": "Сигнал скрыт: live уже изменился.",
        }
    return {
        "source": "hidden-signal-v5.7",
        "version": VERSION,
        "report": _goal_hunter_view(rep),
        "technical": {
            "parser_ok": rep.get("parser_ok"),
            "score_sync": rep.get("score_sync"),
            "data_quality": rep.get("data_quality"),
            "final_freshness": freshness,
        }
    }


@mcp.tool()
async def quick_screenshot_goal_hunter(
    home: str,
    away: str,
    minute: int,
    score_home: int,
    score_away: int,
    xg_home: Optional[float] = None,
    xg_away: Optional[float] = None,
    shots_home: Optional[float] = None,
    shots_away: Optional[float] = None,
    sot_home: Optional[float] = None,
    sot_away: Optional[float] = None,
    box_home: Optional[float] = None,
    box_away: Optional[float] = None,
    corners_home: Optional[float] = None,
    corners_away: Optional[float] = None,
    possession_home: Optional[float] = None,
    possession_away: Optional[float] = None,
    dangerous_home: Optional[float] = None,
    dangerous_away: Optional[float] = None,
    big_home: Optional[float] = None,
    big_away: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Fast screenshot mode for ChatGPT vision:
    visible screenshot numbers -> current live cross-check -> same V5 Goal Hunter logic.
    """
    started = time.time()
    client = ZylaClient()
    try:
        live_r = await client.live()
        matches = flatten_live(live_r.get("data"))
    finally:
        await client.close()

    best_match = None
    best_similarity = 0.0
    for m in matches:
        sim = _match_similarity(m, home, away)
        if sim > best_similarity:
            best_similarity, best_match = sim, m

    screenshot_score = score_obj(score_home, score_away)
    mismatch = []

    if best_match and best_similarity >= 0.58:
        live = dict(best_match)
        if live.get("score") != screenshot_score:
            mismatch.append({"type": "score", "screenshot": screenshot_score, "live": live.get("score")})
        if abs(int(live.get("minute") or 0) - int(minute or 0)) > 4:
            mismatch.append({"type": "minute", "screenshot": minute, "live": live.get("minute")})
        source_note = "Скрин найден в свежем live: счёт и минута перепроверены."
    else:
        live = {
            "match_id": None,
            "home": home,
            "away": away,
            "minute": max(0, min(int(minute or 0), 130)),
            "minute_valid": 0 <= int(minute or 0) <= 130,
            "stage": "Screenshot",
            "score": screenshot_score,
            "red_cards": {"home": 0, "away": 0},
            "live_odds_1x2": {},
        }
        source_note = "Уверенное live-сопоставление не найдено: считаю по самому скрину."

    metrics = _screenshot_metrics(
        xg_home, xg_away, shots_home, shots_away,
        sot_home, sot_away, box_home, box_away,
        corners_home, corners_away, possession_home, possession_away,
        dangerous_home, dangerous_away, big_home, big_away,
    )

    q = quality_guard(metrics)
    pressure = pressure_score(metrics, int(live.get("minute") or 0))
    signals = build_signals(live, metrics, q, pressure, {"available": False})
    nearest = nearest_goal_assessment(signals, live, q, pressure)
    rep = {
        "status": "OK",
        "match_id": live.get("match_id"),
        "match": live,
        "metrics": metrics,
        "pressure": pressure,
        "data_quality": q,
        "all_signals": signals,
        "nearest_goal": nearest,
    }

    return {
        "source": "hidden-signal-v5.7.1-screenshot",
        "version": VERSION,
        "live_match_found": bool(best_match and best_similarity >= 0.58),
        "match_similarity": round1(best_similarity * 100),
        "freshness_mismatch": mismatch,
        "source_note": source_note,
        "report": _goal_hunter_view(rep),
        "latency_ms": int((time.time() - started) * 1000),
    }


@mcp.tool()
async def scan_first_half_live() -> Dict[str, Any]:
    """
    V5.7.5m dedicated first-half scanner.

    Fixed policy:
      - only live matches from 5' through 43' are admitted to deep analysis;
      - every currently available normal provider call is spent on 1H core stats;
      - no second-half match can consume a deep-analysis slot;
      - final live snapshot and targeted Score Sync keep their safety reserves;
      - rolling 24/60s and per-scan 14-call guards are still respected.

    This endpoint intentionally exposes no `focus` argument so MCP schema caching
    cannot silently fall back to balanced mode.
    """
    result = await scan_final_live(
        limit=20,
        max_pool=120,
        concurrency=2,
        focus="first_half",
    )
    if isinstance(result, dict):
        result["FIRST_HALF_ONLY_MODE"] = True
        result["FIRST_HALF_WINDOW"] = [V575K_FIRST_HALF_START_MINUTE, V575K_FIRST_HALF_END_MINUTE]
        result["FIRST_HALF_BUDGET_POLICY"] = {
            "deep_share": 1.0,
            "core_stats_priority": "ALL_AVAILABLE_NORMAL_CALLS",
            "second_half_deep_slots": 0,
            "final_snapshot_reserve": PROVIDER_FINAL_SNAPSHOT_RESERVE,
            "targeted_score_sync_reserve": PROVIDER_TARGETED_VERIFY_RESERVE,
            "rolling_minute_limit": PROVIDER_MAX_CALLS_PER_MINUTE,
            "per_scan_limit": PROVIDER_MAX_CALLS_PER_SCAN,
        }
    return result


@mcp.tool()
async def scan_second_half_live() -> Dict[str, Any]:
    """
    V5.7.5m dedicated second-half scanner.

    Fixed policy:
      - only regulation-time live matches from 46' through 90' are admitted;
      - every currently available normal provider call is spent on 2H core stats;
      - no first-half match can consume a deep-analysis slot;
      - final live snapshot and targeted Score Sync keep their safety reserves;
      - rolling 24/60s and per-scan 14-call guards are still respected.
    """
    result = await scan_final_live(
        limit=20,
        max_pool=120,
        concurrency=2,
        focus="second_half",
    )
    if isinstance(result, dict):
        result["SECOND_HALF_ONLY_MODE"] = True
        result["SECOND_HALF_WINDOW"] = [V575M_SECOND_HALF_START_MINUTE, V575M_SECOND_HALF_END_MINUTE]
        result["SECOND_HALF_BUDGET_POLICY"] = {
            "deep_share": 1.0,
            "core_stats_priority": "ALL_AVAILABLE_NORMAL_CALLS",
            "first_half_deep_slots": 0,
            "final_snapshot_reserve": PROVIDER_FINAL_SNAPSHOT_RESERVE,
            "targeted_score_sync_reserve": PROVIDER_TARGETED_VERIFY_RESERVE,
            "rolling_minute_limit": PROVIDER_MAX_CALLS_PER_MINUTE,
            "per_scan_limit": PROVIDER_MAX_CALLS_PER_SCAN,
        }
    return result


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
        "state_storage_persistent": STATE_STORAGE_PERSISTENT,
        "state_storage_mode": STATE_STORAGE_MODE,
        "persistent_storage_warning": STATE_STORAGE_WARNING,
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
