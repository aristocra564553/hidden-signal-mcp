
"""
Hidden Signal — FIRST HALF ENGINE V3.1 COMPLETE
===============================================

Полноценный движок анализа 1-го тайма.

Добавлено:
- HOME/AWAY pressure отдельно
- pressure last 5 min / last 10 min
- pressure acceleration
- attack chain detector
- static dominance override
- false pressure detector
- goal before HT probability
- next 5 / next 10 probability
- team goal before HT probability
- likely scorer side
- goal window
- freshness guard
- data quality guard
- red card / VAR / post-goal uncertainty
- 60–64 WATCH
- 65–74 EMERGING
- 75+ ENTER
- stale data never ENTER
- low trend_score no longer blocks strong static dominance
- generic adapter for scan_final_live-style reports
- built-in self-tests

ВАЖНО:
Вероятности — эвристические ranking-estimates, не калиброванные истинные вероятности.
Их нужно калибровать на большом числе завершённых матчей.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple
import math


# ============================================================
# CONFIG
# ============================================================

@dataclass(slots=True)
class FirstHalfConfig:
    watch_min: float = 60.0
    emerging_min: float = 65.0
    strong_min: float = 75.0
    take_now_min: float = 78.0

    min_minute: int = 5
    max_minute: int = 44

    min_quality_watch: float = 45.0
    min_quality_enter: float = 65.0
    max_freshness_enter_seconds: float = 45.0

    dominance_pressure_min: float = 30.0
    dominance_sot_min: int = 3
    dominance_shots_min: int = 6
    dominance_secondary_pressure: float = 25.0
    dominance_secondary_sot: int = 2
    dominance_pressure_gap_min: float = 10.0

    min_total_shots_evidence: int = 4
    min_total_sot_evidence: int = 1
    min_total_xg_evidence: float = 0.25
    min_leader_pressure_evidence: float = 18.0

    probability_floor: float = 8.0
    probability_cap: float = 90.0

    prior_goal_rate_per_min: float = 0.026
    xg_pace_weight: float = 0.48

    red_card_uncertainty_penalty: float = 0.92

    # New V3.1
    attack_chain_bonus_max: float = 0.18
    acceleration_bonus_max: float = 0.16
    team_goal_share_floor: float = 0.50
    team_goal_share_cap: float = 0.90


DEFAULT_CONFIG = FirstHalfConfig()


# ============================================================
# INPUT TYPES
# ============================================================

@dataclass(slots=True)
class TeamHalfStats:
    shots: int = 0
    shots_on_target: int = 0
    xg: float = 0.0
    box_touches: int = 0
    dangerous_attacks: int = 0
    corners: int = 0
    big_chances: int = 0
    possession: float = 50.0

    # Recent 5 min
    shots_5: int = 0
    sot_5: int = 0
    xg_5: float = 0.0
    box_5: int = 0
    dangerous_5: int = 0
    corners_5: int = 0

    # Recent 10 min
    shots_10: int = 0
    sot_10: int = 0
    xg_10: float = 0.0
    box_10: int = 0
    dangerous_10: int = 0
    corners_10: int = 0

    red_cards: int = 0


@dataclass(slots=True)
class FirstHalfContext:
    home: str
    away: str
    minute: int
    score_home: int
    score_away: int

    home_stats: TeamHalfStats
    away_stats: TeamHalfStats

    trend_score: float = 0.0

    data_quality: float = 100.0
    freshness_seconds: float = 0.0
    freshness_confirmed: bool = True

    post_goal_cooldown: bool = False
    var_active: bool = False
    match_suspended: bool = False

    pre_match_goal_expectation: Optional[float] = None
    league_goal_factor: Optional[float] = None


# ============================================================
# OUTPUT TYPES
# ============================================================

@dataclass(slots=True)
class PressureSnapshot:
    team: str
    full: float
    last_5: float
    last_10: float
    acceleration: float
    level: str
    false_pressure: bool
    reasons: List[str] = field(default_factory=list)


@dataclass(slots=True)
class AttackChain:
    team: str
    active: bool
    score: float
    components: List[str] = field(default_factory=list)


@dataclass(slots=True)
class FirstHalfSignal:
    match: str
    minute: int
    score: str
    market: str

    probability_goal_before_ht: float
    probability_goal_next_5: float
    probability_goal_next_10: float

    probability_home_goal_before_ht: float
    probability_away_goal_before_ht: float

    decision: str
    confidence_band: str
    timing: str
    goal_window: str

    pressure_team: str
    pressure_home: float
    pressure_away: float
    pressure_last_5_home: float
    pressure_last_5_away: float
    pressure_last_10_home: float
    pressure_last_10_away: float
    pressure_acceleration_home: float
    pressure_acceleration_away: float
    pressure_gap: float

    likely_scorer_side: str
    likely_scorer_share: float

    attack_chain_team: str
    attack_chain_active: bool
    attack_chain_score: float

    trend_score: float
    trend_required: bool
    dominance_override: bool
    false_pressure: bool

    freshness: str
    data_quality: float
    attacking_evidence: bool

    reasons: List[str]
    blockers: List[str]
    debug: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# NORMALIZATION
# ============================================================

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _nint(v: Any) -> int:
    try:
        return max(0, int(v or 0))
    except Exception:
        return 0


def _nfloat(v: Any) -> float:
    try:
        return max(0.0, float(v or 0.0))
    except Exception:
        return 0.0


def _poss(v: Any) -> float:
    try:
        return _clamp(float(v), 0.0, 100.0)
    except Exception:
        return 50.0


def normalize_stats(s: TeamHalfStats) -> TeamHalfStats:
    return TeamHalfStats(
        shots=_nint(s.shots),
        shots_on_target=_nint(s.shots_on_target),
        xg=_nfloat(s.xg),
        box_touches=_nint(s.box_touches),
        dangerous_attacks=_nint(s.dangerous_attacks),
        corners=_nint(s.corners),
        big_chances=_nint(s.big_chances),
        possession=_poss(s.possession),

        shots_5=_nint(s.shots_5),
        sot_5=_nint(s.sot_5),
        xg_5=_nfloat(s.xg_5),
        box_5=_nint(s.box_5),
        dangerous_5=_nint(s.dangerous_5),
        corners_5=_nint(s.corners_5),

        shots_10=_nint(s.shots_10),
        sot_10=_nint(s.sot_10),
        xg_10=_nfloat(s.xg_10),
        box_10=_nint(s.box_10),
        dangerous_10=_nint(s.dangerous_10),
        corners_10=_nint(s.corners_10),

        red_cards=_nint(s.red_cards),
    )


# ============================================================
# PRESSURE
# ============================================================

def _pressure_level(v: float) -> str:
    if v >= 40: return "VERY_HIGH"
    if v >= 30: return "HIGH"
    if v >= 20: return "MEDIUM"
    if v >= 12: return "LOW"
    return "VERY_LOW"


def _window_pressure(
    shots: int,
    sot: int,
    xg: float,
    box: int,
    dangerous: int,
    corners: int,
) -> float:
    return (
        sot * 5.2
        + shots * 1.25
        + min(xg, 1.5) * 7.5
        + box * 0.45
        + dangerous * 0.08
        + corners * 0.95
    )


def calculate_pressure(
    team: str,
    stats: TeamHalfStats,
    opp: TeamHalfStats,
    minute: int,
) -> PressureSnapshot:
    s = normalize_stats(stats)
    o = normalize_stats(opp)

    full = (
        s.shots_on_target * 4.0
        + s.shots * 1.05
        + min(s.xg, 2.5) * 6.0
        + s.box_touches * 0.38
        + s.dangerous_attacks * 0.07
        + s.corners * 0.85
        + s.big_chances * 3.6
    )

    shot_gap = max(0, s.shots - o.shots)
    sot_gap = max(0, s.shots_on_target - o.shots_on_target)
    xg_gap = max(0.0, s.xg - o.xg)
    box_gap = max(0, s.box_touches - o.box_touches)
    big_gap = max(0, s.big_chances - o.big_chances)

    dominance = (
        shot_gap * 0.45
        + sot_gap * 1.6
        + xg_gap * 2.8
        + box_gap * 0.18
        + big_gap * 1.4
    )

    possession_bonus = max(0.0, s.possession - 50.0) * 0.035
    full += dominance + possession_bonus

    if minute >= 12:
        full *= _clamp(30.0 / max(1, minute), 0.82, 1.08)

    p5 = _window_pressure(
        s.shots_5, s.sot_5, s.xg_5, s.box_5, s.dangerous_5, s.corners_5
    )
    p10 = _window_pressure(
        s.shots_10, s.sot_10, s.xg_10, s.box_10, s.dangerous_10, s.corners_10
    )

    # Compare last-5 pace against previous 5 min approximation.
    previous_5 = max(0.0, p10 - p5)
    acceleration = p5 - previous_5

    false_pressure = False
    reasons: List[str] = []

    if (
        s.possession >= 62
        and s.shots <= 3
        and s.shots_on_target == 0
        and s.box_touches <= 4
    ):
        false_pressure = True
        reasons.append("владение без реальной остроты")

    if s.shots >= 7 and s.shots_on_target <= 1 and s.xg < 0.40:
        false_pressure = True
        reasons.append("много низкокачественных ударов")

    if false_pressure:
        full *= 0.72
        p5 *= 0.75
        p10 *= 0.75

    if s.shots_on_target >= 3:
        reasons.append(f"{s.shots_on_target} удара(ов) в створ")
    if s.shots >= 6:
        reasons.append(f"{s.shots} ударов")
    if s.xg >= 0.65:
        reasons.append(f"xG {s.xg:.2f}")
    if s.box_touches >= 10:
        reasons.append(f"{s.box_touches} касаний в штрафной")
    if s.big_chances >= 2:
        reasons.append(f"{s.big_chances} больших момента")
    if p5 >= 12:
        reasons.append("сильное давление за последние 5 минут")
    if acceleration >= 6:
        reasons.append("давление ускоряется")

    full = _clamp(full + p5 * 0.30, 0.0, 100.0)

    return PressureSnapshot(
        team=team,
        full=round(full, 1),
        last_5=round(p5, 1),
        last_10=round(p10, 1),
        acceleration=round(acceleration, 1),
        level=_pressure_level(full),
        false_pressure=false_pressure,
        reasons=reasons,
    )


# ============================================================
# ATTACK CHAIN
# ============================================================

def detect_attack_chain(team: str, s: TeamHalfStats) -> AttackChain:
    s = normalize_stats(s)
    score = 0.0
    components: List[str] = []

    if s.sot_5 >= 1:
        score += 3.0
        components.append("удар в створ за 5 минут")

    if s.shots_5 >= 3:
        score += 2.0
        components.append("3+ удара за 5 минут")

    if s.box_5 >= 4:
        score += 2.0
        components.append("серия входов в штрафную")

    if s.corners_5 >= 2:
        score += 1.5
        components.append("серия угловых")

    if s.xg_5 >= 0.20:
        score += 2.5
        components.append("рост xG за 5 минут")

    if s.dangerous_5 >= 8:
        score += 1.5
        components.append("серия опасных атак")

    active = score >= 5.0

    return AttackChain(
        team=team,
        active=active,
        score=round(score, 1),
        components=components,
    )


# ============================================================
# DOMINANCE OVERRIDE
# ============================================================

def dominance_override(
    leader_p: PressureSnapshot,
    leader_s: TeamHalfStats,
    opp_p: PressureSnapshot,
    cfg: FirstHalfConfig,
) -> Tuple[bool, List[str]]:
    s = normalize_stats(leader_s)
    gap = leader_p.full - opp_p.full
    reasons: List[str] = []

    a = (
        leader_p.full >= cfg.dominance_pressure_min
        and s.shots_on_target >= cfg.dominance_sot_min
        and gap >= cfg.dominance_pressure_gap_min
    )

    b = (
        leader_p.full >= cfg.dominance_secondary_pressure
        and s.shots_on_target >= cfg.dominance_secondary_sot
        and s.shots >= cfg.dominance_shots_min
        and gap >= cfg.dominance_pressure_gap_min
    )

    c = (
        s.big_chances >= 2
        and s.shots_on_target >= 2
        and gap >= 8
    )

    d = (
        leader_p.last_5 >= 14
        and leader_p.acceleration >= 5
        and gap >= 6
    )

    if a: reasons.append("3+ в створ + высокий pressure + большой gap")
    if b: reasons.append("6+ ударов, 2+ в створ и устойчивое доминирование")
    if c: reasons.append("2+ больших момента")
    if d: reasons.append("сильное ускорение давления за последние 5 минут")

    return bool(a or b or c or d), reasons


# ============================================================
# EVIDENCE
# ============================================================

def evidence_present(
    h: TeamHalfStats,
    a: TeamHalfStats,
    leader_pressure: float,
    cfg: FirstHalfConfig,
) -> bool:
    h = normalize_stats(h)
    a = normalize_stats(a)

    return bool(
        h.shots_on_target + a.shots_on_target >= cfg.min_total_sot_evidence
        or h.shots + a.shots >= cfg.min_total_shots_evidence
        or h.xg + a.xg >= cfg.min_total_xg_evidence
        or leader_pressure >= cfg.min_leader_pressure_evidence
        or h.sot_5 + a.sot_5 >= 1
        or h.xg_5 + a.xg_5 >= 0.15
    )


# ============================================================
# PROBABILITY MODEL
# ============================================================

def _prob_from_lambda(lmbda: float) -> float:
    return (1.0 - math.exp(-max(0.0, lmbda))) * 100.0


def estimate_probabilities(
    ctx: FirstHalfContext,
    hp: PressureSnapshot,
    ap: PressureSnapshot,
    h_chain: AttackChain,
    a_chain: AttackChain,
    override: bool,
    false_pressure: bool,
    cfg: FirstHalfConfig,
) -> Tuple[float, float, float, Dict[str, float], List[str]]:
    h = normalize_stats(ctx.home_stats)
    a = normalize_stats(ctx.away_stats)

    minute = _clamp(float(ctx.minute), 1.0, 45.0)
    remaining = max(0.0, 45.0 - minute)

    total_xg = h.xg + a.xg
    total_sot = h.shots_on_target + a.shots_on_target
    total_big = h.big_chances + a.big_chances

    prior_lambda = cfg.prior_goal_rate_per_min * remaining

    if total_xg > 0.01 and minute >= 8:
        observed_rate = total_xg / minute
        observed_lambda = observed_rate * remaining
        w = _clamp(cfg.xg_pace_weight, 0.0, 0.75)
        base_lambda = (1 - w) * prior_lambda + w * observed_lambda
    else:
        observed_rate = 0.0
        observed_lambda = prior_lambda
        base_lambda = prior_lambda

    total_pressure = hp.full + ap.full
    pressure_gap = abs(hp.full - ap.full)

    pressure_boost = min(0.42, total_pressure / 100.0 * 0.52)
    dominance_boost = min(0.20, pressure_gap / 100.0 * 0.42)
    sot_boost = min(0.22, total_sot * 0.052)
    big_boost = min(0.15, total_big * 0.06)

    accel = max(hp.acceleration, ap.acceleration)
    acceleration_boost = _clamp(accel / 100.0, 0.0, cfg.acceleration_bonus_max)

    chain_score = max(h_chain.score, a_chain.score)
    attack_chain_boost = _clamp(
        chain_score * 0.015,
        0.0,
        cfg.attack_chain_bonus_max,
    )

    trend_boost = _clamp(float(ctx.trend_score), -10.0, 25.0) * 0.006
    trend_boost = _clamp(trend_boost, -0.06, 0.15)

    override_boost = 0.14 if override else 0.0

    score_boost = 0.0
    if ctx.score_home != ctx.score_away:
        score_boost += 0.05
    if ctx.score_home + ctx.score_away >= 2:
        score_boost += 0.025

    context_boost = 0.0

    if ctx.pre_match_goal_expectation is not None:
        try:
            context_boost += _clamp(
                (float(ctx.pre_match_goal_expectation) - 2.5) * 0.025,
                -0.06,
                0.08,
            )
        except Exception:
            pass

    if ctx.league_goal_factor is not None:
        try:
            context_boost += _clamp(
                (float(ctx.league_goal_factor) - 1.0) * 0.08,
                -0.05,
                0.05,
            )
        except Exception:
            pass

    multiplier = (
        1.0
        + pressure_boost
        + dominance_boost
        + sot_boost
        + big_boost
        + acceleration_boost
        + attack_chain_boost
        + trend_boost
        + override_boost
        + score_boost
        + context_boost
    )

    notes: List[str] = []

    if false_pressure:
        multiplier *= 0.82
        notes.append("понижение за ложное давление")

    if ctx.post_goal_cooldown:
        multiplier *= 0.86
        notes.append("post-goal cooldown")

    if ctx.var_active:
        multiplier *= 0.95
        notes.append("VAR uncertainty")

    if h.red_cards + a.red_cards > 0:
        multiplier *= cfg.red_card_uncertainty_penalty
        notes.append("red-card uncertainty guard")

    if ctx.match_suspended:
        multiplier *= 0.25
        notes.append("матч/поток приостановлен")

    multiplier = _clamp(multiplier, 0.45, 2.55)

    final_lambda = max(0.0, base_lambda * multiplier)

    p_ht = _clamp(
        _prob_from_lambda(final_lambda),
        cfg.probability_floor,
        cfg.probability_cap,
    )

    rate = final_lambda / remaining if remaining > 0 else 0.0

    p5 = _prob_from_lambda(rate * min(5.0, remaining))
    p10 = _prob_from_lambda(rate * min(10.0, remaining))

    debug = {
        "remaining": round(remaining, 2),
        "prior_lambda": round(prior_lambda, 4),
        "observed_rate": round(observed_rate, 4),
        "observed_lambda": round(observed_lambda, 4),
        "base_lambda": round(base_lambda, 4),
        "pressure_boost": round(pressure_boost, 4),
        "dominance_boost": round(dominance_boost, 4),
        "sot_boost": round(sot_boost, 4),
        "big_boost": round(big_boost, 4),
        "acceleration_boost": round(acceleration_boost, 4),
        "attack_chain_boost": round(attack_chain_boost, 4),
        "trend_boost": round(trend_boost, 4),
        "override_boost": round(override_boost, 4),
        "multiplier": round(multiplier, 4),
        "final_lambda": round(final_lambda, 4),
    }

    return (
        round(p_ht, 1),
        round(_clamp(p5, 0, cfg.probability_cap), 1),
        round(_clamp(p10, 0, cfg.probability_cap), 1),
        debug,
        notes,
    )


# ============================================================
# TEAM GOAL BEFORE HT
# ============================================================

def team_goal_split(
    p_any_goal: float,
    home: str,
    away: str,
    hp: PressureSnapshot,
    ap: PressureSnapshot,
    hs: TeamHalfStats,
    as_: TeamHalfStats,
    cfg: FirstHalfConfig,
) -> Tuple[float, float, str, float]:
    h = normalize_stats(hs)
    a = normalize_stats(as_)

    home_strength = (
        hp.full
        + hp.last_5 * 0.35
        + h.shots_on_target * 2.5
        + h.xg * 4.5
        + h.big_chances * 2.8
    )

    away_strength = (
        ap.full
        + ap.last_5 * 0.35
        + a.shots_on_target * 2.5
        + a.xg * 4.5
        + a.big_chances * 2.8
    )

    total = home_strength + away_strength

    if total <= 0:
        share_h = 0.5
    else:
        share_h = home_strength / total

    share_h = _clamp(
        share_h,
        1.0 - cfg.team_goal_share_cap,
        cfg.team_goal_share_cap,
    )
    share_a = 1.0 - share_h

    p_home = p_any_goal * share_h
    p_away = p_any_goal * share_a

    if abs(share_h - share_a) < 0.10:
        likely = "UNCLEAR"
        likely_share = max(share_h, share_a) * 100
    elif share_h > share_a:
        likely = home
        likely_share = share_h * 100
    else:
        likely = away
        likely_share = share_a * 100

    return (
        round(p_home, 1),
        round(p_away, 1),
        likely,
        round(likely_share, 1),
    )


# ============================================================
# DECISION / TIMING / GOAL WINDOW
# ============================================================

def confidence_band(p: float, cfg: FirstHalfConfig) -> str:
    if p >= cfg.strong_min:
        return "STRONG_75_PLUS"
    if p >= cfg.emerging_min:
        return "GOOD_65_74"
    if p >= cfg.watch_min:
        return "WATCH_60_64"
    return "BELOW_60"


def goal_window(minute: int, p5: float, p10: float) -> str:
    if p5 >= 35:
        return f"{minute+1}–{min(45, minute+5)}'"
    if p10 >= 45:
        return f"{minute+3}–{min(45, minute+10)}'"
    return "NO_CLEAR_WINDOW"


def timing(
    p: float,
    minute: int,
    override: bool,
    evidence: bool,
    cfg: FirstHalfConfig,
) -> str:
    if not evidence:
        return "PASS"

    if p >= cfg.take_now_min and minute <= 40:
        return "TAKE_NOW"

    if p >= cfg.strong_min:
        return "TAKE_NOW" if override and minute <= 41 else "TAKE_SOON"

    if p >= cfg.emerging_min:
        return "TAKE_SOON"

    if p >= cfg.watch_min:
        return "WATCH_NEXT_2_4_MIN"

    return "PASS"


# ============================================================
# MAIN ENGINE
# ============================================================

def analyze_first_half(
    ctx: FirstHalfContext,
    cfg: FirstHalfConfig = DEFAULT_CONFIG,
) -> FirstHalfSignal:

    h = normalize_stats(ctx.home_stats)
    a = normalize_stats(ctx.away_stats)

    blockers: List[str] = []

    if ctx.minute < cfg.min_minute:
        blockers.append("слишком ранняя минута")
    if ctx.minute > cfg.max_minute:
        blockers.append("слишком поздняя минута")

    if not ctx.freshness_confirmed:
        freshness = "UNCONFIRMED"
        blockers.append("freshness не подтверждён")
    elif ctx.freshness_seconds > cfg.max_freshness_enter_seconds:
        freshness = f"STALE_{int(ctx.freshness_seconds)}S"
        blockers.append("данные слишком старые для ENTER")
    else:
        freshness = "CONFIRMED"

    if ctx.match_suspended:
        blockers.append("матч/поток приостановлен")

    if ctx.data_quality < cfg.min_quality_watch:
        blockers.append("data_quality слишком низкий")

    hp = calculate_pressure(ctx.home, h, a, ctx.minute)
    ap = calculate_pressure(ctx.away, a, h, ctx.minute)

    if hp.full >= ap.full:
        leader_p, opp_p = hp, ap
        leader_s, opp_s = h, a
    else:
        leader_p, opp_p = ap, hp
        leader_s, opp_s = a, h

    p_gap = round(leader_p.full - opp_p.full, 1)

    h_chain = detect_attack_chain(ctx.home, h)
    a_chain = detect_attack_chain(ctx.away, a)

    if h_chain.score >= a_chain.score:
        chain = h_chain
    else:
        chain = a_chain

    override, override_reasons = dominance_override(
        leader_p, leader_s, opp_p, cfg
    )

    false_pressure = (
        leader_p.false_pressure
        or (
            leader_s.possession >= 60
            and leader_s.box_touches <= 5
            and leader_s.big_chances == 0
        )
        or (
            opp_s.xg > leader_s.xg + 0.30
            and opp_s.shots_on_target >= leader_s.shots_on_target
        )
    )

    evidence = evidence_present(h, a, leader_p.full, cfg)

    p_ht, p5, p10, debug, notes = estimate_probabilities(
        ctx,
        hp,
        ap,
        h_chain,
        a_chain,
        override,
        false_pressure,
        cfg,
    )

    p_home, p_away, likely_team, likely_share = team_goal_split(
        p_ht,
        ctx.home,
        ctx.away,
        hp,
        ap,
        h,
        a,
        cfg,
    )

    if not evidence:
        blockers.append("нет достаточного атакующего подтверждения")

    if p_ht >= cfg.strong_min and ctx.data_quality < cfg.min_quality_enter:
        blockers.append("data_quality недостаточен для ENTER")

    hard_enter_block = bool(
        not ctx.freshness_confirmed
        or ctx.freshness_seconds > cfg.max_freshness_enter_seconds
        or ctx.data_quality < cfg.min_quality_enter
        or ctx.match_suspended
        or not evidence
        or ctx.minute < cfg.min_minute
        or ctx.minute > cfg.max_minute
    )

    if not evidence or p_ht < cfg.watch_min:
        decision = "PASS"
    elif p_ht < cfg.emerging_min:
        decision = "FIRST_HALF_WATCH"
    elif p_ht < cfg.strong_min:
        decision = "FIRST_HALF_EMERGING"
    else:
        decision = "WATCH_BLOCKED" if hard_enter_block else "ENTER_FIRST_HALF"

    if ctx.data_quality < cfg.min_quality_watch:
        decision = "PASS"

    reasons: List[str] = [
        f"давление: {leader_p.team} {leader_p.full:.1f} vs {opp_p.full:.1f}; gap +{p_gap:.1f}",
        f"последние 5 мин: {ctx.home} {hp.last_5:.1f} / {ctx.away} {ap.last_5:.1f}",
        f"ускорение: {ctx.home} {hp.acceleration:.1f} / {ctx.away} {ap.acceleration:.1f}",
    ]

    reasons.extend(leader_p.reasons[:4])

    if chain.active:
        reasons.append(
            f"ATTACK_CHAIN: {chain.team} score {chain.score:.1f}"
        )
        reasons.extend(chain.components[:2])

    if override:
        reasons.append(
            "STATIC_DOMINANCE_OVERRIDE: trend_score не является обязательным"
        )
        reasons.extend(override_reasons[:2])

    if ctx.trend_score >= 12:
        reasons.append(f"trend_score {ctx.trend_score:.1f}: сильное ускорение")
    elif ctx.trend_score >= 5:
        reasons.append(f"trend_score {ctx.trend_score:.1f}: умеренный рост")
    else:
        reasons.append(
            f"trend_score {ctx.trend_score:.1f}: низкий; проверяем static dominance"
        )

    if likely_team != "UNCLEAR":
        reasons.append(
            f"ближе к голу: {likely_team} ({likely_share:.1f}% team-pressure share)"
        )

    reasons.extend(notes[:2])

    return FirstHalfSignal(
        match=f"{ctx.home} — {ctx.away}",
        minute=int(ctx.minute),
        score=f"{ctx.score_home}:{ctx.score_away}",
        market="AT_LEAST_ONE_MORE_GOAL_BEFORE_HT",

        probability_goal_before_ht=p_ht,
        probability_goal_next_5=p5,
        probability_goal_next_10=p10,

        probability_home_goal_before_ht=p_home,
        probability_away_goal_before_ht=p_away,

        decision=decision,
        confidence_band=confidence_band(p_ht, cfg),
        timing=timing(p_ht, ctx.minute, override, evidence, cfg),
        goal_window=goal_window(ctx.minute, p5, p10),

        pressure_team=leader_p.team,
        pressure_home=hp.full,
        pressure_away=ap.full,
        pressure_last_5_home=hp.last_5,
        pressure_last_5_away=ap.last_5,
        pressure_last_10_home=hp.last_10,
        pressure_last_10_away=ap.last_10,
        pressure_acceleration_home=hp.acceleration,
        pressure_acceleration_away=ap.acceleration,
        pressure_gap=p_gap,

        likely_scorer_side=likely_team,
        likely_scorer_share=likely_share,

        attack_chain_team=chain.team,
        attack_chain_active=chain.active,
        attack_chain_score=chain.score,

        trend_score=round(float(ctx.trend_score), 1),
        trend_required=not override,
        dominance_override=override,
        false_pressure=false_pressure,

        freshness=freshness,
        data_quality=round(float(ctx.data_quality), 1),
        attacking_evidence=evidence,

        reasons=reasons[:14],
        blockers=blockers,
        debug=debug,
    )


# ============================================================
# MULTI MATCH
# ============================================================

def rank_first_half_candidates(
    contexts: List[FirstHalfContext],
    cfg: FirstHalfConfig = DEFAULT_CONFIG,
) -> List[Dict[str, Any]]:

    items = [analyze_first_half(x, cfg).to_dict() for x in contexts]

    visible = [
        x for x in items
        if x["decision"] != "PASS"
        and x["probability_goal_before_ht"] >= cfg.watch_min
    ]

    rank = {
        "ENTER_FIRST_HALF": 4,
        "FIRST_HALF_EMERGING": 3,
        "FIRST_HALF_WATCH": 2,
        "WATCH_BLOCKED": 1,
    }

    visible.sort(
        key=lambda x: (
            rank.get(x["decision"], 0),
            x["probability_goal_before_ht"],
            x["pressure_gap"],
            x["attack_chain_score"],
        ),
        reverse=True,
    )

    return visible


# ============================================================
# GENERIC HIDDEN SIGNAL ADAPTER
# ============================================================

def _read_stats(d: Dict[str, Any]) -> TeamHalfStats:
    return TeamHalfStats(
        shots=d.get("shots", 0),
        shots_on_target=d.get("shots_on_target", d.get("sot", 0)),
        xg=d.get("xg", 0.0),
        box_touches=d.get("box_touches", d.get("box", 0)),
        dangerous_attacks=d.get("dangerous_attacks", d.get("dangerous", 0)),
        corners=d.get("corners", 0),
        big_chances=d.get("big_chances", d.get("big", 0)),
        possession=d.get("possession", 50.0),

        shots_5=d.get("shots_5", d.get("recent_shots", 0)),
        sot_5=d.get("sot_5", d.get("recent_sot", 0)),
        xg_5=d.get("xg_5", d.get("recent_xg", 0.0)),
        box_5=d.get("box_5", d.get("recent_box_touches", 0)),
        dangerous_5=d.get("dangerous_5", d.get("recent_dangerous_attacks", 0)),
        corners_5=d.get("corners_5", d.get("recent_corners", 0)),

        shots_10=d.get("shots_10", 0),
        sot_10=d.get("sot_10", 0),
        xg_10=d.get("xg_10", 0.0),
        box_10=d.get("box_10", 0),
        dangerous_10=d.get("dangerous_10", 0),
        corners_10=d.get("corners_10", 0),

        red_cards=d.get("red_cards", 0),
    )


def build_context_from_report(report: Dict[str, Any]) -> FirstHalfContext:
    hs = report.get("home_stats") or {}
    as_ = report.get("away_stats") or {}

    score = report.get("score")
    if isinstance(score, (list, tuple)) and len(score) >= 2:
        sh, sa = score[0], score[1]
    else:
        sh = report.get("score_home", 0)
        sa = report.get("score_away", 0)

    return FirstHalfContext(
        home=str(report.get("home", "HOME")),
        away=str(report.get("away", "AWAY")),
        minute=_nint(report.get("minute", 0)),
        score_home=_nint(sh),
        score_away=_nint(sa),
        home_stats=_read_stats(hs),
        away_stats=_read_stats(as_),
        trend_score=float(report.get("trend_score", 0.0) or 0.0),
        data_quality=float(report.get("data_quality", 100.0) or 100.0),
        freshness_seconds=float(report.get("freshness_seconds", 0.0) or 0.0),
        freshness_confirmed=bool(report.get("freshness_confirmed", True)),
        post_goal_cooldown=bool(report.get("post_goal_cooldown", False)),
        var_active=bool(report.get("var_active", False)),
        match_suspended=bool(report.get("match_suspended", False)),
        pre_match_goal_expectation=report.get("pre_match_goal_expectation"),
        league_goal_factor=report.get("league_goal_factor"),
    )


def enrich_scan_final_live_report(
    report: Dict[str, Any],
    cfg: FirstHalfConfig = DEFAULT_CONFIG,
) -> Dict[str, Any]:
    """
    Full integration adapter.

    Use on FIRST_HALF deep reports.
    """
    ctx = build_context_from_report(report)
    result = analyze_first_half(ctx, cfg).to_dict()

    out = dict(report)
    out["FIRST_HALF_ENGINE_V3_1"] = result

    out["first_half_probability"] = result["probability_goal_before_ht"]
    out["first_half_probability_next_5"] = result["probability_goal_next_5"]
    out["first_half_probability_next_10"] = result["probability_goal_next_10"]

    out["first_half_home_goal_probability"] = result["probability_home_goal_before_ht"]
    out["first_half_away_goal_probability"] = result["probability_away_goal_before_ht"]

    out["first_half_pressure_team"] = result["pressure_team"]
    out["first_half_pressure_home"] = result["pressure_home"]
    out["first_half_pressure_away"] = result["pressure_away"]
    out["first_half_pressure_last_5_home"] = result["pressure_last_5_home"]
    out["first_half_pressure_last_5_away"] = result["pressure_last_5_away"]
    out["first_half_pressure_acceleration_home"] = result["pressure_acceleration_home"]
    out["first_half_pressure_acceleration_away"] = result["pressure_acceleration_away"]

    out["first_half_attack_chain"] = result["attack_chain_active"]
    out["first_half_attack_chain_team"] = result["attack_chain_team"]
    out["first_half_attack_chain_score"] = result["attack_chain_score"]

    out["first_half_likely_scorer"] = result["likely_scorer_side"]
    out["first_half_goal_window"] = result["goal_window"]

    out["first_half_decision"] = result["decision"]
    out["first_half_timing"] = result["timing"]
    out["first_half_dominance_override"] = result["dominance_override"]
    out["first_half_false_pressure"] = result["false_pressure"]

    return out


# ============================================================
# SELF TESTS
# ============================================================

def self_test() -> None:

    # 1) Static dominance with weak trend
    case1 = FirstHalfContext(
        home="Calcutta Customs",
        away="East Bengal",
        minute=28,
        score_home=0,
        score_away=1,
        home_stats=TeamHalfStats(
            shots=2,
            shots_on_target=0,
            xg=0.10,
            box_touches=4,
            dangerous_attacks=12,
            corners=1,
            possession=43,
            shots_5=0,
            sot_5=0,
            xg_5=0.01,
            box_5=1,
            dangerous_5=2,
            shots_10=1,
            sot_10=0,
            xg_10=0.05,
            box_10=2,
            dangerous_10=4,
        ),
        away_stats=TeamHalfStats(
            shots=6,
            shots_on_target=3,
            xg=0.75,
            box_touches=11,
            dangerous_attacks=27,
            corners=3,
            big_chances=2,
            possession=57,
            shots_5=3,
            sot_5=1,
            xg_5=0.28,
            box_5=5,
            dangerous_5=9,
            corners_5=1,
            shots_10=5,
            sot_10=2,
            xg_10=0.52,
            box_10=9,
            dangerous_10=17,
            corners_10=2,
        ),
        trend_score=3.6,
        data_quality=85,
        freshness_seconds=8,
        freshness_confirmed=True,
    )

    r1 = analyze_first_half(case1)
    assert r1.pressure_team == "East Bengal"
    assert r1.dominance_override is True
    assert r1.trend_required is False
    assert r1.attacking_evidence is True
    assert r1.probability_goal_before_ht >= 60
    assert r1.decision in {
        "FIRST_HALF_WATCH",
        "FIRST_HALF_EMERGING",
        "ENTER_FIRST_HALF",
    }

    # 2) Empty match must never get signal just from remaining time
    case2 = FirstHalfContext(
        home="A",
        away="B",
        minute=10,
        score_home=0,
        score_away=0,
        home_stats=TeamHalfStats(),
        away_stats=TeamHalfStats(),
        data_quality=90,
        freshness_seconds=5,
        freshness_confirmed=True,
    )

    r2 = analyze_first_half(case2)
    assert r2.attacking_evidence is False
    assert r2.decision == "PASS"

    # 3) Strong stale data must not ENTER
    case3 = FirstHalfContext(
        home="A",
        away="B",
        minute=30,
        score_home=0,
        score_away=0,
        home_stats=TeamHalfStats(
            shots=10,
            shots_on_target=5,
            xg=1.3,
            box_touches=18,
            big_chances=3,
            shots_5=4,
            sot_5=2,
            xg_5=0.45,
            box_5=7,
            shots_10=7,
            sot_10=4,
            xg_10=0.90,
            box_10=13,
        ),
        away_stats=TeamHalfStats(
            shots=2,
            shots_on_target=0,
            xg=0.1,
        ),
        trend_score=15,
        data_quality=90,
        freshness_seconds=90,
        freshness_confirmed=True,
    )

    r3 = analyze_first_half(case3)
    assert r3.decision != "ENTER_FIRST_HALF"

    # 4) False possession pressure should not dominate
    case4 = FirstHalfContext(
        home="Possession FC",
        away="Counter FC",
        minute=27,
        score_home=0,
        score_away=0,
        home_stats=TeamHalfStats(
            possession=68,
            shots=2,
            shots_on_target=0,
            xg=0.12,
            box_touches=3,
        ),
        away_stats=TeamHalfStats(
            possession=32,
            shots=4,
            shots_on_target=2,
            xg=0.48,
            box_touches=7,
        ),
        data_quality=80,
        freshness_seconds=5,
        freshness_confirmed=True,
    )

    r4 = analyze_first_half(case4)
    assert r4.pressure_team == "Counter FC"

    print("FIRST_HALF_ENGINE_V3.1 COMPLETE self-test: OK")
    print("Example:")
    print(r1.to_dict())


if __name__ == "__main__":
    self_test()
