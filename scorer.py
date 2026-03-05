"""
12-Factor scoring engine for horse racing runners.

Factors (scored 0–10, weighted sum = total score):
  1.  or_trajectory      OR Trajectory       – is the horse's rating rising or falling?
  2.  or_trip_match      OR Trip Match       – was the rating earned at today's trip?
  3.  trainer_rating     Trainer Rating      – trainer win % / reputation tier
  4.  jockey_rating      Jockey Rating       – jockey win % / reputation tier
  5.  combo_rating       T/J Combo           – trainer-jockey partnership strength
  6.  travellers_check   Traveller's Check   – does the horse run well away from home?
  7.  trends             Race Trends         – historical race/class type trends
  8.  going_suitability  Going Suitability   – record on today's going conditions
  9.  distance_suitability Distance Suitability – record at today's distance
 10.  days_since_run     Days Since Last Run – fitness freshness window
 11.  breeding_flag      Breeding Flag       – pedigree pointers for trip / going
 12.  running_style      Running Style       – pace profile vs field dynamics
"""
from __future__ import annotations

import math
import re
from typing import Optional, TYPE_CHECKING

from models import Runner, RaceCard, FactorScore, ScoredRunner, ScoredRaceCard

if TYPE_CHECKING:
    from course_stats import CourseStatsPack

# ---------------------------------------------------------------------------
# Factor configuration
# ---------------------------------------------------------------------------
FACTOR_CONFIG: dict[str, dict] = {
    "or_trajectory":       {"name": "OR Trajectory",        "weight": 0.10, "desc": "Trend in Official Ratings over recent runs"},
    "or_trip_match":       {"name": "OR Trip Match",        "weight": 0.08, "desc": "Was the OR earned at today's trip?"},
    "trainer_rating":      {"name": "Trainer Rating",       "weight": 0.09, "desc": "Trainer win % and current form"},
    "jockey_rating":       {"name": "Jockey Rating",        "weight": 0.08, "desc": "Jockey win % and current form"},
    "combo_rating":        {"name": "T/J Combo",            "weight": 0.07, "desc": "Trainer–jockey partnership strength"},
    "travellers_check":    {"name": "Traveller's Check",    "weight": 0.07, "desc": "Performance travelling away from home base"},
    "trends":              {"name": "Race Trends",          "weight": 0.08, "desc": "Historical trends for race class/type/distance"},
    "going_suitability":   {"name": "Going Suitability",    "weight": 0.12, "desc": "Record on today's going conditions"},
    "distance_suitability":{"name": "Distance Suitability", "weight": 0.12, "desc": "Record at today's distance"},
    "days_since_run":      {"name": "Days Since Last Run",  "weight": 0.07, "desc": "Fitness and optimal freshness window"},
    "breeding_flag":       {"name": "Breeding Flag",        "weight": 0.06, "desc": "Pedigree pointers for trip and going"},
    "running_style":       {"name": "Running Style",        "weight": 0.06, "desc": "Pace profile vs field dynamics"},
}
assert abs(sum(v["weight"] for v in FACTOR_CONFIG.values()) - 1.0) < 1e-9, "Weights must sum to 1.0"

# ---------------------------------------------------------------------------
# Trainer tiers  (lower-cased partial match)
# ---------------------------------------------------------------------------
_TRAINER_TIER: dict[str, tuple[float, float]] = {  # key → (win_pct, score_0_10)
    # S-tier: 9–10
    "aidan o'brien":        (0.27, 9.5), "willie mullins":      (0.30, 9.5),
    "nicky henderson":      (0.24, 9.0), "paul nicholls":       (0.22, 9.0),
    "gordon elliott":       (0.25, 9.0), "henry de bromhead":   (0.22, 9.0),
    "dan skelton":          (0.21, 8.5), "charlie appleby":     (0.28, 9.0),
    "john gosden":          (0.24, 9.0), "william haggas":      (0.22, 8.5),
    # A-tier: 7–8.5
    "joseph o'brien":       (0.18, 8.0), "gavin cromwell":      (0.16, 7.5),
    "roger varian":         (0.18, 7.5), "andrew balding":      (0.17, 7.5),
    "ralph beckett":        (0.16, 7.5), "mark johnston":       (0.16, 7.0),
    "nigel twiston-davies": (0.15, 7.0), "harry fry":           (0.15, 7.0),
    "venetia williams":     (0.14, 7.0), "kim bailey":          (0.14, 7.0),
    "evan williams":        (0.14, 7.0), "gary moore":          (0.14, 7.0),
    "oliver sherwood":      (0.13, 7.0), "ruth jefferson":      (0.13, 7.0),
    "philip hobbs":         (0.14, 7.0), "colin tizzard":       (0.13, 7.0),
    "jamie snowden":        (0.13, 6.5), "tom george":          (0.13, 6.5),
    "oisin murphy":         (0.19, 7.5), "dermot weld":         (0.18, 7.5),
}

# ---------------------------------------------------------------------------
# Jockey tiers
# ---------------------------------------------------------------------------
_JOCKEY_TIER: dict[str, tuple[float, float]] = {
    # S-tier
    "paul townend":       (0.28, 9.5), "rachael blackmore":  (0.24, 9.0),
    "nico de boinville":  (0.22, 9.0), "harry cobden":       (0.21, 8.5),
    "harry skelton":      (0.22, 9.0), "ryan moore":         (0.24, 9.0),
    "frankie dettori":    (0.22, 9.0), "james doyle":        (0.20, 8.5),
    "william buick":      (0.21, 8.5), "hollie doyle":       (0.18, 8.0),
    "tom marquand":       (0.18, 8.0), "mark walsh":         (0.20, 8.5),
    # A-tier
    "davy russell":       (0.17, 7.5), "tom scudamore":      (0.16, 7.5),
    "paddy brennan":      (0.16, 7.5), "sam twiston-davies": (0.15, 7.0),
    "jack kennedy":       (0.18, 7.5), "bryan cooper":       (0.15, 7.0),
    "aidan coleman":      (0.15, 7.0), "richard johnson":    (0.16, 7.5),
    "sean levey":         (0.17, 7.0), "kieran shoemark":    (0.17, 7.0),
    "david probert":      (0.16, 7.0), "adam kirby":         (0.16, 7.0),
    "silvestre de sousa": (0.17, 7.0), "barry geraghty":     (0.19, 8.0),
    "noel fehily":        (0.17, 7.5),
}

# ---------------------------------------------------------------------------
# Trainer-Jockey combo scores  (trainer_key → {jockey_key → score})
# These reflect historically successful partnerships.
# ---------------------------------------------------------------------------
_COMBO_SCORES: dict[str, dict[str, float]] = {
    "willie mullins":     {"paul townend": 9.5, "rachael blackmore": 9.0, "mark walsh": 8.5},
    "nicky henderson":    {"nico de boinville": 9.5, "barry geraghty": 9.0},
    "paul nicholls":      {"harry cobden": 9.0, "sam twiston-davies": 8.5},
    "gordon elliott":     {"jack kennedy": 9.0, "davy russell": 8.5},
    "dan skelton":        {"harry skelton": 9.5},
    "charlie appleby":    {"william buick": 9.5, "james doyle": 8.5},
    "john gosden":        {"frankie dettori": 9.0, "james doyle": 8.5, "hollie doyle": 8.0},
    "william haggas":     {"tom marquand": 8.5, "silvestre de sousa": 7.5},
    "aidan o'brien":      {"ryan moore": 9.5, "frankie dettori": 8.5},
    "henry de bromhead":  {"rachael blackmore": 9.5, "davy russell": 8.0},
}

# ---------------------------------------------------------------------------
# Breeding database  (sire → {going: score, distance_flag: label, score: base})
# distance_flag: "sprint" <7f, "miler" 7-9f, "middle" 10-13f, "stayer" 14f+
# jump_flag: "novice", "2m", "3m", "4m+"
# ---------------------------------------------------------------------------
_SIRE_DB: dict[str, dict] = {
    # Flat
    "galileo":          {"going": {"soft": 8, "heavy": 7, "good": 7, "firm": 5}, "distance": "stayer",  "score": 7.5},
    "frankel":          {"going": {"good": 9, "soft": 7, "firm": 8},             "distance": "middle",  "score": 8.0},
    "sea the stars":    {"going": {"good": 9, "soft": 8, "heavy": 7, "firm": 7}, "distance": "stayer",  "score": 7.5},
    "dubawi":           {"going": {"good": 9, "firm": 8, "soft": 6},             "distance": "middle",  "score": 8.0},
    "kingman":          {"going": {"good": 9, "firm": 8, "soft": 6},             "distance": "miler",   "score": 7.5},
    "camelot":          {"going": {"soft": 9, "good": 7, "heavy": 8},            "distance": "stayer",  "score": 7.0},
    "new approach":     {"going": {"good": 8, "soft": 7, "firm": 7},             "distance": "middle",  "score": 7.0},
    "invincible spirit":{"going": {"good": 9, "firm": 8, "soft": 5},             "distance": "sprint",  "score": 7.0},
    "dark angel":       {"going": {"good": 9, "firm": 8, "soft": 6},             "distance": "sprint",  "score": 7.0},
    "no nay never":     {"going": {"good": 9, "firm": 8, "soft": 6},             "distance": "sprint",  "score": 6.5},
    "showcasing":       {"going": {"good": 9, "firm": 8},                         "distance": "sprint",  "score": 6.5},
    # National Hunt
    "presenting":       {"going": {"soft": 9, "heavy": 9, "good": 6},            "distance": "3m",      "score": 7.5},
    "kayf tara":        {"going": {"good": 9, "soft": 8},                         "distance": "3m",      "score": 7.5},
    "milan":            {"going": {"soft": 9, "heavy": 8, "good": 7},            "distance": "3m",      "score": 7.0},
    "oscar":            {"going": {"soft": 8, "good": 7},                         "distance": "3m",      "score": 7.0},
    "old vic":          {"going": {"soft": 8, "good": 7},                         "distance": "4m+",     "score": 6.5},
    "king's theatre":   {"going": {"soft": 9, "heavy": 8, "good": 6},            "distance": "3m",      "score": 7.0},
    "dr massini":       {"going": {"soft": 9, "heavy": 9},                        "distance": "3m",      "score": 6.5},
    "flemensfirth":     {"going": {"soft": 9, "heavy": 8, "good": 6},            "distance": "3m",      "score": 7.0},
    "beneficial":       {"going": {"soft": 8, "good": 7},                         "distance": "2m",      "score": 6.5},
    "well chosen":      {"going": {"good": 8, "soft": 7},                         "distance": "2m",      "score": 6.0},
}

# ---------------------------------------------------------------------------
# Race-type trend rules
# ---------------------------------------------------------------------------
def _get_trend_score(race: RaceCard, runner: Runner) -> tuple[float, str]:
    """Apply generic trend analysis based on race class and type."""
    score = 5.0
    notes = []

    rt = (race.race_type or "").lower()
    rc = (race.race_class or "").lower()
    dist = race.distance_furlongs or 0

    # Jumps trends
    if any(x in rt for x in ("chase", "hurdle", "bumper")):
        age = runner.age or 0
        if "grade 1" in rc or "grade1" in rc:
            if age < 5:
                score -= 1.5; notes.append("young horse in Grade 1")
            elif 6 <= age <= 9:
                score += 1.5; notes.append("prime age for Grade 1 jumps")
        if "novice" in rt:
            if runner.form:
                wins = runner.form.count("1")
                if wins >= 2:
                    score += 1.5; notes.append("multiple novice wins")
                elif wins == 0:
                    score -= 1.0; notes.append("no novice wins")
        if dist >= 24 and age and age < 6:
            score -= 1.0; notes.append("under 6yo in 3m+ race")

    # Flat trends
    else:
        age = runner.age or 0
        if age == 3:
            if dist > 12:
                score += 1.0; notes.append("classic distance 3yo")
            else:
                score += 0.5; notes.append("3yo on flat")
        if "handicap" in rt:
            or_ = runner.official_rating
            if or_ and race.runners:
                ors = [r.official_rating for r in race.runners if r.official_rating]
                if ors and or_ == max(ors):
                    score -= 0.5; notes.append("top weight in handicap")
                elif ors and or_ == min(ors):
                    score += 0.5; notes.append("lowly rated in handicap")

    detail = f"Trends: {'; '.join(notes)}" if notes else "Trends: no specific pattern identified"
    return round(max(0.0, min(10.0, score)), 2), detail


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _norm(value: float, lo: float, hi: float, invert: bool = False) -> float:
    if hi <= lo:
        return 5.0
    n = (value - lo) / (hi - lo) * 10.0
    return max(0.0, min(10.0, 10.0 - n if invert else n))


def _parse_form_chars(form: Optional[str]) -> list[Optional[int]]:
    """Return list of finishing positions (None for non-finishes), most recent last, up to 6."""
    if not form:
        return []
    clean = re.sub(r"[\/\-\. ]", "", form.strip().upper())
    results: list[Optional[int]] = []
    i = 0
    while i < len(clean) and len(results) < 6:
        ch = clean[i]
        if ch.isdigit():
            j = i + 1
            while j < len(clean) and clean[j].isdigit():
                j += 1
            results.append(int(clean[i:j]))
            i = j
        elif ch in "FUPRBCS":
            results.append(None)
            i += 1
        else:
            i += 1
    return results


def _going_bucket(going: Optional[str]) -> str:
    """Normalise going string to canonical bucket."""
    if not going:
        return "unknown"
    g = going.lower()
    if "heavy" in g:          return "heavy"
    if "soft" in g:           return "soft"
    if "yield" in g:          return "soft"    # yielding → soft bucket
    if "good to soft" in g:   return "good_soft"
    if "good to firm" in g:   return "good_firm"
    if "good" in g:           return "good"
    if "firm" in g:           return "firm"
    if "standard" in g:       return "good"    # AW
    if "fast" in g:           return "firm"
    return "unknown"


def _going_compatible(bucket: str, other: str) -> bool:
    """Are two going buckets similar enough to be 'same conditions'?"""
    adjacents = {
        "heavy": {"heavy", "soft"},
        "soft":  {"heavy", "soft", "good_soft"},
        "good_soft": {"soft", "good_soft", "good"},
        "good":  {"good_soft", "good", "good_firm"},
        "good_firm": {"good", "good_firm", "firm"},
        "firm":  {"good_firm", "firm"},
    }
    return other in adjacents.get(bucket, {bucket})


def _lookup_trainer(name: Optional[str]) -> tuple[float, float]:
    """Return (win_pct, tier_score) for a trainer name. (0.10, 5.0) if unknown."""
    if not name:
        return 0.10, 5.0
    key = name.strip().lower()
    for k, v in _TRAINER_TIER.items():
        if k in key or key in k:
            return v
    return 0.10, 5.0


def _lookup_jockey(name: Optional[str]) -> tuple[float, float]:
    if not name:
        return 0.10, 5.0
    key = name.strip().lower()
    for k, v in _JOCKEY_TIER.items():
        if k in key or key in k:
            return v
    return 0.10, 5.0


# ---------------------------------------------------------------------------
# Individual factor scoring functions
# ---------------------------------------------------------------------------

def score_or_trajectory(runner: Runner) -> tuple[float, str]:
    """OR Trajectory – is the rating rising or falling?"""
    history = runner.or_history
    if len(history) < 2:
        # Fall back to form-based proxy
        chars = _parse_form_chars(runner.form)
        nums = [p for p in chars if p is not None]
        if len(nums) < 3:
            return 5.0, "No OR history – trajectory unknown"
        recent = sum(nums[-2:]) / 2
        older  = sum(nums[-4:-2]) / max(len(nums[-4:-2]), 1)
        # Better positions = lower numbers; inverted for score
        delta = older - recent  # positive means improving positions
        score = max(0.0, min(10.0, 5.0 + delta * 0.7))
        direction = "Improving (form proxy)" if delta > 0.3 else "Declining (form proxy)" if delta < -0.3 else "Stable"
        return round(score, 2), f"{direction} – using form positions as OR proxy"

    recent_or = history[-1]
    oldest_or = history[0]
    delta = recent_or - oldest_or     # positive = rating rising
    if delta > 10:
        score, note = 9.0, f"strongly rising (+{delta} pts)"
    elif delta > 4:
        score, note = 7.5, f"rising (+{delta} pts)"
    elif delta >= -2:
        score, note = 5.5, f"stable ({delta:+d} pts)"
    elif delta >= -8:
        score, note = 3.5, f"declining ({delta:+d} pts)"
    else:
        score, note = 2.0, f"sharply declining ({delta:+d} pts)"
    return score, f"OR trajectory: {oldest_or}→{recent_or} – {note}"


def score_or_trip_match(runner: Runner, today_dist_f: Optional[float]) -> tuple[float, str]:
    """OR Trip Match – was the current OR earned at today's trip?"""
    if runner.official_rating is None or today_dist_f is None:
        return 5.0, "Insufficient data – OR trip match unknown"

    if runner.or_trip_history:
        or_trip = runner.or_trip_history[-1]     # trip at which most recent OR was set
        diff = abs(today_dist_f - or_trip)
        if diff <= 1:
            score, note = 9.0, f"OR earned at same trip ({or_trip:.0f}f)"
        elif diff <= 2:
            score, note = 7.0, f"OR earned ≈same trip ({or_trip:.0f}f vs today {today_dist_f:.0f}f)"
        elif diff <= 4:
            score, note = 5.5, f"Moderate trip change ({or_trip:.0f}f → {today_dist_f:.0f}f)"
        else:
            score, note = 3.0, f"Large trip change ({or_trip:.0f}f → {today_dist_f:.0f}f)"
        return round(score, 2), note

    # No trip history – use distance_record as proxy
    if runner.distance_record and today_dist_f:
        key = str(int(today_dist_f))
        rec = runner.distance_record.get(key, [])
        if rec:
            w, r = rec[0], rec[1]
            pct = w / r if r else 0
            score = 4.0 + pct * 6.0
            return round(score, 2), f"Distance record at {key}f: {w}W from {r}R"

    return 5.0, "No trip history available – neutral"


def score_trainer_rating(
    runner: Runner,
    cs: Optional["CourseStatsPack"] = None,
) -> tuple[float, str]:
    # ── Course-specific stats (highest priority) ───────────────────────
    if cs:
        stat = cs.find_trainer(runner.trainer)
        if stat and stat.runs >= 5:
            score = min(10.0, max(0.5, stat.win_pct * 35))  # 30% → 10.5 capped at 10
            detail = (
                f"{runner.trainer}: {stat.wins}W / {stat.runs}R at course "
                f"({stat.win_pct:.0%} win rate)"
            )
            if cs.is_hot(runner.trainer):
                score = min(10.0, score + 1.0)
                detail += " 🔥 Hot trainer"
            return round(score, 2), detail

        # Hot trainer flag even without course stats
        if cs.is_hot(runner.trainer):
            wp, tier = _lookup_trainer(runner.trainer)
            score = min(10.0, tier + 1.5)
            return round(score, 2), f"{runner.trainer} 🔥 Hot trainer — tier boosted"

    wp, tier = _lookup_trainer(runner.trainer)
    detail = f"{runner.trainer or 'Unknown'} — est. win% {wp:.0%}"
    return round(tier, 2), detail


def score_jockey_rating(
    runner: Runner,
    cs: Optional["CourseStatsPack"] = None,
) -> tuple[float, str]:
    # ── Course-specific stats (highest priority) ───────────────────────
    if cs:
        stat = cs.find_jockey(runner.jockey)
        if stat and stat.runs >= 5:
            score = min(10.0, max(0.5, stat.win_pct * 35))
            detail = (
                f"{runner.jockey}: {stat.wins}W / {stat.runs}R at course "
                f"({stat.win_pct:.0%} win rate)"
            )
            return round(score, 2), detail

    wp, tier = _lookup_jockey(runner.jockey)
    detail = f"{runner.jockey or 'Unknown'} — est. win% {wp:.0%}"
    return round(tier, 2), detail


def score_combo_rating(runner: Runner) -> tuple[float, str]:
    t_key = (runner.trainer or "").strip().lower()
    j_key = (runner.jockey  or "").strip().lower()
    if not t_key or not j_key:
        return 5.0, "Trainer or jockey unknown – combo score neutral"

    for tk, jockeys in _COMBO_SCORES.items():
        if tk in t_key or t_key in tk:
            for jk, score in jockeys.items():
                if jk in j_key or j_key in jk:
                    return score, f"Known partnership: {runner.trainer} / {runner.jockey}"
            break  # trainer matched but jockey not in list

    # Both are top-tier but no specific combo record → above average
    _, ts = _lookup_trainer(runner.trainer)
    _, js = _lookup_jockey(runner.jockey)
    combo = round((ts + js) / 2 * 0.9, 2)  # slight discount vs known combo
    return combo, f"No recorded combo data — averaged tier scores"


def score_travellers_check(
    runner: Runner,
    today_venue: str,
    cs: Optional["CourseStatsPack"] = None,
) -> tuple[float, str]:
    """
    Traveller's Check, incorporating course record.
    Priority: explicit course_wins/course_runs > venue_history inference.
    A course winner is strongly rewarded; course form without a win is moderate.
    """
    # ── Explicit course record (best data source) ──────────────────────
    if runner.course_runs > 0:
        w, r = runner.course_wins, runner.course_runs
        pct = w / r
        if w >= 2:
            score = 9.5
            note = f"Course specialist: {w}W from {r}R at {today_venue} ({pct:.0%} win rate)"
        elif w == 1:
            score = 8.5
            note = f"Course winner: {w}W from {r}R at {today_venue}"
        else:
            score = 6.0
            note = f"Runs at {today_venue} without a win ({r}R)"
        return round(score, 2), note

    # ── Travellers data: map trainer origin to course win rate ─────────
    if cs and cs.travellers:
        # Identify runner's likely origin from trainer
        trainer_lc = (runner.trainer or "").lower()
        origin_score: Optional[float] = None
        origin_note = ""
        for tstat in cs.travellers:
            olk = tstat.origin.lower()
            # Simple heuristic: Irish trainers match "ireland" / "irish" entries
            if (("ireland" in olk or "irish" in olk) and
                    any(k in trainer_lc for k in ("mullins", "elliott", "cromwell",
                        "de bromhead", "harrington", "o'brien", "weld", "meade"))):
                origin_score = min(10.0, tstat.win_pct * 40)
                origin_note = f"Irish-trained at {today_venue}: {tstat.win_pct:.0%} win rate"
                break
            if ("local" in olk or "uk" in olk or "british" in olk) and not origin_score:
                origin_score = min(10.0, tstat.win_pct * 40)
                origin_note = f"UK-trained at {today_venue}: {tstat.win_pct:.0%} win rate"
        if origin_score is not None:
            return round(origin_score, 2), origin_note

    # ── Fallback: infer from venue_history ────────────────────────────
    venues = [v.lower().strip() for v in runner.venue_history if v]
    today = today_venue.lower().strip()

    if not venues:
        return 5.0, "No course record or venue history – neutral"

    unique_venues = set(venues)
    runs_at_today = sum(1 for v in venues if today and today in v)

    if runs_at_today >= 2:
        score = 6.5; note = f"Has run {runs_at_today}× at {today_venue} (wins unknown)"
    elif runs_at_today == 1:
        score = 5.5; note = f"One previous run at {today_venue} (result unknown)"
    elif len(unique_venues) >= 3:
        score = 5.5; note = "Widely travelled (multiple venues)"
    else:
        score = 5.0; note = "No course history"

    # Small bonus if won last time out (at any track)
    form = _parse_form_chars(runner.form)
    if form and form[-1] == 1:
        score = min(10.0, score + 0.5); note += " — won last time out"

    return round(score, 2), note


def score_trends(
    race: RaceCard,
    runner: Runner,
    cs: Optional["CourseStatsPack"] = None,
) -> tuple[float, str]:
    base_score, base_detail = _get_trend_score(race, runner)

    if not (cs and cs.trends):
        return base_score, base_detail

    # Try to match each uploaded trend bullet against this runner's profile
    matches, mismatches = [], []
    age = runner.age or 0
    for trend in cs.trends:
        tl = trend.lower()

        # Age match
        age_m = re.search(r'(\d+)\s*[-–to]+\s*(\d+)\s*yo', tl)
        if age_m:
            lo, hi = int(age_m.group(1)), int(age_m.group(2))
            (matches if lo <= age <= hi else mismatches).append(trend[:80])
            continue

        # LTO (last time out) winner match
        if "last time out" in tl or "lto" in tl:
            form_chars = _parse_form_chars(runner.form)
            won_lto = form_chars and form_chars[-1] == 1
            (matches if won_lto else mismatches).append(trend[:80])
            continue

        # Front runners / hold up notes — informational only
        if any(w in tl for w in ("front runner", "hold up", "prominent")):
            style = (runner.running_style or "").lower()
            if any(w in tl for w in ("front runner", "prominent")) and any(
                    w in style for w in ("lead", "prominent")):
                matches.append(trend[:80])
            elif "hold up" in tl and "hold" in style:
                matches.append(trend[:80])

    n_match = len(matches)
    n_miss  = len(mismatches)
    modifier = n_match * 0.5 - n_miss * 0.4
    score = max(0.0, min(10.0, base_score + modifier))

    notes = []
    if matches:
        notes.append(f"Trends match: {'; '.join(matches[:2])}")
    if mismatches:
        notes.append(f"Trends against: {'; '.join(mismatches[:1])}")
    detail = " | ".join(notes) if notes else base_detail
    return round(score, 2), detail


def score_going_suitability(
    runner: Runner,
    today_going: Optional[str],
    going_override: str = "",
) -> tuple[float, str]:
    # If user uploaded a going report, that overrides the scraped going
    effective_going = going_override if going_override else today_going
    today_bucket = _going_bucket(effective_going)
    going_label = effective_going or today_going or "unknown"
    if going_override and going_override != _going_bucket(today_going):
        going_label += " (from going report)"

    # Use explicit going_record if available
    if runner.going_record:
        exact = runner.going_record.get(today_bucket, [])
        if exact and exact[1] > 0:
            w, r = exact[0], exact[1]
            pct = w / r
            score = 3.0 + pct * 7.0
            return round(score, 2), f"Going record ({today_going}): {w}W {r}R ({pct:.0%} win)"

        # Adjacent going
        compatible_wins, compatible_runs = 0, 0
        for g_key, rec in runner.going_record.items():
            if _going_compatible(today_bucket, g_key) and len(rec) >= 2:
                compatible_wins += rec[0]; compatible_runs += rec[1]
        if compatible_runs > 0:
            pct = compatible_wins / compatible_runs
            score = 3.0 + pct * 6.0
            return round(score, 2), f"Similar going record: {compatible_wins}W {compatible_runs}R"

    # Proxy: use breeding sire going preference
    sire_data = _SIRE_DB.get((runner.sire or "").strip().lower(), {})
    if sire_data:
        going_scores = sire_data.get("going", {})
        sire_score = going_scores.get(today_bucket, going_scores.get("good", 5.0))
        return float(sire_score), f"Breeding proxy ({runner.sire}) – going score {sire_score}"

    return 5.0, f"No going record for '{today_going}' – neutral"


def score_distance_suitability(runner: Runner, today_dist_f: Optional[float]) -> tuple[float, str]:
    if today_dist_f is None:
        return 5.0, "Distance unknown"

    # Use explicit distance_record
    if runner.distance_record:
        # Look for matching distance bucket (±1 furlong)
        for dist_key, rec in runner.distance_record.items():
            try:
                d = float(dist_key)
            except ValueError:
                continue
            if abs(d - today_dist_f) <= 1 and rec[1] > 0:
                w, r = rec[0], rec[1]
                pct = w / r
                score = 3.0 + pct * 7.0
                return round(score, 2), f"Distance record ({d:.0f}f): {w}W {r}R ({pct:.0%} win)"

    # Proxy: use sire distance flag
    sire_data = _SIRE_DB.get((runner.sire or "").strip().lower(), {})
    if sire_data:
        dist_flag = sire_data.get("distance", "")
        in_range = False
        if dist_flag == "sprint"  and today_dist_f <= 6:     in_range = True
        elif dist_flag == "miler" and 7 <= today_dist_f <= 9: in_range = True
        elif dist_flag == "middle"and 10 <= today_dist_f <= 13: in_range = True
        elif dist_flag == "stayer"and today_dist_f >= 14:    in_range = True
        elif dist_flag == "2m"    and 14 <= today_dist_f <= 17: in_range = True
        elif dist_flag == "3m"    and 18 <= today_dist_f <= 23: in_range = True
        elif dist_flag == "4m+"   and today_dist_f >= 24:    in_range = True
        score = 8.0 if in_range else 4.5
        return score, f"Breeding proxy ({runner.sire}) – suited to {dist_flag}, today {today_dist_f:.0f}f"

    # Fallback: use form at this distance from or_trip_history
    if runner.or_trip_history:
        trips = runner.or_trip_history
        matches = [t for t in trips if abs(t - today_dist_f) <= 1]
        pct = len(matches) / len(trips)
        score = 4.0 + pct * 5.0
        return round(score, 2), f"{len(matches)} of {len(trips)} recent runs at this trip"

    return 5.0, "No distance record – neutral"


def score_days_since_run(days: Optional[int]) -> tuple[float, str]:
    if days is None:
        return 5.0, "Days since last run unknown"
    if days < 7:
        score, note = 3.5, "very quick turnaround (<7 days)"
    elif days <= 13:
        score, note = 7.0, "short break (7–13 days)"
    elif days <= 30:
        score, note = 9.5, "optimal freshness (14–30 days)"
    elif days <= 60:
        score, note = 7.5, "well rested (31–60 days)"
    elif days <= 90:
        score, note = 5.5, "extended break (61–90 days)"
    elif days <= 180:
        score, note = 3.5, "long absence (91–180 days)"
    else:
        score, note = 2.0, "very long absence (180+ days)"
    return round(score, 2), f"{days} days since last run — {note}"


def score_breeding_flag(runner: Runner, race: RaceCard) -> tuple[float, str]:
    sire_key = (runner.sire or "").strip().lower()
    dam_sire_key = (runner.dam_sire or "").strip().lower()
    today_going = _going_bucket(race.going)
    today_dist = race.distance_furlongs

    sire_data = _SIRE_DB.get(sire_key, {})
    dam_data  = _SIRE_DB.get(dam_sire_key, {})

    if not sire_data and not dam_data:
        return 5.0, f"Sire '{runner.sire or '?'}' not in breeding database – neutral"

    scores = []
    notes = []

    for label, data in [("Sire", sire_data), ("DamSire", dam_data)]:
        if not data:
            continue
        base = data.get("score", 5.0)
        # Going modifier
        g_scores = data.get("going", {})
        g_score = g_scores.get(today_going, g_scores.get("good", base))
        # Distance modifier
        dist_flag = data.get("distance", "")
        in_range = False
        if today_dist:
            if dist_flag == "sprint"  and today_dist <= 6:      in_range = True
            elif dist_flag == "miler" and 7 <= today_dist <= 9: in_range = True
            elif dist_flag == "middle"and 10 <= today_dist <= 13: in_range = True
            elif dist_flag == "stayer"and today_dist >= 14:     in_range = True
            elif dist_flag == "2m"    and 14 <= today_dist <= 17: in_range = True
            elif dist_flag == "3m"    and 18 <= today_dist <= 23: in_range = True
            elif dist_flag == "4m+"   and today_dist >= 24:     in_range = True
        dist_bonus = 1.5 if in_range else -1.0
        combined = max(0.0, min(10.0, (base + g_score) / 2 + dist_bonus))
        scores.append(combined)
        notes.append(f"{label}: {runner.sire if label=='Sire' else runner.dam_sire} ({dist_flag}, going {g_score:.0f}/10)")

    if not scores:
        return 5.0, "Breeding data insufficient"

    final = sum(scores) / len(scores)
    return round(final, 2), " | ".join(notes)


def score_running_style(runner: Runner, field: list[Runner]) -> tuple[float, str]:
    """
    Evaluate running style vs field dynamics.
    Front runners score lower in large fields of pace horses.
    Hold-up runners score lower in small slow-pace fields.
    """
    style = (runner.running_style or "").strip().lower()
    if not style:
        return 5.0, "Running style not recorded – neutral"

    # Classify
    is_front = any(x in style for x in ("lead", "make", "prominent"))
    is_hold  = any(x in style for x in ("hold", "rear", "back"))

    # Count style distribution in field
    front_count = sum(
        1 for r in field
        if r.running_style and any(x in r.running_style.lower() for x in ("lead", "make", "prominent"))
    )
    hold_count = sum(
        1 for r in field
        if r.running_style and any(x in r.running_style.lower() for x in ("hold", "rear", "back"))
    )
    n = max(len(field), 1)

    if is_front:
        if front_count >= 4:
            score, note = 4.0, f"Crowded pace — {front_count} front runners"
        elif front_count <= 1:
            score, note = 8.5, "Lone pace advantage"
        else:
            score, note = 6.5, f"{front_count} pace horses — moderate pace"
    elif is_hold:
        if hold_count / n > 0.5:
            score, note = 4.5, f"Many hold-up horses ({hold_count}) — pace may not materialise"
        else:
            score, note = 6.5, "Hold-up style workable in field"
    else:
        score, note = 6.0, f"Midfield / versatile style"

    return round(score, 2), f"Running style: {runner.running_style} — {note}"


# ---------------------------------------------------------------------------
# Main scoring entry point
# ---------------------------------------------------------------------------

def score_runners(
    race: RaceCard,
    cs: Optional["CourseStatsPack"] = None,
) -> ScoredRaceCard:
    active = [r for r in race.runners if not r.non_runner]
    if not active:
        return ScoredRaceCard(
            race=race,
            runners=[],
            factor_weights={k: v["weight"] for k, v in FACTOR_CONFIG.items()},
        )

    scored: list[ScoredRunner] = []

    for runner in active:
        factors: list[FactorScore] = []

        def add(key: str, result: tuple[float, str]) -> None:
            raw, detail = result
            cfg = FACTOR_CONFIG[key]
            factors.append(FactorScore(
                key=key,
                name=cfg["name"],
                score=raw,
                weight=cfg["weight"],
                weighted=round(raw * cfg["weight"], 4),
                detail=detail,
            ))

        going_override = (cs.going_override if cs else "") or ""

        add("or_trajectory",        score_or_trajectory(runner))
        add("or_trip_match",        score_or_trip_match(runner, race.distance_furlongs))
        add("trainer_rating",       score_trainer_rating(runner, cs))
        add("jockey_rating",        score_jockey_rating(runner, cs))
        add("combo_rating",         score_combo_rating(runner))
        add("travellers_check",     score_travellers_check(runner, race.venue, cs))
        add("trends",               score_trends(race, runner, cs))
        add("going_suitability",    score_going_suitability(runner, race.going, going_override))
        add("distance_suitability", score_distance_suitability(runner, race.distance_furlongs))
        add("days_since_run",       score_days_since_run(runner.days_since_last_run))
        add("breeding_flag",        score_breeding_flag(runner, race))
        add("running_style",        score_running_style(runner, active))

        total = round(sum(f.weighted for f in factors), 3)
        scored.append(ScoredRunner(runner=runner, factors=factors, total_score=total))

    scored.sort(key=lambda x: x.total_score, reverse=True)
    for i, sr in enumerate(scored):
        sr.rank = i + 1

    return ScoredRaceCard(
        race=race,
        runners=scored,
        factor_weights={k: v["weight"] for k, v in FACTOR_CONFIG.items()},
    )
