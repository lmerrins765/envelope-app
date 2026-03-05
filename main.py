"""
Envelope Analyser – FastAPI backend.

Serves a single-page HTML frontend and provides an API to:
  POST /api/analyse   – fetch a Sporting Life race card URL and score runners
  GET  /api/sample    – return a scored sample race for demo / testing
"""
from __future__ import annotations

import base64
import json
import math
import re
from collections import Counter
from pathlib import Path
from typing import Optional

import anthropic

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from typing import Annotated

from fastapi import Form, UploadFile, File

from models import AnalyseRequest, ScoredRaceCard, ScoredRunner
import scraper
from scorer import score_runners
from course_stats import (
    CourseStatsPack,
    parse_name_stat_file,
    parse_trends_file,
    parse_travellers_file,
    parse_favourites_file,
    parse_going_report,
    parse_hot_trainers,
)

# ---------------------------------------------------------------------------
# In-memory course stats store (per-process; cleared on restart)
# ---------------------------------------------------------------------------
_cs = CourseStatsPack()

# Market prices uploaded just before race off (name_lc → (fractional, decimal))
_market_prices: dict[str, tuple[str, float]] = {}

# ---------------------------------------------------------------------------
# Pre-meeting image context store (session-only; cleared on restart)
# ---------------------------------------------------------------------------
_context_store: dict[str, object] = {}

EXTRACTION_PROMPTS: dict[str, str] = {
    "ten_year_trends": (
        "Extract the ten year trend data from this racing stats table. "
        "Return JSON with keys: race_type, trend_patterns (array), key_filters (array)."
    ),
    "top_trainers": (
        "Extract trainer name, win %, P&L, and course record from this table. "
        "Return as a JSON array of objects with keys: name, win_pct, pl, course_record."
    ),
    "top_jockeys": (
        "Extract jockey name, win %, course record, and P&L from this table. "
        "Return as a JSON array of objects with keys: name, win_pct, course_record, pl."
    ),
    "going_report": (
        "Extract the going description and any ground reports from this image. "
        "Return as JSON with keys: going, going_stick (if present), notes (array)."
    ),
    "travellers_check": (
        "Extract each trainer and horse travelling to this meeting, including distance "
        "travelled and any notable flags. "
        "Return as a JSON array of objects with keys: trainer, horse, distance_miles, flags (array)."
    ),
}


def _make_context_summary(label: str, data: object) -> str:
    """Return a short human-readable summary of extracted context data."""
    try:
        if label == "top_trainers" and isinstance(data, list):
            names = [e.get("name", "?") for e in data[:3] if isinstance(e, dict)]
            return f"{len(data)} trainers — {', '.join(names)}{' …' if len(data) > 3 else ''}"
        if label == "top_jockeys" and isinstance(data, list):
            names = [e.get("name", "?") for e in data[:3] if isinstance(e, dict)]
            return f"{len(data)} jockeys — {', '.join(names)}{' …' if len(data) > 3 else ''}"
        if label == "travellers_check" and isinstance(data, list):
            horses = [e.get("horse", "?") for e in data[:3] if isinstance(e, dict)]
            return f"{len(data)} travellers — {', '.join(horses)}{' …' if len(data) > 3 else ''}"
        if label == "going_report" and isinstance(data, dict):
            going = data.get("going") or data.get("description") or ""
            stick = data.get("going_stick")
            suffix = f" (GoingStick {stick})" if stick else ""
            return f"{going}{suffix}" if going else "Going report extracted"
        if label == "ten_year_trends" and isinstance(data, dict):
            rt = data.get("race_type") or ""
            patterns = data.get("trend_patterns", [])
            p_count = f" · {len(patterns)} patterns" if patterns else ""
            return f"{rt}{p_count}".strip() or "Trend data extracted"
    except Exception:
        pass
    return "Data extracted successfully"


def _parse_market_prices(text: str) -> dict[str, tuple[str, float]]:
    """
    Parse a market prices file into {lower_horse_name: (fractional_str, decimal)}.
    Handles lines like:
      "Constitution Hill  5/4"
      "El Fabiolo, 3/1"
      "Constitution Hill 2.25"
    """
    prices: dict[str, tuple[str, float]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Split on last whitespace-bounded price-like token
        m = re.search(
            r'^(.+?)\s{1,}([\d]+\/[\d]+|EVS|EVENS|[\d]+\.[\d]+)\s*$',
            line, re.I,
        )
        if not m:
            # Try comma-separated
            parts = line.rsplit(',', 1)
            if len(parts) == 2:
                name_raw, price_raw = parts[0].strip(), parts[1].strip()
            else:
                continue
        else:
            name_raw, price_raw = m.group(1).strip(), m.group(2).strip()

        # Parse decimal
        frac = price_raw.upper()
        if '/' in frac:
            try:
                num, den = frac.split('/')
                dec = int(num) / int(den) + 1.0
            except (ValueError, ZeroDivisionError):
                continue
        elif frac in ('EVS', 'EVENS'):
            dec = 2.0
        else:
            try:
                dec = float(frac)
            except ValueError:
                continue

        prices[name_raw.lower()] = (price_raw, round(dec, 2))
    return prices

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Envelope Analyser", version="1.0")

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# Trainer flagging
# ---------------------------------------------------------------------------
FLAGGED_TRAINER_KEYWORDS = [
    "mullins", "elliott", "cromwell", "skelton", "greenall", "pauling",
]


def is_flagged_trainer(name: str | None) -> bool:
    if not name:
        return False
    lower = name.lower()
    return any(k in lower for k in FLAGGED_TRAINER_KEYWORDS)


# ---------------------------------------------------------------------------
# Trainer multi-runner detection
# ---------------------------------------------------------------------------

def detect_multi_trainer_runners(scored: ScoredRaceCard) -> set[str]:
    """
    Return a set of lower-cased trainer names that have 2+ runners in this race.
    All runners from such trainers are flagged in the output.
    """
    names = [
        (sr.runner.trainer or "").strip().lower()
        for sr in scored.runners
        if not sr.runner.non_runner and sr.runner.trainer
    ]
    return {t for t, c in Counter(names).items() if c >= 2 and t}


# ---------------------------------------------------------------------------
# Seconditis detection
# ---------------------------------------------------------------------------
_FORM_RE = re.compile(r'[\/\-\. ]')


def _parse_positions(form: Optional[str]) -> list[Optional[int]]:
    if not form:
        return []
    clean = _FORM_RE.sub("", form.strip().upper())
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


def detect_course_winner(runner, venue: str) -> tuple[bool, str]:
    """Flag horses with at least one win at today's venue."""
    if runner.course_wins and runner.course_wins > 0:
        msg = (
            f"⭐ Course winner: {runner.course_wins}W from {runner.course_runs}R at {venue}"
        )
        return True, msg
    return False, ""


def detect_seconditis(form: Optional[str]) -> tuple[bool, str]:
    """
    Flag horses with a pattern of finishing 2nd without winning.
    Thresholds:
      - Strong: 3+ seconds, 0 wins in last 6 runs
      - Mild:   2+ seconds, 0 wins in last 4+ completed runs
    """
    chars = _parse_positions(form)
    if len(chars) < 3:
        return False, ""
    seconds = sum(1 for p in chars if p == 2)
    wins    = sum(1 for p in chars if p == 1)
    runs    = len(chars)
    if seconds >= 3 and wins == 0:
        return True, f"⚠ Seconditis: {seconds} runner-up finishes, 0 wins in last {runs} runs"
    if seconds >= 2 and wins == 0 and runs >= 4:
        return True, f"⚠ Seconditis warning: {seconds} runner-up finishes, 0 wins in last {runs} runs"
    return False, ""


# ---------------------------------------------------------------------------
# Selections: winner + each-way value
# Market data is shown for context only — it never influences the pick.
# ---------------------------------------------------------------------------

def _value_metric(sr: ScoredRunner, winner_name: str, all_runners: list[ScoredRunner]) -> float | None:
    """
    Each-way value = analytical score significantly above what the market prices in.

    We compute:
      implied_prob  = score / total_scores            (model probability)
      market_prob   = 1 / decimal_odds                (market probability)
      value_edge    = implied_prob - market_prob       (positive = underrated by market)

    A high score + long odds = best each-way value.
    The market never drives selection; odds only reveal mispricing of our model's findings.
    """
    if sr.runner.name == winner_name:
        return None  # winner already taken
    if not sr.runner.odds_decimal or sr.runner.odds_decimal < 3.0:
        return None  # minimum 2/1 for each-way consideration

    total = sum(r.total_score for r in all_runners) or 1.0
    implied = sr.total_score / total
    market  = 1.0 / sr.runner.odds_decimal
    edge    = implied - market
    return edge


def _market_alignment(winner: ScoredRunner, runners: list[ScoredRunner]) -> dict:
    """
    Compare analytical winner to market favourite.
    Market data is used ONLY to flag disagreements or justify a switch —
    it never independently drives the selection.
    """
    priced = [r for r in runners if r.runner.odds_decimal]
    if not priced:
        return {"status": "no_prices", "note": "No market prices loaded"}

    mkt_fav = min(priced, key=lambda r: r.runner.odds_decimal)
    win_dec  = winner.runner.odds_decimal or 999.0
    fav_dec  = mkt_fav.runner.odds_decimal or 999.0

    if mkt_fav.runner.name == winner.runner.name:
        return {
            "status": "aligned",
            "note":   f"Market agrees — {winner.runner.name} is favourite at {winner.runner.odds}",
            "market_alt": None,
        }

    # How close is the market fav's analytical score to our winner?
    score_gap_pct = (winner.total_score - mkt_fav.total_score) / max(winner.total_score, 0.01)
    divergence    = fav_dec - win_dec   # positive: our pick is longer odds than fav

    # Flag levels
    if divergence > 15 and score_gap_pct < 0.12:
        # Our winner is much longer price than fav AND scores are close → suggest considering fav
        status = "switch_possible"
        note   = (
            f"⚠ Market strongly favours {mkt_fav.runner.name} ({mkt_fav.runner.odds}). "
            f"Score gap is narrow ({score_gap_pct:.0%}) — market alternative highlighted."
        )
        return {
            "status":     status,
            "note":       note,
            "market_alt": {
                "name":  mkt_fav.runner.name,
                "score": mkt_fav.total_score,
                "odds":  mkt_fav.runner.odds or "—",
                "rank":  mkt_fav.rank,
            },
        }

    if divergence > 8:
        return {
            "status": "divergent",
            "note":   (
                f"Market diverges — model picks {winner.runner.name} ({winner.runner.odds or '—'}), "
                f"market favours {mkt_fav.runner.name} ({mkt_fav.runner.odds}). "
                f"Model is confident (score gap {score_gap_pct:.0%})."
            ),
            "market_alt": None,
        }

    return {
        "status": "mild_divergence",
        "note":   (
            f"Mild divergence — market fav is {mkt_fav.runner.name} ({mkt_fav.runner.odds}), "
            f"model rates our pick {score_gap_pct:.0%} higher."
        ),
        "market_alt": None,
    }


def get_selections(scored: ScoredRaceCard) -> dict:
    runners = scored.runners
    if not runners:
        return {"winner": None, "each_way": None}

    # Winner: purely highest analytical score (market irrelevant)
    winner = runners[0]

    # Each-way value: highest value_edge among non-winner runners with odds ≥ 3.0 decimal
    candidates = []
    for sr in runners:
        vm = _value_metric(sr, winner.runner.name, runners)
        if vm is not None:
            candidates.append((vm, sr))

    each_way: ScoredRunner | None = None
    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        each_way = candidates[0][1]
    elif len(runners) > 1:
        each_way = runners[1]

    def _fmt(sr: ScoredRunner) -> dict:
        return {
            "name":            sr.runner.name,
            "score":           sr.total_score,
            "rank":            sr.rank,
            "odds":            sr.runner.odds or "—",
            "odds_decimal":    sr.runner.odds_decimal,
            "trainer":         sr.runner.trainer or "—",
            "jockey":          sr.runner.jockey or "—",
            "flagged_trainer": is_flagged_trainer(sr.runner.trainer),
            "hot_trainer":     _cs.is_hot(sr.runner.trainer),
            "number":          sr.runner.number,
        }

    return {
        "winner":    _fmt(winner),
        "each_way":  _fmt(each_way) if each_way else None,
        "alignment": _market_alignment(winner, runners),
    }


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

def detect_trip_change(runner, today_dist: Optional[float]) -> tuple[str, str]:
    """
    Compare today's distance to the horse's most recent trip.
    Returns (direction, label) where direction is 'up', 'down', or ''.
    """
    if today_dist is None:
        return "", ""
    last_trip: Optional[float] = None
    if runner.or_trip_history:
        last_trip = runner.or_trip_history[-1]
    elif runner.distance_record:
        try:
            last_trip = float(max(runner.distance_record.keys(), key=float))
        except (ValueError, TypeError):
            pass
    if last_trip is None:
        return "", ""
    diff = today_dist - last_trip
    if diff >= 2:
        return "up",   f"⬆ Stepped up {diff:.0f}f (was {last_trip:.0f}f, now {today_dist:.0f}f)"
    if diff <= -2:
        return "down", f"⬇ Dropped back {abs(diff):.0f}f (was {last_trip:.0f}f, now {today_dist:.0f}f)"
    return "", ""


def _serialise(scored: ScoredRaceCard) -> dict:
    """Convert scored race card to a JSON-serialisable dict with all flags."""
    multi_trainer_names = detect_multi_trainer_runners(scored)
    today_dist = scored.race.distance_furlongs

    runners_out = []
    for sr in scored.runners:
        r_dict = sr.runner.model_dump()

        # ── Trainer flags ──────────────────────────────────────────────
        r_dict["flagged_trainer"] = is_flagged_trainer(sr.runner.trainer)
        r_dict["multi_runner_trainer"] = (
            (sr.runner.trainer or "").strip().lower() in multi_trainer_names
        )

        # ── Course winner ──────────────────────────────────────────────
        cw_flag, cw_msg = detect_course_winner(sr.runner, scored.race.venue)
        r_dict["course_winner"]     = cw_flag
        r_dict["course_winner_msg"] = cw_msg

        # ── Seconditis ─────────────────────────────────────────────────
        sec_flag, sec_msg = detect_seconditis(sr.runner.form)
        r_dict["seconditis"] = sec_flag
        r_dict["seconditis_msg"] = sec_msg

        # ── Trip change ────────────────────────────────────────────────
        trip_dir, trip_msg = detect_trip_change(sr.runner, today_dist)
        r_dict["trip_change"]     = trip_dir    # "up" | "down" | ""
        r_dict["trip_change_msg"] = trip_msg

        # ── Merge live market prices if uploaded ───────────────────────
        name_key = sr.runner.name.lower()
        if name_key in _market_prices:
            frac, dec = _market_prices[name_key]
            r_dict["odds"]         = frac
            r_dict["odds_decimal"] = dec

        # ── Hot trainer flag ───────────────────────────────────────────
        r_dict["hot_trainer"] = _cs.is_hot(sr.runner.trainer)

        runners_out.append({
            "runner":      r_dict,
            "factors":     [f.model_dump() for f in sr.factors],
            "total_score": sr.total_score,
            "rank":        sr.rank,
        })
    return {
        "race":           scored.race.model_dump(),
        "runners":        runners_out,
        "factor_weights": scored.factor_weights,
        "selections":     get_selections(scored),
        "course_stats":   _cs.summary(),
    }


# ---------------------------------------------------------------------------
# Upload endpoints
# ---------------------------------------------------------------------------

@app.post("/api/upload-stats")
async def upload_stats(
    type: Annotated[str, Form()],
    file: Annotated[UploadFile, File()],
) -> dict:
    """
    Accept one of 6 file types:
      trainers | jockeys | trends | travellers | favourites | prices
    """
    global _market_prices
    raw = await file.read()
    text = raw.decode("utf-8", errors="replace")

    if type == "trainers":
        _cs.trainers = parse_name_stat_file(text)
        _cs.build_lookups()
        return {"type": type, "loaded": len(_cs.trainers), "status": "ok"}

    if type == "jockeys":
        _cs.jockeys = parse_name_stat_file(text)
        _cs.build_lookups()
        return {"type": type, "loaded": len(_cs.jockeys), "status": "ok"}

    if type == "trends":
        _cs.trends = parse_trends_file(text)
        return {"type": type, "loaded": len(_cs.trends), "status": "ok"}

    if type == "travellers":
        _cs.travellers = parse_travellers_file(text)
        return {"type": type, "loaded": len(_cs.travellers), "status": "ok"}

    if type == "favourites":
        win, place = parse_favourites_file(text)
        _cs.fav_win_rate   = win
        _cs.fav_place_rate = place
        return {"type": type, "win_rate": win, "place_rate": place, "status": "ok"}

    if type == "prices":
        _market_prices = _parse_market_prices(text)
        return {"type": type, "loaded": len(_market_prices), "status": "ok"}

    raise HTTPException(status_code=400, detail=f"Unknown stats type: {type}")


@app.post("/upload-context")
async def upload_context(
    label: Annotated[str, Form()],
    file: Annotated[UploadFile, File()],
) -> dict:
    """
    Receive an image, send it to Claude with a label-specific extraction prompt,
    parse the returned JSON, and store it in _context_store[label].
    """
    if label not in EXTRACTION_PROMPTS:
        raise HTTPException(status_code=400, detail=f"Unknown label: {label!r}")

    raw = await file.read()
    b64 = base64.standard_b64encode(raw).decode()

    # Detect media type from content-type header or filename extension
    ct = (file.content_type or "").lower()
    fname = (file.filename or "").lower()
    if "png" in ct or fname.endswith(".png"):
        media_type = "image/png"
    elif "webp" in ct or fname.endswith(".webp"):
        media_type = "image/webp"
    else:
        media_type = "image/jpeg"

    prompt_text = (
        EXTRACTION_PROMPTS[label]
        + "\n\nIMPORTANT: Return ONLY valid JSON — no markdown fences, no prose."
    )

    try:
        ac = anthropic.AsyncAnthropic()
        message = await ac.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": prompt_text},
                ],
            }],
        )
    except anthropic.AuthenticationError:
        raise HTTPException(status_code=500, detail="Anthropic API key missing or invalid. Set ANTHROPIC_API_KEY.")
    except anthropic.APIStatusError as e:
        raise HTTPException(status_code=502, detail=f"Anthropic API error {e.status_code}: {e.message}")

    raw_text = message.content[0].text.strip()

    # Strip markdown code fences if the model added them
    if raw_text.startswith("```"):
        raw_text = re.sub(r"^```\w*\n?", "", raw_text)
        raw_text = re.sub(r"\n?```$", "", raw_text).strip()

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        # Preserve raw text so the user can still inspect what came back
        parsed = {"raw_text": raw_text}

    _context_store[label] = parsed
    return {
        "label":   label,
        "data":    parsed,
        "summary": _make_context_summary(label, parsed),
        "status":  "ok",
    }


@app.post("/api/set-going")
async def set_going(body: dict) -> dict:
    raw, bucket = parse_going_report(body.get("going", ""))
    _cs.going_report   = raw
    _cs.going_override = bucket
    return {"going_report": raw, "going_override": bucket, "status": "ok"}


@app.post("/api/set-hot-trainers")
async def set_hot_trainers(body: dict) -> dict:
    _cs.hot_trainers = parse_hot_trainers(body.get("names", ""))
    _cs.build_lookups()
    return {"hot_trainers": _cs.hot_trainers, "status": "ok"}


@app.get("/api/course-stats")
async def get_course_stats() -> dict:
    return {**_cs.summary(), "market_prices": len(_market_prices)}


@app.delete("/api/course-stats")
async def clear_course_stats() -> dict:
    global _market_prices
    _market_prices = {}
    _context_store.clear()
    for field in ("trainers", "jockeys", "trends", "travellers",
                  "fav_win_rate", "fav_place_rate",
                  "going_report", "going_override", "hot_trainers"):
        _cs.clear_field(field)
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# Analyse routes
# ---------------------------------------------------------------------------

@app.post("/api/analyse")
async def analyse(request: AnalyseRequest) -> dict:
    try:
        race   = await scraper.fetch_racecard(str(request.url))
        scored = score_runners(race, _cs if _cs.has_any() else None)
        return _serialise(scored)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@app.get("/api/sample")
async def sample() -> dict:
    race   = scraper.get_sample_racecard()
    scored = score_runners(race, _cs if _cs.has_any() else None)
    return _serialise(scored)
