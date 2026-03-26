"""
Envelope Analyser — FastAPI backend.
"""
from __future__ import annotations

import base64
import logging
import os

import anthropic
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from scraper import fetch_racecard, get_sample_racecard
from course_stats import (
    CourseStatsPack,
    parse_name_stat_file,
    parse_trends_file,
    parse_travellers_file,
    parse_favourites_file,
    parse_going_report,
    parse_hot_trainers,
)

# Import scorer — handle both possible function/class names
try:
    from scorer import score_racecard
except ImportError:
    try:
        from scorer import ScoredRaceCard as _ScoredRaceCard
        def score_racecard(racecard, stats, image_context):
            return _ScoredRaceCard(racecard, stats, image_context)
    except ImportError:
        import scorer as _scorer
        # Last resort: find any callable that takes a racecard
        _fn = getattr(_scorer, [x for x in dir(_scorer) if 'score' in x.lower() or 'race' in x.lower()][0])
        def score_racecard(racecard, stats, image_context):
            return _fn(racecard, stats, image_context)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="Envelope Analyser")
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── In-memory state ───────────────────────────────────────────────────────────
_stats = CourseStatsPack()
_image_context: dict[str, str] = {}

# ── Root ──────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root():
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()

# ── Analyse ───────────────────────────────────────────────────────────────────
class AnalyseRequest(BaseModel):
    url: str

@app.post("/api/analyse")
async def analyse(req: AnalyseRequest):
    try:
        racecard = await fetch_racecard(req.url)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        log.exception("Unexpected error fetching racecard")
        raise HTTPException(status_code=500, detail=str(e))
    result = score_racecard(racecard, _stats, _image_context)
    return result

@app.get("/api/sample")
async def sample():
    racecard = get_sample_racecard()
    result = score_racecard(racecard, _stats, _image_context)
    return result

# ── Stats upload ──────────────────────────────────────────────────────────────
@app.post("/api/upload-stats")
async def upload_stats(
    type: str = Form(...),
    file: UploadFile = File(...),
):
    global _stats
    content = await file.read()
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        raise HTTPException(status_code=400, detail="Could not decode file")

    loaded = 0
    if type == "trainers":
        _stats.trainers = parse_name_stat_file(text)
        loaded = len(_stats.trainers)
    elif type == "jockeys":
        _stats.jockeys = parse_name_stat_file(text)
        loaded = len(_stats.jockeys)
    elif type == "trends":
        _stats.trends = parse_trends_file(text)
        loaded = len(_stats.trends)
    elif type == "travellers":
        _stats.travellers = parse_travellers_file(text)
        loaded = len(_stats.travellers)
    elif type == "favourites":
        win_r, place_r = parse_favourites_file(text)
        _stats.fav_win_rate = win_r
        _stats.fav_place_rate = place_r
        loaded = 1 if (win_r is not None or place_r is not None) else 0
        return {"loaded": loaded, "win_rate": win_r, "place_rate": place_r}
    elif type == "prices":
        _stats.trends = parse_trends_file(text)
        loaded = len(_stats.trends)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown stats type: {type}")

    _stats.build_lookups()
    return {"loaded": loaded}

@app.delete("/api/course-stats")
async def clear_stats():
    global _stats
    _stats = CourseStatsPack()
    return {"status": "cleared"}

# ── Going ─────────────────────────────────────────────────────────────────────
class GoingRequest(BaseModel):
    going: str

@app.post("/api/set-going")
async def set_going(req: GoingRequest):
    raw, bucket = parse_going_report(req.going)
    _stats.going_report = raw
    _stats.going_override = bucket
    return {"going_override": bucket, "going_report": raw}

# ── Hot trainers ──────────────────────────────────────────────────────────────
class HotTrainersRequest(BaseModel):
    names: str

@app.post("/api/set-hot-trainers")
async def set_hot_trainers(req: HotTrainersRequest):
    names = parse_hot_trainers(req.names)
    _stats.hot_trainers = names
    _stats.build_lookups()
    return {"hot_trainers": names}

# ── Image upload ──────────────────────────────────────────────────────────────
@app.post("/upload-context")
async def upload_context(
    label: str = Form(...),
    file: UploadFile = File(...),
):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured")

    content = await file.read()
    mime = file.content_type or "image/jpeg"
    if mime not in ("image/jpeg", "image/png", "image/webp", "image/gif"):
        mime = "image/jpeg"

    b64 = base64.standard_b64encode(content).decode()
    label_prompts = {
        "ten_year_trends":  "Extract all trainer and jockey statistics, strike rates, and trends from this racing data image.",
        "top_trainers":     "Extract trainer names and their statistics (wins, runs, strike rate) from this image.",
        "top_jockeys":      "Extract jockey names and their statistics (wins, runs, strike rate) from this image.",
        "going_report":     "Extract the going description, GoingStick readings, and any ground condition notes from this image.",
        "travellers_check": "Extract all horse names, yards/trainers, and distances travelled from this image.",
    }
    prompt = label_prompts.get(label, "Extract all relevant racing statistics and data from this image.")

    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": mime, "data": b64}},
                    {"type": "text", "text": prompt},
                ],
            }],
        )
        summary = message.content[0].text if message.content else ""
        _image_context[label] = summary
        return {"label": label, "summary": summary[:300]}
    except Exception as e:
        log.exception("Claude API error during image upload")
        raise HTTPException(status_code=500, detail=str(e))
