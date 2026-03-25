"""
Envelope Analyser — FastAPI backend.
"""
from __future__ import annotations

import base64
import io
import logging
import os
from typing import Any, Optional

import anthropic
import httpx
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from models import RaceCard, Runner
from scraper import fetch_racecard, get_sample_racecard
from scorer import score_racecard
from course_stats import CourseStatsStore

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="Envelope Analyser")

# ── Static files ──────────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── In-memory stores ──────────────────────────────────────────────────────────
stats_store = CourseStatsStore()
image_context: dict[str, str] = {}   # label → extracted text summary

# ── Root: serve index.html ────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root():
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()

# ── Analyse endpoint ──────────────────────────────────────────────────────────
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

    cs = stats_store.get_stats()
    result = score_racecard(racecard, cs, image_context)
    return result

# ── Sample endpoint ───────────────────────────────────────────────────────────
@app.get("/api/sample")
async def sample():
    racecard = get_sample_racecard()
    cs = stats_store.get_stats()
    result = score_racecard(racecard, cs, image_context)
    return result

# ── Course stats upload ───────────────────────────────────────────────────────
@app.post("/api/upload-stats")
async def upload_stats(
    type: str = Form(...),
    file: UploadFile = File(...),
):
    content = await file.read()
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        raise HTTPException(status_code=400, detail="Could not decode file")

    result = stats_store.ingest(type, text)
    return result

@app.delete("/api/course-stats")
async def clear_stats():
    stats_store.clear()
    return {"status": "cleared"}

# ── Going / hot trainers ──────────────────────────────────────────────────────
class GoingRequest(BaseModel):
    going: str

@app.post("/api/set-going")
async def set_going(req: GoingRequest):
    result = stats_store.set_going(req.going)
    return result

class HotTrainersRequest(BaseModel):
    names: str

@app.post("/api/set-hot-trainers")
async def set_hot_trainers(req: HotTrainersRequest):
    result = stats_store.set_hot_trainers(req.names)
    return result

# ── Pre-meeting image upload (Claude vision) ──────────────────────────────────
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
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": mime,
                                "data": b64,
                            },
                        },
                        {"type": "text", "text": prompt},
                    ],
                }
            ],
        )
        summary = message.content[0].text if message.content else ""
        image_context[label] = summary
        return {"label": label, "summary": summary[:300]}
    except Exception as e:
        log.exception("Claude API error during image upload")
        raise HTTPException(status_code=500, detail=str(e))
