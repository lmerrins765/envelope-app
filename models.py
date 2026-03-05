"""
Pydantic models for the Envelope Analyser racing app.
"""
from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, Field


class Runner(BaseModel):
    # ── Basic card data ──────────────────────────────────────────────────
    number: int = 0
    name: str
    age: Optional[int] = None
    weight_st: Optional[int] = None
    weight_lb: Optional[int] = None
    weight_total_lb: Optional[float] = None
    jockey: Optional[str] = None
    trainer: Optional[str] = None
    official_rating: Optional[int] = None          # current OR
    draw: Optional[int] = None
    form: Optional[str] = None                     # e.g. "1-2-3-1-F0"
    odds: Optional[str] = None
    odds_decimal: Optional[float] = None
    non_runner: bool = False

    # ── OR / trip history ────────────────────────────────────────────────
    or_history: list[int] = Field(default_factory=list)
    # Distances (furlongs) at which each OR in or_history was recorded
    or_trip_history: list[float] = Field(default_factory=list)

    # ── Going / distance records ─────────────────────────────────────────
    # e.g. {"Good": [3, 8], "Soft": [1, 4]}  → [wins, total_runs]
    going_record: dict[str, list[int]] = Field(default_factory=dict)
    # e.g. {"16": [2, 5]}  furlongs as string key
    distance_record: dict[str, list[int]] = Field(default_factory=dict)

    # ── Race history (for traveller's check) ─────────────────────────────
    # List of venue names from recent runs (most recent last)
    venue_history: list[str] = Field(default_factory=list)

    # ── Course record ─────────────────────────────────────────────────────
    course_wins: int = 0   # wins at today's venue
    course_runs: int = 0   # total runs at today's venue

    # ── Breeding ─────────────────────────────────────────────────────────
    sire: Optional[str] = None
    dam_sire: Optional[str] = None

    # ── Running style ─────────────────────────────────────────────────────
    # e.g. "Leads", "Prominent", "Tracks Leader", "Midfield", "Hold Up"
    running_style: Optional[str] = None

    # ── Freshness ─────────────────────────────────────────────────────────
    days_since_last_run: Optional[int] = None

    # ── Home track (base) ─────────────────────────────────────────────────
    home_track: Optional[str] = None


class RaceCard(BaseModel):
    title: str = "Unknown Race"
    venue: str = ""
    date: str = ""
    race_type: str = ""           # Flat / Hurdle / Chase / Bumper / NH Flat
    race_class: Optional[str] = None
    distance_furlongs: Optional[float] = None
    going: Optional[str] = None
    prize_money: Optional[str] = None
    runners: list[Runner] = Field(default_factory=list)
    url: str = ""
    total_runners: int = 0
    # Running styles present in the field (populated by scorer)
    field_running_styles: list[str] = Field(default_factory=list)


class FactorScore(BaseModel):
    key: str
    name: str
    score: float         # 0–10
    weight: float        # 0–1
    weighted: float      # score × weight
    detail: str          # human-readable explanation


class ScoredRunner(BaseModel):
    runner: Runner
    factors: list[FactorScore]
    total_score: float   # weighted sum, 0–10
    rank: int = 0


class ScoredRaceCard(BaseModel):
    race: RaceCard
    runners: list[ScoredRunner]
    factor_weights: dict[str, float]


class AnalyseRequest(BaseModel):
    url: str
