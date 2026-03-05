"""
Course statistics parser and data container.

Handles 7 user-supplied data sources:
  1. Top trainers listing (CSV / TSV / plain text)
  2. Top jockeys listing  (CSV / TSV / plain text)
  3. Ten-year trends      (text bullets or CSV)
  4. Traveller's check    (CSV / plain text)
  5. Favourites success   (text or CSV)
  6. Going report         (free text)
  7. Hot trainers         (comma or newline separated names)
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class NameStat:
    name: str
    wins: int = 0
    runs: int = 0
    win_pct: float = 0.0      # 0-1
    place_pct: float = 0.0    # 0-1


@dataclass
class TravellerStat:
    origin: str
    wins: int = 0
    runs: int = 0
    win_pct: float = 0.0


@dataclass
class CourseStatsPack:
    trainers:    list[NameStat]      = field(default_factory=list)
    jockeys:     list[NameStat]      = field(default_factory=list)
    trends:      list[str]           = field(default_factory=list)   # text bullets
    travellers:  list[TravellerStat] = field(default_factory=list)
    fav_win_rate:   Optional[float]  = None   # 0-1
    fav_place_rate: Optional[float]  = None   # 0-1
    going_report:   str              = ""     # raw text
    going_override: str              = ""     # normalised going bucket
    hot_trainers:   list[str]        = field(default_factory=list)  # lower-cased

    # Derived lookups (call build_lookups() after any change)
    _trainer_lookup: dict[str, NameStat] = field(default_factory=dict, repr=False)
    _jockey_lookup:  dict[str, NameStat] = field(default_factory=dict, repr=False)
    _hot_set:        set[str]            = field(default_factory=set, repr=False)

    def build_lookups(self) -> None:
        self._trainer_lookup = {t.name.lower(): t for t in self.trainers}
        self._jockey_lookup  = {j.name.lower(): j for j in self.jockeys}
        self._hot_set        = set(self.hot_trainers)

    def find_trainer(self, name: Optional[str]) -> Optional[NameStat]:
        if not name:
            return None
        key = name.strip().lower()
        # Exact match
        if key in self._trainer_lookup:
            return self._trainer_lookup[key]
        # Partial match (surname or partial name)
        for k, v in self._trainer_lookup.items():
            if key in k or k in key:
                return v
        return None

    def find_jockey(self, name: Optional[str]) -> Optional[NameStat]:
        if not name:
            return None
        key = name.strip().lower()
        if key in self._jockey_lookup:
            return self._jockey_lookup[key]
        for k, v in self._jockey_lookup.items():
            if key in k or k in key:
                return v
        return None

    def is_hot(self, trainer_name: Optional[str]) -> bool:
        if not trainer_name:
            return False
        key = trainer_name.strip().lower()
        return any(hot in key or key in hot for hot in self._hot_set)

    def has_any(self) -> bool:
        return bool(
            self.trainers or self.jockeys or self.trends or self.travellers
            or self.fav_win_rate is not None or self.going_report or self.hot_trainers
        )

    def summary(self) -> dict:
        return {
            "trainers":      len(self.trainers),
            "jockeys":       len(self.jockeys),
            "trends":        len(self.trends),
            "travellers":    len(self.travellers),
            "fav_win_rate":  round(self.fav_win_rate * 100, 1) if self.fav_win_rate else None,
            "fav_place_rate":round(self.fav_place_rate * 100, 1) if self.fav_place_rate else None,
            "going_report":  self.going_report,
            "going_override":self.going_override,
            "hot_trainers":  self.hot_trainers,
        }

    def clear_field(self, field_name: str) -> None:
        defaults = {
            "trainers":      [],   "jockeys":      [],   "trends":      [],
            "travellers":    [],   "fav_win_rate":  None, "fav_place_rate": None,
            "going_report":  "",   "going_override":"",  "hot_trainers":  [],
        }
        if field_name in defaults:
            setattr(self, field_name, defaults[field_name])
            self.build_lookups()


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _pct(raw: str) -> Optional[float]:
    """Parse '23.4%' or '0.234' or '23' into a 0-1 float."""
    if not raw:
        return None
    s = str(raw).strip().replace(",", "")
    s = re.sub(r'[%％]', '', s)
    try:
        v = float(s)
        return v / 100.0 if v > 1.5 else v
    except ValueError:
        return None


def _int_or(raw, default: int = 0) -> int:
    try:
        return int(str(raw).strip().replace(",", ""))
    except (ValueError, TypeError):
        return default


def _detect_dialect(text: str) -> str:
    """Return 'csv', 'tsv', or 'text'."""
    first_line = text.split('\n')[0]
    if '\t' in first_line:
        return 'tsv'
    if ',' in first_line:
        return 'csv'
    return 'text'


def _sniff_col(headers: list[str], aliases: set[str]) -> Optional[int]:
    """Find the index of the first header that matches any alias (case-insensitive)."""
    for i, h in enumerate(headers):
        if h.strip().lower() in aliases:
            return i
    return None


# ---------------------------------------------------------------------------
# 1 & 2 – Trainer / Jockey stat files
# ---------------------------------------------------------------------------

_NAME_ALIASES  = {"trainer", "jockey", "name", "rider", "handler", "owner"}
_WINS_ALIASES  = {"wins", "w", "winners", "win", "1sts", "1st"}
_RUNS_ALIASES  = {"runs", "r", "runners", "total", "mounts", "trained", "rode"}
_PCT_ALIASES   = {"win%", "sr", "strike rate", "strike%", "win rate", "win pct",
                  "pct", "%", "w%", "winpct", "wr"}
_PLACE_ALIASES = {"place%", "plc%", "placed%", "p%", "place rate", "e/w%"}


def parse_name_stat_file(text: str) -> list[NameStat]:
    """
    Parse a trainer or jockey stats file.
    Handles CSV/TSV with flexible headers, and plain-text tabular formats.
    """
    dialect = _detect_dialect(text)

    # ── CSV / TSV path ────────────────────────────────────────────────
    if dialect in ('csv', 'tsv'):
        delim = ',' if dialect == 'csv' else '\t'
        reader = csv.reader(io.StringIO(text), delimiter=delim)
        rows = [r for r in reader if any(c.strip() for c in r)]
        if not rows:
            return []

        # Try to find header row
        header_row = rows[0]
        headers_lc = [h.strip().lower() for h in header_row]

        i_name  = _sniff_col(headers_lc, _NAME_ALIASES)
        i_wins  = _sniff_col(headers_lc, _WINS_ALIASES)
        i_runs  = _sniff_col(headers_lc, _RUNS_ALIASES)
        i_pct   = _sniff_col(headers_lc, _PCT_ALIASES)
        i_place = _sniff_col(headers_lc, _PLACE_ALIASES)

        if i_name is None:
            # Try first column as name
            i_name = 0

        results = []
        for row in rows[1:]:
            if len(row) <= i_name:
                continue
            name = row[i_name].strip().strip('"')
            if not name or name.lower() in _NAME_ALIASES:
                continue

            # Parse wins/runs from "W-R" cell if no separate columns
            wins, runs, wp, pp = 0, 0, 0.0, 0.0
            wr_match = None
            for cell in row:
                m = re.search(r'(\d+)-(\d+)', cell)
                if m:
                    wr_match = m
                    break

            if wr_match:
                wins = int(wr_match.group(1))
                runs = int(wr_match.group(2))
            else:
                if i_wins is not None and i_wins < len(row):
                    wins = _int_or(row[i_wins])
                if i_runs is not None and i_runs < len(row):
                    runs = _int_or(row[i_runs])

            if i_pct is not None and i_pct < len(row):
                wp = _pct(row[i_pct]) or (wins / runs if runs else 0.0)
            elif runs > 0:
                wp = wins / runs

            if i_place is not None and i_place < len(row):
                pp = _pct(row[i_place]) or 0.0

            results.append(NameStat(name=name, wins=wins, runs=runs,
                                    win_pct=wp, place_pct=pp))
        return results

    # ── Plain-text path ───────────────────────────────────────────────
    return _parse_text_name_stats(text)


def _parse_text_name_stats(text: str) -> list[NameStat]:
    """
    Parse plain-text trainer/jockey listings.
    Handles formats like:
      "1. Willie Mullins  42-180  23%"
      "Willie Mullins: 42 wins from 180 runs (23%)"
    """
    results = []
    for line in text.splitlines():
        line = line.strip()
        if not line or len(line) < 3:
            continue

        # Strip leading number/bullet
        line = re.sub(r'^[\d]+[\.\):\-]\s*', '', line)

        # Extract percentage
        pct_m = re.search(r'([\d]+\.?[\d]*)\s*%', line)
        wp = _pct(pct_m.group(0)) if pct_m else 0.0

        # Extract W-R pair  e.g. "42-180" or "42 from 180" or "42/180"
        wr_m = re.search(r'(\d+)[\s\-\/from]+(\d+)', line)
        wins = int(wr_m.group(1)) if wr_m else 0
        runs = int(wr_m.group(2)) if wr_m else 0
        if runs and not wp:
            wp = wins / runs

        # Name = everything before the first digit
        name_m = re.match(r'^([A-Za-z][A-Za-z\s\'\.\-]+?)(?:\s{2,}|\d)', line)
        if not name_m:
            continue
        name = name_m.group(1).strip().strip(':,-')
        if len(name) < 2:
            continue

        results.append(NameStat(name=name, wins=wins, runs=runs, win_pct=wp))

    return results


# ---------------------------------------------------------------------------
# 3 – Ten-year trends
# ---------------------------------------------------------------------------

def parse_trends_file(text: str) -> list[str]:
    """
    Return a list of trend statements from the uploaded file.
    Works for both bullet-point text and CSV.
    """
    dialect = _detect_dialect(text)
    bullets = []

    if dialect in ('csv', 'tsv'):
        delim = ',' if dialect == 'csv' else '\t'
        for row in csv.reader(io.StringIO(text), delimiter=delim):
            cell = ' | '.join(c.strip() for c in row if c.strip())
            if cell:
                bullets.append(cell)
    else:
        for line in text.splitlines():
            line = re.sub(r'^[-•*>\d\.\)]+\s*', '', line.strip())
            if len(line) >= 5:
                bullets.append(line)

    return bullets


# ---------------------------------------------------------------------------
# 4 – Traveller's check
# ---------------------------------------------------------------------------

def parse_travellers_file(text: str) -> list[TravellerStat]:
    """
    Parse how visitors from different origins perform at the course.
    Accepts CSV or text like:
      "Ireland: 45 from 230 (19.6%)"
      "Origin,Wins,Runs,Win%"
    """
    dialect = _detect_dialect(text)
    results = []

    if dialect in ('csv', 'tsv'):
        delim = ',' if dialect == 'csv' else '\t'
        rows = list(csv.reader(io.StringIO(text), delimiter=delim))
        if not rows:
            return []
        headers_lc = [h.strip().lower() for h in rows[0]]
        i_name = _sniff_col(headers_lc, {"origin", "country", "region", "from", "source", "name"}) or 0
        i_wins = _sniff_col(headers_lc, _WINS_ALIASES)
        i_runs = _sniff_col(headers_lc, _RUNS_ALIASES)
        i_pct  = _sniff_col(headers_lc, _PCT_ALIASES)
        for row in rows[1:]:
            if not row or not row[0].strip():
                continue
            origin = row[i_name].strip()
            wins = _int_or(row[i_wins]) if i_wins and i_wins < len(row) else 0
            runs = _int_or(row[i_runs]) if i_runs and i_runs < len(row) else 0
            wp   = _pct(row[i_pct]) if i_pct and i_pct < len(row) else (wins/runs if runs else 0.0)
            results.append(TravellerStat(origin=origin, wins=wins, runs=runs, win_pct=wp))
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            wr_m = re.search(r'(\d+)\s*(?:from|wins?|[-\/])\s*(\d+)', line, re.I)
            pct_m = re.search(r'([\d.]+)\s*%', line)
            origin_m = re.match(r'^([A-Za-z][A-Za-z\s\'\.\-]+?)[\s:]+', line)
            if not origin_m:
                continue
            origin = origin_m.group(1).strip()
            wins = int(wr_m.group(1)) if wr_m else 0
            runs = int(wr_m.group(2)) if wr_m else 0
            wp   = _pct(pct_m.group(0)) if pct_m else (wins / runs if runs else 0.0)
            results.append(TravellerStat(origin=origin, wins=wins, runs=runs, win_pct=wp))

    return results


# ---------------------------------------------------------------------------
# 5 – Favourites success rates
# ---------------------------------------------------------------------------

def parse_favourites_file(text: str) -> tuple[Optional[float], Optional[float]]:
    """
    Extract favourite win rate and place rate.
    Returns (win_pct, place_pct) as 0-1 floats.
    """
    win_rate = place_rate = None

    for line in text.splitlines():
        line_lc = line.lower()
        pct_m = re.search(r'([\d]+\.?[\d]*)\s*%', line)
        if not pct_m:
            continue
        val = _pct(pct_m.group(0))
        if val is None:
            continue
        if any(w in line_lc for w in ("win", "1st", "first")) and "place" not in line_lc:
            win_rate = val
        elif any(w in line_lc for w in ("place", "2nd", "3rd", "each way", "e/w")):
            place_rate = val

    # Fallback: if only one number found and win_rate not set
    all_pcts = re.findall(r'([\d]+\.?[\d]*)\s*%', text)
    if win_rate is None and all_pcts:
        win_rate = _pct(all_pcts[0])
    if place_rate is None and len(all_pcts) >= 2:
        place_rate = _pct(all_pcts[1])

    return win_rate, place_rate


# ---------------------------------------------------------------------------
# 6 – Going report
# ---------------------------------------------------------------------------

_GOING_KEYWORDS = {
    "heavy":       "heavy",
    "soft":        "soft",
    "yielding":    "soft",
    "good to soft":"good_soft",
    "good to firm":"good_firm",
    "good":        "good",
    "firm":        "firm",
    "hard":        "firm",
    "standard":    "good",   # AW
    "fast":        "firm",
    "slow":        "soft",
    "muddy":       "heavy",
}


def parse_going_report(text: str) -> tuple[str, str]:
    """
    Parse a going report into (raw_text, normalised_bucket).
    Returns the raw text and a normalised going bucket string.
    """
    raw = text.strip()
    lc  = raw.lower()

    # Try longest match first
    for phrase in sorted(_GOING_KEYWORDS, key=len, reverse=True):
        if phrase in lc:
            return raw, _GOING_KEYWORDS[phrase]

    return raw, ""


# ---------------------------------------------------------------------------
# 7 – Hot trainers
# ---------------------------------------------------------------------------

def parse_hot_trainers(text: str) -> list[str]:
    """
    Parse a list of currently in-form trainer names.
    Accepts comma/newline/semicolon separated names, or lines like:
      "Mullins – 8 winners from last 14 runners"
    Returns lower-cased names.
    """
    # Split on common delimiters
    parts = re.split(r'[,;\n]+', text)
    names = []
    for part in parts:
        # Strip stats / extra text — keep only the name (up to first digit or dash+space)
        name = re.split(r'\s*[-–—]\s*\d|\s{3,}|\(|\[', part)[0]
        name = re.sub(r'^[\d\.\)\-\s]+', '', name).strip().strip('.:')
        if len(name) >= 2:
            names.append(name.lower())
    return names
