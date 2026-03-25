"""
Sporting Life race card scraper.
Strategy:
  1. Fetch page with httpx (fast, lightweight).
  2. Extract __NEXT_DATA__ JSON embedded by Next.js.
  3. Use the SL-specific parser first (handles actual field names: rides,
     horse.name, trainer.name, bookmakerOdds, previous_results, etc.).
  4. Fall back to generic JSON / BeautifulSoup parsing if SL structure not found.
Correct Sporting Life URL format (individual race):
  https://www.sportinglife.com/racing/racecards/DATE/VENUE/racecard/RACE_ID/race-slug
Date listing URL (all races on a day):
  https://www.sportinglife.com/racing/racecards/DATE
Venue-only URLs (/racing/racecards/DATE/VENUE) return 404 — the scraper
automatically redirects them to the date listing and filters by venue.
"""
from __future__ import annotations
import asyncio
import functools
import json
import re
import logging
from datetime import date, datetime, timedelta
from typing import Any, Optional
import httpx
from bs4 import BeautifulSoup
from models import Runner, RaceCard
log = logging.getLogger(__name__)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.sportinglife.com/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
# ---------------------------------------------------------------------------
# Sporting Life – specific helpers
# ---------------------------------------------------------------------------
def _going_bucket(going: Optional[str]) -> str:
    """Normalise a going string to a canonical bucket used in going_record keys."""
    if not going:
        return "unknown"
    g = going.lower()
    if "heavy" in g:         return "heavy"
    if "soft" in g:          return "soft"
    if "yield" in g:         return "soft"
    if "good to soft" in g:  return "good_soft"
    if "good to firm" in g:  return "good_firm"
    if "good" in g:          return "good"
    if "firm" in g:          return "firm"
    if "standard" in g:      return "good"   # AW standard
    if "fast" in g:          return "firm"
    return "unknown"
def _sl_distance_to_furlongs(dist_str: str) -> Optional[float]:
    """Convert SL distance strings to furlongs: '1m 7f 156y' → 15.71, '2m' → 16."""
    if not dist_str:
        return None
    s = dist_str.strip().lower()
    miles = furlongs = yards = 0.0
    m = re.search(r'(\d+)m', s)
    if m:
        miles = float(m.group(1))
    f = re.search(r'(\d+(?:\.\d+)?)f', s)
    if f:
        furlongs = float(f.group(1))
    y = re.search(r'(\d+)y', s)
    if y:
        yards = float(y.group(1))
    total = miles * 8 + furlongs + yards / 220
    return round(total, 2) if total > 0 else None
def _sl_race_type(race_summary: dict) -> str:
    """Infer race type from SL race_summary fields."""
    name = (race_summary.get("name") or "").lower()
    if "bumper" in name or "nh flat" in name:
        return "Bumper"
    if "hurdle" in name:
        return "Hurdle"
    if "chase" in name:
        return "Chase"
    surface = ((race_summary.get("course_surface") or {}).get("surface") or "").upper()
    if surface in ("AW", "ALLWEATHER", "ALL_WEATHER"):
        return "Flat AW"
    return "Flat"
def _parse_sl_ride(ride: dict, venue: str) -> Runner:
    """Map a Sporting Life 'ride' object to a Runner model."""
    horse = ride.get("horse") or {}
    name = horse.get("name", "").strip()
    number = ride.get("cloth_number") or 0
    draw = ride.get("draw_number")
    age = horse.get("age")
    days_since_last_run = horse.get("last_ran_days")
    # Form: keep only valid racing characters
    form_raw = ((horse.get("formsummary") or {}).get("display_text") or "")
    form_str = re.sub(r'[^0-9FUPRBCS/\-]', '', form_raw.upper()) or None
    trainer = ((ride.get("trainer") or {}).get("name") or "").strip() or None
    jockey = ((ride.get("jockey") or {}).get("name") or "").strip() or None
    official_rating = ride.get("official_rating")
    # Weight: "9-4" → 9st 4lb
    weight_st = weight_lb = None
    weight_total_lb = None
    wt = ride.get("handicap") or ""
    if wt:
        wm = re.match(r'(\d+)-(\d+)', str(wt))
        if wm:
            weight_st = int(wm.group(1))
            weight_lb = int(wm.group(2))
            weight_total_lb = weight_st * 14.0 + weight_lb
    non_runner = ride.get("ride_status", "RUNNER") in ("WITHDRAWN", "NON_RUNNER", "SCRATCHED")
    # Odds: current_odds first, then best bookmaker price, then forecast (set later)
    betting = ride.get("betting") or {}
    odds = betting.get("current_odds") or ""
    if not odds:
        bm_list = ride.get("bookmakerOdds") or []
        best_bm = next((b for b in bm_list if b.get("bestOdds")), bm_list[0] if bm_list else None)
        if best_bm:
            odds = best_bm.get("fractionalOdds") or ""
    odds_decimal = _parse_decimal_odds(odds) if odds else None
    # Build records from previous_results (most-recent-first in SL JSON)
    prev = horse.get("previous_results") or []
    or_history: list[int] = []
    or_trip_history: list[float] = []
    going_record: dict[str, list[int]] = {}
    distance_record: dict[str, list[int]] = {}
    course_wins = 0
    course_runs = 0
    venue_history: list[str] = []
    for pr in prev:
        bha = pr.get("bha")
        dist_f = _sl_distance_to_furlongs(pr.get("distance") or "")
        going_str = pr.get("going") or ""
        pos = pr.get("position") or 0
        course = pr.get("course_name") or ""
        if isinstance(bha, (int, float)) and bha:
            or_history.append(int(bha))
            if dist_f:
                or_trip_history.append(dist_f)
        g_bucket = _going_bucket(going_str)
        if g_bucket and g_bucket != "unknown":
            going_record.setdefault(g_bucket, [0, 0])
            going_record[g_bucket][1] += 1
            if pos == 1:
                going_record[g_bucket][0] += 1
        if dist_f:
            dkey = str(int(round(dist_f)))
            distance_record.setdefault(dkey, [0, 0])
            distance_record[dkey][1] += 1
            if pos == 1:
                distance_record[dkey][0] += 1
        if course:
            venue_history.append(course)
            if venue and venue.lower() in course.lower():
                course_runs += 1
                if pos == 1:
                    course_wins += 1
    # Reverse so order is oldest → newest (as scorer expects)
    or_history.reverse()
    or_trip_history.reverse()
    venue_history.reverse()
    return Runner(
        name=name,
        number=number,
        draw=draw,
        age=age,
        form=form_str,
        days_since_last_run=days_since_last_run,
        trainer=trainer,
        jockey=jockey,
        official_rating=official_rating,
        weight_st=weight_st,
        weight_lb=weight_lb,
        weight_total_lb=weight_total_lb,
        odds=odds or None,
        odds_decimal=odds_decimal,
        non_runner=non_runner,
        or_history=or_history,
        or_trip_history=or_trip_history,
        going_record=going_record,
        distance_record=distance_record,
        course_wins=course_wins,
        course_runs=course_runs,
        venue_history=venue_history,
    )
def _build_racecard_from_sl_race(race_obj: dict, url: str) -> Optional[RaceCard]:
    """Build a RaceCard from a SL race object (pageProps.race or nextTenRaces entry)."""
    rs = race_obj.get("race_summary") or {}
    venue = rs.get("course_name") or ""
    going = rs.get("going") or ""
    dist_f = _sl_distance_to_furlongs(rs.get("distance") or "")
    prizes_obj = race_obj.get("prizes") or {}
    prize_money = None
    if isinstance(prizes_obj, dict):
        raw = (prizes_obj.get("total") or prizes_obj.get("firstPrize")
               or prizes_obj.get("prize") or prizes_obj.get("totalPrize"))
        prize_money = str(raw) if raw else None
    race = RaceCard(
        url=url,
        title=rs.get("name") or "",
        venue=venue,
        date=rs.get("date") or "",
        going=going,
        race_type=_sl_race_type(rs),
        race_class=str(rs.get("race_class") or "") or None,
        distance_furlongs=dist_f,
        prize_money=prize_money,
    )
    # Build a name→odds lookup from the betting forecast string
    # e.g. "Mermaids Cave (13/8), Pigeon House (2/1), ..."
    forecast_str = race_obj.get("betting_forecast") or ""
    forecast_odds: dict[str, str] = {}
    if forecast_str and "No Forecast" not in forecast_str:
        for fm in re.finditer(r'([A-Za-z][^()]{1,50}?)\s*\(([^)]+)\)', forecast_str):
            forecast_odds[fm.group(1).strip().lower()] = fm.group(2).strip()
    runners: list[Runner] = []
    # FIX: accept rides under either 'rides' or 'runners' key
    ride_list = race_obj.get("rides") or race_obj.get("runners") or []
    for ride in ride_list:
        if not isinstance(ride, dict):
            continue
        runner = _parse_sl_ride(ride, venue)
        if not runner.name:
            continue
        # Use forecast odds as fallback when no live price is available
        if not runner.odds:
            fc = forecast_odds.get(runner.name.lower()) or ""
            if fc:
                runner.odds = fc
                runner.odds_decimal = _parse_decimal_odds(fc)
        runners.append(runner)
    race.runners = runners
    race.total_runners = len([r for r in runners if not r.non_runner])
    return race if runners else None
def _extract_race_id_from_url(url: str) -> Optional[str]:
    """Extract the numeric race ID from a SL racecard URL."""
    m = re.search(r'/racecard/(\d+)(?:/|$)', url)
    return m.group(1) if m else None
def _try_parse_sl_racecard(pp: dict, url: str) -> Optional[RaceCard]:
    """
    Parse Sporting Life's __NEXT_DATA__ pageProps into a RaceCard.
    Handles:
      - Individual race pages: pageProps.race  (has full rides + odds)
      - Date listing pages:    pageProps.nextTenRaces[n]  (next races with riders)
    """
    # Extract race ID from URL for precise nextTenRaces matching
    race_id = _extract_race_id_from_url(url)

    # Path 1: specific race card page → pageProps.race
    # FIX: check for 'rides' OR 'runners' key; use 'is not None' to avoid
    # empty-list falsy bug that caused second-parse failures.
    race_obj = pp.get("race")
    if isinstance(race_obj, dict):
        ride_list = race_obj.get("rides")
        if ride_list is None:
            ride_list = race_obj.get("runners")
        if ride_list is not None:   # key present (even if list is temporarily empty)
            # Normalise to 'rides' key for _build_racecard_from_sl_race
            if "rides" not in race_obj and ride_list:
                race_obj = {**race_obj, "rides": ride_list}
            result = _build_racecard_from_sl_race(race_obj, url)
            if result:
                log.info("SL parser: using pageProps.race")
                return result

    # Path 1b: pageProps.races array (some SL page variants)
    races_list = pp.get("races")
    if isinstance(races_list, list):
        for entry in races_list:
            if not isinstance(entry, dict):
                continue
            # Prefer exact race ID match
            if race_id:
                rs = entry.get("race_summary") or {}
                entry_id = str(rs.get("id") or entry.get("id") or "")
                if entry_id and entry_id != race_id:
                    continue
            ride_list = entry.get("rides") or entry.get("runners") or []
            if ride_list:
                result = _build_racecard_from_sl_race(entry, url)
                if result:
                    log.info("SL parser: using pageProps.races entry")
                    return result

    # Path 2: date listing → nextTenRaces, filtered by race ID or venue slug
    venue_slug = None
    m = re.search(r'/racing/racecards/\d{4}-\d{2}-\d{2}/([a-z0-9-]+)', url, re.I)
    if m:
        venue_slug = m.group(1).lower()

    for entry in (pp.get("nextTenRaces") or []):
        if not isinstance(entry, dict):
            continue
        ride_list = entry.get("rides") or entry.get("runners") or []
        if not ride_list:
            continue
        rs = entry.get("race_summary") or {}
        # Prefer exact race ID match when available
        if race_id:
            entry_id = str(rs.get("id") or entry.get("id") or "")
            if entry_id == race_id:
                log.info("SL parser: using nextTenRaces entry (race ID match)")
                return _build_racecard_from_sl_race(entry, url)
        # Fall back to venue slug matching
        if venue_slug:
            course_slug = rs.get("course_name", "").lower().replace(" ", "-")
            if venue_slug not in course_slug and course_slug not in venue_slug:
                continue
        log.info("SL parser: using pageProps.nextTenRaces entry")
        return _build_racecard_from_sl_race(entry, url)

    return None
def _sl_fallback_url(url: str) -> Optional[str]:
    """
    If the URL is a venue-only SL racecard URL (which returns 404),
    return the date-listing URL to try instead.
    e.g. /racing/racecards/2026-03-04/cheltenham  →  /racing/racecards/2026-03-04
    """
    m = re.match(
        r'(https://www\.sportinglife\.com/racing/racecards/\d{4}-\d{2}-\d{2})/[a-z0-9-]+/?$',
        url.rstrip("/"),
        re.I,
    )
    return m.group(1) if m else None
# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def _fetch_with_requests(url: str) -> str:
    """Synchronous fallback using the requests library."""
    import requests  # optional dependency; only imported on fallback path
    session = requests.Session()
    session.headers.update(HEADERS)
    resp = session.get(url, timeout=20, allow_redirects=True)
    resp.raise_for_status()
    return resp.text
async def _fetch_html(url: str) -> tuple[str | None, str]:
    """
    Fetch a URL, returning (html, last_error).
    Tries httpx first, then a synchronous requests fallback.
    """
    html: str | None = None
    last_error = ""
    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=20) as client:
            resp = await client.get(url)
        if resp.status_code == 200:
            html = resp.text
        else:
            last_error = f"HTTP {resp.status_code}"
            log.warning("httpx got HTTP %s for %s", resp.status_code, url)
    except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError, OSError) as exc:
        last_error = str(exc)
        log.warning("httpx failed (%s), trying requests fallback", exc)
    if html is None:
        try:
            loop = asyncio.get_event_loop()
            html = await loop.run_in_executor(None, functools.partial(_fetch_with_requests, url))
            log.info("requests fallback succeeded for %s", url)
        except Exception as exc:
            log.warning("requests fallback also failed: %s", exc)
            if not last_error or "getaddrinfo" in str(exc) or "ConnectionError" in type(exc).__name__:
                last_error = str(exc)
    return html, last_error
async def fetch_racecard(url: str) -> RaceCard:
    """Fetch and parse a Sporting Life race card URL. Returns a RaceCard."""
    log.info("Fetching: %s", url)
    original_url = url
    html, last_error = await _fetch_html(url)
    # If the URL looks like a venue-only URL (which returns 404 from SL's Next.js),
    # automatically try the date listing URL instead.
    if html is None or (html and '"statusCode":404' in html and '/racecard/' not in url):
        fallback = _sl_fallback_url(url)
        if fallback and fallback != url:
            log.info("Trying date-listing fallback: %s", fallback)
            html2, err2 = await _fetch_html(fallback)
            if html2:
                html = html2
                url = fallback  # use this URL for parser context (venue slug still in original_url)
                last_error = ""
    if html is None:
        if "getaddrinfo" in last_error or "11004" in last_error or "Name or service not known" in last_error:
            detail = (
                "DNS lookup failed — your network could not resolve 'www.sportinglife.com'.\n"
                "• Check your internet connection and DNS settings\n"
                "• Try opening https://www.sportinglife.com in a browser first"
            )
        elif "timed out" in last_error.lower() or "timeout" in last_error.lower():
            detail = "The request to Sporting Life timed out. The site may be slow or blocking requests."
        elif "403" in last_error or "429" in last_error:
            detail = (
                "Sporting Life is blocking automated requests (anti-bot protection). "
                "Try copying the race data manually."
            )
        else:
            detail = last_error or "Unknown network error."
        raise ValueError(
            f"Could not fetch the Sporting Life page.\n{detail}\n\n"
            "Tip: use the 'Load Sample' button to explore the app without a live URL."
        )
    # Pass the original URL so venue slug filtering works correctly
    race = _try_parse_next_data(html, original_url)
    if race:
        log.info("Parsed from __NEXT_DATA__ JSON (%d runners)", len(race.runners))
        return race
    race = _try_parse_inline_json(html, original_url)
    if race:
        log.info("Parsed from inline JSON")
        return race
    race = _parse_html(html, original_url)
    if race and race.runners:
        log.info("Parsed from HTML (found %d runners)", len(race.runners))
        return race
    raise ValueError(
        "Could not extract race card data from this page.\n"
        "For best results use a specific race URL:\n"
        "  sportinglife.com/racing/racecards/DATE/VENUE/racecard/RACE_ID/race-name\n\n"
        "Tip: use the 'Load Sample' button to explore the app without a live URL."
    )
# ---------------------------------------------------------------------------
# Parser: __NEXT_DATA__
# ---------------------------------------------------------------------------
def _try_parse_next_data(html: str, url: str) -> Optional[RaceCard]:
    """Try to extract runner data from Next.js __NEXT_DATA__ script tag."""
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    props = data.get("props", {}).get("pageProps", {})
    # --- Sporting Life-specific parser (highest priority) ---
    race = _try_parse_sl_racecard(props, url)
    if race:
        return race
    # --- Generic Next.js fallback (other racing sites) ---
    for key in ("raceCard", "racecard", "event", "meeting"):
        if key in props:
            return _extract_from_obj(props[key], url)
    runners_obj = _deep_find(props, "runners")
    if runners_obj:
        return _build_racecard_from_runners(runners_obj, props, url)
    return None
def _try_parse_inline_json(html: str, url: str) -> Optional[RaceCard]:
    """Look for large JSON objects in other <script> tags."""
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.S)
    for script in scripts:
        script = script.strip()
        if '"runners"' not in script and '"horses"' not in script:
            continue
        # FIX: use a targeted non-greedy search for JSON objects containing runners.
        # The previous greedy regex could match the entire script as one broken object.
        # Try to parse the whole script first (handles window.__data = {...} patterns).
        clean = re.sub(r'^\s*(?:window\.[A-Za-z_$][^=]*=\s*)?', '', script)
        clean = clean.rstrip(';').strip()
        if clean.startswith('{'):
            try:
                obj = json.loads(clean)
                rc = _extract_from_obj(obj, url)
                if rc and rc.runners:
                    return rc
            except json.JSONDecodeError:
                pass
        # Fallback: find individual JSON objects with a bounded non-greedy search
        for m in re.finditer(r'\{[^{}]{0,200}"runners"[^{}]{0,200}\}', script):
            try:
                obj = json.loads(m.group(0))
                rc = _extract_from_obj(obj, url)
                if rc and rc.runners:
                    return rc
            except json.JSONDecodeError:
                continue
    return None
# ---------------------------------------------------------------------------
# Parser: HTML fallback
# ---------------------------------------------------------------------------
def _parse_html(html: str, url: str) -> RaceCard:
    """BeautifulSoup fallback for static HTML race card content."""
    soup = BeautifulSoup(html, "lxml")
    race = RaceCard(url=url)
    # --- Race title / venue ---
    h1 = soup.find("h1")
    if h1:
        race.title = h1.get_text(strip=True)
    # --- Meta info (going, distance, class) ---
    # Look for common data-attribute patterns used by racing sites
    for el in soup.find_all(attrs={"data-going": True}):
        race.going = el["data-going"]
    for el in soup.find_all(attrs={"data-distance": True}):
        try:
            race.distance_furlongs = float(el["data-distance"])
        except ValueError:
            pass
    # --- Try to find runner rows ---
    # Sporting Life uses React-rendered divs; look for common class patterns
    runners = []
    # Pattern A: rows with horse name links
    horse_links = soup.find_all("a", href=re.compile(r"/racing/profiles/horse/"))
    for link in horse_links:
        name = link.get_text(strip=True)
        if not name or len(name) < 2:
            continue
        runner = Runner(name=name)
        # Try to get surrounding row context
        row = link.find_parent(["tr", "div", "li"])
        if row:
            _enrich_runner_from_tag(runner, row)
        runners.append(runner)
    # Pattern B: structured table rows
    if not runners:
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 4:
                runner = _parse_table_row(cells)
                if runner:
                    runners.append(runner)
    race.runners = runners
    race.total_runners = len(runners)
    return race
def _enrich_runner_from_tag(runner: Runner, tag) -> None:
    """Try to pull jockey, trainer, weight, OR, draw from a containing element."""
    text = tag.get_text(" ", strip=True)
    # Weight e.g. "9-2" or "9st 2lb"
    wt = re.search(r'(\d+)[- ](\d+)', text)
    if wt:
        st, lb = int(wt.group(1)), int(wt.group(2))
        if 7 <= st <= 12:
            runner.weight_st = st
            runner.weight_lb = lb
            runner.weight_total_lb = st * 14 + lb
    # OR e.g. "OR 95" or just a number 70-170
    or_match = re.search(r'\bOR\s*(\d+)', text, re.I)
    if or_match:
        runner.official_rating = int(or_match.group(1))
    # Form – sequences of digits and form chars
    form_match = re.search(r'[0-9FUPRBCS][0-9FUPRBCS\-\/\.]{2,}', text, re.I)
    if form_match:
        runner.form = form_match.group(0)
def _parse_table_row(cells) -> Optional[Runner]:
    texts = [c.get_text(strip=True) for c in cells]
    if not any(t for t in texts if len(t) > 2):
        return None
    runner = Runner(name=texts[1] if len(texts) > 1 else texts[0])
    for t in texts:
        if re.fullmatch(r'\d+', t) and 60 <= int(t) <= 175:
            runner.official_rating = int(t)
    return runner
# ---------------------------------------------------------------------------
# JSON object extraction helpers
# ---------------------------------------------------------------------------
def _extract_from_obj(obj: Any, url: str) -> Optional[RaceCard]:
    """Try to build a RaceCard from an arbitrary parsed JSON object."""
    if not isinstance(obj, dict):
        return None
    race = RaceCard(url=url)
    # Race metadata
    race.title = obj.get("name") or obj.get("title") or obj.get("raceName") or ""
    race.venue = obj.get("venue") or obj.get("course") or obj.get("courseName") or ""
    race.going = obj.get("going") or obj.get("groundConditions") or ""
    race.race_type = obj.get("type") or obj.get("raceType") or ""
    race.race_class = str(obj.get("class") or obj.get("raceClass") or "")
    race.prize_money = obj.get("prizeMoney") or obj.get("prize") or ""
    dist = obj.get("distanceFurlongs") or obj.get("distance")
    if dist is not None:
        try:
            race.distance_furlongs = float(dist)
        except (ValueError, TypeError):
            pass
    date_str = obj.get("date") or obj.get("raceDate") or obj.get("startTime") or ""
    if date_str:
        race.date = str(date_str)[:10]
    runners_raw = (
        obj.get("runners") or obj.get("horses") or obj.get("entrants") or []
    )
    if not isinstance(runners_raw, list) or not runners_raw:
        return None
    race.runners = [_parse_runner_obj(r) for r in runners_raw if isinstance(r, dict)]
    race.runners = [r for r in race.runners if r.name]
    race.total_runners = len(race.runners)
    return race if race.runners else None
def _parse_runner_obj(obj: dict) -> Runner:
    runner = Runner(
        name=(
            obj.get("name") or obj.get("horseName") or obj.get("horse") or ""
        ).strip(),
        number=_int(obj.get("number") or obj.get("saddleCloth") or obj.get("runnerId")),
        jockey=(obj.get("jockey") or obj.get("jockeyName") or ""),
        trainer=(obj.get("trainer") or obj.get("trainerName") or ""),
        official_rating=_int(obj.get("officialRating") or obj.get("or") or obj.get("rating")),
        draw=_int(obj.get("draw") or obj.get("stall") or obj.get("barrierDraw")),
        form=(obj.get("form") or obj.get("formFigures") or obj.get("recentForm") or ""),
        odds=(obj.get("odds") or obj.get("price") or obj.get("startingPrice") or ""),
    )
    # Age
    age_raw = obj.get("age") or obj.get("horseAge")
    if age_raw is not None:
        try:
            runner.age = int(str(age_raw).replace("y", "").strip())
        except ValueError:
            pass
    # Weight
    wt_raw = obj.get("weight") or obj.get("weightCarried") or obj.get("lbs")
    if wt_raw is not None:
        _parse_weight_into(runner, wt_raw)
    # Odds decimal
    runner.odds_decimal = _parse_decimal_odds(runner.odds)
    # Days since last run
    last_run = obj.get("lastRun") or obj.get("daysSinceLastRun") or obj.get("daysSinceRun")
    if last_run is not None:
        try:
            runner.days_since_last_run = int(last_run)
        except (ValueError, TypeError):
            pass
    return runner
def _parse_weight_into(runner: Runner, raw) -> None:
    """Parse various weight formats into stone/lb and total_lb."""
    try:
        if isinstance(raw, (int, float)):
            runner.weight_total_lb = float(raw)
            runner.weight_st = int(raw) // 14
            runner.weight_lb = int(raw) % 14
            return
        s = str(raw).strip()
        # "9-2" or "9st 2lb"
        m = re.match(r'(\d+)[\-\s](\d+)', s)
        if m:
            st, lb = int(m.group(1)), int(m.group(2))
            if 7 <= st <= 12:
                runner.weight_st = st
                runner.weight_lb = lb
                runner.weight_total_lb = st * 14.0 + lb
    except (ValueError, TypeError):
        pass
def _parse_decimal_odds(odds_str: Optional[str]) -> Optional[float]:
    if not odds_str:
        return None
    s = str(odds_str).strip().upper()
    if s in ("EVS", "EVENS", "1/1"):
        return 2.0
    if "/" in s:
        try:
            num, denom = s.split("/")
            return round(int(num) / int(denom) + 1, 2)
        except (ValueError, ZeroDivisionError):
            return None
    try:
        return float(s)
    except ValueError:
        return None
def _build_racecard_from_runners(runners_raw: Any, props: dict, url: str) -> Optional[RaceCard]:
    if not isinstance(runners_raw, list):
        return None
    race = RaceCard(url=url)
    race.runners = [_parse_runner_obj(r) for r in runners_raw if isinstance(r, dict)]
    race.runners = [r for r in race.runners if r.name]
    race.total_runners = len(race.runners)
    return race if race.runners else None
def _deep_find(obj: Any, key: str, depth: int = 0) -> Any:
    """Recursively search a nested dict/list for a given key."""
    if depth > 6:
        return None
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = _deep_find(v, key, depth + 1)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find(item, key, depth + 1)
            if result is not None:
                return result
    return None
def _int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
# ---------------------------------------------------------------------------
# Sample / demo race card (used when scraping is unavailable)
# ---------------------------------------------------------------------------
def get_sample_racecard() -> RaceCard:
    """Return a realistic sample race card for demo purposes."""
    today = date.today().isoformat()
    runners = [
        Runner(name="Appreciate It",     age=7,  weight_st=11, weight_lb=10, weight_total_lb=164, jockey="Paul Townend",     trainer="Willie Mullins",    official_rating=168, draw=5,  form="1-1-1-2-1",   odds="7/4",  odds_decimal=2.75, days_since_last_run=21, course_wins=2, course_runs=3, running_style="Prominent"),
        Runner(name="Constitution Hill", age=8,  weight_st=11, weight_lb=12, weight_total_lb=166, jockey="Nico de Boinville",trainer="Nicky Henderson",  official_rating=174, draw=3,  form="1-1-1-1-2-1", odds="5/4",  odds_decimal=2.25, days_since_last_run=28, course_wins=3, course_runs=4, running_style="Leads"),
        Runner(name="Shishkin",          age=9,  weight_st=11, weight_lb=8,  weight_total_lb=162, jockey="Nico de Boinville",trainer="Nicky Henderson",  official_rating=170, draw=1,  form="1-2-1-F-1-2", odds="9/2",  odds_decimal=5.50, days_since_last_run=35, course_wins=1, course_runs=3, running_style="Tracks Leader"),
        Runner(name="Energumene",        age=10, weight_st=11, weight_lb=6,  weight_total_lb=160, jockey="Paul Townend",     trainer="Willie Mullins",    official_rating=165, draw=8,  form="2-1-2-1-3-1", odds="6/1",  odds_decimal=7.00, days_since_last_run=42, course_wins=0, course_runs=2, running_style="Prominent"),
        Runner(name="El Fabiolo",        age=7,  weight_st=11, weight_lb=4,  weight_total_lb=158, jockey="Paul Townend",     trainer="Willie Mullins",    official_rating=171, draw=2,  form="1-1-1-1-P-1", odds="3/1",  odds_decimal=4.00, days_since_last_run=18, course_wins=1, course_runs=2, running_style="Tracks Leader"),
        Runner(name="Ferny Hollow",      age=8,  weight_st=11, weight_lb=2,  weight_total_lb=156, jockey="Paul Townend",     trainer="Willie Mullins",    official_rating=158, draw=6,  form="3-2-1-2-1-3", odds="10/1", odds_decimal=11.0, days_since_last_run=56, course_wins=0, course_runs=1, running_style="Midfield"),
        Runner(name="Janidil",           age=9,  weight_st=11, weight_lb=0,  weight_total_lb=154, jockey="Mark Walsh",       trainer="Willie Mullins",    official_rating=154, draw=7,  form="2-3-1-4-2-1", odds="14/1", odds_decimal=15.0, days_since_last_run=70, course_wins=0, course_runs=0, running_style="Hold Up"),
        Runner(name="Funambule Sivola",  age=7,  weight_st=10, weight_lb=12, weight_total_lb=152, jockey="Harry Cobden",     trainer="Venetia Williams",  official_rating=150, draw=4,  form="1-3-2-3-1-4", odds="20/1", odds_decimal=21.0, days_since_last_run=14, course_wins=0, course_runs=0, running_style="Hold Up"),
        Runner(name="Darasso",           age=10, weight_st=10, weight_lb=10, weight_total_lb=150, jockey="Tom Scudamore",    trainer="Dan Skelton",       official_rating=148, draw=9,  form="4-3-2-3-2-3", odds="25/1", odds_decimal=26.0, days_since_last_run=90, course_wins=0, course_runs=0, running_style="Midfield"),
        Runner(name="Solo",              age=8,  weight_st=10, weight_lb=8,  weight_total_lb=148, jockey="Rachael Blackmore",trainer="Henry de Bromhead", official_rating=145, draw=10, form="2-5-1-3-4-2", odds="33/1", odds_decimal=34.0, days_since_last_run=105,course_wins=0, course_runs=1, running_style="Hold Up"),
    ]
    return RaceCard(
        title="Betway Queen Mother Champion Chase (Grade 1)",
        venue="Cheltenham",
        date=today,
        race_type="Chase",
        race_class="Grade 1",
        distance_furlongs=16.0,
        going="Good to Soft",
        prize_money="£400,000",
        runners=runners,
        total_runners=len(runners),
        url="sample",
    )