"""
Astrological feature encoder.

Reads birth data from Postgres and writes:
- planetary ecliptic longitudes (Sun..Pluto + optional points)
- house cusps & placements (if Swiss Ephemeris available)
- major aspects with orb strengths
- element & modality ratios
- a flat numeric feature vector (for ML/correlations)

Depends on:
- app.core.settings.Settings (PG_DSN, optional SWEPH_EPHE_PATH)
- app.core.db (pg_conn, pg_cursor)

Priority backend: pyswisseph (Swiss Ephemeris).
Fallback: skyfield (longitudes only; houses skipped).

Run:
    python -m app.workers.astro_features
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Dict, List, Tuple, Optional

import numpy as np
import psycopg2.extras

import os

# ---------------------------
# Backend selection
# ---------------------------
_BACKEND = None
try:
    import swisseph as swe  # type: ignore
    _BACKEND = "swisseph"
except Exception:
    try:
        from skyfield.api import load, wgs84  # type: ignore
        _BACKEND = "skyfield"
    except Exception:
        _BACKEND = None


# ---------------------------
# Domain constants
# ---------------------------
PLANETS = [
    "sun", "moon", "mercury", "venus", "mars",
    "jupiter", "saturn", "uranus", "neptune", "pluto"
]
# Optionally add points later (e.g., mean_node, chiron) and map them.

ASPECTS = {
    "conjunction": 0.0,
    "opposition": 180.0,
    "trine": 120.0,
    "square": 90.0,
    "sextile": 60.0,
}
# Orbs (deg) per aspect; conservative defaults
ASPECT_ORBS = {
    "sun":    {"conjunction": 10, "opposition": 10, "trine": 8, "square": 8, "sextile": 6},
    "moon":   {"conjunction": 10, "opposition": 10, "trine": 8, "square": 8, "sextile": 6},
    "mercury":{"conjunction": 7,  "opposition": 7,  "trine": 6, "square": 6, "sextile": 4},
    "venus":  {"conjunction": 7,  "opposition": 7,  "trine": 6, "square": 6, "sextile": 4},
    "mars":   {"conjunction": 7,  "opposition": 7,  "trine": 6, "square": 6, "sextile": 4},
    "jupiter":{"conjunction": 8,  "opposition": 8,  "trine": 7, "square": 7, "sextile": 5},
    "saturn": {"conjunction": 8,  "opposition": 8,  "trine": 7, "square": 7, "sextile": 5},
    "uranus": {"conjunction": 6,  "opposition": 6,  "trine": 5, "square": 5, "sextile": 4},
    "neptune":{"conjunction": 6,  "opposition": 6,  "trine": 5, "square": 5, "sextile": 4},
    "pluto":  {"conjunction": 6,  "opposition": 6,  "trine": 5, "square": 5, "sextile": 4},
}

SIGNS = [
    "aries","taurus","gemini","cancer","leo","virgo",
    "libra","scorpio","sagittarius","capricorn","aquarius","pisces"
]
SIGN_ELEMENTS = {
    "aries":"fire","leo":"fire","sagittarius":"fire",
    "taurus":"earth","virgo":"earth","capricorn":"earth",
    "gemini":"air","libra":"air","aquarius":"air",
    "cancer":"water","scorpio":"water","pisces":"water",
}
SIGN_MODALITIES = {
    "aries":"cardinal","cancer":"cardinal","libra":"cardinal","capricorn":"cardinal",
    "taurus":"fixed","leo":"fixed","scorpio":"fixed","aquarius":"fixed",
    "gemini":"mutable","virgo":"mutable","sagittarius":"mutable","pisces":"mutable",
}

# ---------------------------
# Helpers
# ---------------------------
def wrap360(x: float) -> float:
    return x % 360.0

def ang_distance(a: float, b: float) -> float:
    """Smallest angular distance in degrees [0,180]."""
    d = abs(wrap360(a) - wrap360(b))
    return d if d <= 180 else 360 - d

def sign_from_longitude(lon: float) -> Tuple[str, float]:
    """Return (sign_name, degrees_into_sign)."""
    lon = wrap360(lon)
    idx = int(lon // 30)
    deg_in_sign = lon - idx*30
    return SIGNS[idx], deg_in_sign

def to_julday_utc(d: datetime.date, t: Optional[datetime.time], tz_offset_minutes: Optional[int]) -> Tuple[float, bool]:
    """
    Convert local birth date/time + tz offset to Julian Day (UTC).
    If time missing, use noon local and mark unknown_time=True.
    """
    if t is None:
        t = dtime(12, 0, 0)
        unknown = True
    else:
        unknown = False
    # convert local time to UTC by subtracting offset
    offset = tz_offset_minutes or 0
    dt_local = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)
    dt_utc = dt_local - timedelta(minutes=offset)
    if _BACKEND == "swisseph":
        # Swiss Ephemeris expects UT as decimal hours
        ut = dt_utc.hour + dt_utc.minute/60 + dt_utc.second/3600
        jd = swe.julday(dt_utc.year, dt_utc.month, dt_utc.day, ut, swe.GREG_CAL)
    else:
        # Skyfield will use UTC datetime directly; we still compute JD for recording
        # Simple formula via astronomy toolkits would be better; approximate here:
        # Use Swiss formula if available; otherwise use a minimal jd calculation:
        # (This approximation is acceptable for record; positions use Skyfield.)
        a = (14 - dt_utc.month)//12
        y = dt_utc.year + 4800 - a
        m = dt_utc.month + 12*a - 3
        jdn = dt_utc.day + ((153*m + 2)//5) + 365*y + y//4 - y//100 + y//400 - 32045
        frac = (dt_utc.hour - 12)/24 + dt_utc.minute/1440 + dt_utc.second/86400
        jd = jdn + frac
    return jd, unknown

from datetime import timedelta  # placed after to avoid confusion in reading order


# ---------------------------
# Swiss Ephemeris backend
# ---------------------------
@dataclass
class SweConfig:
    eph_path: Optional[str] = None   # directory with ephemeris files (*.se1, *.se2, ...)

def swe_planet_longitudes(jd_ut: float, cfg: SweConfig) -> Dict[str, float]:
    if cfg.eph_path:
        swe.set_ephe_path(cfg.eph_path)
    flags = swe.FLG_SWIEPH | swe.FLG_SPEED
    mp = {
        "sun": swe.SUN, "moon": swe.MOON, "mercury": swe.MERCURY, "venus": swe.VENUS, "mars": swe.MARS,
        "jupiter": swe.JUPITER, "saturn": swe.SATURN, "uranus": swe.URANUS, "neptune": swe.NEPTUNE, "pluto": swe.PLUTO
    }
    out = {}
    for name, code in mp.items():
        result = swe.calc_ut(jd_ut, code, flags)
        if isinstance(result, tuple) and len(result) == 2:
            # Newer pyswisseph returns (xx, retflag)
            xx, _retflag = result
            lon, lat, dist, lon_speed, lat_speed, dist_speed = xx
        else:
            lon, lat, dist, lon_speed, lat_speed, dist_speed = result
        out[name] = wrap360(lon)
    return out

def swe_houses(jd_ut: float, lat: float, lon: float, system: str = "P") -> Dict[str, object]:
    """
    Houses and placements via Swiss Ephemeris.
    system: 'P' Placidus, 'K' Koch, 'O' Porphyrius, 'R' Regiomontanus, 'C' Campanus, 'E' Equal, 'W' Whole sign
    """
    # lon east positive in swe
    cusps, ascmc = swe.houses_ex(jd_ut, lat, lon, system)
    houses = {f"house_{i+1}": wrap360(cusps[i]) for i in range(12)}
    # ascendant, mc are in ascmc[0], ascmc[1]
    houses["asc"] = wrap360(ascmc[0]); houses["mc"] = wrap360(ascmc[1])
    return houses

def swe_house_placements(longs: Dict[str, float], houses: Dict[str, float]) -> Dict[str, int]:
    """
    Assign planets to houses by longitude and cusps (Placidus-like, assuming increasing sequence).
    For speed & simplicity, we approximate by traversing cusp arcs.
    """
    cusps = [houses[f"house_{i}"] if i!=0 else houses["house_1"] for i in range(1,13)]
    # ensure increasing wrap
    arcs = []
    for i in range(12):
        a = cusps[i]
        b = cusps[(i+1)%12]
        span = (b - a) % 360.0
        arcs.append((a, span))
    def house_of(lon: float) -> int:
        x = wrap360(lon - cusps[0])  # relative to house 1 cusp
        acc = 0.0
        for idx, (_, span) in enumerate(arcs, start=1):
            if x < acc + span or abs((acc + span) - x) < 1e-6:
                return idx
            acc += span
        return 12
    placements = {}
    for p, lon in longs.items():
        placements[p] = house_of(lon)
    return placements


# ---------------------------
# Skyfield fallback backend
# ---------------------------
_SKYFIELD_EPHE = None
def sf_load():
    global _SKYFIELD_EPHE
    if _SKYFIELD_EPHE is None:
        _SKYFIELD_EPHE = load("de421.bsp")  # small JPL kernel
    return _SKYFIELD_EPHE

def skyfield_longitudes(dt_utc: datetime, lat: float, lon: float) -> Dict[str, float]:
    """
    Ecliptic longitudes for major planets using Skyfield.
    Houses are not computed in fallback.
    """
    eph = sf_load()
    ts = load.timescale()
    t = ts.utc(dt_utc.year, dt_utc.month, dt_utc.day, dt_utc.hour, dt_utc.minute, dt_utc.second)

    earth = eph["earth"]
    observer = earth + wgs84.latlon(latitude_degrees=lat if lat is not None else 0.0,
                                    longitude_degrees=lon if lon is not None else 0.0)

    targets = {
        "sun": eph["sun"], "moon": eph["moon"], "mercury": eph["mercury barycenter"], "venus": eph["venus barycenter"],
        "mars": eph["mars barycenter"], "jupiter": eph["jupiter barycenter"], "saturn": eph["saturn barycenter"],
        "uranus": eph["uranus barycenter"], "neptune": eph["neptune barycenter"], "pluto": eph["pluto barycenter"],
    }

    out = {}
    for name, body in targets.items():
        e = observer.at(t).observe(body).ecliptic_position().au  # ecliptic position vector
        # Convert to longitude
        x, y, z = e
        lon_rad = math.atan2(y, x)
        lon_deg = math.degrees(lon_rad) % 360.0
        out[name] = lon_deg
    return out


# ---------------------------
# Feature construction
# ---------------------------
def compute_aspects(longs: Dict[str, float]) -> List[Dict[str, float]]:
    pairs = []
    names = list(longs.keys())
    for i in range(len(names)):
        for j in range(i+1, len(names)):
            a, b = names[i], names[j]
            d = ang_distance(longs[a], longs[b])
            # decide aspect type by closest target within orb
            best = None
            best_dev = 999.0
            for asp_name, asp_angle in ASPECTS.items():
                dev = abs(d - asp_angle)
                orb_a = ASPECT_ORBS.get(a, {}).get(asp_name, 5)
                orb_b = ASPECT_ORBS.get(b, {}).get(asp_name, 5)
                orb = min(orb_a, orb_b)  # stricter
                if dev <= orb and dev < best_dev:
                    best = (asp_name, asp_angle, orb, dev)
                    best_dev = dev
            if best:
                asp_name, asp_angle, orb, dev = best
                strength = (orb - dev) / orb if orb > 0 else 0.0  # 0..1
                pairs.append({
                    "a": a, "b": b,
                    "aspect": asp_name,
                    "angle": d,
                    "deviation": dev,
                    "strength": round(max(0.0, min(1.0, strength)), 4)
                })
    return pairs

def elem_modality_tallies(longs: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
    elems = {"fire":0.0,"earth":0.0,"air":0.0,"water":0.0}
    mods  = {"cardinal":0.0,"fixed":0.0,"mutable":0.0}
    for p, lon in longs.items():
        sign, _ = sign_from_longitude(lon)
        elems[SIGN_ELEMENTS[sign]] += 1.0
        mods[SIGN_MODALITIES[sign]] += 1.0
    # normalize to sum=1
    se = sum(elems.values()) or 1.0
    sm = sum(mods.values()) or 1.0
    elems = {k: round(v/se, 6) for k, v in elems.items()}
    mods  = {k: round(v/sm, 6) for k, v in mods.items()}
    return elems, mods

def flatten_feature_vec(longs: Dict[str, float],
                        placements: Optional[Dict[str, int]],
                        aspects: List[Dict[str, float]],
                        elems: Dict[str, float],
                        mods: Dict[str, float]) -> Dict[str, float]:
    fv = {}
    # planetary longitudes (sine/cosine encoding avoids wrap discontinuity)
    for p, lon in longs.items():
        rad = math.radians(lon)
        fv[f"lon_{p}_sin"] = round(math.sin(rad), 6)
        fv[f"lon_{p}_cos"] = round(math.cos(rad), 6)
    # house one-hot positions (if available)
    if placements:
        for p, h in placements.items():
            fv[f"house_{p}"] = float(h)  # or one-hot later if desired
    # aspects: add summed strengths per aspect type
    for asp_name in ASPECTS.keys():
        fv[f"aspect_strength_{asp_name}"] = round(sum(a["strength"] for a in aspects if a["aspect"] == asp_name), 6)
    # elements/modalities
    for k, v in elems.items():
        fv[f"elem_{k}"] = v
    for k, v in mods.items():
        fv[f"mod_{k}"] = v
    return fv


# ---------------------------
# Main job
# ---------------------------
def compute_features_for_person(row, backend: str) -> Dict[str, object]:
    """
    row: dict with person_id, date, time, tz_offset_minutes, lat, lon
    backend: 'swisseph' or 'skyfield'
    """
    # --- time & JD
    jd, unknown_time = to_julday_utc(row["date"], row["time"], row["tz_offset_minutes"])

    # --- longitudes & houses
    if backend == "swisseph":
        ephe_path = (
            os.getenv("SWEPH_EPHE_PATH")
            or os.getenv("SE_EPHE_PATH")
        )
        if ephe_path:
            swe.set_ephe_path(ephe_path)  # optional ephemeris dir
        longs = swe_planet_longitudes(jd, SweConfig(eph_path=ephe_path))
        houses = None
        placements = None
        if row["lat"] is not None and row["lon"] is not None:
            try:
                houses = swe_houses(jd, row["lat"], row["lon"], system="P")
                placements = swe_house_placements(longs, houses)
            except Exception:
                houses, placements = None, None
        dt_utc = None  # not needed for Swiss; positions came from JD
    elif backend == "skyfield":
        # reconstruct dt_utc for skyfield longitudes
        local_t = row["time"] or dtime(12, 0, 0)
        offset = row["tz_offset_minutes"] or 0
        dt_local = datetime(row["date"].year, row["date"].month, row["date"].day,
                            local_t.hour, local_t.minute, local_t.second)
        dt_utc = dt_local - timedelta(minutes=offset)
        longs = skyfield_longitudes(dt_utc, row["lat"] or 0.0, row["lon"] or 0.0)
        houses, placements = None, None  # skipped in fallback
    else:
        raise RuntimeError("No ephemeris backend available. Install 'pyswisseph' or 'skyfield'.")

    # --- secondary features
    aspects = compute_aspects(longs)
    elem_ratios, modality_ratios = elem_modality_tallies(longs)
    feature_vec = flatten_feature_vec(longs, placements, aspects, elem_ratios, modality_ratios)

    return {
        "system": backend,
        "jd_utc": jd,
        "unknown_time": bool(unknown_time),
        "longs": {k: round(float(v), 6) for k, v in longs.items()},
        "houses": houses,
        "aspects": aspects,
        "elem_ratios": elem_ratios,
        "modality_ratios": modality_ratios,
        "feature_vec": feature_vec
    }


def _birth_has_tz_offset(cur) -> bool:
    cur.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'birth'
          AND column_name = 'tz_offset_minutes'
        LIMIT 1
    """)
    return cur.fetchone() is not None


def run(batch_size: int = 128) -> int:
    """
    Process births that don't yet have astro_features.
    Returns number of rows written.
    """
    if _BACKEND is None:
        raise RuntimeError("Install 'pyswisseph' (preferred) or 'skyfield' to compute astro features.")

    backend = "swisseph" if _BACKEND == "swisseph" else "skyfield"

    from service.core.db import pg_conn, pg_cursor

    with pg_conn() as conn, pg_cursor(conn) as cur:
        # Find work
        tz_offset_expr = "b.tz_offset_minutes" if _birth_has_tz_offset(cur) else "NULL::int"
        cur.execute(f"""
            SELECT b.person_id, b.date, b.time, {tz_offset_expr} AS tz_offset_minutes, b.lat, b.lon
            FROM birth b
            LEFT JOIN astro_features af ON af.person_id = b.person_id
            WHERE af.person_id IS NULL
            LIMIT %s
        """, (batch_size,))
        rows = cur.fetchall()

        if not rows:
            print("No people pending astro feature computation.")
            return 0

        wrote = 0
        for r in rows:
            try:
                feats = compute_features_for_person(r, backend=backend)
                cur.execute("""
                    INSERT INTO astro_features
                        (person_id, system, jd_utc, unknown_time, longs, houses, aspects, elem_ratios, modality_ratios, feature_vec)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                    ON CONFLICT (person_id) DO UPDATE
                      SET system=EXCLUDED.system,
                          jd_utc=EXCLUDED.jd_utc,
                          unknown_time=EXCLUDED.unknown_time,
                          longs=EXCLUDED.longs,
                          houses=EXCLUDED.houses,
                          aspects=EXCLUDED.aspects,
                          elem_ratios=EXCLUDED.elem_ratios,
                          modality_ratios=EXCLUDED.modality_ratios,
                          feature_vec=EXCLUDED.feature_vec
                """, (
                    r["person_id"], feats["system"], feats["jd_utc"], feats["unknown_time"],
                    psycopg2.extras.Json(feats["longs"]),
                    psycopg2.extras.Json(feats["houses"]) if feats["houses"] is not None else None,
                    psycopg2.extras.Json(feats["aspects"]),
                    psycopg2.extras.Json(feats["elem_ratios"]),
                    psycopg2.extras.Json(feats["modality_ratios"]),
                    psycopg2.extras.Json(feats["feature_vec"])
                ))
                wrote += 1
            except Exception as e:
                # You can add provenance logging here if you have a helper
                print(f"[astro_features] Error person_id={r['person_id']}: {e}")

        print(f"✅ astro_features: wrote {wrote} rows using backend={backend}")
        return wrote


def run_forever():
    """
    Continuous worker loop with backoff when idle.
    """
    batch_size = int(os.getenv("ASTRO_BATCH_SIZE", "128"))
    idle_sleep = float(os.getenv("ASTRO_IDLE_SLEEP_SECONDS", "2"))
    max_idle_sleep = float(os.getenv("ASTRO_MAX_IDLE_SLEEP_SECONDS", "30"))

    sleep_seconds = idle_sleep
    while True:
        wrote = run(batch_size=batch_size)
        if wrote > 0:
            sleep_seconds = idle_sleep
            continue
        time.sleep(sleep_seconds)
        sleep_seconds = min(sleep_seconds * 2, max_idle_sleep)


if __name__ == "__main__":
    run_forever()
