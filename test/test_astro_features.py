import datetime as dt
import types

from service.astro.app.astro_features import (
    PLANETS,
    ang_distance,
    compute_aspects,
    compute_features_for_person,
    elem_modality_tallies,
    sign_from_longitude,
    to_julday_utc,
    wrap360,
)

def test_compute_features_minimal(monkeypatch):
    # Fake longitudes (spread them around the zodiac deterministically)
    fake_longs = {p: (i * 33.3) % 360 for i, p in enumerate(PLANETS)}

    # Patch the skyfield_longitudes function used by the 'skyfield' backend
    import service.astro.app.astro_features as af
    def _fake_sf_longitudes(_dt_utc, _lat, _lon):
        return fake_longs
    monkeypatch.setattr(af, "skyfield_longitudes", _fake_sf_longitudes)

    # Build a minimal birth row like a DB row dict
    row = {
        "person_id": "00000000-0000-0000-0000-000000000001",
        "date": dt.date(1984, 6, 12),
        "time": dt.time(6, 30, 0),
        "tz_offset_minutes": 60,   # UTC+1
        "lat": 59.91,              # Oslo-ish
        "lon": 10.75,
    }

    out = compute_features_for_person(row, backend="skyfield")

    # Basic shape checks
    assert set(out.keys()) == {
        "system","jd_utc","unknown_time","longs","houses",
        "aspects","elem_ratios","modality_ratios","feature_vec"
    }
    assert out["system"] == "skyfield"
    assert isinstance(out["jd_utc"], float)
    assert out["unknown_time"] is False

    # Longitudes come from our fake map
    assert set(out["longs"].keys()) == set(PLANETS)
    for p in PLANETS:
        assert 0.0 <= out["longs"][p] < 360.0

    # Houses are None in skyfield fallback
    assert out["houses"] is None

    # Aspects computed from fake longs → list of dicts
    assert isinstance(out["aspects"], list)
    if out["aspects"]:
        sample = out["aspects"][0]
        assert {"a","b","aspect","angle","deviation","strength"} <= set(sample.keys())

    # Element/modality ratios sum to ~1.0
    er = out["elem_ratios"]; mr = out["modality_ratios"]
    assert abs(sum(er.values()) - 1.0) < 1e-6
    assert abs(sum(mr.values()) - 1.0) < 1e-6

    # Feature vector must include sin/cos entries for each planet
    fv = out["feature_vec"]
    for p in PLANETS:
        assert f"lon_{p}_sin" in fv and f"lon_{p}_cos" in fv


def test_compute_features_deterministic_snapshot(monkeypatch):
    # Force fully deterministic longitudes (all planets at 0° Aries).
    fake_longs = {p: 0.0 for p in PLANETS}

    import service.astro.app.astro_features as af

    def _fake_sf_longitudes(_dt_utc, _lat, _lon):
        return fake_longs

    monkeypatch.setattr(af, "skyfield_longitudes", _fake_sf_longitudes)

    row = {
        "person_id": "00000000-0000-0000-0000-000000000002",
        "date": dt.date(1984, 6, 12),
        "time": dt.time(6, 30, 0),
        "tz_offset_minutes": 60,
        "lat": 59.91,
        "lon": 10.75,
    }

    out = compute_features_for_person(row, backend="skyfield")

    assert out["system"] == "skyfield"
    assert out["unknown_time"] is False
    assert out["houses"] is None
    assert out["longs"] == {p: 0.0 for p in PLANETS}

    # All pairwise aspects become conjunctions with full strength.
    aspects = out["aspects"]
    assert len(aspects) == 45  # C(10,2)
    assert {a["aspect"] for a in aspects} == {"conjunction"}
    assert all(a["strength"] == 1.0 for a in aspects)
    assert all(a["deviation"] == 0.0 for a in aspects)
    assert all(a["angle"] == 0.0 for a in aspects)

    assert out["elem_ratios"] == {"fire": 1.0, "earth": 0.0, "air": 0.0, "water": 0.0}
    assert out["modality_ratios"] == {"cardinal": 1.0, "fixed": 0.0, "mutable": 0.0}

    expected_fv = {
        "lon_sun_sin": 0.0, "lon_sun_cos": 1.0,
        "lon_moon_sin": 0.0, "lon_moon_cos": 1.0,
        "lon_mercury_sin": 0.0, "lon_mercury_cos": 1.0,
        "lon_venus_sin": 0.0, "lon_venus_cos": 1.0,
        "lon_mars_sin": 0.0, "lon_mars_cos": 1.0,
        "lon_jupiter_sin": 0.0, "lon_jupiter_cos": 1.0,
        "lon_saturn_sin": 0.0, "lon_saturn_cos": 1.0,
        "lon_uranus_sin": 0.0, "lon_uranus_cos": 1.0,
        "lon_neptune_sin": 0.0, "lon_neptune_cos": 1.0,
        "lon_pluto_sin": 0.0, "lon_pluto_cos": 1.0,
        "aspect_strength_conjunction": 45.0,
        "aspect_strength_opposition": 0.0,
        "aspect_strength_trine": 0.0,
        "aspect_strength_square": 0.0,
        "aspect_strength_sextile": 0.0,
        "elem_fire": 1.0,
        "elem_earth": 0.0,
        "elem_air": 0.0,
        "elem_water": 0.0,
        "mod_cardinal": 1.0,
        "mod_fixed": 0.0,
        "mod_mutable": 0.0,
    }
    assert out["feature_vec"] == expected_fv


def test_wrap360_and_ang_distance():
    assert wrap360(370.0) == 10.0
    assert wrap360(-10.0) == 350.0
    assert ang_distance(10.0, 350.0) == 20.0


def test_sign_from_longitude_boundaries():
    sign, deg = sign_from_longitude(0.0)
    assert sign == "aries"
    assert deg == 0.0

    sign, deg = sign_from_longitude(29.9)
    assert sign == "aries"
    assert 29.0 < deg < 30.0

    sign, deg = sign_from_longitude(30.0)
    assert sign == "taurus"
    assert deg == 0.0


def test_elem_modality_tallies_all_aries():
    longs = {p: 0.0 for p in PLANETS}
    elems, mods = elem_modality_tallies(longs)
    assert elems == {"fire": 1.0, "earth": 0.0, "air": 0.0, "water": 0.0}
    assert mods == {"cardinal": 1.0, "fixed": 0.0, "mutable": 0.0}


def test_compute_aspects_simple_conjunction():
    longs = {"sun": 0.0, "moon": 5.0}
    aspects = compute_aspects(longs)
    assert aspects
    conj = next(a for a in aspects if a["aspect"] == "conjunction")
    assert {conj["a"], conj["b"]} == {"sun", "moon"}


def test_to_julday_utc_unknown_time():
    jd, unknown = to_julday_utc(dt.date(1984, 6, 12), None, 0)
    assert isinstance(jd, float)
    assert unknown is True
