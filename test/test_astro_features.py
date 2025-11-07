import datetime as dt
import types

from app.workers.astro_features import compute_features_for_person, PLANETS

def test_compute_features_minimal(monkeypatch):
    # Fake longitudes (spread them around the zodiac deterministically)
    fake_longs = {p: (i * 33.3) % 360 for i, p in enumerate(PLANETS)}

    # Patch the skyfield_longitudes function used by the 'skyfield' backend
    import app.workers.astro_features as af
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
