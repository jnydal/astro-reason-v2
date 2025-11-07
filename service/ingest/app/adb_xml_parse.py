# app/cli/adb_xml_parse.py
import argparse, pathlib, xml.etree.ElementTree as ET
import psycopg2, psycopg2.extras

NS = {}  # fill if the XML uses namespaces

def parse_person(elem):
    # Adjust tags to match your ADB XML; these are typical placeholders
    adb_id = elem.get('id') or elem.findtext('id')
    full_name = elem.findtext('name') or elem.findtext('fullname')
    date = elem.findtext('birth/date')
    time = elem.findtext('birth/time')  # may be None
    tz = elem.findtext('birth/tz')      # "+01:00", "−05:00", etc.
    place = elem.findtext('birth/place/name')
    lat = elem.findtext('birth/place/lat')
    lon = elem.findtext('birth/place/lon')
    rating = elem.findtext('birth/rodden_rating') or elem.findtext('birth/rating')

    return {
        "adb_id": adb_id,
        "full_name": full_name,
        "date": date,
        "time": time,
        "tz": tz,
        "place": place,
        "lat": float(lat) if lat else None,
        "lon": float(lon) if lon else None,
        "rating": rating
    }

def parse_and_insert(xml_path, conn):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    with conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        for person in root.findall('.//person', NS):
            rec = parse_person(person)
            if not rec["adb_id"] or not rec["full_name"]:
                continue

            cur.execute("""
                INSERT INTO person_raw (adb_id, full_name, adb_xml_path)
                VALUES (%s, %s, %s)
                ON CONFLICT (adb_id) DO UPDATE SET full_name = EXCLUDED.full_name
                RETURNING person_id
            """, (rec["adb_id"], rec["full_name"], str(xml_path)))

            person_id = cur.fetchone()[0]

            # parse tz → minutes
            tz_mins = None
            if rec["tz"]:
                sign = -1 if "-" in rec["tz"] or "−" in rec["tz"] else 1
                h, m = rec["tz"].replace("+", "").replace("-", "").replace("−", "").split(":")
                tz_mins = sign * (int(h)*60 + int(m))

            cur.execute("""
                INSERT INTO birth (person_id, date, time, tz_offset_minutes, place_name, lat, lon, data_quality)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (person_id) DO UPDATE
                  SET date = EXCLUDED.date,
                      time = EXCLUDED.time,
                      tz_offset_minutes = EXCLUDED.tz_offset_minutes,
                      place_name = EXCLUDED.place_name,
                      lat = EXCLUDED.lat,
                      lon = EXCLUDED.lon,
                      data_quality = EXCLUDED.data_quality
            """, (person_id, rec["date"], rec["time"], tz_mins, rec["place"], rec["lat"], rec["lon"], rec["rating"]))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml", required=True, type=pathlib.Path)
    ap.add_argument("--dsn", required=True, help="postgres DSN, e.g. postgresql://user:pass@localhost:5432/db")
    args = ap.parse_args()

    conn = psycopg2.connect(args.dsn)
    parse_and_insert(args.xml, conn)
    conn.close()

if __name__ == "__main__":
    main()
