# app/utils/adb_parser_stream.py
from typing import Iterator, Dict, Optional
from lxml import etree  # pip install lxml

def parse_person(elem) -> Optional[Dict]:
    # Adjust tag names / namespaces to your XML
    adb_id    = elem.get('id') or elem.findtext('id')
    full_name = elem.findtext('name') or elem.findtext('fullname')
    if not adb_id or not full_name:
        return None

    date   = elem.findtext('birth/date')
    time   = elem.findtext('birth/time')
    tz     = elem.findtext('birth/tz')
    place  = elem.findtext('birth/place/name')
    lat    = elem.findtext('birth/place/lat')
    lon    = elem.findtext('birth/place/lon')
    rating = elem.findtext('birth/rodden_rating') or elem.findtext('birth/rating')

    # If your XML has a biography/bio field:
    bio = elem.findtext('bio') or elem.findtext('biography')

    rec = {
        "adb_id": adb_id,
        "full_name": full_name,
        "date": date, "time": time, "tz": tz,
        "place": place,
        "lat": float(lat) if lat else None,
        "lon": float(lon) if lon else None,
        "rating": rating,
        "bio_text": bio.strip() if bio else None,
    }
    return rec

def iter_people(xml_path: str) -> Iterator[Dict]:
    # Safe, non-expanding, streaming parser
    parser = etree.XMLParser(resolve_entities=False, huge_tree=False, recover=True)
    for _, elem in etree.iterparse(xml_path, events=('end',), tag='person', parser=parser):
        rec = parse_person(elem)
        if rec:
            yield rec
        # free memory: drop children & siblings already processed
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
