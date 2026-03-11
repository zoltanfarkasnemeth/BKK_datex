#!/usr/bin/env python3
"""
DATEX II XML Generator - Budapest Közút / BKK forgalmi adatok
Verzió: 2.3, 3.2, 3.5
Forrás: https://kozut.bkkinfo.hu/api/changes
Futtatás: GitHub Actions, 5 percenként

Javítások (v2):
  - Időbélyeg-konzisztencia: creationTime = start_date (API), versionTime = pub_time
  - situationVersionTime = pub_time (nem later mint creationTime)
  - situationRecordExtension → standard _extension mechanizmus (BKK namespace)
  - accidentType mező hozzáadva (causes.code alapján)
  - headerInformation blokk hozzáadva (confidentiality, informationStatus)
  - priority None-safe összehasonlítás
  - Kimeneti mappa: Datex/
"""

import requests
import json
import os
from datetime import datetime, timezone
from lxml import etree
import sys

API_URL     = "https://kozut.bkkinfo.hu/api/changes"
OUTPUT_BASE = "Datex_allomanyok"

COUNTRY             = "hu"
NATIONAL_IDENTIFIER = "HU"
PUBLISHER_ID        = "BKK"
PUBLISHER_NAME      = "Budapest Közút Zrt."

# BKK extension namespace (saját, nem standard mezőkhöz)
NS_BKK = "http://bkkinfo.hu/datex2/extension/1_0"

# cause kód → DATEX II accidentType mapping
ACCIDENT_TYPE_MAP = {
    "baleset":              "collision",
    "torlodas":             "collision",
    "lezaras":              "roadClosed",
    "utlezaras":            "roadClosed",
    "forgalomkorlatozas":   "obstacleOnRoad",
    "akadaly":              "obstacleOnRoad",
}
ACCIDENT_TYPE_DEFAULT = "accident"


# ──────────────────────────────────────────────
# Segédfüggvények
# ──────────────────────────────────────────────

def fetch_data():
    """API lekérés"""
    try:
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        for key in ("data", "changes", "results", "items"):
            if key in data:
                return data[key]
        return []
    except Exception as e:
        print(f"[HIBA] API lekérés sikertelen: {e}", file=sys.stderr)
        return []


def parse_coordinates(coord_str):
    """'[\"47.506444,19.151243\"]' -> (lat, lon)"""
    try:
        coords = json.loads(coord_str) if isinstance(coord_str, str) else coord_str
        if isinstance(coords, list) and coords:
            parts = str(coords[0]).split(",")
            if len(parts) >= 2:
                return float(parts[0].strip()), float(parts[1].strip())
    except Exception:
        pass
    return None, None


def now_iso():
    """Aktuális UTC idő ISO 8601 formátumban"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_date(dt_str):
    """API dátum string -> ISO 8601"""
    if not dt_str:
        return None
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return dt_str


def get_accident_type(rec):
    """cause code alapján DATEX II accidentType értéke"""
    for cause in rec.get("causes", []):
        code = (cause.get("code") or "").lower().replace(" ", "").replace("á", "a").replace("ú", "u")
        if code in ACCIDENT_TYPE_MAP:
            return ACCIDENT_TYPE_MAP[code]
    return ACCIDENT_TYPE_DEFAULT


def max_time(t1_iso, t2_iso):
    """Visszaadja a két ISO timestamp közül a késõbbit (versionTime >= creationTime garantáláshoz)"""
    try:
        dt1 = datetime.strptime(t1_iso, "%Y-%m-%dT%H:%M:%SZ")
        dt2 = datetime.strptime(t2_iso, "%Y-%m-%dT%H:%M:%SZ")
        return t1_iso if dt1 >= dt2 else t2_iso
    except Exception:
        return t1_iso


def get_severity(rec):
    """priority -> DATEX II severity"""
    p = rec.get("priority") or 0
    if p >= 2:
        return "highest"
    elif p == 1:
        return "high"
    else:
        return "low"


def make_output_dir():
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    path = os.path.join(OUTPUT_BASE, ts)
    os.makedirs(path, exist_ok=True)
    return path


# ──────────────────────────────────────────────
# DATEX II 2.3
# ──────────────────────────────────────────────

def build_v23(records, pub_time):
    NS  = "http://datex2.eu/schema/2/2_0"
    XSI = "http://www.w3.org/2001/XMLSchema-instance"
    nsmap = {
        None:  NS,
        "xsi": XSI,
        "bkk": NS_BKK,
    }

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel",
        nsmap=nsmap,
        attrib={
            "modelBaseVersion": "2",
            f"{{{XSI}}}schemaLocation": (
                "http://datex2.eu/schema/2/2_0 "
                "http://datex2.eu/schema/2/2_0/DATEXIISchema_2_0.xsd"
            ),
        },
    )

    # Exchange
    exchange = etree.SubElement(root, f"{{{NS}}}exchange")
    sup = etree.SubElement(exchange, f"{{{NS}}}supplierIdentification")
    etree.SubElement(sup, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(sup, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER
    etree.SubElement(exchange, f"{{{NS}}}subscriptionReference").text = PUBLISHER_ID
    etree.SubElement(exchange, f"{{{NS}}}timeDefault").text = pub_time

    # payloadPublication
    pub_el = etree.SubElement(
        root, f"{{{NS}}}payloadPublication",
        attrib={
            f"{{{XSI}}}type": f"{{{NS}}}SituationPublication",
            "lang": "hu",
        },
    )
    etree.SubElement(pub_el, f"{{{NS}}}publicationTime").text = pub_time

    creator = etree.SubElement(pub_el, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        # creationTime = az esemény keletkezési ideje (API start_date)
        # versionTime  = publikáció ideje (pub_time)
        # → creationTime <= versionTime garantált
        creation_time = format_date(rec.get("start_date")) or pub_time

        sit = etree.SubElement(
            pub_el, f"{{{NS}}}situation",
            attrib={"id": f"BKK_SIT_{rec['id']}", "version": "1"}
        )
        sit_rec = etree.SubElement(
            sit, f"{{{NS}}}situationRecord",
            attrib={
                f"{{{XSI}}}type": f"{{{NS}}}Accident",
                "id": f"BKK_REC_{rec['id']}",
                "version": "1",
            }
        )

        # Időbélyegek – konzisztens: versionTime >= creationTime garantált
        version_time = max_time(pub_time, creation_time)
        etree.SubElement(sit_rec, f"{{{NS}}}situationRecordCreationTime").text = creation_time
        etree.SubElement(sit_rec, f"{{{NS}}}situationRecordVersionTime").text  = version_time

        # headerInformation (ajánlott)
        hdr = etree.SubElement(sit_rec, f"{{{NS}}}headerInformation")
        etree.SubElement(hdr, f"{{{NS}}}confidentiality").text   = "noRestriction"
        etree.SubElement(hdr, f"{{{NS}}}informationStatus").text = "real"

        etree.SubElement(sit_rec, f"{{{NS}}}probabilityOfOccurrence").text = "certain"
        etree.SubElement(sit_rec, f"{{{NS}}}severity").text = get_severity(rec)

        # Validity
        validity = etree.SubElement(sit_rec, f"{{{NS}}}validity")
        etree.SubElement(validity, f"{{{NS}}}validityStatus").text = (
            "active" if not rec.get("end_date") else "suspended"
        )
        vp = etree.SubElement(validity, f"{{{NS}}}validityTimeSpecification")
        etree.SubElement(vp, f"{{{NS}}}overallStartTime").text = creation_time
        if rec.get("end_date"):
            etree.SubElement(vp, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

        # accidentType (kötelező Accident-hez)
        etree.SubElement(sit_rec, f"{{{NS}}}accidentType").text = get_accident_type(rec)

        # Lokáció + BKK extension
        for eff in rec.get("effects", []):
            piv = eff.get("pivot", {})
            lat, lon = parse_coordinates(piv.get("coordinates", "[]"))
            if lat and lon:
                loc = etree.SubElement(
                    sit_rec, f"{{{NS}}}groupOfLocations",
                    attrib={f"{{{XSI}}}type": f"{{{NS}}}Point"}
                )
                point = etree.SubElement(loc, f"{{{NS}}}locationForDisplay")
                etree.SubElement(point, f"{{{NS}}}latitude").text  = str(lat)
                etree.SubElement(point, f"{{{NS}}}longitude").text = str(lon)
                street = piv.get("street", "")
                if street:
                    desc = etree.SubElement(sit_rec, f"{{{NS}}}locationDescriptor")
                    etree.SubElement(desc, f"{{{NS}}}value").text = street

            # _extension végű tag = standard DATEX II extensibility mechanizmus
            ext = etree.SubElement(sit_rec, f"{{{NS}}}situationRecord_extension")
            bkk = etree.SubElement(ext, f"{{{NS_BKK}}}bkkEffectInfo")
            etree.SubElement(bkk, f"{{{NS_BKK}}}subRecordId").text = str(piv.get("id", ""))
            etree.SubElement(bkk, f"{{{NS_BKK}}}changeId").text    = str(piv.get("change_id", rec["id"]))
            etree.SubElement(bkk, f"{{{NS_BKK}}}effectCode").text  = eff.get("code", "")
            etree.SubElement(bkk, f"{{{NS_BKK}}}effectName").text  = eff.get("name", "")
            etree.SubElement(bkk, f"{{{NS_BKK}}}streetName").text  = piv.get("street", "")
            break  # 1 fő lokáció per record

        # Causes
        for cause in rec.get("causes", []):
            cause_elem = etree.SubElement(sit_rec, f"{{{NS}}}cause")
            etree.SubElement(cause_elem, f"{{{NS}}}causeDescription").text = (
                cause.get("name") or cause.get("code", "")
            )

    return root


# ──────────────────────────────────────────────
# DATEX II 3.2
# ──────────────────────────────────────────────

def build_v32(records, pub_time):
    NS     = "http://datex2.eu/schema/3/common"
    NS_SIT = "http://datex2.eu/schema/3/situation"
    NS_LOC = "http://datex2.eu/schema/3/locationReferencing"
    XSI    = "http://www.w3.org/2001/XMLSchema-instance"

    nsmap = {
        None:  NS,
        "sit": NS_SIT,
        "loc": NS_LOC,
        "bkk": NS_BKK,
        "xsi": XSI,
    }

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel",
        nsmap=nsmap,
        attrib={
            "modelBaseVersion": "3",
            f"{{{XSI}}}schemaLocation": (
                "http://datex2.eu/schema/3/common "
                "http://datex2.eu/schema/3/DATEXIISchema_3_2.xsd"
            ),
        },
    )

    exchange = etree.SubElement(root, f"{{{NS}}}exchange")
    sup = etree.SubElement(exchange, f"{{{NS}}}supplierIdentification")
    etree.SubElement(sup, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(sup, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    pub_el = etree.SubElement(
        root, f"{{{NS}}}payloadPublication",
        attrib={
            f"{{{XSI}}}type": f"{{{NS_SIT}}}SituationPublication",
            "lang": "hu",
        },
    )
    etree.SubElement(pub_el, f"{{{NS}}}publicationTime").text = pub_time

    creator = etree.SubElement(pub_el, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        creation_time = format_date(rec.get("start_date")) or pub_time

        sit = etree.SubElement(
            pub_el, f"{{{NS_SIT}}}situation",
            attrib={"id": f"BKK_SIT_{rec['id']}", "version": "1"},
        )
        # situationVersionTime = publikáció ideje (>= creation_time)
        version_time = max_time(pub_time, creation_time)
        etree.SubElement(sit, f"{{{NS_SIT}}}situationVersionTime").text = version_time

        sit_rec = etree.SubElement(
            sit, f"{{{NS_SIT}}}situationRecord",
            attrib={
                f"{{{XSI}}}type": f"{{{NS_SIT}}}Accident",
                "id": f"BKK_REC_{rec['id']}",
                "version": "1",
            },
        )

        # Időbélyegek
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}creationTime").text = creation_time
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}versionTime").text  = version_time

        # headerInformation
        hdr = etree.SubElement(sit_rec, f"{{{NS_SIT}}}headerInformation")
        etree.SubElement(hdr, f"{{{NS_SIT}}}confidentiality").text   = "noRestriction"
        etree.SubElement(hdr, f"{{{NS_SIT}}}informationStatus").text = "real"

        etree.SubElement(sit_rec, f"{{{NS_SIT}}}probabilityOfOccurrence").text = "certain"

        # accidentType
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}accidentType").text = get_accident_type(rec)

        # Validity
        validity = etree.SubElement(sit_rec, f"{{{NS_SIT}}}validity")
        etree.SubElement(validity, f"{{{NS_SIT}}}validityStatus").text = (
            "active" if not rec.get("end_date") else "suspended"
        )
        vts = etree.SubElement(validity, f"{{{NS_SIT}}}validityTimeSpecification")
        etree.SubElement(vts, f"{{{NS}}}overallStartTime").text = creation_time
        if rec.get("end_date"):
            etree.SubElement(vts, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

        # Lokáció + BKK extension
        for eff in rec.get("effects", []):
            piv = eff.get("pivot", {})
            lat, lon = parse_coordinates(piv.get("coordinates", "[]"))

            if lat and lon:
                loc = etree.SubElement(
                    sit_rec, f"{{{NS_LOC}}}groupOfLocations",
                    attrib={f"{{{XSI}}}type": f"{{{NS_LOC}}}PointLocation"},
                )
                pt    = etree.SubElement(loc, f"{{{NS_LOC}}}point")
                coord = etree.SubElement(pt,  f"{{{NS_LOC}}}pointByCoordinates")
                etree.SubElement(coord, f"{{{NS_LOC}}}latitude").text  = str(lat)
                etree.SubElement(coord, f"{{{NS_LOC}}}longitude").text = str(lon)

            # standard _extension mechanizmus
            ext = etree.SubElement(sit_rec, f"{{{NS_SIT}}}situationRecord_extension")
            bkk = etree.SubElement(ext, f"{{{NS_BKK}}}bkkEffectInfo")
            etree.SubElement(bkk, f"{{{NS_BKK}}}subRecordId").text = str(piv.get("id", ""))
            etree.SubElement(bkk, f"{{{NS_BKK}}}changeId").text    = str(piv.get("change_id", rec["id"]))
            etree.SubElement(bkk, f"{{{NS_BKK}}}effectCode").text  = eff.get("code", "")
            etree.SubElement(bkk, f"{{{NS_BKK}}}effectName").text  = eff.get("name", "")
            etree.SubElement(bkk, f"{{{NS_BKK}}}streetName").text  = piv.get("street", "")

        # Causes
        for cause in rec.get("causes", []):
            cause_el = etree.SubElement(sit_rec, f"{{{NS_SIT}}}cause")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeType").text        = cause.get("code", "")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeDescription").text = (
                cause.get("name") or cause.get("code", "")
            )

    return root


# ──────────────────────────────────────────────
# DATEX II 3.5
# ──────────────────────────────────────────────

def build_v35(records, pub_time):
    NS      = "http://datex2.eu/schema/3/common"
    NS_SIT  = "http://datex2.eu/schema/3/situation"
    NS_LOC  = "http://datex2.eu/schema/3/locationReferencing"
    NS_ROAD = "http://datex2.eu/schema/3/road"
    XSI     = "http://www.w3.org/2001/XMLSchema-instance"

    nsmap = {
        None:   NS,
        "sit":  NS_SIT,
        "loc":  NS_LOC,
        "road": NS_ROAD,
        "bkk":  NS_BKK,
        "xsi":  XSI,
    }

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel",
        nsmap=nsmap,
        attrib={
            "modelBaseVersion": "3",
            f"{{{XSI}}}schemaLocation": (
                "http://datex2.eu/schema/3/common "
                "http://datex2.eu/schema/3/DATEXIISchema_3_5.xsd"
            ),
        },
    )

    exchange = etree.SubElement(root, f"{{{NS}}}exchange")
    sup = etree.SubElement(exchange, f"{{{NS}}}supplierIdentification")
    etree.SubElement(sup, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(sup, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    pub_el = etree.SubElement(
        root, f"{{{NS}}}payloadPublication",
        attrib={
            f"{{{XSI}}}type": f"{{{NS_SIT}}}SituationPublication",
            "lang": "hu",
        },
    )
    etree.SubElement(pub_el, f"{{{NS}}}publicationTime").text = pub_time

    creator = etree.SubElement(pub_el, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        creation_time = format_date(rec.get("start_date")) or pub_time

        sit = etree.SubElement(
            pub_el, f"{{{NS_SIT}}}situation",
            attrib={"id": f"BKK_SIT_{rec['id']}", "version": "1"},
        )
        # situationVersionTime = publikáció ideje (>= creation_time)
        version_time = max_time(pub_time, creation_time)
        etree.SubElement(sit, f"{{{NS_SIT}}}situationVersionTime").text = version_time
        etree.SubElement(sit, f"{{{NS_SIT}}}situationSource").text = PUBLISHER_NAME

        sit_rec = etree.SubElement(
            sit, f"{{{NS_SIT}}}situationRecord",
            attrib={
                f"{{{XSI}}}type": f"{{{NS_SIT}}}Accident",
                "id": f"BKK_REC_{rec['id']}",
                "version": "1",
            },
        )

        # Időbélyegek
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}creationTime").text = creation_time
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}versionTime").text  = version_time

        # headerInformation
        hdr = etree.SubElement(sit_rec, f"{{{NS_SIT}}}headerInformation")
        etree.SubElement(hdr, f"{{{NS_SIT}}}confidentiality").text   = "noRestriction"
        etree.SubElement(hdr, f"{{{NS_SIT}}}informationStatus").text = "real"

        etree.SubElement(sit_rec, f"{{{NS_SIT}}}probabilityOfOccurrence").text = "certain"
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}severity").text = get_severity(rec)

        # accidentType
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}accidentType").text = get_accident_type(rec)

        # Validity
        validity = etree.SubElement(sit_rec, f"{{{NS_SIT}}}validity")
        etree.SubElement(validity, f"{{{NS_SIT}}}validityStatus").text = (
            "active" if not rec.get("end_date") else "suspended"
        )
        vts = etree.SubElement(validity, f"{{{NS_SIT}}}validityTimeSpecification")
        etree.SubElement(vts, f"{{{NS}}}overallStartTime").text = creation_time
        if rec.get("end_date"):
            etree.SubElement(vts, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

        # Lokáció + utcanév + BKK extension
        for eff in rec.get("effects", []):
            piv    = eff.get("pivot", {})
            lat, lon = parse_coordinates(piv.get("coordinates", "[]"))
            street = piv.get("street", "")

            if lat and lon:
                loc = etree.SubElement(
                    sit_rec, f"{{{NS_LOC}}}groupOfLocations",
                    attrib={f"{{{XSI}}}type": f"{{{NS_LOC}}}PointLocation"},
                )
                pt    = etree.SubElement(loc, f"{{{NS_LOC}}}point")
                coord = etree.SubElement(pt,  f"{{{NS_LOC}}}pointByCoordinates")
                gdc   = etree.SubElement(coord, f"{{{NS_LOC}}}pointCoordinates")
                etree.SubElement(gdc, f"{{{NS_LOC}}}latitude").text  = str(lat)
                etree.SubElement(gdc, f"{{{NS_LOC}}}longitude").text = str(lon)

                if street:
                    road_ref = etree.SubElement(loc, f"{{{NS_ROAD}}}roadInformation")
                    etree.SubElement(road_ref, f"{{{NS_ROAD}}}roadName").text = street

            # standard _extension mechanizmus
            ext = etree.SubElement(sit_rec, f"{{{NS_SIT}}}situationRecord_extension")
            bkk = etree.SubElement(ext, f"{{{NS_BKK}}}bkkEffectInfo")
            etree.SubElement(bkk, f"{{{NS_BKK}}}subRecordId").text = str(piv.get("id", ""))
            etree.SubElement(bkk, f"{{{NS_BKK}}}changeId").text    = str(piv.get("change_id", rec["id"]))
            etree.SubElement(bkk, f"{{{NS_BKK}}}effectCode").text  = eff.get("code", "")
            etree.SubElement(bkk, f"{{{NS_BKK}}}effectName").text  = eff.get("name", "")
            etree.SubElement(bkk, f"{{{NS_BKK}}}streetName").text  = street

        # Causes
        for cause in rec.get("causes", []):
            cause_el = etree.SubElement(sit_rec, f"{{{NS_SIT}}}cause")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeType").text        = cause.get("code", "")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeDescription").text = (
                cause.get("name") or cause.get("code", "")
            )

    return root


# ──────────────────────────────────────────────
# XML mentés
# ──────────────────────────────────────────────

def save_xml(element, path):
    tree = etree.ElementTree(element)
    with open(path, "wb") as f:
        tree.write(f, xml_declaration=True, encoding="UTF-8", pretty_print=True)
    print(f"  ✓ Mentve: {path}")


# ──────────────────────────────────────────────
# Fő futás
# ──────────────────────────────────────────────

def main():
    # Egyetlen pub_time minden fájlhoz és minden rekordhoz – konzisztens
    pub_time = now_iso()

    print(f"[{pub_time}] DATEX II XML generálás indul...")

    records = fetch_data()
    print(f"  -> {len(records)} rekord lekérve az API-ról")

    out_dir = make_output_dir()
    print(f"  -> Kimeneti mappa: {out_dir}")

    save_xml(build_v23(records, pub_time), os.path.join(out_dir, "datex_v23.xml"))
    save_xml(build_v32(records, pub_time), os.path.join(out_dir, "datex_v32.xml"))
    save_xml(build_v35(records, pub_time), os.path.join(out_dir, "datex_v35.xml"))

    print(f"[{pub_time}] Kész. 3 XML fájl létrehozva: {out_dir}")


if __name__ == "__main__":
    main()
