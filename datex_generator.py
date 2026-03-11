#!/usr/bin/env python3
"""
DATEX II XML Generator - Budapest Közút / BKK forgalmi adatok
Verzió: 2.3, 3.2, 3.5
Forrás: https://kozut.bkkinfo.hu/api/changes
Futtatás: GitHub Actions, 5 percenként
"""

import requests
import json
import os
from datetime import datetime, timezone
from lxml import etree
import sys

API_URL = "https://kozut.bkkinfo.hu/api/changes"
OUTPUT_BASE = "Datex_allomanyok"

COUNTRY = "hu"
NATIONAL_IDENTIFIER = "HU"
PUBLISHER_ID = "BKK"
PUBLISHER_NAME = "Budapest Közút Zrt."

# ──────────────────────────────────────────────
# Segédfüggvények
# ──────────────────────────────────────────────

def fetch_data():
    """API lekérés"""
    try:
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # ha lista jön vissza közvetlenül, vagy dict-ben van
        if isinstance(data, list):
            return data
        for key in ("data", "changes", "results", "items"):
            if key in data:
                return data[key]
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[HIBA] API lekérés sikertelen: {e}", file=sys.stderr)
        return []


def parse_coordinates(coord_str):
    """'47.506444,19.151243' → (lat, lon)"""
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
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_date(dt_str):
    if not dt_str:
        return None
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return dt_str


def make_output_dir():
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    path = os.path.join(OUTPUT_BASE, ts)
    os.makedirs(path, exist_ok=True)
    return path


# ──────────────────────────────────────────────
# DATEX II 2.3
# ──────────────────────────────────────────────

def build_v23(records):
    NS = "http://datex2.eu/schema/2/2_0"
    XSI = "http://www.w3.org/2001/XMLSchema-instance"
    nsmap = {
        None: NS,
        "xsi": XSI,
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
    etree.SubElement(exchange, f"{{{NS}}}supplierIdentification").text = ""
    sup = etree.SubElement(exchange, f"{{{NS}}}supplierIdentification")
    etree.SubElement(sup, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(sup, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER
    etree.SubElement(exchange, f"{{{NS}}}subscriptionReference").text = PUBLISHER_ID
    etree.SubElement(exchange, f"{{{NS}}}timeDefault").text = now_iso()

    # payloadPublication
    pub = etree.SubElement(
        root,
        f"{{{NS}}}payloadPublication",
        attrib={
            f"{{{XSI}}}type": f"{{{NS}}}SituationPublication",
            "lang": "hu",
        },
    )
    etree.SubElement(pub, f"{{{NS}}}publicationTime").text = now_iso()

    mgmt = etree.SubElement(pub, f"{{{NS}}}publicationCreator")
    etree.SubElement(mgmt, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(mgmt, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        sit = etree.SubElement(pub, f"{{{NS}}}situation", attrib={"id": f"SIT_{rec['id']}", "version": "1"})
        sit_rec = etree.SubElement(sit, f"{{{NS}}}situationRecord",
                                   attrib={
                                       f"{{{XSI}}}type": f"{{{NS}}}Accident",
                                       "id": f"REC_{rec['id']}",
                                       "version": "1",
                                   })
        etree.SubElement(sit_rec, f"{{{NS}}}situationRecordCreationTime").text = now_iso()
        etree.SubElement(sit_rec, f"{{{NS}}}situationRecordVersionTime").text = now_iso()
        etree.SubElement(sit_rec, f"{{{NS}}}probabilityOfOccurrence").text = "certain"
        etree.SubElement(sit_rec, f"{{{NS}}}severity").text = "high" if rec.get("priority", 0) > 0 else "low"
        etree.SubElement(sit_rec, f"{{{NS}}}source").text = PUBLISHER_NAME

        # Validity
        validity = etree.SubElement(sit_rec, f"{{{NS}}}validity")
        etree.SubElement(validity, f"{{{NS}}}validityStatus").text = "active" if not rec.get("end_date") else "suspended"
        vp = etree.SubElement(validity, f"{{{NS}}}validityTimeSpecification")
        etree.SubElement(vp, f"{{{NS}}}overallStartTime").text = format_date(rec.get("start_date")) or now_iso()
        if rec.get("end_date"):
            etree.SubElement(vp, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

        # GroupOfLocations - effects alapján
        for eff in rec.get("effects", []):
            piv = eff.get("pivot", {})
            lat, lon = parse_coordinates(piv.get("coordinates", "[]"))
            if lat and lon:
                loc = etree.SubElement(sit_rec, f"{{{NS}}}groupOfLocations",
                                       attrib={f"{{{XSI}}}type": f"{{{NS}}}Point"})
                point = etree.SubElement(loc, f"{{{NS}}}locationForDisplay")
                etree.SubElement(point, f"{{{NS}}}latitude").text = str(lat)
                etree.SubElement(point, f"{{{NS}}}longitude").text = str(lon)

                street = piv.get("street", "")
                if street:
                    desc = etree.SubElement(sit_rec, f"{{{NS}}}locationDescriptor")
                    etree.SubElement(desc, f"{{{NS}}}value").text = street

            # cause
            for cause in rec.get("causes", []):
                cause_elem = etree.SubElement(sit_rec, f"{{{NS}}}cause")
                etree.SubElement(cause_elem, f"{{{NS}}}causeDescription").text = cause.get("name", cause.get("code", ""))

            break  # 1 effect per record ebben a sémában

    return root


# ──────────────────────────────────────────────
# DATEX II 3.2
# ──────────────────────────────────────────────

def build_v32(records):
    NS = "http://datex2.eu/schema/3/common"
    NS_SIT = "http://datex2.eu/schema/3/situation"
    NS_LOC = "http://datex2.eu/schema/3/locationReferencing"
    XSI = "http://www.w3.org/2001/XMLSchema-instance"

    nsmap = {
        None: NS,
        "sit": NS_SIT,
        "loc": NS_LOC,
        "xsi": XSI,
    }

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel",
        nsmap=nsmap,
        attrib={
            "modelBaseVersion": "3",
            "extensionName": "DATEX II 3.2",
            f"{{{XSI}}}schemaLocation": (
                "http://datex2.eu/schema/3/common "
                "http://datex2.eu/schema/3/DATEXIISchema_3_2.xsd"
            ),
        },
    )

    # Exchange
    exchange = etree.SubElement(root, f"{{{NS}}}exchange")
    sup = etree.SubElement(exchange, f"{{{NS}}}supplierIdentification")
    etree.SubElement(sup, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(sup, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER
    etree.SubElement(exchange, f"{{{NS}}}deliveryBreak").text = "false"
    etree.SubElement(exchange, f"{{{NS}}}keepAlive").text = "false"

    pub = etree.SubElement(
        root,
        f"{{{NS}}}payloadPublication",
        attrib={
            f"{{{XSI}}}type": f"{{{NS_SIT}}}SituationPublication",
            "lang": "hu",
        },
    )
    etree.SubElement(pub, f"{{{NS}}}publicationTime").text = now_iso()

    creator = etree.SubElement(pub, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        sit = etree.SubElement(
            pub,
            f"{{{NS_SIT}}}situation",
            attrib={"id": f"BKK_SIT_{rec['id']}", "version": "1"},
        )
        etree.SubElement(sit, f"{{{NS_SIT}}}situationVersionTime").text = now_iso()

        sit_rec = etree.SubElement(
            sit,
            f"{{{NS_SIT}}}situationRecord",
            attrib={
                f"{{{XSI}}}type": f"{{{NS_SIT}}}Accident",
                "id": f"BKK_REC_{rec['id']}",
                "version": "1",
            },
        )
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}creationTime").text = format_date(rec.get("start_date")) or now_iso()
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}versionTime").text = now_iso()
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}probabilityOfOccurrence").text = "certain"

        validity = etree.SubElement(sit_rec, f"{{{NS_SIT}}}validity")
        etree.SubElement(validity, f"{{{NS_SIT}}}validityStatus").text = "active" if not rec.get("end_date") else "suspended"
        vts = etree.SubElement(validity, f"{{{NS_SIT}}}validityTimeSpecification")
        etree.SubElement(vts, f"{{{NS}}}overallStartTime").text = format_date(rec.get("start_date")) or now_iso()
        if rec.get("end_date"):
            etree.SubElement(vts, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

        for eff in rec.get("effects", []):
            piv = eff.get("pivot", {})
            sub_id = piv.get("id", "")
            lat, lon = parse_coordinates(piv.get("coordinates", "[]"))

            sub = etree.SubElement(
                sit_rec,
                f"{{{NS_SIT}}}situationRecordExtension",
                attrib={"subRecordId": str(sub_id)},
            )
            etree.SubElement(sub, f"{{{NS_SIT}}}effectCode").text = eff.get("code", "")
            etree.SubElement(sub, f"{{{NS_SIT}}}effectName").text = eff.get("name", "")
            street = piv.get("street", "")
            if street:
                etree.SubElement(sub, f"{{{NS_SIT}}}streetName").text = street

            if lat and lon:
                loc = etree.SubElement(sit_rec, f"{{{NS_LOC}}}groupOfLocations",
                                       attrib={f"{{{XSI}}}type": f"{{{NS_LOC}}}PointLocation"})
                pt = etree.SubElement(loc, f"{{{NS_LOC}}}point")
                coord = etree.SubElement(pt, f"{{{NS_LOC}}}pointByCoordinates")
                etree.SubElement(coord, f"{{{NS_LOC}}}latitude").text = str(lat)
                etree.SubElement(coord, f"{{{NS_LOC}}}longitude").text = str(lon)

        for cause in rec.get("causes", []):
            cause_el = etree.SubElement(sit_rec, f"{{{NS_SIT}}}cause")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeType").text = cause.get("code", "")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeDescription").text = cause.get("name", "")

    return root


# ──────────────────────────────────────────────
# DATEX II 3.5
# ──────────────────────────────────────────────

def build_v35(records):
    NS = "http://datex2.eu/schema/3/common"
    NS_SIT = "http://datex2.eu/schema/3/situation"
    NS_LOC = "http://datex2.eu/schema/3/locationReferencing"
    NS_ROAD = "http://datex2.eu/schema/3/road"
    XSI = "http://www.w3.org/2001/XMLSchema-instance"

    nsmap = {
        None: NS,
        "sit": NS_SIT,
        "loc": NS_LOC,
        "road": NS_ROAD,
        "xsi": XSI,
    }

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel",
        nsmap=nsmap,
        attrib={
            "modelBaseVersion": "3",
            "extensionName": "DATEX II 3.5",
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

    pub = etree.SubElement(
        root,
        f"{{{NS}}}payloadPublication",
        attrib={
            f"{{{XSI}}}type": f"{{{NS_SIT}}}SituationPublication",
            "lang": "hu",
        },
    )
    etree.SubElement(pub, f"{{{NS}}}publicationTime").text = now_iso()

    creator = etree.SubElement(pub, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        sit = etree.SubElement(
            pub,
            f"{{{NS_SIT}}}situation",
            attrib={"id": f"BKK_SIT_{rec['id']}", "version": "1"},
        )
        etree.SubElement(sit, f"{{{NS_SIT}}}situationVersionTime").text = now_iso()
        etree.SubElement(sit, f"{{{NS_SIT}}}situationSource").text = PUBLISHER_NAME

        sit_rec = etree.SubElement(
            sit,
            f"{{{NS_SIT}}}situationRecord",
            attrib={
                f"{{{XSI}}}type": f"{{{NS_SIT}}}Accident",
                "id": f"BKK_REC_{rec['id']}",
                "version": "1",
            },
        )
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}creationTime").text = format_date(rec.get("start_date")) or now_iso()
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}versionTime").text = now_iso()
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}probabilityOfOccurrence").text = "certain"
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}severity").text = "high" if rec.get("priority", 0) > 0 else "low"

        validity = etree.SubElement(sit_rec, f"{{{NS_SIT}}}validity")
        etree.SubElement(validity, f"{{{NS_SIT}}}validityStatus").text = (
            "active" if not rec.get("end_date") else "suspended"
        )
        vts = etree.SubElement(validity, f"{{{NS_SIT}}}validityTimeSpecification")
        etree.SubElement(vts, f"{{{NS}}}overallStartTime").text = format_date(rec.get("start_date")) or now_iso()
        if rec.get("end_date"):
            etree.SubElement(vts, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

        for eff in rec.get("effects", []):
            piv = eff.get("pivot", {})
            sub_id = piv.get("id", "")
            change_id = piv.get("change_id", rec["id"])
            lat, lon = parse_coordinates(piv.get("coordinates", "[]"))
            street = piv.get("street", "")

            sub = etree.SubElement(
                sit_rec,
                f"{{{NS_SIT}}}networkManagement",
                attrib={
                    "subRecordId": str(sub_id),
                    "parentChangeId": str(change_id),
                },
            )
            etree.SubElement(sub, f"{{{NS_SIT}}}effectCode").text = eff.get("code", "")
            etree.SubElement(sub, f"{{{NS_SIT}}}effectName").text = eff.get("name", "")
            if street:
                road = etree.SubElement(sub, f"{{{NS_ROAD}}}road")
                etree.SubElement(road, f"{{{NS_ROAD}}}roadName").text = street

            if lat and lon:
                loc = etree.SubElement(
                    sit_rec,
                    f"{{{NS_LOC}}}groupOfLocations",
                    attrib={f"{{{XSI}}}type": f"{{{NS_LOC}}}PointLocation"},
                )
                pt = etree.SubElement(loc, f"{{{NS_LOC}}}point")
                coord = etree.SubElement(pt, f"{{{NS_LOC}}}pointByCoordinates")
                gdc = etree.SubElement(coord, f"{{{NS_LOC}}}pointCoordinates")
                etree.SubElement(gdc, f"{{{NS_LOC}}}latitude").text = str(lat)
                etree.SubElement(gdc, f"{{{NS_LOC}}}longitude").text = str(lon)

        for cause in rec.get("causes", []):
            cause_el = etree.SubElement(sit_rec, f"{{{NS_SIT}}}cause")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeType").text = cause.get("code", "")
            etree.SubElement(cause_el, f"{{{NS_SIT}}}causeDescription").text = cause.get("name", "")

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
    print(f"[{now_iso()}] DATEX II XML generálás indul...")

    records = fetch_data()
    print(f"  → {len(records)} rekord lekérve az API-ról")

    out_dir = make_output_dir()
    print(f"  → Kimeneti mappa: {out_dir}")

    # 2.3
    root_23 = build_v23(records)
    save_xml(root_23, os.path.join(out_dir, "datex_v23.xml"))

    # 3.2
    root_32 = build_v32(records)
    save_xml(root_32, os.path.join(out_dir, "datex_v32.xml"))

    # 3.5
    root_35 = build_v35(records)
    save_xml(root_35, os.path.join(out_dir, "datex_v35.xml"))

    print(f"[{now_iso()}] Kész. 3 XML fájl létrehozva: {out_dir}")


if __name__ == "__main__":
    main()
