#!/usr/bin/env python3
"""
DATEX II XML Generator - Budapest Közút / BKK forgalmi adatok
Verzió: 2.3, 3.2, 3.5
Forrás: https://kozut.bkkinfo.hu/api/changes
Futtatás: GitHub Actions, 5 percenként

Javítások (v3) - validáció alapján feltárt hibák:
  [FIX-T1]  publicationTime = pub_time (generálás pillanata), SOHA nem korábbi cache-elt érték
             → creationTime / versionTime / situationVersionTime mind <= pub_time kell legyen
             → ha az API start_date jövőbeli lenne, pub_time-ra clampeljük
  [FIX-T2]  versionTime = max(creation_time, pub_time) helyett:
             versionTime = pub_time (a rekord legutóbbi publikálásakor érvényes verzióidő)
             situationVersionTime = pub_time (konzisztens a versionTime-mal)
  [FIX-O1]  <cause> MINDIG az <_extension> ELÖTT generálódik (2.x és 3.x XSD sequence)
  [FIX-C1]  <sit:causeType> ELTÁVOLÍTVA build_v32-ből és build_v35-ből:
             nem standard DATEX II elem (sem 3.2, sem 3.5 Cause osztályban nem szerepel)
             → csak causeDescription marad
  [FIX-V1]  validityStatus: end_date esetén "suspended" helyett "active" + overallEndTime
             (suspended = ideiglenesen szünetelő, nem = tervezett befejezésű aktív esemény)
  [FIX-L1]  DATEX II 3.2: loc:pointByCoordinates alatt NINCS loc:pointCoordinates burokelem
             → közvetlen loc:latitude + loc:longitude a pointByCoordinates alatt
  [FIX-A1]  ACCIDENT_TYPE_MAP: "roadClosed" és "obstacleOnRoad" nem érvényes accidentType enum
             → csak az Accident osztály érvényes értékeit használjuk
  [FIX-S1]  get_severity: priority=None safe + medium szint hozzáadva
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

NS_BKK = "http://bkkinfo.hu/datex2/extension/1_0"

# [FIX-A1] Csak érvényes DATEX II Accident xsi:type enum értékek:
# accident | collision | overturnedVehicle | jackknifedArticulatedLorry |
# damagedVehicle | vehicleOnFire | multipleVehicleAccident | other
ACCIDENT_TYPE_MAP = {
    "baleset":            "collision",
    "torlodas":           "accident",
    "lezaras":            "accident",
    "utlezaras":          "accident",
    "forgalomkorlatozas": "accident",
    "akadaly":            "accident",
    "utkozos":            "collision",
    "felborulas":         "overturnedVehicle",
    "tuzesets":           "vehicleOnFire",
    "tomegbaleset":       "multipleVehicleAccident",
}
ACCIDENT_TYPE_DEFAULT = "accident"


# ──────────────────────────────────────────────
# Segédfüggvények
# ──────────────────────────────────────────────

def fetch_data():
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


def get_accident_type(rec):
    for cause in rec.get("causes", []):
        code = (cause.get("code") or "").lower().strip()
        # Normalizálás: ékezetek és szóközök eltávolítása
        code_norm = (code.replace("á","a").replace("é","e").replace("í","i")
                        .replace("ó","o").replace("ö","o").replace("ő","o")
                        .replace("ú","u").replace("ü","u").replace("ű","u")
                        .replace(" ",""))
        if code_norm in ACCIDENT_TYPE_MAP:
            return ACCIDENT_TYPE_MAP[code_norm]
    return ACCIDENT_TYPE_DEFAULT


# [FIX-T1] creation_time clamp: ha az API jövőbeli időt adna vissza,
# pub_time-ra rögzítjük → creationTime sosem lehet jövőbeli a publikációhoz képest
def safe_creation_time(start_date_str, pub_time):
    """
    Visszaad egy ISO timestamp-et, ami garantáltan <= pub_time.
    Ha az API start_date jövőbeli lenne (pl. szinkronizációs hiba),
    pub_time-t adjuk vissza.
    """
    ct = format_date(start_date_str) or pub_time
    try:
        dt_ct  = datetime.strptime(ct,       "%Y-%m-%dT%H:%M:%SZ")
        dt_pub = datetime.strptime(pub_time,  "%Y-%m-%dT%H:%M:%SZ")
        if dt_ct > dt_pub:
            # [FIX-T1] jövőbeli creationTime → pub_time-ra clampeljük
            return pub_time
        return ct
    except Exception:
        return pub_time


# [FIX-S1] Kibővített severity mapping, None-safe
def get_severity(rec):
    p = rec.get("priority")
    if p is None:
        return "low"
    try:
        p = int(p)
    except (ValueError, TypeError):
        return "low"
    if p >= 3:
        return "highest"
    elif p == 2:
        return "high"
    elif p == 1:
        return "medium"
    else:
        return "low"


# [FIX-V1] validityStatus helyes meghatározása:
# - Nincs end_date → "active"
# - Van end_date, esemény még folyamatban → "active" + overallEndTime
# - "suspended" CSAK akkor, ha az esemény ténylegesen szünetel (külön státusz mező kellene)
# → a generátorban end_date jelenlétét "active+endTime"-ként kezeljük
def get_validity_status(rec):
    return "active"   # end_date-et overallEndTime-ban közöljük, nem suspended-ként


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
    nsmap = {None: NS, "xsi": XSI, "bkk": NS_BKK}

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel", nsmap=nsmap,
        attrib={
            "modelBaseVersion": "2",
            f"{{{XSI}}}schemaLocation": (
                "http://datex2.eu/schema/2/2_0 "
                "http://datex2.eu/schema/2/2_0/DATEXIISchema_2_0.xsd"
            ),
        },
    )

    exchange = etree.SubElement(root, f"{{{NS}}}exchange")
    sup = etree.SubElement(exchange, f"{{{NS}}}supplierIdentification")
    etree.SubElement(sup, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(sup, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER
    etree.SubElement(exchange, f"{{{NS}}}subscriptionReference").text = PUBLISHER_ID
    # [FIX-T1] timeDefault = pub_time (= publicationTime)
    etree.SubElement(exchange, f"{{{NS}}}timeDefault").text = pub_time

    pub_el = etree.SubElement(
        root, f"{{{NS}}}payloadPublication",
        attrib={f"{{{XSI}}}type": f"{{{NS}}}SituationPublication", "lang": "hu"},
    )
    etree.SubElement(pub_el, f"{{{NS}}}publicationTime").text = pub_time

    creator = etree.SubElement(pub_el, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        # [FIX-T1] creationTime garantáltan <= pub_time
        creation_time = safe_creation_time(rec.get("start_date"), pub_time)
        # [FIX-T2] versionTime = pub_time (legutóbbi publikálás ideje)
        version_time = pub_time

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

        etree.SubElement(sit_rec, f"{{{NS}}}situationRecordCreationTime").text = creation_time
        etree.SubElement(sit_rec, f"{{{NS}}}situationRecordVersionTime").text  = version_time

        hdr = etree.SubElement(sit_rec, f"{{{NS}}}headerInformation")
        etree.SubElement(hdr, f"{{{NS}}}confidentiality").text   = "noRestriction"
        etree.SubElement(hdr, f"{{{NS}}}informationStatus").text = "real"

        etree.SubElement(sit_rec, f"{{{NS}}}probabilityOfOccurrence").text = "certain"
        etree.SubElement(sit_rec, f"{{{NS}}}severity").text = get_severity(rec)

        # [FIX-V1] validityStatus: active + overallEndTime (nem suspended)
        validity = etree.SubElement(sit_rec, f"{{{NS}}}validity")
        etree.SubElement(validity, f"{{{NS}}}validityStatus").text = get_validity_status(rec)
        vp = etree.SubElement(validity, f"{{{NS}}}validityTimeSpecification")
        etree.SubElement(vp, f"{{{NS}}}overallStartTime").text = creation_time
        if rec.get("end_date"):
            etree.SubElement(vp, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

        etree.SubElement(sit_rec, f"{{{NS}}}accidentType").text = get_accident_type(rec)

        for eff in rec.get("effects", []):
            piv = eff.get("pivot", {})
            lat, lon = parse_coordinates(piv.get("coordinates", "[]"))
            street = piv.get("street", "")

            if lat and lon:
                loc = etree.SubElement(
                    sit_rec, f"{{{NS}}}groupOfLocations",
                    attrib={f"{{{XSI}}}type": f"{{{NS}}}Point"}
                )
                point = etree.SubElement(loc, f"{{{NS}}}locationForDisplay")
                etree.SubElement(point, f"{{{NS}}}latitude").text  = str(lat)
                etree.SubElement(point, f"{{{NS}}}longitude").text = str(lon)
                if street:
                    desc = etree.SubElement(sit_rec, f"{{{NS}}}locationDescriptor")
                    etree.SubElement(desc, f"{{{NS}}}value").text = street

            # [FIX-O1] cause ELŐBB, _extension UTÁNA
            for cause in rec.get("causes", []):
                cause_elem = etree.SubElement(sit_rec, f"{{{NS}}}cause")
                # [FIX-C1] csak causeDescription (causeType nem standard 2.x-ben)
                etree.SubElement(cause_elem, f"{{{NS}}}causeDescription").text = (
                    cause.get("name") or cause.get("code", "")
                )

            ext = etree.SubElement(sit_rec, f"{{{NS}}}situationRecord_extension")
            bkk_ef = etree.SubElement(ext, f"{{{NS_BKK}}}bkkEffectInfo")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}subRecordId").text = str(piv.get("id", ""))
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}changeId").text    = str(piv.get("change_id", rec["id"]))
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}effectCode").text  = eff.get("code", "")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}effectName").text  = eff.get("name", "")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}streetName").text  = street
            break  # 1 fő lokáció per record

    return root


# ──────────────────────────────────────────────
# DATEX II 3.2
# ──────────────────────────────────────────────

def build_v32(records, pub_time):
    NS     = "http://datex2.eu/schema/3/common"
    NS_SIT = "http://datex2.eu/schema/3/situation"
    NS_LOC = "http://datex2.eu/schema/3/locationReferencing"
    XSI    = "http://www.w3.org/2001/XMLSchema-instance"

    nsmap = {None: NS, "sit": NS_SIT, "loc": NS_LOC, "bkk": NS_BKK, "xsi": XSI}

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel", nsmap=nsmap,
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
        attrib={f"{{{XSI}}}type": f"{{{NS_SIT}}}SituationPublication", "lang": "hu"},
    )
    etree.SubElement(pub_el, f"{{{NS}}}publicationTime").text = pub_time

    creator = etree.SubElement(pub_el, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        # [FIX-T1] creationTime garantáltan <= pub_time
        creation_time = safe_creation_time(rec.get("start_date"), pub_time)
        # [FIX-T2] versionTime = situationVersionTime = pub_time
        version_time = pub_time

        sit = etree.SubElement(
            pub_el, f"{{{NS_SIT}}}situation",
            attrib={"id": f"BKK_SIT_{rec['id']}", "version": "1"},
        )
        etree.SubElement(sit, f"{{{NS_SIT}}}situationVersionTime").text = version_time

        sit_rec = etree.SubElement(
            sit, f"{{{NS_SIT}}}situationRecord",
            attrib={
                f"{{{XSI}}}type": f"{{{NS_SIT}}}Accident",
                "id": f"BKK_REC_{rec['id']}",
                "version": "1",
            },
        )

        etree.SubElement(sit_rec, f"{{{NS_SIT}}}creationTime").text = creation_time
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}versionTime").text  = version_time

        hdr = etree.SubElement(sit_rec, f"{{{NS_SIT}}}headerInformation")
        etree.SubElement(hdr, f"{{{NS_SIT}}}confidentiality").text   = "noRestriction"
        etree.SubElement(hdr, f"{{{NS_SIT}}}informationStatus").text = "real"

        etree.SubElement(sit_rec, f"{{{NS_SIT}}}probabilityOfOccurrence").text = "certain"

        etree.SubElement(sit_rec, f"{{{NS_SIT}}}accidentType").text = get_accident_type(rec)

        # [FIX-V1] active + overallEndTime
        validity = etree.SubElement(sit_rec, f"{{{NS_SIT}}}validity")
        etree.SubElement(validity, f"{{{NS_SIT}}}validityStatus").text = get_validity_status(rec)
        vts = etree.SubElement(validity, f"{{{NS_SIT}}}validityTimeSpecification")
        etree.SubElement(vts, f"{{{NS}}}overallStartTime").text = creation_time
        if rec.get("end_date"):
            etree.SubElement(vts, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

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
                # [FIX-L1] 3.2-ben NINCS loc:pointCoordinates burokelem!
                # Közvetlenül latitude/longitude a pointByCoordinates alatt
                etree.SubElement(coord, f"{{{NS_LOC}}}latitude").text  = str(lat)
                etree.SubElement(coord, f"{{{NS_LOC}}}longitude").text = str(lon)

            # [FIX-O1] cause ELŐBB, _extension UTÁNA
            for cause in rec.get("causes", []):
                cause_el = etree.SubElement(sit_rec, f"{{{NS_SIT}}}cause")
                # [FIX-C1] causeType ELTÁVOLÍTVA - nem standard DATEX II 3.2 elem
                etree.SubElement(cause_el, f"{{{NS_SIT}}}causeDescription").text = (
                    cause.get("name") or cause.get("code", "")
                )

            ext = etree.SubElement(sit_rec, f"{{{NS_SIT}}}situationRecord_extension")
            bkk_ef = etree.SubElement(ext, f"{{{NS_BKK}}}bkkEffectInfo")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}subRecordId").text = str(piv.get("id", ""))
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}changeId").text    = str(piv.get("change_id", rec["id"]))
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}effectCode").text  = eff.get("code", "")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}effectName").text  = eff.get("name", "")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}streetName").text  = piv.get("street", "")

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

    nsmap = {None: NS, "sit": NS_SIT, "loc": NS_LOC, "road": NS_ROAD, "bkk": NS_BKK, "xsi": XSI}

    root = etree.Element(
        f"{{{NS}}}d2LogicalModel", nsmap=nsmap,
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
        attrib={f"{{{XSI}}}type": f"{{{NS_SIT}}}SituationPublication", "lang": "hu"},
    )
    etree.SubElement(pub_el, f"{{{NS}}}publicationTime").text = pub_time

    creator = etree.SubElement(pub_el, f"{{{NS}}}publicationCreator")
    etree.SubElement(creator, f"{{{NS}}}country").text = COUNTRY
    etree.SubElement(creator, f"{{{NS}}}nationalIdentifier").text = NATIONAL_IDENTIFIER

    for rec in records:
        # [FIX-T1] creationTime garantáltan <= pub_time
        creation_time = safe_creation_time(rec.get("start_date"), pub_time)
        # [FIX-T2] versionTime = situationVersionTime = pub_time
        version_time = pub_time

        sit = etree.SubElement(
            pub_el, f"{{{NS_SIT}}}situation",
            attrib={"id": f"BKK_SIT_{rec['id']}", "version": "1"},
        )
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

        etree.SubElement(sit_rec, f"{{{NS_SIT}}}creationTime").text = creation_time
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}versionTime").text  = version_time

        hdr = etree.SubElement(sit_rec, f"{{{NS_SIT}}}headerInformation")
        etree.SubElement(hdr, f"{{{NS_SIT}}}confidentiality").text   = "noRestriction"
        etree.SubElement(hdr, f"{{{NS_SIT}}}informationStatus").text = "real"

        etree.SubElement(sit_rec, f"{{{NS_SIT}}}probabilityOfOccurrence").text = "certain"
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}severity").text = get_severity(rec)
        etree.SubElement(sit_rec, f"{{{NS_SIT}}}accidentType").text = get_accident_type(rec)

        # [FIX-V1] active + overallEndTime
        validity = etree.SubElement(sit_rec, f"{{{NS_SIT}}}validity")
        etree.SubElement(validity, f"{{{NS_SIT}}}validityStatus").text = get_validity_status(rec)
        vts = etree.SubElement(validity, f"{{{NS_SIT}}}validityTimeSpecification")
        etree.SubElement(vts, f"{{{NS}}}overallStartTime").text = creation_time
        if rec.get("end_date"):
            etree.SubElement(vts, f"{{{NS}}}overallEndTime").text = format_date(rec["end_date"])

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
                # 3.5-ben loc:pointCoordinates burokelem KÖTELEZŐ
                gdc = etree.SubElement(coord, f"{{{NS_LOC}}}pointCoordinates")
                etree.SubElement(gdc, f"{{{NS_LOC}}}latitude").text  = str(lat)
                etree.SubElement(gdc, f"{{{NS_LOC}}}longitude").text = str(lon)

                if street:
                    road_ref = etree.SubElement(loc, f"{{{NS_ROAD}}}roadInformation")
                    etree.SubElement(road_ref, f"{{{NS_ROAD}}}roadName").text = street

            # [FIX-O1] cause ELŐBB, _extension UTÁNA
            for cause in rec.get("causes", []):
                cause_el = etree.SubElement(sit_rec, f"{{{NS_SIT}}}cause")
                # [FIX-C1] causeType ELTÁVOLÍTVA - nem standard DATEX II 3.5 elem
                etree.SubElement(cause_el, f"{{{NS_SIT}}}causeDescription").text = (
                    cause.get("name") or cause.get("code", "")
                )

            ext = etree.SubElement(sit_rec, f"{{{NS_SIT}}}situationRecord_extension")
            bkk_ef = etree.SubElement(ext, f"{{{NS_BKK}}}bkkEffectInfo")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}subRecordId").text = str(piv.get("id", ""))
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}changeId").text    = str(piv.get("change_id", rec["id"]))
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}effectCode").text  = eff.get("code", "")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}effectName").text  = eff.get("name", "")
            etree.SubElement(bkk_ef, f"{{{NS_BKK}}}streetName").text  = street

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
