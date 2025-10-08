# -*- coding: utf-8 -*-
"""
PropertyGuru extractor — Spyder-friendly
- If ROOT is blank or not found, prompts you to select a folder (GUI if available; else console input).
- Traverses a ROOT directory looking for PropertyGuru ADVIEW payloads (raw Next.js
  JSON from the full scraper, plain .html/.htm, zipped, or gzipped variants)
- Extracts listing fields using the same structural rules as the production scraper
- Writes a CSV named 'propertyguru_extract.csv' inside the selected ROOT

How to run in Spyder:
1) Open this file.
2) (Optional) Put your folder path in ROOT below; or just Run and choose a folder when prompted.
3) Press Run ▶. When finished, see the CSV path printed at the end.
"""

import os
import re
import json
import csv
import zipfile
import gzip
import sys
import time

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependency: bs4. Install with:  pip install beautifulsoup4")
    raise

# ------------------- CONFIG -------------------
ROOT = r""
OUT_BASENAME = "propertyguru_extract.csv"
DOMAIN = "https://www.propertyguru.com.my"

# ------------------- RUNTIME FOLDER PICKER -------------------
def pick_root_if_needed(root):
    if root and os.path.isdir(root):
        return root

    # Try Tkinter folder picker first
    try:
        import tkinter as tk
        from tkinter import filedialog
        tk.Tk().withdraw()
        folder = filedialog.askdirectory(title="Select the adview folder (contains .html/.zip files)")
        if folder and os.path.isdir(folder):
            return folder
    except Exception:
        pass

    # Fallback to console input
    while True:
        try:
            folder = input("Enter folder path to scan (or leave blank to quit): ").strip('"').strip()
        except EOFError:
            folder = ""
        if not folder:
            print("No folder selected. Exiting.")
            sys.exit(0)
        if os.path.isdir(folder):
            return folder
        print("Path not found. Try again.\n")

# ------------------- JSON HELPERS -------------------
def _iter_script_jsons(soup):
    for sc in soup.find_all("script"):
        t = (sc.get("type") or "").lower()
        if sc.get("id") == "__NEXT_DATA__" or t in ("application/json", "application/ld+json"):
            txt = (sc.string or sc.text or "").strip()
            if not txt:
                continue
            try:
                data = json.loads(txt)
            except Exception:
                continue
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, (dict, list)):
                        yield item
            elif isinstance(data, (dict, list)):
                yield data


def _collect_all_json(soup):
    out = []
    for obj in _iter_script_jsons(soup):
        out.append(obj)
        if isinstance(obj, dict):
            props = obj.get("props")
            if isinstance(props, dict):
                page = props.get("pageProps")
                if isinstance(page, dict):
                    out.append(page)
                    pd = page.get("pageData")
                    if isinstance(pd, dict):
                        out.append(pd)
                        dd = pd.get("data")
                        if isinstance(dd, dict):
                            out.append(dd)
    return out


def get_data_root(j):
    if not isinstance(j, dict):
        return {}
    return j.get("props", {}).get("pageProps", {}).get("pageData", {}).get("data", {})


def find_data_root(soup):
    for obj in _collect_all_json(soup):
        if isinstance(obj, dict):
            dd = get_data_root(obj)
            if dd:
                return dd
            if "listingData" in obj and "propertyOverviewData" in obj:
                return obj
    return {}


def get_by_path(d, dotted):
    cur = d
    for tok in dotted.split("."):
        if isinstance(cur, dict) and tok in cur:
            cur = cur[tok]
        elif isinstance(cur, list) and tok.isdigit():
            idx = int(tok)
            if 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return None
        else:
            return None
    return cur


def pick_first(d, paths):
    for p in paths:
        v = get_by_path(d, p)
        if v not in (None, "", []):
            return v
    return ""


def digits_only(x):
    if x in (None, "", []):
        return ""
    return "".join(re.findall(r"\d+", str(x)))


def make_abs(u):
    if not isinstance(u, str) or not u:
        return ""
    return u if u.startswith("http") else (DOMAIN + u)


def parse_money_value(v):
    if v in (None, "", "-"):
        return ""
    if isinstance(v, (int, float)):
        return str(int(round(float(v))))
    s = str(v)
    m = re.search(r'(\d{1,3}(?:,\d{3})+|\d+)(?:\.(\d+))?', s)
    if not m:
        return ""
    whole = m.group(1).replace(",", "")
    dec = m.group(2) or ""
    if dec:
        return str(int(round(float(f"{whole}.{dec}"))))
    return whole


MALAYSIAN_STATES = {
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan", "Pahang", "Perak", "Perlis",
    "Pulau Pinang", "Penang", "Sabah", "Sarawak", "Selangor", "Terengganu",
    "Kuala Lumpur", "W.P. Kuala Lumpur", "Putrajaya", "Labuan",
}

STATE_SYNONYMS = {
    "Penang": "Pulau Pinang",
    "W.P. Kuala Lumpur": "Kuala Lumpur",
}


def find_state_in_address(address):
    if not isinstance(address, str) or not address.strip():
        return ""
    for st in MALAYSIAN_STATES:
        if re.search(rf"\b{re.escape(st)}\b", address, re.I):
            return STATE_SYNONYMS.get(st, st)
    for syn, canon in STATE_SYNONYMS.items():
        if re.search(rf"\b{re.escape(syn)}\b", address, re.I):
            return canon
    return ""


def map_tenure(code):
    if not code:
        return ""
    up = str(code).strip().upper()
    return {"F": "Freehold", "L": "Leasehold"}.get(up, str(code))


# Regexes used when filling details
R_BUMI = re.compile(r"\b(?:Not\s+)?Bumi\s+Lot\b", re.I)
R_TITLE = re.compile(r"\b(Individual|Strata|Master)\s+title\b", re.I)
R_DEV = re.compile(r"^Developed by\s+(.+)$", re.I)
R_COMPLETE_YR = re.compile(r"\b(Completed|Completion)\s+in\s+(\d{4})\b", re.I)
R_FLOOR = re.compile(r"([\d,\.]+)\s*(sqft|sf)\s*floor\s*area\b", re.I)
R_LAND = re.compile(r"([\d,\.]+)\s*(sqft|sf)\s*land\s*area\b", re.I)
R_PSF = re.compile(r"\bRM\s*([\d\.,]+)\s*psf\b", re.I)
R_TENURE_TXT = re.compile(r"\b(Freehold|Leasehold)\s+tenure\b", re.I)


FURNISH_PATHS_STRICT = [
    "propertyOverviewData.propertyInfo.furnishing",
    "listingData.property.furnishing",
    "listingData.furnishing",
    "listingDetail.attributes.furnishing",
]


def normalize_furnishing(s):
    if not isinstance(s, str):
        return ""
    t = s.strip().lower()
    if t in {"bare", "unfurnished", "not furnished", "non furnished", "no furnishing"}:
        return "Unfurnished"
    if t in {"partly furnished", "partially furnished", "semi furnished", "semi-furnished"}:
        return "Partially Furnished"
    if t in {"fully furnished", "furnished"}:
        return "Fully Furnished"
    return ""


def furnishing_from_metatable(dd):
    meta = (dd.get("detailsData") or {}).get("metatable") or {}
    for it in (meta.get("items") or []):
        if not isinstance(it, dict):
            continue
        icon = str(it.get("icon") or "").strip().lower()
        if icon == "furnished-o":
            title = str(it.get("title") or it.get("label") or "").strip()
            value = str(it.get("value") or it.get("text") or "").strip()
            val = normalize_furnishing(value or title)
            if val:
                return val
    return ""


def furnishing_from_labeled_items(dd):
    details = dd.get("detailsData") or {}
    scope = details.get("details") or details.get("data") or {}

    def iter_items(node):
        if isinstance(node, dict):
            if "items" in node and isinstance(node["items"], list):
                for item in node["items"]:
                    yield item
            for v in node.values():
                yield from iter_items(v)
        elif isinstance(node, list):
            for item in node:
                yield from iter_items(item)

    for it in iter_items(scope):
        if not isinstance(it, dict):
            continue
        label = str(it.get("label") or it.get("name") or it.get("title") or "").strip()
        value = str(it.get("value") or it.get("text") or "").strip()
        if label and value and label.lower().startswith("furnish"):
            val = normalize_furnishing(value)
            if val:
                return val
    return ""


def extract_furnishing(dd):
    v = furnishing_from_metatable(dd)
    if v:
        return v, "detailsData.metatable(icon=furnished-o)"
    for path in FURNISH_PATHS_STRICT:
        raw = get_by_path(dd, path) if isinstance(dd, dict) else None
        val = normalize_furnishing(raw if isinstance(raw, str) else "")
        if val:
            return val, path
    v = furnishing_from_labeled_items(dd)
    if v:
        return v, "detailsData.labeled"
    return "", ""


def iter_detail_strings(node):
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, dict) and "items" in v and isinstance(v["items"], list):
                for it in v["items"]:
                    if isinstance(it, dict):
                        for key in ("value", "text", "label", "name"):
                            s = it.get(key)
                            if isinstance(s, str) and s.strip():
                                yield s.strip()
            elif isinstance(v, list) and ("detail" in k.lower() or "item" in k.lower()):
                for it in v:
                    if isinstance(it, dict):
                        for key in ("value", "text", "label", "name"):
                            s = it.get(key)
                            if isinstance(s, str) and s.strip():
                                yield s.strip()
            yield from iter_detail_strings(v)
    elif isinstance(node, list):
        for it in node:
            yield from iter_detail_strings(it)


def fill_from_details(strings, seed):
    for v in strings:
        if not seed["property_title"] and (m := R_TITLE.search(v)):
            seed["property_title"] = m.group(0).title()
        if not seed["bumi_lot"] and (m := R_BUMI.search(v)):
            seed["bumi_lot"] = "Not Bumi Lot" if "Not" in m.group(0) else "Bumi Lot"
        if not seed["developer"] and (m := R_DEV.search(v)):
            seed["developer"] = m.group(1).strip()
        if not seed["completion_year"] and (m := R_COMPLETE_YR.search(v)):
            seed["completion_year"] = m.group(2)
        if not seed["build_up"] and (m := R_FLOOR.search(v)):
            seed["build_up"] = digits_only(m.group(1))
        if not seed["land_area"] and (m := R_LAND.search(v)):
            seed["land_area"] = digits_only(m.group(1))
        if not seed["price_per_square_feet"] and (m := R_PSF.search(v)):
            seed["price_per_square_feet"] = digits_only(m.group(1))
        if not seed["tenure"] and (m := R_TENURE_TXT.search(v)):
            seed["tenure"] = m.group(1).title()
    return seed


def build_amenities(property_info):
    am = (property_info or {}).get("amenities", [])
    if isinstance(am, list) and am:
        out = []
        for item in am:
            if not isinstance(item, dict):
                continue
            unit = str(item.get("unit", "")).strip()
            value = str(item.get("value", "")).strip()
            if unit and value:
                if unit.lower() in {"sqft", "sf"}:
                    out.append(f"{value} {unit}")
                else:
                    out.append(f"{unit} {value}")
        return "; ".join(out)
    return ""


def build_facilities(data):
    fac = (data or {}).get("facilitiesData", {})
    if isinstance(fac, dict):
        items = fac.get("data", [])
        if isinstance(items, list):
            texts = [x.get("text", "").strip() for x in items if isinstance(x, dict) and x.get("text")]
            return ", ".join([t for t in texts if t])
    return ""


# Candidate paths copied from production scraper
URL_PATHS = ["listingData.url"]
TITLE_PATHS = ["listingData.localizedTitle", "listingData.title"]
PROPERTY_TYPE_PATHS = [
    "propertyOverviewData.propertyInfo.propertyType",
    "listingData.propertyType",
    "listingData.property.typeText",
    "listingData.property.type",
]
ADDRESS_PATHS = [
    "propertyOverviewData.propertyInfo.fullAddress",
    "listingData.displayAddress",
    "listingData.address",
    "listingData.property.addressText",
]
STATE_PATHS = [
    "propertyOverviewData.propertyInfo.stateName",
    "listingData.property.stateName",
    "listingData.stateName",
]
DISTRICT_PATHS = [
    "propertyOverviewData.propertyInfo.districtName",
    "listingData.property.districtName",
    "listingData.districtName",
    "listingData.districtText",
]
SUBAREA_PATHS = [
    "propertyOverviewData.propertyInfo.areaName",
    "listingData.property.areaName",
    "listingData.areaName",
    "listingData.areaText",
]
LISTER_NAME_PATHS = [
    "contactAgentData.contactAgentCard.agentInfoProps.agent.name",
    "listingData.agent.name",
]
LISTER_URL_PATHS = [
    "contactAgentData.contactAgentCard.agentInfoProps.agent.profileUrl",
    "listingData.agent.profileUrl",
    "listingData.agent.url",
]
PHONE_PATHS = [
    "contactAgentData.contactAgentCard.agentInfoProps.agent.mobile",
    "listingData.agent.contactNumbers.0.number",
    "listingData.agent.contactNumbers.0.displayNumber",
    "listingData.agent.phoneNumber",
    "listingData.agent.mobile",
    "listingData.agent.contactNumber",
]
PHONE2_PATHS = [
    "contactAgentData.contactAgentCard.agentInfoProps.agent.phone",
    "listingData.agent.contactNumbers.1.number",
    "listingData.agent.contactNumbers.1.displayNumber",
    "listingData.agent.secondaryPhone",
]
AGENCY_NAME_PATHS = [
    "contactAgentData.contactAgentCard.agency.name",
    "listingData.agent.agency.name",
    "listingData.agent.agencyName",
]
AGENCY_REG_PATHS = [
    "contactAgentData.contactAgentCard.agency.registrationNumber",
    "contactAgentData.contactAgentCard.agency.licenseNo",
    "listingData.agent.agency.registrationNumber",
    "listingData.agent.agency.registrationNo",
    "listingData.agent.agency.regNo",
]
REN_PATHS = [
    "listingData.agent.licenseNumber",
    "listingData.agent.renNo",
    "listingData.agent.registrationNo",
    "listingData.agent.ren",
    "contactAgentData.contactAgentCard.agentInfoProps.agent.licenseNumber",
]
PRICE_PATHS = [
    "propertyOverviewData.propertyInfo.price.amount",
    "listingData.priceValue",
    "listingData.pricePretty",
    "listingData.price",
]
CAR_PARK_PATHS = [
    "propertyOverviewData.propertyInfo.carPark",
    "listingData.property.carPark",
    "listingData.carPark",
    "listingData.carParks",
]
EMAIL_PATHS = [
    "contactAgentData.contactAgentCard.agentInfoProps.agent.email",
    "listingData.agent.email",
]
SELLER_NAME_PATHS = [
    "listingData.sellerName",
    "contactAgentData.contactAgentCard.agentInfoProps.agent.sellerName",
]
MARKET_PATHS = [
    "listingData.market",
    "propertyOverviewData.propertyInfo.market",
]
REGION_PATHS = [
    "listingData.regionName",
    "propertyOverviewData.propertyInfo.regionName",
]
RENT_SALE_PATHS = [
    "listingData.listingType",
    "listingData.purpose",
    "listingData.transactionType",
]
TYPE_PATHS = [
    "listingData.type",
    "listingData.property.listingType",
]
POSTED_DATE_PATHS = [
    "listingData.publishedDate",
    "listingData.postedDate",
]
POSTED_TIME_PATHS = [
    "listingData.publishedTime",
    "listingData.postedTime",
]
CREATED_TIME_PATHS = [
    "listingData.createdAt",
    "listingData.createdDate",
    "listingData.createTime",
]
UPDATED_DATE_PATHS = [
    "listingData.updatedAt",
    "listingData.updatedDate",
    "listingData.updateTime",
]
ACTIVATE_DATE_PATHS = [
    "listingData.activateDate",
    "listingData.activationDate",
]
CURRENCY_PATHS = [
    "propertyOverviewData.propertyInfo.price.currency",
    "listingData.currency",
]
ROOMS_PATHS = [
    "propertyOverviewData.propertyInfo.bedrooms",
    "listingData.property.bedrooms",
    "listingData.bedrooms",
]
TOILETS_PATHS = [
    "propertyOverviewData.propertyInfo.bathrooms",
    "listingData.property.bathrooms",
    "listingData.bathrooms",
]
PSF_PATHS = [
    "propertyOverviewData.propertyInfo.price.perSqft",
    "propertyOverviewData.propertyInfo.pricePerSqft",
    "listingData.floorAreaPsf",
]
FLOOR_AREA_PATHS = [
    "propertyOverviewData.propertyInfo.builtUp.size",
    "propertyOverviewData.propertyInfo.builtUpSqft",
    "listingData.floorArea",
    "listingData.property.builtUpArea",
]
LAND_AREA_PATHS = [
    "propertyOverviewData.propertyInfo.landArea.size",
    "propertyOverviewData.propertyInfo.landAreaSqft",
    "listingData.landArea",
    "listingData.property.landArea",
]
TENURE_PATHS = [
    "propertyOverviewData.propertyInfo.tenure",
    "listingData.property.tenure",
    "listingData.tenure",
]
PROPERTY_TITLE_PATHS = [
    "propertyOverviewData.propertyInfo.titleType",
    "listingData.property.titleType",
    "listingData.property.title",
]
BUMI_PATHS = [
    "propertyOverviewData.propertyInfo.bumiLot",
    "listingData.property.bumiLot",
]
TOTAL_UNITS_PATHS = [
    "propertyOverviewData.propertyInfo.totalUnits",
    "listingData.property.totalUnits",
]
COMPLETION_YEAR_PATHS = [
    "propertyOverviewData.propertyInfo.completedYear",
    "propertyOverviewData.propertyInfo.completionYear",
    "listingData.property.completedYear",
    "listingData.property.yearBuilt",
]
DEVELOPER_PATHS = [
    "propertyOverviewData.propertyInfo.developer",
    "listingData.property.developer",
]


# ------------------- FILE ITERATOR -------------------
def _detect_payload_type(text, explicit=None):
    if explicit:
        return explicit
    sample = text.lstrip()
    if sample.startswith("{") or sample.startswith("["):
        return "json"
    return "html"


def iter_payloads(root):
    for dirpath, dirnames, filenames in os.walk(root):
        for fn in filenames:
            path = os.path.join(dirpath, fn)
            lower = fn.lower()

            if lower.endswith(".json"):
                try:
                    with open(path, "r", encoding="utf-8") as fh:
                        text = fh.read()
                    yield path, text, "json"
                except Exception:
                    continue
                continue

            if lower.endswith((".json.gz", ".gz")):
                try:
                    with open(path, "rb") as fh:
                        blob = fh.read()
                    text = gzip.decompress(blob).decode("utf-8", "ignore")
                    yield path, text, _detect_payload_type(text, "json" if lower.endswith(".json.gz") else None)
                except Exception:
                    continue
                continue

            if lower.endswith(".zip"):
                try:
                    with zipfile.ZipFile(path) as z:
                        for n in z.namelist():
                            n_lower = n.lower()
                            try:
                                text = z.read(n).decode("utf-8", "ignore")
                            except Exception:
                                continue
                            explicit = None
                            if n_lower.endswith(".json"):
                                explicit = "json"
                            elif n_lower.endswith((".html", ".htm")):
                                explicit = "html"
                            yield f"{path}|{n}", text, _detect_payload_type(text, explicit)
                except Exception:
                    continue
                continue

            if lower.endswith((".html", ".htm")):
                try:
                    with open(path, "rb") as fh:
                        html = fh.read().decode("utf-8", "ignore")
                    yield path, html, "html"
                except Exception:
                    continue


# ------------------- MAIN EXTRACTION -------------------
def extract_row(name, payload, payload_type):
    soup = None
    data = {}

    if payload_type == "json":
        try:
            obj = json.loads(payload)
        except Exception:
            print(f"[WARN] {name}: JSON decode failed")
            return None
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    data = get_data_root(item)
                    if data:
                        break
        else:
            data = get_data_root(obj)
        if not data and isinstance(obj, dict):
            if "listingData" in obj and "propertyOverviewData" in obj:
                data = obj
        if not data:
            print(f"[WARN] {name}: listing data not found in JSON")
            return None
    else:
        soup = BeautifulSoup(payload, "html.parser")
        data = find_data_root(soup)
        if not data:
            print(f"[WARN] {name}: Next.js data not found")
            return None

    listing = data.get("listingData", {}) or {}
    property_info = ((data.get("propertyOverviewData") or {}).get("propertyInfo") or {})

    url = make_abs(pick_first(data, URL_PATHS)) or ""
    if not url and soup is not None:
        link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
        if link and link.get("href"):
            url = link["href"].strip()

    title = pick_first(data, TITLE_PATHS) or (listing.get("property") or {}).get("typeText") or ""

    address = pick_first(data, ADDRESS_PATHS)
    state = pick_first(data, STATE_PATHS)
    if not state:
        state = find_state_in_address(address)
    district = pick_first(data, DISTRICT_PATHS)
    subarea = pick_first(data, SUBAREA_PATHS)

    location_parts = [p for p in [subarea, district, state] if p]
    if address and state and district:
        location = f"{subarea + ', ' if subarea else ''}{district}, {state}"
    else:
        location = ", ".join(location_parts) if location_parts else (address or "")

    furnishing, furnishing_source = extract_furnishing(data)

    listing_uuid = str(listing.get("id") or "")
    listing_id = str(listing.get("listingId") or "")
    ad_identifier = str(listing.get("adId") or listing_uuid or listing_id or "")

    posted_date_val = str(pick_first(data, POSTED_DATE_PATHS) or listing.get("publishedDate") or listing.get("postedDate") or "")
    posted_time_val = str(pick_first(data, POSTED_TIME_PATHS) or listing.get("publishedTime") or listing.get("postedTime") or "")
    created_time_val = str(pick_first(data, CREATED_TIME_PATHS) or listing.get("createdAt") or listing.get("createdDate") or listing.get("createTime") or "")
    updated_date_val = str(pick_first(data, UPDATED_DATE_PATHS) or listing.get("updatedAt") or listing.get("updatedDate") or listing.get("updateTime") or "")
    activate_date_val = str(pick_first(data, ACTIVATE_DATE_PATHS) or listing.get("activateDate") or listing.get("activationDate") or "")

    car_park_val = str(pick_first(data, CAR_PARK_PATHS) or "")
    currency_val = str(pick_first(data, CURRENCY_PATHS) or "")
    email_val = str(pick_first(data, EMAIL_PATHS) or "")
    seller_name_val = str(pick_first(data, SELLER_NAME_PATHS) or "")
    market_val = str(pick_first(data, MARKET_PATHS) or "")
    phone_primary = str(pick_first(data, PHONE_PATHS) or "")
    phone_secondary = str(pick_first(data, PHONE2_PATHS) or "")
    region_val = str(pick_first(data, REGION_PATHS) or "")
    rent_sale_val = str(pick_first(data, RENT_SALE_PATHS) or listing.get("listingType") or listing.get("purpose") or listing.get("transactionType") or "")
    type_val = str(pick_first(data, TYPE_PATHS) or listing.get("type") or "")

    scrape_unix = int(time.time())
    scrape_date_val = time.strftime("%Y-%m-%d", time.localtime(scrape_unix))

    row = {
        "activate_date": activate_date_val,
        "id": listing_uuid,
        "ad_id": ad_identifier,
        "listing_id": listing_id,
        "agency": pick_first(data, AGENCY_NAME_PATHS) or "",
        "build_up": digits_only(pick_first(data, FLOOR_AREA_PATHS)),
        "land_area": digits_only(pick_first(data, LAND_AREA_PATHS)),
        "car_park": car_park_val,
        "currency": currency_val,
        "email": email_val,
        "furnishing": furnishing,
        "lister": pick_first(data, LISTER_NAME_PATHS) or "",
        "seller_name": seller_name_val,
        "market": market_val,
        "phone_number": phone_primary,
        "phone": phone_primary or phone_secondary,
        "phone_number2": phone_secondary,
        "posted_date": posted_date_val,
        "posted_time": posted_time_val,
        "created_time": created_time_val,
        "price": parse_money_value(pick_first(data, PRICE_PATHS)),
        "property_type": pick_first(data, PROPERTY_TYPE_PATHS) or "",
        "region": region_val,
        "ren": str(pick_first(data, REN_PATHS) or ""),
        "rent_sale": rent_sale_val,
        "rooms": str(pick_first(data, ROOMS_PATHS) or ""),
        "scrape_date": scrape_date_val,
        "source": "PropertyGuru",
        "state": state or "",
        "subregion": district or "",
        "title": title or "",
        "location": location or "",
        "toilets": str(pick_first(data, TOILETS_PATHS) or ""),
        "type": type_val,
        "updated_date": updated_date_val,
        "url": url or "",
    }

    row.update({
        "file": name,
        "address": address or "",
        "subarea": subarea or "",
        "lister_url": make_abs(pick_first(data, LISTER_URL_PATHS)) or "",
        "agency_registration_number": pick_first(data, AGENCY_REG_PATHS) or "",
        "price_per_square_feet": digits_only(pick_first(data, PSF_PATHS)),
        "furnishing_source": furnishing_source,
        "tenure": map_tenure(pick_first(data, TENURE_PATHS)),
        "property_title": pick_first(data, PROPERTY_TITLE_PATHS) or "",
        "bumi_lot": pick_first(data, BUMI_PATHS) or "",
        "total_units": str(pick_first(data, TOTAL_UNITS_PATHS) or ""),
        "completion_year": digits_only(pick_first(data, COMPLETION_YEAR_PATHS)),
        "developer": pick_first(data, DEVELOPER_PATHS) or "",
        "amenities": build_amenities(property_info),
        "facilities": build_facilities(data),
        "scrape_unix": scrape_unix,
    })

    seed = {
        "property_title": row["property_title"],
        "bumi_lot": row["bumi_lot"],
        "developer": row["developer"],
        "completion_year": row["completion_year"],
        "build_up": row["build_up"],
        "land_area": row["land_area"],
        "price_per_square_feet": row["price_per_square_feet"],
        "tenure": row["tenure"],
        "furnishing": row["furnishing"],
    }
    seed = fill_from_details(iter_detail_strings(data), seed)
    row["property_title"] = seed["property_title"] or row["property_title"]
    row["bumi_lot"] = seed["bumi_lot"] or row["bumi_lot"]
    row["developer"] = seed["developer"] or row["developer"]
    row["completion_year"] = seed["completion_year"] or row["completion_year"]
    row["build_up"] = seed["build_up"] or row["build_up"]
    row["land_area"] = seed["land_area"] or row["land_area"]
    row["price_per_square_feet"] = seed["price_per_square_feet"] or row["price_per_square_feet"]
    row["tenure"] = seed["tenure"] or row["tenure"]
    row["furnishing"] = seed["furnishing"] or row["furnishing"]

    return row


# ------------------- MAIN -------------------
def run():
    root = pick_root_if_needed(ROOT)
    rows = []
    seen = 0
    processed = 0
    print(f"Scanning: {root}")

    for name, payload, payload_type in iter_payloads(root):
        seen += 1
        try:
            row = extract_row(name, payload, payload_type)
        except Exception as exc:
            print(f"[WARN] {name}: {exc}")
            row = None
        if row:
            rows.append(row)
            processed += 1

    out_csv = os.path.join(root, OUT_BASENAME)
    primary_fieldnames = [
        "activate_date",
        "id",
        "ad_id",
        "listing_id",
        "agency",
        "build_up",
        "land_area",
        "car_park",
        "currency",
        "email",
        "furnishing",
        "lister",
        "seller_name",
        "market",
        "phone_number",
        "phone",
        "phone_number2",
        "posted_date",
        "posted_time",
        "created_time",
        "price",
        "property_type",
        "region",
        "ren",
        "rent_sale",
        "rooms",
        "scrape_date",
        "source",
        "state",
        "subregion",
        "title",
        "location",
        "toilets",
        "type",
        "updated_date",
        "url",
    ]

    extra_fieldnames = [
        "file",
        "address",
        "subarea",
        "lister_url",
        "agency_registration_number",
        "price_per_square_feet",
        "furnishing_source",
        "tenure",
        "property_title",
        "bumi_lot",
        "total_units",
        "completion_year",
        "developer",
        "amenities",
        "facilities",
        "scrape_unix",
    ]

    fieldnames = primary_fieldnames + extra_fieldnames

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Files seen: {seen} | processed: {processed}")
    print(f"Saved: {out_csv}")
    if rows:
        print("--- Preview (first 5 rows) ---")
        for r in rows[:5]:
            preview_keys = [
                "file",
                "title",
                "price",
                "rooms",
                "toilets",
                "build_up",
                "tenure",
            ]
            print({k: r.get(k) for k in preview_keys})


if __name__ == "__main__":
    run()
