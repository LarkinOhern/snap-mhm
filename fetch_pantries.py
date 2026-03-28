"""
fetch_pantries.py
Collects food pantry / food bank / distribution site data for all 74 MHM counties.

Sources:
  1. Google Places API (New) — primary: locations, hours, phone, website
  2. USDA Food Access Research Atlas — food desert / grocery access metrics by county
  3. Feeding America Map the Meal Gap — county food insecurity rates
  4. 211 Texas (findhelp.org) — supplemental listings via web fetch

Outputs:
  food_pantry_raw.xlsx         — all raw Places results (deduped)
  food_pantry_catalog.xlsx     — cleaned, classified, enriched
  food_access_county.xlsx      — county-level gap metrics
"""

import os, json, time, re
import requests
import pandas as pd
from pathlib import Path
from io import BytesIO

# ── API key ──────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")
if not API_KEY:
    raise SystemExit("Set GOOGLE_PLACES_API_KEY in .env or environment.")

PLACES_URL = "https://places.googleapis.com/v1/places:searchText"
CACHE_DIR  = Path("pantry_cache")
CACHE_DIR.mkdir(exist_ok=True)

# ── 74 MHM county centroids (lat, lon, radius_m) ────────────────────────────
# radius is the locationBias circle — larger for big rural counties
COUNTIES = {
    # Region 06 — Gulf Coast
    "Colorado":    (29.65, -96.53, 45000),
    "Matagorda":   (28.80, -96.00, 50000),
    # Region 07 — Austin / Central
    "Bastrop":     (30.11, -97.32, 35000),
    "Blanco":      (30.10, -98.43, 35000),
    "Burnet":      (30.75, -98.24, 35000),
    "Caldwell":    (29.83, -97.62, 30000),
    "Fayette":     (29.88, -96.93, 40000),
    "Hays":        (29.98, -98.03, 35000),
    "Lampasas":    (31.07, -98.18, 35000),
    "Llano":       (30.70, -98.68, 40000),
    "Mills":       (31.49, -98.60, 35000),
    "San Saba":    (31.17, -98.72, 35000),
    "Travis":      (30.27, -97.74, 30000),
    # Region 08 — San Antonio / SW Texas
    "Atascosa":    (28.89, -98.53, 40000),
    "Bandera":     (29.73, -99.24, 35000),
    "Bexar":       (29.45, -98.52, 30000),
    "Calhoun":     (28.45, -96.62, 35000),
    "Comal":       (29.80, -98.27, 30000),
    "De Witt":     (29.09, -97.35, 40000),
    "Dimmit":      (28.43, -99.76, 50000),
    "Edwards":     (29.98, -100.30, 60000),
    "Frio":        (28.87, -99.11, 45000),
    "Gillespie":   (30.32, -98.95, 40000),
    "Goliad":      (28.66, -97.39, 35000),
    "Gonzales":    (29.46, -97.45, 35000),
    "Guadalupe":   (29.57, -97.95, 30000),
    "Jackson":     (28.96, -96.61, 40000),
    "Karnes":      (28.91, -97.86, 35000),
    "Kendall":     (29.94, -98.71, 30000),
    "Kerr":        (30.05, -99.35, 40000),
    "Kinney":      (29.35, -100.42, 55000),
    "La Salle":    (28.34, -99.10, 55000),
    "Lavaca":      (29.38, -96.94, 40000),
    "Maverick":    (28.74, -100.32, 45000),
    "Medina":      (29.35, -99.11, 40000),
    "Real":        (29.83, -99.82, 50000),
    "Uvalde":      (29.36, -99.79, 45000),
    "Val Verde":   (29.89, -101.15, 65000),
    "Victoria":    (28.79, -96.98, 35000),
    "Wilson":      (29.18, -98.09, 35000),
    "Zavala":      (28.52, -99.76, 45000),
    # Region 09 — West Texas
    "Coke":        (31.89, -100.53, 50000),
    "Concho":      (31.32, -99.87, 50000),
    "Crockett":    (30.72, -101.42, 75000),
    "Irion":       (31.30, -100.99, 55000),
    "Kimble":      (30.49, -99.75, 55000),
    "Mason":       (30.75, -99.23, 45000),
    "McCulloch":   (31.20, -99.35, 50000),
    "Menard":      (30.88, -99.82, 50000),
    "Reagan":      (31.37, -101.52, 60000),
    "Schleicher":  (30.90, -100.54, 55000),
    "Sterling":    (31.82, -101.05, 55000),
    "Sutton":      (30.49, -100.53, 60000),
    "Tom Green":   (31.41, -100.47, 40000),
    "Upton":       (31.37, -102.05, 60000),
    # Region 11 — RGV / Coastal Bend
    "Aransas":     (28.10, -97.01, 30000),
    "Bee":         (28.43, -97.74, 40000),
    "Brooks":      (27.03, -98.22, 45000),
    "Cameron":     (26.15, -97.48, 35000),
    "Duval":       (27.68, -98.54, 50000),
    "Hidalgo":     (26.30, -98.17, 40000),
    "Jim Hogg":    (27.05, -98.70, 50000),
    "Jim Wells":   (27.73, -98.10, 40000),
    "Kenedy":      (26.93, -97.66, 60000),
    "Kleberg":     (27.43, -97.69, 35000),
    "Live Oak":    (28.35, -98.14, 40000),
    "McMullen":    (28.35, -98.57, 55000),
    "Nueces":      (27.73, -97.54, 30000),
    "Refugio":     (28.31, -97.15, 35000),
    "San Patricio":(28.01, -97.52, 35000),
    "Starr":       (26.56, -98.75, 50000),
    "Webb":        (27.76, -99.49, 45000),
    "Willacy":     (26.48, -97.65, 40000),
    "Zapata":      (27.01, -99.18, 50000),
}

# Search queries per county — varied phrasing catches different kinds of sites
QUERIES = [
    "food pantry",
    "food bank",
    "community food distribution",
    "emergency food assistance",
    "SNAP enrollment assistance food",
]

FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.shortFormattedAddress",
    "places.location",
    "places.types",
    "places.primaryType",
    "places.primaryTypeDisplayName",
    "places.regularOpeningHours",
    "places.internationalPhoneNumber",
    "places.websiteUri",
    "places.rating",
    "places.userRatingCount",
    "places.businessStatus",
    "places.editorialSummary",
    "places.googleMapsUri",
    "places.nationalPhoneNumber",
    "nextPageToken",
])

# Place types that indicate a food service organization
FOOD_TYPES = {
    "food_bank", "charitable_organization", "non_profit_organization",
    "social_services_organization", "community_center", "church",
    "meal_delivery", "homeless_shelter", "lodging", "local_government_office",
    "point_of_interest", "establishment",
}

EXCLUDE_TYPES = {
    "restaurant", "cafe", "bakery", "bar", "night_club", "store",
    "supermarket", "grocery_or_supermarket", "convenience_store",
    "meal_takeaway", "fast_food_restaurant", "food", "clothing_store",
    "shoe_store", "hardware_store", "car_dealer", "gas_station",
    "lodging", "hotel", "motel",
}


def places_search(query: str, lat: float, lon: float, radius: float,
                  page_token: str = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    body = {
        "textQuery": query,
        "maxResultCount": 20,
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": float(min(radius, 50000)),  # API max is 50,000m
            }
        },
    }
    if page_token:
        body["pageToken"] = page_token

    resp = requests.post(PLACES_URL, headers=headers, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def is_food_org(place: dict) -> bool:
    """Filter: keep places that look like food assistance organizations."""
    name  = place.get("displayName", {}).get("text", "").lower()
    types = set(place.get("types", []))

    # Strong positive keywords in name
    food_keywords = [
        "food", "pantry", "hunger", "meals", "feeding", "nourish",
        "harvest", "bread", "community kitchen", "soup kitchen",
        "emergency", "assistance", "aid", "relief", "mission",
        "ministry", "outreach", "snap", "familia", "familia",
        "care", "share", "banco de alimentos", "despensa",
    ]
    has_food_keyword = any(kw in name for kw in food_keywords)

    # Primary type exclusion
    primary = place.get("primaryType", "")
    if primary in EXCLUDE_TYPES:
        return False

    # Must have food keyword OR explicitly be a relevant type
    return has_food_keyword or bool(types & FOOD_TYPES - EXCLUDE_TYPES)


def extract_hours(place: dict) -> dict:
    """Parse opening hours into structured fields."""
    oh = place.get("regularOpeningHours", {})
    periods     = oh.get("periods", [])
    weekday_txt = oh.get("weekdayDescriptions", [])

    days_open = set()
    for p in periods:
        day = p.get("open", {}).get("day")  # 0=Sun, 1=Mon, …
        if day is not None:
            days_open.add(day)

    day_names = {0:"Sun",1:"Mon",2:"Tue",3:"Wed",4:"Thu",5:"Fri",6:"Sat"}
    return {
        "hours_text":      " | ".join(weekday_txt) if weekday_txt else None,
        "days_open":       ", ".join(day_names[d] for d in sorted(days_open)) if days_open else None,
        "open_weekends":   bool({0, 6} & days_open),
        "open_evenings":   any(
            p.get("close", {}).get("hour", 0) >= 17
            for p in periods
        ),
        "days_open_count": len(days_open),
    }


def classify_org(place: dict) -> str:
    name  = place.get("displayName", {}).get("text", "").lower()
    types = place.get("types", [])

    if any(x in name for x in ["food bank", "banco de alimentos", "feeding"]):
        return "Food Bank / Warehouse"
    if any(x in name for x in ["mobile", "truck", "route"]):
        return "Mobile Distribution"
    if "church" in types or any(x in name for x in [
        "church", "iglesia", "baptist", "catholic", "methodist",
        "presbyterian", "lutheran", "episcopal", "assembly of god",
        "seventh day", "church of christ", "united methodist",
        "first christian", "sanctuary", "chapel", "parish",
    ]):
        return "Faith-Based Pantry"
    if any(x in name for x in ["salvation army", "catholic charities",
                                "red cross", "united way", "ymca"]):
        return "Large Nonprofit"
    if any(x in name for x in [
        "community", "neighborhood", "centro", "center", "council",
        "action", "coalition", "cooperative",
    ]):
        return "Community Organization"
    if any(x in name for x in ["school", "elementary", "middle", "high school"]):
        return "School Program"
    if "local_government_office" in types or any(x in name for x in [
        "city", "county", "township", "municipal", "government",
    ]):
        return "Government Program"
    return "Other Nonprofit / Pantry"


def snap_likely(place: dict) -> bool:
    """Flag orgs likely to offer SNAP enrollment help."""
    name = place.get("displayName", {}).get("text", "").lower()
    snap_orgs = [
        "catholic charities", "salvation army", "community action",
        "legal aid", "health department", "wic", "snap", "benefits",
        "social services", "family services", "community services",
        "workforce", "united way", "ymca",
    ]
    return any(x in name for x in snap_orgs)


# ── Phase 1: Google Places scrape ────────────────────────────────────────────
def fetch_all_counties() -> pd.DataFrame:
    all_places = {}   # place_id -> raw dict
    county_hits = {}  # county -> list of place_ids

    total_queries = len(COUNTIES) * len(QUERIES)
    done = 0

    for county, (lat, lon, radius) in COUNTIES.items():
        county_hits[county] = []
        for query_base in QUERIES:
            query = f"{query_base} {county} County Texas"
            cache_key = f"{county}_{query_base.replace(' ','_')}.json"
            cache_file = CACHE_DIR / cache_key

            # Use cache if available
            if cache_file.exists():
                data = json.loads(cache_file.read_text())
            else:
                try:
                    data = places_search(query, lat, lon, radius)
                    cache_file.write_text(json.dumps(data))
                    time.sleep(0.15)   # ~6 req/sec — well under 600/min limit
                except Exception as e:
                    print(f"  [ERR] {county} / {query_base}: {e}")
                    done += 1
                    continue

            page_places = data.get("places", [])
            next_token  = data.get("nextPageToken")

            # Paginate once if available
            if next_token:
                p2_cache = CACHE_DIR / f"{cache_key}_p2.json"
                if p2_cache.exists():
                    data2 = json.loads(p2_cache.read_text())
                else:
                    try:
                        time.sleep(0.5)
                        data2 = places_search(query, lat, lon, radius,
                                              page_token=next_token)
                        p2_cache.write_text(json.dumps(data2))
                    except Exception:
                        data2 = {}
                page_places += data2.get("places", [])

            for p in page_places:
                pid = p.get("id")
                if pid and pid not in all_places:
                    all_places[pid] = p
                if pid:
                    county_hits[county].append(pid)

            done += 1
            if done % 50 == 0 or done == total_queries:
                print(f"  Queries: {done}/{total_queries} | Unique places so far: {len(all_places)}")

    # Build county affiliation — which counties does each place appear in
    place_counties = {}
    for county, pids in county_hits.items():
        for pid in pids:
            place_counties.setdefault(pid, set()).add(county)

    print(f"\nTotal unique places found: {len(all_places)}")

    # Filter to likely food orgs
    rows = []
    skipped = 0
    for pid, place in all_places.items():
        if not is_food_org(place):
            skipped += 1
            continue

        name    = place.get("displayName", {}).get("text", "")
        addr    = place.get("formattedAddress", "")
        loc     = place.get("location", {})
        hours   = extract_hours(place)
        summary = place.get("editorialSummary", {}).get("text", "")
        types   = place.get("types", [])
        status  = place.get("businessStatus", "")

        counties_list = sorted(place_counties.get(pid, set()))

        rows.append({
            "place_id":              pid,
            "name":                  name,
            "address":               addr,
            "latitude":              loc.get("latitude"),
            "longitude":             loc.get("longitude"),
            "county_matches":        ", ".join(counties_list),
            "primary_county":        counties_list[0] if counties_list else "",
            "org_type":              classify_org(place),
            "snap_enrollment_likely": snap_likely(place),
            "phone":                 place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber"),
            "website":               place.get("websiteUri"),
            "google_maps_url":       place.get("googleMapsUri"),
            "hours_text":            hours["hours_text"],
            "days_open":             hours["days_open"],
            "days_open_count":       hours["days_open_count"],
            "open_weekends":         hours["open_weekends"],
            "open_evenings":         hours["open_evenings"],
            "google_rating":         place.get("rating"),
            "google_review_count":   place.get("userRatingCount"),
            "business_status":       status,
            "primary_type":          place.get("primaryTypeDisplayName", {}).get("text", ""),
            "place_types":           ", ".join(types[:5]),
            "description":           summary,
            "distribution_frequency": None,   # survey needed
            "pounds_distributed":    None,   # survey needed
            "families_served":       None,   # survey needed
            "annual_budget":         None,   # survey needed
            "client_choice_model":   None,   # survey needed
            "language_access":       None,   # survey needed
            "source":                "Google Places",
        })

    print(f"Kept {len(rows)} food org results (skipped {skipped} non-food)")
    return pd.DataFrame(rows)


# ── Phase 2: USDA Food Access Research Atlas ─────────────────────────────────
def fetch_usda_fara() -> pd.DataFrame:
    print("\nFetching USDA Food Access Research Atlas...")
    url = "https://www.ers.usda.gov/webdocs/DataFiles/80591/DataDownload2019.xlsx"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        xl = pd.ExcelFile(BytesIO(r.content))
        print(f"  Sheets: {xl.sheet_names}")
        # County-level summary sheet
        sheet = next((s for s in xl.sheet_names if "county" in s.lower()), xl.sheet_names[0])
        df = pd.read_excel(BytesIO(r.content), sheet_name=sheet)
        print(f"  FARA: {len(df)} counties, cols: {df.columns[:10].tolist()}")
        return df
    except Exception as e:
        print(f"  FARA download failed: {e}")
        return pd.DataFrame()


# ── Phase 3: Feeding America Map the Meal Gap ─────────────────────────────────
def fetch_map_meal_gap() -> pd.DataFrame:
    print("\nFetching Feeding America Map the Meal Gap data...")
    # MMG publishes county-level data; try the most recent known URL pattern
    urls_to_try = [
        "https://www.feedingamerica.org/sites/default/files/2024-03/Map%20the%20Meal%20Gap%20Data%20by%20County%20in%202022.xlsx",
        "https://map.feedingamerica.org/county/2022/overall/texas/county",
    ]
    for url in urls_to_try:
        try:
            r = requests.get(url, timeout=30,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            if "xlsx" in url or "excel" in r.headers.get("content-type","").lower():
                df = pd.read_excel(BytesIO(r.content))
                print(f"  MMG: {len(df)} rows")
                return df
        except Exception as e:
            print(f"  MMG attempt failed ({url[:60]}...): {e}")

    # Fallback: known 2022 MMG Texas county data via direct spreadsheet
    print("  Trying USDA ERS supplemental food insecurity data...")
    try:
        url = "https://www.ers.usda.gov/webdocs/DataFiles/50764/Table1D.xlsx"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df = pd.read_excel(BytesIO(r.content), skiprows=4)
        print(f"  ERS food insecurity: {len(df)} rows")
        return df
    except Exception as e:
        print(f"  ERS fallback also failed: {e}")
    return pd.DataFrame()


# ── Phase 4: 211 Texas via findhelp.org ──────────────────────────────────────
def fetch_211_texas(sample_counties=None) -> pd.DataFrame:
    """
    Attempts to pull food pantry listings from Texas 211 / findhelp.
    Uses the open search endpoint — returns partial data.
    """
    print("\nFetching 211 Texas data (findhelp.org)...")
    rows = []
    counties_to_check = sample_counties or list(COUNTIES.keys())[:20]

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; research bot)",
        "Accept": "application/json",
    }

    for county in counties_to_check[:20]:   # cap at 20 to avoid overloading
        lat, lon, _ = COUNTIES[county]
        url = (
            f"https://www.findhelp.org/search#"
            f"?lat={lat}&lng={lon}&rad=50&keywords=food+pantry"
        )
        try:
            # findhelp API endpoint (public, no auth)
            api_url = (
                f"https://www.findhelp.org/api/services/search"
                f"?lat={lat}&lng={lon}&category=food&radius=50"
            )
            r = requests.get(api_url, headers=headers, timeout=15)
            if r.status_code == 200:
                data = r.json()
                for item in data.get("data", [])[:20]:
                    rows.append({
                        "name":          item.get("name"),
                        "address":       item.get("address", {}).get("address1"),
                        "city":          item.get("address", {}).get("city"),
                        "phone":         item.get("phone"),
                        "website":       item.get("website"),
                        "description":   item.get("description"),
                        "primary_county": county,
                        "source":        "211 Texas",
                    })
            time.sleep(0.5)
        except Exception:
            pass

    print(f"  211 Texas: {len(rows)} results")
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Phase 5: Gap analysis by county ─────────────────────────────────────────
def build_gap_analysis(pantries: pd.DataFrame, fara: pd.DataFrame,
                       mmg: pd.DataFrame, snap_df: pd.DataFrame) -> pd.DataFrame:
    print("\nBuilding gap analysis by county...")

    # County-level SNAP enrollment (latest available month)
    if not snap_df.empty:
        latest_date = snap_df["date"].max()
        snap_latest = (
            snap_df[snap_df["date"] == latest_date]
            [["county","eligible_individuals","cases"]]
            .copy()
        )
    else:
        snap_latest = pd.DataFrame(columns=["county","eligible_individuals","cases"])

    # Pantry count by county
    pantry_counts = pantries.groupby("primary_county").agg(
        total_sites         = ("place_id","count"),
        food_banks          = ("org_type", lambda x: (x=="Food Bank / Warehouse").sum()),
        faith_pantries      = ("org_type", lambda x: (x=="Faith-Based Pantry").sum()),
        community_orgs      = ("org_type", lambda x: (x=="Community Organization").sum()),
        mobile_sites        = ("org_type", lambda x: (x=="Mobile Distribution").sum()),
        snap_sites          = ("snap_enrollment_likely", "sum"),
        sites_open_weekends = ("open_weekends", "sum"),
        sites_open_evenings = ("open_evenings", "sum"),
        avg_days_open       = ("days_open_count", "mean"),
        has_hours_data      = ("hours_text", lambda x: x.notna().sum()),
    ).reset_index().rename(columns={"primary_county":"county"})

    # Merge with SNAP data
    gap = pd.DataFrame({"county": list(COUNTIES.keys())})
    gap = gap.merge(snap_latest, on="county", how="left")
    gap = gap.merge(pantry_counts, on="county", how="left")

    # Fill missing pantry data with 0
    pantry_cols = ["total_sites","food_banks","faith_pantries","community_orgs",
                   "mobile_sites","snap_sites","sites_open_weekends","sites_open_evenings"]
    for col in pantry_cols:
        gap[col] = gap[col].fillna(0).astype(int)

    # SNAP enrolled per site (coverage ratio proxy)
    gap["snap_enrolled_per_site"] = (
        gap["eligible_individuals"] / gap["total_sites"].replace(0, float("nan"))
    ).round(0)

    # Flag counties with no sites
    gap["no_pantry_found"] = gap["total_sites"] == 0

    # Flag counties with no SNAP assistance
    gap["no_snap_assistance"] = gap["snap_sites"] == 0

    # Add HHS region
    COUNTY_REGION = {
        "Colorado":"06","Matagorda":"06",
        "Bastrop":"07","Blanco":"07","Burnet":"07","Caldwell":"07","Fayette":"07",
        "Hays":"07","Lampasas":"07","Llano":"07","Mills":"07","San Saba":"07","Travis":"07",
        "Atascosa":"08","Bandera":"08","Bexar":"08","Calhoun":"08","Comal":"08",
        "De Witt":"08","Dimmit":"08","Edwards":"08","Frio":"08","Gillespie":"08",
        "Goliad":"08","Gonzales":"08","Guadalupe":"08","Jackson":"08","Karnes":"08",
        "Kendall":"08","Kerr":"08","Kinney":"08","La Salle":"08","Lavaca":"08",
        "Maverick":"08","Medina":"08","Real":"08","Uvalde":"08","Val Verde":"08",
        "Victoria":"08","Wilson":"08","Zavala":"08",
        "Coke":"09","Concho":"09","Crockett":"09","Irion":"09","Kimble":"09",
        "Mason":"09","McCulloch":"09","Menard":"09","Reagan":"09","Schleicher":"09",
        "Sterling":"09","Sutton":"09","Tom Green":"09","Upton":"09",
        "Aransas":"11","Bee":"11","Brooks":"11","Cameron":"11","Duval":"11",
        "Hidalgo":"11","Jim Hogg":"11","Jim Wells":"11","Kenedy":"11","Kleberg":"11",
        "Live Oak":"11","McMullen":"11","Nueces":"11","Refugio":"11","San Patricio":"11",
        "Starr":"11","Webb":"11","Willacy":"11","Zapata":"11",
    }
    gap["hhs_region"] = gap["county"].map(COUNTY_REGION)

    # Gap priority score (higher = more underserved):
    #   no pantry found: +3
    #   <2 sites and >500 enrolled: +2
    #   no SNAP assistance: +1
    #   no weekend/evening hours: +1
    #   snap_enrolled_per_site > 2000: +1
    gap["gap_score"] = (
        gap["no_pantry_found"].astype(int) * 3 +
        ((gap["total_sites"] < 2) & (gap["eligible_individuals"].fillna(0) > 500)).astype(int) * 2 +
        gap["no_snap_assistance"].astype(int) +
        ((gap["sites_open_weekends"] == 0) & (gap["total_sites"] > 0)).astype(int) +
        (gap["snap_enrolled_per_site"].fillna(9999) > 2000).astype(int)
    )

    gap = gap.sort_values("gap_score", ascending=False).reset_index(drop=True)
    return gap


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("MHM Food Pantry Data Collection")
    print("=" * 60)

    # 1. Google Places
    print(f"\n[Phase 1] Google Places API — {len(COUNTIES)} counties x {len(QUERIES)} queries")
    pantries = fetch_all_counties()
    pantries.to_excel("food_pantry_raw.xlsx", index=False)
    print(f"  Saved {len(pantries)} records -> food_pantry_raw.xlsx")

    # 2. USDA FARA
    fara = fetch_usda_fara()
    if not fara.empty:
        fara.to_excel("usda_fara_raw.xlsx", index=False)
        print(f"  Saved USDA FARA -> usda_fara_raw.xlsx")

    # 3. Map the Meal Gap
    mmg = fetch_map_meal_gap()
    if not mmg.empty:
        mmg.to_excel("map_meal_gap_raw.xlsx", index=False)

    # 4. 211 Texas (try all counties)
    tex211 = fetch_211_texas(sample_counties=list(COUNTIES.keys()))
    if not tex211.empty:
        tex211.to_excel("tex211_raw.xlsx", index=False)
        print(f"  Saved 211 Texas -> tex211_raw.xlsx ({len(tex211)} rows)")

    # 5. Gap analysis
    snap_df = pd.DataFrame()
    try:
        snap_raw = pd.read_excel("snap_mhm_counties_2022_feb2026.xlsx")
        snap_raw["date"] = pd.to_datetime(snap_raw[["year","month"]].assign(day=1))
        snap_df = snap_raw
    except Exception:
        print("  [WARN] Could not load SNAP enrollment data for gap analysis")

    gap = build_gap_analysis(pantries, fara, mmg, snap_df)
    gap.to_excel("food_access_county.xlsx", index=False)
    print(f"\nGap analysis: {len(gap)} counties -> food_access_county.xlsx")

    # 6. Clean final catalog
    # Mark permanently closed
    active = pantries[pantries["business_status"] != "CLOSED_PERMANENTLY"].copy()
    active.to_excel("food_pantry_catalog.xlsx", index=False)
    print(f"Final catalog: {len(active)} active sites -> food_pantry_catalog.xlsx")

    # Summary stats
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total sites found:          {len(pantries)}")
    print(f"Active sites:               {len(active)}")
    print(f"Counties with no pantry:    {(gap['total_sites']==0).sum()}")
    print(f"Counties with 1-2 sites:    {((gap['total_sites']>=1)&(gap['total_sites']<=2)).sum()}")
    print(f"Counties with SNAP assist:  {(gap['snap_sites']>0).sum()}")
    print(f"Sites w/ weekend hours:     {active['open_weekends'].sum()}")
    print(f"\nOrg type breakdown:")
    print(active["org_type"].value_counts().to_string())
    print(f"\nTop 15 gap counties (most underserved):")
    cols = ["county","hhs_region","total_sites","snap_enrolled_per_site",
            "snap_sites","no_pantry_found","gap_score"]
    print(gap[cols].head(15).to_string(index=False))


if __name__ == "__main__":
    main()
