"""
fetch_snap_outreach.py
Scrapes Texas Community Partner Program (CPP) for certified SNAP enrollment
assistance organizations across the 74 MHM counties, then cross-references
against food_pantry_catalog_clean.xlsx to update snap_enrollment_likely.

CPP site: https://www.texascommunitypartnerprogram.com/
toh=2 = SNAP enrollment assistance (Level 2 = highest active-assistance tier)
"""

import requests
import urllib3
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
from rapidfuzz import fuzz, process

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.texascommunitypartnerprogram.com/TCPP_Site_FindPartnerResults"
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://www.texascommunitypartnerprogram.com/",
}

# County seat ZIP codes — used as query anchor for CPP radius search
COUNTY_ZIPS = {
    "Aransas":"78382","Atascosa":"78026","Bandera":"78003","Bastrop":"78602",
    "Bee":"78102","Bexar":"78201","Blanco":"78636","Brooks":"78355",
    "Burnet":"78611","Caldwell":"78644","Calhoun":"77979","Cameron":"78520",
    "Comal":"78130","Concho":"76866","Colorado":"78934","Coke":"76945",
    "Crockett":"76943","De Witt":"77954","Dimmit":"78834","Duval":"78384",
    "Edwards":"78880","Fayette":"78945","Frio":"78061","Gillespie":"78624",
    "Goliad":"77963","Gonzales":"78629","Guadalupe":"78155","Hays":"78666",
    "Hidalgo":"78539","Irion":"76941","Jackson":"77957","Jim Hogg":"78361",
    "Jim Wells":"78332","Karnes":"78118","Kendall":"78006","Kenedy":"78385",
    "Kerr":"78028","Kimble":"76849","Kinney":"78832","Kleberg":"78363",
    "La Salle":"78014","Lampasas":"76550","Lavaca":"77964","Live Oak":"78022",
    "Llano":"78643","Mason":"76856","Matagorda":"77414","Maverick":"78852",
    "McCulloch":"76825","McMullen":"78072","Medina":"78861","Menard":"76859",
    "Mills":"76844","Nueces":"78401","Reagan":"76932","Real":"78873",
    "Refugio":"78377","San Patricio":"78387","San Saba":"76877",
    "Schleicher":"76936","Starr":"78582","Sterling":"76951","Sutton":"76950",
    "Tom Green":"76901","Travis":"78701","Upton":"79778","Uvalde":"78801",
    "Val Verde":"78840","Victoria":"77901","Webb":"78040","Willacy":"78580",
    "Wilson":"78114","Zapata":"78076","Zavala":"78839",
}
MHM_COUNTIES = list(COUNTY_ZIPS.keys())


def fetch_county(county_name: str, toh: int = 2, mr: int = 100) -> list[dict]:
    """Query CPP for a county via county seat ZIP. Returns list of org dicts."""
    zip_code = COUNTY_ZIPS.get(county_name, "")
    if not zip_code:
        return []
    params = {
        "zc":  zip_code,
        "sc":  "",
        "sco": "",
        "mr":  mr,
        "toh": toh,
        "lang": "",
    }
    try:
        r = requests.get(BASE_URL, params=params, headers=HEADERS,
                         verify=False, timeout=30)
        r.raise_for_status()
        return extract_orgs(r.text, county_name)
    except Exception as e:
        print(f"  [WARN] {county_name}: {e}")
        return []


def extract_orgs(html: str, county_hint: str = "") -> list[dict]:
    """Parse org records from a CPP results page."""
    soup = BeautifulSoup(html, "html.parser")
    orgs = []

    name_anchors = soup.find_all("a", onclick=re.compile(r"showDetail\(\d+\)"))
    for anchor in name_anchors:
        b_tag = anchor.find("b")
        if not b_tag:
            continue
        org_name = b_tag.get_text(strip=True)

        header_div = anchor.parent
        if header_div is None:
            continue

        img = header_div.find("img")
        icon_src = img.get("src", "") if img else ""
        level_match = re.search(r"number-(\d+)-icon", icon_src)
        level = int(level_match.group(1)) if level_match else None

        street = city_state_zip = ""
        sib = header_div.find_next_sibling("div")
        if sib:
            street = sib.get_text(strip=True)
            sib2 = sib.find_next_sibling("div")
            if sib2:
                ct = sib2.get_text(strip=True)
                if re.match(r".+,\s*(TX|Texas)\s+\d{5}", ct, re.I):
                    city_state_zip = ct
                else:
                    street = ct

        # Extract ZIP
        zip_match = re.search(r"\b(\d{5})\b", city_state_zip)
        zip_code = zip_match.group(1) if zip_match else ""

        orgs.append({
            "name":           org_name,
            "street":         street,
            "city_state_zip": city_state_zip,
            "zip_code":       zip_code,
            "full_address":   ", ".join(filter(None, [street, city_state_zip])),
            "service_level":  level,
            "query_county":   county_hint,
        })

    return orgs


def scrape_all_counties() -> pd.DataFrame:
    all_orgs = []
    for i, county in enumerate(MHM_COUNTIES, 1):
        orgs = fetch_county(county)
        print(f"  [{i:2d}/74] {county:<15} -> {len(orgs)} orgs")
        all_orgs.extend(orgs)
        time.sleep(0.4)          # polite crawl rate

    df = pd.DataFrame(all_orgs)
    if df.empty:
        return df

    # Deduplicate on name + city (same org appears for multiple county queries)
    df["_dedup_key"] = (
        df["name"].str.lower().str.strip()
        + "|"
        + df["city_state_zip"].str.lower().str.strip()
    )
    df = df.drop_duplicates(subset="_dedup_key").drop(columns="_dedup_key")
    df = df.reset_index(drop=True)
    return df


def cross_reference(cpp_df: pd.DataFrame, catalog_path: str) -> pd.DataFrame:
    """
    Fuzzy-match CPP orgs against the pantry catalog.
    Returns catalog with updated snap_enrollment_likely and new snap_source column.
    """
    cat = pd.read_excel(catalog_path)

    # Build match key: name + city extracted from address
    def city_from_addr(addr):
        if pd.isna(addr):
            return ""
        # "123 Main St, San Antonio, TX 78207" → "san antonio"
        parts = str(addr).split(",")
        return parts[-3].strip().lower() if len(parts) >= 3 else ""

    cat["_city"]     = cat["address"].apply(city_from_addr)
    cat["_name_lc"]  = cat["name"].str.lower().str.strip()
    cat["_match_key"]= cat["_name_lc"] + " " + cat["_city"]

    cpp_df["_city_lc"]   = cpp_df["city_state_zip"].str.split(",").str[0].str.lower().str.strip()
    cpp_df["_name_lc"]   = cpp_df["name"].str.lower().str.strip()
    cpp_df["_match_key"] = cpp_df["_name_lc"] + " " + cpp_df["_city_lc"]

    cpp_keys = cpp_df["_match_key"].tolist()

    verified_idx  = set()
    match_details = {}

    for idx, row in cat.iterrows():
        result = process.extractOne(
            row["_match_key"],
            cpp_keys,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=72,
        )
        if result is not None:
            matched_key, score, cpp_idx = result
            matched_row = cpp_df.iloc[cpp_idx]
            verified_idx.add(idx)
            match_details[idx] = {
                "cpp_name":    matched_row["name"],
                "cpp_address": matched_row["full_address"],
                "match_score": score,
            }

    # Update snap_enrollment_likely
    # 0 = not verified, 1 = name-heuristic only, 2 = CPP verified
    cat["snap_source"] = "heuristic"
    cat.loc[list(verified_idx), "snap_enrollment_likely"] = 1
    cat.loc[list(verified_idx), "snap_source"] = "CPP verified"

    # Also mark sites that were already flagged but not CPP-matched as heuristic
    previously_flagged = cat["snap_enrollment_likely"] == 1
    cpp_matched        = cat["snap_source"] == "CPP verified"
    cat.loc[previously_flagged & ~cpp_matched, "snap_source"] = "heuristic"

    # Drop temp columns
    cat.drop(columns=["_city","_name_lc","_match_key"], inplace=True)

    print(f"\nCross-reference results:")
    print(f"  CPP orgs in service area:      {len(cpp_df)}")
    print(f"  Catalog sites CPP-verified:    {len(verified_idx)}")
    print(f"  Previously heuristic-only:     {(cat['snap_source']=='heuristic').sum()}")
    print(f"  Total snap_enrollment_likely:  {cat['snap_enrollment_likely'].sum()}")

    return cat, match_details


def main():
    print("=== Scraping Texas CPP (SNAP enrollment assistance) ===")
    print(f"Querying {len(MHM_COUNTIES)} counties for toh=2 (SNAP assist) orgs...\n")

    cpp_df = scrape_all_counties()
    print(f"\nTotal unique CPP SNAP orgs found: {len(cpp_df)}")
    cpp_df.to_excel("cpp_snap_orgs.xlsx", index=False)
    print("Saved → cpp_snap_orgs.xlsx")

    print("\n=== Cross-referencing against pantry catalog ===")
    try:
        from rapidfuzz import fuzz, process
    except ImportError:
        print("Installing rapidfuzz...")
        import subprocess, sys
        subprocess.run([sys.executable, "-m", "pip", "install", "rapidfuzz"], check=True)
        from rapidfuzz import fuzz, process

    updated_cat, matches = cross_reference(cpp_df, "food_pantry_catalog_clean.xlsx")
    updated_cat.to_excel("food_pantry_catalog_clean.xlsx", index=False)
    print("Updated → food_pantry_catalog_clean.xlsx")

    print("\n=== Sample CPP-verified matches ===")
    for idx, det in list(matches.items())[:10]:
        cat_name = updated_cat.loc[idx, "name"]
        print(f"  Catalog: {cat_name!r}")
        print(f"  CPP:     {det['cpp_name']!r}  (score {det['match_score']})")
        print()

    print("\n=== CPP orgs NOT matched to any catalog site (potential gaps) ===")
    matched_cpp_keys = set()
    for det in matches.values():
        matched_cpp_keys.add(det["cpp_name"].lower().strip())
    unmatched = cpp_df[~cpp_df["name"].str.lower().str.strip().isin(matched_cpp_keys)]
    print(f"  {len(unmatched)} CPP orgs have no catalog match")
    unmatched[["name","full_address","query_county"]].head(20).to_string(index=False)
    print(unmatched[["name","full_address","query_county"]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
