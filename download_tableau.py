"""
download_tableau.py
Downloads missing SNAP monthly data from Texas HHS Tableau dashboard (Sep 2025 - Feb 2026).
July 2025 confirmed NOT available in Tableau — only 6 months to pull.
"""

import time
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright

TABLEAU_URL = (
    "https://pmas-tableau-iamo.hhs.state.tx.us"
    "/t/PMD/views/SNAP_Web_Report/County_SNAP"
    "?%3Aembed=y&%3AisGuestRedirectFromVizportal=y"
)

# July 2025 is NOT in Tableau — only Sep 2025 onward
MISSING_MONTHS = [
    ("September 2025", 2025,  9),
    ("October 2025",   2025, 10),
    ("November 2025",  2025, 11),
    ("December 2025",  2025, 12),
    ("January 2026",   2026,  1),
    ("February 2026",  2026,  2),
]

MHM_COUNTIES = {
    "Aransas","Atascosa","Bandera","Bastrop","Bee","Bexar","Blanco",
    "Brooks","Burnet","Caldwell","Calhoun","Cameron","Comal","Concho",
    "Colorado","Coke","Crockett","De Witt","Dimmit","Duval","Edwards",
    "Fayette","Frio","Gillespie","Goliad","Gonzales","Guadalupe","Hays",
    "Hidalgo","Irion","Jackson","Jim Hogg","Jim Wells","Karnes","Kendall",
    "Kenedy","Kerr","Kimble","Kinney","Kleberg","La Salle","Lampasas",
    "Lavaca","Live Oak","Llano","Matagorda","Maverick","Mason","McCulloch",
    "McMullen","Medina","Menard","Mills","Nueces","Reagan","Real",
    "Refugio","San Patricio","San Saba","Schleicher","Starr","Sterling",
    "Sutton","Tom Green","Travis","Upton","Uvalde","Val Verde","Victoria",
    "Webb","Willacy","Wilson","Zapata","Zavala",
}

county_lookup = {"".join(c.lower().split()): c for c in MHM_COUNTIES}

OUTPUT_COLS = [
    "county","year","month",
    "cases","eligible_individuals",
    "age_under5","age_5_17","age_18_59","age_60_64","age_65plus",
    "total_snap_payments","avg_payment_per_case",
]

OUT_DIR = Path("tableau_downloads")
OUT_DIR.mkdir(exist_ok=True)


def select_month_and_download(page, label, year, month):
    csv_path = OUT_DIR / f"snap_county_{year}_{month:02d}.csv"
    if csv_path.exists():
        print(f"  {label}: already downloaded")
        return csv_path

    # Open month filter
    page.get_by_role("button", name="Filter Month Inclusive").click()
    time.sleep(2)

    # Uncheck everything that's currently checked
    checked = page.locator('[role="checkbox"][aria-checked="true"]').all()
    for cb in checked:
        cb.click()
        time.sleep(0.3)

    # Check our target month
    target = page.get_by_role("checkbox", name=label)
    if not target.is_visible():
        print(f"  {label}: checkbox not found!")
        page.keyboard.press("Escape")
        return None
    target.click()
    time.sleep(0.5)

    # Click Apply
    page.get_by_role("button", name="Apply").click()
    time.sleep(3)  # Wait for chart to refresh

    # Download → Crosstab → CSV → Download
    page.get_by_role("button", name="Download").click()
    time.sleep(1)
    page.get_by_role("menuitem", name="Crosstab").click()
    time.sleep(1)
    page.get_by_text("CSV").click()
    time.sleep(0.5)

    with page.expect_download(timeout=30000) as dl_info:
        page.get_by_role("button", name="Download").last.click()

    dl = dl_info.value
    dl.save_as(csv_path)
    print(f"  {label}: saved -> {csv_path}")
    return csv_path


def parse_tableau_csv(csv_path, year, month):
    """
    Tableau Crosstab CSV format:
    - Row 0: blank, then county names across columns
    - Rows 1+: measure name in col 0, then values per county
    """
    df = pd.read_csv(csv_path, header=None, encoding="utf-8-sig", low_memory=False)

    counties_raw = [str(v).strip() for v in df.iloc[0, 1:].tolist()]

    measures = {}
    for _, row in df.iloc[1:].iterrows():
        key = str(row.iloc[0]).strip()
        vals = [str(v).strip() for v in row.iloc[1:].tolist()]
        if key and key != "nan":
            measures[key] = vals

    measure_map = {
        "Number of Cases":               "cases",
        "Number of Eligible Individuals": "eligible_individuals",
        "Individuals: Age <5":            "age_under5",
        "Individuals: Age 5-17":          "age_5_17",
        "Individuals: Age 18-59":         "age_18_59",
        "Individuals: Age 60-64":         "age_60_64",
        "Individuals: Age 65+":           "age_65plus",
        "Total SNAP Payments":            "total_snap_payments",
        "Average Payment/Case":           "avg_payment_per_case",
    }

    rows = []
    for i, raw in enumerate(counties_raw):
        norm = "".join(raw.lower().split())
        canonical = county_lookup.get(norm)
        if not canonical:
            continue

        row = {"county": canonical, "year": year, "month": month}
        for measure_raw, col_name in measure_map.items():
            # Find the measure (partial match for robustness)
            val_list = next(
                (v for k, v in measures.items() if measure_raw.lower() in k.lower()), None
            )
            val = val_list[i] if val_list and i < len(val_list) else None
            row[col_name] = pd.to_numeric(str(val).replace(",", ""), errors="coerce") if val else None
        rows.append(row)

    result = pd.DataFrame(rows)
    return result[OUTPUT_COLS] if not result.empty else pd.DataFrame(columns=OUTPUT_COLS)


def main():
    print("Launching browser...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print("Loading Tableau dashboard...")
        page.goto(TABLEAU_URL, timeout=60000)
        time.sleep(8)  # Tableau SPA needs time to fully render

        downloaded = []
        for label, year, month in MISSING_MONTHS:
            print(f"Fetching {label}...")
            try:
                path = select_month_and_download(page, label, year, month)
                if path:
                    downloaded.append((path, year, month, label))
            except Exception as e:
                print(f"  ERROR: {e}")
            time.sleep(1)

        browser.close()

    print(f"\n{len(downloaded)} files downloaded. Parsing...")

    new_frames = []
    for path, year, month, label in downloaded:
        try:
            frame = parse_tableau_csv(path, year, month)
            if not frame.empty:
                new_frames.append(frame)
                print(f"  {label}: {len(frame)} MHM counties")
            else:
                print(f"  {label}: parse returned empty — check {path}")
        except Exception as e:
            print(f"  {label}: parse error — {e}")

    if not new_frames:
        print("Nothing to merge.")
        return

    existing = pd.read_excel("snap_mhm_counties_2022_aug2025.xlsx")
    combined = pd.concat([existing] + new_frames, ignore_index=True)
    combined = combined.sort_values(["county","year","month"]).reset_index(drop=True)

    out = "snap_mhm_counties_2022_feb2026.xlsx"
    combined.to_excel(out, index=False)
    print(f"\nDone. {len(combined):,} rows -> {out}")
    print(f"Counties: {combined['county'].nunique()} | Months: {combined[['year','month']].drop_duplicates().shape[0]}")


if __name__ == "__main__":
    main()
