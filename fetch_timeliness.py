"""
fetch_timeliness.py
Downloads TX HHSC SNAP timeliness reports (Jan 2022 – Jan 2026),
extracts application and redetermination timeliness % by HHS region,
then combines with our MHM enrollment data and FRED statewide enrollment
to test the administrative burden hypothesis.
"""

import requests
import pandas as pd
import numpy as np
from io import BytesIO

BASE = "https://www.hhs.texas.gov"

# Month labels used in filenames (same pattern as enrollment files)
MONTH_SLUGS = {
    1:"jan", 2:"feb", 3:"march", 4:"april", 5:"may", 6:"june",
    7:"july", 8:"aug", 9:"sept", 10:"oct", 11:"nov", 12:"dec"
}

# All months to fetch
TIMELINESS_MONTHS = (
    [(2022, m) for m in range(1, 13)] +
    [(2023, m) for m in range(1, 13)] +
    [(2024, m) for m in range(1, 13)] +
    [(2025, m) for m in range(1, 13)] +
    [(2026, 1)]
)

# HHS regions present in the MHM service area
MHM_REGIONS = {"06", "07", "08", "09", "11"}

# Region 09 is reported as "02/09" in the timeliness files
REGION_NORMALIZE = {"02/09": "09"}

# County -> HHS Region mapping (74 MHM counties)
COUNTY_REGION = {
    # Region 06
    "Colorado":"06","Matagorda":"06",
    # Region 07
    "Bastrop":"07","Blanco":"07","Burnet":"07","Caldwell":"07","Fayette":"07",
    "Hays":"07","Lampasas":"07","Llano":"07","Mills":"07","San Saba":"07","Travis":"07",
    # Region 08
    "Atascosa":"08","Bandera":"08","Bexar":"08","Calhoun":"08","Comal":"08",
    "De Witt":"08","Dimmit":"08","Edwards":"08","Frio":"08","Gillespie":"08",
    "Goliad":"08","Gonzales":"08","Guadalupe":"08","Jackson":"08","Karnes":"08",
    "Kendall":"08","Kerr":"08","Kinney":"08","La Salle":"08","Lavaca":"08",
    "Maverick":"08","Medina":"08","Real":"08","Uvalde":"08","Val Verde":"08",
    "Victoria":"08","Wilson":"08","Zavala":"08",
    # Region 09 (reported as 02/09 in timeliness files)
    "Coke":"09","Concho":"09","Crockett":"09","Irion":"09","Kimble":"09",
    "Mason":"09","McCulloch":"09","Menard":"09","Reagan":"09","Schleicher":"09",
    "Sterling":"09","Sutton":"09","Tom Green":"09","Upton":"09",
    # Region 11
    "Aransas":"11","Bee":"11","Brooks":"11","Cameron":"11","Duval":"11",
    "Hidalgo":"11","Jim Hogg":"11","Jim Wells":"11","Kenedy":"11","Kleberg":"11",
    "Live Oak":"11","McMullen":"11","Nueces":"11","Refugio":"11","San Patricio":"11",
    "Starr":"11","Webb":"11","Willacy":"11","Zapata":"11",
}


def parse_timeliness(content: bytes, year: int, month: int) -> pd.DataFrame | None:
    """
    Parse one timeliness Excel file.
    Returns one row per HHS region with:
      year, month, region, app_disposed, app_timely, app_pct, redet_disposed, redet_timely, redet_pct
    """
    xl = pd.ExcelFile(BytesIO(content))
    # Always prefer the SNAP Food Benefits sheet; fall back to first sheet with data
    sheet = next((s for s in xl.sheet_names if "snap" in s.lower() and "food" in s.lower()),
                 xl.sheet_names[0])
    df = pd.read_excel(BytesIO(content), sheet_name=sheet, header=None)
    df = df.iloc[:, :4]
    df.columns = ["region", "disposed", "timely", "pct"]
    df["region"] = df["region"].astype(str).str.strip()

    rows = []
    section = None

    for _, row in df.iterrows():
        r = row["region"]
        if "APPLICATION" in r.upper():
            section = "app"
            continue
        if "REDETERMINATION" in r.upper():
            section = "redet"
            continue
        if section is None:
            continue
        # Normalize "02/09" -> "09"
        region_key = REGION_NORMALIZE.get(r, r)
        # Only keep numeric regions in MHM service area
        if region_key not in MHM_REGIONS:
            continue
        try:
            disposed = int(row["disposed"]) if pd.notna(row["disposed"]) else None
            timely   = int(row["timely"])   if pd.notna(row["timely"])   else None
            pct      = float(row["pct"])    if pd.notna(row["pct"])      else None
        except (ValueError, TypeError):
            continue
        rows.append({
            "year": year, "month": month,
            "region": region_key, "section": section,
            "disposed": disposed, "timely": timely, "pct": pct,
        })

    if not rows:
        return None

    # Pivot so each region has one row with both app_ and redet_ columns
    raw = pd.DataFrame(rows)
    app   = raw[raw["section"] == "app"].drop(columns="section").rename(
        columns={"disposed":"app_disposed","timely":"app_timely","pct":"app_pct"})
    redet = raw[raw["section"] == "redet"].drop(columns="section").rename(
        columns={"disposed":"redet_disposed","timely":"redet_timely","pct":"redet_pct"})

    merged = app.merge(redet, on=["year","month","region"], how="outer")
    return merged


def fetch_fred_texas():
    """Download FRED monthly SNAP enrollment for all of Texas (statewide)."""
    # FRED series BRTX48M647NCEN = SNAP beneficiaries in Texas
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BRTX48M647NCEN"
    print("Fetching FRED Texas statewide enrollment...", end=" ")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        fred = pd.read_csv(BytesIO(r.content))
        fred.columns = ["date", "tx_statewide_enrolled"]
        fred["date"] = pd.to_datetime(fred["date"])
        fred["tx_statewide_enrolled"] = pd.to_numeric(fred["tx_statewide_enrolled"], errors="coerce")
        fred["year"]  = fred["date"].dt.year
        fred["month"] = fred["date"].dt.month
        fred = fred[["year","month","tx_statewide_enrolled"]].dropna()
        print(f"OK ({len(fred)} months, {fred['date'].min() if 'date' in fred else ''} – latest: {fred['year'].max()}-{fred['month'].max():02d})")
        return fred
    except Exception as e:
        print(f"FAILED ({e})")
        return None


def main():
    # -- 1. Download timeliness reports ----------------------------------------
    all_frames = []
    for year, month in TIMELINESS_MONTHS:
        slug  = MONTH_SLUGS[month]
        path  = f"/sites/default/files/documents/timeliness-snap-{slug}-{year}.xlsx"
        url   = BASE + path
        label = f"{slug.capitalize()} {year}"
        print(f"Fetching timeliness {label}...", end=" ")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"FAILED ({e})")
            continue
        frame = parse_timeliness(r.content, year, month)
        if frame is not None and not frame.empty:
            all_frames.append(frame)
            regions = frame["region"].unique().tolist()
            print(f"OK (regions: {regions})")
        else:
            print("SKIPPED (no MHM regions found)")

    if not all_frames:
        print("No timeliness data collected.")
        return

    timeliness = pd.concat(all_frames, ignore_index=True)
    timeliness = timeliness.sort_values(["region","year","month"]).reset_index(drop=True)
    timeliness.to_excel("snap_timeliness_by_region.xlsx", index=False)
    print(f"\nTimeliness: {len(timeliness)} rows -> snap_timeliness_by_region.xlsx")

    # -- 2. FRED statewide ----------------------------------------------------─
    fred = fetch_fred_texas()
    if fred is not None:
        fred.to_excel("snap_fred_texas_statewide.xlsx", index=False)
        print(f"FRED: {len(fred)} rows -> snap_fred_texas_statewide.xlsx")

    # -- 3. MHM enrollment aggregated by region --------------------------------
    print("\nAggregating MHM enrollment by region...")
    mhm = pd.read_excel("snap_mhm_counties_2022_feb2026.xlsx")
    mhm["region"] = mhm["county"].map(COUNTY_REGION)
    unmapped = mhm[mhm["region"].isna()]["county"].unique()
    if len(unmapped):
        print(f"  [WARN] Unmapped counties: {unmapped}")
    mhm_region = (
        mhm.groupby(["year","month","region"])[["eligible_individuals","cases","total_snap_payments"]]
        .sum().reset_index()
    )
    mhm_region.to_excel("snap_mhm_by_region.xlsx", index=False)
    print(f"MHM by region: {len(mhm_region)} rows -> snap_mhm_by_region.xlsx")

    # -- 4. Build combined analysis dataset ------------------------------------
    combined = mhm_region.merge(timeliness, on=["year","month","region"], how="left")

    # Add statewide FRED as context (not region-specific)
    if fred is not None:
        combined = combined.merge(fred[["year","month","tx_statewide_enrolled"]],
                                  on=["year","month"], how="left")

    # Add date column and lag timeliness by 2 months (redetermination failure -> closure lag)
    combined["date"] = pd.to_datetime(combined[["year","month"]].assign(day=1))
    combined = combined.sort_values(["region","date"]).reset_index(drop=True)

    # Month-over-month enrollment change within each region
    combined["enroll_mom_chg"] = combined.groupby("region")["eligible_individuals"].diff()
    combined["enroll_mom_pct"] = (
        combined["enroll_mom_chg"] /
        combined.groupby("region")["eligible_individuals"].shift(1) * 100
    ).round(2)

    combined.to_excel("snap_admin_burden_analysis.xlsx", index=False)
    print(f"Combined analysis: {len(combined)} rows -> snap_admin_burden_analysis.xlsx")

    # -- 5. Quick correlation summary ------------------------------------------
    print("\n-- Correlation: redetermination timeliness % vs enrollment MoM change --")
    corr_data = combined[["redet_pct","enroll_mom_pct"]].dropna()
    if len(corr_data) > 5:
        corr = corr_data.corr().iloc[0, 1]
        print(f"  Pearson r = {corr:.3f}  (n={len(corr_data)} region-months)")
        print("  Interpretation: positive r means lower timeliness -> larger enrollment drops")
    else:
        print("  Not enough overlapping data for correlation.")

    print("\n-- Redetermination timeliness by region (mean over full period) --")
    region_summary = (
        combined.groupby("region")[["app_pct","redet_pct","eligible_individuals"]]
        .agg(app_pct_mean=("app_pct","mean"),
             redet_pct_mean=("redet_pct","mean"),
             avg_enrolled=("eligible_individuals","mean"))
        .reset_index()
    )
    region_summary["app_pct_mean"]   = (region_summary["app_pct_mean"] * 100).round(1)
    region_summary["redet_pct_mean"] = (region_summary["redet_pct_mean"] * 100).round(1)
    region_summary["avg_enrolled"]   = region_summary["avg_enrolled"].round(0).astype(int)
    print(region_summary.to_string(index=False))


if __name__ == "__main__":
    main()
