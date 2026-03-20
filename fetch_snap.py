"""
fetch_snap.py
Downloads monthly SNAP county-level XLS files from Texas HHS (Jan 2022 - Aug 2025),
filters to the 74 MHM/Rio Texas Conference counties, and outputs a long-format Excel file.
"""

import requests
import pandas as pd
from io import BytesIO

# ── 74 MHM / Rio Texas Conference counties ──────────────────────────────────
MHM_COUNTIES = {
    "Aransas", "Atascosa", "Bandera", "Bastrop", "Bee", "Bexar", "Blanco",
    "Brooks", "Burnet", "Caldwell", "Calhoun", "Cameron", "Comal", "Concho",
    "Colorado", "Coke", "Crockett", "De Witt", "Dimmit", "Duval", "Edwards",
    "Fayette", "Frio", "Gillespie", "Goliad", "Gonzales", "Guadalupe", "Hays",
    "Hidalgo", "Irion", "Jackson", "Jim Hogg", "Jim Wells", "Karnes", "Kendall",
    "Kenedy", "Kerr", "Kimble", "Kinney", "Kleberg", "La Salle", "Lampasas",
    "Lavaca", "Live Oak", "Llano", "Matagorda", "Maverick", "Mason", "McCulloch",
    "McMullen", "Medina", "Menard", "Mills", "Nueces", "Reagan", "Real",
    "Refugio", "San Patricio", "San Saba", "Schleicher", "Starr", "Sterling",
    "Sutton", "Tom Green", "Travis", "Upton", "Uvalde", "Val Verde", "Victoria",
    "Webb", "Willacy", "Wilson", "Zapata", "Zavala",
}

BASE = "https://www.hhs.texas.gov"

# Exact URLs scraped from the HHS page — naming is highly inconsistent
FILE_URLS = [
    # 2022
    (2022,  1, "/sites/default/files/documents/snap-case-eligible-county-jan-2022.xls"),
    (2022,  2, "/sites/default/files/documents/snap-case-eligible-county-feb-2022.xls"),
    (2022,  3, "/sites/default/files/documents/snap-case-eligible-county-march-2022.xls"),
    (2022,  4, "/sites/default/files/documents/snap-case-eligible-county-april-2022.xls"),
    (2022,  5, "/sites/default/files/documents/snap-case-eligible-county-may-2022_0.xls"),
    (2022,  6, "/sites/default/files/documents/snap-case-eligible-county-june-2022_1.xls"),
    (2022,  7, "/sites/default/files/documents/snap-case-eligible-county-july-2022.xls"),
    (2022,  8, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-aug-2022.xls"),
    (2022,  9, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-sept-2022.xls"),
    (2022, 10, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-oct-2022.xls"),
    (2022, 11, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-nov-2022.xls"),
    (2022, 12, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-dec-2022.xls"),
    # 2023
    (2023,  1, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-jan-2023.xls"),
    (2023,  2, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-feb-2023.xls"),
    (2023,  3, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-march-2023.xls"),
    (2023,  4, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-april-2023.xls"),
    (2023,  5, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-may-2023.xls"),
    (2023,  6, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-june-2023.xls"),
    (2023,  7, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-july-2023.xls"),
    (2023,  8, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-aug-2023.xls"),
    (2023,  9, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-sept-2023.xls"),
    (2023, 10, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-oct-2023.xls"),
    (2023, 11, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-nov-2023.xls"),
    (2023, 12, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-dec-2023.xls"),
    # 2024 — Jan/Feb have "eligable" typo in filename
    (2024,  1, "/sites/default/files/documents/snap-cases-eligable-ind-county-jan-2024.xls"),
    (2024,  2, "/sites/default/files/documents/snap-cases-eligable-ind-by-county-feb-2024.xls"),
    (2024,  3, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-march-2024.xls"),
    (2024,  4, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-april-2024.xls"),
    (2024,  5, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-may-2024.xls"),
    (2024,  6, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-june-2024.xls"),
    (2024,  7, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-july-2024.xls"),
    (2024,  8, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-aug-2024.xls"),
    (2024,  9, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-sept-2024.xls"),
    (2024, 10, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-oct-2024.xls"),
    (2024, 11, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-nov-2024.xls"),
    (2024, 12, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-dec-2024.xls"),
    # 2025 — June has %3D encoding; July is .xlsx
    (2025,  1, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-jan-2025.xls"),
    (2025,  2, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-feb-2025.xls"),
    (2025,  3, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-march-2025.xls"),
    (2025,  4, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-april-2025.xls"),
    (2025,  5, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-may-2025.xls"),
    (2025,  6, "/sites/default/files/documents/snap-cases-eligible-ind-by%3Dcounty-june-2025.xls"),
    (2025,  7, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-july-2025.xlsx"),
    (2025,  8, "/sites/default/files/documents/snap-cases-eligible-ind-by-county-aug-2025.xls"),
]

OUTPUT_COLS = [
    "county", "year", "month",
    "cases", "eligible_individuals",
    "age_under5", "age_5_17", "age_18_59", "age_60_64", "age_65plus",
    "total_snap_payments", "avg_payment_per_case",
]

MONTH_NAMES = {
    1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
    7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"
}


def parse_file(content: bytes, year: int, month: int) -> pd.DataFrame | None:
    # Header is row 1 for most files; 2025 files added a blank row making it row 2
    # Try both and use whichever finds a county column
    df = None
    for header_row in (1, 2, 0):
        try:
            candidate = pd.read_excel(BytesIO(content), header=header_row)
            candidate.columns = [str(c).strip() for c in candidate.columns]
            if any("county" in c.lower() for c in candidate.columns):
                df = candidate
                break
        except Exception:
            continue
    if df is None:
        print(f"  [ERROR] Could not parse Excel for {year}-{month:02d}")
        return None

    df.columns = [str(c).strip() for c in df.columns]

    # Find county column
    county_col = next((c for c in df.columns if "county" in c.lower()), None)
    if county_col is None:
        print(f"  [WARN] No county column in {year}-{month:02d}")
        return None

    df[county_col] = df[county_col].astype(str).str.strip()

    # Case-insensitive match: build lookup from normalized name -> canonical name
    # Normalize = lowercase + remove spaces (handles "DeWitt" vs "De Witt", "Mc" capitalization)
    county_lookup = {"".join(c.lower().split()): c for c in MHM_COUNTIES}
    df["_county_norm"] = df[county_col].apply(lambda x: "".join(x.lower().split()))
    df["_canonical"] = df["_county_norm"].map(county_lookup)

    df = df[df["_canonical"].notna()].copy().reset_index(drop=True)
    df[county_col] = df["_canonical"]
    if df.empty:
        print(f"  [WARN] No MHM counties matched in {year}-{month:02d}")
        return None

    # Get numeric columns in order
    numeric_cols = [c for c in df.columns if c != county_col and
                    pd.to_numeric(df[c], errors="coerce").notna().sum() > 5]

    out = pd.DataFrame()
    out["county"] = df[county_col].values
    out["year"]   = year
    out["month"]  = month

    col_map = [
        "cases", "eligible_individuals", "age_under5", "age_5_17",
        "age_18_59", "age_60_64", "age_65plus",
        "total_snap_payments", "avg_payment_per_case",
    ]
    for i, name in enumerate(col_map):
        out[name] = pd.to_numeric(df[numeric_cols[i]], errors="coerce") if i < len(numeric_cols) else None

    return out[OUTPUT_COLS]


def main():
    all_frames = []

    for year, month, path in FILE_URLS:
        label = f"{MONTH_NAMES[month]} {year}"
        url = BASE + path
        print(f"Fetching {label}...", end=" ")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"FAILED ({e})")
            continue

        frame = parse_file(r.content, year, month)
        if frame is not None:
            all_frames.append(frame)
            print(f"OK ({len(frame)} counties)")
        else:
            print("SKIPPED")

    if not all_frames:
        print("No data collected.")
        return

    result = pd.concat(all_frames, ignore_index=True)
    result = result.sort_values(["county", "year", "month"]).reset_index(drop=True)

    out_path = "snap_mhm_counties_2022_aug2025.xlsx"
    result.to_excel(out_path, index=False)
    print(f"\nDone. {len(result):,} rows -> {out_path}")
    print(f"Counties: {result['county'].nunique()} | Months: {result[['year','month']].drop_duplicates().shape[0]}")


if __name__ == "__main__":
    main()
