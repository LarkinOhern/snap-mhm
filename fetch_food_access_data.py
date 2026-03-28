"""
fetch_food_access_data.py
Downloads USDA FARA and County Health Rankings food insecurity data,
merges with the pantry gap analysis.
"""
import requests
import pandas as pd
from io import BytesIO

COUNTY_FIPS = {
    "Aransas":"48007","Atascosa":"48013","Bandera":"48019","Bastrop":"48021",
    "Bee":"48025","Bexar":"48029","Blanco":"48031","Brooks":"48047",
    "Burnet":"48053","Caldwell":"48055","Calhoun":"48057","Cameron":"48061",
    "Coke":"48081","Colorado":"48089","Comal":"48091","Concho":"48095",
    "Crockett":"48105","De Witt":"48123","Dimmit":"48127","Duval":"48131",
    "Edwards":"48137","Fayette":"48149","Frio":"48163","Gillespie":"48171",
    "Goliad":"48175","Gonzales":"48177","Guadalupe":"48187","Hays":"48209",
    "Hidalgo":"48215","Irion":"48235","Jackson":"48239","Jim Hogg":"48247",
    "Jim Wells":"48249","Karnes":"48255","Kendall":"48259","Kenedy":"48261",
    "Kerr":"48265","Kimble":"48267","Kinney":"48271","Kleberg":"48273",
    "La Salle":"48283","Lampasas":"48281","Lavaca":"48285","Live Oak":"48297",
    "Llano":"48299","Mason":"48319","Matagorda":"48321","Maverick":"48323",
    "McCulloch":"48307","McMullen":"48311","Medina":"48325","Menard":"48327",
    "Mills":"48333","Nueces":"48355","Reagan":"48383","Real":"48385",
    "Refugio":"48391","San Patricio":"48409","San Saba":"48411",
    "Schleicher":"48413","Starr":"48427","Sterling":"48431","Sutton":"48435",
    "Tom Green":"48451","Travis":"48453","Upton":"48461","Uvalde":"48463",
    "Val Verde":"48465","Victoria":"48469","Webb":"48479","Willacy":"48489",
    "Wilson":"48493","Zapata":"48505","Zavala":"48507",
}


def fetch_fara_texas():
    print("Fetching USDA Food Access Research Atlas...")
    url = "https://www.ers.usda.gov/media/5626/food-access-research-atlas-data-download-2019.xlsx"
    r = requests.get(url, timeout=180, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    fara = pd.read_excel(BytesIO(r.content), sheet_name="Food Access Research Atlas")
    print(f"  FARA: {len(fara)} census tracts")

    tx = fara[fara["State"] == "Texas"].copy()
    tx["county_fips"] = tx["CensusTract"].astype(str).str.zfill(11).str[:5]

    # Aggregate to county
    numeric_cols = ["Pop2010","PovertyRate","MedianFamilyIncome",
                    "LILATracts_1And10","lapop1_10","laseniors1"]
    avail = [c for c in numeric_cols if c in tx.columns]

    agg = {c: ("mean" if c in ["PovertyRate","MedianFamilyIncome"] else "sum")
           for c in avail}
    agg["County"] = "first"

    county = tx.groupby("county_fips").agg(agg).reset_index()
    county["county_fips"] = county["county_fips"].astype(str).str.zfill(5)
    county.rename(columns={"County": "county_name_fara"}, inplace=True)
    print(f"  Texas counties: {len(county)}")
    county.to_excel("usda_fara_texas_county.xlsx", index=False)
    print("  Saved -> usda_fara_texas_county.xlsx")
    return county


def fetch_chr_food_insecurity():
    print("Fetching County Health Rankings 2024...")
    url = ("https://www.countyhealthrankings.org/sites/default/files/"
           "media/document/analytic_data2024.csv")
    r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    df = pd.read_csv(BytesIO(r.content), low_memory=False)

    tx = df[df["State Abbreviation"] == "TX"].copy()
    tx["county_clean"] = tx["Name"].str.replace(" County", "", regex=False).str.strip()

    keep = ["county_clean", "5-digit FIPS Code",
            "Food Insecurity raw value",
            "Food Insecurity numerator",
            "Food Insecurity denominator",
            "Food Environment Index raw value"]
    keep = [c for c in keep if c in tx.columns]
    out = tx[keep].copy()
    out.columns = ["county_name", "county_fips",
                   "food_insecurity_rate",
                   "food_insecure_population",
                   "food_insecurity_denom",
                   "food_env_index"][:len(out.columns)]
    out["county_fips"] = out["county_fips"].astype(str).str.zfill(5)

    print(f"  CHR Texas counties: {len(out)}")
    out.to_excel("chr_food_insecurity_texas.xlsx", index=False)
    print("  Saved -> chr_food_insecurity_texas.xlsx")
    return out


def enrich_gap_analysis(fara_df, chr_df):
    print("Enriching gap analysis...")
    gap = pd.read_excel("food_access_county.xlsx")
    gap["county_fips"] = gap["county"].map(COUNTY_FIPS)

    # Merge CHR
    chr_merge = chr_df[["county_fips","food_insecurity_rate",
                         "food_insecure_population","food_env_index"]].copy()
    gap = gap.merge(chr_merge, on="county_fips", how="left")

    # Merge FARA
    fara_merge_cols = ["county_fips"]
    for c in ["PovertyRate","MedianFamilyIncome","LILATracts_1And10","lapop1_10"]:
        if c in fara_df.columns:
            fara_merge_cols.append(c)
    gap = gap.merge(fara_df[fara_merge_cols], on="county_fips", how="left")

    # Food insecurity %
    gap["food_insecurity_rate"] = pd.to_numeric(gap["food_insecurity_rate"], errors="coerce")
    gap["food_insecurity_pct"] = (gap["food_insecurity_rate"] * 100).round(1)

    # Enriched gap score
    gap["gap_score_enriched"] = (
        gap["no_pantry"].astype(int) * 4 +
        ((gap["total_sites"] <= 2) & (gap["eligible_individuals"].fillna(0) > 300)).astype(int) * 2 +
        gap["no_snap_site"].astype(int) * 2 +
        (gap["snap_enrolled_per_site"].fillna(9999) > 2000).astype(int) * 2 +
        ((gap["pct_sites_open_wknd"].fillna(0) < 15) & (gap["total_sites"] > 0)).astype(int) +
        (gap["food_insecurity_pct"].fillna(0) > 20).astype(int)
    )

    gap = gap.sort_values("gap_score_enriched", ascending=False).reset_index(drop=True)
    gap.to_excel("food_access_county.xlsx", index=False)

    print(f"Enriched gap table saved: {len(gap)} counties -> food_access_county.xlsx")
    print()
    print("=== TOP 20 GAP COUNTIES (enriched with food insecurity data) ===")
    cols = ["county","region_name","eligible_individuals","total_sites",
            "snap_enrolled_per_site","snap_sites","food_insecurity_pct",
            "no_pantry","gap_score_enriched"]
    print(gap[cols].head(20).to_string(index=False))

    print()
    print("=== REGION FOOD INSECURITY SUMMARY ===")
    reg = gap.groupby("region_name").agg(
        counties=("county","count"),
        sites=("total_sites","sum"),
        avg_food_insecurity=("food_insecurity_pct","mean"),
        enrolled=("eligible_individuals","sum"),
        no_snap_site=("no_snap_site","sum"),
        avg_gap=("gap_score_enriched","mean"),
    ).reset_index()
    reg["enrolled_per_site"] = (
        reg["enrolled"] / reg["sites"].replace(0, float("nan"))
    ).round(0).astype("Int64")
    reg["avg_food_insecurity"] = reg["avg_food_insecurity"].round(1)
    reg["avg_gap"] = reg["avg_gap"].round(1)
    print(reg.sort_values("avg_gap", ascending=False).to_string(index=False))

    return gap


def main():
    fara = fetch_fara_texas()
    chr_data = fetch_chr_food_insecurity()
    enrich_gap_analysis(fara, chr_data)
    print("\nDone.")


if __name__ == "__main__":
    main()
