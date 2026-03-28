"""
build_gap_analysis.py
Builds the final clean gap analysis from food_pantry_catalog_clean.xlsx + SNAP enrollment data.
"""
import pandas as pd
import numpy as np

df       = pd.read_excel("food_pantry_catalog_clean.xlsx")
snap_raw = pd.read_excel("snap_mhm_counties_2022_feb2026.xlsx")
snap_raw["date"] = pd.to_datetime(snap_raw[["year","month"]].assign(day=1))
snap_latest = (
    snap_raw[snap_raw["date"] == snap_raw["date"].max()]
    [["county","eligible_individuals","cases"]].copy()
)

MHM_COUNTIES = [
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
]

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
REGION_NAMES = {
    "06":"Gulf Coast","07":"Austin/Central",
    "08":"San Antonio/SW","09":"West TX","11":"RGV/Coastal",
}

# County-level pantry metrics
pantry_agg = df.groupby("primary_county").agg(
    total_sites         = ("place_id","count"),
    food_banks          = ("org_type", lambda x: (x=="Food Bank / Warehouse").sum()),
    faith_pantries      = ("org_type", lambda x: (x=="Faith-Based Pantry").sum()),
    community_orgs      = ("org_type", lambda x: (x=="Community Organization").sum()),
    mobile_sites        = ("org_type", lambda x: (x=="Mobile Distribution").sum()),
    large_nonprofits    = ("org_type", lambda x: (x=="Large Nonprofit").sum()),
    snap_sites          = ("snap_enrollment_likely","sum"),
    sites_open_weekends = ("open_weekends","sum"),
    sites_open_evenings = ("open_evenings","sum"),
    pct_have_phone      = ("phone",       lambda x: round(x.notna().mean()*100,1)),
    pct_have_website    = ("website",     lambda x: round(x.notna().mean()*100,1)),
    pct_have_hours      = ("hours_text",  lambda x: round(x.notna().mean()*100,1)),
    avg_days_open       = ("days_open_count","mean"),
).reset_index().rename(columns={"primary_county":"county"})

# Full 74-county gap table
gap = pd.DataFrame({"county": MHM_COUNTIES})
gap = gap.merge(snap_latest, on="county", how="left")
gap = gap.merge(pantry_agg, on="county", how="left")
gap["hhs_region"]  = gap["county"].map(COUNTY_REGION)
gap["region_name"] = gap["hhs_region"].map(REGION_NAMES)

num_cols = ["total_sites","food_banks","faith_pantries","community_orgs",
            "mobile_sites","large_nonprofits","snap_sites",
            "sites_open_weekends","sites_open_evenings"]
for c in num_cols:
    gap[c] = gap[c].fillna(0).astype(int)

gap["snap_enrolled_per_site"] = (
    gap["eligible_individuals"] / gap["total_sites"].replace(0, np.nan)
).round(0)
gap["pct_sites_open_wknd"] = (
    gap["sites_open_weekends"] / gap["total_sites"].replace(0, np.nan) * 100
).round(1)

# Gap score (higher = more underserved)
gap["no_pantry"]    = gap["total_sites"] == 0
gap["no_snap_site"] = gap["snap_sites"]  == 0
gap["gap_score"] = (
    gap["no_pantry"].astype(int) * 4 +
    ((gap["total_sites"] <= 2) & (gap["eligible_individuals"].fillna(0) > 300)).astype(int) * 2 +
    gap["no_snap_site"].astype(int) * 2 +
    (gap["snap_enrolled_per_site"].fillna(9999) > 2000).astype(int) * 2 +
    ((gap["pct_sites_open_wknd"].fillna(0) < 15) & (gap["total_sites"] > 0)).astype(int)
)
gap = gap.sort_values("gap_score", ascending=False).reset_index(drop=True)
gap.to_excel("food_access_county.xlsx", index=False)

# ── Print results ─────────────────────────────────────────────────────────────
print("=== TOP 20 PRIORITY GAP COUNTIES ===")
cols = ["county","region_name","eligible_individuals","total_sites",
        "snap_enrolled_per_site","snap_sites","no_pantry","no_snap_site",
        "pct_sites_open_wknd","gap_score"]
print(gap[cols].head(20).to_string(index=False))

print()
print("=== REGION SUMMARY ===")
reg = gap.groupby("region_name").agg(
    counties      = ("county","count"),
    sites         = ("total_sites","sum"),
    snap_enrolled = ("eligible_individuals","sum"),
    snap_sites    = ("snap_sites","sum"),
    no_pantry     = ("no_pantry","sum"),
    no_snap_site  = ("no_snap_site","sum"),
    avg_gap_score = ("gap_score","mean"),
).reset_index()
reg["enrolled_per_site"] = (
    reg["snap_enrolled"] / reg["sites"].replace(0, np.nan)
).round(0).astype("Int64")
print(reg.sort_values("avg_gap_score", ascending=False).to_string(index=False))

print()
print("=== KEY STATS ===")
print(f"Total clean sites:           {len(df)}")
print(f"Counties with 0 sites:       {(gap['total_sites']==0).sum()}")
print(f"Counties with 1-3 sites:     {((gap['total_sites']>=1)&(gap['total_sites']<=3)).sum()}")
print(f"Counties with no SNAP site:  {gap['no_snap_site'].sum()}")
print(f"Sites open weekends:         {int(df['open_weekends'].sum())} ({df['open_weekends'].mean()*100:.0f}%)")
print(f"Sites open evenings:         {int(df['open_evenings'].sum())} ({df['open_evenings'].mean()*100:.0f}%)")
print(f"Sites with SNAP assist flag: {int(df['snap_enrollment_likely'].sum())}")
print()
print("Org type breakdown:")
print(df["org_type"].value_counts().to_string())
print()
print("Saved -> food_access_county.xlsx")
