"""
SNAP Enrollment Dashboard — Methodist Healthcare Ministries Service Area
74-County Rio Texas Conference Region | Jan 2022 – Feb 2026
Program Officer Tool: Food Security Trends & Grant-Making Strategy
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import requests, json

st.set_page_config(
    page_title="MHM SNAP Enrollment Dashboard",
    page_icon="🍽️",
    layout="wide",
)

# ── Texas county FIPS codes for all 74 MHM counties ──────────────────────────
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

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load():
    df = pd.read_excel("snap_mhm_counties_2022_feb2026.xlsx")
    df["date"] = pd.to_datetime(df[["year","month"]].assign(day=1))
    df["date_label"] = df["date"].dt.strftime("%b %Y")
    df["age_children"] = df["age_under5"] + df["age_5_17"]
    df["age_senior"]   = df["age_60_64"]  + df["age_65plus"]
    return df

@st.cache_data
def load_timeliness():
    try:
        t = pd.read_excel("snap_timeliness_by_region.xlsx")
        t["date"] = pd.to_datetime(t[["year","month"]].assign(day=1))
        return t
    except Exception:
        return None

@st.cache_data
def load_geojson():
    url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    try:
        r = requests.get(url, timeout=15)
        return r.json()
    except Exception:
        return None

df               = load()
timeliness_raw   = load_timeliness()
counties_geojson = load_geojson()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    try:
        st.image("https://www.mhm.org/wp-content/uploads/MHM-logo-horizontal-RGB.png",
                 use_container_width=True)
    except Exception:
        st.markdown("**Methodist Healthcare Ministries**")
    st.markdown("---")
    st.markdown("**Methodist Healthcare Ministries**  \n74-County Service Area  \nRio Texas Conference Region")
    st.markdown("---")

    # ── Analysis window ───────────────────────────────────────────────────────
    st.markdown("### Analysis Window")
    available = sorted(df["date"].unique())
    date_labels = [pd.Timestamp(d).strftime("%b %Y") for d in available]
    date_map = {label: pd.Timestamp(d) for label, d in zip(date_labels, available)}

    default_start = "Jun 2025"
    default_end   = "Feb 2026"

    start_label = st.selectbox("Start month", date_labels,
                               index=date_labels.index(default_start) if default_start in date_labels else 0)
    end_label   = st.selectbox("End month",   date_labels,
                               index=date_labels.index(default_end) if default_end in date_labels else len(date_labels)-1)

    BASELINE    = date_map[start_label]
    LATEST      = date_map[end_label]
    BASELINE_LABEL = start_label
    LATEST_LABEL   = end_label

    if BASELINE >= LATEST:
        st.error("Start month must be before end month.")
        st.stop()

    st.markdown("---")
    st.markdown(f"**Full data range:** Jan 2022 – Feb 2026")
    st.markdown(f"**Source:** Texas HHS SNAP Statistics")
    st.markdown("---")
    st.markdown("### Navigation")
    section = st.radio("", [
        "Service Area Overview",
        "County Trends",
        "Program Officer Analysis",
        "Admin Burden Analysis",
    ])

# ── Recompute trend from selected window ──────────────────────────────────────
monthly = (
    df.groupby("date")[["cases","eligible_individuals","age_children","age_senior",
                         "age_under5","age_5_17","age_18_59","age_60_64","age_65plus",
                         "total_snap_payments"]]
    .sum().reset_index()
)

base_df   = df[df["date"] == BASELINE][["county","eligible_individuals","cases"]].set_index("county")
latest_df = df[df["date"] == LATEST][["county","eligible_individuals","cases"]].set_index("county")
trend = base_df.join(latest_df, lsuffix="_base", rsuffix="_latest").dropna()
trend["abs_change"]    = trend["eligible_individuals_latest"] - trend["eligible_individuals_base"]
trend["pct_change"]    = (trend["abs_change"] / trend["eligible_individuals_base"] * 100).round(1)
trend["latest_enroll"] = trend["eligible_individuals_latest"]
trend = trend.sort_values("pct_change", ascending=False).reset_index()
trend["fips"] = trend["county"].map(COUNTY_FIPS)

# KPIs
latest_row = monthly[monthly["date"] == LATEST]
base_row   = monthly[monthly["date"] == BASELINE]
total_latest = int(latest_row["eligible_individuals"].values[0])
total_base   = int(base_row["eligible_individuals"].values[0])
total_change = total_latest - total_base
total_pct    = round(total_change / total_base * 100, 1)
total_cases  = int(latest_row["cases"].values[0])
avg_payment  = df[df["date"] == LATEST]["avg_payment_per_case"].mean()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: SERVICE AREA OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
if section == "Service Area Overview":
    st.title("SNAP Enrollment — MHM Service Area")
    st.caption(f"74-county Rio Texas Conference region · Jan 2022 – {LATEST_LABEL}  |  "
               f"Analysis window: {BASELINE_LABEL} → {LATEST_LABEL}")

    # KPI row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Enrolled Individuals", f"{total_latest:,}",
                  delta=f"{total_change:+,} vs {BASELINE_LABEL}")
    with col2:
        st.metric(f"Change ({BASELINE_LABEL}→{LATEST_LABEL})", f"{total_pct:+.1f}%")
    with col3:
        st.metric("Households (Cases)", f"{total_cases:,}")
    with col4:
        st.metric("Avg Benefit / Household", f"${avg_payment:,.0f}/mo")

    st.markdown("---")

    # Main trend chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=monthly["date"], y=monthly["eligible_individuals"],
        mode="lines+markers", name="Enrolled Individuals",
        line=dict(color="#1a5c8a", width=2.5), marker=dict(size=5),
        hovertemplate="%{x|%b %Y}<br><b>%{y:,} individuals</b><extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=monthly["date"], y=monthly["cases"],
        mode="lines", name="Households (Cases)",
        line=dict(color="#e07b39", width=2, dash="dash"),
        hovertemplate="%{x|%b %Y}<br><b>%{y:,} households</b><extra></extra>",
    ))
    fig.add_vrect(
        x0="2023-02-01", x1="2023-04-01",
        fillcolor="#f5c6c6", opacity=0.4, line_width=0,
        annotation_text="Emergency\nAllotments End",
        annotation_position="top left", annotation_font_size=11,
    )
    fig.add_vrect(
        x0=BASELINE.isoformat(), x1=LATEST.isoformat(),
        fillcolor="#d4e9f7", opacity=0.25, line_width=0,
        annotation_text="Analysis Window",
        annotation_position="top right", annotation_font_size=11,
    )
    fig.update_layout(
        title="Monthly SNAP Enrollment — 74-County MHM Service Area",
        xaxis_title=None, yaxis_title="Enrolled",
        yaxis_tickformat=",",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        hovermode="x unified", height=420,
        margin=dict(l=60, r=30, t=60, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.info(
        "**Context:** The sharp decline in early 2023 reflects the end of COVID-era emergency SNAP allotments "
        "(Texas terminated February 2023). Caseloads stabilized through 2024. Enrollment increases within "
        "the current analysis window likely reflect genuine economic stress, not policy artifacts."
    )

    # Age stacked area
    st.markdown("#### Enrollment by Age Group Over Time")
    fig2 = go.Figure()
    for col, label, color in [("age_children","Children (<18)","#2ecc71"),
                               ("age_18_59","Working Age (18–59)","#3498db"),
                               ("age_senior","Seniors (60+)","#e67e22")]:
        fig2.add_trace(go.Scatter(
            x=monthly["date"], y=monthly[col],
            mode="lines", name=label, line=dict(color=color, width=2),
            stackgroup="one",
            hovertemplate=f"{label}<br>%{{y:,}}<extra></extra>",
        ))
    fig2.update_layout(
        height=300, hovermode="x unified",
        yaxis_tickformat=",", margin=dict(l=60, r=30, t=30, b=40),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── Choropleth map ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Enrollment Trend by County — MHM Service Area")
    st.caption(f"% change in enrolled individuals · {BASELINE_LABEL} → {LATEST_LABEL}")

    if counties_geojson:
        map_df = trend[trend["fips"].notna()].copy()

        # Build hover text
        map_df["hover"] = map_df.apply(
            lambda r: (f"<b>{r['county']} County</b><br>"
                       f"Change: {r['pct_change']:+.1f}%<br>"
                       f"{BASELINE_LABEL}: {r['eligible_individuals_base']:,.0f}<br>"
                       f"{LATEST_LABEL}: {r['eligible_individuals_latest']:,.0f}<br>"
                       f"Δ {r['abs_change']:+,.0f} individuals"), axis=1
        )

        abs_max = max(abs(map_df["pct_change"].min()), abs(map_df["pct_change"].max()))

        fig_map = px.choropleth(
            map_df,
            geojson=counties_geojson,
            locations="fips",
            color="pct_change",
            color_continuous_scale="RdYlGn",
            range_color=[-abs_max, abs_max],
            scope="usa",
            custom_data=["hover"],
        )
        fig_map.update_traces(
            hovertemplate="%{customdata[0]}<extra></extra>",
            marker_line_width=0.5,
            marker_line_color="white",
        )
        fig_map.update_geos(
            fitbounds="locations",
            visible=False,
            bgcolor="#0e1117",
        )
        fig_map.update_coloraxes(
            colorbar_title="% Change",
            colorbar_ticksuffix="%",
        )
        fig_map.update_layout(
            height=480,
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="#0e1117",
            geo_bgcolor="#0e1117",
        )
        st.plotly_chart(fig_map, use_container_width=True)
        st.caption("🔴 Red = enrollment falling — people losing access to food support  "
                   "· 🟢 Green = enrollment rising — more people gaining access to needed support")
    else:
        st.warning("Map unavailable — could not load county GeoJSON. Check internet connection.")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: COUNTY TRENDS
# ═══════════════════════════════════════════════════════════════════════════════
elif section == "County Trends":
    st.title("County-Level Enrollment Trends")
    st.caption(f"Change from {BASELINE_LABEL} → {LATEST_LABEL} | All 74 MHM counties  "
               f"· Adjust analysis window in the sidebar")

    col_up, col_dn = st.columns(2)

    top_rising  = trend.head(15)
    top_falling = trend.tail(15).sort_values("pct_change")

    with col_up:
        st.markdown("### Fastest Growing")
        st.caption("% change in enrolled individuals")
        fig_up = go.Figure(go.Bar(
            y=top_rising["county"][::-1], x=top_rising["pct_change"][::-1],
            orientation="h", marker_color="#e05c5c",
            text=top_rising["pct_change"][::-1].apply(lambda v: f"+{v:.1f}%"),
            textposition="outside",
            customdata=top_rising[["abs_change","latest_enroll"]][::-1].values,
            hovertemplate="<b>%{y}</b><br>%{x:+.1f}%<br>+%{customdata[0]:,} individuals<br>Current: %{customdata[1]:,}<extra></extra>",
        ))
        fig_up.update_layout(height=460, margin=dict(l=120, r=80, t=20, b=30),
                             xaxis_title="% Change", xaxis_ticksuffix="%")
        st.plotly_chart(fig_up, use_container_width=True)

    with col_dn:
        st.markdown("### Fastest Declining")
        st.caption("% change in enrolled individuals")
        fig_dn = go.Figure(go.Bar(
            y=top_falling["county"], x=top_falling["pct_change"],
            orientation="h", marker_color="#4a9e6b",
            text=top_falling["pct_change"].apply(lambda v: f"{v:.1f}%"),
            textposition="outside",
            customdata=top_falling[["abs_change","latest_enroll"]].values,
            hovertemplate="<b>%{y}</b><br>%{x:.1f}%<br>%{customdata[0]:,} individuals<br>Current: %{customdata[1]:,}<extra></extra>",
        ))
        fig_dn.update_layout(height=460, margin=dict(l=120, r=80, t=20, b=30),
                             xaxis_title="% Change", xaxis_ticksuffix="%")
        st.plotly_chart(fig_dn, use_container_width=True)

    st.markdown("---")
    st.markdown("#### All 74 Counties — Sortable Table")

    all_counties = sorted(trend["county"].tolist())
    selected = st.multiselect(
        "Filter to specific counties (leave blank to show all)",
        options=all_counties,
        placeholder="Type or select counties...",
    )

    table_data = trend.copy() if not selected else trend[trend["county"].isin(selected)].copy()

    display = table_data[["county","eligible_individuals_base","eligible_individuals_latest",
                           "abs_change","pct_change"]].copy()
    col_base  = f"Enrolled ({BASELINE_LABEL})"
    col_latest = f"Enrolled ({LATEST_LABEL})"
    display.columns = ["County", col_base, col_latest, "Absolute Change", "% Change"]

    total_row = pd.DataFrame([{
        "County":          "TOTAL (shown)",
        col_base:          display[col_base].sum(),
        col_latest:        display[col_latest].sum(),
        "Absolute Change": display["Absolute Change"].sum(),
        "% Change":        (display["Absolute Change"].sum() / display[col_base].sum() * 100)
                            if display[col_base].sum() else 0,
    }])
    display = pd.concat([total_row, display], ignore_index=True)

    display[col_base]          = display[col_base].apply(lambda v: f"{v:,.0f}")
    display[col_latest]        = display[col_latest].apply(lambda v: f"{v:,.0f}")
    display["Absolute Change"] = display["Absolute Change"].apply(lambda v: f"{v:+,.0f}")
    display["% Change"]        = display["% Change"].apply(lambda v: f"{v:+.1f}%")

    st.dataframe(display, use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: PROGRAM OFFICER ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
elif section == "Program Officer Analysis":
    st.title("Program Officer Analysis")
    st.caption(f"Food security trends · Analysis window: {BASELINE_LABEL} → {LATEST_LABEL}")

    latest = df[df["date"] == LATEST].copy()
    latest["child_share"]  = (latest["age_children"] / latest["eligible_individuals"] * 100).round(1)
    latest["senior_share"] = (latest["age_senior"]   / latest["eligible_individuals"] * 100).round(1)
    latest = latest.merge(trend[["county","pct_change","abs_change"]], on="county", how="left")

    def classify(row):
        growing  = row["pct_change"] > 2
        high_vol = row["eligible_individuals"] > latest["eligible_individuals"].median()
        if growing and high_vol:      return "🔴 High Need + Growing"
        elif growing:                 return "🟠 Emerging / Watch"
        elif high_vol:                return "🟡 High Volume / Stable"
        else:                         return "🟢 Lower Need / Improving"

    latest["priority_tier"] = latest.apply(classify, axis=1)

    tab1, tab2, tab3 = st.tabs(["Priority Tiers", "Child & Senior Focus", "Grant Strategy"])

    with tab1:
        st.markdown(f"#### County Priority Tiers — {LATEST_LABEL}")
        tier_order  = ["🔴 High Need + Growing","🟠 Emerging / Watch",
                       "🟡 High Volume / Stable","🟢 Lower Need / Improving"]
        for tier in tier_order:
            group = latest[latest["priority_tier"] == tier].sort_values("eligible_individuals", ascending=False)
            if group.empty: continue
            with st.expander(f"{tier} — {len(group)} counties · {int(group['eligible_individuals'].sum()):,} enrolled",
                             expanded=(tier == "🔴 High Need + Growing")):
                disp = group[["county","eligible_individuals","pct_change","child_share","senior_share"]].copy()
                disp.columns = ["County","Enrolled","9-Mo Change %","Children %","Seniors %"]
                disp["9-Mo Change %"] = disp["9-Mo Change %"].apply(lambda v: f"{v:+.1f}%")
                disp["Children %"]    = disp["Children %"].apply(lambda v: f"{v:.1f}%")
                disp["Seniors %"]     = disp["Seniors %"].apply(lambda v: f"{v:.1f}%")
                disp["Enrolled"]      = disp["Enrolled"].apply(lambda v: f"{v:,.0f}")
                st.dataframe(disp, use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("#### Where Are Children & Seniors Most Concentrated?")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Top 15 by Children Enrolled**")
            ct = latest.nlargest(15, "age_children")[["county","age_children","child_share","eligible_individuals"]].copy()
            ct.columns = ["County","Children","Children %","Total Enrolled"]
            ct["Children"]      = ct["Children"].apply(lambda v: f"{v:,.0f}")
            ct["Total Enrolled"] = ct["Total Enrolled"].apply(lambda v: f"{v:,.0f}")
            ct["Children %"]    = ct["Children %"].apply(lambda v: f"{v:.1f}%")
            st.dataframe(ct, use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**Top 15 by Seniors Enrolled**")
            st_ = latest.nlargest(15, "age_senior")[["county","age_senior","senior_share","eligible_individuals"]].copy()
            st_.columns = ["County","Seniors","Seniors %","Total Enrolled"]
            st_["Seniors"]       = st_["Seniors"].apply(lambda v: f"{v:,.0f}")
            st_["Total Enrolled"] = st_["Total Enrolled"].apply(lambda v: f"{v:,.0f}")
            st_["Seniors %"]     = st_["Seniors %"].apply(lambda v: f"{v:.1f}%")
            st.dataframe(st_, use_container_width=True, hide_index=True)

        st.markdown("#### Service Area Age Composition — Latest Month")
        age_vals   = [latest["age_under5"].sum(), latest["age_5_17"].sum(),
                      latest["age_18_59"].sum(), latest["age_60_64"].sum(), latest["age_65plus"].sum()]
        age_labels = ["Under 5","Ages 5–17","Ages 18–59","Ages 60–64","Ages 65+"]
        fig_pie = go.Figure(go.Pie(
            labels=age_labels, values=age_vals,
            marker_colors=["#e74c3c","#e67e22","#3498db","#9b59b6","#8e44ad"],
            hovertemplate="%{label}<br><b>%{value:,}</b> individuals (%{percent})<extra></extra>",
            textinfo="label+percent",
        ))
        fig_pie.update_layout(height=340, margin=dict(t=20, b=20))
        st.plotly_chart(fig_pie, use_container_width=True)

    with tab3:
        st.markdown("#### Grant-Making Strategy Framework")
        st.markdown(f"*Analysis window: {BASELINE_LABEL} → {LATEST_LABEL}*")
        st.markdown("---")

        rising_counties   = trend[trend["pct_change"] > 5]["county"].tolist()
        high_vol_counties = latest.nlargest(10, "eligible_individuals")["county"].tolist()

        st.markdown("### 🔴 Priority 1 — Food Bank Capacity Grants")
        st.markdown(
            f"High enrollment volume AND growing trend — highest aggregate food insecurity burden.\n\n"
            f"**Top counties:** {', '.join(high_vol_counties[:8])}\n\n"
            f"**Grant types:** Operating support, mobile distribution, cold storage, volunteer capacity."
        )
        st.markdown("### 🟠 Priority 2 — SNAP Outreach & Enrollment Assistance")
        st.markdown(
            f"Rapidly growing enrollment from a lower base — emerging hardship or successful outreach surfacing un-enrolled eligibles.\n\n"
            f"**Watch counties:** {', '.join(rising_counties[:10]) if rising_counties else 'None >5% — monitor monthly'}\n\n"
            f"**Grant types:** Navigator/outreach staffing, application assistance clinics, SNAP+WIC integration."
        )
        st.markdown("### 🟡 Priority 3 — Rural Food Access Gaps")
        st.markdown(
            "Small rural counties with high enrollment relative to population but limited food infrastructure.\n\n"
            "**Key counties:** McMullen, Kenedy, Irion, Sterling, Schleicher — tiny populations, high SNAP share, limited pantry reach.\n\n"
            "**Grant types:** Mobile pantry routes, logistics/fuel support, food co-location at rural health clinics and schools."
        )
        st.markdown("### 🟢 Priority 4 — Children & Family Food Security")
        children_focus = latest.nlargest(8, "age_children")["county"].tolist()
        st.markdown(
            f"Counties with highest absolute child enrollment — most likely to experience benefit gaps between SNAP cycles.\n\n"
            f"**Top counties:** {', '.join(children_focus)}\n\n"
            f"**Grant types:** Weekend/summer food, school pantry networks, after-school meals, baby food/formula."
        )
        st.markdown("---")
        st.info(
            "**Post-2023 baseline matters.** The March 2023 end of emergency allotments caused significant "
            "caseload disruption. Current enrollment growth from the post-2023 floor likely reflects real "
            "economic deterioration. Food banks should anticipate continued demand growth through 2026."
        )
        st.warning(
            "**SNAP enrollment ≠ SNAP eligibility.** A significant unenrolled-but-eligible population likely "
            "exists across the region — particularly in border counties (Hidalgo, Starr, Webb, Zapata, Maverick) "
            "among mixed-status households, elderly residents, and migrant agricultural workers. "
            "Outreach grants in these counties may deliver outsized impact."
        )

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: ADMIN BURDEN ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
if section == "Admin Burden Analysis":
    st.title("Administrative Burden Analysis")
    st.caption("Testing the hypothesis: Are enrollment declines driven by paperwork failures, not reduced need?")

    if timeliness_raw is None:
        st.error("Timeliness data not found. Run fetch_timeliness.py first.")
        st.stop()

    # ── Build monthly aggregates ───────────────────────────────────────────────
    # Enrollment: average across MHM regions
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
    REGION_LABELS = {
        "06": "Region 06 — Gulf Coast",
        "07": "Region 07 — Austin/Central",
        "08": "Region 08 — San Antonio/SW",
        "09": "Region 09 — West TX",
        "11": "Region 11 — RGV/Coastal Bend",
    }

    df2 = df.copy()
    df2["region"] = df2["county"].map(COUNTY_REGION)
    enroll_by_region = (
        df2.groupby(["date","region"])["eligible_individuals"].sum().reset_index()
    )
    # Merge timeliness
    tl = timeliness_raw.copy()
    tl["region"] = tl["region"].astype(str).str.zfill(2)
    combined = enroll_by_region.merge(
        tl[["date","region","app_pct","redet_pct","app_disposed","redet_disposed"]],
        on=["date","region"], how="left"
    )
    combined = combined.sort_values(["region","date"]).reset_index(drop=True)
    combined["enroll_mom"] = combined.groupby("region")["eligible_individuals"].transform(
        lambda x: x.pct_change() * 100
    )
    combined["app_pct_100"]   = combined["app_pct"]   * 100
    combined["redet_pct_100"] = combined["redet_pct"] * 100

    # MHM-wide monthly aggregate
    mhm_monthly = (
        combined.groupby("date")[["eligible_individuals","app_pct","redet_pct"]]
        .agg(eligible_individuals=("eligible_individuals","sum"),
             app_pct=("app_pct","mean"),
             redet_pct=("redet_pct","mean"))
        .reset_index()
    )
    mhm_monthly["enroll_mom"] = mhm_monthly["eligible_individuals"].pct_change() * 100
    mhm_monthly["app_pct_100"]   = mhm_monthly["app_pct"]   * 100
    mhm_monthly["redet_pct_100"] = mhm_monthly["redet_pct"] * 100

    # ── Narrative ─────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("## The Three-Act Story")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Crisis low — Redet timeliness", "54%", delta="Feb 2024 (vs 95% federal standard)", delta_color="inverse")
    with c2:
        st.metric("Enrollment lost (Jun 23–Jun 24)", "−7,630", delta="while timeliness was 54–69%", delta_color="inverse")
    with c3:
        st.metric("Recovery (Jul–Nov 2024)", "+19,700", delta="when timeliness rose to 74–79%")

    st.markdown("---")

    # ── Main dual-axis chart: timeliness vs enrollment ─────────────────────────
    st.markdown("#### SNAP Processing Timeliness vs. MHM Service Area Enrollment")
    st.caption("When the system fails to process redeterminations on time, enrollment bleeds. When it recovers, enrollment follows.")

    fig = go.Figure()

    # Enrollment (left axis)
    fig.add_trace(go.Scatter(
        x=mhm_monthly["date"], y=mhm_monthly["eligible_individuals"],
        name="Enrolled Individuals (left)",
        line=dict(color="#1a5c8a", width=2.5),
        mode="lines+markers", marker=dict(size=4),
        hovertemplate="%{x|%b %Y}<br><b>%{y:,.0f} enrolled</b><extra></extra>",
        yaxis="y1",
    ))

    # Redetermination timeliness (right axis)
    fig.add_trace(go.Scatter(
        x=mhm_monthly["date"], y=mhm_monthly["redet_pct_100"],
        name="Redetermination Timeliness % (right)",
        line=dict(color="#e07b39", width=2, dash="dot"),
        mode="lines", marker=dict(size=4),
        hovertemplate="%{x|%b %Y}<br><b>%{y:.1f}% redeterminations timely</b><extra></extra>",
        yaxis="y2",
    ))

    # Federal standard reference line
    fig.add_hline(y=95, line_dash="dash", line_color="#2ecc71", line_width=1.5,
                  annotation_text="95% federal standard", annotation_position="top right",
                  yref="y2")

    # Annotate key periods
    fig.add_vrect(x0="2023-01-15", x1="2023-04-01",
                  fillcolor="#f5c6c6", opacity=0.35, line_width=0,
                  annotation_text="Emergency\nAllotments End", annotation_font_size=10,
                  annotation_position="top left")
    fig.add_vrect(x0="2023-06-01", x1="2024-06-15",
                  fillcolor="#fde8d0", opacity=0.35, line_width=0,
                  annotation_text="Admin Crisis\n(54–69% timely)", annotation_font_size=10,
                  annotation_position="top left")
    fig.add_vrect(x0="2024-07-01", x1="2024-12-01",
                  fillcolor="#d5f5e3", opacity=0.35, line_width=0,
                  annotation_text="Recovery\n+19,700", annotation_font_size=10,
                  annotation_position="top right")
    fig.add_vrect(x0="2025-01-15", x1="2026-03-01",
                  fillcolor="#e8daef", opacity=0.35, line_width=0,
                  annotation_text="New decline\n(high timeliness)", annotation_font_size=10,
                  annotation_position="top right")

    fig.update_layout(
        height=480,
        hovermode="x unified",
        yaxis=dict(title="Enrolled Individuals", tickformat=",", side="left"),
        yaxis2=dict(title="Timeliness %", side="right", overlaying="y",
                    range=[0, 105], ticksuffix="%"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=60, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Three-act analysis boxes ───────────────────────────────────────────────
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.error("""
**Act 1 — Emergency allotment cliff (Feb–Mar 2023)**

Enrollment dropped −3.0% and −2.6% in two months. This is NOT administrative burden — it's a direct policy shock. Texas terminated COVID-era emergency SNAP allotments, which had roughly doubled benefit amounts. Some households chose not to re-enroll once benefits returned to pre-pandemic levels.

*Timeliness was 41–55% at this time, but the cliff is too sharp and too coincident to be processing failures.*
        """)

        st.warning("""
**Act 2 — The administrative grind (Jun 2023–Jun 2024)**

Enrollment bled −0.5% to −1.5% every month for 12 straight months while redetermination timeliness sat at 54–69%. That is 1 in 3 redeterminations not processed within the required 30-day window.

**Effect:** −7,630 individuals lost over 12 months. At the same time, HHSC's SNAP application backlog peaked at 90,000+ unprocessed applications (Every Texan data). USDA rejected Texas's corrective action plan three times.

*This is consistent with procedural churn: eligible people being dropped because paperwork wasn't processed.*
        """)

    with col2:
        st.success("""
**Act 3 — The smoking gun: Recovery (Jul–Nov 2024)**

When HHSC finally improved processing, redetermination timeliness rose from 60% → 79% over five months. Enrollment **recovered +19,700 individuals** (+9.8%) in that same window.

**This is the key test of the hypothesis.** If the 2023–2024 drops were real (people becoming ineligible), they wouldn't come back when the system improved. The rapid recovery strongly suggests a large share of the dropped cases were still eligible — they had been procedurally removed and returned once processing improved.

*Parallel finding: Georgetown CCF found 74% of Texas Medicaid procedural terminations in 2023–24 re-enrolled within 12 months — same HHSC system, same dynamic.*
        """)

        st.error("""
**Act 4 — A new problem (2025–present)**

Timeliness is now 87–94% — near the federal 95% standard. The administrative crisis is largely resolved. But enrollment is falling again: −0.3% to −2.8% per month through late 2025 into early 2026.

**This is a different force.** The timing (beginning Jan–Feb 2025, accelerating Nov–Dec 2025) is consistent with an immigration enforcement chilling effect. Mixed-status households — where U.S. citizen children are eligible but fear of contact with government agencies has grown — appear to be disenrolling or not re-enrolling.

*The 2025 signal is hardest to act on administratively and most urgent for community outreach.*
        """)

    # ── Region-level breakdown ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Timeliness by HHS Region — MHM Service Area")
    st.caption("The administrative crisis hit all regions but varied in severity. Region 11 (RGV/Coastal Bend) had the most to lose from chilling effects.")

    region_sel = st.selectbox("Select region", list(REGION_LABELS.values()),
                               index=2)  # default: Region 08
    region_key = [k for k, v in REGION_LABELS.items() if v == region_sel][0]
    region_int = int(region_key)

    sub = combined[combined["region"] == region_key].sort_values("date").copy()

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=sub["date"], y=sub["eligible_individuals"],
        name="Enrolled (left)",
        line=dict(color="#1a5c8a", width=2.5), mode="lines+markers", marker=dict(size=4),
        hovertemplate="%{x|%b %Y}<br><b>%{y:,.0f} enrolled</b><extra></extra>",
        yaxis="y1",
    ))
    fig2.add_trace(go.Scatter(
        x=sub["date"], y=sub["redet_pct_100"],
        name="Redet Timeliness % (right)",
        line=dict(color="#e07b39", width=2, dash="dot"), mode="lines",
        hovertemplate="%{x|%b %Y}<br><b>%{y:.1f}% timely</b><extra></extra>",
        yaxis="y2",
    ))
    fig2.add_trace(go.Scatter(
        x=sub["date"], y=sub["app_pct_100"],
        name="App Timeliness % (right)",
        line=dict(color="#9b59b6", width=1.5, dash="dot"), mode="lines",
        hovertemplate="%{x|%b %Y}<br><b>%{y:.1f}% app timely</b><extra></extra>",
        yaxis="y2",
    ))
    fig2.add_hline(y=95, line_dash="dash", line_color="#2ecc71", line_width=1,
                   annotation_text="95% standard", annotation_position="top right",
                   yref="y2")
    fig2.update_layout(
        height=380,
        hovermode="x unified",
        yaxis=dict(title="Enrolled Individuals", tickformat=","),
        yaxis2=dict(title="Timeliness %", side="right", overlaying="y",
                    range=[0, 105], ticksuffix="%"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=40),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── Timeliness summary table ───────────────────────────────────────────────
    st.markdown("#### Average Processing Timeliness by Region and Period")
    periods = {
        "Pre-crisis (2022)":      (combined["date"] < "2023-01-01"),
        "Allotment cliff (Q1 23)": ((combined["date"] >= "2023-01-01") & (combined["date"] < "2023-04-01")),
        "Admin crisis (2023–H1 24)": ((combined["date"] >= "2023-06-01") & (combined["date"] < "2024-07-01")),
        "Recovery (H2 2024)":     ((combined["date"] >= "2024-07-01") & (combined["date"] < "2025-01-01")),
        "2025 (new decline)":     (combined["date"] >= "2025-01-01"),
    }
    rows = []
    for period_label, mask in periods.items():
        sub_p = combined[mask].groupby("region")[["app_pct_100","redet_pct_100","eligible_individuals"]].mean()
        # MHM total
        sub_all = combined[mask][["app_pct_100","redet_pct_100","eligible_individuals"]].mean()
        for region_code in ["07","08","09","11"]:
            if region_code in sub_p.index:
                rows.append({
                    "Period": period_label,
                    "Region": REGION_LABELS[region_code].split(" — ")[1],
                    "App %": round(sub_p.loc[region_code, "app_pct_100"], 1),
                    "Redet %": round(sub_p.loc[region_code, "redet_pct_100"], 1),
                    "Avg Enrolled": int(sub_p.loc[region_code, "eligible_individuals"]),
                })

    tl_summary = pd.DataFrame(rows)
    st.dataframe(tl_summary, use_container_width=True, hide_index=True)

    st.info(
        "**Grant strategy implication:** The 2024 recovery proves that enrollment-assistance investment works. "
        "Funding SNAP navigators who help eligible households complete redetermination paperwork directly "
        "prevents the procedural churn documented here. Priority targets: Region 11 (RGV, largest enrolled "
        "population, highest chilling-effect risk) and Region 08 (San Antonio, largest absolute volume)."
    )
