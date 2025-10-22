
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

st.set_page_config(page_title="Project Rollup Dashboard", layout="wide")

# -------------------------
# Data loading
# -------------------------
@st.cache_data(show_spinner=False)
def load_df(uploaded_file):
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
    else:
        # Sample data so the app runs out of the box
        rng = np.random.default_rng(42)
        projects = ["Apollo", "Hermes", "Zephyr"]
        dev_groups = [
            (1, "Core ETL"),
            (2, "Data Quality"),
            (3, "Reporting"),
            (4, "Platform"),
        ]
        rows = []
        for p in projects:
            for grp_num, grp_name in dev_groups:
                for _ in range(rng.integers(25, 90)):
                    rows.append({
                        "project": p,
                        "dev_grp_num": grp_num,
                        "dev_grp_name": grp_name,
                        "spec_in_pgrs_cnt": rng.integers(0, 2),
                        "spec_done_cnt": rng.integers(0, 2),
                        "etl_in_pgrs_cnt": rng.integers(0, 2),
                        "etl_done_cnt": rng.integers(0, 2),
                        "qa_in_pgrs_cnt": rng.integers(0, 2),
                        "qa_done_cnt": rng.integers(0, 2),
                        "note": ""
                    })
        df = pd.DataFrame(rows)
    return df

@st.cache_data(show_spinner=False)
def clean_and_filter(df, dev_grp_filter=None, project_filter=None):
    df = df.copy()
    # drop noisy column if present


    flag_cols = [
        "spec_in_pgrs_cnt","spec_done_cnt",
        "etl_in_pgrs_cnt","etl_done_cnt",
        "qa_in_pgrs_cnt","qa_done_cnt"
    ]
    for c in flag_cols:
        if c in df.columns:
            df[c] = (
                pd.to_numeric(df[c], errors="coerce")
                  .fillna(0)
                  .clip(lower=0, upper=1)
                  .astype(int)
            )

    if dev_grp_filter:
        df = df[df["dev_grp_name"].isin(dev_grp_filter)]
    if project_filter:
        df = df[df["project"].isin(project_filter)]

    return df

@st.cache_data(show_spinner=False)
def build_summary(df):
    flag_cols = [
        "spec_in_pgrs_cnt","spec_done_cnt",
        "etl_in_pgrs_cnt","etl_done_cnt",
        "qa_in_pgrs_cnt","qa_done_cnt"
    ]
    summary = (
        df.groupby(["project", "dev_grp_name"], dropna=False)
          .agg(
              total=("dev_grp_name", "size"),
              spec_in_pgrs=("spec_in_pgrs_cnt", "sum"),
              spec_done=("spec_done_cnt", "sum"),
              etl_in_pgrs=("etl_in_pgrs_cnt", "sum"),
              etl_done=("etl_done_cnt", "sum"),
              qa_in_pgrs=("qa_in_pgrs_cnt", "sum"),
              qa_done=("qa_done_cnt", "sum"),
          )
          .reset_index()
    )
    #for col in ["spec_in_pgrs","spec_done","etl_in_pgrs","etl_done","qa_in_pgrs","qa_done"]:
    #    summary[f"{col}_pct"] = (summary[col] / summary["total"]).round(3)
    return summary

# -------------------------
# Sidebar controls
# -------------------------
st.sidebar.header("Filters")

#uploaded = st.sidebar.file_uploader("Upload CSV (optional)", type=["csv"])
#For the moment, we are loading the data via csv, but this will come directly from the DLH table.
#df_raw = load_df(uploaded)
df_raw = pd.read_csv('etl_data.csv')

#Take out "Conversion Not Needed" and null groups
df_raw = df_raw[(df_raw['dev_grp_num'] != 99) & (df_raw['dev_grp_num'].notnull())]

projects = sorted(df_raw["project"].dropna().unique().tolist()) if "project" in df_raw.columns else []
dev_nums = sorted(pd.Series(df_raw.get("dev_grp_num", pd.Series(dtype=float))).dropna().unique().tolist())
group_names = sorted(df_raw["dev_grp_name"].dropna().unique().tolist()) if "dev_grp_name" in df_raw.columns else []

proj_sel = st.sidebar.multiselect("Project", projects, default=projects)
dev_sel = st.sidebar.multiselect("Group Name", group_names, default=group_names)

df = clean_and_filter(df_raw, dev_grp_filter=dev_sel, project_filter=proj_sel)
summary = build_summary(df)

# -------------------------
# Header
# -------------------------
st.title("SAS to ETL Migration Dashboard")

# -------------------------
# KPI Tiles (Global)
# -------------------------
def kpi(label, value, help_text=None):
    st.metric(label, value, help=help_text)

total_records = len(df)
distinct_projects = df["project"].nunique() if "project" in df.columns else 0
distinct_dev_groups = df["dev_grp_name"].nunique() if "dev_grp_name" in df.columns else 0

flag_totals = {
    "Spec In Progress": int(df.get("spec_in_pgrs_cnt", pd.Series(dtype=int)).sum()),
    "Spec Done": int(df.get("spec_done_cnt", pd.Series(dtype=int)).sum()),
    "ETL In Progress": int(df.get("etl_in_pgrs_cnt", pd.Series(dtype=int)).sum()),
    "ETL Done": int(df.get("etl_done_cnt", pd.Series(dtype=int)).sum()),
    "QA In Progress": int(df.get("qa_in_pgrs_cnt", pd.Series(dtype=int)).sum()),
    "QA Done": int(df.get("qa_done_cnt", pd.Series(dtype=int)).sum()),
}

cols = st.columns(6)
with cols[0]: kpi("Jobs for Conv.", f"{total_records:,}")
with cols[1]: kpi("Projects", f"{distinct_projects}")
with cols[2]: kpi("Dev Groups", f"{distinct_dev_groups}")
with cols[3]: kpi("Spec Done", f"{flag_totals['Spec Done']:,}")
with cols[4]: kpi("ETL Done", f"{flag_totals['ETL Done']:,}")
with cols[5]: kpi("QA Done/Ready for UAT", f"{flag_totals['QA Done']:,}")

st.divider()

# -------------------------
# Overlay Bar: QA Done vs Total by Dev Group
# -------------------------
st.subheader("QA Done vs Total by Dev Group (Overlay)")

# Aggregate across projects so each dev group has one bar
summary_by_group = (
    summary.groupby("dev_grp_name", as_index=False)
           .agg(total=("total", "sum"),
                qa_done=("qa_done", "sum"))
)

if not summary_by_group.empty:
    base = (
        alt.Chart(summary_by_group)
        .mark_bar(size=40, opacity=0.25)
        .encode(
            x=alt.X("dev_grp_name:N", title="Dev Group", sort="-y"),
            y=alt.Y("total:Q", title="Total ETL Jobs"),
            color=alt.Color("dev_grp_name:N", legend=None),
            tooltip=[
                alt.Tooltip("dev_grp_name:N", title="Dev Group"),
                alt.Tooltip("total:Q", title="Total", format=",.0f"),
            ],
        )
    )

    overlay = (
        alt.Chart(summary_by_group)
        .mark_bar(size=26)
        .encode(
            x=alt.X("dev_grp_name:N", sort="-y"),
            y=alt.Y("qa_done:Q", title="QA Done"),
            color=alt.Color("dev_grp_name:N", title="Dev Group"),
            tooltip=[
                alt.Tooltip("dev_grp_name:N", title="Dev Group"),
                alt.Tooltip("qa_done:Q", title="QA Done", format=",.0f"),
                alt.Tooltip("total:Q", title="Total", format=",.0f"),
            ],
        )
    )

    overlay_chart = alt.layer(base, overlay).properties(height=360)
    st.altair_chart(overlay_chart, use_container_width=True)
else:
    st.info("No data for overlay chart.")

# -------------------------
# Per-Group Tiles
# -------------------------
st.subheader("Progress by Dev Group")
# Create one tile per (project, dev_grp_name)
card_cols = st.columns(4)

def render_card(container, row):
    with container:
        st.markdown(f"##### {row['dev_grp_name']}")
        st.caption(row['project'])
        col1, col2, col3 = st.columns(3)
        col1.metric("Rows", int(row['total']))
        col2.metric("Spec ✓", int(row['spec_done']))
        col3.metric("ETL ✓", int(row['etl_done']))
        col1.metric("QA ✓", int(row['qa_done']))
        col2.metric("Spec →", int(row['spec_in_pgrs']))
        col3.metric("ETL →", int(row['etl_in_pgrs']))
        st.caption("Percent complete (QA Done / Total)")
        pct = 0 if row['total'] == 0 else round(100 * row['qa_done'] / row['total'])
        st.progress(pct / 100)

# sort for stable layout
summary_sorted = summary.sort_values(by=["project", "dev_grp_name"]).reset_index(drop=True)

if summary_sorted.empty:
    st.info("No data after filters.")
else:
    for i, (_, r) in enumerate(summary_sorted.iterrows()):
        render_card(card_cols[i % 4], r)

st.divider()

# -------------------------
# Rollup Table
# -------------------------
st.subheader("ETLs By Project and Dev Group)")
st.dataframe(summary, use_container_width=True)

# -------------------------
# Optional chart
# -------------------------
st.subheader("Counts by Metric")
long_for_charts = summary.melt(
    id_vars=["project", "dev_grp_name", "total"],
    value_vars=["spec_in_pgrs","spec_done","etl_in_pgrs","etl_done","qa_in_pgrs","qa_done"],
    var_name="metric",
    value_name="count"
)
if not long_for_charts.empty:
    chart = (
        alt.Chart(long_for_charts)
        .mark_bar()
        .encode(
            x=alt.X("metric:N", title="Metric"),
            y=alt.Y("count:Q", title="Count"),
            color="project:N",
            column=alt.Column("dev_grp_name:N", title="Dev Group", header=alt.Header(labelOrient="bottom"))
        )
        .properties(height=200)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No chart data.")
