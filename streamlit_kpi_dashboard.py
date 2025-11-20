import os
import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="SAS to ETL Migration Dashboard", layout="wide")

# -------------------------------------------------------------------
# Configuration: status → flag mapping
# -------------------------------------------------------------------
# We interpret *status codes* (PROD, ACC, QA, ETL, SPEC, PEND, CNN, ?, null)
# and map them into flag columns.
#
#   PROD = Implemented in production      → prod_done_cnt
#   ACC  = Acceptance is complete         → acc_done_cnt
#   QA   = QA Completed                   → qa_done_cnt
#   ETL  = Development is complete        → etl_done_cnt
#   SPEC = Spec is complete               → spec_done_cnt
#   PEND = Not Started                    → no flags
#   CNN  = Conversion Not Needed          → drop row
#   ? / null / empty                      → treat as PEND (no flags)
#
STATUS_FLAG_MAPPING = {
    "spec_done_cnt": ["spec"],
    "etl_done_cnt": ["etl"],
    "qa_done_cnt": ["qa"],
    "acc_done_cnt": ["acc"],
    "prod_done_cnt": ["prod"],
    # we keep *_in_pgrs_cnt columns for compatibility, but they
    # don't map to any code right now:
    "spec_in_pgrs_cnt": [],
    "etl_in_pgrs_cnt": [],
    "qa_in_pgrs_cnt": [],
}

RESERVED_CSV_PATH = "latest_etl.csv"


# -------------------------------------------------------------------
# Data loading: upload CSV + remember last file
# -------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_df(uploaded_file):
    """
    1. If user uploads a file this run, read it and also save it
       to RESERVED_CSV_PATH.
    2. If no file uploaded, but RESERVED_CSV_PATH exists, use that.
    3. Otherwise, return empty DataFrame.
    """
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        # persist last-uploaded file for future runs
        with open(RESERVED_CSV_PATH, "wb") as f:
            f.write(uploaded_file.getbuffer())
        return df

    # No new upload this run – try to use last saved CSV
    if os.path.exists(RESERVED_CSV_PATH):
        return pd.read_csv(RESERVED_CSV_PATH)

    # Nothing available yet
    return pd.DataFrame()


def normalize_and_derive_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Rename incoming CSV columns to internal names:
        PRCS AREA CODE  → project
        Dev Group Name  → dev_grp_name
        Status / Status Name → status_code (we prefer Status if present)
    - Drop CNN rows (Conversion Not Needed).
    - Convert ?, null, empty to PEND (Not Started).
    - Derive flag columns based on STATUS_FLAG_MAPPING.
    """
    df = df.copy()

    # Rename to friendlier internal names (only if present)
    rename_map = {}
    if "PRCS AREA CODE" in df.columns:
        rename_map["PRCS AREA CODE"] = "project"
    if "Dev Group Name" in df.columns:
        rename_map["Dev Group Name"] = "dev_grp_name"
    if "Status" in df.columns:
        rename_map["Status"] = "status_raw"
    if "Status Name" in df.columns:
        rename_map["Status Name"] = "status_name_raw"

    df = df.rename(columns=rename_map)

    # Derive a single status_code column in UPPERCASE
    if "status_raw" in df.columns:
        status_series = df["status_raw"]
    elif "status_name_raw" in df.columns:
        status_series = df["status_name_raw"]
    else:
        status_series = pd.Series("", index=df.index)

    df["status_code"] = status_series.astype(str).str.strip()

    # Normalize some empties
    df["status_code"] = df["status_code"].replace(
        {"nan": "", "NaN": "", "None": "", "none": ""}
    )

    # Drop Conversion Not Needed (CNN)
    mask_cnn = df["status_code"].str.upper() == "CNN"
    df = df[~mask_cnn].copy()

    # Treat ?, empty, null as PEND (Not Started)
    df["status_code"] = df["status_code"].where(
        ~df["status_code"].isin(["?", "", " "]),
        other="PEND",
    )

    # Now create a normalized lowercase version for easy matching
    df["status_code_norm"] = df["status_code"].str.lower().str.strip()

    # Initialize all flag columns to 0
    for flag_col in STATUS_FLAG_MAPPING.keys():
        df[flag_col] = 0

    # Set flags based on status code
    for flag_col, codes in STATUS_FLAG_MAPPING.items():
        if not codes:
            continue
        normalized_codes = [c.lower() for c in codes]
        df.loc[
            df["status_code_norm"].isin(normalized_codes),
            flag_col
        ] = 1

    return df


@st.cache_data(show_spinner=False)
def clean_and_filter(df, dev_grp_filter=None, project_filter=None):
    df = normalize_and_derive_flags(df)

    if dev_grp_filter:
        df = df[df["dev_grp_name"].isin(dev_grp_filter)]
    if project_filter:
        df = df[df["project"].isin(project_filter)]

    return df


@st.cache_data(show_spinner=False)
def build_summary(df: pd.DataFrame) -> pd.DataFrame:
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
              acc_done=("acc_done_cnt", "sum"),
              prod_done=("prod_done_cnt", "sum"),
          )
          .reset_index()
    )
    return summary


# -------------------------------------------------------------------
# Sidebar controls
# -------------------------------------------------------------------
st.sidebar.header("Filters")

uploaded = st.sidebar.file_uploader("Upload latest ETL CSV", type=["csv"])
df_raw = load_df(uploaded)

if df_raw.empty:
    st.warning("Upload an ETL CSV to get started. No previous file found.")
    st.stop()

# Normalize for filtering
df_raw = normalize_and_derive_flags(df_raw)

projects = sorted(df_raw["project"].dropna().unique().tolist()) if "project" in df_raw.columns else []
group_names = sorted(df_raw["dev_grp_name"].dropna().unique().tolist()) if "dev_grp_name" in df_raw.columns else []

proj_sel = st.sidebar.multiselect("Project", projects, default=projects)
dev_sel = st.sidebar.multiselect("Group Name", group_names, default=group_names)

df = clean_and_filter(df_raw, dev_grp_filter=dev_sel, project_filter=proj_sel)
summary = build_summary(df)

# -------------------------------------------------------------------
# Header
# -------------------------------------------------------------------
st.title("SAS to ETL Migration Dashboard")

# -------------------------------------------------------------------
# KPI Tiles (Global)
# -------------------------------------------------------------------
def kpi(label, value, help_text=None):
    st.metric(label, value, help=help_text)

total_records = len(df)
distinct_projects = df["project"].nunique() if "project" in df.columns else 0
distinct_dev_groups = df["dev_grp_name"].nunique() if "dev_grp_name" in df.columns else 0

flag_totals = {
    "Spec Done": int(df["spec_done_cnt"].sum()),
    "ETL Done": int(df["etl_done_cnt"].sum()),
    "QA Completed": int(df["qa_done_cnt"].sum()),
    "ACC Completed": int(df["acc_done_cnt"].sum()),
    "PROD Implemented": int(df["prod_done_cnt"].sum()),
}

overall_completed = flag_totals["QA Completed"] + flag_totals["ACC Completed"] + flag_totals["PROD Implemented"]

# First row of KPIs
cols = st.columns(6)
with cols[0]: kpi("Jobs for Conv.", f"{total_records:,}")
with cols[1]: kpi("Projects", f"{distinct_projects}")
with cols[2]: kpi("Dev Groups", f"{distinct_dev_groups}")
with cols[3]: kpi("Spec Done", f"{flag_totals['Spec Done']:,}")
with cols[4]: kpi("ETL Done", f"{flag_totals['ETL Done']:,}")
with cols[5]: kpi("QA Completed", f"{flag_totals['QA Completed']:,}")

# Second row of KPIs for ACC / PROD / Overall
cols2 = st.columns(3)
with cols2[0]: kpi("ACC Completed", f"{flag_totals['ACC Completed']:,}")
with cols2[1]: kpi("PROD Implemented", f"{flag_totals['PROD Implemented']:,}")
with cols2[2]: kpi("All Completed (QA+ACC+PROD)", f"{overall_completed:,}")

st.divider()

# -------------------------------------------------------------------
# Overlay Bar: PROD vs Total by Dev Group
# -------------------------------------------------------------------
st.subheader("PROD Implemented vs Total by Dev Group (Overlay)")

summary_by_group = (
    summary.groupby("dev_grp_name", as_index=False)
           .agg(
               total=("total", "sum"),
               prod_done=("prod_done", "sum"),
               qa_done=("qa_done", "sum"),
               acc_done=("acc_done", "sum"),
           )
)

if not summary_by_group.empty:
    # Light bar = total, overlay bar = PROD
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
            y=alt.Y("prod_done:Q", title="PROD Implemented"),
            color=alt.Color("dev_grp_name:N", title="Dev Group"),
            tooltip=[
                alt.Tooltip("dev_grp_name:N", title="Dev Group"),
                alt.Tooltip("prod_done:Q", title="PROD Implemented", format=",.0f"),
                alt.Tooltip("total:Q", title="Total", format=",.0f"),
            ],
        )
    )

    overlay_chart = alt.layer(base, overlay).properties(height=360)
    st.altair_chart(overlay_chart, use_container_width=True)
else:
    st.info("No data for overlay chart.")

# -------------------------------------------------------------------
# Per-Group Tiles
# -------------------------------------------------------------------
st.subheader("Progress by Dev Group")
card_cols = st.columns(4)

def render_card(container, row):
    with container:
        st.markdown(f"##### {row['dev_grp_name']}")
        st.caption(row['project'])
        col1, col2, col3 = st.columns(3)
        # First line of metrics
        col1.metric("Rows", int(row["total"]))
        col2.metric("Spec ✓", int(row["spec_done"]))
        col3.metric("ETL ✓", int(row["etl_done"]))
        # Second line: QA / ACC / PROD individually
        col1.metric("QA ✓", int(row["qa_done"]))
        col2.metric("ACC ✓", int(row["acc_done"]))
        col3.metric("PROD ✓", int(row["prod_done"]))

        # Completion %: let's use PROD as "fully done", or you can change to QA+ACC+PROD
        denom = row["total"] if row["total"] else 1
        pct = round(100 * row["prod_done"] / denom)
        st.caption("Percent complete (PROD / Total)")
        st.progress(pct / 100)

summary_sorted = summary.sort_values(by=["project", "dev_grp_name"]).reset_index(drop=True)

if summary_sorted.empty:
    st.info("No data after filters.")
else:
    for i, (_, r) in enumerate(summary_sorted.iterrows()):
        render_card(card_cols[i % 4], r)

st.divider()

# -------------------------------------------------------------------
# Rollup Table
# -------------------------------------------------------------------
st.subheader("ETLs By Project and Dev Group")
st.dataframe(summary, use_container_width=True)

# -------------------------------------------------------------------
# Counts by Metric (bar chart)
# -------------------------------------------------------------------
st.subheader("Counts by Metric")

long_for_charts = summary.melt(
    id_vars=["project", "dev_grp_name", "total"],
    value_vars=[
        "spec_done",
        "etl_done",
        "qa_done",
        "acc_done",
        "prod_done",
    ],
    var_name="metric",
    value_name="count",
)

if not long_for_charts.empty:
    chart = (
        alt.Chart(long_for_charts)
        .mark_bar()
        .encode(
            x=alt.X("metric:N", title="Metric"),
            y=alt.Y("count:Q", title="Count"),
            color="project:N",
            column=alt.Column(
                "dev_grp_name:N",
                title="Dev Group",
                header=alt.Header(labelOrient="bottom"),
            ),
            tooltip=[
                alt.Tooltip("project:N", title="Project"),
                alt.Tooltip("dev_grp_name:N", title="Dev Group"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("count:Q", title="Count", format=",.0f"),
            ],
        )
        .properties(height=200)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No chart data.")
