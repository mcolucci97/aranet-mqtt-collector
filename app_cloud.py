#!/usr/bin/env python3
"""
Streamlit dashboard for Aranet data stored in Supabase.

Compatible with Python 3.12.
Designed to:
- avoid Streamlit Arrow / LargeUtf8 table issues
- plot the real measurement value (value_num)
- keep CSV export correct
- show institutional CEA / RadonNET branding
"""

import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from supabase import create_client


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="CEA/LNHB RadonNET Environmental Monitoring Testbed",
    page_icon="🧪",
    layout="wide",
)


# ============================================================
# CUSTOM STYLE
# ============================================================
st.markdown(
    """
    <style>
        .main-header {
            padding: 0.25rem 0 1rem 0;
            border-bottom: 1px solid rgba(120,120,120,0.25);
            margin-bottom: 1rem;
        }

        .main-title {
            font-size: 2rem;
            font-weight: 700;
            line-height: 1.2;
            margin-bottom: 0.25rem;
        }

        .main-subtitle {
            font-size: 1.02rem;
            color: #555;
            margin-bottom: 0.25rem;
        }

        .main-description {
            font-size: 0.97rem;
            color: #666;
            line-height: 1.5;
            margin-top: 0.4rem;
        }

        .section-note {
            background: rgba(240, 242, 246, 0.7);
            border: 1px solid rgba(120,120,120,0.18);
            border-radius: 10px;
            padding: 0.9rem 1rem;
            margin-bottom: 1rem;
        }

        .section-note strong {
            font-weight: 700;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.92rem;
        }

        th, td {
            padding: 0.35rem 0.5rem;
            border: 1px solid #ddd;
            text-align: left;
            white-space: nowrap;
        }

        th {
            background-color: #f5f5f5;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# HEADER WITH OPTIONAL LOGOS
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
CEA_LOGO = BASE_DIR / "cea_logo.png"
RADONNET_LOGO = BASE_DIR / "radonnet_logo.png"


header_logo_col, header_text_col = st.columns([1.6, 5.4])

with header_logo_col:

    logo_cols = st.columns(2)

    with logo_cols[0]:
        if CEA_LOGO.exists():
            st.image(str(CEA_LOGO), width=110)

    with logo_cols[1]:
        if RADONNET_LOGO.exists():
            st.image(str(RADONNET_LOGO), width=200)

with header_text_col:
    st.markdown(
        """
        <div class="main-header">
            <div class="main-title">CEA/LNHB RadonNET Environmental Monitoring Testbed</div>
            <div class="main-subtitle">
                Real-time and historical monitoring of indoor environmental parameters in the CEA/LNHB building
            </div>
            <div class="main-description">
                This dashboard supports a testbed for a distributed network of environmental monitoring instruments
                deployed in the <strong>CEA/LNHB</strong> building to track quantities relevant to indoor air quality
                and controlled laboratory environments, including <strong>radon</strong>, <strong>particulate matter
                (PM)</strong>, <strong>temperature</strong>, <strong>humidity</strong>, <strong>pressure</strong>,
                battery status, and communication indicators.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown(
    """
    <div class="section-note">
        <strong>Testbed scope.</strong> This platform is intended to evaluate and visualize the behavior of a sensor
        network operating in the CEA/LNHB building, with particular interest in environmental parameters relevant to
        radon monitoring, aerosol-related measurements, and indoor ambient characterization.
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# SECRETS
# ============================================================
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception as exc:
    st.error(f"Missing Streamlit secrets: {exc}")
    st.stop()


# ============================================================
# UNITS
# ============================================================
VARIABLE_UNITS = {
    "radon": "Bq/m³",
    "temperature": "°C",
    "humidity": "%",
    "atmosphericpressure": "hPa",
    "battery": "V",
    "rssi": "dBm",
    "pm1": "kg/m³",
    "pm2_5": "kg/m³",
    "pm10": "kg/m³",
}


# ============================================================
# SUPABASE
# ============================================================
@st.cache_resource
def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# HELPERS
# ============================================================
def get_unit(variable):
    return VARIABLE_UNITS.get(str(variable), "")


def with_unit(label, variable):
    unit = get_unit(variable)
    return f"{label} [{unit}]" if unit else label


def format_value(value, decimals=2):
    if pd.isna(value):
        return "NA"

    try:
        value = float(value)
    except Exception:
        return str(value)

    if not math.isfinite(value):
        return str(value)

    abs_val = abs(value)

    if abs_val == 0:
        return "0"

    if abs_val < 1e-3 or abs_val >= 1e4:
        return f"{value:.3e}"

    return f"{value:.{decimals}f}"


def format_value_with_unit(value, variable, decimals=2):
    base = format_value(value, decimals=decimals)
    unit = get_unit(variable)
    return f"{base} {unit}" if unit else base


def choose_tick_format(series):
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None

    numeric = numeric[numeric.map(math.isfinite)]
    if numeric.empty:
        return None

    max_abs = numeric.abs().max()
    if pd.isna(max_abs):
        return None

    if max_abs < 1e-3 or max_abs >= 1e4:
        return ".2e"

    return None


def choose_hover_format(series):
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return ".2f"

    numeric = numeric[numeric.map(math.isfinite)]
    if numeric.empty:
        return ".2f"

    max_abs = numeric.abs().max()
    if pd.isna(max_abs):
        return ".2f"

    if max_abs < 1e-3 or max_abs >= 1e4:
        return ".4e"

    return ".2f"


def safe_sensor_name(row):
    sensor_name = row.get("sensor_name")
    sensor_id = row.get("sensor_id")
    sensor_ref = row.get("sensor_ref")

    if pd.notna(sensor_name) and str(sensor_name).strip():
        return str(sensor_name)
    if pd.notna(sensor_id) and str(sensor_id).strip():
        return str(sensor_id)
    return str(sensor_ref)


def sensor_option_label(row):
    sensor_name = row.get("sensor_name")
    sensor_id = row.get("sensor_id")
    sensor_ref = row.get("sensor_ref")

    name = (
        str(sensor_name).strip()
        if pd.notna(sensor_name) and str(sensor_name).strip()
        else "Unknown sensor"
    )
    sid = (
        str(sensor_id).strip()
        if pd.notna(sensor_id) and str(sensor_id).strip()
        else "no-id"
    )

    return f"{name} ({sid})", str(sensor_ref)


def fetch_all(query_builder, page_size=1000):
    rows = []
    start = 0

    while True:
        end = start + page_size - 1
        response = query_builder.range(start, end).execute()
        batch = response.data or []

        if not batch:
            break

        rows.extend(batch)

        if len(batch) < page_size:
            break

        start += page_size

    return rows


# ============================================================
# SAFE TABLE RENDERING
# ============================================================
def dataframe_for_html(df):
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy().reset_index(drop=True)

    for col in out.columns:
        series = out[col]

        if pd.api.types.is_datetime64_any_dtype(series):
            try:
                out[col] = series.dt.strftime("%Y-%m-%d %H:%M:%S UTC").fillna("")
            except Exception:
                out[col] = series.astype(str).fillna("")
        elif pd.api.types.is_float_dtype(series):
            out[col] = series.map(lambda x: "" if pd.isna(x) else format_value(x))
        elif pd.api.types.is_integer_dtype(series):
            out[col] = series.map(lambda x: "" if pd.isna(x) else str(x))
        else:
            out[col] = series.map(lambda x: "" if pd.isna(x) else str(x))

    out.columns = [str(c) for c in out.columns]
    return out


def safe_table(df, height=None):
    if df is None or df.empty:
        st.info("No rows to display.")
        return

    show_df = dataframe_for_html(df)
    html = show_df.to_html(index=False, escape=True)

    if height is None:
        wrapper_style = (
            "overflow-x:auto; border:1px solid #ddd; "
            "border-radius:8px; padding:4px;"
        )
    else:
        wrapper_style = (
            f"max-height:{height}px; overflow-y:auto; overflow-x:auto; "
            f"border:1px solid #ddd; border-radius:8px; padding:4px;"
        )

    st.markdown(
        f"""
        <div style="{wrapper_style}">
            {html}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# DATA LOADING
# ============================================================
@st.cache_data(ttl=60)
def load_bases():
    sb = get_supabase()

    rows = fetch_all(
        sb.table("bases")
        .select("base_id, base_name, updated_at")
        .order("base_id")
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["base_id", "base_name", "updated_at"])

    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)

    return df


@st.cache_data(ttl=60)
def load_sensors():
    sb = get_supabase()

    rows = fetch_all(
        sb.table("sensors")
        .select("sensor_ref, base_id, sensor_id, sensor_name, product_number, updated_at")
        .order("base_id")
        .order("sensor_id")
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "sensor_ref",
                "base_id",
                "sensor_id",
                "sensor_name",
                "product_number",
                "updated_at",
                "base_name",
            ]
        )

    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)

    bases_df = load_bases()
    if not bases_df.empty and "base_id" in df.columns:
        df = df.merge(
            bases_df[["base_id", "base_name"]],
            on="base_id",
            how="left",
        )

    return df


@st.cache_data(ttl=60)
def load_variables(sensor_refs):
    sb = get_supabase()

    if not sensor_refs:
        return pd.DataFrame(columns=["variable", "n"])

    rows = fetch_all(
        sb.table("measurements")
        .select("sensor_ref, variable")
        .in_("sensor_ref", sensor_refs)
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["variable", "n"])

    out = (
        df.groupby("variable", dropna=True)
        .size()
        .reset_index(name="n")
        .sort_values("variable")
        .reset_index(drop=True)
    )

    return out


@st.cache_data(ttl=60)
def load_multi_timeseries(sensor_refs, variables, start_utc):
    sb = get_supabase()

    if not sensor_refs or not variables:
        return pd.DataFrame()

    rows = fetch_all(
        sb.table("measurements")
        .select(
            "id, payload_time_utc, inserted_at, received_at_utc, payload_time_unix, "
            "base_id, sensor_id, sensor_ref, variable, value_num, value_text, unit"
        )
        .gte("payload_time_utc", start_utc)
        .in_("sensor_ref", sensor_refs)
        .in_("variable", variables)
        .order("payload_time_utc")
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    for col in ["payload_time_utc", "inserted_at", "received_at_utc"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    if "payload_time_unix" in df.columns:
        df["payload_time_unix"] = pd.to_numeric(df["payload_time_unix"], errors="coerce")

    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce")

    if "value_num" in df.columns:
        df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")

    if "value_text" in df.columns:
        df["value_text"] = df["value_text"].map(lambda x: None if pd.isna(x) else str(x))

    if "variable" in df.columns:
        df["variable"] = df["variable"].map(lambda x: None if pd.isna(x) else str(x))

    if "sensor_ref" in df.columns:
        df["sensor_ref"] = df["sensor_ref"].map(lambda x: None if pd.isna(x) else str(x))

    df = df.dropna(subset=["payload_time_utc", "value_num", "variable", "sensor_ref"]).copy()
    df = df[df["value_num"].map(lambda x: pd.notna(x) and math.isfinite(float(x)))].copy()

    sensors_df = load_sensors()
    if not sensors_df.empty:
        meta_cols = [
            c for c in [
                "sensor_ref",
                "base_id",
                "base_name",
                "sensor_id",
                "sensor_name",
                "product_number",
            ]
            if c in sensors_df.columns
        ]

        sensors_meta = sensors_df[meta_cols].drop_duplicates(subset=["sensor_ref"])

        df = df.merge(
            sensors_meta,
            on="sensor_ref",
            how="left",
            suffixes=("", "_meta"),
        )

        for col in ["base_id", "base_name", "sensor_id", "sensor_name", "product_number"]:
            meta_col = f"{col}_meta"
            if meta_col in df.columns:
                if col in df.columns:
                    df[col] = df[col].combine_first(df[meta_col])
                else:
                    df[col] = df[meta_col]
                df = df.drop(columns=[meta_col])

    if "unit" in df.columns:
        df["unit"] = df.apply(
            lambda row: row["unit"]
            if pd.notna(row["unit"]) and str(row["unit"]).strip()
            else get_unit(str(row["variable"])),
            axis=1,
        )

    df["sensor_label"] = df.apply(safe_sensor_name, axis=1)
    df["value_num"] = df["value_num"].astype(float)
    df["sensor_label"] = df["sensor_label"].map(str)

    sort_cols = [c for c in ["sensor_ref", "variable", "payload_time_utc", "id"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


# ============================================================
# PLOT PREP
# ============================================================
def prepare_plot_df(df, variable):
    out = df[df["variable"] == variable].copy()

    if out.empty:
        return out

    out["payload_time_utc"] = pd.to_datetime(out["payload_time_utc"], errors="coerce", utc=True)
    out["value_num"] = pd.to_numeric(out["value_num"], errors="coerce")

    out = out.dropna(subset=["payload_time_utc", "value_num", "sensor_label"]).copy()
    out = out[out["value_num"].map(lambda x: pd.notna(x) and math.isfinite(float(x)))].copy()

    out["value_num"] = out["value_num"].astype(float)
    out["sensor_label"] = out["sensor_label"].map(str)

    sort_cols = [c for c in ["sensor_label", "payload_time_utc", "id"] if c in out.columns]
    out = out.sort_values(sort_cols).reset_index(drop=True)

    return out


def build_timeseries_figure(plot_df, variable, show_points, height):
    y_title = with_unit(variable.capitalize(), variable)
    value_label = with_unit(variable, variable)
    hover_num_format = choose_hover_format(plot_df["value_num"])
    tick_format = choose_tick_format(plot_df["value_num"])

    fig = go.Figure()

    for sensor_name in plot_df["sensor_label"].dropna().unique():
        sub = plot_df[plot_df["sensor_label"] == sensor_name].copy()
        if sub.empty:
            continue

        fig.add_trace(
            go.Scatter(
                x=sub["payload_time_utc"].tolist(),
                y=sub["value_num"].astype(float).tolist(),
                mode="lines+markers" if show_points else "lines",
                name=str(sensor_name),
                hovertemplate=(
                    "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
                    "<b>Sensor</b>: %{fullData.name}<br>"
                    f"<b>{value_label}</b>: %{{y:{hover_num_format}}}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        height=height,
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title="Timestamp (UTC)",
        yaxis_title=y_title,
        hovermode="x unified",
        legend_title="Sensor",
        template="plotly_white",
    )

    if tick_format:
        fig.update_yaxes(tickformat=tick_format)

    return fig


# ============================================================
# SIDEBAR STATE
# ============================================================
if "selected_refs" not in st.session_state:
    st.session_state.selected_refs = []

if "selected_variables" not in st.session_state:
    st.session_state.selected_variables = []


# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.header("Testbed Data Selection")

try:
    sensors_df = load_sensors()
except Exception as exc:
    st.error(f"Unable to load sensors from Supabase: {exc}")
    st.stop()

if sensors_df.empty:
    st.error("No sensors found in Supabase. Is the collector running?")
    st.stop()

sensor_options = {}
for _, row in sensors_df.iterrows():
    label, sensor_ref = sensor_option_label(row)
    sensor_options[sensor_ref] = label

all_sensor_refs = list(sensor_options.keys())

col1, col2 = st.sidebar.columns(2)
if col1.button("Select all detectors"):
    st.session_state.selected_refs = all_sensor_refs

if col2.button("Clear detectors"):
    st.session_state.selected_refs = []

if not st.session_state.selected_refs and all_sensor_refs:
    st.session_state.selected_refs = all_sensor_refs[:1]

selected_refs = st.sidebar.multiselect(
    "Select Sensors",
    options=all_sensor_refs,
    default=st.session_state.selected_refs,
    format_func=lambda x: sensor_options[x],
    key="selected_refs",
)

if not selected_refs:
    st.warning("Please select at least one sensor.")
    st.stop()

try:
    vars_df = load_variables(selected_refs)
except Exception as exc:
    st.error(f"Unable to load variables from Supabase: {exc}")
    st.stop()

if vars_df.empty or "variable" not in vars_df.columns:
    st.warning("No measurements available for the selected sensors.")
    st.stop()

all_variables = vars_df["variable"].dropna().map(str).unique().tolist()

col3, col4 = st.sidebar.columns(2)
if col3.button("Select all variables"):
    st.session_state.selected_variables = all_variables

if col4.button("Clear variables"):
    st.session_state.selected_variables = []

if not st.session_state.selected_variables and all_variables:
    st.session_state.selected_variables = all_variables[: min(3, len(all_variables))]

selected_variables = st.sidebar.multiselect(
    "Select Measurements",
    options=all_variables,
    default=st.session_state.selected_variables,
    key="selected_variables",
)

if not selected_variables:
    st.warning("Please select at least one measurement.")
    st.stop()

range_mode = st.sidebar.radio(
    "Time Range",
    ["Last N days", "Last 24 hours"],
    index=0,
)

if range_mode == "Last N days":
    days = st.sidebar.slider("Historical Range (Days)", min_value=1, max_value=30, value=7)
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
else:
    start_dt = datetime.now(timezone.utc) - timedelta(hours=24)

start_utc = start_dt.isoformat()

show_points = st.sidebar.checkbox("Show markers", value=False)
show_plot_debug = st.sidebar.checkbox("Show plot debug", value=False)
auto_refresh = st.sidebar.checkbox("Auto-refresh every minute", value=False)


# ============================================================
# FETCH DATA
# ============================================================
try:
    data_df = load_multi_timeseries(
        sensor_refs=selected_refs,
        variables=selected_variables,
        start_utc=start_utc,
    )
except Exception as exc:
    st.error(f"Unable to load time series data from Supabase: {exc}")
    st.stop()

if data_df.empty:
    st.warning("No data found for the selected filters.")
    st.stop()


# ============================================================
# DEBUG / SANITY CHECK
# ============================================================
#with st.expander("Debug / data sanity check"):
 #   st.write("Loaded columns:")
  #  st.write(list(data_df.columns))
#
 #   st.write("Dtypes:")
  #  st.write(data_df.dtypes.astype(str))
#
 #   st.write("First rows:")
  #  safe_table(data_df.head(10), height=300)
#
 #   st.write("value_num summary:")
  #  st.write(data_df["value_num"].describe())
#
 #   st.write("Sample variable/value pairs:")
  #  sample_cols = [c for c in ["payload_time_utc", "sensor_label", "variable", "value_num", "value_text"] if c in data_df.columns]
   # safe_table(data_df[sample_cols].head(20), height=300)


# ============================================================
# OVERVIEW
# ============================================================
st.subheader("Testbed Overview")

overview_cols = st.columns(4)
overview_cols[0].metric("Selected sensors", len(selected_refs))
overview_cols[1].metric("Selected variables", len(selected_variables))
overview_cols[2].metric("Rows loaded", len(data_df))
overview_cols[3].metric("Time span start", start_dt.strftime("%Y-%m-%d %H:%M UTC"))

last_global_update = data_df["payload_time_utc"].max()
if pd.isna(last_global_update):
    st.caption("Last update in filtered data: unavailable")
else:
    st.caption(
        f"Last update in filtered data: {last_global_update.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )


# ============================================================
# OVERLAY COMPARISON
# ============================================================
st.subheader("Sensor Network Comparison")

overlay_variable = st.selectbox(
    "Variable for sensor comparison",
    options=selected_variables,
    index=0,
)

overlay_df = prepare_plot_df(data_df, overlay_variable)

if overlay_df.empty:
    st.info("No data available for the selected overlay variable.")
else:
    if show_plot_debug:
        with st.expander("Overlay debug values"):
            st.write("Dtypes used for plotting:")
            st.write(overlay_df[["payload_time_utc", "sensor_label", "value_num"]].dtypes.astype(str))
            st.write("First plotting rows:")
            safe_table(
                overlay_df[["payload_time_utc", "sensor_label", "value_num"]].head(20),
                height=300,
            )

    fig_overlay = build_timeseries_figure(
        plot_df=overlay_df,
        variable=overlay_variable,
        show_points=show_points,
        height=550,
    )
    st.plotly_chart(fig_overlay, use_container_width=True)


# ============================================================
# CURRENT SNAPSHOT
# ============================================================
st.subheader(f"Current Snapshot for '{overlay_variable}'")

snapshot_df = (
    overlay_df.sort_values(["sensor_ref", "payload_time_utc"])
    .groupby("sensor_ref", as_index=False)
    .tail(1)
)

if snapshot_df.empty:
    st.info("No latest snapshot available.")
else:
    ncols = min(4, len(snapshot_df))
    metric_cols = st.columns(ncols)

    for idx, (_, row) in enumerate(snapshot_df.iterrows()):
        metric_cols[idx % ncols].metric(
            row["sensor_label"],
            format_value_with_unit(row["value_num"], overlay_variable),
        )


# ============================================================
# MULTI-VARIABLE SECTION
# ============================================================
st.subheader("Environmental Parameters")

for variable in selected_variables:
    var_df = prepare_plot_df(data_df, variable)
    if var_df.empty:
        continue

    st.markdown(f"### {with_unit(variable.capitalize(), variable)}")

    latest_var_df = (
        var_df.sort_values(["sensor_ref", "payload_time_utc"])
        .groupby("sensor_ref", as_index=False)
        .tail(1)
    )

    avg_val = var_df["value_num"].mean()
    min_val = var_df["value_num"].min()
    max_val = var_df["value_num"].max()

    metrics_cols = st.columns(5)
    metrics_cols[0].metric("Sensors with data", latest_var_df["sensor_ref"].nunique())
    metrics_cols[1].metric("Average", format_value_with_unit(avg_val, variable))
    metrics_cols[2].metric("Minimum", format_value_with_unit(min_val, variable))
    metrics_cols[3].metric("Maximum", format_value_with_unit(max_val, variable))
    metrics_cols[4].metric("Points", len(var_df))

    if show_plot_debug:
        with st.expander(f"Debug plot values - {variable}"):
            st.write(var_df[["payload_time_utc", "sensor_label", "value_num"]].dtypes.astype(str))
            safe_table(
                var_df[["payload_time_utc", "sensor_label", "value_num"]].head(20),
                height=250,
            )

    fig_var = build_timeseries_figure(
        plot_df=var_df,
        variable=variable,
        show_points=show_points,
        height=420,
    )
    st.plotly_chart(fig_var, use_container_width=True)


# ============================================================
# LATEST VALUES TABLE
# ============================================================
st.subheader("Latest Sensor Readings")

latest_rows = (
    data_df.sort_values(["sensor_ref", "variable", "payload_time_utc"])
    .groupby(["sensor_label", "variable"], as_index=False)
    .tail(1)
    .copy()
)

if latest_rows.empty:
    st.info("No latest values available.")
else:
    latest_table = (
        latest_rows.pivot(index="sensor_label", columns="variable", values="value_num")
        .reset_index()
    )

    renamed_columns = {}
    for col in latest_table.columns:
        if col == "sensor_label":
            renamed_columns[col] = "Sensor"
        else:
            renamed_columns[col] = with_unit(str(col), str(col))

    latest_table = latest_table.rename(columns=renamed_columns)
    safe_table(latest_table, height=500)


# ============================================================
# RAW DATA + EXPORT
# ============================================================
st.subheader("Raw Measurements & Export")

with st.expander("View filtered raw data"):
    display_df = data_df.sort_values("payload_time_utc", ascending=False).copy()
    display_df["display_value"] = display_df.apply(
        lambda row: format_value_with_unit(row["value_num"], row["variable"]),
        axis=1,
    )

    display_columns = [
        col for col in [
            "id",
            "payload_time_utc",
            "inserted_at",
            "received_at_utc",
            "payload_time_unix",
            "sensor_label",
            "sensor_ref",
            "variable",
            "value_num",
            "value_text",
            "unit",
            "base_id",
            "base_name",
            "sensor_id",
            "sensor_name",
            "product_number",
            "display_value",
        ] if col in display_df.columns
    ]

    safe_table(display_df[display_columns], height=500)

csv_df = data_df.copy()

for col in ["payload_time_utc", "inserted_at", "received_at_utc"]:
    if col in csv_df.columns:
        csv_df[col] = csv_df[col].dt.strftime("%Y-%m-%d %H:%M:%S%z")

csv_bytes = csv_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="📥 Download filtered CSV",
    data=csv_bytes,
    file_name=f"aranet_dashboard_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)


# ============================================================
# AUTO REFRESH
# ============================================================
if auto_refresh:
    time.sleep(60)
    st.rerun()




