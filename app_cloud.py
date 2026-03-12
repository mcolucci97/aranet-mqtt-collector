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
import base64

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
# HEADER WITH RESPONSIVE LOGOS
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
CEA_LOGO = BASE_DIR / "cea_logo.png"
RADONNET_LOGO = BASE_DIR / "radonnet_logo.png"


def image_to_base64(path: Path) -> str | None:
    if not path.exists():
        return None
    return base64.b64encode(path.read_bytes()).decode("utf-8")


cea_logo_b64 = image_to_base64(CEA_LOGO)
radonnet_logo_b64 = image_to_base64(RADONNET_LOGO)

st.markdown(
    """
    <style>
        .hero-header {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 1rem 1.5rem;
            padding: 0.25rem 0 1rem 0;
            border-bottom: 1px solid rgba(120,120,120,0.25);
            margin-bottom: 1rem;
        }

        .hero-logos {
            display: flex;
            align-items: center;
            gap: 14px;
            flex: 0 0 auto;
        }

        .hero-logo {
            height: 56px;
            width: auto;
            object-fit: contain;
            display: block;
        }

        .hero-text {
            flex: 1 1 420px;
            min-width: 280px;
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

        @media (max-width: 900px) {
            .hero-header {
                align-items: flex-start;
            }

            .hero-text {
                flex-basis: 100%;
                min-width: 0;
            }

            .main-title {
                font-size: 1.6rem;
            }

            .hero-logo {
                height: 48px;
            }
        }
    </style>
    """,
    unsafe_allow_html=True,
)

logos_html = ""
if cea_logo_b64:
    logos_html += f'<img class="hero-logo" src="data:image/png;base64,{cea_logo_b64}" alt="CEA logo">'
if radonnet_logo_b64:
    logos_html += f'<img class="hero-logo" src="data:image/png;base64,{radonnet_logo_b64}" alt="RadonNET logo">'

st.markdown(
    f"""
    <div class="hero-header">
        <div class="hero-logos">
            {logos_html}
        </div>
        <div class="hero-text">
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
def get_unit(variable: str) -> str:
    return VARIABLE_UNITS.get(variable, "")


def with_unit(label: str, variable: str) -> str:
    unit = get_unit(variable)
    return f"{label} [{unit}]" if unit else label


def format_value(value, decimals: int = 2) -> str:
    if pd.isna(value):
        return "NA"

    value = float(value)
    abs_val = abs(value)

    if abs_val == 0:
        return "0"

    if abs_val < 1e-3 or abs_val >= 1e4:
        return f"{value:.3e}"

    return f"{value:.{decimals}f}"


def format_value_with_unit(value, variable: str, decimals: int = 2) -> str:
    base = format_value(value, decimals=decimals)
    unit = get_unit(variable)
    return f"{base} {unit}" if unit else base


def choose_plot_number_format(series: pd.Series):
    if series is None or len(series) == 0:
        return None

    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None

    max_abs = numeric.abs().max()
    if pd.isna(max_abs):
        return None

    if max_abs < 1e-3 or max_abs >= 1e4:
        return ".2e"

    return None


def choose_hover_format(series: pd.Series) -> str:
    if series is None or len(series) == 0:
        return ".2f"

    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return ".2f"

    max_abs = numeric.abs().max()
    if pd.isna(max_abs):
        return ".2f"

    if max_abs < 1e-3 or max_abs >= 1e4:
        return ".4e"

    return ".2f"


def format_sensor_label(row):
    sensor_name = row.get("sensor_name")
    sensor_id = row.get("sensor_id")
    sensor_ref = row.get("sensor_ref")

    display_name = (
        sensor_name if pd.notna(sensor_name) and str(sensor_name).strip()
        else "Unknown sensor"
    )
    display_id = sensor_id if pd.notna(sensor_id) else "no-id"

    return f"{display_name} ({display_id})", sensor_ref


def safe_sensor_name(row, mobile_mode: bool = False):
    sensor_name = row.get("sensor_name")
    sensor_id = row.get("sensor_id")
    sensor_ref = row.get("sensor_ref")

    if mobile_mode:
        if pd.notna(sensor_id):
            return str(sensor_id)
        if pd.notna(sensor_name) and str(sensor_name).strip():
            return str(sensor_name)[:18]
        return str(sensor_ref)

    if pd.notna(sensor_name) and str(sensor_name).strip():
        return str(sensor_name)
    if pd.notna(sensor_id):
        return str(sensor_id)
    return str(sensor_ref)


def compute_metrics(df: pd.DataFrame):
    latest_val = df["value_num"].iloc[-1]
    prev_val = df["value_num"].iloc[-2] if len(df) > 1 else latest_val
    delta = latest_val - prev_val

    min_val = df["value_num"].min()
    max_val = df["value_num"].max()
    avg_val = df["value_num"].mean()
    last_update = df["payload_time_utc"].iloc[-1]

    return {
        "latest": latest_val,
        "previous": prev_val,
        "delta": delta,
        "min": min_val,
        "max": max_val,
        "avg": avg_val,
        "last_update": last_update,
        "count": len(df),
    }


def df_to_records(df: pd.DataFrame, limit: int | None = None):
    if df is None or df.empty:
        return []

    out = df.copy()

    if limit is not None:
        out = out.head(limit)

    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    out = out.where(pd.notnull(out), None)
    return out.to_dict(orient="records")


def fetch_all(query_builder, page_size: int = 1000):
    all_rows = []
    start = 0

    while True:
        end = start + page_size - 1
        response = query_builder.range(start, end).execute()
        batch = response.data or []

        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < page_size:
            break

        start += page_size

    return all_rows


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
            ]
        )

    bases_df = load_bases()
    if not bases_df.empty:
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
        df.groupby("variable")
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
            "received_at_utc, payload_time_unix, payload_time_utc, "
            "base_id, sensor_id, sensor_ref, variable, value_text, value_num, unit"
        )
        .gte("payload_time_utc", start_utc)
        .in_("sensor_ref", sensor_refs)
        .in_("variable", variables)
        .order("payload_time_utc")
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["payload_time_utc"] = pd.to_datetime(df["payload_time_utc"], errors="coerce", utc=True)
    df["received_at_utc"] = pd.to_datetime(df["received_at_utc"], errors="coerce", utc=True)
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
    df["value_text"] = df["value_text"].astype(str)

    df = df.dropna(subset=["payload_time_utc", "value_num"])
    df = df.sort_values("payload_time_utc").reset_index(drop=True)

    sensors_df = load_sensors()
    if not sensors_df.empty:
        sensors_meta = sensors_df[
            [
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
        ].drop_duplicates()

        df = df.merge(
            sensors_meta,
            on="sensor_ref",
            how="left",
            suffixes=("", "_meta"),
        )

        for col in ["base_id", "sensor_id", "sensor_name", "base_name", "product_number"]:
            meta_col = f"{col}_meta"
            if meta_col in df.columns:
                if col in df.columns:
                    df[col] = df[meta_col].combine_first(df[col])
                else:
                    df[col] = df[meta_col]
                df = df.drop(columns=[meta_col])

    return df


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
st.sidebar.header("Data Selection")

try:
    sensors_df = load_sensors()
except Exception as e:
    st.error(f"Unable to load sensors from Supabase: {e}")
    st.stop()

if sensors_df.empty:
    st.error("No sensors found in Supabase. Is the connector running?")
    st.stop()

sensor_options = {}
for _, row in sensors_df.iterrows():
    label, sensor_ref = format_sensor_label(row)
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
except Exception as e:
    st.error(f"Unable to load variables from Supabase: {e}")
    st.stop()

if vars_df.empty or "variable" not in vars_df.columns:
    st.warning("No measurements available for the selected sensors.")
    st.stop()

all_variables = vars_df["variable"].dropna().unique().tolist()

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

time_container = st.sidebar.container()
slider_placeholder = st.sidebar.empty()
options_container = st.sidebar.container()

with time_container:
    range_mode = st.radio(
        "Time Range",
        ["Last N days", "Last 24 hours"],
        index=0,
        key="time_range_radio",
    )

if range_mode == "Last N days":
    with slider_placeholder.container():
        days = st.slider(
            "Historical Range (Days)",
            min_value=1,
            max_value=30,
            value=7,
            key="historical_days_slider",
        )
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
else:
    slider_placeholder.empty()
    start_dt = datetime.now(timezone.utc) - timedelta(hours=24)

start_utc = start_dt.isoformat()

with options_container:
    show_points = st.checkbox(
        "Show markers",
        value=False,
        key="show_markers_checkbox",
    )

    mobile_mode = st.checkbox(
        "Mobile-friendly mode",
        value=True,
        key="mobile_mode_checkbox",
    )

    show_plot_debug = st.checkbox(
        "Show plot debug",
        value=False,
        key="show_plot_debug_checkbox",
    )

    auto_refresh = st.checkbox(
        "Auto-refresh every minute",
        value=False,
        key="auto_refresh_checkbox",
    )


# ============================================================
# FETCH DATA
# ============================================================
try:
    data_df = load_multi_timeseries(
        sensor_refs=selected_refs,
        variables=selected_variables,
        start_utc=start_utc,
    )
except Exception as e:
    st.error(f"Unable to load time series data from Supabase: {e}")
    st.stop()

if data_df.empty:
    st.warning("No data found for the selected filters.")
    st.stop()

data_df["sensor_label"] = data_df.apply(lambda row: safe_sensor_name(row, mobile_mode=mobile_mode), axis=1)


# ============================================================
# MOBILE/DESKTOP LAYOUT SETTINGS
# ============================================================
if mobile_mode:
    overview_cols_n = 2
    snapshot_cols_n = 2
    chart_height_main = 430
    chart_height_secondary = 340
    chart_margin = dict(l=10, r=10, t=25, b=95)
    legend_cfg = dict(
        orientation="h",
        yanchor="top",
        y=-0.28,
        xanchor="center",
        x=0.5,
    )
    raw_limit = 40
else:
    overview_cols_n = 4
    snapshot_cols_n = 4
    chart_height_main = 550
    chart_height_secondary = 420
    chart_margin = dict(l=20, r=20, t=40, b=20)
    legend_cfg = dict(
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.02,
    )
    raw_limit = 100


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
st.subheader("Overview")

overview_cols = st.columns(overview_cols_n)
overview_values = [
    ("Selected sensors", len(selected_refs)),
    ("Selected variables", len(selected_variables)),
    ("Rows loaded", len(data_df)),
    ("Time span start", start_dt.strftime("%Y-%m-%d %H:%M UTC")),
]

for i, (label, value) in enumerate(overview_values):
    overview_cols[i % overview_cols_n].metric(label, value)

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
st.subheader("Overlay Comparison")

overlay_variable = st.selectbox(
    "Variable for sensor comparison",
    options=selected_variables,
    index=0,
)

overlay_df = data_df[data_df["variable"] == overlay_variable].copy()
overlay_unit = get_unit(overlay_variable)

if overlay_df.empty:
    st.info("No data available for the selected overlay variable.")
else:
    hover_num_format = choose_hover_format(overlay_df["value_num"])
    tick_format = choose_plot_number_format(overlay_df["value_num"])
    y_title = with_unit(overlay_variable.capitalize(), overlay_variable)

    fig_overlay = px.line(
        overlay_df,
        x="payload_time_utc",
        y="value_num",
        color="sensor_label",
        labels={
            "payload_time_utc": "Time (UTC)",
            "value_num": y_title,
            "sensor_label": "Sensor",
        },
        template="plotly_white",
    )

    value_label = f"{overlay_variable} [{overlay_unit}]" if overlay_unit else overlay_variable

    fig_overlay.update_traces(
        mode="lines+markers" if show_points else "lines",
        hovertemplate=(
            "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
            "<b>Sensor</b>: %{fullData.name}<br>"
            f"<b>{value_label}</b>: %{{y:{hover_num_format}}}<extra></extra>"
        ),
    )

    layout_kwargs = dict(
        height=chart_height_main,
        margin=chart_margin,
        xaxis_title="Timestamp (UTC)",
        yaxis_title=y_title,
        hovermode="x unified",
        legend_title="Sensor",
        legend=legend_cfg,
    )

    if tick_format:
        layout_kwargs["yaxis"] = dict(tickformat=tick_format)

    fig_overlay.update_layout(**layout_kwargs)
    st.plotly_chart(fig_overlay, use_container_width=True)

    if show_plot_debug:
        st.write("Overlay plot debug:")
        st.json(df_to_records(overlay_df[["payload_time_utc", "sensor_label", "value_num"]], limit=30))


# ============================================================
# CURRENT SNAPSHOT
# ============================================================
st.subheader(f"Current Snapshot for '{overlay_variable}'")

snapshot_df = (
    overlay_df.sort_values("payload_time_utc")
    .groupby("sensor_ref", as_index=False)
    .tail(1)
)

if not snapshot_df.empty:
    metric_cols = st.columns(snapshot_cols_n)
    for idx, (_, row) in enumerate(snapshot_df.iterrows()):
        metric_cols[idx % snapshot_cols_n].metric(
            row["sensor_label"],
            format_value_with_unit(row["value_num"], overlay_variable),
        )


# ============================================================
# MULTI-VARIABLE SECTION
# ============================================================
st.subheader("Selected Variables")

for variable in selected_variables:
    var_df = data_df[data_df["variable"] == variable].copy()
    if var_df.empty:
        continue

    unit = get_unit(variable)
    y_title = with_unit(variable.capitalize(), variable)
    hover_num_format = choose_hover_format(var_df["value_num"])
    tick_format = choose_plot_number_format(var_df["value_num"])

    st.markdown(f"### {y_title}")

    latest_var_df = (
        var_df.sort_values("payload_time_utc")
        .groupby("sensor_ref", as_index=False)
        .tail(1)
    )

    avg_val = var_df["value_num"].mean()
    min_val = var_df["value_num"].min()
    max_val = var_df["value_num"].max()

    metrics_cols = st.columns(5 if not mobile_mode else 2)
    metric_pairs = [
        ("Sensors with data", latest_var_df["sensor_ref"].nunique()),
        ("Average", format_value_with_unit(avg_val, variable)),
        ("Minimum", format_value_with_unit(min_val, variable)),
        ("Maximum", format_value_with_unit(max_val, variable)),
        ("Points", len(var_df)),
    ]

    for i, (label, value) in enumerate(metric_pairs):
        metrics_cols[i % len(metrics_cols)].metric(label, value)

    value_label = f"{variable} [{unit}]" if unit else variable

    fig_var = px.line(
        var_df,
        x="payload_time_utc",
        y="value_num",
        color="sensor_label",
        labels={
            "payload_time_utc": "Time (UTC)",
            "value_num": y_title,
            "sensor_label": "Sensor",
        },
        template="plotly_white",
    )

    fig_var.update_traces(
        mode="lines+markers" if show_points else "lines",
        hovertemplate=(
            "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
            "<b>Sensor</b>: %{fullData.name}<br>"
            f"<b>{value_label}</b>: %{{y:{hover_num_format}}}<extra></extra>"
        ),
    )

    layout_kwargs = dict(
        height=chart_height_secondary,
        margin=chart_margin,
        xaxis_title="Timestamp (UTC)",
        yaxis_title=y_title,
        hovermode="x unified",
        legend_title="Sensor",
        legend=legend_cfg,
    )

    if tick_format:
        layout_kwargs["yaxis"] = dict(tickformat=tick_format)

    fig_var.update_layout(**layout_kwargs)
    st.plotly_chart(fig_var, use_container_width=True)

    if show_plot_debug:
        st.write(f"{variable} plot debug:")
        st.json(df_to_records(var_df[["payload_time_utc", "sensor_label", "value_num"]], limit=20))


# ============================================================
# LATEST VALUES SUMMARY
# ============================================================
st.subheader("Latest Values Summary")

latest_table = (
    data_df.sort_values("payload_time_utc")
    .groupby(["sensor_label", "variable"], as_index=False)
    .tail(1)
    .pivot(index="sensor_label", columns="variable", values="value_num")
    .reset_index()
)

renamed_columns = {}
for col in latest_table.columns:
    if col == "sensor_label":
        renamed_columns[col] = "Sensor"
    else:
        renamed_columns[col] = with_unit(str(col), str(col))

latest_table = latest_table.rename(columns=renamed_columns)
st.json(df_to_records(latest_table))


# ============================================================
# RAW DATA + EXPORT
# ============================================================
st.subheader("Raw Data & Export")

with st.expander("View filtered raw data"):
    display_df = data_df.sort_values("payload_time_utc", ascending=False).copy()
    display_df["display_value"] = display_df.apply(
        lambda row: format_value_with_unit(row["value_num"], row["variable"]),
        axis=1,
    )

    display_columns = [
        col for col in [
            "payload_time_utc",
            "sensor_label",
            "sensor_ref",
            "variable",
            "value_num",
            "value_text",
            "unit",
            "display_value",
            "base_id",
            "base_name",
            "sensor_id",
            "sensor_name",
        ] if col in display_df.columns
    ]

    st.json(df_to_records(display_df[display_columns], limit=raw_limit))

csv_df = data_df.copy()
csv = csv_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="📥 Download filtered CSV",
    data=csv,
    file_name=f"aranet_dashboard_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)


# ============================================================
# AUTO REFRESH
# ============================================================
if auto_refresh:
    time.sleep(60)
    st.rerun()



