#!/usr/bin/env python3
"""
Streamlit dashboard for Aranet data stored in Supabase.

Compatible with Python 3.12.
Designed to:
- avoid Streamlit Arrow / LargeUtf8 table issues
- plot the real measurement value (value_num)
- keep CSV export correct
- show institutional CEA / RadonNET branding
- reduce repeated full downloads from Supabase
- support incremental refresh of time series data
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
import io 


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
    """Return file as base64 string if it exists."""
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
# GLOBAL CONFIGURATION
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

DEFAULT_DASHBOARD_VARIABLES = [
    "radon",
    "temperature",
    "humidity",
    "atmosphericpressure",
]

HISTORICAL_EXPORT_VARIABLES = [
    "radon",
    "temperature",
    "humidity",
    "atmosphericpressure",
]

MAX_DASHBOARD_DAYS = 30


# ============================================================
# SUPABASE CONNECTION
# ============================================================
@st.cache_resource

def get_supabase():
    return create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"],
    )


# ============================================================
# GENERIC HELPERS
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


def safe_sensor_name(row):
    sensor_name = row.get("sensor_name")
    sensor_id = row.get("sensor_id")
    sensor_ref = row.get("sensor_ref")

    if pd.notna(sensor_name) and str(sensor_name).strip():
        return str(sensor_name)
    if pd.notna(sensor_id):
        return str(sensor_id)
    return str(sensor_ref)


def optimize_dataframe_types(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    if "value_num" in out.columns:
        out["value_num"] = pd.to_numeric(out["value_num"], errors="coerce").astype("float32")

    category_candidates = [
        "sensor_ref",
        "variable",
        "unit",
        "base_id",
        "sensor_id",
        "sensor_name",
        "base_name",
        "product_number",
        "sensor_label",
    ]

    for col in category_candidates:
        if col in out.columns:
            out[col] = out[col].astype("category")

    return out


def fetch_all(query_builder, page_size: int = 1000):
    """
    Fetch all rows from a Supabase query using pagination.
    This should be used only for reasonably constrained queries.
    """
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


def merge_sensor_metadata(df: pd.DataFrame, sensors_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or sensors_df.empty:
        return df

    meta_cols = [
        "sensor_ref",
        "base_id",
        "sensor_id",
        "sensor_name",
        "product_number",
        "base_name",
    ]
    sensors_meta = sensors_df[[c for c in meta_cols if c in sensors_df.columns]].drop_duplicates()

    out = df.merge(sensors_meta, on="sensor_ref", how="left", suffixes=("", "_meta"))

    for col in ["sensor_id", "base_id", "sensor_name", "product_number", "base_name"]:
        meta_col = f"{col}_meta"
        if meta_col in out.columns:
            current = out[col] if col in out.columns else pd.Series([None] * len(out), index=out.index)
            out[col] = out[meta_col].combine_first(current)
            out = out.drop(columns=[meta_col])

    return out


def build_png_figure(df: pd.DataFrame, sensor_label: str, start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> io.BytesIO:
    variables = [v for v in HISTORICAL_EXPORT_VARIABLES if v in df["variable"].unique()]
    nrows = max(1, len(variables))

    fig, axes = plt.subplots(nrows=nrows, ncols=1, figsize=(12, 3.2 * nrows), sharex=True)

    if nrows == 1:
        axes = [axes]

    for ax, variable in zip(axes, variables):
        var_df = df[df["variable"] == variable].sort_values("payload_time_utc")
        ax.plot(var_df["payload_time_utc"], var_df["value_num"], linewidth=1.2)
        ax.set_ylabel(with_unit(variable.capitalize(), variable))
        ax.grid(True, alpha=0.3)

    axes[0].set_title(
        f"Historical export - {sensor_label}\n"
        f"{start_dt.strftime('%Y-%m-%d %H:%M UTC')} to {end_dt.strftime('%Y-%m-%d %H:%M UTC')}"
    )
    axes[-1].set_xlabel("Time (UTC)")
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=180, bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    return buffer


# ============================================================
# DATA LOADING
# ============================================================
@st.cache_data(ttl=300)
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


@st.cache_data(ttl=300)
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
            columns=["sensor_ref", "base_id", "sensor_id", "sensor_name", "product_number", "updated_at"]
        )

    bases_df = load_bases()
    if not bases_df.empty:
        df = df.merge(
            bases_df[["base_id", "base_name"]],
            on="base_id",
            how="left",
        )

    return df


@st.cache_data(ttl=300)
def load_dashboard_timeseries(sensor_refs, variables, start_utc):
    sb = get_supabase()

    if not sensor_refs or not variables:
        return pd.DataFrame()

    rows = fetch_all(
        sb.table("measurements_1h")
        .select(
            "bucket_start_utc, sensor_ref, variable, n_points, value_avg, value_min, value_max, value_std"
        )
        .gte("bucket_start_utc", start_utc)
        .in_("sensor_ref", sensor_refs)
        .in_("variable", variables)
        .order("bucket_start_utc")
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns={
        "bucket_start_utc": "payload_time_utc",
        "value_avg": "value_num",
    })
    df["payload_time_utc"] = pd.to_datetime(df["payload_time_utc"], errors="coerce", utc=True)
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
    df = df.dropna(subset=["payload_time_utc", "value_num"])
    df = df.sort_values("payload_time_utc").reset_index(drop=True)

    sensors_df = load_sensors()
    df = merge_sensor_metadata(df, sensors_df)
    df["sensor_label"] = df.apply(safe_sensor_name, axis=1)
    df = optimize_dataframe_types(df)
    return df


@st.cache_data(ttl=300)
def load_historical_export(sensor_ref: str, variables, start_utc: str, end_utc: str):
    sb = get_supabase()

    if not sensor_ref or not variables:
        return pd.DataFrame()

    rows = fetch_all(
        sb.table("measurements")
        .select(
            "payload_time_utc, sensor_ref, variable, value_num, unit"
        )
        .gte("payload_time_utc", start_utc)
        .lte("payload_time_utc", end_utc)
        .eq("sensor_ref", sensor_ref)
        .in_("variable", variables)
        .order("payload_time_utc")
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    df["payload_time_utc"] = pd.to_datetime(df["payload_time_utc"], errors="coerce", utc=True)
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
    df = df.dropna(subset=["payload_time_utc", "value_num"])
    df = df.sort_values("payload_time_utc").reset_index(drop=True)

    sensors_df = load_sensors()
    df = merge_sensor_metadata(df, sensors_df)
    df["sensor_label"] = df.apply(safe_sensor_name, axis=1)
    df = optimize_dataframe_types(df)
    return df


# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.header("Navigation")
mode = st.sidebar.radio(
    "Mode",
    ["Dashboard (≤ 30 days)", "Historical export (> 30 days or detailed request)"],
    index=0,
)

auto_refresh = st.sidebar.checkbox("Auto-refresh every minute", value=False)

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


# ============================================================
# DASHBOARD MODE
# ============================================================
if mode == "Dashboard (≤ 30 days)":
    st.sidebar.header("Dashboard filters")

    selected_refs = st.sidebar.multiselect(
        "Select sensors",
        options=list(sensor_options.keys()),
        format_func=lambda x: sensor_options[x],
        default=list(sensor_options.keys())[: min(4, len(sensor_options))],
    )

    if not selected_refs:
        st.warning("Please select at least one sensor.")
        st.stop()

    dashboard_variables = st.sidebar.multiselect(
        "Select variables",
        options=DEFAULT_DASHBOARD_VARIABLES,
        default=[v for v in ["radon", "temperature", "humidity"] if v in DEFAULT_DASHBOARD_VARIABLES],
    )

    if not dashboard_variables:
        st.warning("Please select at least one variable.")
        st.stop()

    days = st.sidebar.slider(
        "Historical range (days)",
        min_value=1,
        max_value=MAX_DASHBOARD_DAYS,
        value=7,
    )

    show_points = st.sidebar.checkbox("Show markers", value=False)

    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    start_utc = start_dt.isoformat()

    try:
        with st.spinner("Loading hourly aggregated data..."):
            data_df = load_dashboard_timeseries(
                sensor_refs=selected_refs,
                variables=dashboard_variables,
                start_utc=start_utc,
            )
    except Exception as e:
        st.error(f"Unable to load dashboard data from Supabase: {e}")
        st.stop()

    if data_df.empty:
        st.warning("No aggregated hourly data found for the selected filters.")
        st.info("Make sure the measurements_1h table is populated in Supabase.")
        st.stop()

    st.subheader("Overview")
    overview_cols = st.columns(4)
    overview_cols[0].metric("Selected sensors", len(selected_refs))
    overview_cols[1].metric("Selected variables", len(dashboard_variables))
    overview_cols[2].metric("Rows loaded", len(data_df))
    overview_cols[3].metric("Time span start", start_dt.strftime("%Y-%m-%d %H:%M UTC"))

    last_global_update = data_df["payload_time_utc"].max()
    if pd.isna(last_global_update):
        st.caption("Last update in filtered data: unavailable")
    else:
        st.caption(
            f"Last update in filtered data: {last_global_update.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )

    st.subheader("Overlay comparison")
    overlay_variable = st.selectbox(
        "Variable for sensor comparison",
        options=dashboard_variables,
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
        value_label = f"{overlay_variable} [{overlay_unit}]" if overlay_unit else overlay_variable

        fig_overlay = go.Figure()
        for sensor_label, sensor_df in overlay_df.groupby("sensor_label", observed=True):
            fig_overlay.add_trace(
                go.Scattergl(
                    x=sensor_df["payload_time_utc"],
                    y=sensor_df["value_num"],
                    mode="lines+markers" if show_points else "lines",
                    name=str(sensor_label),
                    hovertemplate=(
                        "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
                        "<b>Sensor</b>: %{fullData.name}<br>"
                        f"<b>{value_label}</b>: %{{y:{hover_num_format}}}<extra></extra>"
                    ),
                )
            )

        layout_kwargs = dict(
            height=550,
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis_title="Timestamp (UTC)",
            yaxis_title=y_title,
            hovermode="x unified",
            legend_title="Sensor",
            template="plotly_white",
        )
        if tick_format:
            layout_kwargs["yaxis"] = dict(tickformat=tick_format)

        fig_overlay.update_layout(**layout_kwargs)
        st.plotly_chart(fig_overlay, use_container_width=True)

    st.subheader(f"Current snapshot for '{overlay_variable}'")
    snapshot_df = (
        overlay_df.sort_values("payload_time_utc")
        .groupby("sensor_ref", as_index=False, observed=True)
        .tail(1)
    )

    if not snapshot_df.empty:
        ncols = min(4, len(snapshot_df))
        metric_cols = st.columns(ncols)

        for idx, (_, row) in enumerate(snapshot_df.iterrows()):
            col = metric_cols[idx % ncols]
            col.metric(
                str(row["sensor_label"]),
                format_value_with_unit(row["value_num"], overlay_variable),
            )

    st.subheader("Detailed variable view")
    detailed_variable = st.selectbox(
        "Select one variable for detailed plot",
        options=dashboard_variables,
        index=0,
        key="detailed_variable_dashboard",
    )

    var_df = data_df[data_df["variable"] == detailed_variable].copy()
    if not var_df.empty:
        y_title = with_unit(detailed_variable.capitalize(), detailed_variable)
        hover_num_format = choose_hover_format(var_df["value_num"])
        tick_format = choose_plot_number_format(var_df["value_num"])
        unit = get_unit(detailed_variable)
        value_label = f"{detailed_variable} [{unit}]" if unit else detailed_variable

        latest_var_df = (
            var_df.sort_values("payload_time_utc")
            .groupby("sensor_ref", as_index=False, observed=True)
            .tail(1)
        )

        avg_val = var_df["value_num"].mean()
        min_val = var_df["value_num"].min()
        max_val = var_df["value_num"].max()

        metrics_cols = st.columns(5)
        metrics_cols[0].metric("Sensors with data", latest_var_df["sensor_ref"].nunique())
        metrics_cols[1].metric("Average", format_value_with_unit(avg_val, detailed_variable))
        metrics_cols[2].metric("Minimum", format_value_with_unit(min_val, detailed_variable))
        metrics_cols[3].metric("Maximum", format_value_with_unit(max_val, detailed_variable))
        metrics_cols[4].metric("Points", len(var_df))

        fig_var = go.Figure()
        for sensor_label, sensor_df in var_df.groupby("sensor_label", observed=True):
            fig_var.add_trace(
                go.Scattergl(
                    x=sensor_df["payload_time_utc"],
                    y=sensor_df["value_num"],
                    mode="lines+markers" if show_points else "lines",
                    name=str(sensor_label),
                    hovertemplate=(
                        "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
                        "<b>Sensor</b>: %{fullData.name}<br>"
                        f"<b>{value_label}</b>: %{{y:{hover_num_format}}}<extra></extra>"
                    ),
                )
            )

        layout_kwargs = dict(
            height=450,
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis_title="Timestamp (UTC)",
            yaxis_title=y_title,
            hovermode="x unified",
            legend_title="Sensor",
            template="plotly_white",
        )
        if tick_format:
            layout_kwargs["yaxis"] = dict(tickformat=tick_format)

        fig_var.update_layout(**layout_kwargs)
        st.plotly_chart(fig_var, use_container_width=True)

    st.subheader("Latest values table")
    latest_table = (
        data_df.sort_values("payload_time_utc")
        .groupby(["sensor_label", "variable"], as_index=False, observed=True)
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
    st.dataframe(latest_table, use_container_width=True, hide_index=True)

    with st.expander("View hourly aggregated raw table"):
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
                "n_points",
                "value_min",
                "value_max",
                "value_std",
                "display_value",
                "base_id",
                "base_name",
                "sensor_id",
                "sensor_name",
            ] if col in display_df.columns
        ]

        st.dataframe(
            display_df[display_columns],
            use_container_width=True,
            hide_index=True,
        )

        csv_df = display_df.copy()
        csv = csv_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download hourly aggregated CSV",
            data=csv,
            file_name=f"aranet_dashboard_hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


# ============================================================
# HISTORICAL EXPORT MODE
# ============================================================
else:
    st.sidebar.header("Historical export filters")

    selected_ref = st.sidebar.selectbox(
        "Select one detector",
        options=list(sensor_options.keys()),
        format_func=lambda x: sensor_options[x],
    )

    selected_export_variables = st.sidebar.multiselect(
        "Variables for export",
        options=HISTORICAL_EXPORT_VARIABLES,
        default=HISTORICAL_EXPORT_VARIABLES,
        disabled=True,
    )

    default_end = datetime.now(timezone.utc)
    default_start = default_end - timedelta(days=60)

    start_date = st.sidebar.date_input("Start date (UTC)", value=default_start.date())
    end_date = st.sidebar.date_input("End date (UTC)", value=default_end.date())

    if start_date > end_date:
        st.error("Start date must be earlier than or equal to end date.")
        st.stop()

    start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)

    max_days = (end_dt - start_dt).days + 1
    st.info(
        "Historical export uses raw 10-minute data for one detector at a time and only the 4 main variables."
    )
    st.caption(f"Selected interval: {max_days} day(s)")

    try:
        with st.spinner("Loading detailed historical data..."):
            export_df = load_historical_export(
                sensor_ref=selected_ref,
                variables=selected_export_variables,
                start_utc=start_dt.isoformat(),
                end_utc=end_dt.isoformat(),
            )
    except Exception as e:
        st.error(f"Unable to load historical data from Supabase: {e}")
        st.stop()

    if export_df.empty:
        st.warning("No raw data found for the selected detector and time interval.")
        st.stop()

    selected_sensor_label = sensor_options[selected_ref]

    st.subheader("Historical export overview")
    overview_cols = st.columns(4)
    overview_cols[0].metric("Detector", selected_sensor_label)
    overview_cols[1].metric("Variables", export_df["variable"].nunique())
    overview_cols[2].metric("Rows loaded", len(export_df))
    overview_cols[3].metric(
        "Time span",
        f"{export_df['payload_time_utc'].min().strftime('%Y-%m-%d')} → {export_df['payload_time_utc'].max().strftime('%Y-%m-%d')}",
    )

    st.subheader("Historical chart")
    available_variables = [v for v in HISTORICAL_EXPORT_VARIABLES if v in export_df["variable"].astype(str).unique()]
    fig_hist = make_subplots(
        rows=len(available_variables),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=[with_unit(v.capitalize(), v) for v in available_variables],
    )

    for idx, variable in enumerate(available_variables, start=1):
        var_df = export_df[export_df["variable"].astype(str) == variable].sort_values("payload_time_utc")
        hover_num_format = choose_hover_format(var_df["value_num"])
        fig_hist.add_trace(
            go.Scattergl(
                x=var_df["payload_time_utc"],
                y=var_df["value_num"],
                mode="lines",
                name=variable,
                showlegend=False,
                hovertemplate=(
                    "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
                    f"<b>{with_unit(variable.capitalize(), variable)}</b>: %{{y:{hover_num_format}}}<extra></extra>"
                ),
            ),
            row=idx,
            col=1,
        )
        fig_hist.update_yaxes(title_text=with_unit(variable.capitalize(), variable), row=idx, col=1)

    fig_hist.update_layout(
        height=260 * max(1, len(available_variables)),
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="Timestamp (UTC)",
        hovermode="x unified",
        template="plotly_white",
        title=f"Historical export - {selected_sensor_label}",
    )
    st.plotly_chart(fig_hist, use_container_width=True)

    png_buffer = build_png_figure(export_df, selected_sensor_label, export_df["payload_time_utc"].min(), export_df["payload_time_utc"].max())

    csv_df = export_df.copy()
    csv_df["sensor_label"] = csv_df["sensor_label"].astype(str)
    csv = csv_df.to_csv(index=False).encode("utf-8")

    download_cols = st.columns(2)
    download_cols[0].download_button(
        label="📥 Download historical CSV",
        data=csv,
        file_name=f"historical_export_{selected_ref}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )
    download_cols[1].download_button(
        label="🖼️ Download historical PNG",
        data=png_buffer.getvalue(),
        file_name=f"historical_export_{selected_ref}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
        mime="image/png",
    )

    with st.expander("View raw detailed data"):
        display_df = export_df.sort_values("payload_time_utc", ascending=False).copy()
        display_df["display_value"] = display_df.apply(
            lambda row: format_value_with_unit(row["value_num"], str(row["variable"])),
            axis=1,
        )

        display_columns = [
            col for col in [
                "payload_time_utc",
                "sensor_label",
                "sensor_ref",
                "variable",
                "value_num",
                "unit",
                "display_value",
                "base_id",
                "base_name",
                "sensor_id",
                "sensor_name",
            ] if col in display_df.columns
        ]

        st.dataframe(
            display_df[display_columns],
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# AUTO REFRESH
# ============================================================
if auto_refresh and mode == "Dashboard (≤ 30 days)":
    time.sleep(60)
    st.rerun()
