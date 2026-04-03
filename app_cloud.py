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
import hmac 

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from supabase import create_client
import io 
from plotly.subplots import make_subplots  # for multi-panel figures
import matplotlib.pyplot as plt  # for PNG export
import plotly.express as px


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
# APP PASSWORD
# ============================================================
def check_app_password() -> bool:
    """Protect the whole app with a shared password stored in Streamlit secrets."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.title("🔒 Private dashboard")
    st.write("Enter the shared password to access the dashboard.")

    if "APP_PASSWORD" not in st.secrets:
        st.error("APP_PASSWORD is missing from Streamlit secrets.")
        return False

    password_input = st.text_input("Password", type="password")
    login_clicked = st.button("Login", use_container_width=True)

    if login_clicked:
        expected_password = str(st.secrets["APP_PASSWORD"])
        if hmac.compare_digest(password_input, expected_password):
            st.session_state.authenticated = True
            st.rerun()
        st.error("Wrong password.")

    return False


if not check_app_password():
    st.stop()


# ============================================================
# APP HEADER
# ============================================================
st.title("🔬 Lab Environmental Monitoring")
st.caption("Real-time and historical data from Aranet Base Station")


# ============================================================
# CONFIGURATION
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

DEFAULT_VARIABLE_ORDER = [
    "radon",
    "temperature",
    "humidity",
    "atmosphericpressure",
    "pm1",
    "pm2_5",
    "pm10",
    "battery",
    "rssi",
]

HISTORICAL_EXPORT_VARIABLES = [
    "radon",
    "temperature",
    "humidity",
    "atmosphericpressure",
    "pm1",
    "pm2_5",
    "pm10",
]

MAX_DASHBOARD_DAYS = 30
RAW_FETCH_PAGE_SIZE = 1000
AGG_FETCH_PAGE_SIZE = 1000


# ============================================================
# SUPABASE CONNECTION
# ============================================================
@st.cache_resource

def get_supabase():
    """Create and cache the Supabase client."""
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


def format_sensor_label(row: pd.Series):
    sensor_name = row.get("sensor_name")
    sensor_id = row.get("sensor_id")
    sensor_ref = row.get("sensor_ref")

    display_name = (
        str(sensor_name)
        if pd.notna(sensor_name) and str(sensor_name).strip()
        else "Unknown sensor"
    )
    display_id = str(sensor_id) if pd.notna(sensor_id) else "no-id"
    return f"{display_name} ({display_id})", sensor_ref


def safe_sensor_name(row: pd.Series) -> str:
    sensor_name = row.get("sensor_name")
    sensor_id = row.get("sensor_id")
    sensor_ref = row.get("sensor_ref")

    if pd.notna(sensor_name) and str(sensor_name).strip():
        return str(sensor_name)
    if pd.notna(sensor_id):
        return str(sensor_id)
    return str(sensor_ref)


def order_variables(variables: list[str]) -> list[str]:
    """Return variables ordered by the configured preferred order."""
    order_map = {name: idx for idx, name in enumerate(DEFAULT_VARIABLE_ORDER)}
    return sorted(variables, key=lambda x: (order_map.get(x, 999), x))


def fetch_all(query_builder, page_size: int = 1000):
    """Fetch all rows from a Supabase query using pagination."""
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


def normalize_timeseries_df(df: pd.DataFrame, time_col: str, value_col: str = "value_num") -> pd.DataFrame:
    """Normalize timestamps and numeric values and drop unusable rows."""
    if df.empty:
        return df

    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce", utc=True)
    if value_col in df.columns:
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce").astype("float32")
        df = df.dropna(subset=[time_col, value_col])
    else:
        df = df.dropna(subset=[time_col])

    for col in ["sensor_ref", "variable"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df.sort_values(time_col).reset_index(drop=True)


def attach_sensor_metadata(df: pd.DataFrame, sensors_df: pd.DataFrame) -> pd.DataFrame:
    """Merge sensor metadata on sensor_ref."""
    if df.empty or sensors_df.empty:
        return df

    meta_cols = [
        c for c in [
            "sensor_ref",
            "base_id",
            "sensor_id",
            "sensor_name",
            "product_number",
            "base_name",
        ]
        if c in sensors_df.columns
    ]
    sensors_meta = sensors_df[meta_cols].drop_duplicates()
    out = df.merge(sensors_meta, on="sensor_ref", how="left", suffixes=("", "_meta"))

    for col in ["sensor_id", "base_id", "sensor_name", "product_number", "base_name"]:
        meta_col = f"{col}_meta"
        if meta_col in out.columns:
            if col in out.columns:
                out[col] = out[meta_col].combine_first(out[col])
            else:
                out[col] = out[meta_col]
            out = out.drop(columns=[meta_col])

    return out


def build_png_figure(
    df: pd.DataFrame,
    sensor_label: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
) -> io.BytesIO:
    """Build a multi-panel PNG figure for historical export using Matplotlib."""
    variables = [v for v in HISTORICAL_EXPORT_VARIABLES if v in df["variable"].astype(str).unique()]
    nrows = max(1, len(variables))

    fig, axes = plt.subplots(nrows=nrows, ncols=1, figsize=(12, 3.2 * nrows), sharex=True)
    if nrows == 1:
        axes = [axes]

    for ax, variable in zip(axes, variables):
        var_df = df[df["variable"].astype(str) == variable].copy()
        var_df = var_df.sort_values("payload_time_utc")

        ax.plot(var_df["payload_time_utc"], var_df["value_num"], linewidth=1.2)
        ax.set_ylabel(with_unit(variable, variable))
        ax.grid(True, alpha=0.3)
        ax.set_title(variable)

    axes[-1].set_xlabel("Time (UTC)")
    fig.suptitle(
        f"Historical export - {sensor_label}\n"
        f"{start_dt.strftime('%Y-%m-%d %H:%M UTC')} to {end_dt.strftime('%Y-%m-%d %H:%M UTC')}",
        fontsize=12,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.97])

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=180, bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    return buffer


# ============================================================
# DATA LOADING
# ============================================================
@st.cache_data(ttl=60)
def load_bases() -> pd.DataFrame:
    sb = get_supabase()
    rows = fetch_all(
        sb.table("bases")
        .select("base_id, base_name, updated_at")
        .order("base_id"),
        page_size=RAW_FETCH_PAGE_SIZE,
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["base_id", "base_name", "updated_at"])
    return df


@st.cache_data(ttl=60)
def load_sensors() -> pd.DataFrame:
    sb = get_supabase()
    rows = fetch_all(
        sb.table("sensors")
        .select("sensor_ref, base_id, sensor_id, sensor_name, product_number, updated_at")
        .order("base_id")
        .order("sensor_id"),
        page_size=RAW_FETCH_PAGE_SIZE,
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

    bases_df = load_bases()
    if not bases_df.empty:
        df = df.merge(bases_df[["base_id", "base_name"]], on="base_id", how="left")

    return df


@st.cache_data(ttl=60)
def load_dashboard_variables(sensor_refs: tuple[str, ...]) -> pd.DataFrame:
    """Load available variables from the aggregated hourly table for selected sensors."""
    sb = get_supabase()

    if not sensor_refs:
        return pd.DataFrame(columns=["variable", "n"])

    rows = fetch_all(
        sb.table("measurements_1h")
        .select("sensor_ref, variable")
        .in_("sensor_ref", list(sensor_refs)),
        page_size=AGG_FETCH_PAGE_SIZE,
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
def load_dashboard_timeseries(
    sensor_refs: tuple[str, ...],
    variables: tuple[str, ...],
    start_utc: str,
) -> pd.DataFrame:
    """Load aggregated hourly data for the dashboard."""
    sb = get_supabase()

    if not sensor_refs or not variables:
        return pd.DataFrame()

    rows = fetch_all(
        sb.table("measurements_1h")
        .select(
            "bucket_start_utc, sensor_ref, variable, "
            "n_points, value_avg, value_min, value_max, value_std"
        )
        .gte("bucket_start_utc", start_utc)
        .in_("sensor_ref", list(sensor_refs))
        .in_("variable", list(variables))
        .order("bucket_start_utc"),
        page_size=AGG_FETCH_PAGE_SIZE,
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"bucket_start_utc": "payload_time_utc", "value_avg": "value_num"})
    df = normalize_timeseries_df(df, time_col="payload_time_utc", value_col="value_num")
    df = attach_sensor_metadata(df, load_sensors())
    df["sensor_label"] = df.apply(safe_sensor_name, axis=1)
    return df


@st.cache_data(ttl=60)
def load_historical_raw(
    sensor_ref: str,
    variables: tuple[str, ...],
    start_utc: str,
    end_utc: str,
) -> pd.DataFrame:
    """Load raw data for a single sensor over an arbitrary user-defined period."""
    sb = get_supabase()

    if not sensor_ref or not variables:
        return pd.DataFrame()

    rows = fetch_all(
        sb.table("measurements")
        .select(
            "received_at_utc, payload_time_unix, payload_time_utc, "
            "base_id, sensor_id, sensor_ref, variable, value_text, value_num, unit"
        )
        .eq("sensor_ref", sensor_ref)
        .in_("variable", list(variables))
        .gte("payload_time_utc", start_utc)
        .lte("payload_time_utc", end_utc)
        .order("payload_time_utc"),
        page_size=RAW_FETCH_PAGE_SIZE,
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    df = normalize_timeseries_df(df, time_col="payload_time_utc", value_col="value_num")
    if "received_at_utc" in df.columns:
        df["received_at_utc"] = pd.to_datetime(df["received_at_utc"], errors="coerce", utc=True)

    df = attach_sensor_metadata(df, load_sensors())
    df["sensor_label"] = df.apply(safe_sensor_name, axis=1)
    return df


# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.header("Navigation")
page_mode = st.sidebar.radio(
    "Choose section",
    ["Dashboard", "Historical export"],
    index=0,
)

auto_refresh = st.sidebar.checkbox("Auto-refresh every minute", value=False)
show_points = st.sidebar.checkbox("Show markers", value=False)

try:
    sensors_df = load_sensors()
except Exception as exc:
    st.error(f"Unable to load sensors from Supabase: {exc}")
    st.stop()

if sensors_df.empty:
    st.error("No sensors found in Supabase. Is the connector running?")
    st.stop()

sensor_options = {}
for _, sensor_row in sensors_df.iterrows():
    label, sensor_ref = format_sensor_label(sensor_row)
    sensor_options[sensor_ref] = label


# ============================================================
# DASHBOARD MODE
# ============================================================
if page_mode == "Dashboard":
    st.sidebar.header("Dashboard filters")

    selected_refs = st.sidebar.multiselect(
        "Select Sensors",
        options=list(sensor_options.keys()),
        format_func=lambda x: sensor_options[x],
        default=list(sensor_options.keys())[: min(3, len(sensor_options))],
    )

    if not selected_refs:
        st.warning("Please select at least one sensor.")
        st.stop()

    try:
        vars_df = load_dashboard_variables(tuple(selected_refs))
    except Exception as exc:
        st.error(f"Unable to load variables from aggregated table measurements_1h: {exc}")
        st.stop()

    if vars_df.empty or "variable" not in vars_df.columns:
        st.warning(
            "No aggregated hourly data found for the selected sensors. "
            "Check whether measurements_1h has been populated."
        )
        st.stop()

    available_variables = order_variables(vars_df["variable"].dropna().astype(str).unique().tolist())
    selected_variables = st.sidebar.multiselect(
        "Select Measurements",
        options=available_variables,
        default=available_variables[: min(4, len(available_variables))],
    )

    if not selected_variables:
        st.warning("Please select at least one measurement.")
        st.stop()

    days = st.sidebar.slider(
        "Historical Range (Days)",
        min_value=1,
        max_value=MAX_DASHBOARD_DAYS,
        value=min(7, MAX_DASHBOARD_DAYS),
    )

    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    start_utc = start_dt.isoformat()

    try:
        data_df = load_dashboard_timeseries(
            sensor_refs=tuple(selected_refs),
            variables=tuple(selected_variables),
            start_utc=start_utc,
        )
    except Exception as exc:
        st.error(f"Unable to load hourly dashboard data: {exc}")
        st.stop()

    if data_df.empty:
        st.warning("No data found for the selected dashboard filters.")
        st.stop()

    # Overview
    st.subheader("Overview")
    overview_cols = st.columns(4)
    overview_cols[0].metric("Selected sensors", len(selected_refs))
    overview_cols[1].metric("Selected variables", len(selected_variables))
    overview_cols[2].metric("Rows loaded", len(data_df))
    overview_cols[3].metric("Time span start", start_dt.strftime("%Y-%m-%d %H:%M UTC"))

    last_global_update = data_df["payload_time_utc"].max()
    if pd.notna(last_global_update):
        st.caption(f"Last update in filtered data: {last_global_update.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # Variable-by-variable charts
    st.subheader("Selected Variables")

    for variable in selected_variables:
        var_df = data_df[data_df["variable"].astype(str) == variable].copy()
        if var_df.empty:
            continue

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

        metrics_cols = st.columns(5)
        metrics_cols[0].metric("Sensors with data", latest_var_df["sensor_ref"].nunique())
        metrics_cols[1].metric("Average", format_value_with_unit(avg_val, variable))
        metrics_cols[2].metric("Minimum", format_value_with_unit(min_val, variable))
        metrics_cols[3].metric("Maximum", format_value_with_unit(max_val, variable))
        metrics_cols[4].metric("Hourly points", len(var_df))

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

        unit = get_unit(variable)
        value_label = f"{variable} [{unit}]" if unit else variable
        fig_var.update_traces(
            mode="lines+markers" if show_points else "lines",
            hovertemplate=(
                "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
                "<b>Sensor</b>: %{fullData.name}<br>"
                f"<b>{value_label}</b>: %{{y:{hover_num_format}}}<extra></extra>"
            ),
        )

        layout_kwargs = dict(
            height=420,
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis_title="Timestamp (UTC)",
            yaxis_title=y_title,
            hovermode="x unified",
            legend_title="Sensor",
        )
        if tick_format:
            layout_kwargs["yaxis"] = dict(tickformat=tick_format)

        fig_var.update_layout(**layout_kwargs)
        st.plotly_chart(fig_var, use_container_width=True)

    # Latest values table
    st.subheader("Latest Values Table")
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
    st.dataframe(latest_table, use_container_width=True, hide_index=True)

    # Aggregated raw table and export
    st.subheader("Hourly Aggregated Data & Export")
    with st.expander("View aggregated hourly data"):
        display_df = data_df.sort_values("payload_time_utc", ascending=False).copy()
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
                "n_points",
                "value_min",
                "value_max",
                "value_std",
                "display_value",
                "base_id",
                "base_name",
                "sensor_id",
                "sensor_name",
            ]
            if col in display_df.columns
        ]
        st.dataframe(display_df[display_columns], use_container_width=True, hide_index=True)

    csv_df = data_df.copy()
    csv_bytes = csv_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download hourly aggregated CSV",
        data=csv_bytes,
        file_name=f"aranet_dashboard_hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )


# ============================================================
# HISTORICAL EXPORT MODE
# ============================================================
else:
    st.sidebar.header("Historical export filters")

    selected_sensor_ref = st.sidebar.selectbox(
        "Select one sensor",
        options=list(sensor_options.keys()),
        format_func=lambda x: sensor_options[x],
        index=0,
    )
    selected_sensor_label = sensor_options[selected_sensor_ref]

    export_variables = st.sidebar.multiselect(
        "Select variables",
        options=HISTORICAL_EXPORT_VARIABLES,
        default=HISTORICAL_EXPORT_VARIABLES,
    )

    if not export_variables:
        st.warning("Please select at least one variable for historical export.")
        st.stop()

    default_end = datetime.now(timezone.utc)
    default_start = default_end - timedelta(days=7)

    start_date = st.sidebar.date_input("Start date", value=default_start.date())
    end_date = st.sidebar.date_input("End date", value=default_end.date())

    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    export_start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    export_end_dt = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)

    try:
        export_df = load_historical_raw(
            sensor_ref=selected_sensor_ref,
            variables=tuple(export_variables),
            start_utc=export_start_dt.isoformat(),
            end_utc=export_end_dt.isoformat(),
        )
    except Exception as exc:
        st.error(f"Unable to load historical raw data: {exc}")
        st.stop()

    st.subheader("Historical export")
    st.caption(
        f"Selected sensor: **{selected_sensor_label}**  \\\nSelected period: **{export_start_dt.strftime('%Y-%m-%d')}** to **{export_end_dt.strftime('%Y-%m-%d')}**"
    )

    if export_df.empty:
        st.warning("No historical raw data found for the selected sensor, variables, and period.")
        st.stop()

    # Summary metrics
    summary_cols = st.columns(4)
    summary_cols[0].metric("Sensor", selected_sensor_label)
    summary_cols[1].metric("Variables", export_df["variable"].astype(str).nunique())
    summary_cols[2].metric("Rows loaded", len(export_df))
    summary_cols[3].metric(
        "Last data point",
        export_df["payload_time_utc"].max().strftime("%Y-%m-%d %H:%M UTC"),
    )

    # Multi-panel Plotly chart
    st.subheader("Historical chart")
    available_variables = [v for v in HISTORICAL_EXPORT_VARIABLES if v in export_df["variable"].astype(str).unique()]

    fig_hist = make_subplots(
        rows=len(available_variables),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=[with_unit(v.capitalize(), v) for v in available_variables],
    )

    for row_idx, variable in enumerate(available_variables, start=1):
        var_df = export_df[export_df["variable"].astype(str) == variable].copy()
        hover_num_format = choose_hover_format(var_df["value_num"])
        unit = get_unit(variable)
        value_label = f"{variable} [{unit}]" if unit else variable

        fig_hist.add_trace(
            go.Scatter(
                x=var_df["payload_time_utc"],
                y=var_df["value_num"],
                mode="lines+markers" if show_points else "lines",
                name=variable,
                showlegend=False,
                hovertemplate=(
                    "<b>Time</b>: %{x|%Y-%m-%d %H:%M:%S}<br>"
                    f"<b>{value_label}</b>: %{{y:{hover_num_format}}}<extra></extra>"
                ),
            ),
            row=row_idx,
            col=1,
        )

        tick_format = choose_plot_number_format(var_df["value_num"])
        axis_updates = dict(title_text=with_unit(variable.capitalize(), variable))
        if tick_format:
            axis_updates["tickformat"] = tick_format
        fig_hist.update_yaxes(row=row_idx, col=1, **axis_updates)

    fig_hist.update_xaxes(title_text="Timestamp (UTC)", row=len(available_variables), col=1)
    fig_hist.update_layout(
        template="plotly_white",
        height=max(450, 260 * len(available_variables)),
        margin=dict(l=20, r=20, t=50, b=20),
        hovermode="x unified",
        title=f"Historical raw data - {selected_sensor_label}",
    )
    st.plotly_chart(fig_hist, use_container_width=True)

    # Latest values table for export variables
    st.subheader("Latest values in selected period")
    latest_export_table = (
        export_df.sort_values("payload_time_utc")
        .groupby("variable", as_index=False)
        .tail(1)[["variable", "payload_time_utc", "value_num"]]
        .sort_values("variable")
        .reset_index(drop=True)
    )
    latest_export_table["value_display"] = latest_export_table.apply(
        lambda row: format_value_with_unit(row["value_num"], str(row["variable"])),
        axis=1,
    )
    latest_export_table = latest_export_table.rename(
        columns={
            "variable": "Variable",
            "payload_time_utc": "Last timestamp (UTC)",
            "value_display": "Latest value",
        }
    )
    st.dataframe(
        latest_export_table[["Variable", "Last timestamp (UTC)", "Latest value"]],
        use_container_width=True,
        hide_index=True,
    )

    # Raw data table
    st.subheader("Raw Data")
    with st.expander("View filtered historical raw data"):
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
            ]
            if col in display_df.columns
        ]
        st.dataframe(display_df[display_columns], use_container_width=True, hide_index=True)

    # Downloads
    st.subheader("Downloads")
    csv_bytes = export_df.to_csv(index=False).encode("utf-8")
    png_buffer = build_png_figure(
        export_df,
        selected_sensor_label,
        export_df["payload_time_utc"].min(),
        export_df["payload_time_utc"].max(),
    )

    dl_cols = st.columns(2)
    dl_cols[0].download_button(
        label="📥 Download historical CSV",
        data=csv_bytes,
        file_name=f"historical_export_{selected_sensor_ref}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )
    dl_cols[1].download_button(
        label="🖼️ Download historical PNG",
        data=png_buffer.getvalue(),
        file_name=f"historical_export_{selected_sensor_ref}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
        mime="image/png",
        use_container_width=True,
    )


# ============================================================
# AUTO REFRESH
# ============================================================
if auto_refresh:
    time.sleep(60)
    st.rerun()

