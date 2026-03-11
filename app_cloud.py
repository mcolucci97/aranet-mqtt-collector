import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from supabase import create_client


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Lab Sensor Dashboard",
    page_icon="🔬",
    layout="wide",
)

st.title("🔬 Lab Environmental Monitoring")
st.caption("Real-time and historical data from Aranet Base Station")


# ============================================================
# SECRETS
# ============================================================
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception as e:
    st.error(f"Missing Streamlit secrets: {e}")
    st.stop()


# ============================================================
# UNITS CONFIGURATION
# Update this dictionary as needed.
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


def safe_dataframe(df: pd.DataFrame):
    st.dataframe(df, use_container_width=True)


def fetch_all(query_builder, page_size: int = 1000):
    """
    Fetch all rows from Supabase using pagination.
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

    if "payload_time_utc" in df.columns:
        df["payload_time_utc"] = pd.to_datetime(
            df["payload_time_utc"], errors="coerce", utc=True
        )

    if "received_at_utc" in df.columns:
        df["received_at_utc"] = pd.to_datetime(
            df["received_at_utc"], errors="coerce", utc=True
        )

    if "value_num" in df.columns:
        df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")

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

select_all_sensors = st.sidebar.checkbox("Select all detectors", value=False)
default_sensor_selection = all_sensor_refs if select_all_sensors else all_sensor_refs[:1]

selected_refs = st.sidebar.multiselect(
    "Select Sensors",
    options=all_sensor_refs,
    default=default_sensor_selection,
    format_func=lambda x: sensor_options[x],
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

select_all_variables = st.sidebar.checkbox("Select all variables", value=False)
default_variable_selection = all_variables if select_all_variables else all_variables[: min(3, len(all_variables))]

selected_variables = st.sidebar.multiselect(
    "Select Measurements",
    options=all_variables,
    default=default_variable_selection,
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
except Exception as e:
    st.error(f"Unable to load time series data from Supabase: {e}")
    st.stop()

if data_df.empty:
    st.warning("No data found for the selected filters.")
    st.stop()

data_df["sensor_label"] = data_df.apply(safe_sensor_name, axis=1)


# ============================================================
# DEBUG / SANITY CHECK
# ============================================================
with st.expander("Debug / data sanity check"):
    st.write("Loaded columns:")
    st.write(list(data_df.columns))

    st.write("First rows:")
    safe_dataframe(data_df.head(10))

    if "value_num" in data_df.columns:
        st.write("value_num summary:")
        st.write(data_df["value_num"].describe())

    if {"variable", "value_num", "value_text"}.issubset(data_df.columns):
        st.write("Sample variable/value pairs:")
        sample_debug = data_df[["variable", "value_num", "value_text"]].head(20)
        safe_dataframe(sample_debug)


# ============================================================
# OVERVIEW
# ============================================================
st.subheader("Overview")

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
        height=550,
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis_title="Timestamp (UTC)",
        yaxis_title=y_title,
        hovermode="x unified",
        legend_title="Sensor",
    )

    if tick_format:
        layout_kwargs["yaxis"] = dict(tickformat=tick_format)

    fig_overlay.update_layout(**layout_kwargs)
    st.plotly_chart(fig_overlay, use_container_width=True)


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
    ncols = min(4, len(snapshot_df))
    metric_cols = st.columns(ncols)

    for idx, (_, row) in enumerate(snapshot_df.iterrows()):
        col = metric_cols[idx % ncols]
        col.metric(
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

    metrics_cols = st.columns(5)
    metrics_cols[0].metric("Sensors with data", latest_var_df["sensor_ref"].nunique())
    metrics_cols[1].metric("Average", format_value_with_unit(avg_val, variable))
    metrics_cols[2].metric("Minimum", format_value_with_unit(min_val, variable))
    metrics_cols[3].metric("Maximum", format_value_with_unit(max_val, variable))
    metrics_cols[4].metric("Points", len(var_df))

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


# ============================================================
# LATEST VALUES TABLE
# ============================================================
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
safe_dataframe(latest_table)


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

    safe_dataframe(display_df[display_columns])

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
