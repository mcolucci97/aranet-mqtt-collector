import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client, Client
from datetime import datetime, timedelta

# --- PAGE CONFIG ---
st.set_page_config(page_title="Aranet Cloud Dashboard", layout="wide")

# --- DATABASE CONNECTION ---
# These will be pulled from Streamlit Cloud "Secrets"
@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# --- DATA FETCHING ---
def get_sensors():
    """Fetch unique sensor references from the database"""
    response = supabase.table("measurements").select("sensor_ref").execute()
    df = pd.DataFrame(response.data)
    return df["sensor_ref"].unique() if not df.empty else []

def get_data(sensor_ref, variable, days):
    """Fetch filtered data from Supabase"""
    start_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
    
    response = supabase.table("measurements") \
        .select("*") \
        .eq("sensor_ref", sensor_ref) \
        .eq("variable", variable) \
        .gte("payload_time_utc", start_date) \
        .order("payload_time_utc", desc=True) \
        .execute()
    
    return pd.DataFrame(response.data)

# --- SIDEBAR ---
st.sidebar.title("🔬 Lab Settings")
st.sidebar.info("Data is pulled live from Supabase Cloud.")

sensor_list = get_sensors()
if len(sensor_list) > 0:
    selected_sensor = st.sidebar.selectbox("Select Sensor", sensor_list)
    
    # Get available variables for this sensor
    var_response = supabase.table("measurements") \
        .select("variable") \
        .eq("sensor_ref", selected_sensor) \
        .execute()
    vars_available = pd.DataFrame(var_response.data)["variable"].unique()
    
    selected_var = st.sidebar.selectbox("Variable", vars_available)
    days_to_show = st.sidebar.slider("Days of history", 1, 30, 7)
    
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
else:
    st.sidebar.warning("No data found in Supabase yet.")
    st.stop()

# --- MAIN DASHBOARD ---
st.title(f"Real-time Monitoring: {selected_var}")

df = get_data(selected_sensor, selected_var, days_to_show)

if not df.empty:
    # Convert time to datetime objects
    df["payload_time_utc"] = pd.to_datetime(df["payload_time_utc"])
    
    # KPI Metrics
    latest_val = df.iloc[0]["value_num"]
    st.metric(label=f"Current {selected_var}", value=f"{latest_val:.2f}")

    # Plotly Chart
    fig = px.line(df, x="payload_time_utc", y="value_num", 
                  title=f"{selected_var} History ({selected_sensor})",
                  labels={"payload_time_utc": "Time (UTC)", "value_num": selected_var},
                  template="plotly_dark")
    
    st.plotly_chart(fig, use_container_width=True)

    # Data Table & Export
    with st.expander("View Raw Data Table"):
        st.dataframe(df, use_container_width=True)
    
    st.download_button(
        label="📥 Download as CSV",
        data=df.to_csv(index=False),
        file_name=f"lab_data_{selected_var}.csv",
        mime="text/csv"
    )
else:
    st.warning("No data available for the selected filters.")
