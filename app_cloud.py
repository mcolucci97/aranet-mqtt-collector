import streamlit as st
from supabase import create_client

st.title("Supabase connection test")

try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    st.success("Secrets loaded")
except Exception as e:
    st.error(f"Secrets error: {e}")
    st.stop()

try:
    sb = create_client(url, key)
    res = sb.table("measurements").select("*").limit(5).execute()
    st.success("Supabase connection OK")
    st.write(res.data)
except Exception as e:
    st.error(f"Supabase query error: {e}")
