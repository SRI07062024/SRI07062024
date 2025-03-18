import streamlit as st
import pandas as pd
from snowflake.snowpark import Session  

# ✅ Set Streamlit page config
st.set_page_config(page_title="Dynamic Override System", page_icon="⚡", layout="centered")

# ✅ Snowflake connection using secrets
try:
    session = Session.builder.configs({
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        "database": st.secrets["SNOWFLAKE_DATABASE"],
        "schema": st.secrets["SNOWFLAKE_SCHEMA"],
    }).create()
    st.success("✅ Connected to Snowflake")
except Exception as e:
    st.error(f"❌ Connection failed: {e}")
    st.stop()

# ✅ Fetch table metadata from OVERRIDE_REF
def fetch_override_metadata():
    df = session.table("OVERRIDE_REF").to_pandas()
    df.columns = [col.upper() for col in df.columns]  # Normalize column names
    return df

# Fetch metadata
metadata_df = fetch_override_metadata()

# ✅ Select Source Table
if not metadata_df.empty:
    selected_source_table = st.selectbox("Select Source Table", metadata_df['SOURCE_TABLE'].unique())

    # Get target table and editable column for the selected source table
    table_info = metadata_df[metadata_df['SOURCE_TABLE'] == selected_source_table].iloc[0]
    target_table = table_info['TARGET_TABLE']
    editable_column = table_info['EDITABLE_COLUMN']

    st.write(f"🔹 **Target Table:** {target_table}")
    st.write(f"✏️ **Editable Column:** {editable_column}")

else:
    st.warning("⚠️ No metadata found in OVERRIDE_REF.")
