import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

# Connect to Snowflake
def connect_to_snowflake():
    try:
        connection_parameters = {
            "account": st.secrets["SNOWFLAKE_ACCOUNT"],
            "user": st.secrets["SNOWFLAKE_USER"],
            "password": st.secrets["SNOWFLAKE_PASSWORD"],
            "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
            "database": st.secrets["SNOWFLAKE_DATABASE"],
            "schema": st.secrets["SNOWFLAKE_SCHEMA"],
        }
        session = Session.builder.configs(connection_parameters).create()
        st.success("✅ Connected to Snowflake")
        return session
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.stop()

session = connect_to_snowflake()

# Retrieve Configuration Data from Override_Ref
def fetch_override_ref_data(module_number):
    try:
        df = session.sql(f"SELECT * FROM override_ref WHERE module = {module_number}").to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching Override_Ref data: {e}")
        return pd.DataFrame()

# Example - Assuming module number is passed via query parameters
query_params = st.query_params
module_number = query_params.get("module", 1)
override_ref_df = fetch_override_ref_data(module_number)

if override_ref_df.empty:
    st.warning("No configuration data found in Override_Ref.")
    st.stop()
else:
    st.write("Configuration Retrieved:", override_ref_df)

# Function to fetch data from a given table
def fetch_data(table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        df = session.sql(query).to_pandas()
        # Convert column names to uppercase for consistency
        df.columns = [col.strip().upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Extract source and target table names, editable column, and join keys
source_table = override_ref_df['SOURCE_TABLE'].iloc[0]
target_table = override_ref_df['TARGET_TABLE'].iloc[0]
editable_column = override_ref_df['EDITABLE_COLUMN'].iloc[0].strip().upper()
join_keys = override_ref_df['JOINING_KEYS'].iloc[0].strip().upper().split(',')

st.write(f"📊 **Source Table:** {source_table}")
st.write(f"📥 **Target Table:** {target_table}")
st.write(f"🖋️ **Editable Column:** {editable_column}")
st.write(f"🔑 **Joining Keys:** {join_keys}")

# Fetch and display source data
source_df = fetch_data(source_table)

if source_df.empty:
    st.warning("No data found in the source table.")
    st.stop()

# Step 2: Display Source Data using st.data_editor

# Ensure editable column exists in the source data
if editable_column not in source_df.columns:
    st.error(f"Editable column '{editable_column}' not found in source table.")
    st.stop()

# Create a copy of the source data for editing
editable_df = source_df.copy()

# Highlight the editable column and make it editable
st.write("🖋️ **Editable Data**")
edited_data = st.data_editor(
    editable_df,
    column_config={
        editable_column: st.column_config.NumberColumn(f"{editable_column} (Editable)")
    },
    disabled=[col for col in editable_df.columns if col != editable_column],
    use_container_width=True
)

st.write("✅ Review your changes and click 'Submit' when ready.")
submit_button = st.button("Submit Changes")

if submit_button:
    st.write("🔎 Changes submitted. Proceeding with updates...")

