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

# ✅ Fetch source table data dynamically
def fetch_source_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]  # Normalize column names
        return df
    except Exception as e:
        st.error(f"❌ Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# ✅ Display Source Data
if selected_source_table:
    source_df = fetch_source_data(selected_source_table)

    if not source_df.empty:
        st.subheader(f"🔍 Source Data from {selected_source_table}")

        # ✅ Create input fields for column-wise filtering
        cols = st.columns([max(1, len(col)) for col in source_df.columns])  
        filter_values = {}

        for i, col in enumerate(source_df.columns):
            if col != editable_column:  # Exclude editable column from filtering
                filter_values[col] = cols[i].text_input(f"{col}", key=f"filter_{col}")
            else:
                cols[i].markdown("")  # Keep space aligned, but no search box

        # ✅ Apply filters dynamically
        for col, value in filter_values.items():
            if value:
                source_df = source_df[source_df[col].astype(str).str.contains(value, case=False, na=False)]

        # ✅ Column Configuration for Editable Field
        if pd.api.types.is_numeric_dtype(source_df[editable_column]):
            column_type = st.column_config.NumberColumn
        else:
            column_type = st.column_config.TextColumn
        
        column_config = {
            editable_column: column_type(
                "✏️ " + editable_column,  
                help="This column is editable.",
                required=True,
            )
        }

        # ✅ Show data with only one column editable
        edited_df = st.data_editor(
            source_df,
            column_config=column_config,
            disabled=[col for col in source_df.columns if col != editable_column], 
            num_rows="dynamic",
            use_container_width=True
        )

    else:
        st.info(f"ℹ️ No data available in {selected_source_table}.")

