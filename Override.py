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

# Highlight the editable column
def highlight_editable_column(dataframe, editable_col):
    styles = pd.DataFrame('', index=dataframe.index, columns=dataframe.columns)
    if editable_col in dataframe.columns:
        styles[editable_col] = 'background-color: #FFF8DC'  # Light cream color for editable column
    return styles

# Display the data using st.data_editor with only the editable column enabled
disabled_cols = [col for col in source_df.columns if col != editable_column]

st.write("🔎 **Source Data**")
styled_df = source_df.style.apply(highlight_editable_column, editable_col=editable_column, axis=None)

edited_df = st.data_editor(
    styled_df,
    key="source_data_editor",
    use_container_width=True,
    num_rows="dynamic",
    disabled=disabled_cols
)

# Function to insert records into the target table
def insert_into_target_table(target_table, row_data, editable_column, old_value, new_value):
    try:
        src_insert_ts = row_data['AS_AT_DATE']

        insert_sql = f"""
            INSERT INTO {target_table} (AS_AT_DATE, SRC_INSERT_TS, {editable_column}_OLD, {editable_column}_NEW, RECORD_FLAG, INSERT_TS)
            VALUES ('{src_insert_ts}', '{src_insert_ts}', '{old_value}', '{new_value}', 'A', CURRENT_TIMESTAMP())
        """
        session.sql(insert_sql).collect()
        st.success(f"✅ Record inserted into {target_table}")
    except Exception as e:
        st.error(f"❌ Error inserting into {target_table}: {e}")

# Compare original and edited data to identify changes
def identify_and_insert_changes():
    try:
        # Remove the highlighting for comparison
        source_df_clean = source_df.copy()
        edited_df_clean = edited_df.copy()

        # Rename the editable column to remove the pencil icon (if added earlier)
        edited_df_clean.columns = [col.replace(" ✏️", "") for col in edited_df_clean.columns]

        # Find changes using the editable column
        changed_rows = edited_df_clean[edited_df_clean[editable_column] != source_df_clean[editable_column]]

        if changed_rows.empty:
            st.info("No changes detected.")
            return

        for _, row in changed_rows.iterrows():
            old_value = source_df_clean.loc[_, editable_column]
            new_value = row[editable_column]

            # Perform the insert into the target table
            insert_into_target_table(target_table, row, editable_column, old_value, new_value)
        
        st.success("All changes inserted into the target table.")
    except Exception as e:
        st.error(f"Error during update/insert: {e}")

# Add a Submit button for updates
if st.button("Submit Updates"):
    identify_and_insert_changes()


