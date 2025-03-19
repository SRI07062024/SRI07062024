import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

st.set_page_config(page_title="Override Dashboard", page_icon="📊", layout="centered")

st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Snowflake Connection
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
    st.success("✅ Successfully connected to Snowflake!")
except Exception as e:
    st.error(f"❌ Failed to connect to Snowflake: {e}")
    st.stop()

# Fetch override reference data
def fetch_override_ref_data():
    try:
        df = session.table("OVERRIDE_REF").to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching override reference data: {e}")
        return pd.DataFrame()

# Fetch source data
def fetch_source_data(source_table):
    try:
        df = session.table(source_table).filter("RECORD_FLAG = 'A'").to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching source data from {source_table}: {e}")
        return pd.DataFrame()

# Insert into target table
def insert_into_target_table(target_table, joining_key_values, editable_column, old_value, new_value, src_insert_ts):
    try:
        columns = ", ".join(joining_key_values.keys()) + ", SRC_INSERT_TS, " + editable_column + "_OLD, " + editable_column + "_NEW, RECORD_FLAG, INSERT_TS"
        values = ", ".join(f"'{v}'" for v in joining_key_values.values()) + f", '{src_insert_ts}', '{old_value}', '{new_value}', 'A', CURRENT_TIMESTAMP()"

        insert_sql = f"""
            INSERT INTO {target_table} ({columns})
            VALUES ({values})
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {target_table}: {e}")

# Insert new record into source table
def insert_into_source_table(source_table, row_data, new_value, editable_column):
    try:
        row_data_copy = row_data.copy()
        row_data_copy[editable_column] = new_value  # Update editable column
        row_data_copy['RECORD_FLAG'] = 'A'  # Set new record as active
        row_data_copy['INSERT_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # New timestamp

        columns = ", ".join(row_data_copy.keys())
        values = ", ".join(f"'{v}'" for v in row_data_copy.values())

        insert_sql = f"""
            INSERT INTO {source_table} ({columns})
            VALUES ({values})
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {source_table}: {e}")

# Update old record in source table
def update_source_table_record_flag(source_table, joining_key_values):
    try:
        where_clause = " AND ".join([f"{col} = '{val}'" for col, val in joining_key_values.items()])
        update_sql = f"""
            UPDATE {source_table}
            SET RECORD_FLAG = 'D'
            WHERE {where_clause}
        """
        session.sql(update_sql).collect()
    except Exception as e:
        st.error(f"Error updating record flag in {source_table}: {e}")

# Main application logic
def main():
    override_ref_df = fetch_override_ref_data()

    if override_ref_df.empty:
        st.error("No override reference data found.")
        st.stop()

    # User selects module
    available_modules = override_ref_df['MODULE'].unique()
    selected_module = st.selectbox("Select Module", available_modules)

    # Filter data for selected module
    module_data = override_ref_df[override_ref_df['MODULE'] == selected_module]

    # User selects source table
    available_tables = module_data['SOURCE_TABLE'].unique()
    selected_table = st.selectbox("Select Table", available_tables)

    table_info = module_data[module_data['SOURCE_TABLE'] == selected_table]
    target_table = table_info['TARGET_TABLE'].iloc[0]
    editable_column = table_info['EDITABLE_COLUMN'].iloc[0].upper()
    
    # Handling multiple joining keys
    joining_keys_raw = table_info['JOINING_KEYS'].iloc[0]  # Joining keys are comma-separated
    joining_keys = [key.strip().upper() for key in joining_keys_raw.split(",")]  # Convert to list

    # Fetch source data
    source_df = fetch_source_data(selected_table)

    if source_df.empty:
        st.info(f"No active records in {selected_table}.")
        st.stop()

    # Make only the editable column editable
    edited_df = source_df.copy()
    for col in edited_df.columns:
        if col != editable_column:
            edited_df[col] = edited_df[col].astype(str)  # Convert to string to make them non-editable

    edited_df = st.data_editor(edited_df[[editable_column]], key="data_editor", num_rows="dynamic", use_container_width=True)

    if st.button("Submit Updates"):
        changed_rows = edited_df[edited_df[editable_column] != source_df[editable_column]]

        if not changed_rows.empty:
            for _, row in changed_rows.iterrows():
                # Get joining key values
                joining_key_values = {key: source_df.loc[row.name, key] for key in joining_keys}

                # Get old and new values
                old_value = source_df.loc[row.name, editable_column]
                new_value = row[editable_column]

                # Get original insert timestamp
                src_insert_ts = str(source_df.loc[row.name, 'INSERT_TS'])

                # 1. Insert into target table
                insert_into_target_table(target_table, joining_key_values, editable_column, old_value, new_value, src_insert_ts)

                # 2. Insert new record into source table
                insert_into_source_table(selected_table, source_df.loc[row.name].to_dict(), new_value, editable_column)

                # 3. Update old record in source table
                update_source_table_record_flag(selected_table, joining_key_values)

            st.success("Data updated successfully!")
        else:
            st.info("No changes detected.")

# Run app
if __name__ == "__main__":
    main()
