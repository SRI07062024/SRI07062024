import streamlit as st
import pandas as pd
import snowflake.connector

# Connect to Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user='YOUR_USER',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT',
        warehouse='YOUR_WAREHOUSE',
        database='YOUR_DATABASE',
        schema='YOUR_SCHEMA'
    )

# Fetch tables and columns from Override_Ref
def fetch_override_details(conn):
    query = """
    SELECT Source_table, Target_table, Editable_Column, Joining_Keys
    FROM Override_Ref
    WHERE Is_active = 'Y'
    """
    return pd.read_sql(query, conn)

# Display source table data
def fetch_source_data(conn, table_name):
    query = f"SELECT * FROM {table_name} WHERE record_flag != 'D'"
    return pd.read_sql(query, conn)

# Update data
def perform_override(conn, source_table, target_table, editable_column, joining_keys, data_before, data_after):
    for index, row in data_after.iterrows():
        old_value = data_before.loc[index, editable_column]
        new_value = row[editable_column]
        if old_value != new_value:
            # Insert into target table
            insert_query = f"""
            INSERT INTO {target_table} 
            SELECT *, insert_ts AS src_insert_ts, {old_value} AS {editable_column}_Old, {new_value} AS {editable_column}_New, 'O', CURRENT_TIMESTAMP()
            FROM {source_table}
            WHERE {' AND '.join([f'{k} = {row[k]}' for k in joining_keys.split(',')])}
            """
            conn.cursor().execute(insert_query)
            
            # Update source table (Set old record to 'D', insert new record with 'A')
            update_query = f"""
            UPDATE {source_table}
            SET record_flag = 'D'
            WHERE {' AND '.join([f'{k} = {row[k]}' for k in joining_keys.split(',')])};
            
            INSERT INTO {source_table} 
            SELECT *, '{new_value}', 'A', CURRENT_TIMESTAMP()
            FROM {source_table}
            WHERE {' AND '.join([f'{k} = {row[k]}' for k in joining_keys.split(',')])};
            """
            conn.cursor().execute(update_query)

# Streamlit UI
st.title("Dynamic Data Override App")

conn = get_connection()
override_details = fetch_override_details(conn)

if not override_details.empty:
    for _, row in override_details.iterrows():
        st.subheader(f"Source Table: {row['Source_table']}")
        source_data = fetch_source_data(conn, row['Source_table'])
        editable_data = source_data.copy()
        st.write("Editable Data:")
        edited_data = st.data_editor(editable_data, disabled=[col for col in source_data.columns if col != row['Editable_Column']])
        
        if st.button(f"Submit Changes for {row['Source_table']}"):
            perform_override(conn, row['Source_table'], row['Target_table'], row['Editable_Column'], row['Joining_Keys'], source_data, edited_data)
            st.success(f"Changes submitted for {row['Source_table']}")
else:
    st.warning("No active tables found in Override_Ref.")
