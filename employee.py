import streamlit as st
import pandas as pd
from snowflake.snowpark import Session  
from datetime import datetime

# ✅ Snowflake connection parameters from Streamlit secrets
try:
    connection_parameters = {
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        "database": st.secrets["SNOWFLAKE_DATABASE"],
        "schema": st.secrets["SNOWFLAKE_SCHEMA"],
    }

    # ✅ Create a Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    st.success("✅ Successfully connected to Snowflake!")

except Exception as e:
    st.error(f"❌ Failed to connect to Snowflake: {e}")
    st.stop()

# Table name
table_name = "EMPLOYEE"

# Fetch columns from the Snowflake table
def get_table_columns(table_name):
    try:
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        df_columns = session.sql(query).to_pandas()
        return df_columns['COLUMN_NAME'].tolist()
    except Exception as e:
        st.error(f"Error fetching columns: {e}")
        return []

# Get table columns dynamically
columns = get_table_columns(table_name)

if not columns:
    st.stop()  # Stop execution if no columns are fetched

# Create an empty DataFrame with the fetched columns
empty_df = pd.DataFrame(columns=columns)

st.title('Employee Management System')
st.write(f'Add employee details to the `{table_name}` table')

# Display editable empty table
edited_df = st.data_editor(empty_df, num_rows='dynamic')

# Submit new data
if st.button('Submit'):
    if edited_df.empty:
        st.warning('No data entered. Please add employee details.')
    else:
        try:
            # Insert records into Snowflake using write_pandas
             insert_into_table(edited_df, table_name, mode='append')
           # session.write_pandas(edited_df, table_name, mode='append')
            st.success('Records inserted successfully!')
        except Exception as e:
            st.error(f"Error inserting records: {e}")
