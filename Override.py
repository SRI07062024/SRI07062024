import streamlit as st
import snowflake.connector
import pandas as pd

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

# Fetch override_ref table data
def get_override_ref_data(conn):
    query = "SELECT Source_table, Target_table, Editable_column, Joining_Keys FROM Override_Ref WHERE Is_active = 'Y'"
    return pd.read_sql(query, conn)

# Initialize connection and get data
conn = get_snowflake_connection()
override_data = get_override_ref_data(conn)

# Display fetched configuration data
st.write("Configuration from Override_Ref:")
st.dataframe(override_data)
