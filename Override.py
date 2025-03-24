import streamlit as st
import snowflake.connector
import pandas as pd

# Connect to Snowflake
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

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
