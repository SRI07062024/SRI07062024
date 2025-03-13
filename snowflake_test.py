import streamlit as st
from snowflake.snowpark import Session

st.title("🔗 Snowflake Connection Test")

# Get credentials from Streamlit secrets
connection_parameters = {
    "account": st.secrets["SNOWFLAKE_ACCOUNT"],
    "user": st.secrets["SNOWFLAKE_USER"],
    "password": st.secrets["SNOWFLAKE_PASSWORD"],
    "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
    "database": st.secrets["SNOWFLAKE_DATABASE"],
    "schema": st.secrets["SNOWFLAKE_SCHEMA"],
}

# Test connection
try:
    session = Session.builder.configs(connection_parameters).create()
    st.success("✅ Successfully connected to Snowflake!")
    
    # Run a test query
    query_result = session.sql("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
    
    # Display result
    st.write("### Connection Details:")
    for row in query_result:
        st.write(f"👤 User: {row[0]}")
        st.write(f"🏢 Account: {row[1]}")
        st.write(f"📂 Database: {row[2]}")
        st.write(f"📑 Schema: {row[3]}")
    
    session.close()

except Exception as e:
    st.error("❌ Connection failed! Please check your credentials.")
    st.text(str(e))
