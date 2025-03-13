import streamlit as st
import pandas as pd
from snowflake.snowpark import Session  # ✅ Correct import
from datetime import datetime

# Snowflake connection parameters from Streamlit secrets
connection_parameters = {
    "account": st.secrets["SNOWFLAKE_ACCOUNT"],
    "user": st.secrets["SNOWFLAKE_USER"],
    "password": st.secrets["SNOWFLAKE_PASSWORD"],
    "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
    "database": st.secrets["SNOWFLAKE_DATABASE"],
    "schema": st.secrets["SNOWFLAKE_SCHEMA"],
}

# Create a Snowpark session
session = Session.builder.configs(connection_parameters).create()

st.write("✅ Successfully connected to Snowflake!")

# Page configuration
st.set_page_config(
    page_title="Editable Portfolio Performance Data",
    page_icon="📊",
    layout="wide"
)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Editable Portfolio Performance Data</h1>", unsafe_allow_html=True)

# Get active Snowflake session
session = get_active_session()
if session is None:
    st.error("Unable to establish a Snowflake session. Please ensure you are running this app within a Snowflake environment.")
    st.stop()

# Function to fetch data based on the table name
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch override ref data based on the selected module
def fetch_override_ref_data(selected_module=None, selected_table=None):
    try:
        df = session.table("Override_Ref").to_pandas()
        df.columns = [col.upper() for col in df.columns]

        # Filter based on the selected module if provided
        if selected_module:
            module_num = int(selected_module.split('-')[1])
            df = df[df['MODULE'] == module_num]
            
            # Additionally filter by source table if provided
            if selected_table:
                df = df[df['SOURCE_TABLE'] == selected_table]
                
        return df
    except Exception as e:
        st.error(f"Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Function to update a row in the source table
def update_source_table_row(source_table, as_of_date, portfolio, portfolio_segment, category, description, market_value):
    try:
        # Construct the UPDATE statement
        update_sql = f"""
            UPDATE {source_table}
            SET
                MARKET_VALUE = '{market_value}',
                DESCRIPTION = '{description}'
            WHERE
                AS_OF_DATE = '{as_of_date}' AND
                PORTFOLIO = '{portfolio}' AND
                PORTFOLIO_SEGMENT = '{portfolio_segment}' AND
                CATEGORY = '{category}'
        """

        # Execute the UPDATE statement
        session.sql(update_sql).collect()

        st.success(f"Successfully updated row in {source_table} for As_of_date={as_of_date}, Portfolio={portfolio}")

    except Exception as e:
        st.error(f"Error updating row in {source_table}: {e}")

# Function to insert a row in the target table with RECORD_FLAG = 'O'
def insert_into_target_table(target_table, as_of_date, portfolio, portfolio_segment, category, description, market_value):
    try:
        # Format the current timestamp as a string
        formatted_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        insert_sql = f"""
            INSERT INTO {target_table} (AS_OF_DATE, PORTFOLIO, PORTFOLIO_SEGMENT, CATEGORY, DESCRIPTION, MARKET_VALUE, AS_AT_DATE, RECORD_FLAG)
            VALUES ('{as_of_date}', '{portfolio}', '{portfolio_segment}', '{category}', '{description}', '{market_value}', '{formatted_ts}', 'O')
        """

        session.sql(insert_sql).collect()
        st.success(f"Successfully inserted data to table {target_table} with RECORD_FLAG = 'O'")
    except Exception as e:
        st.error(f"Error inserting values into {target_table}: {e}")

# Function to update and insert data
def update_and_insert(source_table, target_table_name, edited_df, original_df):
    try:
        # Identify rows that have been edited
        edited_rows = edited_df[(edited_df != original_df).any(axis=1)]

        # Iterate through each edited row
        for index, row in edited_rows.iterrows():
            # Extract the values
            as_of_date = row["AS_OF_DATE"].strftime("%Y-%m-%d")
            portfolio = row["PORTFOLIO"]
            portfolio_segment = row["PORTFOLIO_SEGMENT"]
            category = row["CATEGORY"]
            description = row["DESCRIPTION"]
            market_value = row["MARKET_VALUE"]

            # Update source table with the values
            update_source_table_row(source_table, as_of_date, portfolio, portfolio_segment, category, description, market_value)

            # Insert a new row into the target table with RECORD_FLAG = 'O'
            insert_into_target_table(target_table_name, as_of_date, portfolio, portfolio_segment, category, description, market_value)

        # After all updates, fetch and display the source data
        source_df = fetch_data(source_table)
        st.dataframe(source_df, use_container_width=True)

        # Display success message
        st.success("Updated the data successfully!")
    except Exception as e:
        st.error(f"Error updating and inserting data: {e}")

# Main app
# List available modules - Dynamically populate from Override_Ref
override_ref_df = fetch_data("Override_Ref")
if not override_ref_df.empty:
    available_modules = [f"Module-{int(module)}" for module in override_ref_df['MODULE'].unique()]
else:
    available_modules = []
    st.warning("No modules found in Override_Ref table.")

# Select module
selected_module = st.selectbox("Select Module", available_modules)

# Get tables for the selected module
module_tables_df = fetch_override_ref_data(selected_module)
if not module_tables_df.empty:
    available_tables = module_tables_df['SOURCE_TABLE'].unique()
    
    # Select table within the module
    selected_table = st.selectbox("Select Table", available_tables)
    
    # Get target table for selected source table
    table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]
    if not table_info_df.empty:
        target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
        
        # Split the data into two tabs
        tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

        with tab1:
            st.subheader(f"Source Data from {selected_table}")

            # Fetch data at the beginning
            source_df = fetch_data(selected_table)
            if not source_df.empty:
                # Make the dataframe editable
                edited_df = st.data_editor(source_df, num_rows="dynamic", use_container_width=True)

                # Submit button to update the source table and insert to the target table
                if st.button("Submit Updates", type="primary"):
                    update_and_insert(selected_table, target_table_name, edited_df, source_df)
            else:
                st.info(f"No data available in {selected_table}.")

        with tab2:
            st.subheader(f"Overridden Values from {target_table_name}")

            # Fetch overridden data (ONLY the latest value)
            overridden_df = fetch_data(target_table_name)

            # Filter by RECORD_FLAG
            overridden_df = overridden_df[overridden_df['RECORD_FLAG'] == 'O']

            if not overridden_df.empty:
                st.dataframe(overridden_df, use_container_width=True)
            else:
                st.info(f"No overridden values with RECORD_FLAG = 'O' available in {target_table_name}.")
    else:
        st.error(f"No target table configured for source table {selected_table} in Override_Ref.")
else:
    st.error(f"No tables configured for module {selected_module} in Override_Ref.")

# Footer
st.markdown("---")
st.caption("Portfolio Performance Override System • Last updated: March 12, 2025")
