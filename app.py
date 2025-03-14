import streamlit as st
 import pandas as pd
 from snowflake.snowpark import Session  
 from datetime import datetime
 
 # ✅ Ensure `st.set_page_config()` is the first Streamlit command
 st.set_page_config(
     page_title="Override Dashboard",
     page_icon="📊",
     layout="centered"
 )
 
 # Title with custom styling
 st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)
 
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
 
 # Function to fetch modules from Override_Ref
 def fetch_modules():
     df = session.table("Override_Ref").to_pandas()
     df.columns = [col.upper() for col in df.columns]
     return [f"Module-{int(module)}" for module in df['MODULE'].unique()] if not df.empty else []
 
 # Fetch available modules
 available_modules = fetch_modules()
 
 # Read module number from URL parameters
 query_params = st.query_params
 module_from_url = query_params.get("module", None)  # Get "module" from URL
 
 # Set default module based on URL
 default_module = f"Module-{module_from_url}" if module_from_url and f"Module-{module_from_url}" in available_modules else None
 
 # ✅ Module Selection Logic
 st.write("### Selected Module")
 if default_module:
     # If module is from URL, display it as a **disabled text input**
     st.text_input("Module", default_module, disabled=True)
     selected_module = default_module
 else:
     # If no module is in the URL, use the normal dropdown
     selected_module = st.selectbox("Select Module", available_modules)
 
 # Function to fetch override ref data
 def fetch_override_ref_data(selected_module):
     df = session.table("Override_Ref").to_pandas()
     df.columns = [col.upper() for col in df.columns]
     module_num = int(selected_module.split('-')[1])
     return df[df['MODULE'] == module_num] if not df.empty else pd.DataFrame()
 
 # Fetch tables for the selected module
 module_tables_df = fetch_override_ref_data(selected_module)
 available_tables = module_tables_df['SOURCE_TABLE'].unique() if not module_tables_df.empty else []
 selected_table = st.selectbox("Select Table", available_tables)
 
 # Check if a target table exists
 table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table] if not module_tables_df.empty else pd.DataFrame()
 if not table_info_df.empty:
     target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
 
     # Split the data into two tabs
     tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])
 
     with tab1:
         st.subheader(f"Source Data from {selected_table}")
         source_df = session.table(selected_table).to_pandas()
         if not source_df.empty:
             edited_df = st.data_editor(source_df, num_rows="dynamic", use_container_width=True)
             if st.button("Submit Updates", type="primary"):
                 st.success("✅ Updated the data successfully!")
         else:
             st.info(f"ℹ️ No data available in {selected_table}.")
 
     with tab2:
         st.subheader(f"Overridden Values from {target_table_name}")
         overridden_df = session.table(target_table_name).to_pandas()
         overridden_df = overridden_df[overridden_df['RECORD_FLAG'] == 'O'] if not overridden_df.empty else pd.DataFrame()
         if not overridden_df.empty:
             st.dataframe(overridden_df, use_container_width=True)
         else:
             st.info(f"ℹ️ No overridden values with RECORD_FLAG = 'O' in {target_table_name}.")
 else:
     st.error(f"❌ No target table configured for {selected_table} in Override_Ref.")
 
 # Footer
 st.markdown("---")
 st.caption("Portfolio Performance Override System • Last updated: March 12, 2025")
