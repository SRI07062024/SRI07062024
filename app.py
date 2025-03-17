import streamlit as st
import pandas as pd
from snowflake.snowpark import Session  
from datetime import datetime

# ✅ Set Streamlit page configuration
st.set_page_config(
    page_title="Override Dashboard",
    page_icon="📊",
    layout="centered"
)

# ✅ Apply Global Styling
st.markdown("""
    <style>
        .stApp { background-color: #f8f9fa; } /* Light grey background */
        .stButton>button { background-color: #1E88E5; color: white; font-size: 16px; padding: 10px; }
        .stTabs [role="tab"] { font-size: 18px; font-weight: bold; }
        .css-1cpxqw2 { font-size: 16px; }
    </style>
""", unsafe_allow_html=True)

# ✅ Title
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

# ✅ Fetch Module Name from Override_Ref
def fetch_module_name(module_num):
    df = session.table("Override_Ref").to_pandas()
    df.columns = [col.upper() for col in df.columns]
    module_info = df[df['MODULE'] == module_num]['MODULE_NAME'].unique()
    return module_info[0] if len(module_info) > 0 else "Unknown Module"

# ✅ Read module number from Power BI URL
query_params = st.query_params
module_from_url = query_params.get("module", None)

# ✅ Convert to integer and fetch module name
if module_from_url:
    try:
        module_num = int(module_from_url)
        module_name = fetch_module_name(module_num)
        selected_module = f"Module-{module_num}"
    except ValueError:
        module_name = "Invalid Module"
        selected_module = None
else:
    module_name = "No Module Selected"
    selected_module = None

# ✅ Display Module Name as a heading above the Select Table dropdown
#st.markdown(f"### {module_name}", unsafe_allow_html=True)
st.markdown(f"<h2 style='text-align: center; color: black; text-decoration: underline;'>{module_name}</h2>", unsafe_allow_html=True)

# ✅ Fetch override ref data for the selected module
def fetch_override_ref_data(module_num):
    df = session.table("Override_Ref").to_pandas()
    df.columns = [col.upper() for col in df.columns]
    return df[df['MODULE'] == module_num] if not df.empty else pd.DataFrame()

# Fetch data only if a valid module is selected
module_tables_df = fetch_override_ref_data(module_num) if selected_module else pd.DataFrame()
available_tables = module_tables_df['SOURCE_TABLE'].unique() if not module_tables_df.empty else []

# ✅ Select Table Dropdown (with Module Name above)
selected_table = st.selectbox("Select Table", available_tables)

# ✅ Get target table and editable column
table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table] if not module_tables_df.empty else pd.DataFrame()

if not table_info_df.empty:
    target_table_name = table_info_df['TARGET_TABLE'].iloc[0].upper()
    editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0].upper()  # Editable column from Override_Ref

    # ✅ Display Data Tabs
    tab1, tab2 = st.tabs(["📌 Source Data", "📝 Overridden Values"])

    with tab1:
    st.subheader(f"🔍 Source Data from {selected_table}")
    source_df = session.table(selected_table).to_pandas()

    if not source_df.empty:
        if editable_column not in source_df.columns:
            st.error(f"❌ Editable column '{editable_column}' not found in {selected_table}.")
        else:
            # ✅ Create Filter Inputs Above Each Column (Except Editable Column)
            filter_values = {}
            cols_to_filter = [col for col in source_df.columns if col != editable_column]
            
            col1, col2 = st.columns([3, 1])  # Adjust width if needed

            with col1:
                filter_container = st.container()
                
                with filter_container:
                    filter_inputs = {}
                    for col in cols_to_filter:
                        filter_inputs[col] = st.text_input(f"", placeholder=f"🔎 Filter {col}", key=f"filter_{col}")

            # ✅ Apply Filters Dynamically
            filtered_df = source_df.copy()
            for col, value in filter_inputs.items():
                if value:
                    filtered_df = filtered_df[filtered_df[col].astype(str).str.contains(value, case=False, na=False)]

            # ✅ Column Config (Editable Column)
            column_type = (
                st.column_config.NumberColumn if pd.api.types.is_numeric_dtype(source_df[editable_column])
                else st.column_config.TextColumn
            )
            
            column_config = {
                editable_column: column_type(
                    "✏️ " + editable_column,
                    help="This column is editable.",
                    required=True,
                )
            }
            
            # ✅ Editable Data Table with Applied Filters
            edited_df = st.data_editor(
                filtered_df,
                column_config=column_config,
                disabled=[col for col in filtered_df.columns if col != editable_column],
                num_rows="dynamic",
                use_container_width=True
            )

            # ✅ Submit Button
            if st.button("🚀 Submit Updates", type="primary"):
                edited_rows = edited_df[source_df[editable_column] != edited_df[editable_column]]
                if not edited_rows.empty:
                    edited_rows['AS_AT_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    edited_rows['RECORD_FLAG'] = 'O'

                    # Insert edited rows into target table
                    session.write_pandas(edited_rows, target_table_name, overwrite=False)
                    st.success("✅ Edited values inserted as new rows successfully!")
                else:
                    st.info("ℹ️ No changes detected.")

    else:
        st.info(f"ℹ️ No data available in {selected_table}.")

    with tab2:
        st.subheader(f"📝Overridden Values from {target_table_name}")
        overridden_df = session.table(target_table_name).to_pandas()
        overridden_df = overridden_df[overridden_df['RECORD_FLAG'] == 'O'] if not overridden_df.empty else pd.DataFrame()
        if not overridden_df.empty:
            st.dataframe(overridden_df, use_container_width=True)
        else:
            st.info(f"ℹ️ No overridden values with RECORD_FLAG = 'O' in {target_table_name}.")

else:
    st.error(f"❌ No target table configured for {selected_table} in Override_Ref.")

# ✅ Footer
st.markdown("---")
st.caption("Portfolio Performance Override System • Last updated: March 12, 2025")
