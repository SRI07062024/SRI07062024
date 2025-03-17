import streamlit as st
import pandas as pd
from snowflake.snowpark import Session  
from datetime import datetime

# ✅ Set Streamlit page configuration
st.set_page_config(
    page_title="Override Dashboard",
    page_icon="📊",
    layout="wide"
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

# ✅ Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/150", use_column_width=True)  # Placeholder for logo
    st.markdown("### 📌 Navigation")
    st.write("Use the dropdowns to explore and edit data.")

# ✅ Snowflake Connection
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
    st.sidebar.success("✅ Connected to Snowflake!")

except Exception as e:
    st.sidebar.error(f"❌ Failed to connect: {e}")
    st.stop()

# ✅ Fetch Module Name
def fetch_module_name(module_num):
    df = session.table("Override_Ref").to_pandas()
    df.columns = [col.upper() for col in df.columns]
    module_info = df[df['MODULE'] == module_num]['MODULE_NAME'].unique()
    return module_info[0] if len(module_info) > 0 else "Unknown Module"

# ✅ Read Module Number from Power BI URL
query_params = st.query_params
module_from_url = query_params.get("module", None)

# ✅ Convert to integer and fetch module name
if module_from_url:
    try:
        module_num = int(module_from_url)
        module_name = fetch_module_name(module_num)
    except ValueError:
        module_name = "Invalid Module"
else:
    module_name = "No Module Selected"

# ✅ Display Module Name
st.markdown(f"""
    <h2 style='text-align: center; color: black; text-decoration: underline;'>
        {module_name}
    </h2>
""", unsafe_allow_html=True)

# ✅ Fetch Override Ref Data
def fetch_override_ref_data(module_num):
    df = session.table("Override_Ref").to_pandas()
    df.columns = [col.upper() for col in df.columns]
    return df[df['MODULE'] == module_num] if not df.empty else pd.DataFrame()

module_tables_df = fetch_override_ref_data(module_num) if module_from_url else pd.DataFrame()
available_tables = module_tables_df['SOURCE_TABLE'].unique() if not module_tables_df.empty else []

# ✅ Select Table Dropdown
selected_table = st.selectbox("Select Table", available_tables)

# ✅ Get Target Table & Editable Column
table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table] if not module_tables_df.empty else pd.DataFrame()

if not table_info_df.empty:
    target_table_name = table_info_df['TARGET_TABLE'].iloc[0].upper()
    editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0].upper()

    # ✅ Display Data Tabs
    tab1, tab2 = st.tabs(["📌 Source Data", "📝 Overridden Values"])

    with tab1:
        st.subheader(f"🔍 Source Data from {selected_table}")
        source_df = session.table(selected_table).to_pandas()

        if not source_df.empty:
            # ✅ Ensure Editable Column Exists
            if editable_column not in source_df.columns:
                st.error(f"❌ Editable column '{editable_column}' not found in {selected_table}.")
            else:
                # ✅ Apply Highlight Style to Editable Column
                def highlight_editable_column(val):
                    return ["background-color: #FFF3CD" if col == editable_column else "" for col in val.index]

                # ✅ Set Column Type Based on Data
                if pd.api.types.is_numeric_dtype(source_df[editable_column]):
                    column_type = st.column_config.NumberColumn
                else:
                    column_type = st.column_config.TextColumn
                
                column_config = {
                    editable_column: column_type(
                        "✏️ " + editable_column,  
                        help="This column is editable.",
                        required=True,
                    )
                }

                # ✅ Display Editable Data
                edited_df = st.data_editor(
                    source_df.style.apply(highlight_editable_column, axis=1),
                    column_config=column_config,
                    disabled=[col for col in source_df.columns if col != editable_column], 
                    num_rows="dynamic",
                    use_container_width=True
                )

                # ✅ Submit Button with Animation
                if st.button("🚀 Submit Updates", type="primary"):
                    edited_rows = edited_df[source_df[editable_column] != edited_df[editable_column]]
                    if not edited_rows.empty:
                        edited_rows['AS_AT_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        edited_rows['RECORD_FLAG'] = 'O'

                        session.write_pandas(edited_rows, target_table_name, overwrite=False)
                        st.success("✅ Edited values inserted as new rows successfully!")
                    else:
                        st.info("ℹ️ No changes detected.")

        else:
            st.info(f"ℹ️ No data available in {selected_table}.")

    with tab2:
        st.subheader(f"📝 Overridden Values from {target_table_name}")
        overridden_df = session.table(target_table_name).to_pandas()
        overridden_df = overridden_df[overridden_df['RECORD_FLAG'] == 'O'] if not overridden_df.empty else pd.DataFrame()
        if not overridden_df.empty:
            st.dataframe(overridden_df, use_container_width=True)
        else:
            st.info(f"ℹ️ No overridden values in {target_table_name}.")

else:
    st.error(f"❌ No target table configured for {selected_table} in Override_Ref.")

# ✅ Footer
st.markdown("---")
st.caption("Portfolio Performance Override System • Last updated: March 12, 2025")
