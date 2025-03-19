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
        .stApp { background-color: #f8f9fa; }
        .stButton>button { background-color: #1E88E5; color: white; font-size: 16px; padding: 10px; }
        .stTabs [role="tab"] { font-size: 18px; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# ✅ Title
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# ✅ Snowflake connection
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
    st.success("✅ Connected to Snowflake!")
except Exception as e:
    st.error(f"❌ Snowflake connection failed: {e}")
    st.stop()

# ✅ Fetch module information
def fetch_module_name(module_num):
    df = session.table("OVERRIDE_REF").to_pandas()
    df.columns = [col.upper() for col in df.columns]
    module_info = df[df['MODULE'] == module_num]['MODULE_NAME'].unique()
    return module_info[0] if len(module_info) > 0 else "Unknown Module"

# ✅ Read module number from URL
query_params = st.query_params
module_from_url = query_params.get("module", None)

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

st.markdown(f"<h2 style='text-align: center; color: black; text-decoration: underline;'>{module_name}</h2>", unsafe_allow_html=True)

# ✅ Fetch override ref data
def fetch_override_ref_data(module_num):
    df = session.table("OVERRIDE_REF").to_pandas()
    df.columns = [col.upper() for col in df.columns]
    return df[df['MODULE'] == module_num] if not df.empty else pd.DataFrame()

module_tables_df = fetch_override_ref_data(module_num) if selected_module else pd.DataFrame()
available_tables = module_tables_df['SOURCE_TABLE'].unique() if not module_tables_df.empty else []

# ✅ Select table dropdown
selected_table = st.selectbox("Select Table", available_tables)

# ✅ Get target table and editable column
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
            if editable_column not in source_df.columns:
                st.error(f"❌ Editable column '{editable_column}' not found in {selected_table}.")
            else:
                cols = st.columns([max(1, len(col)) for col in source_df.columns])
                filter_values = {}

                for i, col in enumerate(source_df.columns):
                    if col != editable_column:
                        filter_values[col] = cols[i].text_input("", key=f"filter_{col}")
                    else:
                        cols[i].markdown("")  

                for col, value in filter_values.items():
                    if value:
                        source_df = source_df[source_df[col].astype(str).str.contains(value, case=False, na=False)]

                # ✅ Column Configuration
                if pd.api.types.is_numeric_dtype(source_df[editable_column]):
                    column_type = st.column_config.NumberColumn
                else:
                    column_type = st.column_config.TextColumn

                column_config = {
                    editable_column: column_type(
                        "✏️ " + editable_column,
                        help="Editable column.",
                        required=True,
                    )
                }

                # ✅ Data Editor
                edited_df = st.data_editor(
                    source_df,
                    column_config=column_config,
                    disabled=[col for col in source_df.columns if col != editable_column],
                    num_rows="dynamic",
                    use_container_width=True
                )

                # ✅ Submit Button
                if st.button("🚀 Submit Updates", type="primary"):
                    edited_rows = source_df[source_df[editable_column] != edited_df[editable_column]].copy()

                    if not edited_rows.empty:
                        # Prepare dynamic column updates
                        edited_rows[f"{editable_column}_OLD"] = source_df[editable_column]  
                        edited_rows[f"{editable_column}_NEW"] = edited_df[editable_column]  
                        edited_rows.drop(columns=[editable_column], inplace=True)

                        # Add metadata columns
                        edited_rows['AS_AT_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        edited_rows['RECORD_FLAG'] = 'O'

                        # Insert into target table
                        session.write_pandas(edited_rows, target_table_name, overwrite=False)

                        # ✅ Update source table
                        for _, row in edited_rows.iterrows():
                            condition = " AND ".join([f"{col} = '{row[col]}'" for col in source_df.columns if col != editable_column])
                            update_source_sql = f"""
                                UPDATE {selected_table}
                                SET RECORD_FLAG = 'D'
                                WHERE {condition} AND RECORD_FLAG <> 'D';
                                
                                INSERT INTO {selected_table} ({", ".join(source_df.columns)})
                                SELECT {", ".join([f"'{row[col]}'" if isinstance(row[col], str) else row[col] for col in source_df.columns])}
                                , 'A';
                            """
                            session.sql(update_source_sql).collect()

                        st.success("✅ Changes saved successfully!")
                    else:
                        st.info("ℹ️ No changes detected.")

    with tab2:
        st.subheader(f"📝 Overridden Values from {target_table_name}")
        overridden_df = session.table(target_table_name).to_pandas()
        overridden_df = overridden_df[overridden_df['RECORD_FLAG'] == 'O'] if not overridden_df.empty else pd.DataFrame()
        
        if not overridden_df.empty:
            st.dataframe(overridden_df, use_container_width=True)
        else:
            st.info(f"ℹ️ No overridden values with RECORD_FLAG = 'O' in {target_table_name}.")

else:
    st.error(f"❌ No target table configured for {selected_table} in OVERRIDE_REF.")

# ✅ Footer
st.markdown("---")
st.caption("Portfolio Performance Override System • Last updated: March 13, 2025")
