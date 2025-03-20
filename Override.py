import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="centered"
)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Retrieve Snowflake credentials from Streamlit secrets
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

# Function to fetch data from any table
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Fetch data from Override_Ref
def fetch_override_ref_data(selected_module=None):
    try:
        df = session.table("Override_Ref").to_pandas()
        df.columns = [col.upper() for col in df.columns]
        if selected_module:
            df = df[df['MODULE'] == int(selected_module)]
        return df
    except Exception as e:
        st.error(f"Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Insert record into the source table with 'A' flag
def insert_into_source_table(source_table, row_data, new_value, editable_column):
    try:
        row_data_copy = row_data.copy()
        row_data_copy.pop(editable_column.upper(), None)
        row_data_copy.pop('RECORD_FLAG', None)
        row_data_copy.pop('INSERT_TS', None)

        columns = ", ".join(row_data_copy.keys())
        values = ", ".join([f"'{val}'" if isinstance(val, str) else str(val) for val in row_data_copy.values()])

        insert_sql = f"""
            INSERT INTO {source_table} ({columns}, {editable_column}, record_flag, insert_ts)
            VALUES ({values}, '{new_value}', 'A', CURRENT_TIMESTAMP())
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {source_table}: {e}")

# Update record flag as 'D'
def update_source_table_record_flag(source_table, primary_key_values):
    try:
        where_clause = " AND ".join([f"{col} = '{val}'" for col, val in primary_key_values.items()])
        update_sql = f"""
            UPDATE {source_table}
            SET record_flag = 'D',
                insert_ts = CURRENT_TIMESTAMP()
            WHERE {where_clause}
        """
        session.sql(update_sql).collect()
    except Exception as e:
        st.error(f"Error updating record flag in {source_table}: {e}")

# Insert into target table (override table)
def insert_into_override_table(target_table, row_data, old_value, new_value, editable_column):
    try:
        src_insert_ts = str(row_data['INSERT_TS'])
        row_data_copy = row_data.copy()
        row_data_copy.pop('RECORD_FLAG', None)
        row_data_copy.pop(editable_column.upper(), None)

        # Ensure columns for insert
        columns = ', '.join(row_data_copy.keys())
        values = ', '.join([f"'{val}'" if isinstance(val, str) else str(val) for val in row_data_copy.values()])

        insert_sql = f"""
            INSERT INTO {target_table} ({columns}, src_ins_ts, {editable_column}_old, {editable_column}_new, insert_ts, record_flag)
            VALUES ({values}, '{src_insert_ts}', {old_value}, {new_value}, CURRENT_TIMESTAMP(), 'A')
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {target_table}: {e}")

# Main app
def main():
    query_params = st.query_params
    module_number = query_params.get("module", None)
    module_tables_df = fetch_override_ref_data(module_number)

    if module_number and not module_tables_df.empty:
        module_name = module_tables_df['MODULE_NAME'].iloc[0]
        st.markdown(f"<div style='background-color: #E0F7FA; padding: 10px;'><strong>Module:</strong> {module_name}</div>", unsafe_allow_html=True)
    else:
        st.info("Please select a module from Power BI.")
        st.stop()

    available_tables = module_tables_df['SOURCE_TABLE'].unique()
    selected_table = st.selectbox("Select Table", available_tables)
    table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

    if not table_info_df.empty:
        target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
        editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0].upper()
        joining_keys = table_info_df['JOINING_KEYS'].iloc[0].split(',')

        st.selectbox("Editable Column", [editable_column], disabled=True)
        st.markdown(f"**Editable Column:** {editable_column}")

        tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

        with tab1:
            source_df = fetch_data(selected_table)
            source_df = source_df[source_df['RECORD_FLAG'] == 'A']

            if not source_df.empty:
                edited_df = source_df.copy()
                edited_df = edited_df.rename(columns={editable_column: f"{editable_column} ✏️"})
                
                # Disable non-editable columns
                disabled_cols = [col for col in edited_df.columns if col != f"{editable_column} ✏️"]

                edited_df = st.data_editor(
                    edited_df,
                    key="data_editor",
                    use_container_width=True,
                    disabled=disabled_cols
                )

                if st.button("Submit Updates"):
                    try:
                        changed_rows = edited_df[edited_df[f"{editable_column} ✏️"] != source_df[editable_column]]
                        if not changed_rows.empty:
                            for index, row in changed_rows.iterrows():
                                primary_key_values = {key: row[key.strip()] for key in joining_keys}
                                new_value = row[f"{editable_column} ✏️"]
                                old_value = source_df.loc[index, editable_column]

                                # 1. Insert into override table
                                insert_into_override_table(target_table_name, row, old_value, new_value, editable_column)

                                # 2. Insert the new record into the source table
                                insert_into_source_table(selected_table, source_df.loc[index].to_dict(), new_value, editable_column)

                                # 3. Update old record with flag 'D'
                                update_source_table_record_flag(selected_table, primary_key_values)

                            st.success("Data updated successfully!")
                        else:
                            st.info("No changes detected.")
                    except Exception as e:
                        st.error(f"Error during data processing: {e}")

        with tab2:
            override_df = fetch_data(target_table_name)
            st.dataframe(override_df if not override_df.empty else pd.DataFrame(), use_container_width=True)

if __name__ == "__main__":
    main()
