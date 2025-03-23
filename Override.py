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

st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Establish connection to Snowflake using Streamlit secrets
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
        return Session.builder.configs(connection_parameters).create()
    except Exception as e:
        st.error(f"❌ Failed to connect to Snowflake: {e}")
        st.stop()

session = connect_to_snowflake()

# Fetch data from any table
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.strip().upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

print([repr(col) for col in df.columns])

# Fetch Override_Ref data for the selected module
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

# Insert into target table
def insert_into_target_table(target_table, row_data, editable_column, new_value):
    try:
        src_insert_ts = row_data.get('AS_AT_DATE', None)
        if not src_insert_ts:
            st.error("AS_AT_DATE column not found in source data.")
            return

        columns = ", ".join(row_data.keys())
        values = ", ".join([f"'{str(v)}'" if isinstance(v, str) else str(v) for v in row_data.values()])
        
        # Insert into target table
        insert_sql = f"""
            INSERT INTO {target_table} ({columns}, {editable_column}_old, {editable_column}_new, src_insert_ts, insert_ts, record_flag)
            VALUES ({values}, '{row_data[editable_column]}', '{new_value}', '{src_insert_ts}', CURRENT_TIMESTAMP(), 'A')
        """
        session.sql(insert_sql).collect()
        st.success(f"Inserted into {target_table} successfully.")
    except Exception as e:
        st.error(f"Error inserting into {target_table}: {e}")

# Insert into source table
def insert_into_source_table(source_table, row_data, new_value, editable_column):
    try:
        row_data_copy = row_data.copy()
        row_data_copy[editable_column] = new_value
        row_data_copy['RECORD_FLAG'] = 'A'
        row_data_copy['INSERT_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        columns = ", ".join(row_data_copy.keys())
        values = ", ".join([f"'{str(v)}'" if isinstance(v, str) else str(v) for v in row_data_copy.values()])

        insert_sql = f"""
            INSERT INTO {source_table} ({columns})
            VALUES ({values})
        """
        session.sql(insert_sql).collect()
        st.success(f"Inserted new record into {source_table} with flag 'A'.")
    except Exception as e:
        st.error(f"Error inserting into {source_table}: {e}")

# Update source table record flag
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
        st.success("Updated old record with flag 'D'.")
    except Exception as e:
        st.error(f"Error updating {source_table}: {e}")

# Main App
def main():
    query_params = st.query_params
    module_number = query_params.get("module", None)

    module_tables_df = fetch_override_ref_data(module_number)
    if module_number and not module_tables_df.empty:
        module_name = module_tables_df['MODULE_NAME'].iloc[0]
        st.markdown(f"<div style='background-color: #E0F7FA; padding: 10px; border-radius: 5px; text-align: center; font-size: 16px;'><strong>Module:</strong> {module_name}</div>", unsafe_allow_html=True)
    else:
        st.info("Please select a module.")
        st.stop()

    available_tables = module_tables_df['SOURCE_TABLE'].unique()
    selected_table = st.selectbox("Select Table", available_tables)
    table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

    if not table_info_df.empty:
        target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
        editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0].upper()
        joining_keys = table_info_df['JOINING_KEYS'].iloc[0].upper().split(',')

        st.selectbox("Editable Column", [editable_column], disabled=True, key="editable_column_selectbox")
        st.markdown(f"**Editable Column:** {editable_column}")
        
        # Fetch data from source table
        source_df = fetch_data(selected_table)
        if not source_df.empty:
            source_df = source_df[source_df['RECORD_FLAG'] == 'A'].copy()
            edited_df = st.data_editor(source_df, num_rows="dynamic", use_container_width=True, disabled=[col for col in source_df.columns if col != editable_column])

            if st.button("Submit Updates"):
                try:
                    changed_rows = edited_df[edited_df[editable_column] != source_df[editable_column]]
                    if not changed_rows.empty:
                        for index, row in changed_rows.iterrows():
                            primary_key_values = {col: row[col] for col in joining_keys}
                            new_value = row[editable_column]
                            old_value = source_df.loc[index, editable_column]

                            # Step 1: Insert into target table
                            insert_into_target_table(target_table_name, row.to_dict(), editable_column, new_value)

                            # Step 2: Update the old record as 'D'
                            update_source_table_record_flag(selected_table, primary_key_values)

                            # Step 3: Insert the new record with flag 'A'
                            insert_into_source_table(selected_table, row.to_dict(), new_value, editable_column)
                        st.success("Data updated successfully!")
                    else:
                        st.info("No changes detected.")
                except Exception as e:
                    st.error(f"Error during update/insert: {e}")

            # Display Overridden Values
            st.subheader(f"Overridden Values from {target_table_name}")
            override_df = fetch_data(target_table_name)
            if not override_df.empty:
                st.dataframe(override_df, use_container_width=True)
            else:
                st.info(f"No overridden data available in {target_table_name}.")
    else:
        st.warning("No table information found in Override_Ref for the selected table.")

# Run the app
if __name__ == "__main__":
    main()
