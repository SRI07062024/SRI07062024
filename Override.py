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

# Connect to Snowflake
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
    st.success("✅ Successfully connected to Snowflake!")
except Exception as e:
    st.error(f"❌ Failed to connect to Snowflake: {e}")
    st.stop()

# Functions
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

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

def insert_into_source_table(source_table, row_data, new_value, editable_column):
    try:
        row_data_copy = row_data.copy()

        # Remove unnecessary columns
        for col in ['RECORD_FLAG', 'INSERT_TS', editable_column.upper()]:
            if col in row_data_copy:
                del row_data_copy[col]

        columns = ", ".join(row_data_copy.keys())
        values = ", ".join([f"'{val}'" if isinstance(val, str) else str(val) for val in row_data_copy.values()])

        insert_sql = f"""
            INSERT INTO {source_table} ({columns}, {editable_column}, record_flag, insert_ts)
            VALUES ({values}, '{new_value}', 'A', CURRENT_TIMESTAMP())
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {source_table}: {e}")

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

def insert_into_override_table(target_table, row_data, old_value, new_value, editable_column):
    try:
        src_ins_ts = str(row_data['INSERT_TS'])

        insert_sql = f"""
            INSERT INTO {target_table} (asofdate, segment, category, src_ins_ts, amount_old, amount_new, insert_ts, record_flag)
            VALUES ('{row_data['AS_OF_DATE']}', '{row_data['SEGMENT']}', '{row_data['CATEGORY']}', 
                    '{src_ins_ts}', {old_value}, {new_value}, CURRENT_TIMESTAMP(), 'A')
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {target_table}: {e}")

# Main app
def main():
    query_params = st.query_params
    module_number = query_params.get("module", None)
    module_tables_df = fetch_override_ref_data(module_number)

    # Display Module
    if module_number and not module_tables_df.empty:
        module_name = module_tables_df['MODULE_NAME'].iloc[0]
        st.markdown(f"""
            <div style="background-color: #E0F7FA; padding: 10px; border-radius: 5px; text-align: center; font-size: 16px;">
                <strong>Module:</strong> {module_name}
            </div>
        """, unsafe_allow_html=True)
    else:
        st.info("Please select a module from Power BI.")
        st.stop()

    if not module_tables_df.empty:
        available_tables = module_tables_df['SOURCE_TABLE'].unique()
        selected_table = st.selectbox("Select Table", available_tables)

        table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

        if not table_info_df.empty:
            target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
            editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0]
            joining_keys = table_info_df['JOINING_KEYS'].iloc[0].split(',')

            editable_column_upper = editable_column.upper()
            st.selectbox("Editable Column", [editable_column], disabled=True)

            # Tabs for source and target data
            tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

            with tab1:
                source_df = fetch_data(selected_table)
                source_df = source_df[source_df['RECORD_FLAG'] == 'A']

                if not source_df.empty:
                    edited_df = source_df.copy()
                    edited_df = edited_df.rename(columns={editable_column_upper: f"{editable_column_upper} ✏️"})

                    # Disable all columns except the editable one
                    disabled_cols = [col for col in edited_df.columns if col != f"{editable_column_upper} ✏️"]

                    edited_df = st.data_editor(
                        edited_df,
                        key=f"data_editor_{selected_table}_{editable_column}",
                        use_container_width=True,
                        disabled=disabled_cols
                    )

                    if st.button("Submit Updates"):
                        try:
                            changed_rows = edited_df[edited_df[f"{editable_column_upper} ✏️"] != source_df[editable_column_upper]]
                            
                            if not changed_rows.empty:
                                for index, row in changed_rows.iterrows():
                                    primary_key_values = {key: row[key.strip()] for key in joining_keys}
                                    new_value = row[f"{editable_column_upper} ✏️"]
                                    old_value = source_df.loc[index, editable_column_upper]

                                    # Perform operations
                                    insert_into_override_table(target_table_name, row, old_value, new_value, editable_column)
                                    insert_into_source_table(selected_table, source_df.loc[index].to_dict(), new_value, editable_column)
                                    update_source_table_record_flag(selected_table, primary_key_values)

                                st.success("Data updated successfully!")
                            else:
                                st.info("No changes detected.")
                        except Exception as e:
                            st.error(f"Error during data processing: {e}")

            with tab2:
                override_df = fetch_data(target_table_name)
                if not override_df.empty:
                    st.dataframe(override_df, use_container_width=True)
                else:
                    st.info(f"No overridden data available in {target_table_name}.")
    else:
        st.warning("No tables found for the selected module in Override_Ref table.")

if __name__ == "__main__":
    main()
