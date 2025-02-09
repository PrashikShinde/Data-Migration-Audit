import oracledb
import csv
import os
import datetime
import subprocess
import time
import itertools
from notify_on_completion import send_telegram_notification, BOT_TOKEN, CHAT_IDS


# oracle_client_path = r'D:\Users\T000670\Downloads\instantclient-basic-windows.x64-23.6.0.24.10\instantclient_23_6'
# os.environ['ORACLE_HOME'] = oracle_client_path
# os.environ['TNS_ADMIN'] = oracle_client_path
# os.environ['PATH'] = oracle_client_path + ";" + os.environ['PATH']
# oracledb.init_oracle_client()

###############################################################################
# Prompt for User Inputs
###############################################################################

def prompt_user_for_info():
    print("Please enter the following details for the OLD Database:")
    old_db_user = input("  Old DB Username: ").strip()
    old_db_password = input("  Old DB Password: ").strip()
    # old_db_user = "PRASHIK_EXTAUDIT"
    # old_db_password = "Unity@123"

    old_db_dsn = input("  Old DB DSN (e.g. host:port/service_name): ").strip()
    old_schema_name = input("  Old Schema Name: ").strip()

    print("\nPlease enter the following details for the NEW Database:")
    new_db_user = input("  New DB Username: ").strip()
    new_db_password = input("  New DB Password: ").strip()
    # new_db_user = "PRASHIK_EXTAUDIT"
    # new_db_password = "Unity@123"
    new_db_dsn = input("  New DB DSN (e.g. host:port/service_name): ").strip()
    new_schema_name = input("  New Schema Name: ").strip()

    print("\nSpecify a chunk size for any full data comparisons (number of rows per chunk).")
    chunk_size_str = input("  Chunk Size (e.g. 10000): ").strip()
    chunk_size = int(chunk_size_str) if chunk_size_str.isdigit() else 10000

    return {
        "old_db_config": {
            "user": old_db_user,
            "password": old_db_password,
            "dsn": old_db_dsn,
            "schema": old_schema_name
        },
        "new_db_config": {
            "user": new_db_user,
            "password": new_db_password,
            "dsn": new_db_dsn,
            "schema": new_schema_name
        },
        "chunk_size": chunk_size
    }

###############################################################################
# Database Connection Helpers
###############################################################################

def get_oracle_connection(db_config):
    try:
        connection = oracledb.connect(
            user=db_config["user"],
            password=db_config["password"],
            dsn=db_config["dsn"]
        )
        return connection
    except oracledb.DatabaseError as e:
        print(f"[ERROR] Connection failed: {e}")
        raise

def close_connection(connection):
    try:
        if connection:
            connection.close()
    except Exception as e:
        print(f"[ERROR] Closing connection: {e}")

###############################################################################
# Helper Functions for Tables, Schemas, PK
###############################################################################

def get_table_list(connection, schema_name):
    query = """
        SELECT table_name 
        FROM all_tables 
        WHERE owner = UPPER(:schema_param)
        ORDER BY table_name
    """
    cursor = connection.cursor()
    cursor.execute(query, schema_param=schema_name)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_table_schema(connection, schema_name, table_name):
    """
    Retrieves a dictionary of column_name -> (data_type, data_length) for the given table.
    Uses Oracle's ALL_TAB_COLUMNS view.
    """
    query = """
        SELECT column_name, data_type, data_length
        FROM all_tab_columns
        WHERE owner = UPPER(:schema_param)
          AND table_name = UPPER(:table_param)
        ORDER BY column_id
    """
    cursor = connection.cursor()
    cursor.execute(query, schema_param=schema_name, table_param=table_name)
    schema = {}
    for row in cursor.fetchall():
        col_name, data_type, data_length = row
        # Store (data_type, data_length) if needed, e.g., {"CUSTOMER_ID": ("NUMBER", 22)}
        schema[col_name] = (data_type, data_length)
    cursor.close()
    return schema

def get_primary_key_columns(connection, schema_name, table_name):
    """
    Returns a list of primary key column names for the given table (Oracle).
    If the table has no primary key, returns an empty list.
    """
    query = """
        SELECT acc.column_name
        FROM all_constraints ac
        JOIN all_cons_columns acc 
             ON ac.owner = acc.owner
            AND ac.constraint_name = acc.constraint_name
        WHERE ac.owner = UPPER(:schema_param)
          AND ac.table_name = UPPER(:table_param)
          AND ac.constraint_type = 'P'
        ORDER BY acc.position
    """
    cursor = connection.cursor()
    cursor.execute(query, schema_param=schema_name, table_param=table_name)
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns

###############################################################################
# Original get_table_data and other helper functions
###############################################################################

def get_table_data(connection, schema_name, table_name):
    """
    Original helper function (unchanged).
    Fetches all rows (SELECT *), returns (columns, rows).
    """
    query = f"SELECT * FROM {schema_name}.{table_name}"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return columns, rows
    except oracledb.DatabaseError as e:
        print(f"[ERROR] Unable to fetch data for {schema_name}.{table_name}: {e}")
        return [], []
    finally:
        cursor.close()


def save_results_in_batches(results, output_dir, prefix, batch_size=100):
    """
    Saves results in multiple CSV files, each containing a batch of up to `batch_size` tables.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    batch_count = (len(results) // batch_size) + (1 if len(results) % batch_size else 0)

    for i in range(batch_count):
        batch_results = results[i * batch_size: (i + 1) * batch_size]
        output_file = os.path.join(output_dir, f"{prefix}_batch_{i + 1}.csv")

        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=batch_results[0].keys())
            writer.writeheader()
            writer.writerows(batch_results)

        print(f"[INFO] Results saved to {output_file}")

###############################################################################
# Count Validation (SELECT COUNT(*))
###############################################################################

def count_validation(old_conn, new_conn, old_schema, new_schema, results_dir):
    """
    Validates table existence, row counts, and total cell counts
    (rows * columns) between the old and new databases.
    Discrepancies and a detailed comparison are saved to a CSV file.
    """
    count_validation_csv = os.path.join(results_dir, "count_validation.csv")
    discrepancies = []
    detailed_comparison = []

    old_tables = get_table_list(old_conn, old_schema)
    new_tables = get_table_list(new_conn, new_schema)

    # Identify missing and extra tables
    missing_tables = set(old_tables) - set(new_tables)
    extra_tables = set(new_tables) - set(old_tables)

    for table in missing_tables:
        discrepancies.append({
            "Type": "Missing Table",
            "Table": table,
            "Old Row Count": "",
            "New Row Count": "",
            "Old Column Count": "",
            "New Column Count": "",
            "Old Total Values": "",
            "New Total Values": "",
            "Details": "Table is missing in the new database."
        })

    for table in extra_tables:
        discrepancies.append({
            "Type": "Extra Table",
            "Table": table,
            "Old Row Count": "",
            "New Row Count": "",
            "Old Column Count": "",
            "New Column Count": "",
            "Old Total Values": "",
            "New Total Values": "",
            "Details": "Table is extra in the new database."
        })

    # Compare row counts and total cell counts for common tables
    common_tables = set(old_tables).intersection(new_tables)
    for table in common_tables:
        try:
            # Get row count from old DB
            old_cursor = old_conn.cursor()
            old_cursor.execute(f"SELECT COUNT(*) FROM {old_schema}.{table}")
            old_row_count = old_cursor.fetchone()[0]
            old_cursor.close()

            # Get row count from new DB
            new_cursor = new_conn.cursor()
            new_cursor.execute(f"SELECT COUNT(*) FROM {new_schema}.{table}")
            new_row_count = new_cursor.fetchone()[0]
            new_cursor.close()

            # Get column count from old DB schema
            old_table_def = get_table_schema(old_conn, old_schema, table)
            old_col_count = len(old_table_def)

            # Get column count from new DB schema
            new_table_def = get_table_schema(new_conn, new_schema, table)
            new_col_count = len(new_table_def)

            # Compute total cell count (row_count * col_count)
            old_total_values = old_row_count * old_col_count
            new_total_values = new_row_count * new_col_count

            # Check row count mismatch
            if old_row_count != new_row_count:
                discrepancies.append({
                    "Type": "Row Count Mismatch",
                    "Table": table,
                    "Old Row Count": old_row_count,
                    "New Row Count": new_row_count,
                    "Old Column Count": old_col_count,
                    "New Column Count": new_col_count,
                    "Old Total Values": old_total_values,
                    "New Total Values": new_total_values,
                    "Details": f"Row counts do not match: Old={old_row_count}, New={new_row_count}"
                })

            # Check total cell-count mismatch
            if old_total_values != new_total_values:
                discrepancies.append({
                    "Type": "Total Value Count Mismatch",
                    "Table": table,
                    "Old Row Count": old_row_count,
                    "New Row Count": new_row_count,
                    "Old Column Count": old_col_count,
                    "New Column Count": new_col_count,
                    "Old Total Values": old_total_values,
                    "New Total Values": new_total_values,
                    "Details": (
                        f"Mismatch in total values (rows*columns): "
                        f"Old={old_total_values}, New={new_total_values}"
                    )
                })

            # Add to detailed comparison for each table
            detailed_comparison.append({
                "Table": table,
                "Old Row Count": old_row_count,
                "New Row Count": new_row_count,
                "Old Column Count": old_col_count,
                "New Column Count": new_col_count,
                "Old Total Values": old_total_values,
                "New Total Values": new_total_values,
                "Details": "OK"
            })

        except oracledb.DatabaseError as e:
            discrepancies.append({
                "Type": "Database Error",
                "Table": table,
                "Old Row Count": "N/A",
                "New Row Count": "N/A",
                "Old Column Count": "N/A",
                "New Column Count": "N/A",
                "Old Total Values": "",
                "New Total Values": "",
                "Details": str(e)
            })

    # Write CSV
    with open(count_validation_csv, "w", newline="") as f:
        fieldnames = [
            "Type", "Table",
            "Old Row Count", "New Row Count",
            "Old Column Count", "New Column Count",
            "Old Total Values", "New Total Values",
            "Details"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Discrepancies first
        if discrepancies:
            writer.writerows(discrepancies)
        else:
            writer.writerow({
                "Type": "No discrepancies noted",
                "Table": "",
                "Old Row Count": "",
                "New Row Count": "",
                "Old Column Count": "",
                "New Column Count": "",
                "Old Total Values": "",
                "New Total Values": "",
                "Details": ""
            })

        writer.writerow({})
        writer.writerow({})

        # Detailed comparison
        writer.writerow({
            "Type": "Detailed Comparison Below",
            "Table": "",
            "Old Row Count": "",
            "New Row Count": "",
            "Old Column Count": "",
            "New Column Count": "",
            "Old Total Values": "",
            "New Total Values": "",
            "Details": ""
        })
        writer.writerow({})
        for row in detailed_comparison:
            row_out = {
                "Type": "Detailed Comparison",
                "Table": row["Table"],
                "Old Row Count": row["Old Row Count"],
                "New Row Count": row["New Row Count"],
                "Old Column Count": row["Old Column Count"],
                "New Column Count": row["New Column Count"],
                "Old Total Values": row["Old Total Values"],
                "New Total Values": row["New Total Values"],
                "Details": row["Details"]
            }
            writer.writerow(row_out)

    print(f"[INFO] Count validation saved to {count_validation_csv}")


###############################################################################
# Schema Validation
###############################################################################

def schema_validation(old_conn, new_conn, old_schema, new_schema, results_dir):
    schema_validation_csv = os.path.join(results_dir, "schema_validation.csv")
    discrepancies = []
    detailed_comparison = []

    old_tables = get_table_list(old_conn, old_schema)
    new_tables = get_table_list(new_conn, new_schema)
    common_tables = set(old_tables).intersection(new_tables)

    for table in common_tables:
        old_schema_def = get_table_schema(old_conn, old_schema, table)
        new_schema_def = get_table_schema(new_conn, new_schema, table)

        old_cols = set(old_schema_def.keys())
        new_cols = set(new_schema_def.keys())

        missing_cols = old_cols - new_cols
        extra_cols = new_cols - old_cols

        # Missing
        for col in missing_cols:
            dt, ln = old_schema_def.get(col, ("Unknown", ""))
            discrepancies.append({
                "Type": "Missing Column",
                "Table": table,
                "Column": col,
                "Old Data Type": dt,
                "Old Length": ln,
                "New Data Type": "",
                "New Length": "",
                "Details": f"Column '{col}' is missing in new DB."
            })

        # Extra
        for col in extra_cols:
            dt, ln = new_schema_def.get(col, ("Unknown", ""))
            discrepancies.append({
                "Type": "Extra Column",
                "Table": table,
                "Column": col,
                "Old Data Type": "",
                "Old Length": "",
                "New Data Type": dt,
                "New Length": ln,
                "Details": f"Column '{col}' is extra in new DB."
            })

        # Intersection: Check data type mismatch
        intersect_cols = old_cols.intersection(new_cols)
        for col in intersect_cols:
            if old_schema_def[col] != new_schema_def[col]:
                discrepancies.append({
                    "Type": "Data Type Mismatch",
                    "Table": table,
                    "Column": col,
                    "Old Data Type": old_schema_def[col][0],
                    "Old Length": old_schema_def[col][1],
                    "New Data Type": new_schema_def[col][0],
                    "New Length": new_schema_def[col][1],
                    "Details": f"Column '{col}' type differs."
                })

        # Detailed
        union_cols = old_cols.union(new_cols)
        for col in union_cols:
            old_dt, old_ln = old_schema_def.get(col, ("Missing", "N/A"))
            new_dt, new_ln = new_schema_def.get(col, ("Missing", "N/A"))
            status = "Match"
            if (col in missing_cols or col in extra_cols or
                (old_dt, old_ln) != (new_dt, new_ln)):
                status = "Discrepancy"

            detailed_comparison.append({
                "Type": "Detailed Comparison",
                "Table": table,
                "Column": col,
                "Old Data Type": old_dt,
                "Old Length": old_ln,
                "New Data Type": new_dt,
                "New Length": new_ln,
                "Details": status
            })

    with open(schema_validation_csv, "w", newline="") as f:
        fieldnames = [
            "Type", "Table", "Column",
            "Old Data Type", "Old Length",
            "New Data Type", "New Length",
            "Details"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        if discrepancies:
            writer.writerows(discrepancies)
        else:
            writer.writerow({
                "Type": "No discrepancies noted",
                "Table": "",
                "Column": "",
                "Old Data Type": "",
                "Old Length": "",
                "New Data Type": "",
                "New Length": "",
                "Details": ""
            })

        writer.writerow({})
        writer.writerow({})

        writer.writerow({
            "Type": "Detailed Comparison Below",
            "Table": "",
            "Column": "",
            "Old Data Type": "",
            "Old Length": "",
            "New Data Type": "",
            "New Length": "",
            "Details": ""
        })
        writer.writerow({})
        writer.writerows(detailed_comparison)

    print(f"[INFO] Schema validation saved to {schema_validation_csv}")

###############################################################################
# Aggregate Function Validation
###############################################################################

def aggregate_function_validation(old_conn, new_conn, old_schema, new_schema, tables, results_dir):
    aggregate_csv = os.path.join(results_dir, "aggregate_function_validation.csv")
    discrepancies = []
    detailed_comparison = []

    for table in tables:
        print(f"[INFO] Performing aggregate function validation for table '{table}'...")

        old_table_schema = get_table_schema(old_conn, old_schema, table)
        new_table_schema = get_table_schema(new_conn, new_schema, table)

        numerical_columns = [
            c for c, (dt, _) in old_table_schema.items()
            if dt in ("NUMBER", "FLOAT", "DECIMAL")
               and c in new_table_schema
               and new_table_schema[c][0] in ("NUMBER", "FLOAT", "DECIMAL")
        ]

        for col in numerical_columns:
            try:
                old_cursor = old_conn.cursor()
                old_cursor.execute(f"SELECT SUM({col}), AVG({col}) FROM {old_schema}.{table}")
                old_sum, old_avg = old_cursor.fetchone()
                old_cursor.close()

                new_cursor = new_conn.cursor()
                new_cursor.execute(f"SELECT SUM({col}), AVG({col}) FROM {new_schema}.{table}")
                new_sum, new_avg = new_cursor.fetchone()
                new_cursor.close()

                if old_sum != new_sum or old_avg != new_avg:
                    discrepancies.append({
                        "Type": "Aggregate Mismatch",
                        "Table": table,
                        "Column": col,
                        "Old SUM": old_sum,
                        "New SUM": new_sum,
                        "Old AVG": old_avg,
                        "New AVG": new_avg,
                        "Details": (
                            f"Mismatch: Old SUM={old_sum}, New SUM={new_sum}, "
                            f"Old AVG={old_avg}, New AVG={new_avg}"
                        )
                    })

                detailed_comparison.append({
                    "Type": "Detailed Comparison",
                    "Table": table,
                    "Column": col,
                    "Old SUM": old_sum,
                    "New SUM": new_sum,
                    "Old AVG": old_avg,
                    "New AVG": new_avg,
                    "Details": "Match" if (old_sum == new_sum and old_avg == new_avg)
                               else "Mismatch"
                })

            except Exception as e:
                discrepancies.append({
                    "Type": "Error",
                    "Table": table,
                    "Column": col,
                    "Old SUM": "",
                    "New SUM": "",
                    "Old AVG": "",
                    "New AVG": "",
                    "Details": str(e)
                })

    with open(aggregate_csv, "w", newline="") as f:
        fieldnames = [
            "Type", "Table", "Column", "Old SUM",
            "New SUM", "Old AVG", "New AVG", "Details"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        if discrepancies:
            writer.writerows(discrepancies)
        else:
            writer.writerow({
                "Type": "No discrepancies noted",
                "Table": "",
                "Column": "",
                "Old SUM": "",
                "New SUM": "",
                "Old AVG": "",
                "New AVG": "",
                "Details": ""
            })

        writer.writerow({})
        writer.writerow({})

        writer.writerow({
            "Type": "Detailed Comparison Below",
            "Table": "",
            "Column": "",
            "Old SUM": "",
            "New SUM": "",
            "Old AVG": "",
            "New AVG": "",
            "Details": ""
        })
        writer.writerow({})
        writer.writerows(detailed_comparison)

    print(f"[INFO] Aggregate function validation saved to {aggregate_csv}")

###############################################################################
# The ORIGINAL Value-by-Value Comparison (UNMODIFIED)
###############################################################################

# def value_by_value_comparison(old_conn, new_conn, old_schema, new_schema, tables, results_dir):
#     """
#     Performs a value-by-value comparison between old and new databases.
#     Only discrepancies are included in the CSV file. No detailed comparison.
#     Results are saved to a CSV file.
#     """
#     value_comparison_csv = os.path.join(results_dir, "value_comparison.csv")
#     discrepancies = []
#
#     for table in tables:
#         print(f"[INFO] Performing value-by-value comparison for table '{table}'...")
#
#         # Original approach: fetch all data, store in dicts, compare
#         old_columns, old_data = get_table_data(old_conn, old_schema, table)
#         new_columns, new_data = get_table_data(new_conn, new_schema, table)
#
#         # Check if column structures match
#         if old_columns != new_columns:
#             discrepancies.append({
#                 "Type": "Column Structure Mismatch",
#                 "Table": table,
#                 "Details": f"Column structure differs: Old({old_columns}) vs New({new_columns})"
#             })
#             continue
#
#         # Convert data to dictionaries for comparison
#         old_data_dict = {tuple(row): row for row in old_data}
#         new_data_dict = {tuple(row): row for row in new_data}
#
#         # Find missing rows in the new database
#         missing_in_new = set(old_data_dict.keys()) - set(new_data_dict.keys())
#         for missing_row in missing_in_new:
#             discrepancies.append({
#                 "Type": "Missing Row in New",
#                 "Table": table,
#                 "Details": f"Row missing in the new database: {old_data_dict[missing_row]}"
#             })
#
#         # Find extra rows in the new database
#         extra_in_new = set(new_data_dict.keys()) - set(old_data_dict.keys())
#         for extra_row in extra_in_new:
#             discrepancies.append({
#                 "Type": "Extra Row in New",
#                 "Table": table,
#                 "Details": f"Row extra in the new database: {new_data_dict[extra_row]}"
#             })
#
#         # Check for mismatched values in rows with the same keys
#         common_keys = set(old_data_dict.keys()).intersection(new_data_dict.keys())
#         for key in common_keys:
#             old_row = old_data_dict[key]
#             new_row = new_data_dict[key]
#             for col_idx, column in enumerate(old_columns):
#                 if old_row[col_idx] != new_row[col_idx]:
#                     discrepancies.append({
#                         "Type": "Cell Value Mismatch",
#                         "Table": table,
#                         "Column": column,
#                         "Row Key": key,
#                         "Old Value": old_row[col_idx],
#                         "New Value": new_row[col_idx],
#                         "Details": f"Mismatch in column '{column}' for key {key}: "
#                                    f"Old({old_row[col_idx]}) vs New({new_row[col_idx]})"
#                     })
#
#     # Save only discrepancies to CSV
#     with open(value_comparison_csv, "w", newline="") as f:
#         writer = csv.DictWriter(f,
#                                 fieldnames=["Type", "Table", "Column", "Row Key", "Old Value", "New Value", "Details"])
#         writer.writeheader()
#         if discrepancies:
#             writer.writerows(discrepancies)
#         else:
#             writer.writerow({
#                 "Type": "No discrepancies noted",
#                 "Table": "",
#                 "Column": "",
#                 "Row Key": "",
#                 "Old Value": "",
#                 "New Value": "",
#                 "Details": ""
#             })
#
#     print(f"[INFO] Value-by-value comparison saved to {value_comparison_csv}")

def value_by_value_check(old_conn, new_conn, old_schema, new_schema, tables, results_dir):
    """
    Performs a value-by-value comparison and saves results in separate CSV files every 100 tables.
    """
    value_mismatch_results = []

    for table in tables:
        print(f"[INFO] Performing value-by-value comparison for table '{table}'...")

        old_columns, old_data = get_table_data(old_conn, old_schema, table)
        new_columns, new_data = get_table_data(new_conn, new_schema, table)

        if old_columns != new_columns:
            value_mismatch_results.append({
                "Table": table,
                "Type": "Column Structure Mismatch",
                "Details": f"Old({old_columns}) vs New({new_columns})"
            })
            continue

        old_data_dict = {tuple(row): row for row in old_data}
        new_data_dict = {tuple(row): row for row in new_data}

        missing_in_new = set(old_data_dict.keys()) - set(new_data_dict.keys())
        for missing_row in missing_in_new:
            value_mismatch_results.append({
                "Table": table,
                "Type": "Missing Row in New",
                "Details": f"Row missing in the new database: {old_data_dict[missing_row]}"
            })

        extra_in_new = set(new_data_dict.keys()) - set(old_data_dict.keys())
        for extra_row in extra_in_new:
            value_mismatch_results.append({
                "Table": table,
                "Type": "Extra Row in New",
                "Details": f"Row extra in the new database: {new_data_dict[extra_row]}"
            })

        common_keys = set(old_data_dict.keys()).intersection(new_data_dict.keys())
        for key in common_keys:
            old_row = old_data_dict[key]
            new_row = new_data_dict[key]
            for col_idx, column in enumerate(old_columns):
                if old_row[col_idx] != new_row[col_idx]:
                    value_mismatch_results.append({
                        "Table": table,
                        "Type": "Cell Value Mismatch",
                        "Column": column,
                        "Row Key": key,
                        "Old Value": old_row[col_idx],
                        "New Value": new_row[col_idx],
                        "Details": f"Mismatch in column '{column}' for key {key}: "
                                   f"Old({old_row[col_idx]}) vs New({new_row[col_idx]})"
                    })

    save_results_in_batches(value_mismatch_results, results_dir, "value_by_value_check")


###############################################################################
# Null Value Verification (Added Back)
###############################################################################

def null_value_verification(old_conn, new_conn, old_schema, new_schema, tables, results_dir):
    """
    Verifies if null values are exactly the same in the old and new databases.
    Discrepancies are noted first, followed by a detailed comparison.
    Results are saved to a CSV file.
    """
    null_csv = os.path.join(results_dir, "null_value_verification.csv")
    discrepancies = []
    detailed_comparison = []

    for table in tables:
        print(f"[INFO] Performing null value verification for table '{table}'...")

        # Fetch columns for the table
        old_table_schema = get_table_schema(old_conn, old_schema, table)
        new_table_schema = get_table_schema(new_conn, new_schema, table)

        # Identify common columns
        common_columns = set(old_table_schema.keys()).intersection(new_table_schema.keys())

        for column in common_columns:
            try:
                # Null count query for old database
                old_query = f"SELECT COUNT(*) FROM {old_schema}.{table} WHERE {column} IS NULL"
                old_cursor = old_conn.cursor()
                old_cursor.execute(old_query)
                old_null_count = old_cursor.fetchone()[0]
                old_cursor.close()

                # Null count query for new database
                new_query = f"SELECT COUNT(*) FROM {new_schema}.{table} WHERE {column} IS NULL"
                new_cursor = new_conn.cursor()
                new_cursor.execute(new_query)
                new_null_count = new_cursor.fetchone()[0]
                new_cursor.close()

                # Compare results
                if old_null_count != new_null_count:
                    discrepancies.append({
                        "Type": "Null Count Mismatch",
                        "Table": table,
                        "Column": column,
                        "Old Null Count": old_null_count,
                        "New Null Count": new_null_count,
                        "Details": f"Mismatch in null count for column '{column}' in table '{table}'."
                    })

                # Add detailed comparison
                detailed_comparison.append({
                    "Table": table,
                    "Column": column,
                    "Old Null Count": old_null_count,
                    "New Null Count": new_null_count
                })

            except Exception as e:
                print(f"[ERROR] Failed to verify null values for table '{table}', column '{column}': {e}")

    # Save to CSV
    with open(null_csv, "w", newline="") as f:
        fieldnames = ["Type", "Table", "Column", "Old Null Count", "New Null Count", "Details"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        if discrepancies:
            writer.writerows(discrepancies)
        else:
            writer.writerow({
                "Type": "No discrepancies noted",
                "Table": "",
                "Column": "",
                "Old Null Count": "",
                "New Null Count": "",
                "Details": ""
            })

        # Add blank lines for separation
        writer.writerow({})
        writer.writerow({})

        # Detailed comparison
        writer.writerow({
            "Type": "Detailed Comparison Below",
            "Table": "",
            "Column": "",
            "Old Null Count": "",
            "New Null Count": "",
            "Details": ""
        })
        writer.writerow({})
        for row in detailed_comparison:
            writer.writerow({
                "Type": "Detailed Comparison",
                "Table": row["Table"],
                "Column": row["Column"],
                "Old Null Count": row["Old Null Count"],
                "New Null Count": row["New Null Count"],
                "Details": ""
            })

    print(f"[INFO] Null value verification saved to {null_csv}")

###############################################################################
# SQL Join Validation (Primary Key)
###############################################################################

def sql_join_operation(old_conn, new_conn, old_schema, new_schema, tables, results_dir):
    """
    Performs SQL JOIN-based validation, ensuring INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN are covered.
    Saves results in separate CSV files every 100 tables.
    """
    join_mismatch_results = []

    for table in tables:
        print(f"[INFO] Performing SQL JOIN operation check for table '{table}'...")

        old_columns, old_data = get_table_data(old_conn, old_schema, table)
        new_columns, new_data = get_table_data(new_conn, new_schema, table)

        if old_columns != new_columns:
            join_mismatch_results.append({
                "Table": table,
                "Type": "Column Structure Mismatch",
                "Details": f"Column structure differs: Old({old_columns}) vs New({new_columns})"
            })
            continue

        old_data_dict = {tuple(row): row for row in old_data}
        new_data_dict = {tuple(row): row for row in new_data}

        common_rows = set(old_data_dict.keys()).intersection(new_data_dict.keys())
        for row_key in common_rows:
            if old_data_dict[row_key] != new_data_dict[row_key]:
                join_mismatch_results.append({
                    "Table": table,
                    "Type": "Data Mismatch (INNER JOIN)",
                    "Details": f"Values differ for row {row_key}: Old({old_data_dict[row_key]}) vs New({new_data_dict[row_key]})"
                })

        missing_in_new = set(old_data_dict.keys()) - set(new_data_dict.keys())
        for row in missing_in_new:
            join_mismatch_results.append({
                "Table": table,
                "Type": "Missing in New DB (LEFT JOIN)",
                "Details": f"Row {row} exists in Old DB but not in New DB"
            })

        missing_in_old = set(new_data_dict.keys()) - set(old_data_dict.keys())
        for row in missing_in_old:
            join_mismatch_results.append({
                "Table": table,
                "Type": "Extra in New DB (RIGHT JOIN)",
                "Details": f"Row {row} exists in New DB but not in Old DB"
            })

        for row in missing_in_old | missing_in_new:
            join_mismatch_results.append({
                "Table": table,
                "Type": "Full Outer Join Discrepancy",
                "Details": f"Row {row} exists in one DB but not in the other"
            })

    save_results_in_batches(join_mismatch_results, results_dir, "sql_join_comparison")


# def sql_join_operation(old_conn, new_conn, old_schema, new_schema, tables, results_dir):
#     """
#     Performs SQL JOIN-based validation, ensuring INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN are covered.
#     """
#     join_comparison_csv = os.path.join(results_dir, "sql_join_comparison.csv")
#     discrepancies = []
#
#     for table in tables:
#         print(f"[INFO] Performing SQL JOIN operation check for table '{table}'...")
#
#         # Fetch table data
#         old_columns, old_data = get_table_data(old_conn, old_schema, table)
#         new_columns, new_data = get_table_data(new_conn, new_schema, table)
#
#         if old_columns != new_columns:
#             discrepancies.append({
#                 "Type": "Column Structure Mismatch",
#                 "Table": table,
#                 "Details": f"Column structure differs: Old({old_columns}) vs New({new_columns})"
#             })
#             continue
#
#         # Convert data to dictionary for comparison
#         old_data_dict = {tuple(row): row for row in old_data}
#         new_data_dict = {tuple(row): row for row in new_data}
#
#         # ✅ INNER JOIN: Compare matching rows
#         common_rows = set(old_data_dict.keys()).intersection(new_data_dict.keys())
#         for row_key in common_rows:
#             if old_data_dict[row_key] != new_data_dict[row_key]:
#                 discrepancies.append({
#                     "Type": "Data Mismatch (INNER JOIN)",
#                     "Table": table,
#                     "Row": row_key,
#                     "Old Value": old_data_dict[row_key],
#                     "New Value": new_data_dict[row_key],
#                     "Details": "Values differ"
#                 })
#
#         # ✅ LEFT JOIN: Rows missing in the new database
#         missing_in_new = set(old_data_dict.keys()) - set(new_data_dict.keys())
#         for row in missing_in_new:
#             discrepancies.append({
#                 "Type": "Missing in New DB (LEFT JOIN)",
#                 "Table": table,
#                 "Row": row,
#                 "Details": "Row exists in Old DB but not in New DB"
#             })
#
#         # ✅ RIGHT JOIN: Rows missing in the old database
#         missing_in_old = set(new_data_dict.keys()) - set(old_data_dict.keys())
#         for row in missing_in_old:
#             discrepancies.append({
#                 "Type": "Extra in New DB (RIGHT JOIN)",
#                 "Table": table,
#                 "Row": row,
#                 "Details": "Row exists in New DB but not in Old DB"
#             })
#
#         # ✅ FULL OUTER JOIN: Log rows missing in either database
#         for row in missing_in_old | missing_in_new:
#             discrepancies.append({
#                 "Type": "Full Outer Join Discrepancy",
#                 "Table": table,
#                 "Row": row,
#                 "Details": "Row exists in one DB but not in the other"
#             })
#
#     # Write to CSV
#     with open(join_comparison_csv, "w", newline="") as f:
#         fieldnames = ["Type", "Table", "Row", "Details"]
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(discrepancies)
#
#     print(f"[INFO] SQL JOIN operation validation saved to {join_comparison_csv}")

# def sql_join_operation_validation_with_details(old_conn, new_conn, old_schema, new_schema, tables, results_dir):
#     """
#     Performs LEFT, RIGHT, and FULL OUTER JOIN comparisons of each table
#     from old_schema vs new_schema, checking for missing rows and data mismatches.
#     """
#
#     join_validation_csv = os.path.join(results_dir, "sql_join_validation.csv")
#     discrepancies = []
#     detailed_comparison = []
#
#     # Make sure results directory exists
#     os.makedirs(results_dir, exist_ok=True)
#
#     for table in tables:
#         print(f"[INFO] Performing SQL join operation validation for table '{table}'...")
#
#         try:
#             # Get schema details
#             old_table_schema = get_table_schema(old_conn, old_schema, table)
#             new_table_schema = get_table_schema(new_conn, new_schema, table)
#
#             # Skip if table not found or no columns
#             if not old_table_schema or not new_table_schema:
#                 continue
#
#             # Get PK columns, fallback if none
#             pk_cols = get_primary_key_columns(old_conn, old_schema, table)
#             if not pk_cols:
#                 pk_cols = [list(old_table_schema.keys())[0]]
#
#             # Build the join condition using PK columns (unquoted)
#             # If your columns might have special chars or mixed case, you'll need quotes.
#             join_condition = " AND ".join([
#                 f"o.{col} = n.{col}" for col in pk_cols
#             ])
#             if not join_condition:
#                 continue
#
#             # Collect columns
#             old_columns = list(old_table_schema.keys())
#             new_columns = list(new_table_schema.keys())
#
#             # Create SELECT aliases (unquoted)
#             old_col_str = ", ".join([
#                 f"o.{col} AS old_{col}" for col in old_columns
#             ])
#             new_col_str = ", ".join([
#                 f"n.{col} AS new_{col}" for col in new_columns
#             ])
#
#             # 1) LEFT JOIN
#             left_join_query = f"""
#                 SELECT {old_col_str}, {new_col_str}
#                 FROM "{old_schema}"."{table}" o
#                 LEFT JOIN "{new_schema}"."{table}" n
#                 ON {join_condition}
#             """
#             cursor = old_conn.cursor()
#             cursor.execute(left_join_query)
#             left_join_rows = cursor.fetchall()
#             left_join_cols = [desc[0] for desc in cursor.description]
#             cursor.close()
#
#             # 2) RIGHT JOIN
#             right_join_query = f"""
#                 SELECT {old_col_str}, {new_col_str}
#                 FROM {old_schema}.{table} o
#                 RIGHT JOIN {new_schema}.{table} n
#                 ON {join_condition}
#             """
#             cursor = new_conn.cursor()
#             cursor.execute(right_join_query)
#             right_join_rows = cursor.fetchall()
#             right_join_cols = [desc[0] for desc in cursor.description]
#             cursor.close()
#
#             # 3) FULL OUTER JOIN
#             full_outer_query = f"""
#                 SELECT {old_col_str}, {new_col_str}
#                 FROM {old_schema}.{table} o
#                 FULL OUTER JOIN {new_schema}.{table} n
#                 ON {join_condition}
#             """
#             cursor = new_conn.cursor()
#             cursor.execute(full_outer_query)
#             full_outer_rows = cursor.fetchall()
#             full_outer_cols = [desc[0] for desc in cursor.description]
#             cursor.close()
#
#             # Check for missing rows (Left, Right) & data mismatches (Full)
#             # -- Left Join --
#             for row in left_join_rows:
#                 row_dict = dict(zip(left_join_cols, row))
#                 null_new_cols = [
#                     col for col in new_columns
#                     if row_dict.get(f"NEW_{col}") is None
#                 ]
#                 if null_new_cols:
#                     discrepancies.append({
#                         "Type": "Left Join Discrepancy",
#                         "Table": table,
#                         "Row": str(row_dict),
#                         "Join Key": ", ".join(pk_cols),
#                         "Details": f"Row in OLD DB but missing in NEW DB (NULL in {null_new_cols})"
#                     })
#
#             # -- Right Join --
#             for row in right_join_rows:
#                 row_dict = dict(zip(right_join_cols, row))
#                 null_old_cols = [
#                     col for col in old_columns
#                     if row_dict.get(f"OLD_{col}") is None
#                 ]
#                 if null_old_cols:
#                     discrepancies.append({
#                         "Type": "Right Join Discrepancy",
#                         "Table": table,
#                         "Row": str(row_dict),
#                         "Join Key": ", ".join(pk_cols),
#                         "Details": f"Row in NEW DB but missing in OLD DB (NULL in {null_old_cols})"
#                     })
#
#             # -- Full Outer Join --
#             for row in full_outer_rows:
#                 row_dict = dict(zip(full_outer_cols, row))
#                 null_old_cols = [
#                     col for col in old_columns
#                     if row_dict.get(f"OLD_{col}") is None
#                 ]
#                 null_new_cols = [
#                     col for col in new_columns
#                     if row_dict.get(f"NEW_{col}") is None
#                 ]
#
#                 if null_old_cols or null_new_cols:
#                     # Entire row is missing on one side
#                     discrepancies.append({
#                         "Type": "Full Outer Join Discrepancy",
#                         "Table": table,
#                         "Row": str(row_dict),
#                         "Join Key": ", ".join(pk_cols),
#                         "Details": (
#                             f"Row missing on one side. "
#                             f"NULL old cols: {null_old_cols}, NULL new cols: {null_new_cols}"
#                         )
#                     })
#                 else:
#                     # If both sides exist, compare columns
#                     for col in old_columns:
#                         if col in new_columns:
#                             old_val = row_dict.get(f"OLD_{col}")
#                             new_val = row_dict.get(f"NEW_{col}")
#                             if old_val != new_val:
#                                 discrepancies.append({
#                                     "Type": "Data Mismatch",
#                                     "Table": table,
#                                     "Row": str(row_dict),
#                                     "Join Key": ", ".join(pk_cols),
#                                     "Details": (
#                                         f"Column '{col}' mismatch: "
#                                         f"old_value={old_val} vs new_value={new_val}"
#                                     )
#                                 })
#
#             # Summaries
#             detailed_comparison.append({
#                 "Type": "Detailed Comparison",
#                 "Table": table,
#                 "Join Key": ", ".join(pk_cols),
#                 "Left Join Rows": len(left_join_rows),
#                 "Right Join Rows": len(right_join_rows),
#                 "Full Outer Join Rows": len(full_outer_rows),
#                 "Details": "Join analysis complete"
#             })
#
#         except Exception as e:
#             discrepancies.append({
#                 "Type": "Join Error",
#                 "Table": table,
#                 "Row": "",
#                 "Join Key": "",
#                 "Details": str(e)
#             })
#
#     # Write CSV
#     with open(join_validation_csv, "w", newline="") as f:
#         fieldnames = ["Type", "Table", "Row", "Join Key", "Details"]
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#
#         if discrepancies:
#             writer.writerows(discrepancies)
#         else:
#             writer.writerow({
#                 "Type": "No discrepancies noted",
#                 "Table": "",
#                 "Row": "",
#                 "Join Key": "",
#                 "Details": ""
#             })
#
#         # Blank lines
#         writer.writerow({})
#         writer.writerow({})
#
#         # Summaries
#         writer.writerow({
#             "Type": "Detailed Comparison Below",
#             "Table": "",
#             "Row": "",
#             "Join Key": "",
#             "Details": ""
#         })
#         writer.writerow({})
#
#         fieldnames2 = [
#             "Type", "Table", "Join Key",
#             "Left Join Rows", "Right Join Rows",
#             "Full Outer Join Rows", "Details"
#         ]
#         writer2 = csv.DictWriter(f, fieldnames=fieldnames2)
#         writer2.writeheader()
#         writer2.writerows(detailed_comparison)
#
#     print(f"[INFO] SQL join operation validation saved to {join_validation_csv}")


###############################################################################
# Miscellaneous Discrepancies
###############################################################################

def get_indexes(connection, schema_name, table_name):
    query = """
      SELECT index_name
      FROM all_indexes
      WHERE owner = UPPER(:schema_param)
        AND table_name = UPPER(:table_param)
    """
    cursor = connection.cursor()
    cursor.execute(query, schema_param=schema_name, table_param=table_name)
    indexes = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return indexes

def get_triggers(connection, schema_name, table_name):
    query = """
      SELECT trigger_name
      FROM all_triggers
      WHERE table_owner = UPPER(:schema_param)
        AND table_name = UPPER(:table_param)
    """
    cursor = connection.cursor()
    cursor.execute(query, schema_param=schema_name, table_param=table_name)
    triggers = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return triggers

def get_sequences(connection, schema_name):
    query = """
      SELECT sequence_name
      FROM all_sequences
      WHERE sequence_owner = UPPER(:schema_param)
    """
    cursor = connection.cursor()
    cursor.execute(query, schema_param=schema_name)
    sequences = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return sequences

def get_views(connection, schema_name):
    query = """
      SELECT view_name
      FROM all_views
      WHERE owner = UPPER(:schema_param)
    """
    cursor = connection.cursor()
    cursor.execute(query, schema_param=schema_name)
    views = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return views

def miscellaneous_discrepancies(old_conn, new_conn, old_schema, new_schema, results_dir):
    misc_csv = os.path.join(results_dir, "table_hygiene_check.csv")
    discrepancies = []
    detailed_comparison = []

    old_tables = get_table_list(old_conn, old_schema)
    new_tables = get_table_list(new_conn, new_schema)
    missing_tables = set(old_tables) - set(new_tables)
    extra_tables = set(new_tables) - set(old_tables)
    common_tables = set(old_tables).intersection(new_tables)

    # Missing/extra tables
    for t in missing_tables:
        discrepancies.append({
            "Type": "Missing Table",
            "Table": t,
            "Object": "",
            "Details": "Table exists in old DB but not in new DB."
        })
    for t in extra_tables:
        discrepancies.append({
            "Type": "Extra Table",
            "Table": t,
            "Object": "",
            "Details": "Table exists in new DB but not in old DB."
        })

    # Compare indexes, triggers in common tables
    for t in common_tables:
        old_idx = get_indexes(old_conn, old_schema, t)
        new_idx = get_indexes(new_conn, new_schema, t)
        missing_idx = old_idx - new_idx
        extra_idx = new_idx - old_idx

        for i in missing_idx:
            discrepancies.append({
                "Type": "Missing Index",
                "Table": t,
                "Object": i,
                "Details": f"Index '{i}' is missing in new DB."
            })
        for i in extra_idx:
            discrepancies.append({
                "Type": "Extra Index",
                "Table": t,
                "Object": i,
                "Details": f"Index '{i}' is extra in new DB."
            })

        old_trg = get_triggers(old_conn, old_schema, t)
        new_trg = get_triggers(new_conn, new_schema, t)
        missing_trg = old_trg - new_trg
        extra_trg = new_trg - old_trg

        for trg in missing_trg:
            discrepancies.append({
                "Type": "Missing Trigger",
                "Table": t,
                "Object": trg,
                "Details": f"Trigger '{trg}' is missing in new DB."
            })
        for trg in extra_trg:
            discrepancies.append({
                "Type": "Extra Trigger",
                "Table": t,
                "Object": trg,
                "Details": f"Trigger '{trg}' is extra in new DB."
            })

        detailed_comparison.append({
            "Type": "Detailed Comparison",
            "Table": t,
            "Object": "Indexes/Triggers",
            "Details": (
                f"Old indexes={old_idx}, New indexes={new_idx}; "
                f"Old triggers={old_trg}, New triggers={new_trg}"
            )
        })

    # Compare sequences
    old_seq = get_sequences(old_conn, old_schema)
    new_seq = get_sequences(new_conn, new_schema)
    missing_seq = old_seq - new_seq
    extra_seq = new_seq - old_seq

    for s in missing_seq:
        discrepancies.append({
            "Type": "Missing Sequence",
            "Table": "",
            "Object": s,
            "Details": f"Sequence '{s}' is missing in new DB."
        })
    for s in extra_seq:
        discrepancies.append({
            "Type": "Extra Sequence",
            "Table": "",
            "Object": s,
            "Details": f"Sequence '{s}' is extra in new DB."
        })

    # Compare views
    old_vw = get_views(old_conn, old_schema)
    new_vw = get_views(new_conn, new_schema)
    missing_vw = old_vw - new_vw
    extra_vw = new_vw - old_vw

    for v in missing_vw:
        discrepancies.append({
            "Type": "Missing View",
            "Table": "",
            "Object": v,
            "Details": f"View '{v}' is missing in new DB."
        })
    for v in extra_vw:
        discrepancies.append({
            "Type": "Extra View",
            "Table": "",
            "Object": v,
            "Details": f"View '{v}' is extra in new DB."
        })

    detailed_comparison.append({
        "Type": "Detailed Comparison",
        "Table": "",
        "Object": "Sequences",
        "Details": f"Old sequences={old_seq}, New sequences={new_seq}"
    })
    detailed_comparison.append({
        "Type": "Detailed Comparison",
        "Table": "",
        "Object": "Views",
        "Details": f"Old views={old_vw}, New views={new_vw}"
    })

    with open(misc_csv, "w", newline="") as f:
        fieldnames = ["Type", "Table", "Object", "Details"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        if discrepancies:
            writer.writerows(discrepancies)
        else:
            writer.writerow({
                "Type": "No discrepancies noted",
                "Table": "",
                "Object": "",
                "Details": ""
            })

        writer.writerow({})
        writer.writerow({})

        writer.writerow({
            "Type": "Detailed Comparison Below",
            "Table": "",
            "Object": "",
            "Details": ""
        })
        writer.writerow({})
        writer.writerows(detailed_comparison)

    print(f"[INFO] Miscellaneous discrepancies saved to {misc_csv}")

###############################################################################
# Main
###############################################################################

# def main():
#     num_iterations = int(input("Enter the number of times you want to run the migration audit process: ").strip())
#     params_list = []
#
#     for i in range(num_iterations):
#         print(f"\n[INFO] Enter details for Run {i + 1}:")
#         params = prompt_user_for_info()
#         params_list.append(params)
#
#     for run_num, params in enumerate(params_list, start=1):
#         print(f"\n[INFO] Starting Migration Run {run_num} of {num_iterations}...")
#
#         notification_process = subprocess.Popen(["python", "notify_on_completion.py"], stdout=subprocess.DEVNULL,
#                                                 stderr=subprocess.DEVNULL)
#         time.sleep(2)  # Allow bot script to initialize
#
#         old_db_config = params["old_db_config"]
#         new_db_config = params["new_db_config"]
#         old_schema = old_db_config["schema"]
#         new_schema = new_db_config["schema"]
#
#         timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#         results_dir = os.path.join("audit_results", f"{old_schema}_{new_schema}", timestamp)
#         os.makedirs(results_dir, exist_ok=True)
#
#         total_steps = 7
#         step = 0
#
#         try:
#             print("\n[INFO] Establishing database connections...")
#             old_conn = get_oracle_connection(old_db_config)
#             new_conn = get_oracle_connection(new_db_config)
#             print("Connection Established Successfully!!")
#             send_telegram_notification(BOT_TOKEN, CHAT_IDS, "✅ Database Connection Established Successfully!")
#
#             old_tables = get_table_list(old_conn, old_schema)
#             new_tables = get_table_list(new_conn, new_schema)
#             common_tables = set(old_tables).intersection(new_tables)
#
#             steps = [
#                 ("Validating Table Sanity", miscellaneous_discrepancies,
#                  [old_conn, new_conn, old_schema, new_schema, results_dir]),
#                 ("Validating Schema", schema_validation, [old_conn, new_conn, old_schema, new_schema, results_dir]),
#                 ("Checking Row Counts", count_validation, [old_conn, new_conn, old_schema, new_schema, results_dir]),
#                 ("Performing Aggregate Checks", aggregate_function_validation,
#                  [old_conn, new_conn, old_schema, new_schema, common_tables, results_dir]),
#                 ("Running SQL Join Validations", sql_join_operation,
#                  [old_conn, new_conn, old_schema, new_schema, common_tables, results_dir]),
#                 ("Comparing Data", value_by_value_check,
#                  [old_conn, new_conn, old_schema, new_schema, common_tables, results_dir]),
#                 ("Checking for NULL Values", null_value_verification,
#                  [old_conn, new_conn, old_schema, new_schema, common_tables, results_dir])
#             ]
#
#             for task_name, function, args in steps:
#                 progress = int((step / total_steps) * 100)
#                 send_telegram_notification(BOT_TOKEN, CHAT_IDS, f"📊 Progress: {progress}% - {task_name}...")
#                 function(*args)
#                 step += 1
#
#             send_telegram_notification(BOT_TOKEN, CHAT_IDS,
#                                        f"✅ Data Migration Audit Run {run_num} Completed Successfully!")
#             print(f"\n[INFO] Data Migration Audit Run {run_num} Completed Successfully!")
#
#         except Exception as e:
#             send_telegram_notification(BOT_TOKEN, CHAT_IDS, f"⚠️ Error in Run {run_num}: {str(e)}")
#             print(f"[ERROR] Migration Run {run_num} failed: {e}")
#
#         finally:
#             close_connection(old_conn)
#             close_connection(new_conn)
#             print("Connection Closed!!")
#             send_telegram_notification(BOT_TOKEN, CHAT_IDS, f"❎ Database Connection Closed for Run {run_num}!")
#
#     print("\n[INFO] All Migration Runs Completed Successfully!")
#     send_telegram_notification(BOT_TOKEN, CHAT_IDS, "🎉 All Migration Runs Completed Successfully!")
#
#
# if __name__ == "__main__":
#     main()

def main():
    num_iterations = int(input("Enter the number of times you want to run the migration process: ").strip())
    params_list = []

    for i in range(num_iterations):
        print(f"\n[INFO] Enter details for Run {i + 1}:")
        params = prompt_user_for_info()
        params_list.append(params)

    for run_num, params in enumerate(params_list, start=1):
        old_db_config = params["old_db_config"]
        new_db_config = params["new_db_config"]
        old_schema = old_db_config["schema"]
        new_schema = new_db_config["schema"]

        send_telegram_notification(BOT_TOKEN, CHAT_IDS,f"🔍 Starting Migration Run {run_num} of {num_iterations} for Database: {old_schema} -> {new_schema}...")

        print(f"\n[INFO] Starting Migration Run {run_num} of {num_iterations} for Database: {old_schema} -> {new_schema}...")

        notification_process = subprocess.Popen(["python", "notify_on_completion.py"], stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        time.sleep(2)  # Allow bot script to initialize

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = os.path.join("audit_results", f"{old_schema}_{new_schema}", timestamp)
        os.makedirs(results_dir, exist_ok=True)

        total_steps = 7
        step = 0

        try:
            print("\n[INFO] Establishing database connections...")
            old_conn = get_oracle_connection(old_db_config)
            new_conn = get_oracle_connection(new_db_config)
            print("Connection Established Successfully!!")
            send_telegram_notification(BOT_TOKEN, CHAT_IDS,f"✅ Database Connection Established Successfully for {old_schema} -> {new_schema}!")

            old_tables = get_table_list(old_conn, old_schema)
            new_tables = get_table_list(new_conn, new_schema)
            common_tables = set(old_tables).intersection(new_tables)

            steps = [
                ("Validating Table Sanity", miscellaneous_discrepancies,[old_conn, new_conn, old_schema, new_schema, results_dir]),
                ("Validating Schema", schema_validation, [old_conn, new_conn, old_schema, new_schema, results_dir]),
                ("Checking Row Counts", count_validation, [old_conn, new_conn, old_schema, new_schema, results_dir]),
                ("Performing Aggregate Checks", aggregate_function_validation,[old_conn, new_conn, old_schema, new_schema, common_tables, results_dir]),
                ("Running SQL Join Validations", sql_join_operation,[old_conn, new_conn, old_schema, new_schema, common_tables, results_dir]),
                ("Comparing Data", value_by_value_check,[old_conn, new_conn, old_schema, new_schema, common_tables, results_dir]),
                ("Checking for NULL Values", null_value_verification,[old_conn, new_conn, old_schema, new_schema, common_tables, results_dir])
            ]

            for task_name, function, args in steps:
                progress = int((step / total_steps) * 100)
                send_telegram_notification(BOT_TOKEN, CHAT_IDS,f"📊 Progress: {progress}% - {task_name} for {old_schema} -> {new_schema}...")
                function(*args)
                step += 1

            send_telegram_notification(BOT_TOKEN, CHAT_IDS,f"✅ Data Migration Audit Run {run_num} Completed Successfully for {old_schema} -> {new_schema}!")
            print(f"\n[INFO] Data Migration Audit Run {run_num} Completed Successfully for {old_schema} -> {new_schema}!")

        except Exception as e:
            send_telegram_notification(BOT_TOKEN, CHAT_IDS,f"⚠️ Error in Run {run_num} for {old_schema} -> {new_schema}: {str(e)}")
            print(f"[ERROR] Migration Run {run_num} failed for {old_schema} -> {new_schema}: {e}")

        finally:
            close_connection(old_conn)
            close_connection(new_conn)
            print("Connection Closed!!")
            send_telegram_notification(BOT_TOKEN, CHAT_IDS,f"❎ Database Connection Closed for Run {run_num} ({old_schema} -> {new_schema})!")

    print("\n[INFO] All Migration Runs Completed Successfully!")
    send_telegram_notification(BOT_TOKEN, CHAT_IDS, "🎉 All Migration Runs Completed Successfully!")


if __name__ == "__main__":
    main()
