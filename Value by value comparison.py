import oracledb
import csv
import os
from datetime import datetime

def get_oracle_connection(user, password, dsn):
    try:
        connection = oracledb.connect(user=user, password=password, dsn=dsn)
        print(f"[INFO] Connected to {dsn} as {user}")
        return connection
    except oracledb.DatabaseError as e:
        print(f"[ERROR] Connection failed: {e}")
        raise

def execute_query(cursor, query, params=None):
    try:
        cursor.execute(query, params or {})
        return cursor.fetchall()
    except oracledb.DatabaseError as e:
        print(f"[ERROR] Query failed: {e}")
        raise

def get_table_list(connection):
    cursor = connection.cursor()
    query = "SELECT table_name FROM user_tables"
    tables = execute_query(cursor, query)
    cursor.close()
    return sorted([row[0] for row in tables])  # Sort table names alphabetically

def get_primary_key_columns(connection, table_name):
    cursor = connection.cursor()
    query = """
        SELECT cols.column_name
        FROM all_constraints cons
        JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
        WHERE cons.constraint_type = 'P'
          AND cons.owner = USER
          AND cols.table_name = :table_name
    """
    cursor.execute(query, table_name=table_name.upper())
    pk_columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return pk_columns

def get_primary_keys(connection, table_name, pk_columns):
    cursor = connection.cursor()
    pk_query = f"SELECT {', '.join(pk_columns)} FROM {table_name}"
    cursor.execute(pk_query)
    pk_rows = cursor.fetchall()
    cursor.close()
    return sorted(pk_rows, key=lambda x: x[0])  # Sort primary keys numerically

def fetch_row_by_pk(connection, table_name, pk_columns, pk_values):
    pk_clause = " AND ".join([f"{col} = :{idx+1}" for idx, col in enumerate(pk_columns)])
    query = f"SELECT * FROM {table_name} WHERE {pk_clause}"
    cursor = connection.cursor()
    cursor.execute(query, pk_values)
    row = cursor.fetchone()
    columns = [desc[0] for desc in cursor.description] if row else []
    cursor.close()
    return columns, row

def compare_rows(old_row, new_row, columns, pk_values, table_name, mismatches):
    if not new_row:
        print(f"[DISCREPANCY] Row with PK {pk_values} missing in new DB.")
        mismatches.append({
            "Table": table_name,
            "Primary Key": pk_values,
            "Column": "N/A",
            "Old Value": old_row,
            "New Value": "Row Missing",
            "Details": "Row exists in old DB but missing in new DB"
        })
        return

    for idx, col in enumerate(columns):
        if old_row[idx] != new_row[idx]:
            print(f"[MISMATCH] Table: {table_name}, PK: {pk_values}, Column: {col}")
            print(f"  Old Value: {old_row[idx]}")
            print(f"  New Value: {new_row[idx]}")
            mismatches.append({
                "Table": table_name,
                "Primary Key": pk_values,
                "Column": col,
                "Old Value": old_row[idx],
                "New Value": new_row[idx],
                "Details": "Value Mismatch"
            })

def compare_entire_database(old_conn, new_conn, output_dir):
    mismatches = []
    tables_without_pk = []

    # Get and sort table lists
    old_tables = get_table_list(old_conn)
    new_tables = get_table_list(new_conn)
    common_tables = sorted(set(old_tables).intersection(new_tables))  # Sort alphabetically

    for table in common_tables:
        print(f"\n[INFO] Comparing table: {table}")
        pk_columns = get_primary_key_columns(old_conn, table)
        if not pk_columns:
            print(f"[WARN] No primary key found for table {table}. Logging and Skipping.")
            tables_without_pk.append({"Table": table})
            continue

        # Get and sort primary keys
        pk_rows = get_primary_keys(old_conn, table, pk_columns)

        for pk_values in pk_rows:
            print(f"[INFO] Comparing row with PK {pk_values} in table {table}")

            old_columns, old_row = fetch_row_by_pk(old_conn, table, pk_columns, pk_values)
            _, new_row = fetch_row_by_pk(new_conn, table, pk_columns, pk_values)

            if old_row:
                compare_rows(old_row, new_row, old_columns, pk_values, table, mismatches)
            else:
                print(f"[DISCREPANCY] Row with PK {pk_values} missing in old DB.")
                mismatches.append({
                    "Table": table,
                    "Primary Key": pk_values,
                    "Column": "N/A",
                    "Old Value": "Row Missing",
                    "New Value": new_row,
                    "Details": "Row exists in new DB but missing in old DB"
                })

    # Save results to CSV
    save_mismatches_to_csv(mismatches, output_dir)
    save_tables_without_pk_to_csv(tables_without_pk, output_dir)

def save_mismatches_to_csv(mismatches, output_dir):
    if not mismatches:
        print("\n[INFO] No mismatches found. Database comparison completed successfully.")
        return

    output_file = os.path.join(output_dir, "database_mismatches.csv")
    with open(output_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["Table", "Primary Key", "Column", "Old Value", "New Value", "Details"])
        writer.writeheader()
        for row in mismatches:
            writer.writerow(row)

    print(f"\n[INFO] Mismatches saved to {output_file}")

def save_tables_without_pk_to_csv(tables_without_pk, output_dir):
    if not tables_without_pk:
        print("\n[INFO] All tables have primary keys.")
        return

    output_file = os.path.join(output_dir, "tables_without_primary_key.csv")
    with open(output_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["Table"])
        writer.writeheader()
        for row in tables_without_pk:
            writer.writerow(row)

    print(f"[INFO] Tables without primary keys saved to {output_file}")

def main():
    # Prompt user for connection details
    old_user = input("Enter OLD DB Username: ").strip()
    old_password = input("Enter OLD DB Password: ").strip()
    old_dsn = input("Enter OLD DB DSN (e.g., host:port/service_name): ").strip()

    new_user = input("Enter NEW DB Username: ").strip()
    new_password = input("Enter NEW DB Password: ").strip()
    new_dsn = input("Enter NEW DB DSN (e.g., host:port/service_name): ").strip()

    # Generate timestamped output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"./value_comparison_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    print(f"[INFO] Output will be saved to: {output_dir}")

    # Establish connections
    old_conn = get_oracle_connection(old_user, old_password, old_dsn)
    new_conn = get_oracle_connection(new_user, new_password, new_dsn)

    try:
        compare_entire_database(old_conn, new_conn, output_dir)
    finally:
        old_conn.close()
        new_conn.close()

if __name__ == "__main__":
    main()
