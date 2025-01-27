import cx_Oracle
import random
import string
import sys


def prompt_user_for_info():
    """
    Prompts the user for all required inputs to create two schemas,
    generate data, and introduce discrepancies.
    """
    print("Enter Oracle DBA credentials (with privileges to create users/schemas):")
    dba_user = input("  DBA Username (e.g. SYSTEM): ").strip()
    dba_password = input("  DBA Password: ").strip()
    dba_dsn = input("  DBA DSN (e.g. host:port/service_name): ").strip()

    print("\nEnter OLD schema details (where we'll generate original data):")
    old_schema = input("  Old Schema Name (e.g. MIGRATION_OLD): ").strip()
    old_password = input("  Old Schema Password: ").strip()

    print("\nEnter NEW schema details (copy + discrepancies):")
    new_schema = input("  New Schema Name (e.g. MIGRATION_NEW): ").strip()
    new_password = input("  New Schema Password: ").strip()

    print("\nNumber of tables to create in OLD schema:")
    try:
        num_tables = int(input("  (e.g. 100): "))
    except ValueError:
        num_tables = 100

    print("\nNumber of rows per table (in OLD schema):")
    try:
        rows_per_table = int(input("  (e.g. 100000): "))
    except ValueError:
        rows_per_table = 100000

    return {
        "dba_user": dba_user,
        "dba_password": dba_password,
        "dba_dsn": dba_dsn,
        "old_schema": old_schema.upper(),
        "old_password": old_password,
        "new_schema": new_schema.upper(),
        "new_password": new_password,
        "num_tables": num_tables,
        "rows_per_table": rows_per_table
    }


def drop_user_cascade(cursor, schema_name):
    """
    Force-drops a user/schema with all its objects and data.
    Ignores errors if the user does not exist.
    """
    try:
        cursor.execute(f"DROP USER {schema_name} CASCADE")
        print(f"  [INFO] Dropped user {schema_name} (CASCADE).")
    except cx_Oracle.DatabaseError as e:
        # Often occurs if the user doesn't exist
        print(f"  [WARN] Could not drop user {schema_name} (maybe doesn't exist). Error: {e}")


def create_schema(cursor, schema_name, schema_password):
    """
    Creates (or recreates) a user/schema with UNLIMITED QUOTA on USERS tablespace
    and grants CONNECT, RESOURCE.
    Ignores errors if the user already exists. You can modify for production usage.
    """
    # Create user
    try:
        cursor.execute(f"""
            CREATE USER {schema_name} IDENTIFIED BY {schema_password}
            DEFAULT TABLESPACE USERS
            TEMPORARY TABLESPACE TEMP
            QUOTA UNLIMITED ON USERS
        """)
        print(f"  [INFO] Created user {schema_name}")
    except cx_Oracle.DatabaseError as e:
        print(f"  [WARN] Could not create user {schema_name} (may already exist). Error: {e}")

    # Grant privileges
    try:
        cursor.execute(f"GRANT CONNECT, RESOURCE TO {schema_name}")
    except cx_Oracle.DatabaseError as e:
        print(f"  [WARN] Could not grant privileges to {schema_name}. Error: {e}")


def create_tables_in_old_schema(cursor, old_schema, num_tables):
    """
    Creates the specified number of tables in old_schema.
    Each table has a simple structure (ID, NAME, AMOUNT, CREATED_ON, STATUS).
    """
    for i in range(1, num_tables + 1):
        table_name = f"TABLE_{i}"
        sql = f"""
            CREATE TABLE {old_schema}.{table_name} (
                ID NUMBER GENERATED ALWAYS AS IDENTITY,
                NAME VARCHAR2(100),
                AMOUNT NUMBER(12,2),
                CREATED_ON DATE DEFAULT SYSDATE,
                STATUS VARCHAR2(20)
            )
        """
        try:
            cursor.execute(sql)
            print(f"  [INFO] Created {old_schema}.{table_name}")
        except cx_Oracle.DatabaseError as e:
            print(f"  [WARN] Could not create {old_schema}.{table_name}. Error: {e}")


def populate_tables_in_old_schema(conn, old_schema, num_tables, rows_per_table):
    """
    Inserts random data into each table in old_schema.
    For each table, generates rows_per_table of data.
    Uses batch inserts to improve performance.
    """
    cursor = conn.cursor()
    batch_size = 1000  # commit after each batch of 1000 inserts
    for i in range(1, num_tables + 1):
        table_name = f"TABLE_{i}"
        print(f"\n[INFO] Inserting data into {old_schema}.{table_name} with {rows_per_table} rows...")
        insert_sql = f"INSERT INTO {old_schema}.{table_name} (NAME, AMOUNT, STATUS) VALUES (:1, :2, :3)"
        batch_data = []
        total_inserted = 0
        for r in range(rows_per_table):
            # Random string for NAME
            name_val = ''.join(random.choices(string.ascii_uppercase, k=10))
            # Random float for AMOUNT
            amount_val = round(random.uniform(1, 999999), 2)
            # Status
            status_val = "ACTIVE" if (r % 5 != 0) else "INACTIVE"
            batch_data.append((name_val, amount_val, status_val))

            if len(batch_data) == batch_size:
                cursor.executemany(insert_sql, batch_data)
                batch_data = []
                total_inserted += batch_size

        # Insert remainder
        if batch_data:
            cursor.executemany(insert_sql, batch_data)
            total_inserted += len(batch_data)

        conn.commit()
        print(f"  [INFO] Inserted {total_inserted} rows into {table_name}.")
    cursor.close()


def copy_tables_to_new_schema(cursor, old_schema, new_schema):
    """
    Copies all tables from old_schema to new_schema.
    This uses CREATE TABLE new_schema.X AS SELECT * FROM old_schema.X
    for each table in old_schema.
    """
    sql = f"""
    DECLARE
      CURSOR c1 IS
        SELECT table_name
          FROM all_tables
         WHERE owner = UPPER('{old_schema}');
    BEGIN
      FOR t IN c1 LOOP
        BEGIN
          EXECUTE IMMEDIATE 'CREATE TABLE {new_schema}.' || t.table_name ||
                           ' AS SELECT * FROM {old_schema}.' || t.table_name;
        EXCEPTION
          WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Could not copy table ' || t.table_name || ': ' || SQLERRM);
        END;
      END LOOP;
    END;
    """
    cursor.execute(sql)
    print(f"[INFO] Copied all tables from {old_schema} to {new_schema}.")


def introduce_discrepancies(conn, new_schema):
    """
    Introduces intentional discrepancies in the new_schema:
      1. Drops a couple of tables
      2. Creates an extra table
      3. Adds a column in one table
      4. Drops a column in another
      5. Modifies some data
      6. Introduces some NULL values
    """
    cursor = conn.cursor()

    # 1. Drop a couple of tables (TABLE_5, TABLE_42) if they exist
    for tbl in [5, 42]:
        table_name = f"{new_schema}.TABLE_{tbl}"
        try:
            cursor.execute(f"DROP TABLE {table_name} PURGE")
            print(f"  [INFO] Dropped {table_name}")
        except:
            print(f"  [WARN] Could not drop {table_name}, possibly doesn't exist.")

    # 2. Create an extra table in new schema
    extra_table_name = f"{new_schema}.EXTRA_TABLE_999"
    try:
        cursor.execute(f"""
            CREATE TABLE {extra_table_name} (
                ID NUMBER GENERATED ALWAYS AS IDENTITY,
                DATA VARCHAR2(100)
            )
        """)
        print(f"  [INFO] Created extra table {extra_table_name}")
        # Insert some rows
        insert_sql = f"INSERT INTO {extra_table_name} (DATA) VALUES (:1)"
        batch_data = []
        for _ in range(10000):
            data_val = ''.join(random.choices(string.ascii_lowercase, k=20))
            batch_data.append((data_val,))
            if len(batch_data) == 1000:
                cursor.executemany(insert_sql, batch_data)
                batch_data = []
        # remainder
        if batch_data:
            cursor.executemany(insert_sql, batch_data)
        conn.commit()
        print(f"  [INFO] Inserted ~10000 rows into {extra_table_name}")
    except cx_Oracle.DatabaseError as e:
        print(f"  [WARN] Could not create {extra_table_name}. Error: {e}")

    # 3. Add a column to TABLE_10
    table_10 = f"{new_schema}.TABLE_10"
    try:
        cursor.execute(f"ALTER TABLE {table_10} ADD (EXTRA_COL VARCHAR2(50))")
        print(f"  [INFO] Added EXTRA_COL to {table_10}")
    except cx_Oracle.DatabaseError as e:
        print(f"  [WARN] Could not alter {table_10}. Error: {e}")

    # 4. Drop a column in TABLE_20 (STATUS for example)
    table_20 = f"{new_schema}.TABLE_20"
    try:
        cursor.execute(f"ALTER TABLE {table_20} DROP COLUMN STATUS")
        print(f"  [INFO] Dropped STATUS column from {table_20}")
    except cx_Oracle.DatabaseError as e:
        print(f"  [WARN] Could not drop column STATUS from {table_20}. Error: {e}")

    # 5. Modify some data in TABLE_1 (double the AMOUNT for first 100 rows)
    table_1 = f"{new_schema}.TABLE_1"
    try:
        cursor.execute(f"UPDATE {table_1} SET AMOUNT = AMOUNT * 2 WHERE ROWNUM <= 100")
        conn.commit()
        print(f"  [INFO] Modified data in {table_1} for first 100 rows.")
    except cx_Oracle.DatabaseError as e:
        print(f"  [WARN] Could not update {table_1}. Error: {e}")

    # 6. Introduce some NULL values in random rows
    #    Example: pick TABLE_2, set 50 random rows to NULL in NAME and AMOUNT
    table_2 = f"{new_schema}.TABLE_2"
    try:
        # Null out NAME for some random rows
        cursor.execute(f"""
            UPDATE {table_2} 
               SET NAME = NULL 
             WHERE ID IN (
                SELECT ID FROM {table_2} WHERE ROWNUM <= 50
             )
        """)
        # Null out AMOUNT for some random rows
        cursor.execute(f"""
            UPDATE {table_2} 
               SET AMOUNT = NULL 
             WHERE ID IN (
                SELECT ID FROM {table_2} WHERE ROWNUM <= 50
             )
        """)
        conn.commit()
        print(f"  [INFO] Introduced NULL values in {table_2}.")
    except cx_Oracle.DatabaseError as e:
        print(f"  [WARN] Could not introduce NULLs in {table_2}. Error: {e}")

    cursor.close()


def main():
    params = prompt_user_for_info()

    dba_user = params["dba_user"]
    dba_password = params["dba_password"]
    dba_dsn = params["dba_dsn"]
    old_schema = params["old_schema"]
    old_pass = params["old_password"]
    new_schema = params["new_schema"]
    new_pass = params["new_password"]
    num_tables = params["num_tables"]
    rows_per_table = params["rows_per_table"]

    # 1) Connect as DBA
    try:
        print(f"\n[INFO] Connecting as {dba_user} to {dba_dsn} ...")
        dba_conn = cx_Oracle.connect(dba_user, dba_password, dba_dsn)
    except cx_Oracle.DatabaseError as e:
        print(f"[ERROR] Could not connect as DBA. {e}")
        sys.exit(1)

    dba_cursor = dba_conn.cursor()

    # 2) Completely remove (if exists) and re-create new schema
    print(f"\n[INFO] Dropping user {new_schema} CASCADE if exists ...")
    drop_user_cascade(dba_cursor, old_schema)
    drop_user_cascade(dba_cursor, new_schema)
    print(f"[INFO] Creating (or verifying) old schema {old_schema} ...")
    create_schema(dba_cursor, old_schema, old_pass)
    print(f"[INFO] Creating (or verifying) new schema {new_schema} ...")
    create_schema(dba_cursor, new_schema, new_pass)
    dba_conn.commit()

    # 3) Create X tables in old schema
    print(f"\n[INFO] Creating {num_tables} tables in schema {old_schema} ...")
    create_tables_in_old_schema(dba_cursor, old_schema, num_tables)
    dba_conn.commit()

    # 4) Insert random data into old schema
    #    For large row counts, this might take a while
    print(f"\n[INFO] Populating tables in {old_schema} with random data ...")
    # Reconnect as old_schema user to do the inserts
    try:
        old_conn = cx_Oracle.connect(old_schema, old_pass, dba_dsn)
    except cx_Oracle.DatabaseError as e:
        print(f"[ERROR] Could not connect as {old_schema}. {e}")
        sys.exit(1)

    populate_tables_in_old_schema(old_conn, old_schema, num_tables, rows_per_table)
    old_conn.close()

    # 5) Copy from old to new
    #    Reconnect as DBA to do the cross-schema copies
    print(f"\n[INFO] Copying tables from {old_schema} to {new_schema} ...")
    copy_tables_to_new_schema(dba_cursor, old_schema, new_schema)
    dba_conn.commit()

    # 6) Introduce discrepancies + some NULL data
    #    Reconnect as new_schema to do data changes
    print(f"\n[INFO] Introducing discrepancies (and NULL values) in {new_schema} ...")
    try:
        new_conn = cx_Oracle.connect(new_schema, new_pass, dba_dsn)
    except cx_Oracle.DatabaseError as e:
        print(f"[ERROR] Could not connect as {new_schema}. {e}")
        sys.exit(1)

    introduce_discrepancies(new_conn, new_schema)
    new_conn.close()

    # Clean up
    dba_cursor.close()
    dba_conn.close()

    print("\n[INFO] Done! Your test environment is ready.")
    print("   - OLD schema has multiple tables filled with random data.")
    print("   - NEW schema is an initial copy, then 'emptied', recreated, and has some introduced differences + NULLs.")
    print("Use your migration/audit scripts to validate the discrepancies now!")


if __name__ == "__main__":
    main()