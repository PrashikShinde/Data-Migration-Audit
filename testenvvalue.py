import oracledb
import random
import string

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
    except oracledb.DatabaseError as e:
        print(f"[ERROR] Query failed: {e}")
        raise

def create_schema(cursor, schema_name, schema_password):
    try:
        execute_query(cursor, f"DROP USER {schema_name} CASCADE")
        print(f"[INFO] Existing user '{schema_name}' dropped.")
    except oracledb.DatabaseError:
        print(f"[INFO] User '{schema_name}' does not exist. Proceeding to create...")

    execute_query(cursor, f"""
        CREATE USER {schema_name} IDENTIFIED BY {schema_password}
        DEFAULT TABLESPACE USERS
        TEMPORARY TABLESPACE TEMP
        QUOTA UNLIMITED ON USERS
    """)
    execute_query(cursor, f"GRANT CONNECT, RESOURCE TO {schema_name}")
    print(f"[INFO] Schema '{schema_name}' created successfully.")

def create_table(cursor, schema_name, table_index):
    table_name = f"TABLE_{table_index}"
    execute_query(cursor, f"""
        CREATE TABLE {schema_name}.{table_name} (
            ID NUMBER PRIMARY KEY,
            NAME VARCHAR2(100),
            EMAIL VARCHAR2(100),
            BALANCE NUMBER,
            CREATED_AT DATE
        )
    """)
    print(f"[INFO] Table '{schema_name}.{table_name}' created.")
    return table_name

def create_table_without_pk(cursor, schema_name, table_index):
    table_name = f"NOPK_TABLE_{table_index}"
    execute_query(cursor, f"""
        CREATE TABLE {schema_name}.{table_name} (
            LOG_ID NUMBER,
            MESSAGE VARCHAR2(255),
            LOGGED_AT DATE
        )
    """)
    print(f"[INFO] Table '{schema_name}.{table_name}' created (No Primary Key).")
    return table_name

def populate_table(cursor, schema_name, table_name, num_rows, is_old_db=True):
    # Insert base data
    for i in range(1, num_rows + 1):
        name = f"User_{i}"
        email = f"user{i}@example.com"
        balance = round(random.uniform(100, 1000), 2)
        execute_query(cursor, f"""
            INSERT INTO {schema_name}.{table_name} (ID, NAME, EMAIL, BALANCE, CREATED_AT)
            VALUES (:1, :2, :3, :4, SYSDATE)
        """, (i, name, email, balance))

    # Introduce discrepancies in NEW DB
    if not is_old_db:
        # Modify one row
        execute_query(cursor, f"""
            UPDATE {schema_name}.{table_name}
            SET NAME = 'Modified_User', BALANCE = 9999.99
            WHERE ID = 1
        """)

        # Add an extra row with a unique ID
        new_id = num_rows + 1000  # Ensuring a non-conflicting ID
        execute_query(cursor, f"""
            INSERT INTO {schema_name}.{table_name} (ID, NAME, EMAIL, BALANCE, CREATED_AT)
            VALUES (:1, :2, :3, :4, SYSDATE)
        """, (new_id, 'New_Only_User', 'newonly@example.com', 500.00))

def populate_table_without_pk(cursor, schema_name, table_name, num_rows):
    for i in range(1, num_rows + 1):
        message = f"Log Message {i}"
        execute_query(cursor, f"""
            INSERT INTO {schema_name}.{table_name} (LOG_ID, MESSAGE, LOGGED_AT)
            VALUES (:1, :2, SYSDATE)
        """, (i, message))

def main():
    # Prompt user for Oracle DBA credentials
    dba_user = input("Enter DBA Username (e.g., SYSTEM): ").strip()
    dba_password = input("Enter DBA Password: ").strip()
    dba_dsn = input("Enter Oracle DSN (e.g., host:port/service_name): ").strip()

    # Prompt user for old and new schema names and passwords
    old_schema = input("Enter OLD schema name (e.g., OLD_TEST_SCHEMA): ").strip()
    old_password = input(f"Enter password for {old_schema}: ").strip()

    new_schema = input("Enter NEW schema name (e.g., NEW_TEST_SCHEMA): ").strip()
    new_password = input(f"Enter password for {new_schema}: ").strip()

    # Prompt for number of tables and rows
    num_tables = int(input("Enter the number of tables to create: "))
    num_rows = int(input("Enter the number of rows per table: "))

    # Connect as DBA
    conn = get_oracle_connection(dba_user, dba_password, dba_dsn)
    cursor = conn.cursor()

    try:
        # Step 1: Create Schemas
        create_schema(cursor, old_schema, old_password)
        create_schema(cursor, new_schema, new_password)

        # Step 2: Create Tables and Populate Data
        for i in range(1, num_tables + 1):
            # Create table with primary key
            old_table = create_table(cursor, old_schema, i)
            new_table = create_table(cursor, new_schema, i)

            # Populate tables
            populate_table(cursor, old_schema, old_table, num_rows, is_old_db=True)
            populate_table(cursor, new_schema, new_table, num_rows, is_old_db=False)

            # Every 3rd table without primary key
            if i % 3 == 0:
                old_nopk_table = create_table_without_pk(cursor, old_schema, i)
                new_nopk_table = create_table_without_pk(cursor, new_schema, i)

                populate_table_without_pk(cursor, old_schema, old_nopk_table, num_rows)
                populate_table_without_pk(cursor, new_schema, new_nopk_table, num_rows)

        conn.commit()
        print("\n✅ Test environment created successfully.")

    except Exception as e:
        print(f"[ERROR] {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
