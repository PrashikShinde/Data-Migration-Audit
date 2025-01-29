import cx_Oracle


def fetch_data():
    try:
        dsn = cx_Oracle.makedsn("localhost", 1522, sid="xe")
        connection = cx_Oracle.connect(user="system", password="test", dsn=dsn)

        print("Database connection established successfully.")

        cursor = connection.cursor()

        # No semicolon at the end
        query = "SELECT table_name FROM user_tables WHERE table_name = 'BANK_DATABASE_SYSTEM'"

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            print(row)

    except cx_Oracle.DatabaseError as e:
        print(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Database connection closed.")


fetch_data()
