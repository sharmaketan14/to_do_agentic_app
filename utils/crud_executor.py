import mysql.connector
from mysql.connector import Error

def connect_to_mysql():
    try:
        # Replace these with your actual values if different
        connection = mysql.connector.connect(
            host="127.0.0.1",  # Service name in Kubernetes
            user="root",
            password="mysqlpassword",  # Password from the Kubernetes secret
            database="mysql"  # Default database
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print(f"Connected to database: {record[0]}")

            # Example query
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            print("\nAvailable tables:")
            for table in tables:
                print(table[0])

        return connection

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def main():
    connection = connect_to_mysql()
    
    if connection is not None and connection.is_connected():
        # Example: Create a new database
        try:
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS test_db")
            print("\nCreated new database 'test_db'")
            
            # Switch to the new database
            cursor.execute("USE test_db")
            
            # Create a sample table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    email VARCHAR(255)
                )
            """)
            print("Created 'users' table")
            
        except Error as e:
            print(f"Error: {e}")
            
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection closed.")

if __name__ == "__main__":
    main()