from getpass import getpass
from mysql.connector import connect, Error

host = "localhost"
user = "root"
password = getpass("Enter password: ")

def create_test_table():
    try:
        with connect(host=host, user=user,password=password) as connection:
            create_db_query = "CREATE DATABASE IF NOT EXISTS kafka_messages"
            create_table_query = """
            CREATE TABLE IF NOT EXISTS kafka_messages.test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                message VARCHAR(100)
            )
            """
            with connection.cursor() as cursor:
                cursor.execute(create_db_query)
                cursor.execute(create_table_query)

    except Error as e:
        print("Error: ", e)


def insert_into_test_table(message_data):
    try:
        with connect(host=host, user=user,password=password, database="kafka_messages") as connection:
            insert_query = "INSERT INTO test_table (message) VALUES (%s)"
            with connection.cursor() as cursor:
                cursor.execute(insert_query,message_data)
                connection.commit()
                print("message {} inserted".format(message_data))
    except Error as e:
        print("Error: ", e)

