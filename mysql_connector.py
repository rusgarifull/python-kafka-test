from getpass import getpass
from mysql.connector import connect, Error
import re

host = "localhost"
user = "root"
password = getpass("Enter password: ")

def anti_injections_check(name):
    """
    Checks if a string is a proper table/database name. 
    It helps to prevent SQL injections. 
    Returns True if a string is a proper name. Otherwise raises an error
    """
    name_validator = re.compile(r'^[0-9a-zA-Z_\$]+$')
    if not name_validator.match(name):
        raise ValueError('Hey!  No SQL injecting allowed!')
    else:
        return True


def create_table(db, table):
    """
    Creates a table in a specified database
    """
    try:
        with connect(host=host, user=user,password=password) as connection:
            create_db_query = "CREATE DATABASE IF NOT EXISTS {}".format(db)
            create_table_query = """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                message VARCHAR(100)
            )
            """.format(db,table)

            # check for potentials SQL injections and create a table 
            if anti_injections_check(table):
                with connection.cursor() as cursor:
                    cursor.execute(create_db_query)
                    cursor.execute(create_table_query)

    except Error as e:
        print("Error: ", e)


def insert_into_table(db, table, data):
    """
    Inserts data to a table
    """
    try:
        with connect(host=host, user=user, password=password, database=db) as connection:
            # check for potentials SQL injections and create a table 
            if anti_injections_check(table):
                insert_query = "INSERT INTO {} (message) VALUES (%s)".format(table)
                with connection.cursor() as cursor:
                    cursor.execute(insert_query,data)
                    connection.commit()
                    print("Message {} inserted".format(data))
    except Error as e:
        print("Error: ", e)
        