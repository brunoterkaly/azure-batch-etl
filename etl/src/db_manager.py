"""Main driver for application logic"""
from __future__ import print_function
import uuid
import pymssql
import petl
from etl.src.db_manager_interface import DBManagerInterface

from etl.config.secrets import (
    _DBPWD,
)


class DBManager(DBManagerInterface):
    """class"""

    def __init__(self):
        super().__init__()
        pass

    def connect_to_db(self):
        try:
            server = "bonobosql.database.windows.net"
            database = "ohdsi"
            uid = "sqladmin"
            pwd = _DBPWD

            conn = pymssql.connect(server, uid, pwd, database)
            print("Connected to {}!".format(server))
            return conn
        except Exception as ex:
            print(ex)

    def create_table(self, cursor):
        try:
            cursor.execute(
                """
					IF OBJECT_ID('persons', 'U') IS NOT NULL
							DROP TABLE persons
					CREATE TABLE persons (
							id varchar(40),
							date datetime,
							bodysite_code varchar(50),
							modality_description VARCHAR(100),
							PRIMARY KEY(id)
					)
					"""
            )
            print("Table created...")
        except Exception as ex:
            print(ex)

    def insert_data(self, conn, cursor, input_file):
        columns = ["Id", "DATE", "BODYSITE_CODE", "MODALITY_DESCRIPTION"]
        person_table = petl.fromcsv(input_file)
        person_table = petl.cut(person_table, *columns)

        insert_statement = "unassigned"
        try:
            insert_cols = ",".join(map(str, columns))
            # Loop through rows
            print("Number of rows to insert = {}".format(len(person_table)))
            for i in range(1, len(person_table)):
                row = person_table[i]
                query = "INSERT INTO persons ( {}".format(insert_cols)
                values = ")VALUES('{}',".format(uuid.uuid1())
                # Loop through columns
                for j in range(1, len(row)):
                    values += "'{}'".format(
                        row[j]
                    )  # +1 because we skip the first value
                    if not j + 1 == len(row):
                        values += ","
                values += ")"
                # Execute insert
                insert_statement = "{} {}".format(query, values)
                print("{}".format(insert_statement))
                cursor.execute(insert_statement)
        except Exception as ex:
            print(ex)

    def show_data(self, cursor, query):
        try:
            cursor.execute(query)
            row = cursor.fetchone()
            while row:
                print("ID=%s, bodysite_code=%s" % (row[0], row[2]))
                row = cursor.fetchone()

        except Exception as ex:
            print(query)
            print(ex)

if __name__ == "__main__":
    obj = DBManager()
    obj.connect_to_db()
