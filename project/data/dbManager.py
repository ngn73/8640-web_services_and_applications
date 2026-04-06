'''
Name: dbManager.py
Description: 
Object-based DBManager class separates the database connection and query execution logic from the rest of the application.
For multiple inserts/updates this uses one connection and one transaction, not a new connection for every row.
'''

from fileinput import filename
from turtle import pd
import app_logger as logger
import mysql.connector
from mysql.connector import Error
from contextlib import contextmanager
import config as cfg

class dbManager:
    def __init__(self, host: str, user: str, password: str, database: str):
        #Logger to use
        self.mylogger = logger.app_logger(__name__)
        self.config = {
            "host": cfg.CRUD["host"],
            "user": cfg.CRUD["user"],
            "password": cfg.CRUD["password"],
            "database": cfg.CRUD["database"],
        }

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = mysql.connector.connect(user=self.config["user"], host=self.config["host"], database=self.config["database"])
            yield conn
        except Exception as ex:
            self.mylogger.logErrorMessage(f"An unexpected error occurred while connecting to the database : {ex}")
        finally:
            if conn is not None and conn.is_connected():
                conn.close()

    @contextmanager
    def get_cursor(self, conn, dictionary: bool = False):
        cursor = None
        try:
            cursor = conn.cursor(dictionary=dictionary)
            yield cursor
        except Exception as ex:
            self.mylogger.logErrorMessage(f"An unexpected error generating cursor : {ex}")
        finally:
            if cursor is not None:
                cursor.close()

