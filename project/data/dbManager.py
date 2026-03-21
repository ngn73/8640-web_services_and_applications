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
import jinja2 as tpl #for updating script templates(NO SQL IN CODE!!)

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

    def extract_query(self, filename:str, params=None):
        str_query = ""
        str_err = ""
        try:
            with open(filename, 'r') as f:
                str_query = f.read()
        except FileNotFoundError:
            str_err = f"The file path {filename} does not exist."
        except pd.errors.EmptyDataError:
            str_err = f"The file '{filename}' is empty."
        except PermissionError:
            str_err = f"Permission denied to Read {filename}."
        except Exception as ex:
            str_err = f"An unexpected error occurred while loading file {filename} : {ex}"
        
        if(len(str_err) == 0):
            final_query = str_query
            template = tpl.Template(str_query)
            final_query = template.render( params or {})  #Avoid None type errors (some scripts need 0 parameters)
            final_query = final_query.strip('\n')  #finally remove additional carriage returns from file
            return final_query
        else:
            self.mylogger.logErrorMessage(f"Error extracting query string : {str_err}")
            return ""