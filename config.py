import os
import pyodbc
from dotenv import load_dotenv
import mysql.connector

class FilePathManager:
    def __init__(self):
        self._filepath = None
        self._archivepath = None
        self._errorpath = None

    def set_filepath(self, filepath):
        self._filepath = filepath

    def get_filepath(self):
        return self._filepath

    def set_archivepath(self, archivepath):
        self._archivepath = archivepath

    def get_archivepath(self):
        return self._archivepath

    def set_errorpath(self, errorpath):
        self._errorpath = errorpath

    def get_errorpath(self):
        return self._errorpath


def get_database_connection(TenantValue):
    FPM=FilePathManager()
    #TenantValue="sql"
    load_env(TenantValue)
    Path=os.environ.get("FILE_PATH")
    ArchPath=os.environ.get("ARCHIEVE_PATH")
    ErrorPath=os.environ.get("ERROR_PATH")
    FPM.set_filepath(Path)
    FPM.set_archivepath(ArchPath)
    FPM.set_errorpath(ErrorPath)
    driverName = os.environ.get("DB_DRIVER_NAME", "SQL Server")
    if driverName=="MSSQL":
        driver = os.environ.get("DB_DRIVER", "SQL Server")
        server = os.environ.get("DB_SERVER", "DESKTOP-T6SLROH\SQL2019")
        database = os.environ.get("DB_DATABASE", "AML")
        uid = os.environ.get("DB_UID", "sa")
        pwd = os.environ.get("DB_PWD", "benchmatrix786?")
        
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={uid};"
            f"PWD={pwd};"
        )

        conn = pyodbc.connect(connection_string)
        print("CONNECTION MADE SUCCSESSFULLY")
        return conn
    else:
        # host = os.environ.get("client1-db-ip", "10.96.81.188")
        host = os.environ.get("client1-db-ip", "35.192.138.65")
        database = os.environ.get("client1-db-name", "public_watchlisttest")
        user = os.environ.get("client1-db-user", "root")
        password = os.environ.get("client1-db-password", "aml@12345678")
        conn = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        print("CONNECTION MADE SUCCESSFULLY")
        return conn
def load_env(header):
    env_file_name = f"{header}.env"
    if os.path.exists(env_file_name):
        load_dotenv(env_file_name)
