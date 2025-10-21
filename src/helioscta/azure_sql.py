import os

import pandas as pd
import pyodbc

# ignore warnings .. UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
import warnings
warnings.filterwarnings("ignore")

# AZURE SQL CREDENTIALS
AZURE_SQL_DB_HOST: str = "heliosazuresql.database.windows.net"
AZURE_SQL_DB_USER: str = "heliosadmin"
AZURE_SQL_DB_PASSWORD: str = "admin!2024"
AZURE_SQL_DB_PORT: int = 1433

"""
"""

def _connect_to_azure_sql(
        database: str,
    ) -> pyodbc.Connection:
    """
    """
    connection = pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={AZURE_SQL_DB_HOST};"
        f"UID={AZURE_SQL_DB_USER};"
        f"PWD={AZURE_SQL_DB_PASSWORD};"
        f"Database={database};"
    )
    print(f"Connected to Azure SQL ... Database: {database}")

    return connection
    

def pull_from_db(
        query: str,
        database: str,
    ) -> pd.DataFrame:

    try: 
        # Create a database connection
        connection = _connect_to_azure_sql(database=database)
        
        # SQL query
        # print(query)
        
        # Execute the query and fetch the data
        df = pd.read_sql(query, connection)
        
        # check dtypes
        # print(f"DTYPES ... {[f'{column}: {type(df[column][0])}' for column in list(df.columns)]}")
        return df

    except Exception as e:
        print(e)
        return None
