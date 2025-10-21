import csv
import io
import os
import re
import requests
from datetime import date, datetime, timedelta
from typing import List

import numpy as np
import pandas as pd
import psycopg2

# ignore warnings
import warnings
warnings.filterwarnings("ignore")

# AZURE POSTGRESQL CREDENTIALS
AZURE_POSTGRESQL_DB_HOST: str = "heliosctadb.postgres.database.azure.com"
AZURE_POSTGRESQL_DB_USER: str = "helioscta"
AZURE_POSTGRESQL_DB_PASSWORD: str = "admin!2024"
AZURE_POSTGRESQL_DB_PORT: int = 5432

"""
"""

def _connect_to_azure_postgressql(
        database: str = "helioscta",
    ) -> psycopg2.extensions.connection:
    """
    """
    connection = psycopg2.connect(
        user=AZURE_POSTGRESQL_DB_USER,
        password=AZURE_POSTGRESQL_DB_PASSWORD,
        host=AZURE_POSTGRESQL_DB_HOST,
        port=AZURE_POSTGRESQL_DB_PORT,
        dbname=database,
    )
    return connection
    

def pull_from_db(
        query: str,
        database: str,
        drop_cols: bool = True,
    ) -> pd.DataFrame:

    try: 
        # Create a database connection
        connection = _connect_to_azure_postgressql(database=database)
        
        # Execute the query and fetch the data
        df = pd.read_sql(query, connection)
        for col in ["created_at", "updated_at"]:
            if col in df.columns and drop_cols: df.drop(columns=[col], inplace=True)
        # close connection
        connection.close()

        return df

    except Exception as e:
        print(e)
        return None
