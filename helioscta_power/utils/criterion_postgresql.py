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

# CRITERION POSTGRESQL CREDENTIALS
CRITERION_POSTGRESQL_DB_HOST: str = "dda.criterionrsch.com"
CRITERION_POSTGRESQL_DB_USER: str = "c_helios"
CRITERION_POSTGRESQL_DB_PASSWORD: str = "M7YP1o4wfu"
CRITERION_POSTGRESQL_DB_PORT: int = 443

"""
"""

def _connect_to_criterion_postgressql(
        database: str = "production",
    ) -> psycopg2.extensions.connection:
    """
    """
    connection = psycopg2.connect(
        user=CRITERION_POSTGRESQL_DB_USER,
        password=CRITERION_POSTGRESQL_DB_PASSWORD,
        host=CRITERION_POSTGRESQL_DB_HOST,
        port=CRITERION_POSTGRESQL_DB_PORT,
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
        connection = _connect_to_criterion_postgressql(database=database)
        
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
