from helioscta_power.utils import azure_postgresql
from helioscta_power.utils import data_utils

"""
"""

def get_data(
        database: str = "helioscta",
        sql_filename: str = "pjm_wh_da_forecasting_v1_2025_OCT_16.sql",
    ):

    # Load SQL query from file
    cwd = data_utils.get_current_directory()
    filepath = data_utils.get_file_path(filename=f"sql/{sql_filename}")
    with open(filepath, 'r') as file:
        query = file.read()

    print(query)

    # pull from db
    df = azure_postgresql.pull_from_db(query=query, database=database)

    return df

"""
"""

if __name__ == "__main__":

    df = get_data()
    # utils.write_pandas_parquet(df=df, filename='pjm_wh_da_forecasting_v1_2025_OCT_16.parquet')    