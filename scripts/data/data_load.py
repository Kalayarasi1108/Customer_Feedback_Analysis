import pandas as pd
import snowflake.connector
import requests
import os

def fetch_data_from_api(api_url):
    try:
        df = pd.DataFrame(requests.get(api_url).json())
        print(f"Fetched {len(df)} records from the API.")
        return df
    except Exception as e:
        print(f"Error fetching data from API: {e}")

def insert_data_into_snowflake(cur, table_name, df):
    data_tuples = [tuple(x) for x in df.values]
    columns = [f'"{col}"' for col in df.columns]
    placeholders = ', '.join(['%s'] * len(df.columns))
    cur.executemany(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})", data_tuples)
    print(f"Successfully inserted {len(data_tuples)} records into {table_name}.")

def load_data_to_snowflake(api_url, table_name):
    df = fetch_data_from_api(api_url)
    if df is not None and not df.empty:
        try:
            snowflake_config = {
                'user': os.environ['SNOWFLAKE_USER'],
                'password': os.environ['SNOWFLAKE_PASSWORD'],
                'account': os.environ['SNOWFLAKE_ACCOUNT'],
                'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
                'database': os.environ['SNOWFLAKE_DATABASE'],
                'schema': os.environ['SNOWFLAKE_SCHEMA']
            }
            with snowflake.connector.connect(**snowflake_config) as conn:
                with conn.cursor() as cur:
                    insert_data_into_snowflake(cur, table_name, df)
            print("Data load complete.")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")

# API URL and table name
api_url = "https://run.mocky.io/v3/f5f303a5-cebe-4ebc-bc4a-05a59870bb59"
table_name = "APPLICATION_LOG_DATA"

if __name__ == "__main__":
    load_data_to_snowflake(api_url, table_name)
