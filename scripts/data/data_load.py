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
    columns = [col.upper() for col in df.columns]  # Convert columns to uppercase to match Snowflake's default behavior
    print(columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    cur.executemany(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})", data_tuples)
    print(f"Successfully inserted {len(data_tuples)} records into {table_name}.")

def load_data_to_snowflake(api_url, table_name):
    df = fetch_data_from_api(api_url)
    if df is not None and not df.empty:
        try:
            snowflake_config = {
                'user': 'Krishna',
                'password': 'Parthi@4120',
                'account': 'mw22458.central-india.azure',
                'warehouse': 'COMPUTE_WH',
                'database': 'BLOG',
                'schema': 'FEEDBACK_ANALYSIS'
            }
            with snowflake.connector.connect(**snowflake_config) as conn:
                with conn.cursor() as cur:
                    insert_data_into_snowflake(cur, table_name, df)
            print("Data load complete.")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")

# API URL and table name
api_url = "https://run.mocky.io/v3/fa12b212-cb25-4c74-93be-0dfe943d32a9"
table_name = "APPLICATION_LOG_DATA"

if __name__ == "__main__":
    load_data_to_snowflake(api_url, table_name)
