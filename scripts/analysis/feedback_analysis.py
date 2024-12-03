import os, time, pandas as pd, nltk, snowflake.connector
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from concurrent.futures import ThreadPoolExecutor

# Initialize libraries and connections
nltk.download("vader_lexicon")

# Snowflake connection details
conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema=os.environ['SNOWFLAKE_SCHEMA']
)

print("✅ - Connected to Snowflake.")
cursor = conn.cursor()

# Step 1: Fetch source table schema
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'CUSTOMER_FEEDBACK'
    ORDER BY ORDINAL_POSITION
""")
source_schema = cursor.fetchall()

# Define new columns
new_columns = [
    ("SENTIMENT_SCORE", "FLOAT"),
    ("SENTIMENT_CATEGORY", "VARCHAR(50)"),
    ("CUSTOMER_SATISFACTION_INDEX", "FLOAT"),
]

# Combine source and new columns in correct order
full_schema = source_schema + new_columns

# Step 2: Dynamically create the target table with the correct column order
create_table_query = f"""
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK_ANALYSIS (
    {', '.join([f"{col[0]} {col[1]}" for col in full_schema])}
)
"""
cursor.execute(create_table_query)

# Step 3: Fetch data from source table
start_time = time.time()
query = """
    SELECT * FROM CUSTOMER_FEEDBACK
"""
df = cursor.execute(query).fetch_pandas_all()
print(f"✅ - Fetched {len(df)} records in {time.time() - start_time:.2f}s.")

# Initialize sentiment analysis model
analyzer = SentimentIntensityAnalyzer()

# Step 4: Perform sentiment and satisfaction analysis with adjusted thresholds
def analyze(row):
    sentiment = analyzer.polarity_scores(row["PRODUCT_REVIEW_TEXT"])
    sentiment_score = sentiment["compound"]
    
    # Adjust the sentiment threshold for better categorization
    if sentiment_score >= 0.1:
        sentiment_category = "Positive"
    elif sentiment_score <= -0.1:
        sentiment_category = "Negative"
    else:
        sentiment_category = "Neutral"
    
    csi = ((sentiment_score + 1) / 2) * 50 + (row["CUSTOMER_SUPPORT_RATING"] / 5) * 50
    return sentiment_score, sentiment_category, csi

print("⚡ - Starting sentiment and satisfaction index calculation...")
start_time = time.time()
with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(analyze, df.to_dict("records")))

df[["SENTIMENT_SCORE", "SENTIMENT_CATEGORY", "CUSTOMER_SATISFACTION_INDEX"]] = pd.DataFrame(results)

# Rescale sentiment score for accuracy calculation
df["SENTIMENT_SCORE_SCALED"] = (df["SENTIMENT_SCORE"] + 1) * 2.5  # [-1,1] → [1,5]

# Step 5: Insert data into Snowflake
insert_columns = [col[0] for col in full_schema]
data_records = df[insert_columns].values.tolist()

insert_query = f"""
    INSERT INTO CUSTOMER_FEEDBACK_ANALYSIS (
        {', '.join(insert_columns)}
    )
    VALUES ({', '.join(['%s'] * len(insert_columns))})
"""
chunk_size = 1000
for i in range(0, len(data_records), chunk_size):
    cursor.executemany(insert_query, data_records[i:i + chunk_size])
conn.commit()
print("✅ - Data inserted successfully.")

# Close connections
cursor.close()
conn.close()
print("✅ - Process complete.")
