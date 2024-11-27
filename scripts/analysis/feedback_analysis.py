import os
import snowflake.connector
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from datetime import datetime
from sklearn.metrics import accuracy_score

# 🌟 Connect to Snowflake
snowflake_config = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

# Create Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_config['user'],
    password=snowflake_config['password'],
    account=snowflake_config['account'],
    warehouse=snowflake_config['warehouse'],
    database=snowflake_config['database'],
    schema=snowflake_config['schema']
)
print("✅ - Successfully connected to Snowflake.")

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# 🔍 Query to get customer_feedback table (focused on required columns)
query = """
    SELECT customer_name, customer_email, customer_gender, customer_loyalty_program,
           product_category, product_sub_category, product_name, product_rating, product_review_comments,
           order_id, order_status, purchase_mode, payment_mode, discount_applied,
           avg_product_rating, total_orders, unique_customers, avg_sentiment_score, 
           positive_feedback_count, negative_feedback_count, neutral_feedback_count, 
           follow_up_required, feedback_date, feedback_category, feedback_sub_category, 
           sentiment, customer_sentiment_score, customer_support_rating, resolution_status
    FROM customer_feedback
"""
cursor.execute(query)
df = cursor.fetch_pandas_all()

# 🧠 Initialize Sentiment Analyzer (VADER)
analyzer = SentimentIntensityAnalyzer()

# 💬 Intent Classification using Hugging Face BART zero-shot classification
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# Function to perform sentiment analysis 🎯
def get_sentiment_score(review_text):
    sentiment = analyzer.polarity_scores(review_text)
    return sentiment['compound']  # Returns the compound sentiment score

# 💡 Calculate Customer Satisfaction Index (CSI) in percentage
def calculate_csi(sentiment_score, support_rating):
    sentiment_index = ((sentiment_score + 1) / 2) * 50  # Scaling sentiment score to percentage
    support_index = (support_rating / 5) * 50  # Scaling support rating to percentage
    csi = sentiment_index + support_index
    return csi

# 🌈 Analyzing each row and calculating predictions
results = []
for index, row in df.iterrows():
    review_text = row['product_review_comments']
    
    # Sentiment analysis prediction
    sentiment_score = get_sentiment_score(review_text)
    
    # Classify sentiment
    if sentiment_score >= 0.5:
        sentiment_category = 'Positive'
    elif sentiment_score <= -0.5:
        sentiment_category = 'Negative'
    else:
        sentiment_category = 'Neutral'
    
    # Calculate Customer Satisfaction Index (CSI)
    csi = calculate_csi(sentiment_score, row['customer_support_rating'])
    
    # Dynamically append all the columns from the source data plus analysis results
    analysis_row = row.to_dict()  # Convert row to dictionary
    analysis_row.update({
        'sentiment_score': sentiment_score,
        'sentiment_category': sentiment_category,
        'customer_satisfaction_index': csi
    })
    results.append(analysis_row)

# 🌐 Convert results to DataFrame
analysis_df = pd.DataFrame(results)

# 🛠 Create the customer_feedback_analysis table in Snowflake dynamically
create_table_query = """
    CREATE OR REPLACE TABLE customer_feedback_analysis (
        customer_name STRING,
        customer_email STRING,
        customer_gender STRING,
        customer_loyalty_program STRING,
        product_category STRING,
        product_sub_category STRING,
        product_name STRING,
        product_rating INT,
        product_review_comments STRING,
        order_id STRING,
        order_status STRING,
        purchase_mode STRING,
        payment_mode STRING,
        discount_applied STRING,
        avg_product_rating FLOAT,
        total_orders INT,
        unique_customers INT,
        avg_sentiment_score FLOAT,
        positive_feedback_count INT,
        negative_feedback_count INT,
        neutral_feedback_count INT,
        follow_up_required STRING,
        feedback_date DATE,
        feedback_category STRING,
        feedback_sub_category STRING,
        sentiment STRING,
        customer_sentiment_score FLOAT,
        customer_support_rating INT,
        resolution_status STRING,
        sentiment_score FLOAT,
        sentiment_category STRING,
        customer_satisfaction_index FLOAT
    )
"""
cursor.execute(create_table_query)

# 💾 Insert the analysis data into the customer_feedback_analysis table
for index, row in analysis_df.iterrows():
    insert_query = f"""
        INSERT INTO customer_feedback_analysis (
            customer_name, customer_email, customer_gender, customer_loyalty_program,
            product_category, product_sub_category, product_name, product_rating, product_review_comments,
            order_id, order_status, purchase_mode, payment_mode, discount_applied,
            avg_product_rating, total_orders, unique_customers, avg_sentiment_score, 
            positive_feedback_count, negative_feedback_count, neutral_feedback_count, 
            follow_up_required, feedback_date, feedback_category, feedback_sub_category, 
            sentiment, customer_sentiment_score, customer_support_rating, resolution_status,
            sentiment_score, sentiment_category, customer_satisfaction_index
        ) VALUES (
            '{row['customer_name']}', '{row['customer_email']}', '{row['customer_gender']}', '{row['customer_loyalty_program']}',
            '{row['product_category']}', '{row['product_sub_category']}', '{row['product_name']}', {row['product_rating']}, '{row['product_review_comments']}',
            '{row['order_id']}', '{row['order_status']}', '{row['purchase_mode']}', '{row['payment_mode']}', '{row['discount_applied']}',
            {row['avg_product_rating']}, {row['total_orders']}, {row['unique_customers']}, {row['avg_sentiment_score']}, 
            {row['positive_feedback_count']}, {row['negative_feedback_count']}, {row['neutral_feedback_count']}, 
            '{row['follow_up_required']}', '{row['feedback_date']}', '{row['feedback_category']}', '{row['feedback_sub_category']}',
            '{row['sentiment']}', {row['customer_sentiment_score']}, {row['customer_support_rating']}, '{row['resolution_status']}',
            {row['sentiment_score']}, '{row['sentiment_category']}', {row['customer_satisfaction_index']}
        )
    """
    cursor.execute(insert_query)

# 💾 Commit and close the connection
conn.commit()
cursor.close()
conn.close()

# Calculate Accuracy
sentiment_accuracy = accuracy_score(true_sentiment, sentiment_predictions)
intent_accuracy = accuracy_score(true_intent, intent_predictions)

print(f"✅ Sentiment Accuracy: {sentiment_accuracy * 100:.2f}%")
print(f"✅ Intent Accuracy: {intent_accuracy * 100:.2f}%")
print("Customer Feedback Analysis table has been successfully created and populated! 🎉")