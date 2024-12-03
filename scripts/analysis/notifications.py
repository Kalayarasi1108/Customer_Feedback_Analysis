import os, requests
import pandas as pd
import snowflake.connector

# Fetch credentials and webhook URL (example placeholders)
WEBHOOK_URL = os.environ['WEBHOOK_URL']
WEBHOOK_URL_TRAINING = os.environ['WEBHOOK_URL_TRAINING']

# Connect to Snowflake (replace with your credentials)
conn = snowflake.connector.connect(
                'user': os.environ['SNOWFLAKE_USER'],
                'password': os.environ['SNOWFLAKE_PASSWORD'],
                'account': os.environ['SNOWFLAKE_ACCOUNT'],
                'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
                'database': os.environ['SNOWFLAKE_DATABASE'],
                'schema': os.environ['SNOWFLAKE_SCHEMA']
)

# Fetch the data from Snowflake (replace with your actual query)
query = """
    SELECT STAFF_EMAIL, CUSTOMER_SUPPORT_RATING AS FEEDBACK_SCORE, 
           CUSTOMER_EMAIL AS customer_email, RESOLUTION_STATUS AS COMPLAINT_STATUS,
           PRODUCT_ISSUE_TYPE
    FROM CUSTOMER_FEEDBACK_ANALYSIS
"""
df = pd.read_sql(query, conn)

# ------------------- Feedback Analysis -------------------
# Top 5 Staff with Low Feedback
low_feedback = df[df["FEEDBACK_SCORE"] < 2.5].sort_values(by="FEEDBACK_SCORE").head(5)
low_feedback_message = low_feedback[["STAFF_EMAIL", "FEEDBACK_SCORE"]].to_html(
    index=False, border=1, justify="center", header=True, classes="table table-bordered"
)

# ------------------- Customer Insights -------------------
# Recently Resolved and Unresolved Complaints
total_resolved = len(df[df["COMPLAINT_STATUS"] == "Resolved"])
total_unresolved = len(df[df["COMPLAINT_STATUS"] == "Unresolved"])

# Product Issue Type - Percentage Breakdown
product_issue_counts = df["PRODUCT_ISSUE_TYPE"].value_counts(normalize=True) * 100
product_issue_message = (
    product_issue_counts.to_frame()
    .reset_index()
    .to_html(
        index=False,
        border=1,
        justify="center",
        header=["Product Issue", "Percentage"],
        classes="table table-bordered",
    )
)

# ------------------- Staff Performance -------------------
# Worst Performers (Low Feedback)
worst_performers = low_feedback[["STAFF_EMAIL", "FEEDBACK_SCORE"]].sort_values(
    by="FEEDBACK_SCORE"
)
worst_performers_message = worst_performers.to_html(
    index=False, border=1, justify="center", header=True, classes="table table-bordered"
)

# Best Performers (High Feedback)
best_performers = (
    df[df["FEEDBACK_SCORE"] >= 4]
    .groupby("STAFF_EMAIL")["FEEDBACK_SCORE"]
    .mean()
    .sort_values(ascending=False)
    .head(5)
)
best_performers_message = best_performers.reset_index().to_html(
    index=False,
    border=1,
    justify="center",
    header=["Staff Name", "Avg. Feedback Score"],
    classes="table table-bordered",
)

# ------------------- Training Recommendations -------------------
# Identifying staff needing training based on low feedback scores
def get_training_recommendation(feedback_score):
    """Return training recommendation based on feedback score."""
    if feedback_score < 2.0:
        return "Product Knowledge, Customer Handling, Communication Skills"
    elif feedback_score < 2.5:
        return "Customer Handling, Communication Skills"
    elif feedback_score < 3.0:
        return "Communication Skills"
    else:
        return "No Training Required"


# Add training recommendations to the dataframe
low_feedback["Training_Recommendation"] = low_feedback["FEEDBACK_SCORE"].apply(
    get_training_recommendation
)

# Format the training recommendations into a table
training_recommendations_message = (
    low_feedback[["STAFF_EMAIL", "FEEDBACK_SCORE", "Training_Recommendation"]]
    .sort_values(by="FEEDBACK_SCORE")
    .to_html(
        index=False,
        border=1,
        justify="center",
        header=["Staff Name", "Feedback Score", "Training Recommendation"],
        classes="table table-bordered",
    )
)

# ------------------- Function to Send Notifications -------------------
def send_chunked_to_teams(webhook_url, message, chunk_size=3000):
    """Send the message in chunks if it's too long for Teams."""
    for i in range(0, len(message), chunk_size):
        chunk = message[i : i + chunk_size]
        payload = {"text": chunk}
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            print("✅ Notification sent to Teams.")
        else:
            print(
                f"❌ Failed to send notification to Teams: {response.status_code} - {response.text}"
            )


# ------------------- Prepare Messages -------------------
# Feedback Analysis Message
feedback_analysis_message = (
    f"📉 **Feedback Analysis**:\nTop 5 Staff with Low Feedback:\n{low_feedback_message}"
)

# Customer Insights Message
customer_insights_message = f"📊 **Customer Insights**:\nTotal Recently Resolved Complaints: {total_resolved}\nTotal Unresolved Complaints: {total_unresolved}\n{product_issue_message}"

# Staff Performance Message
staff_performance_message = f"🏅 **Staff Performance**:\n🔴 Worst Performers (Needs Investigation):\n{worst_performers_message}\n\n🟢 Best Performers (Appreciate):\n{best_performers_message}"

# Training Recommendations Message
training_message = f"**Training Recommendations:**\n{training_recommendations_message}"

# ------------------- Send Notifications -------------------
print("📢 Sending notifications to Teams in chunks...")
send_chunked_to_teams(WEBHOOK_URL, feedback_analysis_message)
send_chunked_to_teams(WEBHOOK_URL, customer_insights_message)
send_chunked_to_teams(WEBHOOK_URL, staff_performance_message)
send_chunked_to_teams(WEBHOOK_URL_TRAINING, training_message)

# Close the Snowflake connection
conn.close()