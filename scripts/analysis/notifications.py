import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import snowflake.connector

# Fetch credentials from GitHub Secrets
EMAIL_USER = os.getenv('EMAIL_USER')  # Sender email from GitHub Secrets
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')  # Sender email password from GitHub Secrets
OWNER_EMAIL = os.getenv('SHOP_OWNER_EMAIL')  # Shop owner's email from GitHub Secrets

# Email Configuration
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

# Function to send email
def send_email(subject, body, recipient_email):
    """Send an email notification."""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_USER, recipient_email, msg.as_string())
        server.quit()
        print(f"✅ Email sent to {recipient_email}")
    except Exception as e:
        print(f"❌ Failed to send email to {recipient_email}: {e}")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)
print("✅ Connected to Snowflake.")

# Query to fetch feedback data
query = """
    SELECT staff_email, feedback_score, customer_email, complaint_status
    FROM customer_feedback
"""
df = pd.read_sql(query, conn)

# Notify staff and owner for low feedback
def notify_bad_feedback(df):
    for _, row in df.iterrows():
        if row['feedback_score'] < 2.5:  # Low feedback threshold
            # Notify Staff
            staff_subject = "🔔 Low Feedback Alert"
            staff_body = (
                "You have received low feedback from a customer. "
                "Please review and take necessary actions.\n\nThanks!"
            )
            send_email(staff_subject, staff_body, row['staff_email'])

            # Notify Owner
            owner_subject = "🚨 Staff Performance Alert"
            owner_body = (
                f"Staff member {row['staff_email']} has consistently received low feedback. "
                f"Please look into this issue.\n\nThanks!"
            )
            send_email(owner_subject, owner_body, OWNER_EMAIL)

# Notify customers about resolved complaints
def notify_customer_complaints(df):
    for _, row in df.iterrows():
        if row['complaint_status'] == "Resolved":
            customer_subject = "✅ Update on Your Complaint"
            customer_body = (
                "Your complaint has been resolved. "
                "Thank you for your patience.\n\nThanks!"
            )
            send_email(customer_subject, customer_body, row['customer_email'])

# Execute Notifications
print("📢 Sending notifications...")
notify_bad_feedback(df)
notify_customer_complaints(df)
print("🎉 Notifications sent!")

# Close connection
conn.close()
