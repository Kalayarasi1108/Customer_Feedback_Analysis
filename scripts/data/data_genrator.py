import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker object
fake = Faker()

# Product categories and sub-categories for daily-use products
product_categories = ["Household Items", "Groceries", "Kitchen Appliances", "Personal Care", "Stationery"]
product_sub_categories = {
    "Household Items": ["Cleaning Supplies", "Furniture", "Bedding"],
    "Groceries": ["Fruits", "Vegetables", "Snacks", "Beverages"],
    "Kitchen Appliances": ["Microwaves", "Toasters", "Blenders", "Rice Cookers"],
    "Personal Care": ["Skincare", "Hair Care", "Oral Care"],
    "Stationery": ["Notebooks", "Pens", "Pencils", "Paper"]
}

# Staff details (for simplicity, generating a few random staff names and emails)
staff_data = {
    "staff_name": [fake.name() for _ in range(10)],
    "staff_email": [fake.email() for _ in range(10)]
}

staff_df = pd.DataFrame(staff_data)

# Generate 1000 rows of synthetic customer feedback data
data = {
    # Customer Information
    "customer_name": [fake.name() for _ in range(500)],
    "customer_email": [fake.email() for _ in range(500)],
    "customer_gender": [random.choice(["Male", "Female", "Other"]) for _ in range(500)],
    "customer_age_group": [random.choice(["18-24", "25-34", "35-44", "45-54", "55+"]) for _ in range(500)],
    "customer_loyalty_program": [random.choice(["Yes", "No"]) for _ in range(500)],

    # Product Details
    "product_category": [random.choice(product_categories) for _ in range(500)],
    "product_sub_category": [random.choice(product_sub_categories[random.choice(product_categories)]) for _ in range(500)],
    "product_name": [fake.bs() for _ in range(500)],
    "product_rating": [random.randint(1, 5) for _ in range(500)],
    "product_review_text": [fake.text(max_nb_chars=200) for _ in range(500)],
    "product_return_status": [random.choice(["Yes", "No"]) for _ in range(500)],
    "product_issue_type": [random.choice(["Defective", "Missing Parts", "Not as Described", "Other"]) for _ in range(500)],

    # Order Information
    "order_id": [fake.uuid4() for _ in range(500)],
    "order_status": [random.choice(["Confirmed", "Not Confirmed"]) for _ in range(500)],
    "purchase_mode": [random.choice(["Online", "Offline"]) for _ in range(500)],
    "payment_mode": [random.choice(["Cash", "Card", "UPI"]) for _ in range(500)],
    "discount_applied": [random.choice(["Yes", "No"]) for _ in range(500)],
    "store_location": [random.choice(["New York", "Los Angeles", "Chicago", "Mumbai", "London"]) for _ in range(500)],
    "delivery_status": [random.choice(["Delivered", "Pending", "Shipped"]) for _ in range(500)],
    "order_fulfillment_time": [random.randint(1, 7) for _ in range(500)],
    "response_time": [random.randint(1, 24) for _ in range(500)],  # in hours
    "follow_up_required": [random.choice(["Yes", "No"]) for _ in range(500)],

    # Feedback and Sentiment Analysis
    "feedback_date": [fake.date_this_year() for _ in range(500)],
    "feedback_category": [random.choice(["Quality", "Service", "Price", "Delivery", "Customer Support", "Usability"]) for _ in range(500)],
    "feedback_sub_category": [random.choice(["Excellent", "Good", "Average", "Poor"]) for _ in range(500)],
    "sentiment": [random.choice(["Positive", "Negative", "Neutral"]) for _ in range(500)],
    "customer_sentiment_score": [random.uniform(-1, 1) for _ in range(500)],
    "feedback_text_length": [len(fake.text(max_nb_chars=200)) for _ in range(500)],
    "customer_support_rating": [random.randint(1, 5) for _ in range(500)],
    "resolution_status": [random.choice(["Resolved", "Not Resolved"]) for _ in range(500)],

    # Staff Information (Assign staff to complaints)
    "staff_name": [random.choice(staff_df['staff_name']) for _ in range(500)],
    "staff_email": [random.choice(staff_df['staff_email']) for _ in range(500)],
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save to a CSV file
df.to_csv('generated_customer_feedback_data.csv', index=False)

print("Data has been generated and saved as 'generated_customer_feedback_data.csv'")
