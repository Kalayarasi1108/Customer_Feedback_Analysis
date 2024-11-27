import pandas as pd

# Function to convert CSV to JSON
def csv_to_json(csv_file, json_file):
    try:
        # Read CSV file into a DataFrame with 'ISO-8859-1' encoding to handle special characters
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')  # 'ISO-8859-1' or 'latin1' can be used
        
        # Save the DataFrame to a JSON file (without nesting)
        df.to_json(json_file, orient='records', lines=False)
        print(f"CSV file has been successfully converted to JSON and saved to {json_file}")
    except Exception as e:
        print(f"Error: {e}")

# Specify the input CSV file and output JSON file
csv_file = 'scripts/data/raw_customer_feedback_data.csv'  # Path to your CSV file
json_file = 'scripts/data/raw_customer_feedback_data.json'  # Path to save the JSON file

# Call the function to convert CSV to JSON
csv_to_json(csv_file, json_file)
