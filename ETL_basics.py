#  ETL Basics

# Purpose: Demonstrate basic ETL pipeline in Python

import pandas as pd
import sqlite3
import os

# Create folder for data if it doesn't exist
if not os.path.exists("data"):
    os.makedirs("data")


# Step 1: Extract

# Create sample CSV
csv_file = "data/sales_data.csv"
sample_data = {
    "order_id": [1, 2, 3, 4, 5],
    "customer": ["Aravind", "Rahul", "Priya", "Sita", "John"],
    "quantity": [2, 5, 1, 3, 4],
    "price": [100, 50, 200, 150, 80],
    "order_date": ["2023-01-01", "2023-02-01", "2023-01-15", "2023-03-05", "2023-03-20"]
}

df = pd.DataFrame(sample_data)
df.to_csv(csv_file, index=False)

# Read the CSV (Extract)
df = pd.read_csv(csv_file)
print("Extracted Data:")
print(df)


# Step 2: Transform

# Calculate total sales
df['total_sales'] = df['quantity'] * df['price']

# Convert order_date to datetime
df['order_date'] = pd.to_datetime(df['order_date'])

# Remove duplicates if any
df.drop_duplicates(inplace=True)

print("\nTransformed Data:")
print(df)


# Step 3: Load

# Load data into SQLite
conn = sqlite3.connect("data/sales_db.sqlite")
df.to_sql("sales", conn, if_exists="replace", index=False)

# Verify by reading from DB
df_db = pd.read_sql("SELECT * FROM sales", conn)
print("\nData Loaded into SQLite Database:")
print(df_db)

conn.close()
print("\nETL pipeline executed successfully!")
