#  Python ETL Scripts

# Purpose: Build a complete ETL pipeline using Python

import pandas as pd
import sqlite3
import os

# Create data folder if it doesn't exist
if not os.path.exists("data"):
    os.makedirs("data")


# Step 1: Extract

# Sample CSV file
csv_file = "data/customers.csv"

# Create sample data if CSV does not exist
sample_data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Aravind", "Rahul", "Priya", None, "John"],
    "city": ["Coventry", "London", "Manchester", "London", "London"],
    "signup_date": ["2023-01-01", "2023-02-15", "2023-03-10", "2023-01-25", "2023-04-05"]
}

df = pd.DataFrame(sample_data)
df.to_csv(csv_file, index=False)

# Read CSV
df = pd.read_csv(csv_file)
print("Extracted Data:")
print(df)


# Step 2: Transform

# Fill missing names
df["name"] = df["name"].fillna("Unknown")


# Standardize city names
df["city"] = df["city"].str.title().str.strip()

# Convert signup_date to datetime
df["signup_date"] = pd.to_datetime(df["signup_date"])

# Add a new column: signup_year
df["signup_year"] = df["signup_date"].dt.year

print("\nTransformed Data:")
print(df)


# Step 3: Load

# Load into SQLite
conn = sqlite3.connect("data/customers_db.sqlite")
df.to_sql("customers", conn, if_exists="replace", index=False)

# Verify by reading from DB
df_db = pd.read_sql("SELECT * FROM customers", conn)
print("\nData Loaded into SQLite Database:")
print(df_db)

conn.close()
print("\nETL pipeline executed successfully!")
