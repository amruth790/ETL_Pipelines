# ETL_Pipelines

# 1
# ETL Basics

**Topics Covered:**
- Introduction to ETL (Extract, Transform, Load)
- Extracting data from CSV
- Transforming data with Python (calculate new columns, convert types, remove duplicates)
- Loading data into a SQLite database
- Verifying data after loading

**Exercise:**
- Build a simple Python ETL pipeline:
  1. Extract data from CSV (`sales_data.csv`)
  2. Transform data (calculate `total_sales`, convert dates)
  3. Load data into SQLite (`sales` table)
  4. Print data at each step for verification

**Skills Demonstrated:**
- Understanding ETL concepts
- Python data manipulation with Pandas
- SQLite database integration
- Building a basic data pipeline

# Expected Output

Extracted Data:
   order_id customer  quantity  price  order_date
0         1  Aravind         2    100  2023-01-01
1         2    Rahul         5     50  2023-02-01
2         3    Priya         1    200  2023-01-15
3         4     Sita         3    150  2023-03-05
4         5     John         4     80  2023-03-20

Transformed Data:
   order_id customer  quantity  price order_date  total_sales
0         1  Aravind         2    100 2023-01-01          200
1         2    Rahul         5     50 2023-02-01          250
2         3    Priya         1    200 2023-01-15          200
3         4     Sita         3    150 2023-03-05          450
4         5     John         4     80 2023-03-20          320

Data Loaded into SQLite Database:
   order_id customer  quantity  price           order_date  total_sales
0         1  Aravind         2    100  2023-01-01 00:00:00          200
1         2    Rahul         5     50  2023-02-01 00:00:00          250
2         3    Priya         1    200  2023-01-15 00:00:00          200
3         4     Sita         3    150  2023-03-05 00:00:00          450
4         5     John         4     80  2023-03-20 00:00:00          320

ETL pipeline executed successfully!
