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


# 2
#  Python ETL Scripts

**Topics Covered:**
- Build a complete ETL pipeline using Python
- Extract data from CSV
- Transform data (handle missing values, standardize city names, convert dates, add new columns)
- Load data into SQLite database
- Verify results

**Exercise:**
- Python ETL script (`day22_python_etl.py`) that:
  1. Reads `customers.csv`
  2. Cleans missing and inconsistent data
  3. Transforms data (adds signup_year column)
  4. Loads into SQLite database `customers_db.sqlite`
  5. Prints data at each step

**Skills Demonstrated:**
- Python ETL pipeline development
- Pandas for data cleaning and transformation
- SQLite integration for data storage
- Practical experience with Extract-Transform-Load workflow


# Sample Outputs
Extracted Data:
   id     name        city signup_date
0   1  Aravind    Coventry  2023-01-01
1   2    Rahul      London  2023-02-15
2   3    Priya  Manchester  2023-03-10
3   4      NaN      London  2023-01-25
4   5     John      London  2023-04-05

Transformed Data:
   id     name        city signup_date  signup_year
0   1  Aravind    Coventry  2023-01-01         2023
1   2    Rahul      London  2023-02-15         2023
2   3    Priya  Manchester  2023-03-10         2023
3   4  Unknown      London  2023-01-25         2023
4   5     John      London  2023-04-05         2023

Data Loaded into SQLite Database:
   id     name        city          signup_date  signup_year
0   1  Aravind    Coventry  2023-01-01 00:00:00         2023
1   2    Rahul      London  2023-02-15 00:00:00         2023
2   3    Priya  Manchester  2023-03-10 00:00:00         2023
3   4  Unknown      London  2023-01-25 00:00:00         2023
4   5     John      London  2023-04-05 00:00:00         2023

ETL pipeline executed successfully!




# 3 
# DAG 
# Airflow Introduction (Minimal ETL DAG)

**Goal:** Learn how to schedule and monitor ETL with Apache Airflow using a simple DAG.

**What’s in the DAG (`dags/etl_dag.py`):**
- **Extract:** Reads `data/customers.csv` (auto-creates a sample if missing).
- **Transform:** Fills missing names, title-cases city, parses `signup_date`, adds `signup_year`.
- **Load:** Writes to SQLite at `data/customers_db.sqlite` (table: `customers`).

**How to run:**
1. **Install Airflow** (example for Airflow 2.x):
   ```bash
   AIRFLOW_VERSION=2.7.1
   PYTHON_VERSION="$(python -V | awk '{print $2}' | cut -d. -f1,2)"
   CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
   pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
   pip install pandas

# 4
# Multiple_DAG_File

**Skills Demonstrated:**
- Multi-task Airflow DAG design
- Task dependencies (`>>` operator)
- Modular Python functions for reusability
- File-based data handoff between tasks
  


# 5
# Dynamic Airflow ETL Pipeline

**Goal:**  
Learn how to create dynamic DAGs in Airflow that automatically generate ETL tasks for multiple data sources.

**DAG ID:** `dynamic_etl_pipeline`

**Overview:**  
Instead of writing static tasks, this DAG loops over a list of dataset names (`customers`, `orders`, `products`) and creates individual ETL tasks dynamically.

**Task Pattern:**
extract_customers → transform_customers → load_customers
extract_orders → transform_orders → load_orders
extract_products → transform_products → load_products

**Skills Demonstrated:**
- Dynamic task creation using Python loops
- Parameterized PythonOperators (`op_args`)
- Managing multiple data sources efficiently
- Writing clean, scalable DAGs

# 6
#  Conditional Branching in Airflow ETL

**Goal:**  
Add intelligence to ETL workflows using Airflow’s `BranchPythonOperator`.  
This enables the DAG to decide automatically whether to proceed or skip ETL steps.

**DAG ID:** `conditional_branching_etl`

**Overview:**  
This DAG checks if the dataset file exists and meets the size requirement before running ETL.  
If the file is missing or too small, the DAG branches to a “skip” task instead of failing.

**Workflow:**
start → extract → check_file_condition
 |--transform → load → end
 |--skip_etl → end


**Features:**
- Conditional task flow using `BranchPythonOperator`
- Validation check for file existence and size
- Graceful handling of missing data (no failed DAGs)
- Clean, readable DAG graph

**Expected Output:**
- CSV file in `dags/data/`
- SQLite database `conditional_etl.db`
- Smart branching visible in Airflow’s Graph View

