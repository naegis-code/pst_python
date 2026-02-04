import pandas as pd
from sqlalchemy import create_engine, text
import db_connect
from sqlalchemy.exc import SQLAlchemyError

# Settings
employee_code = '20353085'
end = '2026-12-31'

table = 'empdate'

# DB engine
engine = create_engine(db_connect.db_url_pstdb)

# Read specific employee data
query = text(f"""
    SELECT employee_code, sub_hub AS hub, position, first_work_date 
    FROM employees 
    WHERE last_work_date > '{end}'
        or last_work_date IS NULL
""")

query_result  = text(f"""
    SELECT * 
    FROM {table}
    WHERE last_work_date > '{end}' AND employee_code = '{employee_code}'
""")

df_emp = pd.read_sql_query(query, con=engine)
df_emp_existing = pd.read_sql_query(query_result, con=engine)


# Get start date
default_start = '2026-01-01'
start = df_emp['first_work_date'].iloc[0] if not df_emp['first_work_date'].isna().iloc[0] else default_start

# Generate date range
date_range = pd.date_range(start=start, end=end)

# Create DataFrame with employee_code and date
df = pd.DataFrame({
    'employee_code': employee_code,
    'date': date_range
})

# Merge hub and position
df = df.merge(df_emp[['employee_code', 'hub', 'position']], on='employee_code', how='left')

# Convert date to string
df['date'] = df['date'].astype(str)

df = df.merge(df_emp_existing[['employee_code','date', 'hub', 'position']], on=['employee_code', 'date'], how='left', suffixes=('', '_existing'))

# Check for existing records
df = df[df['hub_existing'].isna()]
df = df.drop(columns=['hub_existing', 'position_existing'])

# Insert into database
try:
    df.to_sql(table, engine, if_exists='append', index=False)
    print(f"Inserted {len(df)} rows into {table}.")
except SQLAlchemyError as e:
    print(f"Error inserting into {table}: {e}")
