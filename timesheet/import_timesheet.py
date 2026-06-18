import pandas as pd
from sqlalchemy import create_engine, text

# Paths to the Excel file
path1 = r'D:\Users\prthanap\Central Group\PST Performance Team - เอกสาร\Apps\time_sheet_excel.xlsx'
path2 = r'C:\Users\Administrator\Central Group\PST Performance Team - Documents\Apps\time_sheet_excel.xlsx'

# Read the Excel file into a DataFrame
df = pd.read_excel(path2, sheet_name='Sheet1', usecols="A:N")

# Rename the columns based on the mapping
column_mapping = {
    'employeecode': 'empcode',
    'checkin_date': 'check_in_date',
    'checkin_hh': 'check_in_hour',
    'checkin_mm': 'check_in_minute',
    'checkout_date': 'check_out_date',
    'checkout_hh': 'check_out_hour',
    'checkout_mm': 'check_out_minute',
    'transportation_expenses': 'transport_expense',
    'timestamp': 'stamptime',
    'pda': 'pda_no'
}
df.rename(columns=column_mapping, inplace=True)

# Database connection string
db_url = 'postgresql+psycopg2://prthanapat:20020015@localhost:5432/pstdb'

# Create the engine to connect to PostgreSQL
engine = create_engine(db_url)

# Fetch the existing data from the stime_sheet table
query = "SELECT id FROM stime_sheet"

with engine.connect() as connection:
    result = connection.execute(text(query))
    dfsql = pd.DataFrame(result.fetchall(), columns=result.keys())

# Perform a left join to find rows in df that are not in dfsql
new_data = pd.merge(df, dfsql, on='id', how='left', indicator=True)
new_data = new_data[new_data['_merge'] == 'left_only'].drop(columns=['_merge'])

# Insert only the new rows into the stime_sheet table
if not new_data.empty:
    new_data.to_sql('stime_sheet', engine, if_exists='append', index=False)
    print("New data successfully inserted into 'stime_sheet'.")
else:
    print("No new data to insert.")