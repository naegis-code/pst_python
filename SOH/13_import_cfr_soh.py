import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from db_connect import db_url_pstdb, db_url_pstdb2
import os
import pathlib

# Set file path
user_path = pathlib.Path.home()

table = 'cfr_soh2'
table_soh_update = 'soh_update'
bu = 'CFR'

def load_and_process_data(path):
    sheets = pd.read_excel(path, sheet_name=['สาขา RMS V.12', 'สาขา RMS V.16'], skiprows=3, usecols="A:O", dtype=str, header=None)
    
    column_map = {
        1: 'stcode',
        7: 'food_credit',
        9: 'nonfood_consign',
        11: 'perishable_nonmer',
        13: 'totalsoh'
    }

    def process_sheet(df):
        df = df.copy()  # <--- Add this line
        df.columns = [column_map.get(i, i) for i in range(len(df.columns))]
        df['bu'] = 'CFR'
        df = df[['bu', 'stcode', 'food_credit', 'nonfood_consign', 'perishable_nonmer', 'totalsoh']]
        return df

    df1 = process_sheet(sheets['สาขา RMS V.12'])
    df2 = process_sheet(sheets['สาขา RMS V.16'])

    # Extract and format date
    date_info = pd.read_excel(path, sheet_name='สาขา RMS V.12', header=None, dtype=str, usecols="A", nrows=1)
    get_date = pd.to_datetime(date_info.iloc[0, 0].split()[-1], format='%d-%b-%Y').strftime('%Y%m%d')

    df1['date'] = get_date
    df2['date'] = get_date

    # Create 'code' column
    df1['code'] = df1['bu'] + df1['stcode']
    df2['code'] = df2['bu'] + df2['stcode']

    return pd.concat([df1, df2], ignore_index=True)



path = user_path / 'Downloads' / 'cfr_soh.xlsx'
df = load_and_process_data(path)
df = df.rename(columns={'date': 'data_date'})
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Print log message
print(f"Running cfr_soh.py at {timestamp} with {len(df)} rows")


# Create a connection to the database
from sqlalchemy.exc import SQLAlchemyError

# Use environment variables or a configuration file for sensitive information

try:
	engine = create_engine(db_url_pstdb2)
	df.to_sql('cfr_soh2', con=engine, if_exists='append', index=False)
	timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print(f"Data cfr_soh2 imported to database successfully at {timestamp}")
except FileNotFoundError:
	print("Error: The specified file was not found.")
except SQLAlchemyError as e:
	print(f"Database error occurred: {e}")
except Exception as e:
	print(f"An unexpected error occurred: {e}")
	
df.rename(columns={"data_date": "DATE"}, inplace=True)

try:
    engine = create_engine(db_url_pstdb)
    df.to_sql(table_soh_update, con=engine, if_exists='append', index=False)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Data imported to database successfully at {timestamp}")
    os.remove(path)
except SQLAlchemyError as e:
    print("❌ Failed to insert data into database.")
    print("Error:", e)
    print("⚠️ File was NOT deleted:", path)