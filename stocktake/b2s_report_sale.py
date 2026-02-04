import os
import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib
import datetime
from tqdm import tqdm


# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

table = 'b2s_sale_this_year'

folder_path = filepath / 'Shared' / 'Performance' / 'report' / 'report3' / 'B2S'

# List all Excel files in the folder
excel_files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
print(f"{len(excel_files)} Files found. ",pd.Timestamp.now())

# Create an empty list to store the dataframes
dataframes = []

# Loop through the list of files and read each one into a dataframe
for file in tqdm(excel_files, desc="Processing files", unit="file"):
    file_path = os.path.join(folder_path, file)

    tqdm.write(f"Processing file: {file} {pd.Timestamp.now()}")
    df = pd.read_excel(file_path, sheet_name='Sale', usecols='A:I', dtype={'MDSTOR2': str})
    
    # Extract parts of the filename to create new columns (cols1, cols2, cols3, cols4)
    file_parts = os.path.splitext(file)[0].split('_')
    
    # Ensure the filename has enough parts
    if len(file_parts) == 5:
        cols1, cols2, cols3, cols4, cols5 = file_parts
    else:
        # Handle cases where the filename format is unexpected
        cols1, cols2, cols3, cols4, cols5 = None, None, None, None, None
    
    # Add new columns to the dataframe
    df['cntdate'] = cols4
    df = df[df['MDSTOR2'] == cols3]

    # Append the dataframe to the list
    dataframes.append(df)

# Combine all dataframes into one
df = pd.concat(dataframes, ignore_index=True)

# Rename columns
columns_rename = {
            'MDSTOR2': 'mdstor',
            'STORE NAME': 'mdstrn',
            'MDDEPT': 'mddept',
            'DEPT NAME': 'mddptn',
            'MDSDPT': 'mdsdpt',
            'SDPT NAME': 'mdsdpn',
            'Credit': 'credit',
            'Consign': 'consignment',
            'ผลรวมทั้งหมด': 'grand_total'
        }


df = df.rename(columns=columns_rename)
df.columns = df.columns.str.lower()

print("Dataframes combined : ",len(df))

# PostgreSQL connection details
engine_db = create_engine(db_connect.db_url_pstdb)
engine_db3 = create_engine(db_connect.db_url_pstdb3)
query_plan = text(f"SELECT stcode as mdstor, cntdate, branch FROM planall where bu = 'B2S' and atype = '3F'")
query_result = text(f"SELECT mdstor ,cntdate ,'check' as recheck FROM {table} ")

# Execute the query and load the result into a dataframe
with engine_db.connect() as connection:
    plan_result = pd.read_sql(query_plan, connection)

# Merge the dataframes to get branch information
df = df.merge(plan_result, on=['mdstor','cntdate'], how='left')
df = df[df['branch'].notna()].drop(columns=['branch'])

print("Plan merged : ",len(df))

# Execute the query and load the result into a dataframe
with engine_db3.connect() as connection:
    db3_result = pd.read_sql(query_result, connection)

# Merge the dataframes to find existing records
df = df.merge(db3_result, on=['mdstor', 'cntdate'], how='left')

print("Result merged : ",len(df))

# Filter out existing records
df = df[df['recheck'].isna()].drop(columns=['recheck']).drop_duplicates()


print("Final dataframe ready to insert : ",len(df))
# Insert the final dataframe into the PostgreSQL table

df.to_sql(table, engine_db3, if_exists='append', index=False)

print("Process completed at ",pd.Timestamp.now())