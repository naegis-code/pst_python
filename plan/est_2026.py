import pandas as pd
from sqlalchemy import create_engine,text
import pathlib
import db_connect

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

filename = 'Annual Plan 2026 All Update (By Div).xlsx'

table_name = 'est2026'

path = filepath / 'Report/2026/99 Plan' / filename

df = pd.read_excel(path,sheet_name='estman',usecols="f:j")

column_mapping = {
    'EMPCODE':'empcode',
    'DATE':'date',
    'ACTIVITIES':'activities',
    'SHUB':'shub',
    'Position':'position'
}
df.rename(columns=column_mapping,inplace=True)
# Filter rows where the time part of the 'date' column is not 00:00:00
df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Ensure 'date' column is in datetime format
df.dropna(subset=['date'], inplace=True)  # Drop rows where 'date' is NaT
df = df[df['empcode'] != 0] # Drop rows where 'empcode' equals 0

# Create the engine to connect to PostgreSQL
engine = create_engine(db_connect.db_url_pstdb)

# Execute the delete query to clear the 'employees' table
with engine.connect() as connection:
    trans = connection.begin()  # Start a transaction
    try:
        connection.execute(text(f"DELETE FROM {table_name}"))
        trans.commit()  # Commit the transaction
        print(f"Existing data in '{table_name}' table deleted.")
    except:
        trans.rollback()  # Rollback the transaction if there's an error
        print("An error occurred. Transaction rolled back.")

# Insert the DataFrame into the 'employees' table
df.to_sql(table_name, engine, if_exists='append', index=False)

print(f"Data successfully inserted into '{table_name}'.")