import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib

year = '2026'

# Database connection pstdb
db_url = db_connect.db_url_pstdb
engine = create_engine(db_url)
query = text(f'SELECT * FROM soh_update_{year}_check_week_1')

# Load data
df = pd.read_sql(query, engine)

# Drop unnecessary columns
df = df.drop(columns=['week_number'], errors='ignore')

# Add missing columns with default values
df['bu'] = df['code'].str[:3]
df['stcode'] = df['code'].str[3:]
df['DATE'] = f'{year}0101'
df['food_credit'] = 0
df['nonfood_consign'] = 0
df['perishable_nonmer'] = 0
df['totalsoh'] = 0

if df.empty:
    print("❌No data to upload.")
else:
    # upload to database
    df.to_sql('soh_update', engine, if_exists='append', index=False)

    print(f"✅Data uploaded successfully: ",{len(df)}," records.")