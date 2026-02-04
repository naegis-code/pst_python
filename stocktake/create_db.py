from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import pandas as pd
from datetime import datetime
import db_connect

# Constants
path_db = Path('D:/Program Files/pooledstocktake/b2s/Apps/pda-master.db')
path_import = Path('C:/TEXTFILE/Book1.xlsx')
path_import_sheet = 'Sheet1'
bu = 'B2S'
atype = 'F'
stcode = '50100'
cntdate_pick = '2025-07-10'
cntdate = datetime.strptime(cntdate_pick, '%Y-%m-%d').strftime('%d%m%y')

username = 'prthanapat'
password = '20020015'

# Step 1: Get store name from main DB
engine = create_engine(db_connect.db_url_pstdb)
query = f"""SELECT stname_th FROM plan_dba WHERE stcode = '{stcode}' AND bu = '{bu}';"""
df_query = pd.read_sql_query(text(query), engine)
store_name = df_query['stname_th'].iloc[0]

# Step 2: Get current run count from pstdb3
db_url_3 = f'postgresql+psycopg2://{username}:{password}@103.22.182.82:5432/pstdb3'
engine_3 = create_engine(db_url_3)
query_3 = f"""SELECT COUNT(cntnum) AS run FROM stocktakeid 
              WHERE cntnum LIKE '{bu+stcode+atype+cntdate}%'
              GROUP BY cntnum;"""
df_query_3 = pd.read_sql_query(text(query_3), engine_3)

if df_query_3.empty:
    cntnum = bu + stcode + atype + cntdate + '001'
else:
    cntnum = bu + stcode + atype + cntdate + str(int(df_query_3['run'].iloc[0]) + 1).zfill(3)

# Step 3: Update SQLite using SQLAlchemy
db_url_sqlite = f'sqlite:///{path_db}'
engine_sqlite = create_engine(db_url_sqlite)

try:
    with engine_sqlite.begin() as connection:
        query_delete = text("delete FROM pda_masters;")
        query_delete_2 = text("delete FROM users where id > 1;")
        connection.execute(query_delete)
        connection.execute(query_delete_2)
        print("Old records deleted successfully from pda_masters and users.")
except SQLAlchemyError as e:
    print(f"An error occurred while deleting from pda_masters: {e}")
    raise


created_on_str = datetime.strptime(cntdate_pick, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')
updated_on_str = created_on_str

query_update_cntnum = f"""
    UPDATE stocktakes
    SET countName = :cntnum,
        storeCode = :stcode,
        bu = :bu,
        storeName = :store_name,
        branch = :store_name,
        createdON = :created_on,
        updatedOn = :updated_on;
"""

params = {
    "cntnum": cntnum,
    "stcode": stcode,
    "bu": bu,
    "store_name": store_name,
    "branch": store_name,
    "created_on": created_on_str,
    "updated_on": updated_on_str
}

df = pd.read_excel(path_import, sheet_name=path_import_sheet,dtype={'SKU': str, 'UPC': str, 'DES': str, 'PRICE': float, 'DEPT': str})

df['sku'] = df['UPC']
df['barcodeIBC'] = df['UPC']
df['stocktakeid'] = cntnum
df['storeCode'] = stcode
df['storeName'] = store_name
df['status'] = 'A'
df['createdOn'] = created_on_str
df['updatedOn'] = updated_on_str

df.drop(columns=['SKU', 'UPC'], inplace=True, errors='ignore')

df.rename(columns={
    'DES': 'productName',
    'PRICE': 'retailPrice',
    'DEPT': 'deptCode',
}, inplace=True)

df.columns = df.columns.str.lower()


df.to_sql('pda_masters', con=engine_sqlite, if_exists='append', index=False)

try:
    with engine_sqlite.begin() as connection:  # ensures auto-commit
        result = connection.execute(text(query_update_cntnum), params)
        print(f"Database updated successfully with cntnum: {cntnum}")
except SQLAlchemyError as e:
    print(f"An error occurred while updating the database: {e}")
    raise

try:
    with engine.begin() as connection:
        df_query_user = pd.read_sql_query(text("select employee_code ,encryptedpassword from employees union select employee_code ,encryptedpassword from employees_outsource;"), engine)
        df_query_user['username'] = df_query_user['employee_code']
        df_query_user.rename(columns={'employee_code': 'empCode'}, inplace=True)
        df_query_user.to_sql('users', con=engine_sqlite, if_exists='append', index=False)
except SQLAlchemyError as e:
    print(f"An error occurred while querying the database: {e}")
    raise
# Step 4: Compact (VACUUM) the SQLite database to reduce file size
try:
    with engine_sqlite.begin() as connection:
        connection.execute(text("VACUUM;"))
        print("Database compacted successfully.")
except SQLAlchemyError as e:
    print(f"An error occurred while compacting the database: {e}")
    raise

