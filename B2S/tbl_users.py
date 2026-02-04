import pandas as pd
from sqlalchemy import create_engine, text
import os
import db_connect
import sys


path_sqllite = r'D:\Program Files\B2S\Apps\pda-master.db'
# Create a connection to the SQLite database
engine_sqlite = create_engine(f'sqlite:///{path_sqllite}')

# Delete existing 'Outsource' users to avoid duplicates
query_sqllite_delete_default = f"""Delete
    from users
    where "position" not in('Admin','Outsource');"""

# Execute the query to delete existing 'Outsource' users
with engine_sqlite.connect() as conn:
    conn.execute(text(query_sqllite_delete_default))
    conn.commit()

# Add new users from the main database
connect_db = create_engine(db_connect.db_url_pstdb)
query_db = f"""select 
    employee_code as username
    ,email 
    ,encryptedpassword as "encryptedPassword"
    ,employee_code as empCode
    ,split_part(eng_name  , ' ', 1) AS "firstName"
    ,split_part(eng_name , ' ', 2) AS "lastName"
    ,first_name as "firstNameTh"
    ,last_name as "lastNameTh"
    ,sub_hub as hub
    ,"position" 
from employees
where job_status is null"""



df_user = pd.read_sql(query_db, connect_db)


df_user.to_sql('users', engine_sqlite, if_exists='append', index=False)

print("User table updated successfully.")