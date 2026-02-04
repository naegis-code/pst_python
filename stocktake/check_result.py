import pandas as pd
from sqlalchemy import create_engine, text
import db_connect


connect_db = create_engine(db_connect.db_url_pstdb)
query_plan = text("select * from planall2")
df_plan = pd.read_sql(query_plan, connect_db)

# ระบุคอลัมน์ที่ต้องการเก็บไว้
keep = {'bu', 'stcode', 'branch', 'shub', 'type1', 'cntdate'}

# เลือกเฉพาะคอลัมน์ที่มีอยู่ใน DataFrame
df_plan = df_plan[[col for col in keep if col in df_plan.columns]]

print(df_plan)






