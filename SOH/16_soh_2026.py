import pandas as pd
from sqlalchemy import create_engine,text
import os
import db_connect

engine = create_engine(db_connect.db_url_pstdb)
connection = engine.connect()
query_delete = text("""DELETE FROM soh_update WHERE "DATE" = '20260101';""")

try:
    with engine.begin() as connection:
        connection.execute(query_delete)
        print("✅ Deleted existing data for DATE = '20260101'")
except Exception as e:
    print(f"❌ Error deleting existing data: {e}")

query_insert = text("""with maxcode as (
select code 
	,bu
	,stcode
	,max("DATE") 
from soh_update su 
where "DATE" >= '20250101'
group by code ,bu,stcode
)
select s.code ,
	s.bu ,
	s.stcode ,
	'20260101' as "DATE",
	s.food_credit ,
	s.nonfood_consign ,
	s.perishable_nonmer ,
	s.totalsoh
from maxcode m
left join soh_update s on m.code = s.code and m.max = s."DATE"
left join (select distinct bu,stcode,branch from plan2026) p on m.bu = p.bu and m.stcode = p.stcode
where p.branch is not null""")

try:
    df_insert = pd.read_sql(query_insert, con=engine)
    df_insert.to_sql('soh_update', con=engine, if_exists='append', index=False)
    print("✅ Inserted new data for DATE = '20260101'")
except Exception as e:
    print(f"❌ Error inserting new data: {e}")

