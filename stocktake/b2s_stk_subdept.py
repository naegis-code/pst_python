import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib

user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

path_export = filepath / 'Apps' /'b2s_stk_analysis_sdept.csv'

date_start_manual = '20260101'
date_end_manual = '20260430'
bu = 'B2S'
rpname = 'STK2'


print(f"Processing B2S Stocktake Data from {date_start_manual} to {date_end_manual}")


db = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)


query_db3 = text("""select skutype
                        ,dpt 
                        ,sdpt 
                        ,sum(soh*"cost") as vsoh
                        ,sum(case when extcst_var > 0 then extcst_var else 0 end) as vgain
                        ,sum(case when extcst_var < 0 then extcst_var else 0 end) as vloss
                    from b2s_stk_this_year b
                    where cntdate between :date_start_manual and :date_end_manual and rpname = :rpname
                    group by skutype,dpt ,sdpt """)
df3 = pd.read_sql_query(query_db3, db3,params={"date_start_manual": date_start_manual, "date_end_manual": date_end_manual, "rpname": rpname})

df3['bu'] = bu

query_dept = text("""select 
                        bu,
                        dept,
                        dept_name,
                        sub_dept as sdpt,
                        sub_dept_name
                    from master_dept
                    where bu = :bu""")
df_dept = pd.read_sql_query(query_dept, db, params={"bu": bu.upper()})

df = df3.merge(df_dept, on=['bu','sdpt'], how='left')

print(f"Number of rows in the merged dataframe: {len(df)}")

df.to_csv(path_export, index=False)
print(f"Analysis exported to {path_export}")



