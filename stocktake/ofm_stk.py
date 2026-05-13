import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib

user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'



date_start_manual = '20260101'
date_end_manual = '20260430'
bu = 'OFM'
rpname = 'STK2'
path_export = filepath / 'Apps' / f'{bu.lower()}_stk_analysis.csv'

date_last_year = str(pd.to_datetime(date_start_manual).year - 1) + '1231'
print(f"Processing {bu} Stocktake Data from {date_start_manual} to {date_end_manual}")
print(f"Comparing with last year's data from {date_last_year} for analysis.")

db = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)

query_perparation = text(f"""update ofm_stk_this_year set bu = '{bu.upper()}',stcode = store where bu is null""")
with db3.begin() as conn:
    conn.execute(query_perparation)
    print("Data preparation completed successfully.")

query_db3 = text("""select b.stcode
                        ,b.cntdate
                        ,b.skutype
                        ,sum(soh*"cost") as vsoh
                        ,sum(case when extcst_var > 0 then extcst_var else 0 end) as vgain
                        ,sum(case when extcst_var < 0 then extcst_var else 0 end) as vloss
                        ,sum(extcst_var) as vnet
                        ,sum(case when extcst_var > 0 then extcst_var else 0 end)-
                            sum(case when extcst_var < 0 then extcst_var else 0 end) as vabs
                        ,1-(sum(case when extcst_var > 0 then extcst_var else 0 end)-
                            sum(case when extcst_var < 0 then extcst_var else 0 end))/sum(soh*"cost") as acc
                    from ofm_stk_this_year b
                    where b.cntdate between :date_start_manual and :date_end_manual and b.rpname = :rpname
                    group by b.stcode ,b.cntdate ,b.skutype 
                    """)
df3 = pd.read_sql_query(query_db3, db3,params={"date_start_manual": date_start_manual, "date_end_manual": date_end_manual, "rpname": rpname})


query_db_last_year = text("""select b.stcode
                        ,b.cntdate
                        ,b.skutype
                        ,sum(soh*"cost") as vsoh
                        ,sum(case when extcst_var > 0 then extcst_var else 0 end) as vgain
                        ,sum(case when extcst_var < 0 then extcst_var else 0 end) as vloss
                        ,sum(extcst_var) as vnet
                        ,sum(case when extcst_var > 0 then extcst_var else 0 end)-
                            sum(case when extcst_var < 0 then extcst_var else 0 end) as vabs
                        ,1-(sum(case when extcst_var > 0 then extcst_var else 0 end)-
                            sum(case when extcst_var < 0 then extcst_var else 0 end))/sum(soh*"cost") as acc
                    from ofm_stk b
                    where b.cntdate <= :date_last_year and b.rpname = :rpname
                    group by b.stcode ,b.cntdate ,b.skutype""")
df_last_year = pd.read_sql_query(query_db_last_year, db,params={"date_last_year": date_last_year, "rpname": rpname})


df = pd.concat([df3, df_last_year], ignore_index=True)

df['bu'] = bu

query_plan = text("""select bu
                  ,stcode 
                  ,cntdate 
                  ,branch 
                  ,type1 
                  ,"size" 
                  ,row_number()over(partition by bu,stcode order by cntdate desc) as running
                  from planall2 p 
                  where atype = '3F'""")
df_plan = pd.read_sql_query(query_plan, db)

df = df.merge(df_plan, on=['bu','stcode','cntdate'], how='left')

print(f"Number of rows in the merged dataframe: {len(df)}")

df.to_csv(path_export, index=False)
print(f"Analysis exported to {path_export}")