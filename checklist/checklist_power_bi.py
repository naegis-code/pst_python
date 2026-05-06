import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import pathlib

userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

save_path = filepath / 'Apps' / 'checklist_power_bi.csv'

filter_count_date = '20240101'

engine_db = create_engine(db_connect.db_url_pstdb)

q_checklist = text("""
                   SELECT 
                   bu,
                   stcode,
                   cntdate,
                   point,
                   zone,
                   weight,
                   no,
                   section,
                   subject,
                   subdescription,
                   "full",
                   act
                   FROM checklist
                   where cntdate >= :filter_count_date
                   """)

df_checklist = pd.read_sql(q_checklist, engine_db, params={'filter_count_date': filter_count_date})
#print(df_checklist.head())

# จาก planall2 เพิ่ม Type1 ,Branch,HUB,Type,Running

q_planall2 = text("""
                   SELECT bu, stcode, cntdate, branch, shub, type1, row_number() over (partition by bu, stcode order by cntdate desc) as running
                     FROM planall2
                     where cntdate >= :filter_count_date
                            and atype in ('3F','3Q')
                     """)
df_planall2 = pd.read_sql(q_planall2, engine_db, params={'filter_count_date': filter_count_date})


df = df_checklist.merge(df_planall2, on=['bu', 'stcode', 'cntdate'], how='left')
print(df)
df.to_csv(save_path, index=False)