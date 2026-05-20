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

save_path_checklist = filepath / 'Apps' / 'checklist_all.csv'
save_path_planall2 = filepath / 'Apps' / 'planall2_checklist.csv'

path = r"D:\Users\prthanap\OneDrive - Central Group\Apps\checklist_all.csv"

filter_count_date = '20240101'

engine_db = create_engine(db_connect.db_url_pstdb)
#'postgresql+psycopg2://prthanapat:20020015@103.22.182.82:5432/pstdb'

q_checklist = text("""
                   select c.stcode 
                        ,c.cntdate as check_date
                        ,c.question_code as "Attribute"
                        ,c.point as "Value"
                        ,c.bu as "BU"
                        ,p.type1 as "Type1"
                        ,c.zone as "ZONE"
                        ,c.weight as "Weight"
                        ,c."section" as "Subject"
                        ,c.subject as "Description"
                        ,c.subdescription as "SubDesctiption"
                        ,c."full" as "Full"
                        ,c.act as "Act"
                    from checklist c 
                    left join planall2 p
                        on c.bu = p.bu and c.stcode = p.stcode and c.cntdate = p.cntdate 
                    where p.atype in ('3F','3Q')
                        and c.checkdate >= :filter_count_date
                        and p.branch is not null
                   """)

df_checklist = pd.read_sql(q_checklist, engine_db, params={'filter_count_date': filter_count_date})

q_planall2 = text("""
                    select p.*
                        ,row_number() over (partition by bu, stcode order by cntdate desc) as running
                    from planall2 p 
                    where atype in('3F')
                        and cntdate >= :filter_count_date
                     """)
df_planall2 = pd.read_sql(q_planall2, engine_db, params={'filter_count_date': filter_count_date})


df_checklist.to_csv(save_path_checklist, index=False)
print(f"✅ Checklist data saved to {save_path_checklist} successfully. Total rows: {len(df_checklist)}")

df_planall2.to_csv(save_path_planall2, index=False)
print(f"✅ Planall2 data saved to {save_path_planall2} successfully. Total rows: {len(df_planall2)}")