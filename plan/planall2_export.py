import pandas as pd
import sqlalchemy
import os
import datetime
import files_path
import db_connect


path_export = files_path.filepath / 'Apps' / 'planall2.csv'

query = "select * from planall2"
engine = sqlalchemy.create_engine(db_connect.db_url_pstdb)
connection = engine.connect()
df = pd.read_sql(query, connection)
query2 = """with variance as (
            select
                p.bu,
                p.stcode,
                p.cntdate,
                (select  max(to_date(v.cntdate, 'YYYYMMDD'))
                from planall2 v
                where v.bu = p.bu
                    and v.stcode = p.stcode
                    and v.atype = '4V'
                    and to_date(v.cntdate, 'YYYYMMDD') >= to_date(p.cntdate, 'YYYYMMDD')
                    and to_date(v.cntdate, 'YYYYMMDD') < coalesce((
                            select min(to_date(n.cntdate, 'YYYYMMDD'))
                            from planall2 n
                            where n.bu = p.bu
                            and n.stcode = p.stcode
                            and n.atype = '3F'
                            and to_date(n.cntdate, 'YYYYMMDD') > to_date(p.cntdate, 'YYYYMMDD')), '9999-12-31'::date)
                    ) as variance_date
                from planall2 p
                where p.atype = '3F'
            )
            select
                p.*,
                row_number() over (partition by p.bu, p.stcode order by to_date(p.cntdate,'YYYYMMDD') desc) as running,
                p2.total_soh,
                coalesce(v.variance_date - to_date(p.cntdate, 'YYYYMMDD'),0) as variance_day
            from planall2 p
            left join planall p2
                on p.bu = p2.bu
                and p.stcode = p2.stcode
                and p.cntdate = p2.cntdate
            left join variance v
                on p.bu = v.bu
            and p.stcode = v.stcode
            and p.cntdate = v.cntdate
            where p.atype = '3F'"""
df2 = pd.read_sql(query2, connection)
connection.close()
print("Dataframe loaded from SQL database.")
df.to_csv(path_export, index=False, encoding='utf-8-sig')
df2.to_csv(files_path.filepath / 'Apps' / 'planall2_3F.csv', index=False, encoding='utf-8-sig')
print(f"Dataframe exported successfully")