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
query2 = "select *,row_number() over(partition by bu,stcode  order by cntdate desc) as running from planall2 where atype = '3F'"
df2 = pd.read_sql(query2, connection)
connection.close()
print("Dataframe loaded from SQL database.")
df.to_csv(path_export, index=False, encoding='utf-8-sig')
df2.to_csv(files_path.filepath / 'Apps' / 'planall2_3F.csv', index=False, encoding='utf-8-sig')
print(f"Dataframe exported to {path_export}")