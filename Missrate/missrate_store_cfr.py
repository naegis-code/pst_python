import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import datetime

print("Start : ",datetime.datetime.now())

date_start_manual = '20260101'
date_end_manual = '20260131'
date_start_auto = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y%m%d')
date_end_auto = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')

date_start = date_start_manual
date_end = date_end_manual

print(f"Processing Missrate Store Data from {date_start} to {date_end}")

table = 'missrate_store'
table_view = 'missrate_job_view_2'

engine = create_engine(db_connect.db_url_pstdb)

query = text(f"""
    SELECT bu,stcode,cntdate,'PST' as vendor,pst_scan as first,pst_miss as missrate
    FROM {table_view}
    WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
        and pst_scan > 0
    union all
    SELECT bu,stcode,cntdate,outsource_name as vendor,ost_scan as first,ost_miss as missrate
    FROM {table_view}
    WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
        and ost_scan > 0;
    """)

query_missrate_store = text(f"""
    SELECT bu,stcode,cntdate,vendor
    FROM {table}
    WHERE cntdate BETWEEN to_date('{date_start}', 'YYYYMMDD') AND to_date('{date_end}', 'YYYYMMDD');
    """)


# Read data from database
df_query = pd.read_sql(query, engine)

df_missrate_store = pd.read_sql(query_missrate_store, engine)

df_query = df_query.merge(df_missrate_store, on=['bu','stcode','cntdate','vendor'], how='left', indicator=True)
df_query = df_query[df_query['_merge'] == 'left_only'].drop(columns=['_merge'])

# test print
print(df_query)

# Insert data to table
df_query.to_sql(table, engine, if_exists='append', index=False)

print(f"Data: {len(df_query)} record", table,"inserted successfully.")

print("End : ",datetime.datetime.now())

