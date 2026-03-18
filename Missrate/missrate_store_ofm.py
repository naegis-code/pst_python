import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import datetime

print("Start : ",datetime.datetime.now())


date_start_manual = '20260101'
date_end_manual = '20260228'
date_start_auto = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y%m%d')
date_end_auto = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')

db = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)

date_start = date_start_manual
date_end = date_end_manual

print(f"Processing Missrate Store Data from {date_start} to {date_end}")

bu = 'ofm'
table_bu = f'{bu.lower()}_var_this_year'
table_missrate = 'missrate_store'


query = text(f"""
            select store as stcode ,
                to_date(cntdate, 'YYYYMMDD') as cntdate ,
                batch ,
                sku ,
                sum(case when rpname = 'VAR1' then qty_count else 0 end) as first,
                sum(case when rpname = 'VAR2' then qty_count else 0 end) as final
            from {table_bu}
             where (cntdate BETWEEN '{date_start}' AND '{date_end}')
                and batch not like 'D%' and batch not like 'R%'
             group by store,cntdate ,batch ,sku

    """)



# Read data from database
df = pd.read_sql(query, db3)
df['missrate'] = (df['first'] - df['final']).abs()

df_grouped = df.groupby(['stcode', 'cntdate']).agg({
    'first': 'sum',
    'missrate': 'sum'
    }).reset_index()

df_grouped['vendor'] = 'PST'
df_grouped['bu'] = bu.upper()

query_old = text(f"""
            select bu,stcode ,cntdate
            from {table_missrate}
            where cntdate BETWEEN '{datetime.datetime.strptime(date_start, '%Y%m%d').date()}' AND '{datetime.datetime.strptime(date_end, '%Y%m%d').date()}'
                and bu = '{bu.upper()}'
    """)

# Read old data from database
df_old = pd.read_sql(query_old, db)
# Merge new data with old data
df_merged = pd.merge(df_grouped, df_old, on=['bu', 'stcode', 'cntdate'], how='left',indicator=True)
df_merged = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

df_merged.to_sql(table_missrate, db, if_exists='append', index=False)
print(f"Data inserted into {len(df_merged)} rows successfully.")

print("End : ",datetime.datetime.now())
