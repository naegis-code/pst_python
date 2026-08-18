import pandas as pd
from sqlalchemy import create_engine,text
import datetime
import numpy as np

user = ''
password = ''
date_start_manual = '20260101'
date_end_manual = '20261231'

print("Start : ",datetime.datetime.now())

date_start_auto = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y%m%d')
date_end_auto = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')

db = create_engine(f'postgresql+psycopg2://{user}:{password}@localhost:5432/pstdb')
db3 = create_engine(f'postgresql+psycopg2://{user}:{password}@localhost:5432/pstdb3')

date_start = date_start_manual
date_end = date_end_manual

print(f"Processing Missrate Store Data from {date_start} to {date_end}")

bu = 'b2s'
table_type = 'stk'
table_bu = f'{bu.lower()}_{table_type.lower()}_this_year'
table_missrate = 'missrate_store'

query_perparation = text(f"""update b2s_stk_this_year set bu = 'B2S',stcode = store where bu is null""")
with db3.begin() as conn:
    conn.execute(query_perparation)
    print("Data preparation completed successfully.")

query = text(f"""
            with rec as (
            select bu,
                stcode ,
                cntdate ,
                sku ,
                sum(qty_count) as qty_count 
            from {bu.lower()}_reconcile_this_year bsrty 
            group by bu,stcode ,cntdate ,sku 
            ), bef_fvf as (
            select svty.bu,
                svty.stcode ,
                svty.cntdate ,
                svty.sku ,
                sum(case when svty.rpname = '{table_type.upper()}1' then svty.qty_count else 0 end) as first,
                sum(case when svty.rpname = '{table_type.upper()}2' then svty.qty_count else 0 end) as final
            from {bu.lower()}_{table_type.lower()}_this_year svty 
            left join {bu.lower()}_block_this_year bsbty 
                on svty.bu = bsbty.bu
                and svty.stcode = bsbty.stcode 
                and svty.cntdate = bsbty.cntdate 
                and svty.sku = bsbty.sku
            where (svty.cntdate between '{date_start}' and '{date_end}')
                and bsbty.sku is null
            group by svty.bu,svty.stcode,svty.cntdate ,svty.sku
            )
            select bf.bu ,
                bf.stcode ,
                to_date(bf.cntdate,'YYYYMMDD') as cntdate,
                bf.sku,
                bf.first,
                bf.final,
                rec.qty_count as rec
            from bef_fvf bf
            left join rec
                on bf.bu = rec.bu
                and bf.stcode = rec.stcode 
                and bf.cntdate = rec.cntdate 
                and bf.sku = rec.sku""")

# Read data from database
df = pd.read_sql(query, db3)

# ป้องกัน Error จากค่าว่างก่อนคำนวณ
df[['first', 'final', 'rec']] = df[['first', 'final', 'rec']].fillna(0)

# คำนวณ missrate ตามเงื่อนไขที่กำหนด
df['missrate'] = np.where(
    df['first'] < df['final'],          # เงื่อนไข
    (df['first'] + df['rec'] - df['final']).abs(),   # ถ้า True (first < final)
    (df['first'] - df['final']).abs()  # ถ้า False (first >= final)
)
df = df.drop(columns=['rec'])

df_grouped = df.groupby(['stcode', 'cntdate']).agg({
    'first': 'sum',
    'missrate': 'sum'
    }).reset_index()

df_grouped['vendor'] = 'PST'
df_grouped['bu'] = bu.upper()

print(f"Data processing completed successfully with {len(df_grouped)} records.")
print(df_grouped)

try:
    with db.begin() as conn:
        # Delete old records for the specified date range and business unit
        delete_query = text(f"""
            DELETE FROM {table_missrate}
            WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
                AND bu = '{bu.upper()}'
        """)
        conn.execute(delete_query)
        print(f"Old records deleted for {bu.upper()} between {date_start} and {date_end}.")

    df_grouped.to_sql(table_missrate, db, if_exists='append', index=False)

    print(df_grouped)
    print(f"Data inserted into {len(df_grouped)} rows successfully.")

except Exception as e:
    print(f"Error occurred while deleting old records: {e}")

print("End : ",datetime.datetime.now())

