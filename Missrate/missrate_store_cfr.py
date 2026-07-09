import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import datetime

print("Start : ",datetime.datetime.now())

bu = 'cfr'
date_start_manual = '20260101'
date_end_manual = '20261231'
date_start_auto = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y%m%d')
date_end_auto = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')

date_start = date_start_manual
date_end = date_end_manual

print(f"Processing Missrate Store Data from {date_start} to {date_end}")


table_missrate = 'missrate_store'
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

df = pd.read_sql(query, engine)

try:
    with engine.begin() as conn:
        # Delete old records for the specified date range and business unit
        delete_query = text(f"""
            DELETE FROM {table_missrate}
            WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
                AND bu = '{bu.upper()}'
        """)
        conn.execute(delete_query)
        print(f"Old records deleted for {bu.upper()} between {date_start} and {date_end}.")

    df.to_sql(table_missrate, engine, if_exists='append', index=False)

    print(df)
    print(f"Data inserted into {len(df)} rows successfully.")

except Exception as e:
    print(f"Error occurred while deleting old records: {e}")

print("End : ",datetime.datetime.now())

