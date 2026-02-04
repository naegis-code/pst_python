from sqlalchemy import create_engine,text
import pandas as pd
import psycopg2


db_url_pstdb2 = 'postgresql+psycopg2://prthanapat:20020015@103.22.182.82:5432/pstdb2'
db_url_pstdb = 'postgresql+psycopg2://prthanapat:20020015@103.22.182.82:5432/pstdb'

query_db = """SELECT * FROM soh_update WHERE "DATE" >= '20250101'"""
query_db2 = """SELECT * FROM soh_update_v2 WHERE data_date >= '20250101'"""

# Database connection
engine_pstdb2 = create_engine(db_url_pstdb2)
engine_pstdb = create_engine(db_url_pstdb)

# Load data
df_db2 = pd.read_sql(query_db2, engine_pstdb2)
df_db = pd.read_sql(query_db, engine_pstdb)

# Merge with left join
df_merged = df_db2.merge(
    df_db, 
    left_on=["code", "data_date"], 
    right_on=["code", "DATE"], 
    how="left", 
    indicator=True
)

# Filter where db.code is null (i.e., unmatched in db)
df_result = df_merged[df_merged["_merge"] == "left_only"][["code", "data_date", "totalsoh"]]

# Rename columns
df_result.rename(columns={"data_date": "DATE"}, inplace=True)

# Output result
print(df_result)