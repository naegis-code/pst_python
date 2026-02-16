import pandas as pd
from sqlalchemy import create_engine,text
import db_connect

engine = create_engine(db_connect.db_url_pstdb)
    
# Query blocked vendors for 'all' sdpt
query_block_all = text("""
                    select veno,sdpt from soh_b2s_block_veno where sdpt = 'all'
                   """
)
df_block_all = pd.read_sql(query_block_all, engine)
print(df_block_all.info())