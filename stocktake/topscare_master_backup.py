import pandas as pd
from sqlalchemy import create_engine, text
import db_connect
import tqdm

clear_year = '2025'

engine_db = create_engine(db_connect.db_url_pstdb3)

q_topscare_master = text(f"""
                         select
                            stcode,
                            cntnum,
                            barcode,
                            sku,
                            description,
                            pack,
                            qnt,
                            price,
                            location,
                            sdate,
                            stime
                         from topscare_master where sdate like '%/{clear_year}'
                         """)

df_topscare_master = pd.read_sql(q_topscare_master, engine_db)

chunk_size = 1000

insert_query = text("""
    INSERT INTO topscare_master_backup
    (stcode, cntnum, barcode, sku, description, pack, qnt, price, location, sdate, stime)
    VALUES
    (:stcode, :cntnum, :barcode, :sku, :description, :pack, :qnt, :price, :location, :sdate, :stime)
""")

records = df_topscare_master.to_dict(orient="records")

with engine_db.begin() as conn:
    for i in tqdm.tqdm(range(0, len(records), chunk_size)):
        chunk = records[i:i + chunk_size]
        conn.execute(insert_query, chunk)
