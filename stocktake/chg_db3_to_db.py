import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import db_connect

bu = 'chg'
date_start = '20250101'
date_end = '20251231'
chunksize = 10000

db = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)

def var_to_db3(bu, date_start, date_end):
    table = 'var'
    keys = ['bu', 'stcode', 'cntdate', 'skutype', 'rpname']

    # ===== โหลด key จาก db (ปลายทาง) ครั้งเดียว =====
    q_db = f"""
        SELECT DISTINCT bu, stcode, cntdate, skutype, rpname
        FROM {bu}_{table}
        WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
    """
    df_db = pd.read_sql(q_db, db)
    target_index = set(
        df_db[keys].itertuples(index=False, name=None)
    )

    print(f'🔎 Existing keys : {len(target_index):,}')

    # ===== query ฝั่ง db3 (ต้นทาง) =====
    q_db3 = f"""
        SELECT *
        FROM {bu}_{table}_this_year
        WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
    """

    total_inserted = 0

    # ===== stream ทีละ chunk =====
    for chunk in tqdm(
        pd.read_sql(q_db3, db3, chunksize=chunksize),
        desc='📥 Read & Insert',
        unit='chunk'
    ):
        # anti-join ทีละ chunk
        chunk_keys = list(
            chunk[keys].itertuples(index=False, name=None)
        )
        mask = [k not in target_index for k in chunk_keys]
        df_new = chunk.loc[mask]

        if not df_new.empty:
            df_new.to_sql(
                f'{bu}_{table}',
                db,
                if_exists='append',
                index=False,
                method='multi'
            )
            total_inserted += len(df_new)

            # update key set กัน insert ซ้ำ
            target_index.update(
                df_new[keys].itertuples(index=False, name=None)
            )

    print(f'✅ Insert completed : {total_inserted:,} rows')

var_to_db3(bu, date_start, date_end)