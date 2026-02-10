import pandas as pd
from sqlalchemy import create_engine,text
from tqdm import tqdm
import db_connect

chunksize = 10000

db  = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)

def var_to_db3(bu, date_start, date_end):
    table = 'var'
    keys = ['bu', 'stcode', 'cntdate', 'skutype', 'rpname']

    # ===== 1. โหลด key จาก db ปลายทาง (เล็กกว่า) =====
    q_db = f"""
        SELECT DISTINCT bu, stcode, cntdate, skutype, rpname
        FROM {bu}_{table}
        WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
    """
    df_db = pd.read_sql(q_db, db)
    target_keys = set(df_db[keys].itertuples(index=False, name=None))

    print(f'🔑 existing keys : {len(target_keys):,}')

    # ===== 2. stream จาก db3 ทีละ chunk =====
    q_db3 = f"""
        SELECT *
        FROM {bu}_{table}_this_year
        WHERE cntdate BETWEEN '{date_start}' AND '{date_end}'
    """

    inserted = 0

    for chunk in tqdm(
        pd.read_sql(q_db3, db3, chunksize=chunksize),
        desc='📥 Reading db3',
        unit='chunk'
    ):
        # ===== 3. anti-join ต่อ chunk =====
        chunk_keys = chunk[keys].itertuples(index=False, name=None)
        mask = [k not in target_keys for k in chunk_keys]
        df_new = chunk.loc[mask]

        # ===== 4. insert ต่อ chunk =====
        if not df_new.empty:
            df_new.to_sql(
                f'{bu}_{table}',
                db,
                if_exists='append',
                index=False,
                method='multi'
            )
            inserted += len(df_new)

            # ป้องกัน insert ซ้ำใน chunk ถัดไป
            target_keys.update(
                df_new[keys].itertuples(index=False, name=None)
            )

    print(f'✅ Inserted : {inserted:,} rows')
