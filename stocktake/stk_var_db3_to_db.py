import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
from tqdm import tqdm
from datetime import datetime

date_start = '20250101'
date_end = '20251231'

# def stocktake
def stocktake_upfront_db3_to_db(date_start,date_end, bu):
    table = 'stk'
    chunk_size = 10000

    # --- เชื่อมต่อ DB ปลายทาง ---
    db = create_engine(db_connect.db_url_pstdb)

    # --- ดึงรายการที่มีอยู่แล้วในปลายทาง ---
    q_db = text(f"""
        SELECT DISTINCT
            stmerch, cntdate, skutype, rpname, 'check' as recheck
        FROM {bu}_{table}
        WHERE cntdate between '{date_start}' and '{date_end}'
    """)
    df_db = pd.read_sql(q_db, db)

    try:
        # --- เชื่อมต่อ DB3 (ต้นทาง) ---
        db3 = create_engine(db_connect.db_url_pstdb3)

        q_db3 = f"""
            SELECT 
                cntnum, stmerch, cntdate, deptcode, deptname, subdeptcode, subdeptname,
                sku, sbc, ibc, bndname, prname, prmodel,
                soh, cntqnt, varianceqnt,
                extphycnt_retail, extphycnt_cost,
                extphy_retailvar, extphy_costvar,
                skutype, rpname 
            FROM {bu}_{table}_this_year
            WHERE cntdate between '{date_start}' and '{date_end}'
        """

        # --- อ่านข้อมูลจาก DB3 แบบ chunk ---
        chunks = pd.read_sql(q_db3, db3, chunksize=chunk_size)

        total_inserted = 0
        for chunk in tqdm(chunks, desc=f"Processing {bu}", unit="chunk"):

            # --- ทำ Anti-join เพื่อตรวจสอบข้อมูลที่ยังไม่มีในปลายทาง ---
            chunk = chunk.merge(df_db, on=['stmerch', 'cntdate', 'skutype', 'rpname'], how='left')
            chunk = chunk[chunk['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

            if not chunk.empty:
                chunk.to_sql(
                    f"{bu}_{table}",
                    db,
                    if_exists='append',
                    index=False
                )
                total_inserted += len(chunk)

        print(f"✅ {bu}: Inserted {total_inserted} rows into {bu}_{table} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"❌ Error connecting or inserting for BU {bu}: {e}")
def stocktake_jda_db3_to_db(date_start,date_end,bu):
    
    table = 'stk'
    chunk_size = 10000

    # --- เชื่อมต่อ DB ปลายทาง ---
    db = create_engine(db_connect.db_url_pstdb)

    # --- ดึงรายการที่มีอยู่แล้วในปลายทาง ---
    q_db = text(f"""
                SELECT DISTINCT
                store, cntdate, skutype, rpname, 'check' as recheck
                FROM {bu}_{table}
                WHERE cntdate between '{date_start}' and '{date_end}'
                """)
    df_db = pd.read_sql(q_db, db)

    try:
        # --- เชื่อมต่อ DB3 (ต้นทาง) ---
        db3 = create_engine(db_connect.db_url_pstdb3)

        q_db3 = text(f"""
                     SELECT 
                     store,countname,sku,ibc,sbc,brandid,sku_des,vendor,vend_nam,dpt,sdpt,cls,
                     scls,retail,cost,soh,qty_count,qty_var,phycnt_rtl,phycnt_cst,extrtl_var,extcst_var,skutype,rpname,cntdate
                     FROM {bu}_{table}_this_year
                     WHERE cntdate between '{date_start}' and '{date_end}'
                     """)

        # --- อ่านข้อมูลจาก DB3 แบบ chunk ---
        chunks = pd.read_sql(q_db3, db3, chunksize=chunk_size)

        total_inserted = 0
        for chunk in tqdm(chunks, desc=f"Processing {bu}", unit="chunk"):

            # --- ทำ Anti-join เพื่อตรวจสอบข้อมูลที่ยังไม่มีในปลายทาง ---
            chunk = chunk.merge(df_db, on=['store', 'cntdate', 'skutype', 'rpname'], how='left')
            chunk = chunk[chunk['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

            if not chunk.empty:
                chunk.to_sql(
                    f"{bu}_{table}",
                    db,
                    if_exists='append',
                    index=False
                )
                total_inserted += len(chunk)

        print(f"✅ {bu}: Inserted {total_inserted} rows into {bu}_{table} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"❌ Error connecting or inserting for BU {bu}: {e}")
def stocktake_cfr_db3_to_db(date_start,date_end,bu):
    table = 'stk'
    chunk_size = 10000

    # --- เชื่อมต่อ DB ปลายทาง ---
    db = create_engine(db_connect.db_url_pstdb)

    # --- ดึงรายการที่มีอยู่แล้วในปลายทาง ---
    q_db = text(f"""
                SELECT DISTINCT
                stcode, cntdate, skutype, rpname, 'check' as recheck
                FROM {bu}_{table}
                WHERE cntdate between '{date_start}' and '{date_end}'
                """)
    df_db = pd.read_sql(q_db, db)

    try:
        # --- เชื่อมต่อ DB3 (ต้นทาง) ---
        db3 = create_engine(db_connect.db_url_pstdb3)

        q_db3 = text(f"""
                        SELECT 
                        *
                        FROM {bu}_{table}_this_year
                        WHERE cntdate between '{date_start}' and '{date_end}'
                        """)

        # --- อ่านข้อมูลจาก DB3 แบบ chunk ---
        chunks = pd.read_sql(q_db3, db3, chunksize=chunk_size)

        total_inserted = 0
        for chunk in tqdm(chunks, desc=f"Processing {bu}", unit="chunk"):

            # --- ทำ Anti-join เพื่อตรวจสอบข้อมูลที่ยังไม่มีในปลายทาง ---
            chunk = chunk.merge(df_db, on=['stcode', 'cntdate', 'skutype', 'rpname'], how='left')
            chunk = chunk[chunk['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

            if not chunk.empty:
                chunk.to_sql(
                    f"{bu}_{table}",
                    db,
                    if_exists='append',
                    index=False
                )
                total_inserted += len(chunk)

        print(f"✅ {bu}: Inserted {total_inserted} rows into {bu}_{table} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"❌ Error connecting or inserting for BU {bu}: {e}")

# def variance
def variance_upfront_db3_to_db(date_start,date_end, bu):
    table = 'var'
    chunk_size = 10000

    # --- เชื่อมต่อ DB ปลายทาง ---
    db = create_engine(db_connect.db_url_pstdb)

    # --- ดึงรายการที่มีอยู่แล้วในปลายทาง ---
    q_db = text(f"""
        SELECT DISTINCT
            cntnum, prtype , rpname, 'check' as recheck
        FROM {bu}_{table}
        WHERE concat('20', "left"("right"(cntnum, 5), 2), "left"("right"(cntnum, 7), 2), "left"("right"(cntnum, 9), 2)) between '{date_start}' and '{date_end}'
    """)
    df_db = pd.read_sql(q_db, db)

    try:
        # --- เชื่อมต่อ DB3 (ต้นทาง) ---
        db3 = create_engine(db_connect.db_url_pstdb3)

        q_db3 = f"""
            SELECT 
                rpname,cntnum,deptcode,deptname,location,skcode,baribc,barsbc1,barsbc2,prname,bndname,model,color,soh,variance,cntqnt,prtype
            FROM {bu}_{table}_this_year
            WHERE concat('20', "left"("right"(cntnum, 5), 2), "left"("right"(cntnum, 7), 2), "left"("right"(cntnum, 9), 2)) between '{date_start}' and '{date_end}'
        """

        # --- อ่านข้อมูลจาก DB3 แบบ chunk ---
        chunks = pd.read_sql(q_db3, db3, chunksize=chunk_size)

        total_inserted = 0
        for chunk in tqdm(chunks, desc=f"Processing {bu}", unit="chunk"):

            # --- ทำ Anti-join เพื่อตรวจสอบข้อมูลที่ยังไม่มีในปลายทาง ---
            chunk = chunk.merge(df_db, on=['cntnum', 'prtype', 'rpname'], how='left')
            chunk = chunk[chunk['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

            if not chunk.empty:
                chunk.to_sql(
                    f"{bu}_{table}",
                    db,
                    if_exists='append',
                    index=False
                )
                total_inserted += len(chunk)

        print(f"✅ {bu}: Inserted {total_inserted} rows into {bu}_{table} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"❌ Error connecting or inserting for BU {bu}: {e}")
def variance_jda_db3_to_db(date_start,date_end,bu):
    
    table = 'var'
    chunk_size = 10000

    # --- เชื่อมต่อ DB ปลายทาง ---
    db = create_engine(db_connect.db_url_pstdb)

    # --- ดึงรายการที่มีอยู่แล้วในปลายทาง ---
    q_db = text(f"""
                SELECT DISTINCT
                countname, store, skutype , rpname, 'check' as recheck
                FROM {bu}_{table}
                WHERE cntdate between '{date_start}' and '{date_end}'
                """)
    df_db = pd.read_sql(q_db, db)

    try:
        # --- เชื่อมต่อ DB3 (ต้นทาง) ---
        db3 = create_engine(db_connect.db_url_pstdb3)

        q_db3 = text(f"""
                     SELECT 
                     countname,store,batch,dept,sdept,class,sclass,sku,ibc,sbc,sku_desc,brndname,brnddesc,catalogue,color,size,retail,cost,phycnt_rtl,phycnt_cst,qty_count,count_user,cntdate,rpname,skutype
                     FROM {bu}_{table}_this_year
                     WHERE cntdate between '{date_start}' and '{date_end}'
                     """)

        # --- อ่านข้อมูลจาก DB3 แบบ chunk ---
        chunks = pd.read_sql(q_db3, db3, chunksize=chunk_size)

        total_inserted = 0
        for chunk in tqdm(chunks, desc=f"Processing {bu}", unit="chunk"):

            # --- ทำ Anti-join เพื่อตรวจสอบข้อมูลที่ยังไม่มีในปลายทาง ---
            chunk = chunk.merge(df_db, on=['countname', 'store','skutype', 'rpname'], how='left')
            chunk = chunk[chunk['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

            if not chunk.empty:
                chunk.to_sql(
                    f"{bu}_{table}",
                    db,
                    if_exists='append',
                    index=False
                )
                total_inserted += len(chunk)

        print(f"✅ {bu}: Inserted {total_inserted} rows into {bu}_{table} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"❌ Error connecting or inserting for BU {bu}: {e}")

'''
stocktake_upfront_db3_to_db(date_start,date_end,'cfw')
stocktake_upfront_db3_to_db(date_start,date_end,'pwb')
stocktake_upfront_db3_to_db(date_start,date_end,'chg')

stocktake_jda_db3_to_db(date_start,date_end,'b2s')
stocktake_jda_db3_to_db(date_start,date_end,'ofm')

stocktake_jda_db3_to_db(date_start,date_end,'ssp')

stocktake_cfr_db3_to_db(date_start,date_end,'cfr')
'''

variance_upfront_db3_to_db(date_start,date_end,'cfw')
variance_upfront_db3_to_db(date_start,date_end,'pwb')
variance_upfront_db3_to_db(date_start,date_end,'chg')

variance_jda_db3_to_db(date_start,date_end,'b2s')
variance_jda_db3_to_db(date_start,date_end,'ofm')
variance_jda_db3_to_db(date_start,date_end,'ssp')