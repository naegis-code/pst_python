import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
from tqdm import tqdm

bu = 'ofm'
chunksize = 1000

db = create_engine(db_connect.db_url_pstdb)
db3 = create_engine(db_connect.db_url_pstdb3)

def stk_to_db(bu, date_start, date_end):
    try:
        print(f'Processing BU: {bu}, Date Range: {date_start} to {date_end}')
        table = 'stk'
        # เตียมข้อมูลจาก db3
        q_db3 = text(f"""
        SELECT store, countname, sku, ibc, sbc, brandid, sku_des, vendor, vend_nam, dpt, 
                     sdpt, cls, scls, retail, cost, soh, qty_count, qty_var, phycnt_rtl, phycnt_cst,
                     extrtl_var, extcst_var, skutype, rpname, cntdate, stocktakeid, bu, stcode, username
        FROM {bu}_{table}_this_year
        where cntdate between '{date_start}' and '{date_end}'
        """)
        df_db3 = pd.read_sql(q_db3, db3)
        
        # เตียมข้อมูลจาก db
        q_db = f"""
        SELECT distinct bu,stcode,cntdate,skutype,rpname
        FROM {bu}_{table}
        WHERE cntdate between '{date_start}' and '{date_end}'
        """
        df_db = pd.read_sql(q_db, db)

        # Faster anti-join
        keys = ['bu', 'stcode', 'cntdate', 'skutype', 'rpname']
        mask = ~df_db3.set_index(keys).index.isin(df_db.set_index(keys).index)
        df = df_db3[mask].reset_index(drop=True)

        # ===== Insert with tqdm =====
        total = len(df)

        with tqdm(total=total, desc=f'🚀 Insert {table}', unit='rows') as pbar:
            for start in range(0, total, chunksize):
                end = start + chunksize
                df.iloc[start:end].to_sql(
                    f'{bu}_{table}',
                    db,
                    if_exists='append',
                    index=False,
                    method='multi'   # เร็วขึ้นมาก
                )
                pbar.update(end - start)

        print('✅ Insert completed')

        return
    except Exception as e:
        print(f'❌ Error: {e}')
        return


def var_to_db(bu, date_start, date_end):
    try:
        print(f'Processing BU: {bu}, Date Range: {date_start} to {date_end}')
        table = 'var'
        # เตียมข้อมูลจาก db3
        q_db3 = text(f"""
        SELECT countname,store,batch,dept,sdept
            ,class,sclass,sku,ibc,sbc
            ,sku_desc,brndname,brnddesc,catalogue,color
            ,size,retail,cost,phycnt_rtl,phycnt_cst
            ,qty_count,count_user,cntdate,rpname,skutype
            ,bu,stcode,username,stocktakeid
        FROM {bu}_{table}_this_year
        where cntdate between '{date_start}' and '{date_end}'
        """)
        df_db3 = pd.read_sql(q_db3, db3)
        
        # เตียมข้อมูลจาก db
        q_db = f"""
        SELECT distinct bu,stcode,cntdate,skutype,rpname
        FROM {bu}_{table}
        WHERE cntdate between '{date_start}' and '{date_end}'
        """
        df_db = pd.read_sql(q_db, db)

        # Faster anti-join
        keys = ['bu', 'stcode', 'cntdate', 'skutype', 'rpname']
        mask = ~df_db3.set_index(keys).index.isin(df_db.set_index(keys).index)
        df = df_db3[mask].reset_index(drop=True)

        # ===== Insert with tqdm =====
        total = len(df)

        with tqdm(total=total, desc=f'🚀 Insert {table}', unit='rows') as pbar:
            for start in range(0, total, chunksize):
                end = start + chunksize
                df.iloc[start:end].to_sql(
                    f'{bu}_{table}',
                    db,
                    if_exists='append',
                    index=False,
                    method='multi'   # เร็วขึ้นมาก
                )
                pbar.update(end - start)

        print('✅ Insert completed')

        return
    except Exception as e:
        print(f'❌ Error: {e}')
        return


#===================VAR====================    
var_to_db(bu, '20250101', '20250131')
var_to_db(bu, '20250201', '20250231')
var_to_db(bu, '20250301', '20250331')
var_to_db(bu, '20250401', '20250431')
var_to_db(bu, '20250501', '20250531')
var_to_db(bu, '20250601', '20250631')
var_to_db(bu, '20250701', '20250731')
var_to_db(bu, '20250801', '20250831')
var_to_db(bu, '20250901', '20250931')
var_to_db(bu, '20251001', '20251031')
var_to_db(bu, '20251101', '20251131')
var_to_db(bu, '20251201', '20251231')
'''
#===================STK====================
stk_to_db(bu, '20250101', '20250131')
stk_to_db(bu, '20250201', '20250231')
stk_to_db(bu, '20250301', '20250331')
stk_to_db(bu, '20250401', '20250431')
stk_to_db(bu, '20250501', '20250531')
stk_to_db(bu, '20250601', '20250631')
stk_to_db(bu, '20250701', '20250731')
stk_to_db(bu, '20250801', '20250831')
stk_to_db(bu, '20250901', '20250931')
stk_to_db(bu, '20251001', '20251031')
stk_to_db(bu, '20251101', '20251131')
stk_to_db(bu, '20251201', '20251231')
'''