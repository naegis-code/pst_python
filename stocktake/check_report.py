import polars as pl
import pandas as pd
from sqlalchemy import create_engine,text
import db_connect

#===== Parameters =====#
date_start = '20250101'
date_end = '20251231'
rpname = 'stk2'

def ault_if_none(bu,rpname):
    print(f"Processing BU: {bu}")
    # Determine stcode field based on bu
    if bu in ['b2s', 'ofm','ssp']:
        stcode = 'store as stcode'
    elif bu in ['chg','pwb','cfw']:
        stcode = 'stmerch as stcode'
    else:
        stcode = 'stcode'
    # Determine filter_type1 based on bu
    if bu in ['b2s', 'ofm','ssp']:
        filter_type1 = ['Warehouse','Online']
    else:
        filter_type1 = ['Warehouse','Frozen','Wine','Tops Care','WHSVC','Online']

    #===== Load Data =====#
    engine_pstdb = create_engine(db_connect.db_url_pstdb)
    engine_pstdb3 = create_engine(db_connect.db_url_pstdb3)

    query_plan_db = text(f"""
        SELECT bu, stcode, branch, type1, to_date(cntdate,'YYYYMMDD') as cntdate, shub from planall2
        WHERE bu = '{bu.upper()}' and atype = '3F' AND cntdate BETWEEN '{date_start}' AND '{date_end}' and type1 not IN {tuple(filter_type1)}
        """)


    query_stk_db3 = text(f"""
        SELECT DISTINCT {stcode}, to_date(cntdate,'YYYYMMDD') as cntdate, skutype, rpname FROM {bu.lower()}_stk_this_year
        WHERE cntdate BETWEEN '{date_start}' AND '{date_end}' and rpname = '{rpname.upper()}'
        """)

    df = pl.read_database(
        query=query_plan_db,
        connection=engine_pstdb
    )

    df_plan = df.select([
        pl.col('stcode').alias('stcode'),
        pl.col('cntdate').alias('cntdate'),
        pl.col('bu').alias('bu'),
        pl.col('branch').alias('branch'),
        pl.col('type1').alias('type1'),
        pl.col('shub').alias('shub')
    ])

    print(f"Plan Data Loaded: {df_plan.shape[0]} rows")
    print(df_plan)

    df_result = pl.read_database(
        query=query_stk_db3,
        connection=engine_pstdb3
    )


    df = df.join(
        df_result,
        left_on=['stcode', 'cntdate'],
        right_on=['stcode', 'cntdate'],
        how='anti'
    )

    print(f"Result Data After Anti Join: {df.shape[0]} rows")
    print(df)

    #===== Process Data =====#

'''
ault_if_none('ssp', 'stk2')
ault_if_none('b2s', 'stk2')
ault_if_none('ofm', 'stk2')
ault_if_none('cfr', 'stk2')
ault_if_none('chg', 'stk2')
ault_if_none('pwb', 'stk2')'''


#ault_if_none('ssp', 'stk1')
#ault_if_none('b2s', 'stk1')
#ault_if_none('ofm', 'stk1')
#ault_if_none('chg', 'stk1')
ault_if_none('cfr', 'stk2')   