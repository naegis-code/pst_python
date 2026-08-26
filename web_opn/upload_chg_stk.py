import pandas as pd
from sqlalchemy import text

usecols = [
    'result','docname','buname','prndate','cntnum',
    'cntname','stmerch','stname','postdate','freezedate',
    'cntdate','deptcode','deptname','subdeptcode','subdeptname',
    'sku','sbc','ibc','bndcode','bndname','prname','prmodel','soh',
    'cntqnt','varianceqnt','varianceperc','extphycnt_retail','extphycnt_cost',
    'extphy_retailvar','extphy_costvar','extphycnt_retail_exvat','gmperc'
]

def process_chg_stk(df: pd.DataFrame, bu: str, stcode: str, cntdate: str, rpname: str, skutype: str, engine3):
    clean_cntdate = cntdate.replace("-", "")

    col_str = ['result','docname','buname','prndate','cntnum',
                  'cntname','stmerch','stname','postdate','freezedate',
                  'cntdate','deptcode','deptname','subdeptcode','subdeptname',
                  'sku','sbc','ibc','bndcode','bndname','prname','prmodel'
    ]
    col_num = ['soh','cntqnt','varianceqnt','varianceperc','extphycnt_retail','extphycnt_cost',
                  'extphy_retailvar','extphy_costvar','extphycnt_retail_exvat','gmperc'
    ]
    
    #แปลงคอลัมน์ข้อความให้อยู่ในรูป string (ป้องกัน NaN กลายเป็น float)
    for col in col_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # แปลงคอลัมน์ตัวเลขให้เป็น float ทั้งหมด (ป้องกัน ufunc 'add' error)
    for col in col_num:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 2. Assign parameters
    df['bu'] = bu
    df['stcode'] = stcode
    if 'cntdate' not in df.columns or df['cntdate'].isnull().all():
        df['cntdate'] = clean_cntdate
    df['rpname'] = rpname
    df['skutype'] = skutype

    # 3. Validation
    if 'stmerch' in df.columns and (df['stcode'] != df['stmerch'].astype(str)).any():
        raise ValueError("Some stcode values do not match stmerch values.")

    if (df['cntdate'].astype(str) != clean_cntdate).any():
        raise ValueError("Some cntdate values do not match clean cntdate.")

    #คำนวณ vsoh และ vrsoh
    df_stk = df.copy()
    df_stk['vsoh'] = df_stk['extphycnt_cost'] - df_stk['extphy_costvar']
    df_stk['vrsoh'] = df_stk['extphycnt_retail'] - df_stk['extphy_retailvar']
    df_stk['dept'] = df_stk['deptcode'] + ' ' + df_stk['deptname']
    df_stk['subdept'] = df_stk['subdeptcode'] + ' ' + df_stk['subdeptname']

    #ส่วน stk_report
    stk_report = df_stk.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
        as_index=False
    ).agg(
        sku=('sku', 'count'),
        sgain=('varianceqnt', lambda x: (x > 0).sum()),  # CountIF: นับเฉพาะรายการที่ varianceqnt > 0
        sloss=('varianceqnt', lambda x: (x < 0).sum()),  # CountIF: นับเฉพาะรายการที่ varianceqnt < 0
        psoh=('soh', 'sum'),
        pqty=('cntqnt', 'sum'),
        pgain=('varianceqnt', lambda x: x[x > 0].sum()),  # SumIF: รวมเฉพาะรายการที่ varianceqnt > 0
        ploss=('varianceqnt', lambda x: x[x < 0].sum()),  # SumIF: รวมเฉพาะรายการที่ varianceqnt < 0
        vsoh=('vsoh', 'sum'),
        vqty=('extphycnt_cost', 'sum'),
        vgain=('extphy_costvar', lambda x: x[x > 0].sum()),  # SumIF: รวมเฉพาะรายการที่ extphy_costvar > 0
        vloss=('extphy_costvar', lambda x: x[x < 0].sum()),  # SumIF: รวมเฉพาะรายการที่ extphy_costvar < 0
        vrsoh=('vrsoh', 'sum'),
        vrqty=('extphycnt_retail', 'sum'),
        vrgain=('extphy_retailvar', lambda x: x[x > 0].sum()),  # SumIF: รวมเฉพาะรายการที่ extphy_retailvar > 0
        vrloss=('extphy_retailvar', lambda x: x[x < 0].sum())  # SumIF: รวมเฉพาะรายการที่ extphy_retailvar < 0
    )

    #ส่วน stk_report_subdept
    stk_report_subdept = df_stk.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname','dept','subdept'],
        as_index=False
    ).agg(
        sku=('sku', 'count'),
        sgain=('varianceqnt', lambda x: (x > 0).sum()),  # CountIF: นับเฉพาะรายการที่ varianceqnt > 0
        sloss=('varianceqnt', lambda x: (x < 0).sum()),  # CountIF: นับเฉพาะรายการที่ varianceqnt < 0
        psoh=('soh', 'sum'),
        pqty=('cntqnt', 'sum'),
        pgain=('varianceqnt', lambda x: x[x > 0].sum()),  # SumIF: รวมเฉพาะรายการที่ varianceqnt > 0
        ploss=('varianceqnt', lambda x: x[x < 0].sum()),  # SumIF: รวมเฉพาะรายการที่ varianceqnt < 0
        vsoh=('vsoh', 'sum'),
        vqty=('extphycnt_cost', 'sum'),
        vgain=('extphy_costvar', lambda x: x[x > 0].sum()),  # SumIF: รวมเฉพาะรายการที่ extphy_costvar > 0
        vloss=('extphy_costvar', lambda x: x[x < 0].sum()),  # SumIF: รวมเฉพาะรายการที่ extphy_costvar < 0
        vrsoh=('vrsoh', 'sum'),
        vrqty=('extphycnt_retail', 'sum'),
        vrgain=('extphy_retailvar', lambda x: x[x > 0].sum()),  # SumIF: รวมเฉพาะรายการที่ extphy_retailvar > 0
        vrloss=('extphy_retailvar', lambda x: x[x < 0].sum())  # SumIF: รวมเฉพาะรายการที่ extphy_retailvar < 0
    )

    # 5. Database Operations
    params = {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype}
    select_query = text(f"SELECT 1 FROM chg_stk_this_year WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname AND skutype = :skutype")
    update_query = text(f"UPDATE chg_stk_this_year SET bu = concat(:bu, 'E') WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname AND skutype = :skutype")
    delete_query_stk_report = text(f"""delete from stk_report where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype""")
    delete_query_stk_report_subdept = text(f"""delete from stk_report_subdept where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype""")

    with engine3.begin() as conn:  # Auto commit transaction
        result = conn.execute(select_query, params)
        exists = result.fetchone() is not None
        if exists:
            conn.execute(update_query, params)
            conn.execute(delete_query_stk_report, params)
            conn.execute(delete_query_stk_report_subdept, params)
            df.to_sql('chg_stk_this_year', conn, if_exists='append', index=False)
            stk_report.to_sql('stk_report', conn, if_exists='append', index=False)
            stk_report_subdept.to_sql('stk_report_subdept', conn, if_exists='append', index=False)

    return len(df)