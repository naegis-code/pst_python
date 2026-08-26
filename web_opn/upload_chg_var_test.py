import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv

bu = 'CHG'
stcode = '60927'
cntdate = '2026-08-13'
rpname = 'VAR1'
skutype = 'Credit'

file = 'var1_credit.xls'
path = f"D:\\Users\\prthanap\\Documents\\chg\\{file}"
table_name = 'chg_var_this_year'

usecols = ['RESULT','DOCNAME','BUNAME','PRNDATE','CNTNUM',
           'CNTNAME','STMERCH','STNAME','POSTDATE','FREEZEDATE',
           'CNTDATE','DEPTCODE','DEPTNAME','SUBDEPTCODE','SUBDEPTNAME',
           'SKU','SBC','IBC','BNDCODE','BNDNAME','PRNAME','PRMODEL','SOH',
           'CNTQNT','VARIANCEQNT','VARIANCEPERC','EXTPHYCNT_RETAIL','EXTPHYCNT_COST',
           'EXTPHY_RETAILVAR','EXTPHY_COSTVAR','EXTPHYCNT_RETAIL_EXVAT','GMPERC']

clean_cntdate = cntdate.replace("-", "")  # ลบขีดออกจากวันที่
print(f"Cleaned cntdate: {clean_cntdate}")

user = "prthanapat"
paas = "20020015"

# ตั้งค่า Pool Connection รองรับผู้ใช้พร้อมกัน
engine1 = create_engine(
    f"postgresql://{user}:{paas}@localhost:5432/pstdb",
    pool_size=10,
    max_overflow=20,
)
engine3 = create_engine(
    f"postgresql://{user}:{paas}@localhost:5432/pstdb3",
    pool_size=10,
    max_overflow=20,
)

try:
    df = pd.read_excel(path, sheet_name=0, usecols=usecols, dtype={'RESULT': str, 'DOCNAME': str, 'BUNAME': str, 'PRNDATE': str, 'CNTNUM': str, 
                        'CNTNAME': str, 'STMERCH': str, 'STNAME': str, 'POSTDATE': str, 'FREEZEDATE': str, 'CNTDATE': str, 'DEPTCODE': str, 'DEPTNAME': str, 
                        'SUBDEPTCODE': str, 'SUBDEPTNAME': str, 'SKU': str, 'SBC': str, 'IBC': str, 'BNDCODE': str, 'BNDNAME': str, 'PRNAME': str, 'PRMODEL': str, 
                        'SOH': float, 'CNTQNT': float, 'VARIANCEQNT': float, 'VARIANCEPERC': float, 'EXTPHYCNT_RETAIL': float, 'EXTPHYCNT_COST': float,
                        'EXTPHY_RETAILVAR': float, 'EXTPHY_COSTVAR': float, 'EXTPHYCNT_RETAIL_EXVAT': float, 'GMPERC': float})
    df.columns = df.columns.str.strip().str.lower()  # ลบช่องว่างออกจากชื่อคอลัมน์

    df['bu'] = bu
    df['stcode'] = stcode

    if 'cntdate' not in df.columns:
        df['cntdate'] = clean_cntdate

    df['rpname'] = rpname
    df['skutype'] = skutype

    if (df['stcode'] != df['stmerch']).any():
        print("Warning: Some stcode values do not match stmerch values.")
        exit()

    if (df['cntdate'] != clean_cntdate).any():
        print("Warning: Some cntdate values do not match the clean cntdate.")
        exit()

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

    print(df)
    print(stk_report)
    print(stk_report_subdept)


    
    #table Detail
    if not df.empty:
        query = text(f"""select 1 from {table_name} where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype""")
        with engine3.begin() as conn:
            result = conn.execute(query, {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype})
            exists = result.fetchone() is not None
            if exists:
                conn.execute(text(f"""update {table_name} set bu = concat(:bu,'E') where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype"""), {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype})
                conn.execute(text(f"""delete from stk_report where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype"""), {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype})
                conn.execute(text(f"""delete from stk_report_subdept where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype"""), {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype})
                print(f"Updated existing records in {table_name} for bu={bu}, stcode={stcode}, cntdate={clean_cntdate}, rpname={rpname}, skutype={skutype}.")
                df.to_sql(table_name, engine3, if_exists='append', index=False)
                stk_report.to_sql('stk_report', engine3, if_exists='append', index=False)
                stk_report_subdept.to_sql('stk_report_subdept', engine3, if_exists='append', index=False)
                print(f"Appended new records to {table_name}.")
        exit()

    

except Exception as e:
    print(f"Error reading Excel file: {e}")