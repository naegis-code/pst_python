import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv

bu = 'CHG'
stcode = '60978'
cntdate = '2026-08-20'
rpname = 'VAR1'
skutype = 'Credit'

file = 'var1_60978_20260820.xlsx'
path = f"D:\\Users\\prthanap\\Documents\\chg\\{file}"

usecols = ['RESULT','DOCNAME','BUNAME','PRNDATE','CNTNUM','FREEZTSTT','ALLSKU','LOSSAMT1','LOSSAMT2','GAINAMT1','GAINAMT2',
           'DEPTCODE','DEPTNAME','LOCATION','SKCODE','BARIBC','BARSBC1','BARSBC2','PRNAME','BNDCODE','BNDNAME','MODEL','COLOR',
           'SOH','VARIANCE','CNTQNT','PRTYPE','BARIBCPRINT','BARLOCATION','BARCNTNUM'
            ]
col_str = ['result','docname','buname','prndate','cntnum','freeztstt','allsku','lossamt1','lossamt2','gainamt1','gainamt2',
                'deptcode','deptname','location','skcode','baribc','barsbc1','barsbc2','prname','bndcode','bndname','model',
                'color','prtype','baribcprint','barlocation','barcntnum'
            ]
col_num = ['soh','variance','cntqnt'
            ]

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
    df = pd.read_excel(path, sheet_name=0, usecols=usecols)
    df.columns = df.columns.str.strip().str.lower()  # ลบช่องว่างออกจากชื่อคอลัมน์

    df['bu'] = bu
    df['stcode'] = stcode

    df['cntdate'] = '20' + df['cntnum'].str[10:12] + df['cntnum'].str[8:10] + df['cntnum'].str[6:8]  # แปลง cntnum เป็น cntdate

    
    if 'cntdate' not in df.columns:
        df['cntdate'] = clean_cntdate

    df['rpname'] = rpname
    df['skutype'] = df['prtype']


    if (df['stcode'] != df['cntnum'].str[0:5]).any():
        print("Warning: Some stcode values do not match stmerch values.")
        exit()

    if (df['cntdate'] != clean_cntdate).any():
        print("Warning: Some cntdate values do not match the clean cntdate.")
        exit()
    
    #คำนวณ vsoh และ vrsoh
    df_var = df.copy()

    #ส่วน stk_report
    var_report = df_var.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
        as_index=False
    ).agg(
        pqty=('cntqnt', 'sum'),
    )
    
    #table Detail
    if not df.empty:
        query = text(f"""select 1 from chg_var_this_year where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype""")
        with engine3.begin() as conn:
            result = conn.execute(query, {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype})
            exists = result.fetchone() is not None
            if exists:
                conn.execute(text(f"""update chg_var_this_year set bu = concat(:bu,'E') where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype"""), {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype})
                conn.execute(text(f"""delete from var_report where bu = :bu and stcode = :stcode and cntdate = :cntdate and rpname = :rpname and skutype = :skutype"""), {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype})
                print(f"Updated existing records in chg_var_this_year for bu={bu}, stcode={stcode}, cntdate={clean_cntdate}, rpname={rpname}, skutype={skutype}.")
                df.to_sql('chg_var_this_year', engine3, if_exists='append', index=False)
                var_report.to_sql('var_report', engine3, if_exists='append', index=False)
                print(f"Appended new records to chg_var_this_year.")
        exit()

except Exception as e:
    print(f"Error reading Excel file: {e}")