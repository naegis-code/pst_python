import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv

bu = 'CHG'
stcode = '60983'
cntdate = '2026-08-27'
rpname = 'VAR1'
skutype = 'Credit'

file = 'VAR2_Credit_CHG_60983_260827.xls'
path = f"D:\\Users\\prthanap\\Documents\\chg\\{file}"

engine3 = create_engine("postgresql+psycopg2://prthanapat:20020015@localhost:5432/pstdb3")

def process_chg_var(file_contents: bytes, bu: str, stcode: str, cntdate: str, rpname: str, skutype: str, engine3, username: str):

    usecols = [
    'RESULT','DOCNAME','BUNAME','PRNDATE','CNTNUM','FREEZTSTT','ALLSKU','LOSSAMT1','LOSSAMT2','GAINAMT1','GAINAMT2',
    'DEPTCODE','DEPTNAME','LOCATION','SKCODE','BARIBC','BARSBC1','BARSBC2','PRNAME','BNDCODE','BNDNAME','MODEL','COLOR',
    'SOH','VARIANCE','CNTQNT','PRTYPE','BARIBCPRINT','BARLOCATION','BARCNTNUM'
    ]
    col_str = [
        'result','docname','buname','prndate','cntnum','freeztstt','allsku','lossamt1','lossamt2','gainamt1','gainamt2',
        'deptcode','deptname','location','skcode','baribc','barsbc1','barsbc2','prname','bndcode','bndname','model',
        'color','prtype','baribcprint','barlocation','barcntnum'
    ]
    col_num = ['soh','variance','cntqnt']

    clean_cntdate = cntdate.replace("-", "")
    
    try:
        df = pd.read_excel(
            file_contents,
            sheet_name=0,
            usecols=usecols,
            dtype=str
        )
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {e}")

    df.columns = df.columns.str.strip().str.lower()
    # แปลงคอลัมน์ข้อความให้อยู่ในรูป string โดยคงค่าว่างเดิมไว้
    for col in col_str:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # แปลงคอลัมน์ตัวเลขให้เป็น float ทั้งหมด
    for col in col_num:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['bu'] = bu
    df['stcode'] = stcode
    df['cntdate'] = '20' + df['cntnum'].str[10:12] + df['cntnum'].str[8:10] + df['cntnum'].str[6:8]
    df['rpname'] = rpname
    df['skutype'] = df['prtype']
    df['username'] = username

    # Validation: เปลี่ยนจาก exit() เป็น raise ValueError เพื่อให้ FastAPI จับไปตอบ HTTP 400
    if (df['stcode'] != df['cntnum'].str[0:5]).any():
        raise ValueError("Some stcode values do not match cntnum values.")

    if (df['cntdate'] != clean_cntdate).any():
        raise ValueError("Some cntdate values do not match the clean cntdate.")

    if (df['skutype'] != skutype).any():
        raise ValueError("Some skutype values do not match the provided skutype.")
    
    # คำนวณ var_report
    
    var_report = df.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
        as_index=False
    ).agg(
        pqty=('cntqnt', 'sum'),
    )
    df_nocount = df[df['location'].isna()]

    df_nocount = df_nocount[['bu', 'stcode', 'cntdate', 'skutype', 'prndate', 'prname', 'deptcode', 'location','skcode','baribc','bndname','model','barsbc1','variance','rpname','username']]
    df_nocount.rename(columns={'baribc': 'ibc','barsbc1': 'sbc','prndate': 'printdate'}, inplace=True)

    df_nocount['cnt'] = 0
    df_nocount['buname'] = ''
    df_nocount['total'] = df_nocount['variance']
    df_nocount['deptcode'] = df_nocount['deptcode'].str[0:4]
    df_nocount['rpname'] = 'NOC' + df_nocount['rpname'].str[3:4]


    df_zerocount = df[(df['location'].notna())]
    df_zerocount['printdate'] = df_zerocount['prndate'].iloc[0]
    df_zerocount = df_zerocount.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype','printdate', 'prname', 'deptcode','skcode', 'baribc','bndname','model','barsbc1','variance','rpname','username'
         ],
        as_index=False,dropna=False
    ).agg(
        cntqnt=('cntqnt', 'sum'),
    )

    df_zerocount.rename(columns={'baribc': 'ibc','barsbc1': 'sbc'}, inplace=True)

    df_zerocount = df_zerocount[df_zerocount['cntqnt'] == 0]
    df_zerocount['location'] = ''
    df_zerocount['buname'] = ''
    df_zerocount['total'] = df_zerocount['variance']
    df_zerocount['deptcode'] = df_zerocount['deptcode'].str[0:4]
    df_zerocount['rpname'] = 'ZEC' + df_zerocount['rpname'].str[3:4]



    # Database Operations
    if not df.empty:
        params = {
            "bu": bu, 
            "stcode": stcode, 
            "cntdate": clean_cntdate, 
            "rpname": rpname,
            "skutype": skutype
        }
        
        # ค้นหาโดยไม่ล็อก skutype เพื่อรองรับกรณีในไฟล์มีหลาย skutype
        query_dept = text("""
            SELECT DISTINCT ON (SUBSTRING(subdept, 1, 4)) subdept,SUBSTRING(subdept, 1, 4) as deptcode
            FROM stk_report_subdept
        """)

        select_query = text("""
            SELECT 1 FROM chg_var_this_year 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)
        
        update_query = text("""
            UPDATE chg_var_this_year SET bu = concat(:bu, 'E') 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype;
            update chg_nocount_this_year SET bu = concat(:bu, 'E')
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = 'NOC' + df_nocount['rpname'].str[3:4].unique()[0] and skutype = :skutype;
            update chg_zerocount_this_year SET bu = concat(:bu, 'E')
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = 'ZEC' + df_zerocount['rpname'].str[3:4].unique()[0] and skutype = :skutype;
        """)
        
        delete_report_query = text("""
            DELETE FROM var_report 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype;
            delete from noc_zec_report
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname in ('NOC' + df_nocount['rpname'].str[3:4].unique()[0], 'ZEC' + df_zerocount['rpname'].str[3:4].unique()[0]) and skutype = :skutype;
            delete from noc_zec_report_subdept
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname in ('NOC' + df_nocount['rpname'].str[3:4].unique()[0], 'ZEC' + df_zerocount['rpname'].str[3:4].unique()[0]) and skutype = :skutype;
        """)

        with engine3.connect() as conn:
            with conn.begin():  # Auto Commit Transaction ทั้งหมดในบล็อกนี้
                result = conn.execute(select_query, params)
                exists = result.fetchone() is not None
                
                if exists:
                    #conn.execute(update_query, params)
                    #conn.execute(delete_report_query, params)
                    dept_list = pd.read_sql(query_dept, conn)
                    df_nocount = df_nocount.merge(dept_list, on='deptcode', how='left')
                    df_nocount['repname'] = 'รายงาน No count Report แผนก ' + df_nocount['subdept']
                    df_nocount.rename(columns={'skcode': 'sku', 'subdept': 'dept'}, inplace=True)
                    df_nocount.drop(columns=['deptcode'], inplace=True)
                    df_nocount_report = df_nocount.groupby(
                        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
                        as_index=False
                    ).agg(
                        pqty=('sku', 'count'),
                    )
                    df_nocount_report_subdept = df_nocount.groupby(
                        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname', 'dept'],
                        as_index=False
                    ).agg(
                        pqty=('sku', 'count'),
                    )
                    df_nocount_report_subdept['subdept'] = df_nocount_report_subdept['dept']
                    
                    df_zerocount = df_zerocount.merge(dept_list, on='deptcode', how='left')
                    df_zerocount['repname'] = 'รายงาน Zero count Report แผนก ' + df_zerocount['subdept']
                    df_zerocount.rename(columns={'skcode': 'sku', 'subdept': 'dept'}, inplace=True)
                    df_zerocount.drop(columns=['deptcode'], inplace=True)
                    df_zerocount_report = df_zerocount.groupby(
                        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
                        as_index=False
                    ).agg(
                        pqty=('sku', 'count'),
                    )
                    df_zerocount_report_subdept = df_zerocount.groupby(
                        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname', 'dept'],
                        as_index=False
                    ).agg(
                        pqty=('sku', 'count'),
                    )
                    df_zerocount_report_subdept['subdept'] = df_zerocount_report_subdept['dept']


                # 🎯 ส่ง conn เข้าไปใน to_sql แทน engine3 เพื่อให้อยู่ใน Transaction เดียวกัน

                #df.to_sql('chg_var_this_year', conn, if_exists='append', index=False, method='multi', chunksize=1000)
                #var_report.to_sql('var_report', conn, if_exists='append', index=False)
                #df_nocount.to_sql('chg_nocount_this_year', conn, if_exists='append', index=False, method='multi', chunksize=1000)
                #df_nocount_report.to_sql('noc_zec_report', conn, if_exists='append', index=False)
                #df_nocount_report_subdept.to_sql('noc_zec_report_subdept', conn, if_exists='append', index=False)
                #df_zerocount.to_sql('chg_zerocount_this_year', conn, if_exists='append', index=False, method='multi', chunksize=1000)
                #df_zerocount_report.to_sql('zerocount_report', conn, if_exists='append', index=False)
                #df_zerocount_report_subdept.to_sql('zerocount_report_subdept', conn, if_exists='append', index=False)
        
        #print(df.shape)
        #print(df)
        #print(var_report)
        #print(df_nocount.shape)
        #print(df_nocount.info())
        print(df_zerocount)
        #print(df_zerocount.info())


    return len(df)


process_chg_var(path, bu, stcode, cntdate, rpname, skutype, engine3, 'test_user')