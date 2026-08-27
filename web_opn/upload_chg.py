import io
import pandas as pd
from sqlalchemy import text


def process_chg_stk(file_contents: bytes, bu: str, stcode: str, cntdate: str, rpname: str, skutype: str, engine3, username: str):

    usecols = [
    'RESULT','DOCNAME','BUNAME','PRNDATE','CNTNUM',
    'CNTNAME','STMERCH','STNAME','POSTDATE','FREEZEDATE',
    'CNTDATE','DEPTCODE','DEPTNAME','SUBDEPTCODE','SUBDEPTNAME',
    'SKU','SBC','IBC','BNDCODE','BNDNAME','PRNAME','PRMODEL','SOH',
    'CNTQNT','VARIANCEQNT','VARIANCEPERC','EXTPHYCNT_RETAIL','EXTPHYCNT_COST',
    'EXTPHY_RETAILVAR','EXTPHY_COSTVAR','EXTPHYCNT_RETAIL_EXVAT','GMPERC'
    ]
    col_str = [
    'result','docname','buname','prndate','cntnum',
    'cntname','stmerch','stname','postdate','freezedate',
    'cntdate','deptcode','deptname','subdeptcode','subdeptname',
    'sku','sbc','ibc','bndcode','bndname','prname','prmodel'
    ]
    col_num = [
    'soh','cntqnt','varianceqnt','varianceperc','extphycnt_retail','extphycnt_cost',
    'extphy_retailvar','extphy_costvar','extphycnt_retail_exvat','gmperc'
    ]

    clean_cntdate = cntdate.replace("-", "")

    try:
        # 1. อ่านไฟล์ Excel จาก Memory
        df = pd.read_excel(
            io.BytesIO(file_contents),
            sheet_name=0,
            usecols=usecols
        )
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {e}")

    df.columns = df.columns.str.strip().str.lower()

    # แปลงคอลัมน์ข้อความให้อยู่ในรูป string
    for col in col_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # แปลงคอลัมน์ตัวเลขให้เป็น float ทั้งหมด (ป้องกัน ufunc error)
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
    df['username'] = username

    # 3. Validation
    if 'stmerch' in df.columns and (df['stcode'] != df['stmerch'].astype(str)).any():
        raise ValueError("Some stcode values do not match stmerch values.")

    if (df['cntdate'].astype(str) != clean_cntdate).any():
        raise ValueError("Some cntdate values do not match clean cntdate.")

    # 4. คำนวณ vsoh และ vrsoh
    df_stk = df.copy()
    df_stk['vsoh'] = df_stk['extphycnt_cost'] - df_stk['extphy_costvar']
    df_stk['vrsoh'] = df_stk['extphycnt_retail'] - df_stk['extphy_retailvar']
    df_stk['dept'] = df_stk['deptcode'] + ' ' + df_stk['deptname']
    df_stk['subdept'] = df_stk['subdeptcode'] + ' ' + df_stk['subdeptname']

    # ส่วน stk_report
    stk_report = df_stk.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
        as_index=False
    ).agg(
        sku=('sku', 'count'),
        sgain=('varianceqnt', lambda x: (x > 0).sum()),
        sloss=('varianceqnt', lambda x: (x < 0).sum()),
        psoh=('soh', 'sum'),
        pqty=('cntqnt', 'sum'),
        pgain=('varianceqnt', lambda x: x[x > 0].sum()),
        ploss=('varianceqnt', lambda x: x[x < 0].sum()),
        vsoh=('vsoh', 'sum'),
        vqty=('extphycnt_cost', 'sum'),
        vgain=('extphy_costvar', lambda x: x[x > 0].sum()),
        vloss=('extphy_costvar', lambda x: x[x < 0].sum()),
        vrsoh=('vrsoh', 'sum'),
        vrqty=('extphycnt_retail', 'sum'),
        vrgain=('extphy_retailvar', lambda x: x[x > 0].sum()),
        vrloss=('extphy_retailvar', lambda x: x[x < 0].sum())
    )

    # ส่วน stk_report_subdept
    stk_report_subdept = df_stk.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname','dept','subdept'],
        as_index=False
    ).agg(
        sku=('sku', 'count'),
        sgain=('varianceqnt', lambda x: (x > 0).sum()),
        sloss=('varianceqnt', lambda x: (x < 0).sum()),
        psoh=('soh', 'sum'),
        pqty=('cntqnt', 'sum'),
        pgain=('varianceqnt', lambda x: x[x > 0].sum()),
        ploss=('varianceqnt', lambda x: x[x < 0].sum()),
        vsoh=('vsoh', 'sum'),
        vqty=('extphycnt_cost', 'sum'),
        vgain=('extphy_costvar', lambda x: x[x > 0].sum()),
        vloss=('extphy_costvar', lambda x: x[x < 0].sum()),
        vrsoh=('vrsoh', 'sum'),
        vrqty=('extphycnt_retail', 'sum'),
        vrgain=('extphy_retailvar', lambda x: x[x > 0].sum()),
        vrloss=('extphy_retailvar', lambda x: x[x < 0].sum())
    )

    # 5. Database Operations
    params = {"bu": bu, "stcode": stcode, "cntdate": clean_cntdate, "rpname": rpname, "skutype": skutype}
    select_query = text("SELECT 1 FROM chg_stk_this_year WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname AND skutype = :skutype")
    update_query = text("UPDATE chg_stk_this_year SET bu = concat(:bu, 'E') WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname AND skutype = :skutype")
    delete_query_stk_report = text("DELETE FROM stk_report WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname AND skutype = :skutype")
    delete_query_stk_report_subdept = text("DELETE FROM stk_report_subdept WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname AND skutype = :skutype")

    with engine3.connect() as conn:
        with conn.begin():  # Auto Commit Transaction ทั้งหมดเมื่อทำงานจบโดยไม่มี Error
            result = conn.execute(select_query, params)
            exists = result.fetchone() is not None
            
            # ถ้ามีข้อมูลเดิม ให้ Mark ข้อมูลเก่า และลบ Report เก่าออกก่อน
            if exists:
                conn.execute(update_query, params)
                conn.execute(delete_query_stk_report, params)
                conn.execute(delete_query_stk_report_subdept, params)
            
            # 🎯 ย้ายออกมาข้างนอก if exists: เพื่อให้ทำงานเสมอไม่ว่าข้อมูลจะเคยมีหรือยังไม่เคยมีก็ตาม
            df.to_sql('chg_stk_this_year', conn, if_exists='append', index=False)
            stk_report.to_sql('stk_report', conn, if_exists='append', index=False)
            stk_report_subdept.to_sql('stk_report_subdept', conn, if_exists='append', index=False)

    return len(df)

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
            io.BytesIO(file_contents),
            sheet_name=0,
            usecols=usecols
        )
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {e}")

    df.columns = df.columns.str.strip().str.lower()

    # แปลงคอลัมน์ข้อความให้อยู่ในรูป string
    for col in col_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

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
    df_var = df.copy()
    var_report = df_var.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
        as_index=False
    ).agg(
        pqty=('cntqnt', 'sum'),
    )
    
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
        select_query = text("""
            SELECT 1 FROM chg_var_this_year 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)
        
        update_query = text("""
            UPDATE chg_var_this_year SET bu = concat(:bu, 'E') 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)
        
        delete_report_query = text("""
            DELETE FROM var_report 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype  
        """)

        with engine3.connect() as conn:
            with conn.begin():  # Auto Commit Transaction ทั้งหมดในบล็อกนี้
                result = conn.execute(select_query, params)
                exists = result.fetchone() is not None
                
                if exists:
                    conn.execute(update_query, params)
                    conn.execute(delete_report_query, params)
                
                # 🎯 ส่ง conn เข้าไปใน to_sql แทน engine3 เพื่อให้อยู่ใน Transaction เดียวกัน
                df.to_sql('chg_var_this_year', conn, if_exists='append', index=False)
                var_report.to_sql('var_report', conn, if_exists='append', index=False)

    return len(df)

def process_chg_nocount(file_contents: bytes, bu: str, stcode: str, cntdate: str, rpname: str, skutype: str, engine3, username: str):

    usecols = [
        'BUName','RepName','PrintDate','SKU','IBC','SBC','รายละเอียด','ยี่ห้อ','รุ่น','Cnt','Variance','Location','Total','Dept']

    col_str = [
        'buname','repname','printdate','sku','ibc','sbc','รายละเอียด','ยี่ห้อ','รุ่น','location','dept']
    
    col_num = [
        'cnt','variance','total']

    

    clean_cntdate = cntdate.replace("-", "")
    
    try:
        df = pd.read_excel(
            io.BytesIO(file_contents),
            sheet_name=0,
            usecols=usecols
        )
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {e}")

    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={'รายละเอียด': 'prname', 'ยี่ห้อ': 'bndname', 'รุ่น': 'model'}, inplace=True)
    # แปลงคอลัมน์ข้อความให้อยู่ในรูป string
    for col in col_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # แปลงคอลัมน์ตัวเลขให้เป็น float ทั้งหมด
    for col in col_num:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['bu'] = bu
    df['stcode'] = stcode
    df['cntdate'] = clean_cntdate
    df['rpname'] = rpname
    df['skutype'] = skutype
    df['username'] = username
    
    # คำนวณ var_report
    df_cal = df.copy()
    report = df_cal.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
        as_index=False
    ).agg(
        pqty=('sku', 'count'),
    )

    df_cal['subdept'] = df_cal['dept'].str[0:4]

    subdept = df_cal.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname', 'subdept'],
        as_index=False
    ).agg(
        pqty=('sku', 'count'),
    )
    
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
        select_query = text("""
            SELECT 1 FROM chg_nocount_this_year 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)
        
        update_query = text("""
            UPDATE chg_nocount_this_year SET bu = concat(:bu, 'E') 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)
        
        delete_report_query = text("""
            DELETE FROM noc_zec_report 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype  
        """)

        delete_subdept_query = text("""
            DELETE FROM noc_zec_report_subdept
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)

        with engine3.connect() as conn:
            with conn.begin():  # Auto Commit Transaction ทั้งหมดในบล็อกนี้
                result = conn.execute(select_query, params)
                exists = result.fetchone() is not None
                
                if exists:
                    conn.execute(update_query, params)
                    conn.execute(delete_report_query, params)
                    conn.execute(delete_subdept_query, params)
                
                # 🎯 ส่ง conn เข้าไปใน to_sql แทน engine3 เพื่อให้อยู่ใน Transaction เดียวกัน
                df.to_sql('chg_nocount_this_year', conn, if_exists='append', index=False)
                report.to_sql('noc_zec_report', conn, if_exists='append', index=False)
                subdept.to_sql('noc_zec_report_subdept', conn, if_exists='append', index=False)

    return len(df)

def process_chg_zerocount(file_contents: bytes, bu: str, stcode: str, cntdate: str, rpname: str, skutype: str, engine3, username: str):

    usecols = [
        'BUName','RepName','PrintDate','SKU','IBC','SBC','รายละเอียด','ยี่ห้อ','รุ่น','Cnt','Variance','Location','Total','Dept']

    col_str = [
        'buname','repname','printdate','sku','ibc','sbc','รายละเอียด','ยี่ห้อ','รุ่น','location','dept']
    
    col_num = [
        'cnt','variance','total']

    clean_cntdate = cntdate.replace("-", "")
    
    try:
        df = pd.read_excel(
            io.BytesIO(file_contents),
            sheet_name=0,
            usecols=usecols
        )
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {e}")

    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={'รายละเอียด': 'prname', 'ยี่ห้อ': 'bndname', 'รุ่น': 'model'}, inplace=True)
    # แปลงคอลัมน์ข้อความให้อยู่ในรูป string
    for col in col_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # แปลงคอลัมน์ตัวเลขให้เป็น float ทั้งหมด
    for col in col_num:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['bu'] = bu
    df['stcode'] = stcode
    df['cntdate'] = clean_cntdate
    df['rpname'] = rpname
    df['skutype'] = skutype
    df['username'] = username
    
    # คำนวณ var_report
    df_cal = df.copy()

    report = df_cal.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname'],
        as_index=False
    ).agg(
        pqty=('sku', 'count'),
    )

    df_cal['subdept'] = df_cal['dept'].str[0:4]

    subdept = df_cal.groupby(
        ['bu', 'stcode', 'cntdate', 'skutype', 'rpname', 'subdept'],
        as_index=False
    ).agg(
        pqty=('sku', 'count'),
    )
    
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
        select_query = text("""
            SELECT 1 FROM chg_zerocount_this_year 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)
        
        update_query = text("""
            UPDATE chg_zerocount_this_year SET bu = concat(:bu, 'E') 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)
        
        delete_report_query = text("""
            DELETE FROM noc_zec_report 
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)

        delete_subdept_query = text("""
            DELETE FROM noc_zec_report_subdept
            WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate AND rpname = :rpname and skutype = :skutype
        """)

        with engine3.connect() as conn:
            with conn.begin():  # Auto Commit Transaction ทั้งหมดในบล็อกนี้
                result = conn.execute(select_query, params)
                exists = result.fetchone() is not None
                
                if exists:
                    conn.execute(update_query, params)
                    conn.execute(delete_report_query, params)
                    conn.execute(delete_subdept_query, params)
                
                # 🎯 ส่ง conn เข้าไปใน to_sql แทน engine3 เพื่อให้อยู่ใน Transaction เดียวกัน
                df.to_sql('chg_zerocount_this_year', conn, if_exists='append', index=False)
                report.to_sql('noc_zec_report', conn, if_exists='append', index=False)
                subdept.to_sql('noc_zec_report_subdept', conn, if_exists='append', index=False)

    return len(df)