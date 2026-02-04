import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
from pathlib import Path
from tqdm import tqdm

# ========== PARAMETERS ==========
bu = 'ofm'
file_1 = 'combined_ofm_STK1_Credit_07112025.CSV'
path_1 = Path(f'E:/STK_VAR/{bu.upper()}') / file_1

file_2 = 'combined_b2s_VAR1_Credit_07112025.CSV'
path_2 = Path(f'E:/STK_VAR/{bu.upper()}') / file_2

file_3 = 'combined_b2s_STK1_Consign_07112025.CSV'
path_3 = Path(f'E:/STK_VAR/{bu.upper()}') / file_3

file_4 = 'combined_b2s_STK1_Credit_07112025.CSV'
path_4 = Path(f'E:/STK_VAR/{bu.upper()}') / file_4

file_5 = '.CSV'
path_5 = Path(f'E:/STK_VAR/{bu.upper()}') / file_5

file_6 = '.CSV'
path_6 = Path('E:/STK_VAR/CHG') / file_6



def upfront_var (bu, file, path):

    # ========== DATABASE CONNECTION ==========
    connect_db = create_engine(db_connect.db_url_pstdb)
    connect_db3 = create_engine(db_connect.db_url_pstdb3)
    query_plan = text(f"""select stcode, cntdate, branch from planall2 where atype in ('3F','3Q') and bu='{bu.upper()}'""")
    query_plan_add = text(f"""select stcode, cntdate, branch from plan_add where bu='{bu.upper()}'""")
    query_result = text(f"""SELECT distinct cntnum, rpname, 'check' as recheck FROM {bu.lower()}_var_this_year""")

    # ========== GET DATA FROM TABLE ==========

    df_plan = pd.read_sql(query_plan, con=connect_db)
    df_result = pd.read_sql(query_result, con=connect_db3)
    df_plan_add = pd.read_sql(query_plan_add, con=connect_db3)
    df_plan = pd.concat([df_plan, df_plan_add], ignore_index=True)


    # ========== READ DATA FROM FILE ==========
    df = pd.read_csv(path, dtype=str,encoding='utf-8',delimiter=',')
    df.columns = df.columns.str.lower()

    print(len(df), "rows read from", file)

    df = df[df['skcode'].fillna('') != ''].drop_duplicates()

    df['stcode'] = df['cntnum'].str[:5]
    df['cntdate'] = '20'+df['cntnum'].str[10:12]+df['cntnum'].str[8:10]+df['cntnum'].str[6:8]

    df = df.merge(df_plan, how='left', on=['stcode', 'cntdate'])

    # แยกแถวที่มีค่า Branch เป็น NaN เพื่อรายงาน
    df_plan_na = df[df['branch'].isna()]

    if df_plan_na.empty:
        print("✅ All rows have valid plans.")
    else :
        df_plan_na = df_plan_na.groupby(['cntnum', 'rpname',]).size().reset_index(name='counts')
        print("❌Plan incorrect in result:")
        print(df_plan_na)
    

    # กรองเฉพาะแถวที่ branch เป็น NaN เพื่อรายงาน
    df = df[df['branch'].notna()].drop(columns=['branch'], errors='ignore')

    print(len(df), "rows planall2 processing")

    df = df.merge(df_result, how='left', on=['cntnum', 'rpname'])

    # แยกแถวที่มีค่า recheck ไม่เป็น NaN เพื่อรายงาน
    df_result_notna = df[df['recheck'].notna()]
    if df_result_notna.empty:
        print("✅ All batches found in result.")
    else : 
        df_result_notna = df_result_notna.groupby(['cntnum', 'rpname',]).size().reset_index(name='counts')
        print("❌Batch incorrect in result:")
        print(df_result_notna)

    # กรองเฉพาะแถวที่ recheck เป็น NaN และลบ column recheck ทิ้ง
    df = df[df['recheck'].isna()].drop(columns=['recheck','stcode','cntdate'], errors='ignore')

    print(len(df), "rows after check result processing")

    chunksize = 1000
    total_chunks = (len(df) + chunksize - 1) // chunksize

    for i in tqdm(range(total_chunks), desc="uploading to db", unit="chunk"):
        start = i * chunksize
        chunk = df.iloc[start:start + chunksize]
        if not chunk.empty:
            chunk.to_sql(
                name=f"{bu.lower()}_var_this_year",
                con=connect_db3,
                if_exists='append',
                index=False,
                method='multi'
            )
    print(f"✅ {file} uploaded to {bu.lower()}_var_this_year")

def upfront_stk (bu, file, path):

    # ========== DATABASE CONNECTION ==========
    connect_db = create_engine(db_connect.db_url_pstdb)
    connect_db3 = create_engine(db_connect.db_url_pstdb3)
    query_plan = text(f"""select stcode as stmerch, cntdate, branch from planall2 where atype in ('3F','3Q') and bu='{bu.upper()}'""")
    query_palan_add = text(f"""select stcode as stmerch, cntdate, branch from plan_add where bu='{bu.upper()}'""")
    query_result = text(f"""SELECT distinct cntnum, rpname, 'check' as recheck FROM {bu.lower()}_stk_this_year""")

    # ========== GET DATA FROM TABLE ==========

    df_plan = pd.read_sql(query_plan, con=connect_db)
    df_result = pd.read_sql(query_result, con=connect_db3)
    df_plan_add = pd.read_sql(query_palan_add, con=connect_db3)
    df_plan = pd.concat([df_plan, df_plan_add], ignore_index=True)


    # ========== READ DATA FROM FILE ==========
    df = pd.read_csv(path, dtype=str,encoding='utf-8',delimiter=',')
    df.columns = df.columns.str.lower()

    print(len(df), "rows read from", file)

    df = df[df['sku'].fillna('') != ''].drop_duplicates()


    df = df.merge(df_plan, how='left', on=['stmerch', 'cntdate'])

    # แยกแถวที่มีค่า branch เป็น NaN เพื่อรายงาน
    df_plan_na = df[df['branch'].isna()]
    df_plan_na = df_plan_na.groupby(['stmerch', 'cntdate','countnum']).size().reset_index(name='counts')
    print("❌Batch incorrect in planall2:")
    print(df_plan_na)

    # กรองเฉพาะแถวที่ branch ไม่เป็น NaN และลบ column branch ทิ้ง
    df = df[df['branch'].notna()].drop(columns=['branch'], errors='ignore')

    print(len(df), "rows planall2 processing")

    df = df.merge(df_result, how='left', on=['cntnum', 'rpname'])

    # แยกแถวที่มีค่า recheck ไม่เป็น NaN เพื่อรายงาน
    df_result_notna = df[df['recheck'].notna()]
    df_result_notna = df_result_notna.groupby(['cntnum', 'rpname',]).size().reset_index(name='counts')
    print("❌Batch incorrect in result:")
    print(df_result_notna)

    # กรองเฉพาะแถวที่ recheck เป็น NaN และลบ column recheck ทิ้ง
    df = df[df['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

    print(len(df), "rows after check result processing")

    try:
        chunksize = 1000
        total_chunks = (len(df) + chunksize - 1) // chunksize

        for i in tqdm(range(total_chunks), desc="uploading to db", unit="chunk"):
            start = i * chunksize
            chunk = df.iloc[start:start + chunksize]
            if not chunk.empty:
                chunk.to_sql(
                    name=f"{bu.lower()}_stk_this_year",
                    con=connect_db3,
                    if_exists='append',
                    index=False,
                )
    except Exception as e:
        print(f"❌ Error uploading {file} to {bu.lower()}_stk_this_year:", e)
    print(f"✅ {file} uploaded to {bu.lower()}_stk_this_year")

def jda_var (bu,file,path):

    # ========== DATABASE CONNECTION ==========
    connect_db = create_engine(db_connect.db_url_pstdb)
    connect_db3 = create_engine(db_connect.db_url_pstdb3)
    query_plan = text(f"""select stcode as store, cntdate, branch from planall2 where atype = '3F' and bu='{bu.upper()}'""")
    query_result = text(f"""SELECT distinct countname, rpname, 'check' as recheck FROM {bu.lower()}_var_this_year""")

    # ========== GET DATA FROM TABLE ==========

    df_plan = pd.read_sql(query_plan, con=connect_db)
    df_result = pd.read_sql(query_result, con=connect_db3)


    # ========== READ DATA FROM FILE ==========
    df = pd.read_csv(path, dtype=str,encoding='utf-8',delimiter=',')
    df.columns = df.columns.str.lower()

    print(len(df), "rows read from", file)

    df = df[df['sku'].fillna('') != ''].drop_duplicates()

    df = df.merge(df_plan, how='left', on=['store', 'cntdate'])

    # แยกแถวที่มีค่า branch เป็น NaN เพื่อรายงาน
    df_plan_na = df[df['branch'].isna()]
    df_plan_na = df_plan_na.groupby(['store', 'cntdate','countname']).size().reset_index(name='counts')
    print("❌Batch incorrect in planall2:")
    print(df_plan_na)

    df = df[df['branch'].notna()].drop(columns=['branch'], errors='ignore')

    print(len(df), "rows planall2 processing")

    df = df.merge(df_result, how='left', on=['countname', 'rpname'])

    # แยกแถวที่มีค่า recheck ไม่เป็น NaN เพื่อรายงาน
    df_result_notna = df[df['recheck'].notna()]
    df_result_notna = df_result_notna.groupby(['store', 'cntdate','countname']).size().reset_index(name='counts')
    print("❌Batch not found in result:")
    print(df_result_notna)


    # กรองเฉพาะแถวที่ recheck เป็น NaN และลบ column recheck ทิ้ง
    df = df[df['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

    print(len(df), "rows after check result processing")

    chunksize = 1000
    total_chunks = (len(df) + chunksize - 1) // chunksize

    for i in tqdm(range(total_chunks), desc="uploading to db", unit="chunk"):
        start = i * chunksize
        chunk = df.iloc[start:start + chunksize]
        if not chunk.empty:
            chunk.to_sql(
                name=f"{bu.lower()}_var_this_year",
                con=connect_db3,
                if_exists='append',
                index=False,
                method='multi'
            )
    print(f"✅ {file} uploaded to {bu.lower()}_var_this_year")

def jda_stk (bu,file,path):

    # ========== DATABASE CONNECTION ==========
    connect_db = create_engine(db_connect.db_url_pstdb)
    connect_db3 = create_engine(db_connect.db_url_pstdb3)
    query_plan = text(f"""select stcode as store, cntdate, branch from planall2 where atype = '3F' and bu='{bu.upper()}'""")
    query_result = text(f"""SELECT distinct countname, rpname, 'check' as recheck FROM {bu.lower()}_stk_this_year""")

    # ========== GET DATA FROM TABLE ==========

    df_plan = pd.read_sql(query_plan, con=connect_db)
    df_result = pd.read_sql(query_result, con=connect_db3)


    # ========== READ DATA FROM FILE ==========
    df = pd.read_csv(path, dtype=str,encoding='utf-8',delimiter=',')
    df.columns = df.columns.str.lower()

    print(len(df), "rows read from", file)

    df = df[df['sku'].fillna('') != ''].drop_duplicates()

    df = df.merge(df_plan, how='left', on=['store', 'cntdate'])

    # แยกแถวที่มีค่า branch เป็น NaN เพื่อรายงาน
    df_plan_na = df[df['branch'].isna()]
    df_plan_na = df_plan_na.groupby(['store', 'cntdate','countname']).size().reset_index(name='counts')
    print("❌Batch incorrect in planall2:")
    print(df_plan_na)

    # กรองเฉพาะแถวที่ branch ไม่เป็น NaN และลบ column branch ทิ้ง
    df = df[df['branch'].notna()].drop(columns=['branch'], errors='ignore')

    print(len(df), "rows planall2 processing")

    # รวมข้อมูลกับ df_result เพื่อตรวจสอบรายการที่มีอยู่แล้ว
    df = df.merge(df_result, how='left', on=['countname', 'rpname'])

    # แยกแถวที่มีค่า recheck ไม่เป็น NaN เพื่อรายงาน
    df_result_notna = df[df['recheck'].notna()]
    df_result_notna = df_result_notna.groupby(['store', 'cntdate','countname']).size().reset_index(name='counts')
    print("❌Batch not found in result:")
    print(df_result_notna)

    # กรองเฉพาะแถวที่ recheck เป็น NaN และลบ column recheck ทิ้ง
    df = df[df['recheck'].isna()].drop(columns=['recheck'], errors='ignore')

    print(len(df), "rows after check result processing")

    chunksize = 1000
    total_chunks = (len(df) + chunksize - 1) // chunksize

    for i in tqdm(range(total_chunks), desc="uploading to db", unit="chunk"):
        start = i * chunksize
        chunk = df.iloc[start:start + chunksize]
        if not chunk.empty:
            chunk.to_sql(
                name=f"{bu.lower()}_stk_this_year",
                con=connect_db3,
                if_exists='append',
                index=False,
                method='multi'
            )
    print(f"✅ {file} uploaded to {bu.lower()}_stk_this_year")


#upfront_stk(bu, file_1, path_1)
#upfront_stk(bu, file_2, path_2)
#upfront_stk(bu, file_3, path_3)

#upfront_var(bu, file_2, path_2)
#upfront_var(bu, file_2, path_2)
#upfront_var(bu, file_6, path_6)

jda_stk(bu, file_1, path_1)
#jda_stk(bu, file_4, path_4)

jda_var(bu, file_2, path_2)
#jda_var(bu, file_2, path_2)











