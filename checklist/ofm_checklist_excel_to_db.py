import pandas as pd
from sqlalchemy import text
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from tunnel import engine1  # engine1=pstdb
from checklist_common import replace_sets

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'ofm'
sheet  = 'ofm_v1'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = engine1

# =================================
df = pd.read_excel(src_path, sheet_name=sheet,dtype=str)

# Select only the necessary columns
keep_columns = [
    'ID', 'Start time','stcode','check_date',
    'OFMV1A01','OFMV1A02','OFMV1A03','OFMV1A04','OFMV1A05','OFMV1A06','OFMV1A07','OFMV1A08','OFMV1A09','OFMV1A10',
    'OFMV1A11','OFMV1A12','OFMV1A13','OFMV1A14','OFMV1A15','OFMV1A16','OFMV1A17','OFMV1A18','OFMV1A19','OFMV1A20',
    'OFMV1A21','OFMV1A22','OFMV1A23','OFMV1A24','OFMV1A25','OFMV1A26','OFMV1A27','OFMV1A28','OFMV1A29','OFMV1A30',
    'OFMV1A31','OFMV1A32','OFMV1A33','OFMV1A34','OFMV1A35',

    'OFMV1B01','OFMV1B02','OFMV1B03','OFMV1B04','OFMV1B05','OFMV1B06','OFMV1B07','OFMV1B08','OFMV1B09',
    'OFMV1B10','OFMV1B11','OFMV1B12','OFMV1B13','OFMV1B14','OFMV1B15','OFMV1B16','OFMV1B17',

    'OFMV1F01','OFMV1F02','OFMV1F03','OFMV1F04','OFMV1F05','OFMV1F06','OFMV1F07','OFMV1F08','OFMV1F09'

]

df = df[keep_columns]

# 2️⃣ dtype ของแต่ละ column
dtype_map = {
    'ID': 'Int64',
    'Start time': 'datetime64[ns]',
    'stcode': 'string',
    'check_date': 'datetime64[ns]',
}

# auto ใส่ int ให้ OFMV1A01–OFMV1F09
dtype_map.update({f'OFMV1A{i:02d}': 'Int64' for i in range(1, 36)})
dtype_map.update({f'OFMV1B{i:02d}': 'Int64' for i in range(1, 18)})
dtype_map.update({f'OFMV1F{i:02d}': 'Int64' for i in range(1, 10)})

# select + cast
df = df.astype(dtype_map)

df.rename(columns={'ID':'id','Start time':'checkdate','check_date':'cntdate'}, inplace=True)

df = df.sort_values(by='id', ascending=False).drop_duplicates(subset=['stcode', 'cntdate'])

# Convert checkdate and cntdate to yyyymmdd format
df['checkdate'] = pd.to_datetime(df['checkdate']).dt.strftime('%Y%m%d')
df['cntdate'] = pd.to_datetime(df['cntdate'], format='%d/%m/%Y').dt.strftime('%Y%m%d')

# Unpivot the DataFrame to have a long format
df = df.melt(
    id_vars=['id', 'checkdate', 'stcode', 'cntdate'],
    var_name='question_code',
    value_name='point'
)


df_mapping = pd.read_excel(src_path, sheet_name='Mapping')

df_mapping.columns = df_mapping.columns.str.lower()

df_mapping.rename(columns={'code':'question_code'}, inplace=True)

df = df.merge(
    df_mapping[['question_code', 'bu', 'zone', 'weight', 'no','subject','desceiption',
                'subdescription']],
    on='question_code',
    how='left'
)

rename_columns = {
    'subject':'section',
    'desceiption':'subject'
}   

df.rename(columns=rename_columns, inplace=True)

# Calculate 'full' and 'act' columns
df['full'] = 5 * df['weight']
df['act'] = df['point'] * df['weight']

# Replace 'zone' values: 'B' with 'Back' and 'F' with 'Sale'
df['zone'] = df['zone'].replace({'B': 'Back', 'F': 'Sale'})

# check branch from planall2
df_plan = pd.read_sql(
    text(f"SELECT bu, stcode, cntdate, branch FROM planall2 WHERE bu = '{bu.upper()}'"),
    connect_db
)
df = df.merge(
    df_plan,
    on=['bu', 'stcode', 'cntdate'],
    how='left'
)

# แจ้งสาขา+วันที่ใน Excel ที่ไม่ตรง planall2 (เช่น หน้าร้านใส่วันที่/รหัสสาขาผิด) — ไม่ถูกโหลด
df_unmatched = (
    df.loc[df['branch'].isna(), ['stcode', 'cntdate']]
    .drop_duplicates()
    .sort_values(['cntdate', 'stcode'])
)
if not df_unmatched.empty:
    print(f"⚠️ ไม่ตรง planall2 {len(df_unmatched)} ชุด (ไม่ถูกโหลด):")
    print(df_unmatched.to_string(index=False))

df = df[df['branch'].notna()].drop(columns=['branch','id'], errors='ignore')

print(df.shape)
# แทนที่ทีละชุด (bu + stcode + cntdate) — รันด้วย --dry-run เพื่อดูจำนวนโดยไม่เขียนข้อมูล
replace_sets(connect_db, df, bu)

