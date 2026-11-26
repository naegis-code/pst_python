import pandas as pd
from sqlalchemy import text
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from tunnel import engine1  # engine1=pstdb
from checklist_common import upsert_df

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'cfr'

src_path = filepath / 'Shared' / 'Checklists_Online' / 'checklist_raw.xlsx'

connect_db = engine1

# =================================
df_cfr_v2 = pd.read_excel(src_path, sheet_name='cfr_v2')

# Select only the necessary columns
keep_columns = [
    'ID', 'Start time','รหัสสาขา','วันที่ตรวจนับ','CFRTMRV2B01','CFRTMRV2B02','CFRTMRV2B03','CFRTMRV2B04','CFRTMRV2B05',
    'CFRTMRV2B06','CFRTMRV2B07','CFRTMRV2B08','CFRTMRV2B09','CFRTMRV2B10','CFRTMRV2B11','CFRTMRV2B12','CFRTMRV2B13',
    'CFRTMRV2B14','CFRTMRV2B15','CFRTMRV2B16','CFRTMRV2B17','CFRTMRV2B18','CFRTMRV2B19','CFRTMRV2B20','CFRTMRV2B21',
    'CFRTMRV2B22','CFRTMRV2B23','CFRTMRV2B24','CFRTMRV2B25','CFRTMRV2B26','CFRTMRV2B27','CFRTMRV2B28','CFRTMRV2B29',
    'CFRTMRV2B30','CFRTMRV2B31','CFRTMRV2B32','CFRTMRV2B33','CFRTMRV2B34','CFRTMRV2B35','CFRTMRV2B36','CFRTMRV2B37',
    'CFRTMRV2F01','CFRTMRV2F02','CFRTMRV2F03','CFRTMRV2F04','CFRTMRV2F05','CFRTMRV2F06','CFRTMRV2F07','CFRTMRV2F08',
    'CFRTMRV2F09','CFRTMRV2F10','CFRTMRV2F11','CFRTMRV2F12','CFRTMRV2F13','CFRTMRV2F14','CFRTMRV2F15','CFRTMRV2F16',
    'CFRTMRV2F17','CFRTMRV2F18','CFRTMRV2F19','CFRTMRV2F20','CFRTMRV2F21','CFRTMRV2F22','CFRTMRV2F23','CFRTMRV2F24',
    'CFRTMRV2F25','CFRTMRV2F26','CFRTMRV2F27','CFRTMRV2F28','CFRTMRV2F29','CFRTMRV2F30','CFRTMRV2F31'
]
df_cfr_v2 = df_cfr_v2[keep_columns]

# Rename columns to match the checklist table
df_cfr_v2.rename(columns={'ID':'id','Start time':'checkdate','รหัสสาขา':'stcode','วันที่ตรวจนับ':'cntdate'}, inplace=True)

# Remove duplicates based on 'stcode' and 'cntdate', keeping the latest entry based on 'id'
df_cfr_v2 = df_cfr_v2.sort_values(by='id', ascending=False).drop_duplicates(subset=['stcode', 'cntdate'])

# Convert checkdate and cntdate to yyyymmdd format
df_cfr_v2['checkdate'] = pd.to_datetime(df_cfr_v2['checkdate']).dt.strftime('%Y%m%d')
df_cfr_v2['cntdate'] = pd.to_datetime(df_cfr_v2['cntdate'], format='%d/%m/%Y').dt.strftime('%Y%m%d')

# Unpivot the DataFrame to have a long format
df_cfr_v2 = df_cfr_v2.melt(
    id_vars=['id', 'checkdate', 'stcode', 'cntdate'],
    var_name='question_code',
    value_name='point'
)


df_mapping = pd.read_excel(src_path, sheet_name='Mapping')

df_mapping.columns = df_mapping.columns.str.lower()

df_mapping.rename(columns={'code':'question_code'}, inplace=True)

df_cfr_v2 = df_cfr_v2.merge(
    df_mapping[['question_code', 'bu', 'zone', 'weight', 'no','subject','desceiption',
                'subdescription']],
    on='question_code',
    how='left'
)

rename_columns = {
    'subject':'section',
    'desceiption':'subject'
}   

df_cfr_v2.rename(columns=rename_columns, inplace=True)

# Calculate 'full' and 'act' columns
df_cfr_v2['full'] = 5 * df_cfr_v2['weight']
df_cfr_v2['act'] = df_cfr_v2['point'] * df_cfr_v2['weight']

# Replace 'zone' values: 'B' with 'Back' and 'F' with 'Sale'
df_cfr_v2['zone'] = df_cfr_v2['zone'].replace({'B': 'Back', 'F': 'Sale'})

# check branch from planall2
df_plan = pd.read_sql(
    text(f"SELECT bu, stcode, cntdate, branch FROM planall2 WHERE bu = '{bu.upper()}'"),
    connect_db
)
df_cfr_v2 = df_cfr_v2.merge(
    df_plan,
    on=['bu', 'stcode', 'cntdate'],
    how='left'
)
# แจ้งสาขา+วันที่ใน Excel ที่ไม่ตรง planall2 (เช่น หน้าร้านใส่วันที่/รหัสสาขาผิด) — ไม่ถูกโหลด
df_unmatched = (
    df_cfr_v2.loc[df_cfr_v2['branch'].isna(), ['stcode', 'cntdate']]
    .drop_duplicates()
    .sort_values(['cntdate', 'stcode'])
)
if not df_unmatched.empty:
    print(f"⚠️ ไม่ตรง planall2 {len(df_unmatched)} ชุด (ไม่ถูกโหลด):")
    print(df_unmatched.to_string(index=False))

df_cfr_v2 = df_cfr_v2[df_cfr_v2['branch'].notna()].drop(columns=['branch','id'], errors='ignore')

print(df_cfr_v2.shape)
# UPSERT ตามคีย์ (stcode, cntdate, question_code) — รันด้วย --dry-run เพื่อดูจำนวนโดยไม่เขียนข้อมูล
upsert_df(connect_db, df_cfr_v2, bu)

