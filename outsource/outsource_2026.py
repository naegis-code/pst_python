import pandas as pd
from sqlalchemy import create_engine, text
import db_connect
import pathlib
from datetime import datetime

# ========== PATH SETUP ==========
print("process start time :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

first_path = pathlib.Path('Report') / '2026' / '99 Plan' / 'Outsource 2026'

file_pieces = filepath / first_path / 'Outsource pieces/All Outsource Pieces 2026.xlsx'
file_manday = filepath / first_path / 'Outsource Manday/All Local Manday 2026.xlsx'

sheet = 'Refresh Annual Plan'

db = create_engine(db_connect.db_url_pstdb)

# Delete old data before inserting new data
q_del_old = text("delete from outsources where cntdate >= '2026-01-01'")
with db.begin() as w:
    w.execute(q_del_old)
    print("✅Old data deleted.")

def outsource_pieces():
    df = pd.read_excel(file_pieces, sheet_name=sheet, usecols='A:AB', skiprows=2, dtype=str)

    # Normalize column names: remove newlines/carriage returns, collapse multiple spaces,
    # strip leading/trailing whitespace and lowercase for consistent matching.
    df.columns = (
        df.columns
        .str.replace('\n', ' ', regex=False)
        .str.replace('\r', ' ', regex=False)
        .str.replace(r"\s+", ' ', regex=True)
        .str.strip()
        .str.lower()
    )

    column_mapping = {
        'bus.': 'bu',
        'store code': 'stcode',
        'branch': 'branch',
        'province': 'province',
        'hub': 'shub',
        'type': 'st_type',
        'cntdate': 'cntdate',
        'ประเภทการตรวจนับของ outsource': 'cnt_type',
        'vendor': 'outsource_name',
        'actual manday control': 'act_man_control',
        'for checking': 'hiring_status',
        'outsource expense': 'outsource_expense',
        'van expense': 'van_expense',
        'total soh outsource': 'total_soh',
        'cost per pieces': 'price_per_piece',
        'expired check': 'expired_check',
        'est. price': 'est_price_per_store',
        'food': 'food_soh',
        'nonfood': 'nonfood_soh',
        'perishable': 'perishable_soh',
        'est. manday': 'est_man_total',
    }

    # rename columns
    df = df.rename(columns=column_mapping)

    df = df[df['hiring_status'].str.contains('YES', na=False)]
    
    #convert numeric columns
    df['actual manday count'] = pd.to_numeric(df['actual manday count'], errors='coerce')
    df['act_man_control'] = pd.to_numeric(df['act_man_control'], errors='coerce')
    df['act_man_outsource'] = df['act_man_control'] + df['actual manday count']
    df['expired_check'] = df.apply(
        lambda row: 1000 if row['bu'] == 'CFR' and row['st_type'] in ['TM', 'FH'] and row['outsource_name'] in ['PCS', 'AJIS']
        else (900 if row['bu'] == 'CFR' and row['st_type'] in ['TM', 'FH'] and row['outsource_name'] == 'SSD' else 0),
        axis=1
    )
    df['food_soh'] = pd.to_numeric(df['food_soh'], errors='coerce')
    df['nonfood_soh'] = pd.to_numeric(df['nonfood_soh'], errors='coerce')
    df['perishable_soh'] = pd.to_numeric(df['perishable_soh'], errors='coerce')

    # get per_piece from plan_speed
    q_speed = text("""select bu,st_type,atype,per_piece from plan_speed where bu = 'CFR' and end_date = '9999-12-31' and per_piece is not null""")
    df_q_speed = pd.read_sql(q_speed, db)
    df = df.merge(
        df_q_speed[['bu','st_type','atype','per_piece']],
        how='left'
    )

    # Calculate estimated mandays
    df['est_man_food'] = df.apply(
        lambda row: round(row['food_soh'] / row['per_piece'], 0) if pd.notna(row['per_piece']) and row['per_piece'] != 0 else 0,
        axis=1
    )
    df['est_man_nonfood'] = df.apply(
        lambda row: round(row['nonfood_soh'] / row['per_piece'], 0) if pd.notna(row['per_piece']) and row['per_piece'] != 0 else 0,
        axis=1
    )
    df['est_man_perishable'] = df.apply(
        lambda row: round(row['perishable_soh'] / row['per_piece'], 0) if pd.notna(row['per_piece']) and row['per_piece'] != 0 else 0,
        axis=1
    )

    df = df.drop(columns=['no', 'for checking','atype','time','outsource ส่งรายชื่อ excel control','outsource ส่งรายชื่อ excel count',
                          'actual soh (ตาม miss rate)','actual manday count','actual price','speed est. soh','per_piece','status'], errors='ignore')

    df['soh_outsource'] = df['food_soh'] + df['nonfood_soh'] + df['perishable_soh']    

    # import new data
    df.to_sql('outsources', con=db, if_exists='append', index=False)
    print("✅New data Pieces inserted. end time :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def outsource_manday():
    df = pd.read_excel(file_manday, sheet_name=sheet, usecols='A:T', skiprows=2, dtype=str)

    # Normalize column names: remove newlines/carriage returns, collapse multiple spaces,
    # strip leading/trailing whitespace and lowercase for consistent matching.
    df.columns = (
        df.columns
        .str.replace('\n', ' ', regex=False)
        .str.replace('\r', ' ', regex=False)
        .str.replace(r"\s+", ' ', regex=True)
        .str.strip()
        .str.lower()
    )

    column_mapping = {
        'actual manday' : 'act_man_outsource',
        'branch' : 'branch',
        'ประเภทการตรวจนับของ outsource' : 'cnt_type',
        'cntdate' : 'cntdate',
        'est. manday ช่อง outsource หรือ outsource by div' : 'est_man_total',
        'est. price' : 'est_price_per_store',
        'for checking' : 'hiring_status',
        'vendor' : 'outsource_name',
        'cost per manday' : 'price_per_piece',
        'province' : 'province',
        'hub' : 'shub',
        'type' : 'st_type',
        'store code' : 'stcode',
        'bus.' : 'bu'
    }

    # rename columns
    df = df.rename(columns=column_mapping)

    df = df[df['hiring_status'].str.contains('YES', na=False)]

    # get per_piece from plan_speed
    db = create_engine(db_connect.db_url_pstdb)

    # drop unused columns
    df = df.drop(columns=['no', 'for checking','atype','time','outsource ส่งรายชื่อ excel control','outsource ส่งรายชื่อ excel',
                          'actual soh (ตาม miss rate)','actual manday count','actual price','speed est. soh','per_piece','status'], errors='ignore')

    # import new data
    df.to_sql('outsources', con=db, if_exists='append', index=False)

    print("✅New data Manday inserted. end time :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
outsource_pieces()
outsource_manday()
