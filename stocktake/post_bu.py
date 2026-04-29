import pandas as pd
from sqlalchemy import create_engine,text
from datetime import datetime
import db_connect
import numpy as np
import pathlib

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

export_path = filepath / 'Apps'

engine = create_engine(db_connect.db_url_pstdb)

query_plan = text("""SELECT * FROM planall2
                  Where atype in ('3F','3Q','4V')""")

query_holiday = text("""SELECT date FROM plan_holidays""")   

df_plan = pd.read_sql_query(query_plan, engine)
df_holiday = pd.read_sql_query(query_holiday, engine)

df_holiday['date'] = pd.to_datetime(df_holiday['date'], format='%Y-%m-%d')

df_4v = df_plan[df_plan['atype'] == '4V']
df_4v['year'] = df_4v['cntdate'].str[:4]
df_4v.drop(columns=['acronym','branch','province','shub','type1','size','job_status','post_date','hiring_outsource','outsource_cnt_type','code_for_copy'], inplace=True)

df_3F_3Q = df_plan[df_plan['atype'].isin(['3F','3Q'])]
df_3F_3Q['year'] = df_3F_3Q['cntdate'].str[:4]

df = pd.merge(df_3F_3Q, df_4v, on=['bu','stcode', 'year','round'], how='left')

df['cntdate_cal'] = np.where(df['cntdate_y'].isna(), df['cntdate_x'], df['cntdate_y'])

df.rename(columns={'atype_x': 'atype',
                   'cntdate_x': 'cntdate',
                   }, inplace=True)

df.drop(columns=['atype_y', 'cntdate_y','year'], inplace=True)

#convert cntdate_cal and cntdate and post_date to datetime format
df['cntdate_cal'] = pd.to_datetime(df['cntdate_cal'], format='%Y%m%d')
df['cntdate'] = pd.to_datetime(df['cntdate'], format='%Y%m%d')
df['post_date'] = pd.to_datetime(df['post_date'], format='%Y%m%d').fillna(0)

df['PostAging'] = np.busday_count(df['cntdate_cal'].values.astype('datetime64[D]'), 
                                  df['post_date'].values.astype('datetime64[D]'), 
                                  holidays=df_holiday['date'].values.astype('datetime64[D]'))

# --- ส่วนการจัด Group (ใช้ pd.cut จะแม่นยำและเร็วกว่ามาก) ---

# 1. กำหนดช่วง (bins) และ ชื่อกลุ่ม (labels)
# ช่วง: [0, 2, 4, 6, 8, 10, 16, 31, infinity]
# หมายเหตุ: รวม 0 ถึง 1, รวม 2 ถึง 3...
bins = [-1,1, 3, 5, 7, 9, 15, 30, np.inf]
labels = ['G 0-1', 'G 2-3', 'G 4-5', 'G 6-7', 'G 8-9', 'G 10-15', 'G 16-30', 'G >30']

# 2. ใช้ pd.cut เพื่อสร้าง Column Group
df['Group'] = pd.cut(df['PostAging'], bins=bins, labels=labels)

df['Group'] = np.where(df['Group'].isna() & (df['cntdate_cal'] <= pd.to_datetime(pd.Timestamp.now(), format='%Y%m%d')), 'Pending', df['Group'])
df['Group'] = np.where(df['Group'].isna() & (df['cntdate_cal'] > pd.to_datetime(pd.Timestamp.now(), format='%Y%m%d')), 'Not Yet', df['Group'])
df['PostAging'] = np.where(df['PostAging'] < 0, 'null', df['PostAging'])
df['post_date'] = np.where(df['post_date'] == 0, 'null', df['post_date'])

df.rename(columns={'bu': 'BUs.', 'stcode': 'Store Code', 'branch': 'Branch', 'province': 'Province', 'shub': 'HUB',
                   'type1': 'Type', 'atype': 'Atype', 'cntdate': 'CNTDATE', 'job_status': 'Status2', 'post_date': 'POST DATE'
                   }, inplace=True)

df.drop(columns=['cntdate_cal','code_for_copy','hiring_outsource','size','round','acronym','outsource_cnt_type'], inplace=True)

df.to_csv(export_path / 'post_aging.csv', index=False, encoding='utf-8-sig')
print(f"✅ Exported post_aging.csv to {export_path} ({len(df)} rows)")
