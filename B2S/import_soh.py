import pandas as pd
from sqlalchemy import create_engine, text
import os

# Define the path to the CSV file
path_soh = r'D:\Users\prthanap\OneDrive - Central Group\python\B2S\soh.csv'

# Create a connection to the PostgreSQL database
engine = create_engine('postgresql+psycopg2://prthanapat:20020015@103.22.182.82:5432/pstdb3')

# Define the column names
column_names = ['msstor','msstrn','mstrnc','mstrnd','mstype','msvdno','msvdnm','msdept','msdptn','mssdpt',
                'mssdpn','msbrnd','msclas','msclsn','msscls','msscln','mssku','mssdes','msibc','mssbc',
                'mspopg','mscatl','msskus','msstkr','msstkc','msstoh','msregp','msorgp','msancp','msascn',
                'msasdt','mspoor','mstoor','msrtvv','msrtvi','msdist','msobsf','msmqty','msage','msaget',
                'att_nam_1','att_val_1','att_desc_1','att_nam_2','att_val_2','att_desc_2','att_nam_3','att_val_3','att_desc_3','att_nam_4',
                'att_val_4','att_desc_4','att_nam_5','att_val_5','att_desc_5','att_nam_6','att_val_6','att_desc_6','att_nam_7','att_val_7',
                'att_desc_7','preord','mbyum','mslum','mstdpk','rpl_code','special_attribute'
                ]

# Read the CSV file without headers
df = pd.read_csv(path_soh, encoding='ANSI', header=None, names=column_names)

# Write the DataFrame to the PostgreSQL table 'b2s_soh'
df.to_sql('b2s_soh', engine, if_exists='append', index=False)



'''
# Create a connection to the database
engine = create_engine('postgresql+psycopg2://prthanapat:20020015@103.22.182.82:5432/pstdb3')
query = text('SELECT * FROM b2s_master_initial')
df_pg = pd.read_sql(query, engine)


path_db = r'D:\Program Files\B2S\Apps\pda-master.db'
engine = create_engine(f'sqlite:///{path_db}')

# อ่านข้อมูลจากตาราง b2s_master_initial
query_sqlite = 'SELECT * FROM b2s_master_initial'
df_db = pd.read_sql(query_sqlite, engine)

print(df_db)
'''