import pandas as pd
from sqlalchemy import create_engine
import pathlib
import db_connect
from datetime import datetime, timedelta
import os
from openpyxl import load_workbook
import shutil
import pathlib

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'
# Specify the folder containing the Excel files

f_path = filepath / 'Shared' / 'Performance' / 'report' / 'report3' / 'SSP' / 'Y2026 WDS from mail'

file_name = '090731_2026-03-04_Final Diff (เดอะมอลล์ท่าพระ).xls'

df = pd.read_excel(f_path / file_name, sheet_name='ข้อมูลตรวจนับ', usecols='B:Q', dtype=str, skiprows=5)

print(df.head(5))