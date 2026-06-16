import polars as pl
from sqlalchemy import create_engine,text
import pathlib

# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

#Report\2026\00 Summary Report Stock\08CFR_Special
path = filepath / 'Report' / '2026' / '00 Summary Report Stock' / '08CFR_Special' / 'ITEMMASTER.xlsx'
sname = 'Sheet1'
cols = ['Barcode','Item','ItemNameTH']

df = pl.read_excel(path, sheet_name=sname).select(cols).with_columns(pl.col(c).cast(pl.Utf8) for c in cols)
df = df.with_columns(pl.col('ItemNameTH').str.contains(r'[\u0E00-\u0E7F]').alias('has_thai')).sort('has_thai', descending=True) # เรียงข้อมูลโดยให้แถวที่มีภาษาไทยอยู่ด้านบน


df = df.unique(subset=['Barcode', 'Item'], keep='first', maintain_order=True)
print(df)

