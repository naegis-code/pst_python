import polars as pl
from sqlalchemy import create_engine
from datetime import datetime
import db_connect as db

#Parameters
table = 'cfr_stk_this_year'
start_date = '20260101'
end_date = '20261231'
st_codes = ['443','140','005']
export_path = r"D:\Users\prthanap\Documents\cfr_stk_this_year.csv"

print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

db3 = create_engine(db.db_url_pstdb3)

query = f"""
             select *
            from {table}
            where cntdate between '{start_date}' and '{end_date}'
                and stcode in({', '.join(f"'{code}'" for code in st_codes)})"""

df = pl.read_database(query, db3).with_row_index("index")

print(df)
df.write_csv(export_path)
print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Use Time: {(datetime.now() - datetime.strptime(start_date, '%Y%m%d'))}")
