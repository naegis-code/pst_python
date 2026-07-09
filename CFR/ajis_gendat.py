import pathlib
import polars as pl

# ตั้งค่าให้โชว์คอลัมน์ครบ (ไม่ตัด ...)
pl.Config.set_tbl_cols(-1)

user_home = pathlib.Path.home()
filename = 'gendat_CFR_EZ_134_26062026'
extension = '.csv'
path = user_home / 'Downloads' / (filename + extension)

df = pl.read_csv(
    path, 
    has_header=False, 
    infer_schema_length=None
    ).rename({
        'column_1': 'status',
        'column_4': 'seq',
        'column_6': 'location',
        'column_7': 'barcode',
        'column_8': 'cost',
        'column_9': 'cntqnt',
        'column_13': 'timestamp',
    }).with_columns([
        pl.col('status').cast(pl.Utf8),
        pl.col('seq').cast(pl.Int32),
        pl.col('location').cast(pl.Utf8),
        pl.col('barcode').cast(pl.Utf8),
        pl.col('cost').cast(pl.Float32),
        pl.col('cntqnt').cast(pl.Float32),
        pl.col('timestamp').cast(pl.Utf8),
    ])



df = df.sql("""
            SELECT location,barcode ,
                sum(case when status = '1' then cntqnt else 0 end) as cntqnt_1,
                sum(case when status = '5' then cntqnt else 0 end) as cntqnt_5,
                sum(case when status = '9' then cntqnt else 0 end) as cntqnt_9
            FROM self
            group by location,barcode
            """)

print(df)

# Write to Excel
output_path = user_home / 'Downloads' / (filename + '.xlsx')
df.write_excel(output_path)