import polars as pl
import psycopg2
import db_connect as dbc
import pathlib

# ตั้งค่าให้โชว์คอลัมน์ครบ (ไม่ตัด ...)
pl.Config.set_tbl_cols(-1).set_tbl_rows(-1)

engine = psycopg2.connect(dbc.db_url_pstdb_native)
query = 'SELECT * FROM date_master;'
df_date_master = pl.read_database(query, engine)


# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

filename = 'Annual Plan 2026 All Update (By Div).xlsx'
table_name = 'est2026'
path = filepath / 'Report/2026/99 Plan' / filename

df = (pl.read_excel(path, table_name='estman')
        .select(pl.all().name.to_lowercase())
        .select('empcode','date','activities','shub','position')
        .filter((pl.col('empcode') > '0') & (pl.col('date') > pl.datetime(1899, 12, 31))))
    
df = (df.join(df_date_master, on='date', how='left').
      select('empcode','date','activities','shub','position','tdate').
      with_columns(pl.col('activities').
                   str.replace_all(r"\s+", "").alias('activities_test')))


df2 = df.sql("""select empcode, date, activities, shub, position,
                    case when activities_test like '%พักร้อน%' then 'Annual Leave'
                         when activities_test like '%วันลา%' then 'Take Leave'
                         when activities_test like '%(C)%' then 'Checklist'
                         when activities_test like '%(CH)%' then 'Checklist at Home'
                         when activities_test like '%(F)%' then 'Fullcount'
                         when activities_test like '%(P)%' then 'Precount'
                         when activities_test like '%(Q)%' then 'Quarterly'
                         when activities_test like '%(T)%' then 'Travelling'
                         when activities_test like '%(V)%' then 'Variance'
                         when activities_test like '%(M)%' or activities_test like '%(O)%' then 'Supportive'
                         when activities_test like '%(VI)%' then 'Visit'
                         when activities_test like '%ชดเชย%' and tdate = 'Working' then 'Compensate'
                         when activities_test like '%วันหยุด%' or activities_test = '' and (tdate = 'Holiday' or tdate = 'Weekend') then 'Weekend/Holiday'
                         when activities_test = '' and tdate = 'Working' then 'Vacant'
                         else 'Can''t identify'
                    end as activity_type
                from self
             """)

df0 = df2.sql("""select *
                from self
                where activity_type = 'Can''t identify'
              """)
print(df0)

df3 = df2.sql("""select activity_type, count(*) as count_activity
                from self
                group by activity_type
              """)
print(df3)

try:
    db_url = dbc.db_url_pstdb_native
    df2.write_database(
        table_name="est2026",
        connection=db_url,
        if_table_exists="replace"
    )
    print(f'Inserted {len(df2)} records into est2026 table.')
except Exception as e:
    print(f"Error inserting records into est2026 table: {e}")


