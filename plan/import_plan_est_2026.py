import polars as pl
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv,find_dotenv
import pathlib
import os

load_dotenv(find_dotenv())

# ตั้งค่าให้โชว์คอลัมน์ครบ (ไม่ตัด ...)
#pl.Config.set_tbl_cols(-1).set_tbl_rows(-1)

engine = create_engine(f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@103.22.182.82:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}")

query = text("""SELECT * FROM date_master;""")
df_date_master = pl.read_database(query, engine)

# Set file path
user_path = pathlib.Path.home()

if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

#"C:\Users\shthanapat\Downloads\Annual Plan 2026 All Update (By Div).xlsx"

#user_path = pathlib.Path("C:/Users/shthanapat/Downloads")

filename = 'Annual Plan 2026 All Update (By Div).xlsx'

path = filepath / 'Report/2026/99 Plan' / filename

table_plan = 'AnnualPlan'
table_est = 'estman'

db_plan = 'plan2026'
db_est = 'est2026'

df_plan = (pl.read_excel(path,table_name=table_plan,infer_schema_length=0)
            .select(
                pl.all().name.to_lowercase())
            .with_columns(
                pl.col("bus.").alias("bu"),
                pl.col("store code").alias("stcode"),
                pl.col("hub").alias("shub"),
                pl.col("food").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("food_soh"),
                pl.col("nonfood").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("nonfood_soh"),
                pl.col("perishable").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("perishable_soh"),
                pl.col("total soh").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("total_soh"),
                pl.col("type").alias("type1"),
                pl.col("total est.man").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("est_man_total"),
                pl.col("est.mancontrol").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("est_man_control"),
                pl.col("est.manexpire").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("est_man_expire"),
                pl.col("est.mancount").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("est_man_count"),
                pl.col("total planmanday").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_pman_total"),
                pl.col("manday control").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_pman_control"),
                pl.col("manday expire2").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_pman_expire"),
                pl.col("manday count").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_pman_count"),
                pl.col("manstore").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_pman_store"),
                pl.col("outsource").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_cman_outsource"),
                pl.col("part-time local").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_pman_pt"),
                pl.col("outsource by div").cast(pl.Decimal(21, 3), strict=False).fill_null(0).alias("div_pman_outsource"),
                pl.col("status การจ้าง outsource").alias("hiring_outsource"),
                pl.col("ประเภทการตรวจนับ").alias("outsource_cnt_type"),
                pl.col("status2").alias("job_status"),
                pl.col("post date").alias("post_date"),
                pl.col("case lp no").alias("case_lp_no"),
                pl.col("case lp date").alias("case_lp_date"),
                pl.col("code for copy").str.replace_all("  ", " ").str.strip_chars().alias("code_for_copy"),
                pl.col('cntdate').str.to_date("%Y-%m-%d %H:%M:%S", strict=False))
            .select([
                'no', 'bu', 'acronym', 'stcode', 'branch', 'province', 'shub', 'food_soh',
                'nonfood_soh', 'perishable_soh', 'total_soh', 'size', 'type1', 'atype',
                'est_man_total', 'est_man_control', 'est_man_expire', 'est_man_count',
                'cntdate', 'day', 'month', 'div_pman_total', 'div_pman_control', 
                'div_pman_expire', 'div_pman_count', 'div_pman_store', 'div_cman_outsource',
                'div_pman_pt', 'div_pman_outsource', 'hiring_outsource', 'outsource_cnt_type',
                'round', 'job_status', 'post_date', 'case_lp_no', 'case_lp_date', 'code_for_copy'])
)

df_est = (pl.read_excel(path,table_name=table_est,infer_schema_length=0)
            .select(pl.all().name.to_lowercase())
            .select('empcode','date','activities','shub','position')
            .with_columns(pl.col('date').str.to_date("%Y-%m-%d %H:%M:%S", strict=False))
            .filter((pl.col('empcode') > '0') & (pl.col('date') > pl.datetime(1899, 12, 31)))
)


df_est = (df_est.join(df_date_master, on='date', how='left')
            .select('empcode','date','activities','shub','position','tdate')
            .with_columns(pl.col('activities').str.replace_all(r"\s+", "").alias('activities_test'))
)


df_est = df_est.sql("""select empcode, date, activities, shub, position,
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
              
          

try:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {db_plan}"))
        conn.execute(text(f"DELETE FROM {db_est}"))

    df_plan.write_database(db_plan, engine, if_table_exists='append')
    df_est.write_database(db_est, engine, if_table_exists='append')
except Exception as e:
    print(e)
