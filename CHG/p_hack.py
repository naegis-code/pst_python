import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text
from datetime import datetime
import db_connect
import pathlib

# =============================
# CONFIG
# =============================
datestart = '20250101'
dateend   = '20251231'

user_path = pathlib.Path.home()
base_path = user_path / 'Documents'

path_export_all    = f'{base_path}\\p_hack_all_{datestart}_to_{dateend}.csv'
path_export_twd    = f'{base_path}\\p_hack_twd_{datestart}_to_{dateend}.csv'
path_export_hrbrid = f'{base_path}\\p_hack_hrbrid_{datestart}_to_{dateend}.csv'
path_export_small  = f'{base_path}\\p_hack_small_{datestart}_to_{dateend}.csv'

TWD = [
    '60921','60922','60923','60924','60925','60926','60927','60928','60929','60930',
    '60931','60932','60933','60934','60935','60936','60937','60938','60939','60940',
    '60941','60942','60944','60945','60946','60947','60951','60952','60953','60954',
    '60955','60956','60958','60959','60960','60961','60962','60963','60964','60966',
    '60968','60969','60970','60971','60972','60973','60975','60976','60978','60979',
    '60980','60981','60982','60983','60984','60985','60987','60988','60989','60990',
    '60991','60992','60993','60994','60995','60996','60997','61600'
]

Hybrid = [
    '60016','60020','60022','60023','60024','60025','60026','60027','60028','60029',
    '60030','60031','60918','60919','60920'
]

Small = ['60974','60986']


# =============================
# HELPER
# =============================
def sql_in_list(lst):
    """ convert python list -> SQL IN ('x','y') """
    return ','.join(f"'{x}'" for x in lst)


def build_query(stmerch_list=None):
    merch_filter = ""
    if stmerch_list:
        merch_filter = f"AND cs.stmerch IN ({sql_in_list(stmerch_list)})"

    return text(f"""
        WITH master AS (
            SELECT DISTINCT ON (sku)
                sku,
                prname
            FROM chg_stk
            WHERE rpname = 'STK2'
              AND cntdate BETWEEN '{datestart}' AND '{dateend}'
        )
        SELECT 
            cs.sku,
            m.prname,
            COUNT(*) FILTER (WHERE cs.varianceqnt = 0) AS eq0,
            COUNT(*) FILTER (WHERE cs.varianceqnt > 0) AS gain,
            COUNT(*) FILTER (WHERE cs.varianceqnt < 0) AS loss,
            COUNT(*) AS overall
        FROM chg_stk cs
        JOIN master m ON cs.sku = m.sku
        WHERE cs.rpname = 'STK2'
          AND cs.cntdate BETWEEN '{datestart}' AND '{dateend}'
          {merch_filter}
        GROUP BY cs.sku, m.prname
        ORDER BY overall DESC
    """)


# =============================
# DATABASE
# =============================
engine = create_engine(db_connect.db_url_pstdb)

print(f"✅ Start Process at {datetime.now():%Y-%m-%d %H:%M:%S}")


# =============================
# EXPORT FUNCTIONS
# =============================
def export_csv(query, path):
    df = pl.read_database(query, engine)
    df.write_csv(path)
    print(f"✔ Exported {path} ({df.height} rows) at {datetime.now():%Y-%m-%d %H:%M:%S}")


# =============================
# RUN
# =============================
export_csv(build_query(), path_export_all)
export_csv(build_query(TWD), path_export_twd)
export_csv(build_query(Hybrid), path_export_hrbrid)
export_csv(build_query(Small), path_export_small)

print("🎉 All data exported successfully.")
print(f"✅ End Process at {datetime.now():%Y-%m-%d %H:%M:%S}")