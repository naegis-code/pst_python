import polars as pl
from sqlalchemy import create_engine, text
import shutil
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tunnel import ensure_tunnel, engine1,engine3
# ---------- ตั้งค่า ----------
bu = 'B2S'
stcode = '50008'
atype = '3F'
cntdate = '20261027'
path_master = 'D:\\Master.db'
countName = bu + stcode + atype[-1] + cntdate

timestamp = datetime.now().strftime("%Y%m%d%H%M")   # 12 หลัก: ปีเดือนวันชั่วโมงนาที
dest = f"D:\\Master_{countName}_{timestamp}.db"

# ---------- สร้างรหัส location ----------

df_front = pl.DataFrame({
    "zone": [1, 3, 5],
    "gon":  [99, 99, 99],
    "bay":  [150, 150, 150],
})

df_back = pl.DataFrame({
    "zone": ["B"],
    "bay":  [1000],
})

# ---------- สร้างรหัส location ----------

result = (
    df_front
    .with_columns(
        gon=pl.int_ranges(1, pl.col("gon") + 1),
        bay=pl.int_ranges(1, pl.col("bay") + 1),
    )
    .explode("gon")
    .explode("bay")
    .with_columns(
        location=pl.concat_str([
            pl.col("zone").cast(pl.Utf8),
            pl.col("gon").cast(pl.Utf8).str.zfill(2),
            pl.col("bay").cast(pl.Utf8).str.zfill(3),
        ])
    )
    .select("location")
)

result_back = (
    df_back
    .with_columns(bay=pl.int_ranges(1, pl.col("bay") + 1))
    .explode("bay")
    .with_columns(
        location=pl.concat_str([
            pl.col("zone"),
            pl.col("bay").cast(pl.Utf8).str.zfill(5),
        ])
    )
    .select("location")
)

df_location_masters = pl.concat([result, result_back]).with_columns(
    stocktakeid=pl.lit(countName)
)

# ---------- connect to databases ----------

engine_master = create_engine(f"sqlite:///{path_master}")

# ---------- ดึง plan ----------
query_plan = """
    SELECT branch
    FROM planall2
    WHERE bu = :bu
      AND stcode = :stcode
      AND atype = :atype
      AND cntdate = :cntdate
"""

params = {"bu": bu, "stcode": stcode, "atype": atype, "cntdate": cntdate}

plan = pl.read_database(
    query_plan,
    connection=engine1,
    execute_options={"parameters": params},
)

if not plan.is_empty():
    
    storeCode = stcode
    branch = plan["branch"][0]
    storeName = branch

    df_users = pl.read_database(
        """
        SELECT employee_code as username,
               encryptedpassword,
               employee_code as "empCode"
        FROM employees
        WHERE last_work_date is null or last_work_date > CURRENT_DATE
        """,
        connection=engine1,
    )
    df_extra_users = pl.DataFrame({
        "username": [f"{i:03d}" for i in range(1, 51)],
    }).with_columns(
        encryptedpassword=pl.lit("$2a$10$d.QrLlbWIQwZ/hoqBFxCVeSJTvQTyy/KSW7kh3Rf6bnMcnKWpKCrS"),
        empCode=pl.col("username"),
    )

    df_users = pl.concat(
        [df_users, df_extra_users.select(df_users.columns)],
        how="vertical_relaxed",
    )

    query_get_master = """
        SELECT
            upc as ibc,
            des as "productName",
            0 as stock,
            price as "retailPrice",
            'A' as status
        FROM b2s_master_bar
    """

    df_get_master = (
        pl.read_database(query_get_master, connection=engine3)
        .with_columns(
            pl.col("retailPrice").cast(pl.Float64),
            sku=pl.col("ibc").cast(pl.Utf8).str.zfill(13)   # pad barcode
        )
        .with_columns(
            barcodeIBC=pl.col("sku"),
            stocktakeid=pl.lit(countName),
            storeCode=pl.lit(storeCode),
            storeName=pl.lit(storeName),
        )
        .drop("ibc")
    )

    with engine_master.begin() as connection:
        connection.execute(text("""
            UPDATE stocktakes
            SET countName = :countName,
                storeCode = :storeCode,
                bu = :bu,
                branch = :branch,
                storeName = :storeName
            WHERE id = 1
        """), {
            "countName": countName,
            "storeCode": storeCode,
            "bu": bu,
            "branch": branch,
            "storeName": storeName,
        })
        print(f"Updated stocktakes with new {countName}, {storeCode}, {bu}, {branch}, and {storeName}")

        connection.execute(text("DELETE FROM pda_masters"))
        print("Deleted all rows from pda_masters")
        connection.execute(text("DELETE FROM users"))
        print("Deleted all rows from users")
        connection.execute(text("DELETE FROM location_masters"))
        print("Deleted all rows from location_masters")
        connection.execute(text("DELETE FROM sqlite_sequence WHERE name='location_masters'"))
        print("Reset sqlite_sequence for location_masters")
        connection.execute(text("DELETE FROM sqlite_sequence WHERE name='pda_masters'"))
        print("Reset sqlite_sequence for pda_masters")
        connection.execute(text("DELETE FROM sqlite_sequence WHERE name='users'"))
        print("Reset sqlite_sequence for users")

        df_get_master.write_database(
            "pda_masters", connection=connection,
            if_table_exists="append", engine="sqlalchemy",
        )
        print(f"✅ Inserted Master {df_get_master.height} records")
        df_users.write_database(
            "users", connection=connection,
            if_table_exists="append", engine="sqlalchemy",
        )
        print(f"✅ Inserted Users {df_users.height} records")

        df_location_masters.write_database(
            "location_masters", connection=connection,
            if_table_exists="append", engine="sqlalchemy",
        )
        print(f"✅ Inserted Location {df_location_masters.height} records")


    # VACUUM (ต้องอยู่นอก transaction)
    with engine_master.connect() as conn:
        conn.execute(text("VACUUM"))
        print("Vacuumed the master database")

    shutil.copy(path_master, dest)
    print(f"✅ Master database created: {dest}")

else:
    print("No plan found for the given parameters.")