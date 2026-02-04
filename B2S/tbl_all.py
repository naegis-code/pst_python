import os
import shutil
import pathlib
import time
import pandas as pd
from sqlalchemy import create_engine, text
import db_connect
from tqdm import tqdm

# -------------------------
# Configuration
bu = "B2S"
stcode = "50008"
cntdate = "031025"  # ddmmyy

# -------------------------
# Utility Functions
# -------------------------

def get_engine(db_url: str):
    """Create SQLAlchemy engine from db url."""
    return create_engine(db_url, pool_pre_ping=True, future=True)


def run_query(engine, query, params=None, fetch=True):
    """Execute SQL query with optional params."""
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return result.fetchall() if fetch else None


def run_exec(engine, query, params=None):
    """Execute SQL statement with autocommit (INSERT/UPDATE/DELETE)."""
    with engine.begin() as conn:
        conn.execute(text(query), params or {})


def reset_sqlite_table(engine, table_name: str):
    """Clear a table and reset its AUTOINCREMENT (if exists)."""
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table_name};"))
        conn.execute(text(f"DELETE FROM sqlite_sequence WHERE name='{table_name}';"))


# -------------------------
# Main Workflow
# -------------------------

def main():
    count_name = f"{bu.upper()}{stcode}F{cntdate}001"

    print(f"Processing stocktake ID: {count_name}")

    # Connections
    engine_db = get_engine(db_connect.db_url_pstdb)
    engine_db3 = get_engine(db_connect.db_url_pstdb3)
    sqlite_path = r"D:\Program Files\B2S\Apps\pda-master.db"
    engine_sqlite = get_engine(f"sqlite:///{sqlite_path}")

    # --- Step 1: Validate store from plan_dba
    store_info = pd.read_sql_query(
        text("SELECT bu, stcode, stname_th as branch "
             "FROM plan_dba WHERE bu = :bu AND stcode = :stcode"),
        engine_db, params={"bu": bu, "stcode": stcode}
    )
    if store_info.empty:
        print(f"❌ Store code {stcode} not found in plan_dba.")
        return
    store_name = store_info.at[0, "branch"]

    # --- Step 2: Insert into stocktakeid (if not exists)
    cntdate_fmt = f"20{cntdate[4:]}-{cntdate[2:4]}-{cntdate[:2]}"
    exists = run_query(
        engine_db3,
        "SELECT 1 FROM stocktakeid WHERE cntnum = :cntNum AND bu = :bu AND stcode = :stcode AND cntdate = :cntdate",
        {"cntNum": count_name, "bu": bu, "stcode": stcode, "cntdate": cntdate_fmt}
    )
    if exists:
        print(f"❌ Stocktake ID {count_name} already exists")
    else:
        run_exec(engine_db3,
                 "INSERT INTO stocktakeid (cntnum, bu, stcode, cntdate, atype, status) "
                 "VALUES (:cntNum, :bu, :stcode, :cntdate, 'F', 'Count_1')",
                 {"cntNum": count_name, "bu": bu, "stcode": stcode, "cntdate": cntdate_fmt})
        print(f"✅ Stocktake ID {count_name} inserted")

    # --- Step 3: Update SQLite stocktakes
    run_exec(engine_sqlite, """
        UPDATE stocktakes SET 
            countName = :countName,
            storeCode = :storeCode,
            storeName = :storeName,
            bu = :bu,
            branch = :branch
        WHERE id = 1;
    """, {"countName": count_name, "storeCode": stcode, "storeName": store_name,
          "bu": bu, "branch": store_name})

    print("✅ Stocktake info updated in SQLite")

    # --- Step 4: Refresh users
    run_exec(engine_sqlite, "DELETE FROM users WHERE position NOT IN ('Admin','Outsource')")
    df_users = pd.read_sql("""
        SELECT employee_code as username, email, encryptedpassword as "encryptedPassword",
               employee_code as empCode,
               split_part(eng_name, ' ', 1) AS "firstName",
               split_part(eng_name, ' ', 2) AS "lastName",
               first_name as "firstNameTh", last_name as "lastNameTh",
               sub_hub as hub, position
        FROM employees WHERE job_status IS NULL
    """, engine_db)
    df_users.to_sql("users", engine_sqlite, if_exists="append", index=False)
    print(f"✅ Users refreshed ({len(df_users)} records)")

    # --- Step 5: Refresh location_masters
    reset_sqlite_table(engine_sqlite, "location_masters")
    df_loc = pd.read_sql(
    text("SELECT location, cntnum as stocktakeId FROM topscare_location WHERE cntnum = :cntNum"),
    engine_db3, params={"cntNum": count_name}
    )
    df_loc.to_sql("location_masters", engine_sqlite, if_exists="append", index=False)
    print(f"✅ location_masters updated ({len(df_loc)} records)")

    # --- Step 6: Refresh pda_masters
    reset_sqlite_table(engine_sqlite, "pda_masters")
    df_master = pd.read_sql("""
        SELECT vendor as "vendorCode", v_name as "vendorName",
               lpad(sku_no, 13, '0') as sku, lpad(sku_no, 13, '0') as "barcodeIBC",
               CASE WHEN ibc = '0' THEN NULL ELSE lpad(ibc, 13, '0') END as barcode1,
               CASE WHEN "sbc#1" = '0' THEN NULL ELSE lpad("sbc#1", 13, '0') END as barcode2,
               CASE WHEN "sbc#2" = '0' THEN NULL ELSE lpad("sbc#2", 13, '0') END as barcode3,
               CASE WHEN "sbc#3" = '0' THEN NULL ELSE lpad("sbc#3", 13, '0') END as barcode4,
               CASE WHEN "sbc#4" = '0' THEN NULL ELSE lpad("sbc#4", 13, '0') END as barcode5,
               CASE WHEN "sbc#5" = '0' THEN NULL ELSE lpad("sbc#5", 13, '0') END as barcode6,
               regexp_replace(sku_descr, E'[\\n\\r,]', '', 'g') as "productName",
               color_des as "color", inner_pack as "unitOfMeasure", size_des as "size",
               reg_retail as "cost", 0 as stock, 'A' as status
        FROM b2s_master WHERE sku_type <> '03'
    """, engine_db3)

    stocktake_info = pd.read_sql("SELECT countName as stocktakeId, storeCode, storeName FROM stocktakes", engine_sqlite)
    df_master = df_master.assign(
        stocktakeId=stocktake_info.at[0, "stocktakeId"],
        storeCode=stocktake_info.at[0, "storeCode"],
        storeName=stocktake_info.at[0, "storeName"]
    )
    df_master.to_sql("pda_masters", engine_sqlite, if_exists="append", index=False)

    run_exec(engine_sqlite, "VACUUM")
    print(f"✅ pda_masters updated ({len(df_master)} records)")

    # --- Step 7: Export DB
    export_path = pathlib.Path.home() / "Downloads"
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    export_filename = f"pda_masters_{count_name}_{timestamp}.db"
    shutil.copy(sqlite_path, export_path / export_filename)
    print(f"✅ SQLite exported to {export_path / export_filename}")


if __name__ == "__main__":
    main()
