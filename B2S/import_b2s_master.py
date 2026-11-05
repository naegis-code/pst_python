from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tunnel import ensure_tunnel, engine3
import polars as pl
from sqlalchemy import create_engine, text


userpath = user_path = Path.home()
file_path = userpath / 'Downloads' / 'b2s_master.csv'
table = "b2s_master_bar"

with open(file_path, encoding="cp874", errors="replace", newline="") as src, \
     open("converted_utf8.csv", "w", encoding="utf-8", newline="") as dst:
    while chunk := src.read(1 << 20):
        dst.write(chunk)

df = pl.read_csv(
    "converted_utf8.csv",
    has_header=False,
    infer_schema=False,
    new_columns=["sku", "upc", "des", "price", "dept"],
).with_columns(
    pl.col(pl.String)
    .str.replace_all(r"[\r\n,]+", " ")   # newline และลูกน้ำ -> เว้นวรรค
    .str.replace_all(r"\s+", " ")        # ยุบช่องว่างซ้ำ
    .str.strip_chars()
).with_columns(
    pl.col("price").cast(pl.Float64, strict=False),
)

print(df)

with engine3.begin() as conn:
    conn.execute(text(f"DELETE FROM {table}"))
    print(f"Deleted all rows from {table}")
    df.write_database(
        table_name=table,
        connection=conn,              # ใช้ connection เดิม ไม่ใช่ engine
        if_table_exists="append",
        engine="sqlalchemy",
    )
    print(f"Reloaded {df.height} rows into {table}")


