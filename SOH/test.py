import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

file = f'14_MSTKVAL20251105.csv'
path = Path('D:/Users/prthanap/Downloads') / file

# total rows csv
with open(path, 'r', encoding='cp874', errors='ignore') as f:
    total_lines = sum(1 for _ in f)
total_rows = total_lines - 1  # exclude header
print(f"📄 Total rows in file: {total_rows:,}")

chunksize = 20000
print("Reading CSV in chunks with Progress Bar...")

dataframes = []

for chunk in tqdm(
        pd.read_csv(path, encoding='cp874', dtype=str, low_memory=False, chunksize=chunksize),
        total=total_rows // chunksize + 1,
        desc="📦 Importing",
        unit="chunk"
    ):

    # lowercase columns
    chunk.columns = chunk.columns.str.lower()

    # strip ' only on object columns
    obj_cols = chunk.select_dtypes(include=['object']).columns
    chunk[obj_cols] = chunk[obj_cols].apply(lambda col: col.str.strip("'"))

    dataframes.append(chunk)

df = pd.concat(dataframes, ignore_index=True)
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

