from sqlalchemy import text, create_engine
import pandas as pd
import files_path
import db_connect

# Load the CSV data into a dataframe
df = pd.read_excel(
    files_path.oustource_2025_file,
    sheet_name=files_path.oustource_2025_sheet,
    skiprows=files_path.outsource_2025_skiprows,
    usecols=files_path.outsource_2025_usecols,
    dtype={'Store Code': str}
)

df.dropna(subset=['BUs.'], inplace=True)

df['act_man_control'] = df['Outsource Control']
df['act_man_outsource'] = df['Outsource Control'] + df['Expire manday'] + df['Outsource counter']
df['est_man_food'] = df.apply(lambda row: 0 if pd.isna(row['Outsource (count pcs.)']) or row['Outsource (count pcs.)'] == 0 else round(row['FOOD'] / 10500, 0), axis=1)
df['est_man_nonfood'] = df.apply(lambda row: 0 if pd.isna(row['Outsource (count pcs.)']) or row['Outsource (count pcs.)'] == 0 else round(row['NONFOOD'] / 10500, 0), axis=1)
df['est_man_perishable'] = df.apply(lambda row: 0 if pd.isna(row['Outsource (count pcs.)']) or row['Outsource (count pcs.)'] == 0 else round(row['PERISHABLE'] / 10500, 0), axis=1)

df = df.rename(columns=files_path.outsource_2025_column_mapping)
df = df[files_path.outsource_2025_column_keep]



# Database connection
engine = create_engine(db_connect.db_url_pstdb)

with engine.connect() as connection:
    trans = connection.begin()  # Start a transaction
    try:
        connection.execute(text("DELETE FROM outsources WHERE cntdate BETWEEN '2025-01-01' AND '2025-12-31'"))
        trans.commit()  # Commit the transaction
        print("Existing data in 'outsources' table deleted.")
    except Exception as e:
        trans.rollback()  # Rollback the transaction
        print(f"Error deleting data: {e}")

# Use `engine` instead of `connection` for `to_sql`
df.to_sql('outsources', engine, if_exists='append', index=False)
print("Data inserted into 'outsources' table.")
