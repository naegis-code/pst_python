import pandas as pd
from sqlalchemy import create_engine, text
import traceback  # For detailed error logging
import pathlib
import db_connect

# Set file path
user_path = pathlib.Path.home()
if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

path2 = filepath / 'Report' / '2025' / '99 Plan' / 'Annual Plan 2025 All Update (By Div).xlsx'
# Specify the folder containing the Excel files

# Define log file function
def log_error(error_message):
    with open("error_log.txt", "a", encoding="utf-8") as log_file:
        log_file.write(error_message + "\n")

try:
    # Try reading the Excel file
    try:
        df = pd.read_excel(path2, sheet_name='Annual Plan All Bu 2025', usecols="B:AZ")
    except Exception as e:
        error_message = f"Error reading Excel file: {str(e)}\n{traceback.format_exc()}"
        log_error(error_message)
        print(error_message)
        raise  # Stop execution if the file cannot be read

    # Column mapping
    column_mapping = {
        'No': 'no',
        'BUs.': 'bu',
        'Acronym': 'acronym',
        'Store Code': 'stcode',
        'Branch': 'branch',
        'Province': 'province',
        'HUB': 'shub',
        'FOOD': 'food_soh',
        'NONFOOD': 'nonfood_soh',
        'PERISHABLE': 'perishable_soh',
        'Total SOH': 'total_soh',
        'Size': 'size',
        'Type': 'type1',
        'Atype': 'atype',
        'Total EST.Man': 'est_man_total',
        'EST.ManControl': 'est_man_control',
        'EST.ManExpire': 'est_man_expire',
        'EST.ManCount': 'est_man_count',
        'CNTDATE': 'cntdate',
        'Day': 'day',
        'Month': 'month',
        'Total PlanManday': 'div_pman_total',
        'Manday Control': 'div_pman_control',
        'Manday Expire2': 'div_pman_expire',
        'Manday Count': 'div_pman_count',
        'ManStore': 'div_pman_store',
        'Outsource': 'div_cman_outsource',
        'Part-Time local': 'div_pman_pt',
        'Outsource By DIV': 'div_pman_outsource',
        'Status การจ้าง Outsource': 'hiring_outsource',
        'ประเภทการตรวจนับ': 'outsource_cnt_type',
        'Round': 'round',
        'Status2': 'job_status',
        'POST Date': 'post_date',
        'Case LP No': 'case_lp_no',
        'Case LP Date': 'case_lp_date',
        'Code For Copy':'code_for_copy'
    }

    # Select required columns
    keepcolumn = [
        'no', 'bu', 'acronym', 'stcode', 'branch', 'province', 'shub', 'food_soh',
        'nonfood_soh', 'perishable_soh', 'total_soh', 'size', 'type1', 'atype',
        'est_man_total', 'est_man_control', 'est_man_expire', 'est_man_count',
        'cntdate', 'day', 'month', 'div_pman_total', 'div_pman_control', 
        'div_pman_expire', 'div_pman_count', 'div_pman_store', 'div_cman_outsource',
        'div_pman_pt', 'div_pman_outsource', 'hiring_outsource', 'outsource_cnt_type',
        'round', 'job_status', 'post_date', 'case_lp_no', 'case_lp_date', 'code_for_copy'
    ]

    try:
        df.rename(columns=column_mapping, inplace=True)
    except Exception as e:
        error_message = f"Error renaming columns: {str(e)}\n{traceback.format_exc()}"
        log_error(error_message)
        print(error_message)
        raise

    # Fill NaN values in specific columns with 0
    fillna_columns = [
        'est_man_total', 'est_man_control', 'est_man_expire', 'est_man_count',
        'div_pman_control', 'div_pman_count', 'div_pman_expire', 'div_pman_store',
        'div_cman_outsource', 'div_pman_outsource', 'div_pman_pt', 'div_pman_total'
    ]

    try:
        df[fillna_columns] = df[fillna_columns].fillna(0)
    except Exception as e:
        error_message = f"Error filling NaN values: {str(e)}\n{traceback.format_exc()}"
        log_error(error_message)
        print(error_message)
        raise

    df = df[keepcolumn]

    try:
        df.dropna(subset=['bu'], inplace=True)
    except Exception as e:
        error_message = f"Error dropping NaN in 'bu' column: {str(e)}\n{traceback.format_exc()}"
        log_error(error_message)
        print(error_message)
        raise

    db_url = db_connect.db_url_pstdb
    engine = create_engine(db_url)

    try:
        # Execute the delete query to clear the 'plan2025' table
        with engine.connect() as connection:
            trans = connection.begin()  # Start a transaction
            try:
                connection.execute(text("DELETE FROM plan2025"))
                trans.commit()  # Commit the transaction
                print("Existing data in 'plan2025' table deleted.")
            except Exception as e:
                trans.rollback()  # Rollback the transaction if there's an error
                error_message = f"Error deleting data from 'plan2025': {str(e)}\n{traceback.format_exc()}"
                log_error(error_message)
                print(error_message)
                raise

        # Insert the DataFrame into the 'plan2025' table
        df.to_sql('plan2025', engine, if_exists='append', index=False)
        print("Data successfully inserted into 'plan2025'.")

    except Exception as e:
        error_message = f"Error inserting data into 'plan2025': {str(e)}\n{traceback.format_exc()}"
        log_error(error_message)
        print(error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}\n{traceback.format_exc()}"
    log_error(error_message)
    print(error_message)
