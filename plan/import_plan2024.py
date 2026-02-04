import pandas as pd
from sqlalchemy import create_engine,text

path1 = r'D:\Users\prthanap\Central Group\PST Performance Team - เอกสาร\Report\2024\99 Plan\Annual Plan 2024 All Update (By Div).xlsx'
path2 = r'C:\Users\Administrator\Central Group\PST Performance Team - Documents\Report\2024\99 Plan\Annual Plan 2024 All Update (By Div).xlsx'
df = pd.read_excel(path2,sheet_name='Annual Plan All Bu 2024',usecols="B:AX")

column_mapping = {
    'No':'no',
    'BUs.':'bu',
    'Acronym':'acronym',
    'Store Code':'stcode',
    'Branch':'branch',
    'Province':'province',
    'HUB':'shub',
    'FOOD':'food_soh',
    'NONFOOD':'nonfood_soh',
    'PERISHABLE':'perishable_soh',
    'Total SOH':'total_soh',
    'Size':'size',
    'Type':'type1',
    'Atype':'atype',
    'Total EST.Man':'est_man_total',
    'EST.ManControl':'est_man_control',
    'EST.ManExpire':'est_man_expire',
    'EST.ManCount':'est_man_count',
    'CNTDATE':'cntdate',
    'Day':'day',
    'Month':'month',
    'Total PlanManday':'div_pman_total',
    'Manday Control':'div_pman_control',
    'Manday Expire2':'div_pman_expire',
    'Manday Count':'div_pman_count',
    'ManStore':'div_pman_store',
    'Outsource':'div_cman_outsource',
    'Part-Time local':'div_pman_pt',
    'Outsource By DIV':'div_pman_outsource',
    'Status การจ้าง Outsource':'hiring_outsource',
    'ประเภทการตรวจนับ':'outsource_cnt_type',
    'Round':'round',
    'Status2':'job_status',
    'POST Date':'post_date',
    'Code For Copy':'code_for_copy'
}

keepcolumn = [
    'no',
    'bu',
    'acronym',
    'stcode',
    'branch',
    'province',
    'shub',
    'food_soh',
    'nonfood_soh',
    'perishable_soh',
    'total_soh',
    'size',
    'type1',
    'atype',
    'est_man_total',
    'est_man_control',
    'est_man_expire',
    'est_man_count',
    'cntdate',
    'day',
    'month',
    'div_pman_total',
    'div_pman_control',
    'div_pman_expire',
    'div_pman_count',
    'div_pman_store',
    'div_cman_outsource',
    'div_pman_pt',
    'div_pman_outsource',
    'hiring_outsource',
    'outsource_cnt_type',
    'round',
    'job_status',
    'post_date',
    'code_for_copy'
]

df.rename(columns=column_mapping,inplace=True)

df = df[keepcolumn]


# Database connection string
db_url = 'postgresql+psycopg2://prthanapat:20020015@localhost:5432/pstdb'

# Create the engine to connect to PostgreSQL
engine = create_engine(db_url)

# Execute the delete query to clear the 'employees' table
with engine.connect() as connection:
    trans = connection.begin()  # Start a transaction
    try:
        connection.execute(text("DELETE FROM plan2024"))
        trans.commit()  # Commit the transaction
        print("Existing data in 'plan2024' table deleted.")
    except:
        trans.rollback()  # Rollback the transaction if there's an error
        print("An error occurred. Transaction rolled back.")

# Insert the DataFrame into the 'employees' table
df.to_sql('plan2024', engine, if_exists='append', index=False)

print("Data successfully inserted into 'plan2024'.")