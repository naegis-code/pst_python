import pandas as pd
from sqlalchemy import create_engine,text
import db_connect

engine_db = create_engine(db_connect.db_url_pstdb)
engine_db4 = create_engine(db_connect.db_url_pstdb4)

q_planall2 = text('''
                SELECT *
                from planall2
                ''')

df_planall2 = pd.read_sql(q_planall2, engine_db)

try:
    with engine_db4.begin() as connection:
        connection.execute(
            text('''delete from planall2''')
        )
        print("Existing data in 'planall2' table deleted successfully in pstdb4.")

        df_planall2.to_sql('planall2', engine_db4, if_exists='append', index=False)

        print("Table 'planall2' inserted in pstdb4.")
except Exception as e:
    print(f"An error occurred while creating the table: {e}")
    