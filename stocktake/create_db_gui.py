import tkinter as tk
from tkinter import filedialog, messagebox
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import pandas as pd
from datetime import datetime,date
import db_connect
from tkcalendar import DateEntry
import shutil

def run_process():
    try:
        # Collect user inputs
        path_import = Path(entry_excel_path.get())
        path_import_sheet = 'Sheet1'
        bu = bu_value()
        atype = atype_var()
        stcode = entry_store_code.get()
        cntdate_pick = date_var.get()
        cntdate = datetime.strptime(cntdate_pick, '%Y-%m-%d').strftime('%d%m%y')

        username = 'prthanapat'
        password = '20020015'

        # Step 1: Get store name
        engine = create_engine(db_connect.db_url_pstdb)
        query = f"SELECT stname_th FROM plan_dba WHERE stcode = '{stcode}' AND bu = '{bu}';"
        df_query = pd.read_sql_query(text(query), engine)
        store_name = df_query['stname_th'].iloc[0]

        # Step 2: Get cntnum from pstdb3
        db_url_3 = f'postgresql+psycopg2://{username}:{password}@103.22.182.82:5432/pstdb3'
        engine_3 = create_engine(db_url_3)
        query_3 = f"""SELECT COUNT(cntnum) AS run FROM stocktakeid 
                      WHERE cntnum LIKE '{bu+stcode+atype+cntdate}%'
                      GROUP BY cntnum;"""
        df_query_3 = pd.read_sql_query(text(query_3), engine_3)

        if df_query_3.empty:
            cntnum = bu + stcode + atype + cntdate + '001'
        else:
            cntnum = bu + stcode + atype + cntdate + str(int(df_query_3['run'].iloc[0]) + 1).zfill(3)

        # Step 3: SQLite updates
        path_db = Path('D:/Program Files/pooledstocktake/b2s/Apps/pda-master.db')
        db_url_sqlite = f'sqlite:///{path_db}'
        engine_sqlite = create_engine(db_url_sqlite)

        with engine_sqlite.begin() as connection:
            connection.execute(text("DELETE FROM pda_masters;"))
            connection.execute(text("DELETE FROM users WHERE id > 1;"))

        created_on_str = datetime.strptime(cntdate_pick, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')

        params = {
            "cntnum": cntnum,
            "stcode": stcode,
            "bu": bu,
            "store_name": store_name,
            "branch": store_name,
            "created_on": created_on_str,
            "updated_on": created_on_str
        }

        query_update_cntnum = """
            UPDATE stocktakes
            SET countName = :cntnum,
                storeCode = :stcode,
                bu = :bu,
                storeName = :store_name,
                branch = :store_name,
                createdON = :created_on,
                updatedOn = :updated_on;
        """

        df = pd.read_excel(path_import, sheet_name=path_import_sheet,
                           dtype={'SKU': int, 'UPC': str, 'DES': str, 'PRICE': float, 'DEPT': str})
        df['sku'] = df['UPC']
        df['barcodeIBC'] = df['UPC']
        df['stocktakeid'] = cntnum
        df['storeCode'] = stcode
        df['storeName'] = store_name
        df['status'] = 'A'
        df['createdOn'] = created_on_str
        df['updatedOn'] = created_on_str

        df.drop(columns=['SKU', 'UPC'], inplace=True, errors='ignore')

        df.rename(columns={
            'DES': 'productName',
            'PRICE': 'retailPrice',
            'DEPT': 'deptCode',
        }, inplace=True)

        df.columns = df.columns.str.lower()
        df.to_sql('pda_masters', con=engine_sqlite, if_exists='append', index=False)

        with engine_sqlite.begin() as connection:
            connection.execute(text(query_update_cntnum), params)

        # Load users
        with engine.begin() as connection:
            df_users = pd.read_sql_query(text(
                "SELECT employee_code, encryptedpassword FROM employees "
                "UNION SELECT employee_code, encryptedpassword FROM employees_outsource;"
            ), engine)
            df_users['username'] = df_users['employee_code']
            df_users.rename(columns={'employee_code': 'empCode'}, inplace=True)
            df_users.to_sql('users', con=engine_sqlite, if_exists='append', index=False)

        # Step 4: Compact (VACUUM) the SQLite database to reduce file size
        with engine_sqlite.begin() as connection:
            connection.execute(text("VACUUM;"))
        
        # Ask user where to save the .db file
        save_path = filedialog.asksaveasfilename(
            defaultextension=".db",
            filetypes=[("SQLite Database", "*.db"), ("All files", "*.*")],
            title="Save SQLite database as..."
        )
        if save_path:
            # Copy current DB file to user selected location
            shutil.copy2(path_db, save_path)
            messagebox.showinfo("Success", f"Database vacuumed and saved to:\n{save_path}")
        else:
            messagebox.showinfo("Info", "Vacuum completed, but save was cancelled.")

    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

# --- Tkinter UI ---
root = tk.Tk()
root.title("Stocktake Upload")

# Excel path
tk.Label(root, text="Excel File Path:").grid(row=0, column=0, sticky='e')
entry_excel_path = tk.Entry(root, width=50)
entry_excel_path.grid(row=0, column=1)
tk.Button(root, text="Browse", command=lambda: entry_excel_path.insert(0, filedialog.askopenfilename())).grid(row=0, column=2)

# BU (Read-only Label)
tk.Label(root, text="BU:").grid(row=1, column=0, sticky='e')
bu_value = 'B2S'
label_bu = tk.Label(root, text=bu_value)
label_bu.grid(row=1, column=1, sticky='w')


# Atype (Dropdown)
tk.Label(root, text="Type:").grid(row=2, column=0, sticky='e')
atype_var = tk.StringVar()
atype_options = {'Fullcount': 'F', 'QSerpircheck': 'Q'}
atype_var.set('Fullcount')  # default

atype_menu = tk.OptionMenu(root, atype_var, *atype_options.keys())
atype_menu.grid(row=2, column=1)


# Store code
tk.Label(root, text="Store Code:").grid(row=3, column=0, sticky='e')
entry_store_code = tk.Entry(root)
entry_store_code.insert(0, '50002')
entry_store_code.grid(row=3, column=1)

# Count date (Date picker)
tk.Label(root, text="Count Date (YYYY-MM-DD):").grid(row=4, column=0, sticky='e')
date_var = tk.StringVar()
date_picker = DateEntry(root, textvariable=date_var, date_pattern='yyyy-mm-dd')
date_picker.set_date(date.today())  # default date
date_picker.grid(row=4, column=1)

# Submit button
tk.Button(root, text="Run Process", command=run_process, bg="green", fg="white").grid(row=5, column=1, pady=10)

root.mainloop()
