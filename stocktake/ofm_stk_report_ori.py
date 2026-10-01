import polars as pl
import pathlib
import os
from dotenv import load_dotenv, find_dotenv
import socket
import subprocess
import time
from datetime import datetime

# ==================== โหลดค่าจาก .env ====================
load_dotenv(find_dotenv())

def is_port_open(host, port, timeout=2):
    """เช็คว่าพอร์ต local เปิด (มีอะไร listening อยู่) หรือไม่"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        result = s.connect_ex((host, port))
        return result == 0

DB_HOST = "localhost"
DB_PORT = 5432

if is_port_open(DB_HOST, DB_PORT):
    print(f"✅ พอร์ต {DB_PORT} ที่ {DB_HOST} เปิดอยู่ — tunnel ทำงานอยู่")
else:
    print(f"❌ พอร์ต {DB_PORT} ที่ {DB_HOST} ปิดอยู่ — tunnel ยังไม่เปิด")
    subprocess.Popen(
        "start /b ssh -f -N pst-db",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    print("🔑 กำลังเปิด SSH tunnel... (รอ 5 วินาที)")
    time.sleep(5)
    print("✅ SSH tunnel เปิดแล้ว") if is_port_open(DB_HOST, DB_PORT) else print("❌ SSH tunnel ยังไม่เปิด — ตรวจสอบการเชื่อมต่อ SSH")

engine1 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}"
engine2 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}"
engine3 = f"{os.getenv('DB_CONN_NATIVE')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}"

start_time = datetime.now()
print(f"starttime: {start_time}")
# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'OFM'
sdate = '20260101'
edate = '20261231'

path_report = filepath / 'Apps' / 'Stocktake' / f'{bu.lower()}_stk_report.csv'
path_report_dept = filepath / 'Apps' / 'Stocktake' / f'{bu.lower()}_stk_report_dept.csv'

q_plan = f"""SELECT bu,
                    stcode,
                    acronym,
                    branch,
                    shub,
                    type1,
                    cntdate,
                    round,
                    post_date,
                    hiring_outsource,
                    outsource_cnt_type
              FROM planall2
              WHERE bu = '{bu.upper()}'
                AND atype = '3F'
                AND cntdate between '{sdate}' and '{edate}'
              """

df_plan = pl.read_database_uri(q_plan, engine1)
print(f"✅ Plan data retrieved successfully. Total rows: {len(df_plan)}")

q_report = f"""with miss as (
    select store,cntdate,new_phycnt_qty ,new_phycnt_amount ,qty_missrate ,amount_missrate ,abs_first_qty ,abs_final_qty ,abs_first_amount ,abs_final_amount 
    from {bu.lower()}_calculate_missrate bscm
    where cntdate between '{sdate}' and '{edate}'
    )
    select bs.store as stcode ,bs.cntdate ,bs.rpname ,bs.skutype ,
        count(bs.sku) as sku_count,
        sum(
            case when bs.qty_var = 0 then 1 else 0 end) as sku_eq,
        sum(
            case when bs.qty_var > 0 then 1 else 0 end) as sku_gain,
        sum(
            case when bs.qty_var < 0 then 1 else 0 end) as sku_loss,
        sum(bs.soh) as qnt_soh,
        sum(bs.qty_count) as qnt_physical,
        sum(
            case when bs.qty_var > 0 then bs.qty_var else 0 end) as qnt_gain,
        sum(
            case when bs.qty_var < 0 then bs.qty_var else 0 end) as qnt_loss,
        sum(bs.qty_var) as qnt_variance,
        sum(bs.phycnt_rtl-bs.extrtl_var) as retail_soh,
        sum(bs.phycnt_cst-bs.extcst_var) as cost_soh,
        sum(bs.phycnt_rtl) as retail_physical,
        sum(bs.phycnt_cst) as cost_physical,
        sum(
            case when bs.extrtl_var > 0 then bs.extrtl_var else 0 end) as retail_gain,
        sum(
            case when bs.extcst_var > 0 then bs.extcst_var else 0 end) as cost_gain,
        sum(
            case when bs.extrtl_var < 0 then bs.extrtl_var else 0 end) as retail_loss,
        sum(
            case when bs.extcst_var < 0 then bs.extcst_var else 0 end) as cost_loss,
        sum(bs.extrtl_var) as retail_net,
        sum(bs.extcst_var) as cost_net,
        0 as cost_sale,
        m.new_phycnt_qty,
        m.new_phycnt_amount,
        m.qty_missrate,
        m.amount_missrate,
        m.abs_first_qty,
        m.abs_final_qty,
        m.abs_first_amount,
        m.abs_final_amount
    from {bu.lower()}_stk_this_year bs
    left join miss m on bs.store = m.store and bs.cntdate = to_char(m.cntdate,'yyyymmdd')
    where bs.cntdate between '{sdate}' and '{edate}'
    group by bs.store ,bs.cntdate ,bs.rpname ,bs.skutype , m.new_phycnt_qty, m.new_phycnt_amount, m.qty_missrate, m.amount_missrate, m.abs_first_qty, m.abs_final_qty, m.abs_first_amount, m.abs_final_amount
    """

df_report = pl.read_database_uri(q_report, engine3)
print(f"✅ Report data retrieved successfully. Total rows: {len(df_report)}")

query_mandays = f"""select stcode,to_char(cntdate,'yyyymmdd') as cntdate ,pst_percent ,store_percent ,out_percent 
                    from b2s_manday_ratio bsmr
                    where cntdate between '{sdate}' and '{edate}'"""

df_mandays = pl.read_database_uri(query_mandays, engine1)
print(f"✅ Mandays data retrieved successfully. Total rows: {len(df_mandays)}")

df_report = df_report.join(df_mandays, left_on=['stcode', 'cntdate'], right_on=['stcode', 'cntdate'], how='left')
print(df_report)

df_report.write_csv(path_report)

print(f"✅ Report data saved to {path_report} successfully. Total rows: {len(df_report)}")

q_dept = f"""
                select 
                stcode,
                cntdate,
                rpname,
                skutype,
                dpt,
                sdpt,
                count(*) as sku_count,
                sum(case when qty_var = 0 then 1 else 0 end) as sku_eq,
                sum(case when qty_var > 0 then 1 else 0 end) as sku_gain,
                sum(case when qty_var < 0 then 1 else 0 end) as sku_loss,
                sum(soh) as qnt_soh,
                sum(qty_count) as qnt_physical,
                sum(case when qty_var > 0 then qty_var else 0 end) as qnt_gain,
                sum(case when qty_var < 0 then qty_var else 0 end) as qnt_loss,
                sum(qty_var) as qnt_variance,
                sum(soh*retail) as retail_soh,
                sum(soh*"cost") as cost_soh,
                sum(phycnt_rtl) as retail_physical,
                sum(phycnt_cst) as cost_physical,
                sum(case when extrtl_var  > 0 then extrtl_var  else 0 end) as retail_gain,
                sum(case when extcst_var  > 0 then extcst_var  else 0 end) as cost_gain,
                sum(case when extrtl_var < 0 then extrtl_var else 0 end) as retail_loss,
                sum(case when extcst_var < 0 then extcst_var else 0 end) as cost_loss,
                sum(extrtl_var) as retail_net,
                sum(extcst_var) as cost_net
            from {bu.lower()}_stk_this_year osty 
            where rpname = 'STK2'
                and cntdate between '{sdate}' and '{edate}'
            group by 
                stcode,
                cntdate,
                rpname,
                skutype,
                dpt,
                sdpt
                """
df_dept = pl.read_database_uri(q_dept, engine3)

q_master_dept = f"""select dept as dpt,sub_dept as sdpt,concat(dept,' ',dept_name) as dept,concat(sub_dept,' ',sub_dept_name) as sub_dept
                    from master_dept md 
                    where bu = '{bu.upper()}'
                    """
df_master_dept = pl.read_database_uri(q_master_dept, engine1)

df_dept = df_dept.join(df_master_dept, on=['dpt', 'sdpt'], how='left').drop(['dpt', 'sdpt'])


print(f"✅ OFM department report data retrieved successfully. Total rows: {len(df_dept)}")

df_dept = df_dept.join(df_plan, on=['stcode', 'cntdate'], how='left')
print(f"✅ Department data merged successfully. Total rows after merge: {len(df_dept)}")
df_dept.write_csv(path_report_dept)
print(f"✅ Department report data saved to {path_report_dept} successfully. Total rows: {len(df_dept)}")

end_time = datetime.now()
print(f"endtime: {end_time}")
print(f"Usetime: {end_time - start_time}")
