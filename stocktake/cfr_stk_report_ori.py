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

bu = 'CFR'
sdate = '20260101'
edate = '20261231'

path_report = filepath / 'Apps' / 'Stocktake' / 'cfr_stk_report.csv'
path_report_dept = filepath / 'Apps' / 'Stocktake' / 'cfr_stk_report_dept.csv'

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

q_report_topscare = f"""
                with master_initial as (
                    SELECT cntnum ,
                        stcode, 
                        to_char(to_date(sdate,'dd/mm/yyyy'),'YYYYMMDD') as cntdate,
                        sku, 
                        barcode, 
                        description, 
                        qnt AS soh, 
                        price
                    FROM topscare_master
                    ),bar_all as (
                    select cntnum 
                        ,sku as barcode 
                        ,sku
                    from topscare_master tm 
                    union all
                    select cntnum
                        ,barcode 
                        ,sku
                    from topscare_master tm 	
                    ),edit_max as (
                    select es.cntnum ,
                        es."location",
                        es.seq,
                        es.barcode ,
                        ba.sku,
                        max(es.id) as id
                    from edit_01_seq es 
                    left join bar_all ba
                        on es.cntnum = ba.cntnum
                        and es.barcode = ba.barcode
                    where es.cntnum like 'IU%'
                        and ba.sku is not null
                    group by es.cntnum ,
                        es."location" ,
                        es.seq,
                        es.barcode,
                        ba.sku
                    ),edited as (
                    select ex.cntnum,
                        ex.seq as seq,
                        ex.location,
                        ex.sku,
                        eseq.qnt
                    from edit_max ex
                    left join edit_01_seq eseq
                        on ex.id = eseq.id
                    ),count_by_seq as (
                    select cty.stocktakeid ,
                        cty."location" ,
                        cty.seq ,
                        substring(cty.sku,6,8) as sku ,
                        cty.qnt
                    from cntfiles_this_year cty 
                    where cty.stocktakeid like 'IU%'
                    ), variance_all as (
                    select cbs.stocktakeid ,
                        cbs."location" ,
                        cbs.seq ,
                        cbs.sku ,
                        coalesce(e.qnt,cbs.qnt) as qnt ,
                        mi.price,
                        mi.stcode,
                        mi.cntdate
                    from count_by_seq cbs
                    left join master_initial mi
                        on cbs.stocktakeid = mi.cntnum and cbs.sku = mi.sku
                    left join edited e
                        on cbs.stocktakeid = e.cntnum and cbs."location" = e."location" and cbs.seq = e.seq and cbs.sku = e.sku
                    union all 
                    select e.cntnum as stocktakeid ,
                        e."location",
                        e.seq,
                        e.sku,
                        e.qnt,
                        mi.price,
                        mi.stcode,
                        mi.cntdate
                    from edited e
                    left join master_initial mi
                        on e.cntnum = mi.cntnum and e.sku = mi.sku
                    left join count_by_seq cbs
                        on e.cntnum = cbs.stocktakeid and e."location" = cbs."location" and e.seq = cbs.seq and e.sku = cbs.sku
                    where cbs."location" is null and mi.price is not null
                    ),stocktake as (
                    select 
                        stocktakeid,
                        stcode,
                        cntdate,
                        sku,
                        sum(qnt) as qnt,
                        sum(qnt*price) as amount
                    from variance_all v
                    group by stocktakeid,stcode,cntdate,sku
                    )
                    select 'All' as dept ,
                            'All' as sub_dept ,
                            v.sku ,
                            tm.barcode ,
                            tm.description as product_name ,
                            v.qnt ,
                            tm.price*v.qnt as qnt_retail ,
                            v.stcode ,
                            v.cntdate ,
                            'Credit' as skutype	,
                            'STK2' as rpname ,
                            v.stocktakeid
                    from variance_all v
                    left join (select distinct stcode,cntdate from cfr_stk_this_year where rpname = 'STK2' and cntdate between '{sdate}' and '{edate}') old
                        on v.stcode = old.stcode and v.cntdate = old.cntdate
                    left join topscare_master tm
                        on v.stocktakeid = tm.cntnum and v.sku = tm.sku
                    where v.stcode is not null 
                        and old.stcode is null
                        and v.cntdate between '{sdate}' and '{edate}'
                """

df_report_topscare = pl.read_database_uri(q_report_topscare, engine3)
print(f"✅ Topscare report data retrieved successfully. Total rows: {len(df_report_topscare)}")

df_report_topscare.write_database('cfr_stk_this_year', engine3, if_table_exists='append')
print(f"✅ Topscare report data inserted into cfr_stk_this_year successfully. Total rows: {len(df_report_topscare)}")

q_report_cfr = f"""
                select stcode,
                    cntdate,
                    rpname,
                    skutype,
                    count(*) as sku_count,
                    sum(case when variance = 0 then 1 else 0 end) as sku_eq,
                    sum(case when variance > 0 then 1 else 0 end) as sku_gain,
                    sum(case when variance < 0 then 1 else 0 end) as sku_loss,
                    sum(stock) as qnt_soh,
                    sum(qnt) as qnt_physical,
                    sum(case when variance > 0 then variance else 0 end) as qnt_gain,
                    sum(case when variance < 0 then variance else 0 end) as qnt_loss,
                    sum(variance) as qnt_variance,
                    sum(qnt_retail-var_retail) as retail_soh,
                    sum(qnt_cost-var_cost) as cost_soh,
                    sum(qnt_retail) as retail_physical,
                    sum(qnt_cost) as cost_physical,
                    sum(case when var_retail > 0 then var_retail else 0 end) as retail_gain,
                    sum(case when var_cost > 0 then var_cost else 0 end) as cost_gain,
                    sum(case when var_retail < 0 then var_retail else 0 end) as retail_loss,
                    sum(case when var_cost < 0 then var_cost else 0 end) as cost_loss,
                    sum(var_retail) as retail_net,
                    sum(var_cost) as cost_net
                from cfr_stk_this_year csty 
                where cntdate between '{sdate}' and '{edate}'
                group by stcode	,cntdate ,rpname ,skutype
                """

df_report_cfr = pl.read_database_uri(q_report_cfr, engine3)
print(f"✅ CFR report data retrieved successfully. Total rows: {len(df_report_cfr)}")

summary_report_cfr = df_plan.join(df_report_cfr, on=['stcode', 'cntdate'], how='left')
print(f"✅ Data merged successfully. Total rows after merge: {len(df_report_cfr)}")

summary_report_cfr.write_csv(path_report)

q_dept_cfr = f"""
                select 
                    stcode,
                    cntdate,
                    rpname,
                    skutype,
                    dept,
                    sub_dept,
                    count(*) as sku_count,
                    sum(case when variance = 0 then 1 else 0 end) as sku_eq,
                    sum(case when variance > 0 then 1 else 0 end) as sku_gain,
                    sum(case when variance < 0 then 1 else 0 end) as sku_loss,
                    sum(stock) as qnt_soh,
                    sum(qnt) as qnt_physical,
                    sum(case when variance > 0 then variance else 0 end) as qnt_gain,
                    sum(case when variance < 0 then variance else 0 end) as qnt_loss,
                    sum(variance) as qnt_variance,
                    sum(qnt_retail-var_retail) as retail_soh,
                    sum(qnt_cost-var_cost) as cost_soh,
                    sum(qnt_retail) as retail_physical,
                    sum(qnt_cost) as cost_physical,
                    sum(case when var_retail > 0 then var_retail else 0 end) as retail_gain,
                    sum(case when var_cost > 0 then var_cost else 0 end) as cost_gain,
                    sum(case when var_retail < 0 then var_retail else 0 end) as retail_loss,
                    sum(case when var_cost < 0 then var_cost else 0 end) as cost_loss,
                    sum(var_retail) as retail_net,
                    sum(var_cost) as cost_net
                from cfr_stk_this_year csty 
                where rpname = 'STK2'
                    and cntdate between '{sdate}' and '{edate}'
                group by 
                    stcode,
                    cntdate,
                    rpname,
                    skutype,
                    dept,
                    sub_dept
                """
df_dept_cfr = pl.read_database_uri(q_dept_cfr, engine3)
print(f"✅ CFR department report data retrieved successfully. Total rows: {len(df_dept_cfr)}")

df_dept = df_dept_cfr.join(df_plan, on=['stcode', 'cntdate'], how='left')
print(f"✅ Department data merged successfully. Total rows after merge: {len(df_dept)}")
df_dept.write_csv(path_report_dept)
print(f"✅ Department report data saved to {path_report_dept} successfully. Total rows: {len(df_dept)}")


print(f"✅ Report data saved to {path_report} successfully. Total rows: {df_report_cfr.shape[0]}")
print(f"✅ Department report data saved to {path_report_dept} successfully. Total rows: {df_dept.shape[0]}")

end_time = datetime.now()
print(f"endtime: {end_time}")
print(f"Usetime: {end_time - start_time}")
