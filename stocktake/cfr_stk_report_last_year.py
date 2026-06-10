import pandas as pd
from sqlalchemy import create_engine, text
import db_connect as db
import pathlib

start_time = pd.Timestamp.now()
print(f"starttime: {start_time}")
# ========== PATH SETUP ==========
userpath = pathlib.Path.home()
filepath = (
    userpath / 'Central Group/PST Performance Team - เอกสาร'
    if (userpath / 'Central Group/PST Performance Team - เอกสาร').exists()
    else userpath / 'Central Group/PST Performance Team - Documents'
)

bu = 'CFR'
sdate = '20250101'
edate = '20251231'

path_report = filepath / 'Apps' / 'Stocktake' / 'cfr_stk_report_last_year.csv'
path_report_dept = filepath / 'Apps' / 'Stocktake' / 'cfr_stk_report_dept_last_year.csv'

engine = create_engine(db.db_url_pstdb)
engine3 = create_engine(db.db_url_pstdb3)

q_plan = text("""
              SELECT bu,
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
              WHERE bu = :bu
                AND atype = '3F'
                AND cntdate between :sdate and :edate
              """)

df_plan = pd.read_sql(q_plan, engine, params={'bu': bu, 'sdate': sdate, 'edate': edate})

q_report_cfr = text("""
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
                from cfr_stk csty 
                where cntdate between :sdate and :edate
                group by stcode	,cntdate ,rpname ,skutype
                """)

df_report_cfr = pd.read_sql(q_report_cfr, engine, params={'sdate': sdate, 'edate': edate})
print(f"✅ CFR report data retrieved successfully. Total rows: {len(df_report_cfr)}")

q_report_topscare = text("""
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
                select 
                    stcode,
                    cntdate,
                    'STK2' as rpname,
                    'Credit' as skutype,
                    count(*) as sku_count,
                    sum(qnt) as qnt_physical,
                    sum(amount) as retail_physical
                from stocktake s
                where stcode is not null and cntdate between :sdate and :edate
                group by stcode,cntdate
                """)

df_report_topscare = pd.read_sql(q_report_topscare, engine3, params={'sdate': sdate, 'edate': edate})
print(f"✅ Topscare report data retrieved successfully. Total rows: {len(df_report_topscare)}")

df_summary = pd.concat([df_report_cfr, df_report_topscare], ignore_index=True)

df_summary = pd.merge(df_plan, df_summary, on=['stcode', 'cntdate'], how='left')
print(f"✅ Data merged successfully. Total rows after merge: {len(df_summary)}")

df_summary.to_csv(path_report, index=False)


q_dept_cfr = text("""
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
                    and cntdate between :sdate and :edate
                group by 
                    stcode,
                    cntdate,
                    rpname,
                    skutype,
                    dept,
                    sub_dept
                """)
df_dept_cfr = pd.read_sql(q_dept_cfr, engine3, params={'sdate': sdate, 'edate': edate})
print(f"✅ CFR department report data retrieved successfully. Total rows: {len(df_dept_cfr)}")

df_dept_topscare = df_report_topscare.copy()
df_dept_topscare['dept'] = 'ALL'
df_dept_topscare['sub_dept'] = 'ALL'
print(f"✅ Topscare department report data prepared successfully. Total rows: {len(df_dept_topscare)}")

df_dept_summary = pd.concat([df_dept_cfr, df_dept_topscare], ignore_index=True)
print(f"✅ Department data concatenated successfully. Total rows after concatenation: {len(df_dept_summary)}")

df_dept = pd.merge(df_plan, df_dept_summary, on=['stcode', 'cntdate'], how='left')
print(f"✅ Department data merged successfully. Total rows after merge: {len(df_dept)}")
df_dept.to_csv(path_report_dept, index=False)
print(f"✅ Department report data saved to {path_report_dept} successfully. Total rows: {len(df_dept)}")


print(f"✅ Report data saved to {path_report} successfully. Total rows: {len(df_summary)}")
print(f"✅ Department report data saved to {path_report_dept} successfully. Total rows: {len(df_dept)}")

end_time = pd.Timestamp.now()
print(f"endtime: {end_time}")
print(f"Usetime: {end_time - start_time}")
