import pandas as pd
from sqlalchemy import create_engine, text
import db_connect as db
import pathlib

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

sdatef = pd.to_datetime(sdate, format='%Y%m%d')
edatef = pd.to_datetime(edate, format='%Y%m%d')

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

q_report = text("""
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
                where cntdate between :sdate and :edate
                group by stcode	,cntdate ,rpname ,skutype
                """)

df_report = pd.read_sql(q_report, engine3, params={'sdate': sdate, 'edate': edate})

df = pd.merge(df_plan, df_report, on=['stcode', 'cntdate'], how='left')

df.to_csv(path_report, index=False)
print(f"✅ Report data saved to {path_report} successfully. Total rows: {len(df)}")
