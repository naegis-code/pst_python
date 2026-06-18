import polars as pl
from sqlalchemy import create_engine
import db_connect as dbc
import datetime as dt

startime = dt.datetime.now()
print(f"Script started at: {startime}")

#Parameters
sdate = '20250101'
edate = '20251231'

#Connect to databases
db = create_engine(dbc.db_url_pstdb)
db3 = create_engine(dbc.db_url_pstdb3)

#queries
query_var = f"""
            with master_initial as (
            SELECT cntnum ,
                stcode, 
                to_char(to_date(sdate,'dd/mm/yyyy'),'YYYYMMDD') as cntdate,
                sku, 
                barcode, 
                description, 
                qnt AS soh, 
                price
            FROM topscare_master_backup
            where to_char(to_date(sdate,'dd/mm/yyyy'),'YYYYMMDD') between '{sdate}' and '{edate}'
            ),bar_all as (
            select cntnum 
                ,sku as barcode 
                ,sku
            from topscare_master_backup tm 
            union all
            select cntnum
                ,barcode 
                ,sku
            from topscare_master_backup tm 	
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
                mi.barcode,
                mi.description,
                coalesce(e.qnt,cbs.qnt) as qnt ,
                mi.price,
                mi.stcode,
                mi.cntdate
            from count_by_seq cbs
            left join master_initial mi
                on cbs.stocktakeid = mi.cntnum and cbs.sku = mi.sku
            left join edited e
                on cbs.stocktakeid = e.cntnum and cbs."location" = e."location" and cbs.seq = e.seq and cbs.sku = e.sku
            where mi.stcode is not null
            union all 
            select e.cntnum as stocktakeid ,
                e."location",
                e.seq,
                e.sku,
                mi.barcode,
                mi.description, 
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
                barcode,
                description,
                sum(qnt) as qnt,
                sum(qnt*price) as amount
            from variance_all v
            where stcode is not null
            group by stocktakeid,stcode,cntdate,sku,barcode,description
            ),summary as (
            select 
                stcode,
                cntdate,
                'STK2' as rpname,
                'Credit' as skutype,
                count(*) as sku_count,
                sum(qnt) as qnt_physical,
                sum(amount) as retail_physical
            from stocktake s
            where stcode is not null
            group by stcode,cntdate
            )
            select *
            from variance_all
            """


query_plan = f"""
            select stcode,
                cntdate,
                branch
            from planall2
            where atype = '3F'
                and cntdate between '{sdate}' and '{edate}'
                and bu = 'CFR'
            """

query_old = f"""
            select distinct 
                stocktakeid
            from cfr_stk_this_year
            where cntdate between '{sdate}' and '{edate}'
            """


df_var = pl.read_database(query_var, db3)
df_plan = pl.read_database(query_plan, db)
df_old = pl.read_database(query_old, db3)

df = df_var.join(df_plan, on=['stcode','cntdate'], how='semi')

df = (df
      .group_by(['stocktakeid','sku','barcode','description','stcode','cntdate'])
      .agg(
          pl.col('qnt').sum().alias('qnt'),
          (pl.col('qnt') * pl.col('price')).sum().alias('qnt_retail')
          )
    )

df_del = df_old.join(df, on='stocktakeid', how='semi')
id_list = df_del['stocktakeid'].to_list()

# ========== Delete → Insert ==========

try:
    df.write_database(db3, 'cfr_stk_this_year', if_exists='append', index=False)
    print(f"Data inserted successfully. {len(df)} records inserted.")
except Exception as e:
    print(f"Error inserting data: {e}")

print(f"")
print(f"Script End at: {dt.datetime.now()}")
print(f"Script completed in: {dt.datetime.now() - startime}")