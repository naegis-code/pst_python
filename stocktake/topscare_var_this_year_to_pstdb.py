from sqlalchemy import text,create_engine
import pandas as pd
import db_connect as dbc

#Parameters
startime = pd.to_datetime('now')
print(f"Script started at: {startime}")
sdate = '20250101'
edate = '20251231'

#Connect to databases
db = create_engine(dbc.db_url_pstdb)
db3 = create_engine(dbc.db_url_pstdb3)

#Queries
query_var = text("""
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
            )select 
                *,
                'All' as dept,
                'All' as sub_dept,
                'VAR2' as rpname,
                'Credit' as skutype 
            from variance_all w
            where cntdate between :sdate and :edate
             """)
query_var_old = text("""
                    select distinct 
                        stocktakeid,
                        'check' as check
                    from cfr_var 
                    where stocktakeid like 'IU%'
                        and cntdate between :sdate and :edate
                    """)
query_plan = text("""
                select stcode,cntdate,branch
                  from planall2
                  where atype = '3F'
                    and cntdate between :sdate and :edate
                """)

#Dataframe creation and processing
df_var = pd.read_sql_query(query_var,db3,params={'sdate':sdate,'edate':edate})
print(f"Length of df_var: {len(df_var)}")

#Remove records that are already in cfr_var
df_var_old = pd.read_sql_query(query_var_old,db,params={'sdate':sdate,'edate':edate})
df_var = df_var.merge(df_var_old,on='stocktakeid',how='left')
df_var = df_var[df_var['check'].isna()].drop(columns=['check'])
print(f"Length of df_var after removing old records: {len(df_var)}")

#Merge with plan to get branch information and filter out records without branch
df_plan = pd.read_sql_query(query_plan,db,params={'sdate':sdate,'edate':edate})
df_var = df_var.merge(df_plan,on=['stcode','cntdate'],how='left')
df_var = df_var[df_var['branch'].notna()].drop(columns=['branch'])
print(f"Length of df_var after merging with plan: {len(df_var)}")
print(df_var.info())

df_var.rename(columns={
    'description':'product_name',
    'price':'retail'
}, inplace=True)

'''
df_var.to_sql('cfr_var',db,if_exists='append',index=False)
'''

df_stk = df_var.copy()
df_stk['qnt_retail'] = df_stk['qnt'] * df_stk['retail']
df_stk['rpname'] = 'STK2'
df_stk.drop(columns=['retail','location','seq'],inplace=True)
df_stk = df_stk.groupby(['dept', 'sub_dept', 'sku', 'product_name', 'cntdate', 'stcode', 'skutype', 'rpname']).agg({
    'qnt': 'sum',
    'qnt_retail': 'sum'
}).reset_index()
print(df_stk)
print(f"Data inserted into cfr_var successfully usetime: {pd.to_datetime('now')-startime}")

