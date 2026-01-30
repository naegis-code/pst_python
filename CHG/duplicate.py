#import pandas as pd
#from sqlalchemy import create_engine,text,bindparam
#import db_connect

cntnum_in = ['60016S050625001','60020S180625001','60022S120225002','60023S040625001','60025S060825001',
             '60026S210525001','60027S200825001','60028S050325001','60030S110625002','60031S030925001',
             '60918S230425001','60920S110625002','60921S081025001','60922S080525001','60923S151025001',
             '60924S151025001','60925S120325001','60926S100925001','60927S140825001','60928S250625001',
             '60929S120325001','60930S250625001','60931S020625001','60932S190325001','60934S110625001',
             '60935S170925001','60936S100425001','60937S170925001','60938S050325001','60939S260225001',
             '60940S140525001','60941S190225001','60942S160725001','60944S221025001','60945S060825001',
             '60946S280525001','60947S050825001','60950S270525001','60951S020725001','60952S190325001',
             '60953S070525001','60954S100925001','60955S230725001','60956S120325001','60958S210525001',
             '60959S090425001','60960S170425001','60961S090725001','60962S280525001','60976S030425001',
             '60978S210825001','60980S050225001','60981S070525001','60982S090725001','60983S090725001',
             '60984S190225001','60985S260325001','60986S260325001','60987S230425001','60989S221025001',
             '60990S230925001','60991S270825001','60992S030425001','60993S190625001','60996S151025001',
             '60997S030925001','61600S040625001']

rpname = 'STK1'
skutype = 'Paint'

engine_db = create_engine(db_connect.db_url_pstdb)
engine_db3 = create_engine(db_connect.db_url_pstdb3)

query = text("""
    SELECT cntnum,stmerch,cntdate,deptcode,deptname,
             subdeptcode,subdeptname,sku,sbc,ibc,bndname,
             prname,prmodel,soh,cntqnt,varianceqnt,extphycnt_retail,
             extphycnt_cost,extphy_retailvar,extphy_costvar,skutype,rpname
    FROM chg_stk_this_year
    WHERE rpname = :rpname
      AND skutype = :skutype
      AND cntnum IN :cntnum_in
""").bindparams(
    bindparam("cntnum_in", expanding=True)
)

df = pd.read_sql_query(query,engine_db3,
    params={
        "rpname": rpname,
        "skutype": skutype,
        "cntnum_in": cntnum_in
    }
)

print(f"Total records found: {len(df)}")

query_delete = text("""
    DELETE FROM chg_stk
    WHERE rpname = :rpname
      AND skutype = :skutype
      AND cntnum IN :cntnum_in
""").bindparams(
    bindparam("cntnum_in", expanding=True)
)
with engine_db.connect() as conn:
    conn.execute(
        query_delete,
        {
            "rpname": rpname,
            "skutype": skutype,
            "cntnum_in": cntnum_in
        }
    )
    conn.commit()
    print("Existing records deleted.")

df.to_sql('chg_stk',engine_db,if_exists='append',index=False,chunksize=1000,method='multi')
print("New records inserted.")