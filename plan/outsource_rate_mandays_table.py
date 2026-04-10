import pandas as pd
from sqlalchemy import create_engine, text
import db_connect

engine = create_engine(db_connect.db_url_pstdb)

query = """WITH vendor AS (
         SELECT unnest(ARRAY['AJIS'::text, 'PCS'::text, 'SSD'::text, 'Innovation'::text, 'Smollan'::text, 'Daywork'::text, 'PSTDIV'::text, 'Store'::text, 'HRoot'::text]) AS vendorname
        ), bus AS (
         SELECT unnest(ARRAY['CFR'::text, 'CFW'::text, 'B2S'::text, 'SSP'::text, 'OFM'::text, 'PWB'::text, 'CentralTham'::text, 'CHG'::text]) AS bu
        ), monthindex AS (
         SELECT unnest(ARRAY['1'::text, '2'::text, '3'::text, '4'::text, '5'::text, '6'::text, '7'::text, '8'::text, '9'::text, '10'::text, '11'::text, '12'::text]) AS mindex,
            unnest(ARRAY['January'::text, 'February'::text, 'March'::text, 'April'::text, 'May'::text, 'June'::text, 'July'::text, 'August'::text, 'September'::text, 'October'::text, 'November'::text, 'December'::text]) AS mname
        ), season AS (
         SELECT unnest(ARRAY['Moon'::text, 'Night'::text]) AS seasonindex
        ), std AS (
         SELECT province_hub.province,
            vendor.vendorname,
            season.seasonindex,
            bus.bu,
            monthindex.mindex,
            monthindex.mname,
                CASE
                    WHEN province_hub.province::text = ANY (ARRAY['กรุงเทพมหานคร'::character varying::text, 'นนทบุรี'::character varying::text, 'สมุทรปราการ'::character varying::text, 'นครปฐม'::character varying::text, 'สมุทรสาคร'::character varying::text, 'ปทุมธานี'::character varying::text]) THEN 'BKK'::text
                    WHEN province_hub.province::text = 'สุราษฎร์ธานี'::text AND vendor.vendorname = 'AJIS'::text THEN 'BKK'::text
                    ELSE 'UPC'::text
                END AS darea
           FROM province_hub,
            vendor,
            bus,
            monthindex,
            season
        )
 SELECT province,
    vendorname,
    seasonindex,
    bu,
    mindex,
    mname,
    darea,
        CASE
            WHEN vendorname = 'Store'::text THEN 500
            WHEN vendorname = 'PSTDIV'::text THEN 500
            WHEN vendorname = 'Daywork'::text THEN 600
            WHEN vendorname = 'SSD'::text THEN 900
            WHEN vendorname = 'HRoot'::text THEN 950
            WHEN vendorname = 'Smollan'::text AND seasonindex = 'Moon'::text THEN 575
            WHEN vendorname = 'Smollan'::text AND seasonindex = 'Night'::text THEN 600
            WHEN vendorname = 'Innovation'::text AND seasonindex = 'Moon'::text THEN 700
            WHEN vendorname = 'Innovation'::text AND seasonindex = 'Night'::text THEN 780
            WHEN vendorname = 'PCS'::text AND darea = 'BKK'::text THEN 1000
            WHEN vendorname = 'PCS'::text AND darea = 'UPC'::text THEN 1500
            WHEN vendorname = 'AJIS'::text AND bu <> 'CFW'::text AND seasonindex = 'Moon'::text THEN 1000
            WHEN vendorname = 'AJIS'::text AND bu <> 'CFW'::text AND seasonindex = 'Night'::text AND (mindex = ANY (ARRAY['1'::text, '4'::text, '5'::text, '6'::text, '7'::text, '12'::text])) THEN 1100
            WHEN vendorname = 'AJIS'::text AND bu <> 'CFW'::text AND seasonindex = 'Night'::text AND (mindex = ANY (ARRAY['2'::text, '3'::text, '8'::text, '9'::text, '10'::text, '11'::text])) THEN 1200
            WHEN vendorname = 'AJIS'::text AND bu = 'CFW'::text THEN 1500
            ELSE NULL::integer
        END AS price
   FROM std;"""

df = pd.read_sql_query(query, engine)

with engine.connect() as connection:
        connection.execute(text("delete from outsource_rate_manday;"))
        connection.commit()
        df.to_sql('outsource_rate_manday', con=engine, if_exists='append', index=False)
        print("Data inserted successfully into outsource_rate_mandays table.")
