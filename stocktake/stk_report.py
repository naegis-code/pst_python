"""
สรุปผลตรวจนับสต็อก (stocktake) ของแต่ละ BU แล้วเพิ่มลงตาราง stk_report (pstdb3)

ขั้นตอน:
    1. อ่านตาราง {bu}_stk_this_year แล้ว group by สาขา / วันนับ / รอบนับ (rpname) / skutype
    2. เทียบกับ stk_report ด้วย anti-join -> เหลือเฉพาะแถวที่ยังไม่เคยโหลด
    3. เขียนแถวใหม่ด้วย COPY (เร็วกว่า to_sql มาก) ใน transaction เดียว

หมายเหตุ:
    - เพิ่มแถวใหม่อย่างเดียว ไม่ลบ/ไม่อัปเดตแถวเดิม -> รันซ้ำได้ ไม่เกิดข้อมูลซ้ำ
    - ถ้าข้อมูลต้นทางของสาขา/วันนับเดิมถูกแก้ภายหลัง แถวใน stk_report จะไม่เปลี่ยนตาม
    - ยังไม่รองรับ CFR (คอลัมน์ต้นทางต่างกัน)

วิธีใช้:
    python stocktake/stk_report.py            # รัน BU ที่กำหนดใน __main__
    from stocktake.stk_report import import_stk_report; import_stk_report('SSP')
"""
import io
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))  # ให้ import tunnel.py จาก root ของ repo ได้

from tunnel import engine3  # import แล้ว tunnel.py จะเช็ค/เปิด SSH tunnel ให้เอง

# คีย์ที่ใช้ระบุแถวซ้ำระหว่างต้นทางกับ stk_report
KEY_COLS = ['bu', 'stcode', 'cntdate', 'skutype', 'rpname']


def import_stk_report(bu):
    bu = bu.upper()
    table_src = f'{bu.lower()}_stk_this_year'
    table_dis = 'stk_report'

    # ---- 1. query สรุปผลนับ: แต่ละกลุ่ม BU ใช้ชื่อคอลัมน์ต้นทางต่างกัน แต่ผลลัพธ์ชื่อเดียวกัน ----
    # p* = จำนวนชิ้น (pieces), v* = มูลค่าทุน (cost), vr* = มูลค่าขาย (retail)
    # s* = จำนวน SKU ที่ gain/loss
    if bu in ['CHG', 'PWB', 'CFW']:
        query_src = f"""
            select stmerch as stcode ,cntdate ,rpname ,skutype ,
                count(*) as sku,
                sum(
                    case when varianceqnt > 0 then 1 else 0 end) as sgain,
                sum(
                    case when varianceqnt < 0 then 1 else 0 end) as sloss,
                sum(soh) as psoh,
                sum(cntqnt) as pqty,
                sum(
                    case when varianceqnt > 0 then varianceqnt else 0 end) as pgain,
                sum(
                    case when varianceqnt < 0 then varianceqnt else 0 end) as ploss,
                sum(extphycnt_retail -extphy_retailvar) as vrsoh,
                sum(extphycnt_cost -extphy_costvar) as vsoh,
                sum(extphycnt_retail) as vrqty,
                sum(extphycnt_cost) as vqty,
                sum(
                    case when extphy_retailvar > 0 then extphy_retailvar else 0 end) as vrgain,
                sum(
                    case when extphy_costvar > 0 then extphy_costvar else 0 end) as vgain,
                sum(
                    case when extphy_retailvar < 0 then extphy_retailvar else 0 end) as vrloss,
                sum(
                    case when extphy_costvar < 0 then extphy_costvar else 0 end) as vloss
            from {table_src} psty
            group by stmerch ,cntdate ,rpname ,skutype
        """
    elif bu in ['SSP', 'B2S', 'OFM']:
        # ใช้ store ไม่ใช้ stcode เพราะ b2s_stk_this_year.stcode มีค่า NULL บางแถว
        query_src = f"""
                select store as stcode ,cntdate ,rpname ,skutype ,
                    count(*) as sku,
                    sum(
                        case when qty_var > 0 then 1 else 0 end) as sgain,
                    sum(
                        case when qty_var < 0 then 1 else 0 end) as sloss,
                    sum(soh) as psoh,
                    sum(qty_count) as pqty,
                    sum(
                        case when qty_var > 0 then qty_var else 0 end) as pgain,
                    sum(
                        case when qty_var < 0 then qty_var else 0 end) as ploss,
                    sum(phycnt_rtl-extrtl_var) as vrsoh,
                    sum(phycnt_cst-extcst_var) as vsoh,
                    sum(phycnt_rtl) as vrqty,
                    sum(phycnt_cst) as vqty,
                    sum(
                        case when extrtl_var > 0 then extrtl_var else 0 end) as vrgain,
                    sum(
                        case when extcst_var > 0 then extcst_var else 0 end) as vgain,
                    sum(
                        case when extrtl_var < 0 then extrtl_var else 0 end) as vrloss,
                    sum(
                        case when extcst_var < 0 then extcst_var else 0 end) as vloss
                from {table_src} ssty
                group by store ,cntdate ,rpname ,skutype
            """
    else:
        raise ValueError(f"Unsupported BU: {bu}")

    df_src = pd.read_sql(query_src, engine3)
    df_src['bu'] = bu

    # ---- 2. เอาเฉพาะแถวที่ยังไม่มีใน stk_report (left_anti ต้องใช้ pandas >= 3.0) ----
    df_dis = pd.read_sql(f"select {', '.join(KEY_COLS)} from {table_dis}", engine3)
    df = df_src.merge(df_dis, how='left_anti', on=KEY_COLS)

    print(f"[{bu}] source rows: {len(df_src):,} | existing rows: {len(df_dis):,} | new rows: {len(df):,}")

    if df.empty:
        print("ไม่มีข้อมูลใหม่")
        return

    # ---- 3. เขียนด้วย COPY: แปลง df เป็น CSV ในหน่วยความจำ แล้วส่งทีเดียว ----
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    col_list = ", ".join(f'"{c}"' for c in df.columns)  # ใส่ quote กันชื่อคอลัมน์ชน keyword

    conn = engine3.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table_dis} ({col_list}) FROM STDIN WITH (FORMAT csv)", buf)
            print(f"Inserted {cur.rowcount:,} rows into {table_dis}")
        conn.commit()
    except Exception:
        conn.rollback()  # error -> ไม่มีแถวไหนถูกเขียน ข้อมูลเดิมไม่เสีย
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    import_stk_report('B2S')
    import_stk_report('OFM')
    import_stk_report('SSP')
