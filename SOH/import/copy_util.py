"""Helper: โหลด DataFrame เข้า PostgreSQL ด้วย COPY (เร็วกว่า to_sql มาก)

- แบบ append เท่านั้น (ไม่ DELETE) เพราะตารางดิบเก็บประวัติ
- ทำใน transaction เดียว ถ้าพังจะ rollback ทั้งก้อน
- ครอบชื่อคอลัมน์ด้วยเครื่องหมายคำพูดเสมอ (class, size, ...)
"""
import io


def copy_df(engine, table, df):
    # convert_dtypes: ทำให้ค่าเต็มที่เป็นจำนวนเต็มไม่ถูกเขียนเป็น "1.0" (COPY ใส่ใน int column ไม่ได้)
    df = df.convert_dtypes()

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    col_list = ", ".join(f'"{c}"' for c in df.columns)

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.copy_expert(f'COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv)', buf)
            rows = cur.rowcount
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
