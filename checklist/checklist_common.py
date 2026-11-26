"""ฟังก์ชันกลางสำหรับโหลด checklist จาก Excel เข้าตาราง checklist (pstdb)

replace_sets: แทนที่ทีละชุด (bu + stcode + cntdate)
  - ชุดที่ยังไม่มีในตาราง  -> เพิ่มเข้าไป
  - ชุดที่มีแล้วและเนื้อหาต่างจาก Excel -> ลบแถวของชุดนั้นทั้งหมดแล้วใส่ชุดใหม่
  - ชุดที่เหมือนเดิมทุกแถว -> ไม่แตะ
ใช้กับ BU ที่ question_code เดียวมีหลายข้อย่อย (ชีต Mapping มีหลายแถวต่อ code)
เพราะไม่มีคีย์เฉพาะระดับแถวให้ UPSERT

รันสคริปต์ด้วย --dry-run เพื่อดูจำนวนอย่างเดียว ไม่เขียนข้อมูล
"""
import io
import os
import sys
import time


# คอลัมน์ที่ใช้เทียบว่าชุดเปลี่ยนหรือไม่ (ไม่เทียบข้อความอธิบาย: zone, subject, subdescription, section, no)
COMPARE_COLS = ['stcode', 'cntdate', 'question_code', 'point', 'weight', 'full', 'act', 'checkdate']


def replace_sets(engine, df, bu, table='checklist', dry_run=None, compare_cols=COMPARE_COLS):
    if dry_run is None:
        dry_run = '--dry-run' in sys.argv
    if df.empty:
        print("ℹ️ No rows to load.")
        return

    df = df.convert_dtypes()  # กันจำนวนเต็มถูกเขียนเป็น "1.0"
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    q = lambda cols: ", ".join(f'"{c}"' for c in cols)
    cols = list(df.columns)
    n_cols = ", ".join(f'n."{c}"' for c in cols)
    bu = bu.upper()
    in_new = "(stcode, cntdate) IN (SELECT stcode, cntdate FROM tmp_new)"
    in_changed = "(stcode, cntdate) IN (SELECT stcode, cntdate FROM tmp_changed)"

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            # 1) โหลดข้อมูลใหม่ลง temp table (COPY)
            cur.execute(f'CREATE TEMP TABLE tmp_new ON COMMIT DROP AS '
                        f'SELECT {q(cols)} FROM {table} WITH NO DATA')
            cur.copy_expert(f'COPY tmp_new ({q(cols)}) FROM STDIN WITH (FORMAT csv)', buf)

            # 2) หาชุดที่ต่างจากในตาราง (เทียบแบบ multiset ทั้งสองทาง; EXCEPT ALL ถือว่า NULL เท่ากัน)
            cur.execute(f"""
                CREATE TEMP TABLE tmp_changed ON COMMIT DROP AS
                SELECT DISTINCT stcode, cntdate FROM (
                    (SELECT {q(compare_cols)} FROM tmp_new
                     EXCEPT ALL
                     SELECT {q(compare_cols)} FROM {table} WHERE bu = %s AND {in_new})
                    UNION ALL
                    (SELECT {q(compare_cols)} FROM {table} WHERE bu = %s AND {in_new}
                     EXCEPT ALL
                     SELECT {q(compare_cols)} FROM tmp_new)
                ) d
            """, (bu, bu))

            cur.execute("SELECT count(*) FROM (SELECT DISTINCT stcode, cntdate FROM tmp_new) t")
            n_sets = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM tmp_changed")
            n_changed = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM tmp_changed t WHERE EXISTS (SELECT 1 FROM {table} c "
                        f"WHERE c.bu = %s AND c.stcode = t.stcode AND c.cntdate = t.cntdate)", (bu,))
            n_replace = cur.fetchone()[0]
            n_add = n_changed - n_replace
            cur.execute(f"SELECT count(*) FROM {table} WHERE bu = %s AND {in_changed}", (bu,))
            n_delete = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM tmp_new WHERE {in_changed}")
            n_insert = cur.fetchone()[0]

            print(f"ชุดสาขา+วัน ใน Excel: {n_sets} | เหมือนเดิม: {n_sets - n_changed} | "
                  f"เพิ่มใหม่: {n_add} | แทนที่: {n_replace}")
            print(f"แถวที่จะลบ: {n_delete} | แถวที่จะเพิ่ม: {n_insert}")

            if dry_run:
                print("🔎 dry-run: ไม่ได้เขียนข้อมูล")
                conn.rollback()
                return
            if n_changed == 0:
                print("✅ ไม่มีอะไรเปลี่ยน")
                conn.rollback()
                return

            # 3) เก็บ area เดิมไว้ (คอลัมน์ area ถูกเติมโดยกระบวนการเก่า ไม่ได้มาจาก Excel)
            cur.execute(f"""
                CREATE TEMP TABLE tmp_area ON COMMIT DROP AS
                SELECT DISTINCT ON (stcode, cntdate, question_code)
                       stcode, cntdate, question_code, area
                FROM {table}
                WHERE bu = %s AND {in_changed} AND area IS NOT NULL
                ORDER BY stcode, cntdate, question_code, id DESC
            """, (bu,))

            # สำรองแถวที่จะถูกลบเป็น CSV (ตั้ง CHECKLIST_BACKUP_DIR เพื่อเปิดใช้)
            backup_dir = os.environ.get('CHECKLIST_BACKUP_DIR')
            if backup_dir and n_delete:
                bk = os.path.join(backup_dir, f"{bu.lower()}_replaced_{time.strftime('%Y%m%d_%H%M%S')}.csv")
                with open(bk, 'w', encoding='utf-8-sig', newline='') as f:
                    cur.copy_expert(f"COPY (SELECT * FROM {table} WHERE bu = '{bu}' AND {in_changed}) "
                                    f"TO STDOUT WITH (FORMAT csv, HEADER)", f)
                print(f"💾 สำรองแถวที่จะลบ {n_delete} แถว -> {bk}")

            # 4) ลบชุดเก่า แล้วใส่ชุดใหม่ ใน transaction เดียว
            cur.execute(f"DELETE FROM {table} WHERE bu = %s AND {in_changed}", (bu,))
            if cur.rowcount != n_delete:
                raise RuntimeError(f"ลบได้ {cur.rowcount} แถว ไม่ตรงที่คาด {n_delete} -> rollback")
            cur.execute(f"""
                INSERT INTO {table} ({q(cols)}, area)
                SELECT {n_cols}, a.area
                FROM tmp_new n
                JOIN tmp_changed t ON t.stcode = n.stcode AND t.cntdate = n.cntdate
                LEFT JOIN tmp_area a ON a.stcode = n.stcode AND a.cntdate = n.cntdate
                     AND a.question_code = n.question_code
            """)
            if cur.rowcount != n_insert:
                raise RuntimeError(f"เพิ่มได้ {cur.rowcount} แถว ไม่ตรงที่คาด {n_insert} -> rollback")
        conn.commit()
        print(f"✅ แทนที่ชุดสำเร็จ — ลบ {n_delete} แถว, เพิ่ม {n_insert} แถว")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# upsert_df: UPSERT ตามคีย์ (ใช้กับ BU ที่ question_code เดียว = 1 แถวต่อสาขา+วัน)
# ต้องมี partial unique index: CREATE UNIQUE INDEX ... ON checklist (stcode, cntdate, question_code) WHERE bu = '<BU>'
# ---------------------------------------------------------------------------
KEY_COLS = ['stcode', 'cntdate', 'question_code']
UPDATE_COLS = ['checkdate', 'section', 'no', 'subject', 'subdescription',
               'weight', 'full', 'act', 'point', 'zone']
# แถวจะถูก update ก็ต่อเมื่อคอลัมน์เหล่านี้ต่างจากในตาราง (ไม่นับข้อความอธิบาย/zone ที่ต่างกันแค่รูปแบบ)
# เมื่อ update แล้วจะเขียนทุกคอลัมน์ใน UPDATE_COLS ตาม Excel/Mapping ปัจจุบัน
CHANGE_COLS = ['checkdate', 'weight', 'full', 'act', 'point']


def upsert_df(engine, df, bu, table='checklist', key_cols=KEY_COLS, update_cols=UPDATE_COLS,
              change_cols=CHANGE_COLS, dry_run=None):
    if dry_run is None:
        dry_run = '--dry-run' in sys.argv
    if df.empty:
        print("ℹ️ No rows to upsert.")
        return

    bu = bu.upper()
    dup = df.duplicated(subset=['bu'] + key_cols, keep=False)
    if dup.any():
        # คีย์ซ้ำในชุดเดียวกัน (เช่น Mapping มีหลายแถวต่อ code) -> UPSERT ทำไม่ได้ ต้องใช้ replace_sets แทน
        raise RuntimeError(f"พบคีย์ซ้ำในข้อมูลที่จะโหลด {int(dup.sum())} แถว — "
                           f"BU นี้ไม่เหมาะกับ upsert (ใช้ replace_sets)")
    if not (df['bu'].str.upper() == bu).all():
        raise RuntimeError(f"คอลัมน์ bu ในข้อมูลมีค่าอื่นนอกจาก {bu}")

    df = df.convert_dtypes()
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    q = lambda cols: ", ".join(f'"{c}"' for c in cols)
    all_cols = list(df.columns)
    set_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    new_sql = "(" + ", ".join(f'EXCLUDED."{c}"' for c in change_cols) + ")"
    old_sql = "(" + ", ".join(f'{table}."{c}"' for c in change_cols) + ")"
    t_sql = "(" + ", ".join(f't."{c}"' for c in change_cols) + ")"
    c_sql = "(" + ", ".join(f'c."{c}"' for c in change_cols) + ")"
    join_sql = " AND ".join(f'c."{k}" = t."{k}"' for k in key_cols)

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TEMP TABLE tmp_upsert ON COMMIT DROP AS '
                        f'SELECT {q(all_cols)} FROM {table} WITH NO DATA')
            cur.copy_expert(f'COPY tmp_upsert ({q(all_cols)}) FROM STDIN WITH (FORMAT csv)', buf)
            if dry_run:
                cur.execute(f"""
                    SELECT count(*) FILTER (WHERE c.id IS NULL),
                           count(*) FILTER (WHERE c.id IS NOT NULL AND {c_sql} IS DISTINCT FROM {t_sql})
                    FROM tmp_upsert t
                    LEFT JOIN {table} c ON c.bu = %s AND {join_sql}
                """, (bu,))
                inserted, updated = cur.fetchone()
                conn.rollback()
                print(f"🔎 dry-run — จะ inserted: {inserted}, updated: {updated}, "
                      f"unchanged: {len(df) - inserted - updated} (ไม่ได้เขียนข้อมูล)")
                return
            cur.execute(f"""
                WITH up AS (
                    INSERT INTO {table} ({q(all_cols)})
                    SELECT {q(all_cols)} FROM tmp_upsert
                    ON CONFLICT ({q(key_cols)}) WHERE bu = '{bu}'
                    DO UPDATE SET {set_sql}
                    WHERE {old_sql} IS DISTINCT FROM {new_sql}
                    RETURNING (xmax = 0) AS inserted
                )
                SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) FROM up
            """)
            inserted, updated = cur.fetchone()
        conn.commit()
        print(f"✅ Upsert checklist สำเร็จ — inserted: {inserted}, updated: {updated}, "
              f"unchanged: {len(df) - inserted - updated}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
