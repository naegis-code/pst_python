# CLAUDE.md

สคริปต์ Python สำหรับงาน stocktake / ตรวจนับสต็อก ของหลาย BU (CHG, PWB, CFW, SSP, B2S, OFM, CFR ฯลฯ)
ส่วนใหญ่เป็นสคริปต์เดี่ยวที่รันตรง ไม่มี package, test หรือ build system
คุยกับผู้ใช้เป็นภาษาไทย

## โครงสร้าง

- โฟลเดอร์ระดับบนแยกตาม BU หรือตามงาน: `B2S/`, `CHG/`, `PWB/`, `OFM/`, `CFR/`, `SOH/`, `Missrate/`, `master/`, `checklist/` ฯลฯ
- `stocktake/` — นำเข้า และสรุปผลตรวจนับ รวมถึงรายงานของทุก BU
  - `stocktake/stk_report.py` — สรุปผลจาก `{bu}_stk_this_year` แล้วเพิ่มลงตาราง `stk_report` (ดู skill `stk-report-import`)
- `tunnel.py` (root) — engine กลางของฐานข้อมูล และตัวเปิด SSH tunnel

## ฐานข้อมูล

- PostgreSQL ต่อผ่าน SSH tunnel ด้วย alias `pst-db` ค่าเชื่อมต่อทั้งหมดอยู่ใน `.env` (DB_CONN, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_pstdb, DB_pstdb2, DB_pstdb3 …)
- สคริปต์ใหม่ให้ใช้ `tunnel.py`:
  ```python
  ROOT = Path(__file__).resolve().parents[1]
  sys.path.insert(0, str(ROOT))
  from tunnel import engine3          # engine1=pstdb, engine2=pstdb2, engine3=pstdb3
  ```
  แค่ import `tunnel` ก็จะเช็คและเปิด tunnel ให้อัตโนมัติ
- ตาราง stocktake ส่วนใหญ่อยู่ใน **pstdb3** (`engine3`) เช่น `{bu}_stk_this_year` และ `stk_report`
- `stocktake/db_connect.py` เป็นวิธีเก่าที่เขียนรหัสผ่านไว้ในโค้ด ห้ามใช้ในสคริปต์ใหม่

## แนวทางการเขียนโค้ด

- โหลดข้อมูลเข้า PostgreSQL ด้วย **COPY** (`cur.copy_expert`) ผ่าน `engine.raw_connection()` ใน transaction เดียว ห้ามใช้ `to_sql(method='multi')` และห้ามใช้ `if_exists='replace'` กับตารางที่มีอยู่แล้ว
- ใส่เครื่องหมายคำพูดครอบชื่อคอลัมน์ใน COPY เสมอ เพราะบางชื่อชนกับ keyword เช่น `class`, `size`
- ไฟล์ CSV และ text ภาษาไทยรุ่นเก่ามักเป็น `cp874`
- pandas 3.0 ใช้ `merge(how='left_anti')` เพื่อหาแถวที่ยังไม่มีในตารางปลายทางได้
- ถ้ารันผ่าน shell ที่ไม่ใช่ UTF-8 ให้ตั้ง `PYTHONIOENCODING=utf-8` เพราะ `tunnel.py` พิมพ์ emoji ออกมา

## ข้อควรระวัง

- ตาราง `stk_report` มีแถวซ้ำของ CFR อยู่ก่อนแล้ว **ห้ามลบหรือแก้** จนกว่าผู้ใช้จะสั่ง
- สคริปต์ส่วนใหญ่เขียนลงฐานข้อมูลจริงทันทีที่รัน ให้ถามผู้ใช้ก่อนรันสคริปต์ที่ลบหรืออัปเดตข้อมูล
