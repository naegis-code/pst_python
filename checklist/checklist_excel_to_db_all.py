"""รันสคริปต์ checklist_raw.xlsx -> ตาราง checklist ของทุก BU ตามลำดับ

    python -X utf8 checklist_excel_to_db_all.py             # รันจริง
    python -X utf8 checklist_excel_to_db_all.py --dry-run   # ดูจำนวนอย่างเดียว ไม่เขียนข้อมูล
    python -X utf8 checklist_excel_to_db_all.py ssp chg     # เลือกเฉพาะบาง BU

แต่ละสคริปต์รันเป็น process แยก ถ้าตัวใดล้มเหลวจะรันตัวถัดไปต่อ แล้วสรุปผลตอนท้าย
(ปิดไฟล์ checklist_raw.xlsx ใน Excel ก่อนรัน)
"""
import os
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent

# ชื่อย่อ -> ไฟล์สคริปต์
SCRIPTS = {
    'ssp': 'ssp_checklist_excel_to_db.py',
    'chg': 'chg_checklist_excel_to_db.py',
    'cfr': 'cfr_checklist_excel_to_db.py',
    'cfr_cmk': 'cfr_cmk_checklist_excel_to_db.py',
    'cfr_pnm': 'cfr_pnm_checklist_excel_to_db.py',
    'cfr_topscare': 'cfr_topscare_checklist_excel_to_db.py',
    'b2s': 'b2s_checklist_excel_to_db.py',
    'ofm': 'ofm_checklist_excel_to_db.py',
    'pwb': 'pwb_checklist_excel_to_db.py',
}


def main():
    args = sys.argv[1:]
    flags = [a for a in args if a.startswith('--')]
    names = [a.lower() for a in args if not a.startswith('--')] or list(SCRIPTS)

    unknown = [n for n in names if n not in SCRIPTS]
    if unknown:
        sys.exit(f"ไม่รู้จัก: {', '.join(unknown)} (เลือกได้: {', '.join(SCRIPTS)})")

    env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
    results = {}
    for name in names:
        print(f"\n{'=' * 20} {name} {'=' * 20}", flush=True)
        rc = subprocess.run(
            [sys.executable, '-X', 'utf8', str(HERE / SCRIPTS[name]), *flags],
            cwd=HERE, env=env,
        ).returncode
        results[name] = rc

    print(f"\n{'=' * 20} สรุป {'=' * 20}")
    for name, rc in results.items():
        print(f"{'✅' if rc == 0 else '❌'} {name}" + ('' if rc == 0 else f" (exit {rc})"))
    sys.exit(1 if any(results.values()) else 0)


if __name__ == '__main__':
    main()
