import io
import json
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
import pandas as pd
from sqlalchemy import create_engine, text

app = FastAPI()

user = "prthanapat"
paas = "20020015"

# ตั้งค่า Pool Connection รองรับผู้ใช้พร้อมกัน
engine1 = create_engine(
    f"postgresql://{user}:{paas}@localhost:5432/pstdb",
    pool_size=10,
    max_overflow=20,
)
engine3 = create_engine(
    f"postgresql://{user}:{paas}@localhost:5432/pstdb3",
    pool_size=10,
    max_overflow=20,
)


@app.get("/api/stores")
def get_stores(cntdate: str):
    cntdate = cntdate.replace("-", "")  # ลบขีดออกจากวันที่
    query = text("""
        SELECT bu, stcode, acronym, branch 
        FROM planall2 
        WHERE cntdate = :cntdate
          AND atype = '3F' 
        ORDER BY stcode
    """)
    with engine1.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"cntdate": cntdate})

    return json.loads(df.to_json(orient="records"))


@app.get("/api/report")
def get_stk_report(bu: str, stcode: str, cntdate: str):
    cntdate = cntdate.replace("-", "")  # ลบขีดออกจากวันที่

    query = text(f"""
        SELECT bu, stcode, cntdate, skutype, rpname, vgain + vloss AS net
        FROM stk_report
        WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate
        UNION ALL
        SELECT bu, stcode, cntdate, skutype, rpname, pqty AS net
        FROM var_report
        WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate
        UNION ALL
        SELECT bu, stcode, cntdate, skutype, rpname, pqty AS net
        FROM noc_zec_report
        WHERE bu = :bu AND stcode = :stcode AND cntdate = :cntdate
    """)
    with engine3.connect() as conn:
        df = pd.read_sql_query(
            query,
            conn,
            params={"bu": bu, "stcode": stcode, "cntdate": cntdate},
        )
        print(df)

    return json.loads(df.to_json(orient="records"))


@app.post("/api/upload-report")
async def upload_report(
    bu: str = Form(...),
    stcode: str = Form(...),
    cntdate: str = Form(...),
    rpname: str = Form(...),
    skutype: str = Form(...),
    file: UploadFile = File(...),
):
    # 1. ตรวจสอบนามสกุลไฟล์
    if not (file.filename.endswith(".xlsx") or file.filename.endswith(".xls")):
        raise HTTPException(
            status_code=400, detail="รองรับเฉพาะไฟล์ .xlsx หรือ .xls เท่านั้น"
        )

    try:
        # 2. อ่านไฟล์ Sheet แรกเสมอ (sheet_name=0)
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents), sheet_name=0)
        df.columns = df.columns.str.strip().str.lower()  # ลบช่องว่างรอบคอลัมน์และแปลงเป็นตัวพิมพ์เล็ก

        if df.empty:
            raise HTTPException(status_code=400, detail="ไฟล์ไม่มีข้อมูล")

        if (bu == "CHG" or bu == "PWB") and rpname.startswith("STK") and df.columns.tolist() == ['result', 'docname', 'buname', 'prndate', 'cntnum',
                                                                                         'cntname', 'stmerch', 'stname', 'postdate', 'freezedate',
                                                                                           'cntdate', 'deptcode', 'deptname', 'subdeptcode', 'subdeptname',
                                                                                             'sku', 'sbc', 'ibc', 'bndcode', 'bndname', 'prname', 'prmodel',
                                                                                               'soh', 'cntqnt', 'varianceqnt', 'varianceperc', 'extphycnt_retail',
                                                                                                 'extphycnt_cost', 'extphy_retailvar', 'extphy_costvar',
                                                                                                   'extphycnt_retail_exvat', 'gmperc'] and (df['stmerch'] == stcode).any() :
            raise HTTPException(
                status_code=400,
                detail="ไฟล์ STK ของ CHG หรือ PWB ต้องมีคอลัมน์ 32 คอลัมน์ และ stmerch ต้องตรงกับ stcode",
            )

        # จัดเตรียมข้อมูล Metadata
        clean_cntdate = cntdate.replace("-", "")
        df["bu"] = bu
        df["stcode"] = stcode
        df["cntdate"] = clean_cntdate
        df["rpname"] = rpname
        df["skutype"] = skutype

        # เลือกตารางปลายทาง
        table_name = (
            "cntfiles_this_year" if rpname == "CNTFILE" else (
                f"{bu.lower()}_var_this_year" if rpname.upper().startswith("VAR") else (
                    f"{bu.lower()}_stk_this_year" if rpname.upper().startswith("STK") else (
                        f"{bu.lower()}_sale_this_year" if rpname.upper().startswith("SALE") else (
                            f"{bu.lower()}_nocount_this_year" if rpname.upper().startswith("NoCount") else (
                                f"{bu.lower()}_zerocount_this_year" if rpname.upper().startswith("ZeroCount") else None
                            )
                        )
                    )
                )
            )
        )

        table_name_summary = (
            "cntfiles_report" if rpname == "CNTFILE" else (
                "var_report" if rpname.upper().startswith("VAR") else (
                    "stk_report" if rpname.upper().startswith("STK") else (
                        "sale_report" if rpname.upper().startswith("SALE") else (
                            "noc_report" if rpname.upper().startswith("NoCount") else (
                                "zerocount_report" if rpname.upper().startswith("ZeroCount") else None
                            )
                        )
                    )
                )
            )
        )
        with engine3.begin() as conn:
            # soft delete ข้อมูลเดิมก่อนอัปโหลดข้อมูลใหม่
            update_query = text(f"""
                UPDATE {table_name} 
                SET stcode = CONCAT(:stcode, 'E')
                WHERE stcode = :stcode 
                  AND cntdate = :cntdate
                  AND rpname = :rpname 
                  AND skutype = :skutype
            """)
            conn.execute(
                update_query,
                {
                    "stcode": stcode,
                    "cntdate": clean_cntdate,  # 💥 แก้ไข: ใช้ clean_cntdate
                    "rpname": rpname,
                    "skutype": skutype,
                },
            )

            # 4. บันทึกข้อมูลชุดใหม่ลง PostgreSQL
            df.to_sql(
                name=table_name, con=conn, if_exists="append", index=False
            )

        return {
            "status": "success",
            "message": f"อัปโหลด/แก้ไขข้อมูล {rpname}-{skutype} จำนวน {len(df)} รายการเรียบร้อยแล้ว",
        }

    except HTTPException as he:
        raise he  # 💥 ปล่อยให้ HTTPException คืนค่า Status 400 ตามปกติ
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"เกิดข้อผิดพลาดในการประมวลผล: {str(e)}"
        )