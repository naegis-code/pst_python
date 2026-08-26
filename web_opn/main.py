import io
import json
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
import pandas as pd
from sqlalchemy import create_engine, text
from upload_chg_stk import process_chg_stk

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
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents), sheet_name=0)
        df.columns = df.columns.str.strip().str.lower()
        print(df)
        if df.empty:
            raise HTTPException(status_code=400, detail="ไฟล์ไม่มีข้อมูล")

        # 🎯 เช็คเงื่อนไข bu == 'CHG' และ rpname ขึ้นต้นด้วย 'STK'
        if bu.upper() == "CHG" and rpname.upper().startswith("STK"):
            record_count = process_chg_stk(
                df=df,
                bu=bu,
                stcode=stcode,
                cntdate=cntdate,
                rpname=rpname,
                skutype=skutype,
                engine3=engine3
            )
        else:
            # Code สำหรับ Logic การอัปโหลดแบบปกติ
            record_count = len(df)

        return {
            "status": "success",
            "message": f"อัปโหลด/แก้ไขข้อมูล {rpname}-{skutype} จำนวน {record_count} รายการเรียบร้อยแล้ว",
        }

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"เกิดข้อผิดพลาดในการประมวลผล: {str(e)}"
        )