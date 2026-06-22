from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from datetime import datetime
from typing import Optional
import db_connect as dbc
from decimal import Decimal # 👈 นำเข้า Decimal

# ข้อ 1: เชื่อมต่อฐานข้อมูลโดยตรงตาม URL ที่ระบุ
DATABASE_URL = dbc.db_url_pstdbtest

engine = create_async_engine(DATABASE_URL, pool_size=20, max_overflow=10)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

app = FastAPI(title="PST Stocktake API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_db():
    async with async_session() as session:
        yield session

# --- Models ---
class LoginModel(BaseModel):
    username: str
    password: str

class ScanBarcodeModel(BaseModel):
    stocktakeid: str
    barcode: str

class SaveCountModel(BaseModel):
    stocktakeid: str
    location_id: str
    barcode: str
    sku: str
    description: str
    status: str
    color: str
    size: str
    retail: float
    qty: float
    username: str

# --- API Endpoints ---

# ข้อ 1: ระบบตรวจสอบ Login ตรงๆ จากฐานข้อมูล
@app.post("/api/login")
async def login(data: LoginModel, db: AsyncSession = Depends(get_db)):
    query = text("SELECT username FROM app_users WHERE username = :u AND password = :p")
    result = await db.execute(query, {"u": data.username, "p": data.password})
    username = result.fetchone()
    if not username:
        raise HTTPException(status_code=401, detail="Username หรือ Password ไม่ถูกต้อง")
    return {"status": "success", "username": username[0]}

# ข้อ 3: ตรวจสอบความถูกต้องของ StocktakeID ก่อนเริ่มงาน
@app.get("/api/check-stocktake/{stocktakeid}")
async def check_stocktake(stocktakeid: str, db: AsyncSession = Depends(get_db)):
    query = text("SELECT stocktakeid, status FROM stocktake_id WHERE stocktakeid = :id")
    result = await db.execute(query, {"id": stocktakeid})
    st = result.fetchone()
    if not st:
        raise HTTPException(status_code=444, detail="ไม่พบข้อมูล Stocktake ID นี้ในระบบ")
    return {"status": "success", "stocktakeid": st[0], "st_status": st[1]}

# ข้อ 5: ค้นหาข้อมูลสินค้าจากตาราง Master โดยผูกกับ Stocktake ID
@app.post("/api/find-product")
async def find_product(data: ScanBarcodeModel, db: AsyncSession = Depends(get_db)):
    query = text("""
        SELECT sku, description, status, color, size, retail 
        FROM master 
        WHERE stocktakeid = :st_id AND barcode = :barcode
    """)
    result = await db.execute(query, {"st_id": data.stocktakeid, "barcode": data.barcode})
    product = result.fetchone()
    
    if not product:
        raise HTTPException(status_code=404, detail="ไม่พบข้อมูลสินค้า")
        
    return {
        "status": "success",
        "sku": product[0],
        "description": product[1],
        "status_code": product[2],
        "color": product[3],
        "size": product[4],
        "retail": Decimal(product[5])
    }

# ข้อ 6: บันทึกข้อมูลการสแกนลงตาราง count_scan
@app.post("/api/save-count")
async def save_count(data: SaveCountModel, db: AsyncSession = Depends(get_db)):
    try:
        query = text("""
            INSERT INTO count_scan (stocktakeid, location_id, sku, barcode, description, status, color, size, retail, qty, username, created, modify)
            VALUES (:st_id, :loc_id, :sku, :barcode, :desc, :status, :color, :size, :retail, :qty, :username, :now, :now)
        """)
        await db.execute(query, {
            "st_id": data.stocktakeid, "loc_id": data.location_id, "sku": data.sku, 
            "barcode": data.barcode, "desc": data.description, "status": data.status, 
            "color": data.color, "size": data.size, "retail": Decimal(str(data.retail)), "qty": Decimal(str(data.qty)), 
            "username": data.username, "now": datetime.now()
        })
        await db.commit()
        return {"status": "success", "message": "บันทึกข้อมูลสำเร็จ"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    

@app.get("/")
async def read_index():
    # ตรวจสอบว่าไฟล์ index.html อยู่ในโฟลเดอร์เดียวกันไหม
    # ถ้าอยู่คนละโฟลเดอร์ให้ใส่ path ให้ถูกต้อง เช่น "D:/Users/prthanap/Documents/html5/index.html"
    return FileResponse('index.html')