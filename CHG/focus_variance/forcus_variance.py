import pathlib
import pandas as pd
from fpdf import FPDF

# --- ตั้งค่าตัวแปรเริ่มต้น ---
cntnum = '60023F150726001'
percent = 0.5

userpath = pathlib.Path.home()
path_save = userpath / "Downloads" / "forcus_variance.pdf"
path_input = userpath / "Downloads" / "book1.xlsx"

# --- โหลดข้อมูลจาก Excel ---
df = pd.read_excel(path_input, sheet_name="Sheet1", dtype=str)

print("--- ข้อมูลที่โหลดเข้ามาได้ ---")
print(df.head(5))  # แสดง 5 แถวแรกเพื่อตรวจสอบข้อมูล
print(df.info())

# --- 1. ออกแบบโครงสร้างเลเอาต์ (กำหนดพิกัดกล่องข้อความบนกระดาษ A4 หน่วยเป็น mm) ---
elements = [
    {
        "name": "step",
        "style": "B", "size": 16,
        "x1": 20, "y1": 20, "x2": 100, "y2": 30,
        "foreground": 0x000000
    },
    {
        "name": "deptcode",
        "style": "B", "size": 20, "align": "R",
        "x1": 150, "y1": 20, "x2": 190, "y2": 30,
    },
    {
        "name": "sku",
        "size": 12,
        "x1": 20, "y1": 50, "x2": 150, "y2": 60,
    },
    {
        "name": "barcode",
        "size": 12,
        "x1": 160, "y1": 50, "x2": 190, "y2": 60,
    },
    {
        "name": "prname",
        "size": 12,
        "x1": 20, "y1": 80, "x2": 190, "y2": 100,
    },
    {
        "name": "first_cntqnt",
        "size": 12,
        "x1": 20, "y1": 110, "x2": 80, "y2": 120,
    },
    {
        "name": "first_var_cost",
        "size": 12,
        "x1": 90, "y1": 110, "x2": 150, "y2": 120,
    },
    {
        "name": "final_cntqnt",
        "size": 12,
        "x1": 20, "y1": 130, "x2": 80, "y2": 140,
    },
    {
        "name": "final_var_cost",
        "size": 12,
        "x1": 90, "y1": 130, "x2": 150, "y2": 140,
    },
    {
        "name": "status",
        "size": 12,
        "x1": 20, "y1": 150, "x2": 190, "y2": 160,
    },
    {
        "name": "diff",
        "size": 12,
        "x1": 20, "y1": 170, "x2": 190, "y2": 180,
    }
]

# --- 2. เปิดใช้งานคลาส FPDF และตั้งค่าพื้นฐาน ---
fpdf = FPDF(format="A4", unit="mm")
fpdf.set_auto_page_break(auto=False)

# หาตำแหน่งโฟลเดอร์ย่อยที่โค้ดไฟล์นี้ตั้งอยู่จริง ๆ
current_dir = pathlib.Path(__file__).parent  
font_regular_path = current_dir / "Fonts" / "Sarabun-Regular.ttf"
font_bold_path = current_dir / "Fonts" / "Sarabun-Bold.ttf"

# แก้ปัญหาเรื่อง Encoding โดยการเปลี่ยน Path ของ Windows (\\) ให้เป็นรูปแบบสากล (/) ด้วย .as_posix()
# วิธีนี้จะทำให้แกนกลางของ FPDF2 มองเห็นสัญลักษณ์สแลชที่ถูกต้องและเปิดไฟล์ผ่านระบบสากลได้โดยไม่แครช
fpdf.add_font("THSarabun", style="", fname=font_regular_path.as_posix())
fpdf.add_font("THSarabunBold", style="", fname=font_bold_path.as_posix())

# ฟังก์ชันแปลงรหัสสี Hex เป็น RGB
def hex_to_rgb(hex_val):
    if hex_val is None:
        return 0, 0, 0
    r = (hex_val >> 16) & 0xFF
    g = (hex_val >> 8) & 0xFF
    b = hex_val & 0xFF
    return r, g, b

# --- 3. วนลูปข้อมูลใน DataFrame ทีละแถว เพื่อสร้าง PDF แยกหน้ากัน ---
print("\nกำลังประมวลผลไฟล์ PDF...")
for index, row in df.iterrows():
    fpdf.add_page()
    page_data = row.to_dict()
    
    # วนลูปนำค่าจาก elements เลเอาต์มาวาดลงหน้ากระดาษปัจจุบันทีละตัว
    for element in elements:
        name = element["name"]
        text_value = str(page_data.get(name, ""))
        
        x1 = element["x1"]
        y1 = element["y1"]
        x2 = element["x2"]
        y2 = element["y2"]
        
        # จัดการเรื่องตัวหนา / ตัวธรรมดา
        style = element.get("style", "")
        if "B" in style.upper():
            font_name = "THSarabunBold"
            current_style = ""
        else:
            font_name = "THSarabun"
            current_style = ""
            
        # เพิ่มขนาดฟอนต์อีก +4 เพื่อให้สัดส่วนของ Sarabun อ่านง่ายขึ้นบนหน้ากระดาษ
        size = element.get("size", 12) + 4 
        align = element.get("align", "L")
        
        w = x2 - x1
        h = y2 - y1
        
        # สั่งตั้งค่าฟอนต์ สี และตำแหน่งพิกัดเพื่อพิมพ์ข้อความ
        fpdf.set_font(font_name, style=current_style, size=size)
        
        r, g, b = hex_to_rgb(element.get("foreground", 0x000000))
        fpdf.set_text_color(r, g, b)
        
        fpdf.set_xy(x1, y1)
        fpdf.cell(w=w, h=h, txt=text_value, border=0, align=align)

# --- 4. บันทึกไฟล์ PDF ทั้งหมดรวมกันเป็นไฟล์เดียว ---
fpdf.output(str(path_save))

print(f"\nสร้าง PDF สำเร็จแล้วทั้งหมด {len(df)} หน้า!")
print(f"บันทึกไฟล์ไว้ที่: {path_save}")