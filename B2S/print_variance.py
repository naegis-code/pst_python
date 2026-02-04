import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, PageBreak, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
import os

# โหลด CSV
file_path = r'D:\Users\prthanap\OneDrive - Central Group\python\B2S\b2s_count4.csv'
df = pd.read_csv(file_path)
df['sbc1'].astype(str)
df['sbc2'].astype(str)
df['sbc3'].astype(str)
df['sbc4'].astype(str)
df['sbc5'].astype(str)

# ฟังก์ชันรวมค่าโดยไม่เอาศูนย์และใช้ ',' คั่น
def format_barcode(row):
    columns = ['sbc1', 'sbc2', 'sbc3', 'sbc4', 'sbc5']
    values = [str(row[col]) for col in columns if row[col] != 0]  # กรองค่า 0 ออก
    return ",".join(values) if values else None  # ถ้าไม่มีค่าให้เป็น None

# ใช้ apply() กับแต่ละแถว
df["barcode"] = df.apply(format_barcode, axis=1)

# เลือกเฉพาะคอลัมน์ที่ต้องการ
df = df[["cntnum", "dept", "dept_name", "sub_dpt", "s_dpt_name", "location", "sku", "ibc", "barcode", "sku_descr", "qnt", "extcst_var"]]

# แปลงคอลัมน์ให้เป็น `str` ป้องกันปัญหา NaN หรือ int
df = df.astype(str)

# จัดกลุ่มข้อมูล
df_grouped = df.groupby(["cntnum", "dept", "dept_name", "sub_dpt", "s_dpt_name"])

# เรียงลำดับข้อมูล
df = df.sort_values(by=["dept", "sku", "location"])

# สร้าง PDF ไฟล์
output_file = "variance_report.pdf"
pdf = SimpleDocTemplate(output_file, pagesize=A4)

elements = []
styles = getSampleStyleSheet()

# ตั้งค่าฟอนต์ไทย (ต้องมีไฟล์ฟอนต์ `.ttf` ในเครื่อง)
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics

pdfmetrics.registerFont(TTFont('Noto Sans Thai', r"D:\Users\prthanap\OneDrive - Central Group\python\B2S\Sarabun\Sarabun-Regular.ttf"))

# Loop สร้างตารางตาม `dept`
current_dept = None

for (cntnum, dept, dept_name, sub_dpt, s_dpt_name), group in df_grouped:
    # ขึ้นหน้าใหม่เมื่อแผนกเปลี่ยน
    if current_dept != dept:
        if current_dept is not None:
            elements.append(PageBreak())  # ขึ้นหน้าใหม่

        elements.append(Paragraph(f"แผนก: {dept_name} ({dept})", styles["Title"]))
        current_dept = dept  

    # หัวตาราง
    table_data = [["SKU","IBC", "Barcode", "รายละเอียด", "จำนวน", "มูลค่า"]]

    # เพิ่มข้อมูลลงตาราง
    for _, row in group.iterrows():
        table_data.append([
            row["sku"],
            row["ibc"],
            row["barcode"],
            row["sku_descr"],
            row["qnt"],
            row["extcst_var"]
        ])

    # สร้างตาราง
    table = Table(table_data, colWidths=[40, 50, 10, 10, 10,10])
    table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Noto Sans Thai"),
        ("SIZE", (0, 0), (-1, -1), 8),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 1, colors.black)
    ]))

    elements.append(table)


# บันทึก PDF
pdf.build(elements)
print(f"✅ PDF สร้างสำเร็จ: {output_file}")
