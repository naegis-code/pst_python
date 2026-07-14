import os
import pandas as pd
from fpdf import FPDF
from sqlalchemy import create_engine, text
import db_connect as dbc

# 1. การตั้งค่า Path ของฟอนต์โดยอ้างอิงตำแหน่งสคริปต์ปัจจุบัน
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FONT_DIR = os.path.join(BASE_DIR, "Fonts")

FONT_REGULAR = os.path.join(FONT_DIR, "Sarabun-Regular.ttf")
FONT_BOLD = os.path.join(FONT_DIR, "Sarabun-Bold.ttf")
FONT_ITALIC = os.path.join(FONT_DIR, "Sarabun-Italic.ttf")
FONT_BARCODE = os.path.join(FONT_DIR, "LibreBarcode128Text-Regular.ttf")

# 2. ออกแบบคลาส PDF แนวนอน (Landscape) พร้อมกำหนดโครงสร้าง Layout
class VarianceReportPDF(FPDF):
    def __init__(self, title_text="", doc_info=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title_text = title_text
        self.doc_info = doc_info if doc_info is not None else {}
        self.current_dept_code = ""
        self.current_dept_name = ""
        # ปิดระบบเช็คสิทธิ์ฟอนต์ลิขสิทธิ์ของไฟล์บาร์โค้ด W39MC.ttf
        self.protect_embedded_fonts = False

    def header(self):
        # โหลดฟอนต์หลักสำหรับหัวข้อรายงาน
        self.set_font('Sarabun', 'B', 16)
        
        # --- บรรทัดที่ 1: docname เป็นชื่อ Report ตรงกลาง ---
        docname = self.doc_info.get('docname', self.title_text)
        self.cell(0, 10, docname, ln=True, align='C')
        
        self.set_font('Sarabun', '', 11)
        
        # --- บรรทัดที่ 2: buname (ซ้าย) / lossamt1, lossamt2 (กลาง) / prndate (ขวา) ---
        buname = f"BU: {self.doc_info.get('buname', '')}"
        loss_text = f"Loss Amt 1: {self.doc_info.get('lossamt1', '0.00')}   Loss Amt 2: {self.doc_info.get('lossamt2', '0.00')}"
        prndate = f"พิมพ์เมื่อ: {self.doc_info.get('prndate', '')}"
        
        self.cell(70, 7, buname, ln=False, align='L')
        self.set_x(70)  # ย้ายตำแหน่งเพื่อเขียนกึ่งกลางหน้า
        self.cell(137, 7, loss_text, ln=False, align='C')
        self.set_x(207) # ขยับตำแหน่งไปชิดขวา
        self.cell(70, 7, prndate, ln=True, align='R')
        
        # --- บรรทัดที่ 3: cntnum (ซ้าย) / gainamt1, gainamt2 (กลาง) / freeztstt (ขวา) ---
        cntnum = f"เลขที่ตรวจนับ: {self.doc_info.get('cntnum', '')}"
        gain_text = f"Gain Amt 1: {self.doc_info.get('gainamt1', '0.00')}   Gain Amt 2: {self.doc_info.get('gainamt2', '0.00')}"
        freeztstt = f"สถานะ Freeze: {self.doc_info.get('freeztstt', '')}"
        
        self.cell(70, 7, cntnum, ln=False, align='L')
        self.set_x(70)
        self.cell(137, 7, gain_text, ln=False, align='C')
        self.set_x(207)
        self.cell(70, 7, freeztstt, ln=True, align='R')
        
        # --- บรรทัดที่ 4: แสดงแผนก deptcode, deptname เลื่อนลงมาชิดซ้าย ---
        self.set_font('Sarabun', 'B', 12)
        dept_text = f"แผนก: {self.current_dept_code} - {self.current_dept_name}"
        self.cell(0, 8, dept_text, ln=True, align='L')
        self.ln(2)
        
        # เรียกฟังก์ชันวาด Column Header เสมอเมื่อมีการขึ้นหน้าใหม่
        self.draw_column_header()

    def draw_column_header(self):
        self.set_font('Sarabun', 'B', 10)
        self.set_fill_color(230, 235, 245) # สีพื้นหลังหัวตาราง
        
        # กำหนดความกว้างคอลัมน์ (รวมกันได้ 277 มม. พอดีหน้ากระดาษ A4 แนวนอนหักระยะขอบ)
        headers = [
            ("Location", 20), ("SKU Code", 22), ("Barcode IBC", 28), 
            ("Barcode SBC1", 28), ("Barcode SBC2", 28), ("Product Name", 55), 
            ("Qty", 12), ("Type", 12), ("Var", 12), ("นับใหม่", 18), 
            ("เซ็นต์", 18), ("Bar Loc SKU", 44)
        ]
        
        for text_label, width in headers:
            self.cell(width, 8, text_label, border=1, align='C', fill=True)
        self.ln(8)
        self.set_font('Sarabun', '', 10) # คืนฟอนต์ตัวธรรมดา

    def footer(self):
        # ลายเซ็นท้ายกระดาษ 3 คน (Verify, Store, Editor)
        self.set_y(-25)
        self.set_font('Sarabun', '', 10)
        
        self.cell(92, 5, "Verify ____________________", border=0, ln=False, align='C')
        self.cell(92, 5, "Store ____________________", border=0, ln=False, align='C')
        self.cell(93, 5, "Editor ____________________", border=0, ln=True, align='C')
        
        # แสดงเลขหน้าอยู่ระดับล่างสุดขวาจัด
        self.set_y(-12)
        self.cell(0, 5, f"หน้า {self.page_no()}/{{nb}}", border=0, align='R')

def generate_pdf(df):
    if df.empty:
        print("ไม่มีข้อมูลส่งเข้ามาสร้างรายงาน")
        return
        
    # จัดเตรียมข้อมูลรายละเอียดหัวข้อเดี่ยวๆ จากแถวแรกสุด
    first_row = df.iloc[0]
    
    def format_money(val):
        try:
            return f"{float(val):,.2f}" if pd.notna(val) and val != "" else "0.00"
        except ValueError:
            return "0.00"

    doc_info = {
        'docname': str(first_row.get('docname', 'รายงาน Variance')),
        'buname': str(first_row.get('buname', '')),
        'prndate': str(first_row.get('prndate', '')),
        'cntnum': str(first_row.get('cntnum', '')),
        'freeztstt': str(first_row.get('freeztstt', '')),
        'lossamt1': format_money(first_row.get('lossamt1')),
        'lossamt2': format_money(first_row.get('lossamt2')),
        'gainamt1': format_money(first_row.get('gainamt1')),
        'gainamt2': format_money(first_row.get('gainamt2')),
    }

    # ประกาศหน้ากระดาษ แนวนอน ('L')
    pdf = VarianceReportPDF(orientation='L', unit='mm', format='A4', doc_info=doc_info)
    pdf.alias_nb_pages()
    
    # ลงทะเบียนฟอนต์ภาษาไทยและบาร์โค้ด
    pdf.add_font('Sarabun', '', FONT_REGULAR, uni=True)
    pdf.add_font('Sarabun', 'B', FONT_BOLD, uni=True)
    pdf.add_font('Sarabun', 'I', FONT_ITALIC, uni=True)
    pdf.add_font('Barcode', '', FONT_BARCODE, uni=True)

    # จัดกลุ่มเรียงตามแผนกก่อน เพื่อรันคำสั่ง Pagebreak ให้ถูกต้อง
    df_sorted = df.sort_values(by=['deptcode']).reset_index(drop=True)
    
    current_dept = None
    line_height = 7
    
    for index, row in df_sorted.iterrows():
        dept_code = str(row.get('deptcode', ''))
        dept_name = str(row.get('deptname', ''))
        
        # --- ตรรกะ Pagebreak: ถ้าขึ้น deptcode ใหม่ ให้ขึ้นแผ่นใหม่ทันที ---
        if current_dept is None:
            current_dept = dept_code
            pdf.current_dept_code = dept_code
            pdf.current_dept_name = dept_name
            pdf.add_page()
        elif dept_code != current_dept:
            current_dept = dept_code
            pdf.current_dept_code = dept_code
            pdf.current_dept_name = dept_name
            pdf.add_page()  # สั่งเปิดหน้าแผ่นใหม่
            
        # --- DETAIL ROWS ---
        pdf.cell(20, line_height, str(row.get('location', '')), border=1, align='C')
        pdf.cell(22, line_height, str(row.get('skcode', '')), border=1, align='C')
        pdf.cell(28, line_height, str(row.get('baribc', '')), border=1, align='L')
        pdf.cell(28, line_height, str(row.get('barsbc1', '')), border=1, align='L')
        pdf.cell(28, line_height, str(row.get('barsbc2', '')), border=1, align='L')
        
        # คุมความยาว Product Name ไม่ให้ล้นตาราง
        prname = str(row.get('prname', ''))
        if len(prname) > 28:
            prname = prname[:26] + ".."
        pdf.cell(55, line_height, prname, border=1, align='L')
        
        # ตัวเลขจำนวนและส่วนต่าง (Format ปราศจากทศนิยมตามข้อมูลคุณ)
        cntqnt_val = f"{float(row.get('cntqnt')):,.0f}" if pd.notna(row.get('cntqnt')) and row.get('cntqnt') != "" else "0"
        variance_val = f"{float(row.get('variance')):,.0f}" if pd.notna(row.get('variance')) and row.get('variance') != "" else "0"
        
        pdf.cell(12, line_height, cntqnt_val, border=1, align='R')
        pdf.cell(12, line_height, str(row.get('prtype', '')), border=1, align='C')
        pdf.cell(12, line_height, variance_val, border=1, align='R')
        
        # ช่องว่างเปล่าเสริมสำหรับงานเขียนมือ: "นับใหม่" และ "เซ็นต์"
        pdf.cell(18, line_height, "", border=1, align='C')
        pdf.cell(18, line_height, "", border=1, align='C')
        
        # คอลัมน์ขวาสุด สลับใช้ฟอนต์ Barcode วาดรูปบาร์โค้ด
        pdf.set_font('Barcode', '', 12)
        pdf.cell(44, line_height, str(row.get('bar_loc_sku', '')), border=1, align='C')
        pdf.set_font('Sarabun', '', 10)  # สลับกลับเป็นฟอนต์รายงานปกติ
        
        pdf.ln(line_height)
        
    output_pdf = os.path.join(BASE_DIR, "variance_report_final.pdf")
    pdf.output(output_pdf)
    print(f"แปลงไฟล์ PDF สำเร็จแล้ว บันทึกไฟล์ที่: {output_pdf}")

# --- ตัวบล็อกรันเชื่อมโยงฐานข้อมูลตามสคริปต์ของคุณ ---
if __name__ == "__main__":
    # การดึงข้อมูล SQL ผ่าน SQLAlchemy
    db3 = create_engine(dbc.db_url_pstdb3)
    stocktakeid = '60016F180626001'
    
    var = text("""
        select docname, buname, prndate, cntnum, freeztstt, lossamt1, lossamt2, gainamt1, gainamt2, 
               deptcode, deptname, location, skcode, baribc, barsbc1, barsbc2, prname, bndname, 
               model, color, cntqnt, prtype, variance, concat('*', location, skcode, '*') as bar_loc_sku
        from chg_var_this_year cvty 
        where rpname = 'VAR1' and cntnum = :stocktakeid
    """)
    
    df = pd.read_sql_query(sql=var, con=db3, params={"stocktakeid": stocktakeid})
    
    # ส่งข้อมูลเข้าฟังก์ชันแปลง PDF
    generate_pdf(df)