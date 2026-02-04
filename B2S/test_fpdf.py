import pandas as pd
from fpdf import FPDF

# Load CSV with error handling
file_path = r'D:\Users\prthanap\OneDrive - Central Group\python\B2S\b2s_count4.csv'
df = pd.read_csv(file_path)

# Convert columns to string
for col in ['sbc1', 'sbc2', 'sbc3', 'sbc4', 'sbc5']:
    df[col] = df[col].astype(str)

# Function to format barcode
def format_barcode(row):
    columns = ['sbc1', 'sbc2', 'sbc3', 'sbc4', 'sbc5']
    values = [row[col] for col in columns if row[col] != '0']  # Filter out '0'
    return ",".join(values) if values else None

# Apply the function to each row
df["barcode"] = df.apply(format_barcode, axis=1)

# Select required columns
df = df[["cntnum", "dept", "dept_name", "sub_dpt", "s_dpt_name", "location", "sku", "ibc", "barcode", "sku_descr", "qnt", "extcst_var"]]

# Convert columns to string
df["sku_descr"] = df["sku_descr"].astype(str)
df["barcode"] = df["barcode"].astype(str)

# Group data
df_grouped = df.groupby(["cntnum", "dept", "dept_name", "sub_dpt", "s_dpt_name"])

# Sort data
df = df.sort_values(by=["dept", "sku", "location"])

# Create PDF class
class PDF(FPDF):
    def header(self):
        self.set_font("saraban", "B", 16)
        self.cell(200, 10, "Variance Report", ln=True, align="C")
        self.ln(5)

# Create PDF
pdf = PDF()
# Add fonts
pdf.add_font('saraban', '', r"D:\Users\prthanap\OneDrive - Central Group\python\B2S\Sarabun\Sarabun-Regular.ttf", uni=True)
pdf.add_font('saraban', 'B', r"D:\Users\prthanap\OneDrive - Central Group\python\B2S\Sarabun\Sarabun-SemiBold.ttf", uni=True)

pdf.add_page()
pdf.set_auto_page_break(auto=True, margin=15)

# Add fonts
pdf.add_font('saraban', '', r"D:\Users\prthanap\OneDrive - Central Group\python\B2S\Sarabun\Sarabun-Regular.ttf", uni=True)
pdf.add_font('saraban', 'B', r"D:\Users\prthanap\OneDrive - Central Group\python\B2S\Sarabun\Sarabun-SemiBold.ttf", uni=True)

pdf.set_font("saraban", size=8)

current_dept = None

# Loop to create table
for (cntnum, dept, dept_name, sub_dpt, s_dpt_name), group in df_grouped:
    if current_dept != dept:
        pdf.add_page()
        pdf.set_font("saraban", "B", 16)
        pdf.cell(0, 10, f"แผนก: {dept_name} ({dept})", ln=True)
        pdf.ln(5)
        current_dept = dept

    pdf.set_font("saraban", "B", 8)
    pdf.cell(0, 7, f"{sub_dpt} - {s_dpt_name}", ln=True)
    
    pdf.cell(13, 7, "SKU", 1)
    pdf.cell(14, 7, "IBC", 1)
    pdf.cell(25, 7, "Barcode", 1)
    pdf.cell(60, 7, "รายละเอียด", 1)
    pdf.cell(20, 7, "จำนวน", 1)
    pdf.cell(20, 7, "มูลค่า", 1)
    pdf.ln()

    pdf.set_font("saraban", size=8)
    for _, row in group.iterrows():
        pdf.cell(13, 7, str(row["sku"]), 1)
        pdf.cell(14, 7, str(row["ibc"]), 1)
        pdf.cell(25, 7, str(row["barcode"]), 1)
        pdf.cell(60, 7, str(row["sku_descr"]), 1)
        pdf.cell(20, 7, str(row["qnt"]), 1)
        pdf.cell(20, 7, str(row["extcst_var"]), 1)
        pdf.ln()
    
    pdf.ln(5)

# Save PDF
pdf.output("variance_report.pdf")
print("✅ PDF สร้างสำเร็จ: variance_report.pdf")
