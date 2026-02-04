import pandas as pd
import qrcode
from PIL import Image, ImageDraw, ImageFont
from fpdf import FPDF
import os
import pathlib

# Path to user's Documents directory
userpath = pathlib.Path.home() / 'Documents'

EXCEL_FILE = 'label_location.xlsx'      # ชื่อไฟล์ Excel
SHEET_NAME = 'Sheet1'                   # ปรับชื่อ Sheet ตาม Excel

LABEL_WIDTH_CM = 5.0
LABEL_HEIGHT_CM = 3.0
PAGE_WIDTH_CM = 10.4
PAGE_HEIGHT_CM = 3.075
LEFT_MARGIN_CM = 0.2                    # ขอบซ้าย/ขวาของทั้งหน้า
DPI = 150                               # ความละเอียดสำหรับ label image

LABEL_WIDTH_PX = int(LABEL_WIDTH_CM * DPI / 2.54)
LABEL_HEIGHT_PX = int(LABEL_HEIGHT_CM * DPI / 2.54)
QR_SIZE_CM = 1.2                        # ขนาด QR code
QR_SIZE_PX = int(QR_SIZE_CM * DPI / 2.54)
IN_LABEL_MARGIN_PX = int(0.2 * DPI / 2.54)  # ขอบใน label

FONT_PATH = "THSarabunNew.ttf"          # ใช้ฟอนต์ไทย (หรือ Arial.ttf สำหรับอังกฤษ)
LOCATION_FONT_SIZE = 240                # Font ขนาดใหญ่ขึ้นสำหรับ location
LOCATIONNAME_FONT_SIZE = 120            # Font ขนาดปกติสำหรับ locationname

df = pd.read_excel(userpath / EXCEL_FILE, sheet_name=SHEET_NAME)

# สร้าง PDF ขนาด 10.4x3.075cm (104x30.75mm)
pdf = FPDF(unit="mm", format=(PAGE_WIDTH_CM, PAGE_HEIGHT_CM))
num_labels = len(df)

for idx in range(0, num_labels, 2):  # หน้าละ 2 label
    pdf.add_page()
    for i in range(2):
        label_idx = idx + i
        if label_idx >= num_labels:
            break
        row = df.iloc[label_idx]

        # สร้าง label เป็นภาพ
        label_img = Image.new("RGB", (LABEL_WIDTH_PX, LABEL_HEIGHT_PX), "white")
        draw = ImageDraw.Draw(label_img)
        # วาด QR code
        qr_img = qrcode.make(str(row['location']))
        qr_img = qr_img.resize((QR_SIZE_PX, QR_SIZE_PX), Image.LANCZOS)
        # วาง QR ชิดที่มุมซ้ายบน
        qr_x = IN_LABEL_MARGIN_PX
        qr_y = IN_LABEL_MARGIN_PX
        label_img.paste(qr_img, (qr_x, qr_y))

        # ฟอนต์สำหรับ location และ locationname
        try:
            location_font = ImageFont.truetype(FONT_PATH, LOCATION_FONT_SIZE)
            locationname_font = ImageFont.truetype(FONT_PATH, LOCATIONNAME_FONT_SIZE)
        except:
            location_font = ImageFont.load_default()
            locationname_font = ImageFont.load_default()

        # ข้อความ
        location_text = str(row['location'])
        locationname_text = str(row['locationname'])

        # ขนาดข้อความด้วย textbbox
        loc_bbox = draw.textbbox((0, 0), location_text, font=location_font)
        loc_w = loc_bbox[2] - loc_bbox[0]
        loc_h = loc_bbox[3] - loc_bbox[1]

        locname_bbox = draw.textbbox((0, 0), locationname_text, font=locationname_font)
        locname_w = locname_bbox[2] - locname_bbox[0]
        locname_h = locname_bbox[3] - locname_bbox[1]

        # ตำแหน่งข้อความ ให้อยู่ด้านล่างของ QR code
        y_text = qr_y + QR_SIZE_PX + int(0.1 * DPI)   # 0.1cm ห่างใต้ QR
        x_loc = (LABEL_WIDTH_PX - loc_w) // 2
        draw.text((x_loc, y_text), location_text, font=location_font, fill="black")

        y_locname = y_text + loc_h
        x_locname = (LABEL_WIDTH_PX - locname_w) // 2
        draw.text((x_locname, y_locname), locationname_text, font=locationname_font, fill="black")

        # วาดกรอบ label
        draw.rectangle(
            [(0, 0), (LABEL_WIDTH_PX - 1, LABEL_HEIGHT_PX - 1)],
            outline="black",
            width=3
        )

        # วาดกรอบ label สำหรับใส่จำนวนสินค้า (ถ้ามี)
        draw.rectangle(
            [(),
             (LABEL_WIDTH_PX - IN_LABEL_MARGIN_PX - 20, LABEL_HEIGHT_PX - IN_LABEL_MARGIN_PX - 20)],
            outline="black",
            width=3
        )
        # Export เป็นไฟล์ภาพชั่วคราว
        temp_file = os.path.join(userpath, f'label_temp_{label_idx}.png')
        label_img.save(temp_file)

        # ตำแหน่ง label ซ้าย = 0.2 cm, label ขวา = 5.2 cm
        x_pos = LEFT_MARGIN_CM + i * LABEL_WIDTH_CM
        pdf.image(temp_file, x=x_pos, y=0.0375, w=LABEL_WIDTH_CM, h=LABEL_HEIGHT_CM)

        os.remove(temp_file)

# Save PDF
output_file = userpath / "continuous_location_labels.pdf"
pdf.output(str(output_file))
print('PDF saved:', output_file)