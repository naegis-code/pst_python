from datetime import date
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Upload Data", layout="wide")
st.title("Upload your data")

API_URL = "http://localhost:8502/api"

# ---------------------------------------------------------
# 1. ส่วนเลือก วันที่, BU และ สาขา
# ---------------------------------------------------------
cntdate = st.date_input("Select Count Date", format="YYYY-MM-DD")
cntdate_str = cntdate.strftime("%Y-%m-%d") if cntdate else ""

# ดึงข้อมูลสาขาจาก FastAPI
response_stores = requests.get(
    f"{API_URL}/stores", params={"cntdate": cntdate_str}
)
df_plan = (
    pd.DataFrame(response_stores.json())
    if response_stores.status_code == 200
    else pd.DataFrame()
)

if df_plan.empty:
    st.warning(
        f"⚠️ ไม่มีข้อมูลสาขาในวันที่ {cntdate}. กรุณาตรวจสอบวันที่ที่เลือก"
    )
    st.stop()

bu = st.selectbox("Select BU", options=df_plan["bu"].unique(), index=None)

if not bu:
    st.stop()

df_bu = df_plan[df_plan["bu"] == bu]

stcode = st.selectbox(
    "Select Store Code",
    options=df_bu["stcode"].unique(),
    format_func=lambda x: f"{x} - {df_bu.loc[df_bu['stcode'] == x, 'acronym'].values[0]} - {df_bu.loc[df_bu['stcode'] == x, 'branch'].values[0]}",
    index=None,
)

if not stcode:
    st.stop()

# ---------------------------------------------------------
# 2. กำหนด Target Reports แยกตาม BU
# ---------------------------------------------------------
if bu == "CHG":
    table_stk = f"{bu.lower()}_stk_this_year"
    c_stk = "cntqnt"
    table_var = f"{bu.lower()}_var_this_year"
    c_var = "cntqnt"
    target_reports = [
        ("STK1", "Credit"),
        ("STK1", "Consign"),
        ("STK2", "Credit"),
        ("STK2", "Consign"),
        ("VAR1", "Credit"),
        ("VAR1", "Consign"),
        ("VAR2", "Credit"),
        ("VAR2", "Consign"),
    ]
elif bu == "PWB":
    table_stk = f"{bu.lower()}_stk_this_year"
    c_stk = "cntqnt"
    table_var = f"{bu.lower()}_var_this_year"
    c_var = "cntqnt"
    target_reports = [
        ("CNTFILE", "ALL"),
        ("STK1", "Credit"),
        ("STK1", "Consign"),
        ("STK2", "Credit"),
        ("STK2", "Consign"),
        ("VAR1", "Credit"),
        ("VAR1", "Consign"),
        ("VAR2", "Credit"),
        ("VAR2", "Consign"),
    ]
elif bu == "B2S":
    table_stk = f"{bu.lower()}_stk_this_year"
    c_stk = "cntqnt"
    table_var = f"{bu.lower()}_var_this_year"
    c_var = "cntqnt"
    target_reports = [
        ("SALE", "ALL"),
        ("STK1", "Credit"),
        ("STK1", "Consign"),
        ("STK2", "Credit"),
        ("STK2", "Consign"),
        ("VAR1", "Credit"),
        ("VAR1", "Consign"),
        ("VAR2", "Credit"),
        ("VAR2", "Consign"),
    ]

total_expected = len(target_reports)

# ดึงข้อมูลรายงานสะสมล่าสุดจาก API
response = requests.get(
    f"{API_URL}/report",
    params={"bu": bu, "stcode": stcode, "cntdate": cntdate_str},
)
df_report = (
    pd.DataFrame(response.json())
    if response.status_code == 200
    else pd.DataFrame()
)

if df_report.empty:
    df_report = pd.DataFrame(columns=["rpname", "skutype", "net"])


# ---------------------------------------------------------
# 3. Pop-up Dialog สำหรับอัปโหลด/แก้ไขไฟล์
# ---------------------------------------------------------
@st.dialog("จัดการข้อมูลรายงาน")
def upload_dialog(bu, stcode, cntdate, rpname, skutype, has_data=False):
    # 🎯 ดึงชื่อผู้ใช้งานจาก session_state
    current_user = st.session_state.get("username", "Guest")
    
    action_title = "แก้ไขข้อมูล" if has_data else "อัปโหลดข้อมูลใหม่"
    st.write(f"📌 **รายงาน:** `{rpname}-{skutype}` ({action_title})")
    st.write(f"🏢 **BU:** {bu} | **Store:** {stcode} | **Date:** {cntdate}")
    st.write(f"👤 **ผู้ดำเนินการ:** {current_user}")

    if has_data:
        st.info("ℹ️ การอัปโหลดไฟล์ใหม่จะทำการแทนที่ (Replace) ข้อมูลเดิม")

    uploaded_file = st.file_uploader(
        "เลือกไฟล์ Excel (.xlsx, .xls)",
        type=["xlsx", "xls"],
        key=f"uploader_{rpname}_{skutype}",
    )

    if uploaded_file is not None:
        btn_label = "บันทึกแก้ไขข้อมูล" if has_data else "ยืนยันนำเข้าข้อมูล"
        if st.button(btn_label, type="primary"):
            with st.spinner("กำลังประมวลผลข้อมูล..."):
                payload = {
                    "bu": bu,
                    "stcode": stcode,
                    "cntdate": str(cntdate),
                    "rpname": rpname,
                    "skutype": skutype,
                    "username": current_user,  # 👈 แนบ username ไปยัง FastAPI Form Data
                }
                files = {
                    "file": (
                        uploaded_file.name,
                        uploaded_file.getvalue(),
                        uploaded_file.type,
                    )
                }

                res_upload = requests.post(
                    f"{API_URL}/upload-report", data=payload, files=files
                )

                if res_upload.status_code == 200:
                    try:
                        result = res_upload.json()
                        st.success(f"🎉 {result.get('message', 'อัปโหลดสำเร็จ')}")
                        st.rerun()
                    except Exception:
                        st.success("🎉 อัปโหลดสำเร็จเรียบร้อยแล้ว")
                        st.rerun()
                else:
                    # ป้องกัน JSONDecodeError กรณี Server ตอบกลับมาเป็น HTML หรือ Text เปล่า
                    try:
                        err_detail = res_upload.json().get("detail", "เกิดข้อผิดพลาดไม่ทราบสาเหตุ")
                    except Exception:
                        err_detail = res_upload.text if res_upload.text else f"เกิดข้อผิดพลาดจาก Server (Status Code: {res_upload.status_code})"
                    
                    st.error(f"❌ {err_detail}")


# ---------------------------------------------------------
# 4. ตรวจสอบจำนวนรายการที่พบจริง
# ---------------------------------------------------------
total_found = 0
if not df_report.empty and "net" in df_report.columns:
    for rp, sku in target_reports:
        matched = df_report[
            (df_report["rpname"] == rp) & (df_report["skutype"] == sku)
        ]
        if not matched.empty and pd.notna(matched["net"].iloc[0]):
            total_found += 1

if total_found == total_expected:
    status_label = f"ตรวจสอบสำเร็จ: ครบถ้วน ({total_found}/{total_expected})"
    status_state = "complete"
else:
    status_label = (
        f"เตือน: ข้อมูลไม่ครบถ้วน (พบ {total_found}/{total_expected} รายการ)"
    )
    status_state = "error"

# ---------------------------------------------------------
# 5. แสดงผล UI แยกเป็น Tabs (ALL, Credit, Consign)
# ---------------------------------------------------------
with st.status(status_label, state=status_state, expanded=True):

    # แยกกลุ่ม target_reports ตามประเภท skutype
    reports_all = [(rp, sku) for rp, sku in target_reports if sku == "ALL"]
    reports_credit = [
        (rp, sku) for rp, sku in target_reports if sku == "Credit"
    ]
    reports_consign = [
        (rp, sku) for rp, sku in target_reports if sku == "Consign"
    ]

    # ฟังก์ชัน Helper สำหรับวาด UI การ์ดและปุ่มกดภายใน Tab
    def render_report_items(items_list, df_rep):
        col1, col2 = st.columns(2)
        for i, (rp, sku) in enumerate(items_list):
            matched = df_rep[
                (df_rep["rpname"] == rp) & (df_rep["skutype"] == sku)
            ]
            target_col = col1 if i % 2 == 0 else col2

            with target_col:
                key_name = f"{rp}_{sku}"
                has_data = not matched.empty and pd.notna(
                    matched["net"].iloc[0]
                )

                c1, c2 = st.columns([0.65, 0.35])
                with c1:
                    if has_data:
                        val = float(matched["net"].iloc[0])
                        st.write(f"🟢 **{rp}-{sku}:** `{val:,.2f}`")
                    else:
                        st.write(f"🔴 **{rp}-{sku}:** *ไม่มีข้อมูล*")

                with c2:
                    btn_text = "✏️ แก้ไข" if has_data else "📤 อัปโหลด"
                    if st.button(btn_text, key=f"btn_{key_name}"):
                        upload_dialog(
                            bu, stcode, cntdate_str, rp, sku, has_data=has_data
                        )

    # สร้างรายการ Tab Dynamic ตามที่มีจริงใน BU นั้นๆ
    tab_titles = []
    if reports_all:
        tab_titles.append("📦 ALL")
    if reports_credit:
        tab_titles.append("💳 Credit")
    if reports_consign:
        tab_titles.append("🏬 Consign")

    tabs = st.tabs(tab_titles)

    tab_idx = 0
    if reports_all:
        with tabs[tab_idx]:
            st.caption("รายการรายงานทั่วไป")
            render_report_items(reports_all, df_report)
        tab_idx += 1

    if reports_credit:
        with tabs[tab_idx]:
            st.caption("รายการรายงานประเภท Credit")
            render_report_items(reports_credit, df_report)
        tab_idx += 1

    if reports_consign:
        with tabs[tab_idx]:
            st.caption("รายการรายงานประเภท Consign (ฝากขาย)")
            render_report_items(reports_consign, df_report)