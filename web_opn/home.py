import streamlit as st
import requests

# -------------------------------------------------------------
# 1. ฟังก์ชั่นยิง API ตรวจสอบการ Login กับ FastAPI
# -------------------------------------------------------------
FASTAPI_LOGIN_URL = "http://localhost:8502/api/login"  # ปรับ URL ให้ตรงกับ FastAPI ของคุณ

def login_page():
    st.title("🔒 เข้าสู่ระบบ (Login)")
    
    with st.form("login_form"):
        username = st.text_input("ชื่อผู้ใช้งาน (Username)")
        password = st.text_input("รหัสผ่าน (Password)", type="password")
        submit_button = st.form_submit_button("เข้าสู่ระบบ")
        
        if submit_button:
            if not username or not password:
                st.warning("กรุณากรอก Username และ Password ให้ครบถ้วน")
                return

            try:
                # ยิง POST Request ไปยัง FastAPI
                response = requests.post(
                    FASTAPI_LOGIN_URL,
                    json={"username": username, "password": password},
                    timeout=5
                )
                
                if response.status_code == 200:
                    data = response.json()
                    # บันทึก Session การเข้าระบบ
                    st.session_state["authenticated"] = True
                    st.session_state["username"] = username
                    st.session_state["token"] = data.get("access_token")
                    st.success("เข้าสู่ระบบสำเร็จ!")
                    st.rerun()  # รีโหลดเพื่อเข้าหน้าแอปหลัก
                elif response.status_code == 401:
                    st.error("ชื่อผู้ใช้งานหรือรหัสผ่านไม่ถูกต้อง")
                else:
                    st.error(f"เกิดข้อผิดพลาดจากเซิร์ฟเวอร์ ({response.status_code})")

            except requests.exceptions.RequestException as e:
                st.error(f"ไม่สามารถเชื่อมต่อกับ API Server ได้: {e}")

def logout():
    st.session_state["authenticated"] = False
    st.session_state["username"] = None
    st.session_state["token"] = None
    st.rerun()

# -------------------------------------------------------------
# 2. ตรวจสอบ สถานะการเข้าระบบ
# -------------------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

# -------------------------------------------------------------
# 3. ควบคุมการ Navigation ตามสถานะ Login
# -------------------------------------------------------------
if not st.session_state["authenticated"]:
    # ถ้ายังไม่ได้ Login ให้แสดงเฉพาะหน้า Login เท่านั้น
    login_page()
else:
    # เพิ่มปุ่ม Logout และบอกผู้ใช้งานตรง Sidebar
    st.sidebar.write(f"👤 ผู้ใช้งาน: **{st.session_state['username']}**")
    if st.sidebar.button("🚪 ออกจากระบบ (Logout)"):
        logout()
    st.sidebar.divider()

    # แสดงโครงสร้าง เมนู Multi-page Navigation เดิม
    pages = {
        "Your account": [
            st.Page("profile.py", title="Profile your account"),
        ],
        "BU": [
            st.Page("chg.py", title="CHG"),
        ],
        "UPLOAD": [
            st.Page("upload.py", title="Upload your data"),
        ],
    }

    pg = st.navigation(pages)
    pg.run()