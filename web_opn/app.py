import streamlit as st
import requests
import urllib.parse

# ==========================================
# 1. CONFIG & CONSTANTS
# ==========================================
LINE_CLIENT_ID = "YOUR_LINE_CLIENT_ID"        # ใส่ Channel ID จาก LINE Console
LINE_CLIENT_SECRET = "YOUR_LINE_CLIENT_SECRET" # ใส่ Channel Secret จาก LINE Console
REDIRECT_URI = "http://localhost:8501/"         # Callback URL ที่ลงทะเบียนไว้

# ฐานข้อมูล Mock สำหรับ Username / Password ปกติ
DEMO_USERS = {
    "admin": "1234",
    "user1": "password"
}

st.set_page_config(page_title="Streamlit Login Demo", page_icon="🔐", layout="centered")

# ==========================================
# 2. HELPER FUNCTIONS FOR LINE OAUTH
# ==========================================
def get_line_auth_url():
    """สร้าง URL สำหรับ Redirect ไปหน้า Login ของ LINE"""
    params = {
        "response_type": "code",
        "client_id": LINE_CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "state": "random_csrf_token_12345", # ในระบบจริงควรสร้าง token แบบสุ่มเพื่อป้องกัน CSRF
        "scope": "profile openid email",
    }
    return f"https://access.line.me/oauth2/v2.1/authorize?{urllib.parse.urlencode(params)}"

def get_line_user_profile(code):
    """นำ Code ที่ได้จาก LINE ไปแลก Access Token และดึง User Profile"""
    token_url = "https://api.line.me/oauth2/v2.1/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": LINE_CLIENT_ID,
        "client_secret": LINE_CLIENT_SECRET,
    }
    
    # 1. แลกเปลี่ยน Code เป็น Access Token
    token_response = requests.post(token_url, headers=headers, data=data)
    if token_response.status_code != 200:
        return None
    
    token_json = token_response.json()
    access_token = token_json.get("access_token")

    # 2. ใช้ Access Token ดึง Profile
    profile_url = "https://api.line.me/v2/profile"
    profile_headers = {"Authorization": f"Bearer {access_token}"}
    profile_response = requests.get(profile_url, headers=profile_headers)
    
    if profile_response.status_code == 200:
        return profile_response.json()
    return None

def logout():
    """ออกจากระบบ ล้าง Session State และ Query Params"""
    st.session_state.logged_in = False
    st.session_state.user_info = None
    st.query_params.clear()
    st.rerun()

# ==========================================
# 3. SESSION STATE INITIALIZATION
# ==========================================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "user_info" not in st.session_state:
    st.session_state.user_info = None

# ==========================================
# 4. HANDLE LINE CALLBACK (QUERY PARAMS)
# ==========================================
# ตรวจสอบว่ามี Query Param 'code' ที่ LINE ส่งกลับมาหรือไม่
query_params = st.query_params
if "code" in query_params and not st.session_state.logged_in:
    auth_code = query_params["code"]
    with st.spinner("กำลังเข้าสู่ระบบด้วย LINE..."):
        line_profile = get_line_user_profile(auth_code)
        if line_profile:
            st.session_state.logged_in = True
            st.session_state.user_info = {
                "name": line_profile.get("displayName"),
                "picture": line_profile.get("pictureUrl"),
                "user_id": line_profile.get("userId"),
                "provider": "LINE"
            }
            # ล้าง URL query params หลังรับ code เรียบร้อย
            st.query_params.clear()
            st.rerun()
        else:
            st.error("เกิดข้อผิดพลาดในการเข้าสู่ระบบผ่าน LINE")

# ==========================================
# 5. UI DISPLAY LOGIC
# ==========================================
# หน้าจอหลังจาก Login สำเร็จแล้ว
if st.session_state.logged_in:
    user = st.session_state.user_info
    st.success(f"ยินดีต้อนรับคุณ {user['name']}!")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if user.get("picture"):
            st.image(user["picture"], width=100)
    with col2:
        st.write(f"**ชื่อผู้ใช้งาน:** {user['name']}")
        st.write(f"**ช่องทาง เข้าสู่ระบบ:** {user['provider']}")
        if "user_id" in user:
            st.caption(f"User ID: {user['user_id']}")

    st.divider()
    st.markdown("### 📊 หน้าหลักของแอปพลิเคชัน (Protected Area)")
    st.write("นี่คือเนื้อหาสำหรับผู้ที่เข้าสู่ระบบแล้วเท่านั้น")
    
    if st.button("🚪 ออกจากระบบ (Logout)", type="secondary"):
        logout()

# หน้าจอสำหรับ Login
else:
    st.title("🔐 เข้าสู่ระบบ")
    
    # --------------------------------------
    # Form 1: Standard Username/Password
    # --------------------------------------
    with st.form("login_form"):
        st.subheader("เข้าสู่ระบบด้วยบัญชีทั่วไป")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit_btn = st.form_submit_button("Log In", use_container_width=True)
        
        if submit_btn:
            if username in DEMO_USERS and DEMO_USERS[username] == password:
                st.session_state.logged_in = True
                st.session_state.user_info = {
                    "name": username,
                    "provider": "Standard (User/Pass)"
                }
                st.success("เข้าสู่ระบบสำเร็จ!")
                st.rerun()
            else:
                st.error("Username หรือ Password ไม่ถูกต้อง")

    st.markdown("<h4 style='text-align: center; color: gray;'>หรือ</h4>", unsafe_allow_html=True)

    # --------------------------------------
    # Form 2: LINE Login Button
    # --------------------------------------
    line_url = get_line_auth_url()
    
    # สร้างปุ่มแต่งลิงก์ให้ดูเหมือนปุ่ม LINE Login
    st.markdown(
        f"""
        <a href="{line_url}" target="_self" style="text-decoration: none;">
            <div style="
                background-color: #06C755;
                color: white;
                text-align: center;
                padding: 10px;
                border-radius: 8px;
                font-weight: bold;
                font-size: 16px;
                cursor: pointer;
                box-shadow: 0px 2px 4px rgba(0,0,0,0.1);
            ">
                💬 เข้าสู่ระบบด้วย LINE
            </div>
        </a>
        """,
        unsafe_allow_html=True
    )