import os
import dotenv
import streamlit as st

dotenv.load_dotenv(dotenv.find_dotenv())

# ใช้ @st.cache_resource ป้องกันไม่ให้สร้าง connection ใหม่ทุกครั้งที่กดปุ่ม
@st.cache_resource
def get_engines():
    engine1 = f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb')}"
    engine2 = f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb2')}"
    engine3 = f"{os.getenv('DB_CONN')}{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_pstdb3')}"
    
    return engine1, engine2, engine3