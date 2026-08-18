import streamlit as st
import dotenv
import os

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