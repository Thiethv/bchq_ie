import streamlit as st
from streamlit_option_menu import option_menu
from ui_setup.main_page import MainPage
from ui_setup.utils.login import login
from ui_setup.utils.session_manager import get_session

# Cấu hình
st.set_page_config(page_title="IE Assistant", layout="wide")

session = get_session()

# Kiểm tra đăng nhập
if session.get("logged_in"):
    MainPage(session)

else:
    login()
    st.stop()
