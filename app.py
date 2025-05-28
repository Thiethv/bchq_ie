import streamlit as st
from streamlit_option_menu import option_menu
from ui_setup.utils.login import login, check_login, get_role

# Cấu hình
st.set_page_config(page_title="IE Assistant", layout="wide")

# Kiểm tra đăng nhập
if not check_login():
    login()
    st.stop()

# Sidebar menu chính sau khi đăng nhập
with st.sidebar:
    st.markdown(f"👤 **Người dùng:** `{st.session_state['user']}`")
    selected = option_menu(
        "📊 MENU", ["DM Technical", "DM Actual", "Report", "Function"],
        icons=["tools", "box", "bar-chart", "gear"], default_index=0
    )

# Điều hướng trang
if selected == "DM Technical":
    import ui_setup.pages.dm_technical_page as dm
    dm.run()

elif selected == "DM Actual":
    import ui_setup.pages.dm_actual_page as actual
    actual.run()

elif selected == "Report":
    import ui_setup.pages.report_page as rpt
    rpt.run()

elif selected == "Function":
    import ui_setup.pages.function as fn
    fn.run()
