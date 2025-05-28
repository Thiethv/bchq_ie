import streamlit as st
from streamlit_option_menu import option_menu

from ui_setup.utils.register import register_user
from ui_setup.utils.session_manager import *


class MainPage:
    def __init__(self, session):
        super().__init__()
        self.session = session
        self.show_technical_page()

    def show_technical_page(self):
        # Kiểm tra xem đang ở chế độ thêm người dùng không
        if st.session_state.get("mode") == "register":
            register_user()  # hiển thị form đăng ký
            if st.button("⬅ Quay lại"):
                st.session_state["mode"] = "main"
                st.rerun()
            return  # dừng hiển thị giao diện chính
        
        # Corrected code to move col1, col2, col3 into the sidebar
        if self.session['role'] == 'admin':
            with st.sidebar:
                st.markdown(f"👤 **Người dùng:** `{self.session.get('username')}`")
                selected = option_menu(
                    "📊 MENU", ["DM Technical", "DM Actual", "Report", "Function"],
                    icons=["tools", "box", "bar-chart", "gear"], default_index=0
                )
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Add user", use_container_width=True):
                        st.session_state["mode"] = "register"
                        st.rerun()
                with col2:
                    if st.button("Logout", use_container_width=True):
                        logout_user()
                        st.rerun()

            self.run_page(selected)
            
        else:
            with st.sidebar:
                st.markdown(f"👤 **Người dùng:** `{self.session.get('username')}`")
                selected = option_menu(
                    "📊 MENU", ["DM Technical", "DM Actual", "Report", "Function"],
                    icons=["tools", "box", "bar-chart", "gear"], default_index=0
                )
                
                col1, col2, col3 = st.columns([1,2,1])
                with col2:
                    if st.button("Logout", use_container_width=True):
                        logout_user()
                        st.rerun()

            self.run_page(selected)
    
    def run_page(self, selected):
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