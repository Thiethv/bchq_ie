import streamlit as st
import bcrypt

from database.connect_supabase import SupabaseFunctions
from ui_setup.utils.session_manager import *

supa_func = SupabaseFunctions()


def login():
    st.sidebar.subheader("🔐 Đăng nhập hệ thống")
    remembered_username = st.session_state.get("remembered_username", "")

    username = st.sidebar.text_input("Tên đăng nhập", value=remembered_username)
    password = st.sidebar.text_input("Nhập Mật khẩu", type="password")
    remember = st.sidebar.checkbox("Lưu tên đăng nhập", value=bool(remembered_username))
    col1, col2, col3 = st.columns([1,1,1])
    with col2:
        login_btn = st.sidebar.button("Login", use_container_width=True)

    if login_btn:
        response = supa_func.get_user_by_username(username)
   
        if response.data:
            user = response.data[0]
            if bcrypt.checkpw(password.encode(), user['password'].encode()):
                # Lưu hoặc xóa tên đăng nhập tuỳ chọn
                if remember:
                    st.session_state["remembered_username"] = username
                else:
                    st.session_state.pop("remembered_username", None)

                # Cập nhật trạng thái đăng nhập
                login_user(username, user['role'])
                st.success("Đăng nhập thành công!")
                st.rerun()
            else:
                st.error("Sai mật khẩu!")
        else:
            st.error("Người dùng không tồn tại!")
