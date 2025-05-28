### auth/register.py
import streamlit as st
from database.connect_supabase import SupabaseFunctions
import bcrypt

supa_func = SupabaseFunctions()

def register_user():
    st.subheader("Đăng ký người dùng mới")

    username = st.text_input("Tên đăng nhập mới")
    password = st.text_input("Mật khẩu", type="password")
    role = st.selectbox("Vai trò", ["user", "admin"])

    if st.button("Thêm người dùng"):
        hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        response = supa_func.create_user(username, hashed_pw, role)
        if response.data:
            st.success("Đăng ký thêm người dùng thành công!")
        else:
            st.error("Lỗi khi đăng ký: " + str(response))