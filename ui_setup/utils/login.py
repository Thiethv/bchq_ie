import streamlit as st

# Dữ liệu user giả lập (có thể thay bằng Supabase/DB thực)
USER_DATA = {
    "admin": {"password": "admin123", "role": "admin"},
    "user": {"password": "user123", "role": "viewer"},
}

def login():
    st.sidebar.subheader("🔐 Đăng nhập hệ thống")
    username = st.sidebar.text_input("Tên đăng nhập")
    password = st.sidebar.text_input("Mật khẩu", type="password")
    login_btn = st.sidebar.button("Đăng nhập")

    if login_btn:
        user = USER_DATA.get(username)
        if user and user["password"] == password:
            st.session_state["user"] = username
            st.session_state["role"] = user["role"]
            st.success(f"Chào {username}!")
            st.rerun()
        else:
            st.error("Sai tên đăng nhập hoặc mật khẩu!")

def check_login():
    return "user" in st.session_state

def get_role():
    return st.session_state.get("role", "viewer")
