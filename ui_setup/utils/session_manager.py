### auth/session_manager.py
import streamlit as st

def get_session():
    if 'session' not in st.session_state:
        st.session_state.session = {
            "logged_in": False,
            "username": None,
            "role": None,
            "remember": False
        }
    return st.session_state.session

def login_user(username, role):
    st.session_state.session.update({
        "logged_in": True,
        "username": username,
        "role": role
    })

def logout_user():
    st.session_state.session.update({
        "logged_in": False,
        "username": None,
        "role": None
    })