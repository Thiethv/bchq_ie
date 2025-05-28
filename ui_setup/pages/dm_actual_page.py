import streamlit as st
import pandas as pd
from ui_setup.data_dmtt.fabric_trans import FabricTrans
from ui_setup.data_dmtt.jo_process_wip import JoProcessWip
from ui_setup.data_dmtt.submat_trans import SubmatTrans

from ui_setup.components.dm_actual import DmActual

def run():
    st.title("📦 DM Actual")
    col1, col2, col3 = st.columns([4,1,1])
    with col1:
        selected_data = st.radio(
            "Chọn dữ liệu:",
            ("DM Actual", "Fabric Trans","Process wip", "Submat Trans"),
            horizontal=True,
            key="selected_data_radio"
        )

    with col2:
        
        from_date = st.date_input("From Date:", key="from_date", format="YYYY-MM-DD")

    with col3:
        st.write("")
        st.write("")
        st.button("Get Data", key="get_clicked")

    from_date = st.session_state.get("from_date")
    if st.session_state.get("get_clicked"):
        if selected_data == "Fabric Trans":
            if FabricTrans(from_date).process_data():
                st.success("✅ Thêm dữ liệu Fabric Trans thành công")

        elif selected_data == "Submat Trans":
            if SubmatTrans(from_date):
                st.success("✅ Thêm dữ liệu Submat Trans thành công")

        elif selected_data == "Process wip":
            if JoProcessWip().process_wip():
                st.success("✅ Thêm dữ liệu Process wip thành công")

        elif selected_data == "DM Actual":
            if DmActual().update_note_actual():
                st.success("✅ Thêm dữ liệu DM Actual thành công")

    else:
        if selected_data == "Fabric Trans":
            st.warning("Chú ý chọn dữ liệu từ ngày, tháng, năm của SQL")

        elif selected_data == "Submat Trans":
            st.warning("Chú ý chọn dữ liệu từ ngày, tháng, năm của SQL")

        elif selected_data == "Process wip":
            st.warning("Lấy dữ liệu JO Process wip từ SQL")

        elif selected_data == "DM Actual":
            st.warning("Lấy dữu liệu khi đã đày đủ dữ liệu Fabric Trans, Submat Trans, Process wip")