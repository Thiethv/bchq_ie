import streamlit as st
import pandas as pd
from ui_setup.utils.excel_loader import load_excel
from ui_setup.data_dmkt.cutting_forecast import CuttingForecast
from ui_setup.data_dmkt.data_master_list import MasterList
from ui_setup.data_dmkt.get_dmsm_sql import DemandSM
from ui_setup.components.dm_technical import DemandTechnical

def run():
    st.title("🧰 DM Technical")
    file = st.file_uploader("Chọn file", type=["xlsx", "xls"])
    

    col1, col2, col3 = st.columns([3,2,1])
    with col1:
        selected_data = st.radio(
            "Chọn dữ liệu:",
            ("DM_Technical","List GO", "Cutting Forecast", "Data SQL", "Master List"),
            horizontal=True,
            key="selected_data_radio"
        )


    if "last_selected" not in st.session_state:
        st.session_state.last_selected = selected_data

    if selected_data != st.session_state.last_selected:
        st.session_state.last_selected = selected_data

    if selected_data == "List GO":
        
        confirmed, check_delete = list_go(col3)

        if file is None:
            st.warning("Vui lòng chọn file!")
            return
        df = load_excel(file)

        if confirmed and not df.empty:
            if MasterList().insert_list_go(df, check_delete):
                st.success("✅ Đã đưa dữ liệu List GO lên thành công")
        
        st.dataframe(df, use_container_width=True)

    elif selected_data == "Cutting Forecast":
        confirmed, check_delete = cutting_forecast(col3)
        if not confirmed:
            st.warning("Đảm bảo đã thêm list GO cần kiểm tra trước khi lấy cutting forecast!")
        if confirmed:
            if CuttingForecast(check_delete):
                st.success("✅ Đã lấy dữ liệu Cutting Forecast thành công")

    elif selected_data == "Master List":
        
        selected_box, confirmed, check_delete = insert_update_list(col2, col3)
        if file is None:
            st.warning("Vui lòng chọn file!")
            return
        df = load_excel(file)

        if selected_box == "Trims list":
            data = df.iloc[:, [1, 3, 7]]
            data.columns = ["THV_CODE", "CODE_CUSTOMS", "CONVERT"]
        elif selected_box == "Fabric list":
            data = df.iloc[:,[1, 2, 4, 5]]
            data.columns = ["PO_Item", "PO_NO", "Width", "CODE_CUSTOMS"]
        elif selected_box == "Range demand":
            data = df
        
        if data.empty:
            st.warning("Vui lòng chọn đúng file!")
            return
        data = data.dropna()
        data = data.drop_duplicates()
        
        if confirmed:
            
            if selected_box == "Trims list":
                if MasterList().insert_list_trims_to_supabase(data, check_delete):
                    st.success("✅ Đã đưa dữ liệu Strims list lên thành công")
                else:
                    st.error("❌ Lỗi khi đưa dữ liệu trims!")
            elif selected_box == "Fabric list":
                if MasterList().insert_list_fabric_to_supabase(data, check_delete):
                    st.success("✅ Đã đầu dữ liệu Fabric list lên thành công")
                else:
                    st.error("❌ Lỗi khi đưa dữ liệu Fabric!")
            elif selected_box == "Range demand":
                if MasterList().insert_range_demand_to_supabase(data, check_delete):
                    st.success("✅ Đã đầu dữ liệu Range demand lên thành công")
                else:
                    st.error("❌ Lỗi khi đưa dữ liệu Range demand!")
                    
        st.dataframe(data, use_container_width=True)

    elif selected_data == "Data SQL":
        confirmed, from_date, selected_box =data_sql(col2, col3)
        if not confirmed: 
            st.warning("Lấy dữ liệu từ tháng/năm và kiểm tra List GO!")
        if confirmed:
            if selected_box == "Go Quantity":
                year = from_date.year
                DemandSM().get_go_quantity(year)

            elif selected_box == "Submat Demand":
                year = from_date.year
                from_month = from_date.month
                DemandSM().get_data_demand(year, from_month)

    elif selected_data == "DM_Technical":
        confrimed = dm_technical(col2)
        if not confrimed:
            st.warning("Kiểm tra đầy đủ dữ liệu trước khi tính toán DM Technical!")

        if confrimed:
            if DemandTechnical().get_results_dm_technical():
                st.success("✅ Đã tính toán DM Technical")
            else:
                st.error("❌ Lỗi khi tính toán DM Technical")


def dm_technical(col2):
    with col2:
        st.write("")
        confirm = st.button("Get DM_Technical", key="get_clicked")

    return confirm

def list_go(col3):

    with col3:
        delete_all = st.checkbox("Xóa tất cả dữ liệu", value=True)
        confirm = st.button("Upload", key="upload_clicked")

    return confirm, delete_all

def cutting_forecast(col3):
    with col3:
        delete_all = st.checkbox("Xóa tất cả dữ liệu", value=False)
        confirm = st.button("Upload", key="upload_clicked")

    return confirm, delete_all

def insert_update_list(col2, col3):
    with col2:
        selected_box = st.selectbox(
            "List:", 
            ["Trims list", "Fabric list", "Range demand"], 
            key="insert_update_list"
        )
    with col3:
        delete_all = st.checkbox("Xóa tất cả dữ liệu", value=False)
        confirm = st.button("Upload", key="confirm_insert_update_list")

    # Trả về cả lựa chọn và trạng thái nút xác nhận
    return selected_box, confirm, delete_all

def data_sql(col2, col3):
    with col2:
        c1, c2 = st.columns([1,2])
        with c1:
            from_date = st.date_input("From Date:", key="frome_date", format="YYYY-MM-DD")
        with c2:
            selected_box = st.selectbox(
                    "Data input:", 
                    ["Go Quantity", "Submat Demand"], 
                    key="insert_update_demand"
                )
    with col3:
        
        confirm = st.button("Upload", key="upload_clicked")

    return confirm, from_date, selected_box