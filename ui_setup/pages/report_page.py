import streamlit as st
import pandas as pd
from ui_setup.utils.excel_loader import load_excel
from io import BytesIO
from database.connect_supabase import SupabaseFunctions

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def run():
    st.title("📊 Báo cáo")
    data = pd.DataFrame()
    col1, col2, col3, col4 = st.columns([3,3,1,2])
    with col1:
        selected_data = st.selectbox(
            "Module:",
            ("DM Technical", "DM Actual", "Compare DM"),
            key="selected_data_report"
        )
    
    with col2:
        if selected_data == "DM Technical":
            sub_selected_data = st.selectbox(
                "Function:",
                ("","Report Technical","List GO", "Cutting Forecast", "Go Quantity", "Submat Demand", "Master Fabric List", "Master Trims List"),
                key="sub_selected_data_technical"
            )
        elif selected_data == "DM Actual":
            sub_selected_data = st.selectbox(
                "Function:",
                ("","Report Actual","Fabric Trans Summary","JO Process Wip", "Submat Trans Summary"),
                key="sub_selected_data_actual"
            )
        elif selected_data == "Compare DM":
            sub_selected_data = st.selectbox(
                "Function:",
                ("","Report Compare"),
                key="sub_selected_data_compare_dm"
            )
    with col3:
        st.write("")
        st.write("")
        st.checkbox("List GO", value=False, key="list_go_check")

    with col4:
        c1, c2 = st.columns(2)
        with c1:
            st.write("")
            st.write("")
            confrim = st.button("Query", key="get_report")
            if confrim:
                data = query_data(selected_data, sub_selected_data)
                if not data.empty:

                    st.session_state['report_data'] = data  # Lưu vào session_state    

        with c2:
            st.write("")
            st.write("")
            data = st.session_state.get('report_data', pd.DataFrame())  # Lấy lại từ session_state
            if not data.empty:
                excel_data = to_excel(data)
                st.download_button(
                    label="Tải file Excel",
                    data=excel_data,
                    file_name=f"{sub_selected_data}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
    if not data.empty:

        st.success("Truy vấn dữ liệu thành công!") 
        st.dataframe(data.reset_index(drop=True), use_container_width=True)

    else:
        st.warning("Chưa truy vấn dữ liệu!")

def query_data(selected_data, sub_selected_data):
    sc_nos_str = ''
    if st.session_state.get("list_go_check"):
        sc_nos_str = list_go_check()

    if selected_data == "DM Technical":
        if sub_selected_data == "Report Technical":
            data = SupabaseFunctions().get_data("dm_technical", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("dm_technical", "*")

        elif sub_selected_data == "List GO":
            data = SupabaseFunctions().get_data("list_go", "*")

        elif sub_selected_data == "Cutting Forecast":
            data = SupabaseFunctions().get_data("cutting_forecast", "*", f' "GO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("cutting_forecast", "*")

        elif sub_selected_data == "Go Quantity":
            data = SupabaseFunctions().get_data("go_quantity", "*", f' "GO_No" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("go_quantity", "*")

        elif sub_selected_data == "Submat Demand":
            data = SupabaseFunctions().get_data("submat_demand", "*", f' "GO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("submat_demand", "*")

        elif sub_selected_data == "Master Fabric List":
            data = SupabaseFunctions().get_data("fabric_list", "*")

        elif sub_selected_data == "Master Trims List":
            data = SupabaseFunctions().get_data("trims_list", "*")
    
    elif selected_data == "DM Actual":
        if sub_selected_data == "Report Actual":
            data = SupabaseFunctions().get_data("dm_actual", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("dm_actual", "*")

        elif sub_selected_data == "Fabric Trans Summary":
            data = SupabaseFunctions().get_data("fabric_trans", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("fabric_trans", "*")

        elif sub_selected_data == "JO Process Wip":
            data = SupabaseFunctions().get_data("process_wip", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("process_wip", "*")

        elif sub_selected_data == "Submat Trans Summary":
            data = SupabaseFunctions().get_data("submat_trans", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("submat_trans", "*")
            
    elif selected_data == "Compare DM":
        if sub_selected_data == "Report Compare":
            data_techncial = SupabaseFunctions().get_data("dm_technical", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("dm_technical", "*")
            
            data_actual = SupabaseFunctions().get_data("dm_actual", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                else SupabaseFunctions().get_data("dm_actual", "*")
            
            data = process_data_compare(data_techncial, data_actual)
    
    data = data.drop(columns=["id"])
    data = data.sort_values(by=[data.columns[0]])

    return data

def process_data_compare(data_technical, data_actual):
    data = data_technical.merge(data_actual, how='left', on=['SC_NO', 'CODE_CUSTOMS']).rename(columns = {"id_x": "id"})
    cols = ["id", "SC_NO", "CODE_CUSTOMS", "TOTAL_AT", "TOTAL_PCS_AT", "DEMAND_AT", "TOTAL", "TOTAL_PCS", "DEMAND"]
    data = data[cols]

    data["COMPARE"] = ((data["DEMAND_AT"] / data["DEMAND"])*100).where(data["DEMAND"] > 0, 0)
    data["COMPARE"] = data["COMPARE"].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "")

    return data

def list_go_check():
    data_go = SupabaseFunctions().get_data("list_go", "*")
    sc_nos = data_go["SC_NO"].unique().tolist()
    sc_nos_str = ','.join(f"'{sc_no}'" for sc_no in sc_nos)
    return sc_nos_str