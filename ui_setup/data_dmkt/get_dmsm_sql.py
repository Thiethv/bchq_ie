import pandas as pd
from datetime import datetime
import streamlit as st

from database.connect_sqlserver import ConnectSQLServer
from database.connect_supabase import SupabaseFunctions


class DemandSM:
    def __init__(self):
        self.queries = ConnectSQLServer()
        self.supa_func = SupabaseFunctions()

    def get_data_demand(self, year, from_month):
        '''
        Lấy dữ liệu Demand submat theo khoảng thời gian: 1 tháng
        Đã đưa dữ liệu lên supabase đến tháng 4/2025
        '''
        data_go = self.supa_func.get_data("list_go", "*")
        if data_go.empty:
            print("❌ Không tìm thấy dữ liệu list_go")
            return
        jo_nos = data_go["SC_NO"].str[1:].unique().tolist()
        jo_nos_str = ','.join(f"'{jo_no}'" for jo_no in jo_nos)

        from_date = datetime(year, from_month, 1)
        
        query = f"""
            SELECT * 
            FROM dbo.V_MRP_JO_Demand_EHV
            WHERE [Required Qty] > 0 
            AND [Required Qty] IS NOT NULL
            AND [Create Date] >= '{from_date.strftime('%Y-%m-%d')}'
            AND LEFT([JO NO], 8) IN ({jo_nos_str})
        """

        df_month = self.queries.getData(query)        

        if df_month.empty:
            st.warning(f"❌ Không tìm thấy dữ liệu trong tháng {from_month}/{year}, chọn lại tháng khác!")
            return
        df_month["GO"] = "S" + df_month['JO NO'].str[:8]

        df_all = df_month
        cols = [0, 7, 8, 9, 10, 11, 15, 17, 19, 20, 21]
        df_all = df_all.iloc[:, cols]

        df_all.columns = [
            "JO_NO",
            "Required_Qty",
            "Allocated_Qty",
            "Issued_Qty",
            "Demand_Qty",
            "UOM",
            "Manual_Demand",
            "Create_Date",
            "Product_Code",
            "Dimm_No",
            "GO"
        ]

        # --- Đẩy lên supbase ---
        if df_all.empty:
            st.warning(f"✅ Dữ liệu đã có trong Database!")
            return
        
        df_remaining = df_all
        df_remaining = df_remaining.dropna()
        if "Create_Date" in df_remaining.columns:
            df_remaining["Create_Date"] = df_remaining["Create_Date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        data_json = df_remaining.to_dict('records')

        if self.supa_func.delete_data("submat_demand", f' LEFT("JO_NO", 8) IN ({jo_nos_str}) ') == True:
            if self.supa_func.insert_data("submat_demand", data_json) == True:
                st.success(f"✅ Đã lý dữ liệu submat demand so với list GO: {df_remaining['GO'].nunique()} / {data_go['SC_NO'].nunique()}")
    
    def get_go_quantity(self, year):
        '''
            Đã đưa lên supabase dữ liệu năm 2023, 2024 và 2025 ngày 15/5
        '''
        data_go = self.supa_func.get_data("list_go", "*")
        if data_go.empty:
            print("❌ Không tìm thấy dữ liệu list_go")
            return
        jo_nos = data_go["SC_NO"].unique().tolist()
        jo_nos_str = ','.join(f"'{jo_no}'" for jo_no in jo_nos)

        query = f"""
            SELECT [GO No], [Order QTY], [Year] 
            FROM escmowner.V_GO
            WHERE [Factory Code] = 'EHV'
            AND [Year] >= '{year}'
            AND [Order QTY] > 0
            AND [GO No] IN ({jo_nos_str})
            """

        df = self.queries.getData(query)

        if df.empty:
            st.warning(f"❌ Không tìm thấy dữ liệu từ năm {year}, chọn lại năm khác!")
            return None
        
        df_remaining = df

        if df_remaining.empty:
            st.warning(f"✅ Dữ liệu đã có trong Database")
            return
        df_remaining.columns = ["GO_No", "Order_QTY", "Year"]

        df_remaining = df_remaining.dropna()

        df_remaining['Year'] = df_remaining['Year'].astype(int).astype(str)
        df_remaining['Order_QTY'] = df_remaining['Order_QTY'].astype(int)
        if self.supa_func.delete_data("go_quantity", f' "GO_No" IN ({jo_nos_str}) '):
            if self.supa_func.insert_data("go_quantity", df_remaining.to_dict('records')):
                st.success(f"✅ Đã lấy dữ liệu được so với list GO: {df_remaining['GO_No'].nunique()} / {data_go['SC_NO'].nunique()}")

        