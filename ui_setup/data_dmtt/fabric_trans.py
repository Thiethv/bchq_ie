
import pandas as pd
from database.connect_sqlserver import ConnectSQLServer
from database.connect_supabase import SupabaseFunctions


class FabricTrans():
    def __init__(self, from_date):
        self.sql_query = ConnectSQLServer()
        self.supa_func = SupabaseFunctions()
        self.from_date = from_date

    def get_table(self):
        data_go = self.supa_func.get_data("list_go", "*")
        if data_go.empty:
            print("❌ Không tìm thấy dữ liệu list_go")
            return
        jo_nos = data_go["SC_NO"].unique().tolist()
        jo_nos_str = ','.join(f"'{jo_no}'" for jo_no in jo_nos)

        query = f'''
            SELECT * FROM [dbo].[V_Fabric_Trans_Summary_EHV]
            WHERE [SC_NO] IN ({jo_nos_str})
            AND [TRANS_DATE] >= '{self.from_date}'
        '''
        data = self.sql_query.getData(query)
        return data, jo_nos_str
    
    def process_data(self):
        data, jo_nos_str = self.get_table()
        if data.empty:
            print("❌ Không tìm thấy dữ liệu fabric_trans")
            return
        cols = [3, 4, 5, 6, 7, 8, 9, 19, 20]
        data = data.iloc[:, cols]
        data.columns = ["SC_NO", "JO NO", "TRANS_DATE", "TRANS_CD", "ITEM_CODE", "PO_NO", "TRANS TYPE", "TRANS_UOM", "QTY"]
        data["PO_Item"] = data["PO_NO"] + " " + data["ITEM_CODE"]
        # Thêm các cột CUSTOMS CODE, Width, Total
        data_fabric_supbase = self.supa_func.get_data("fabric_list", "*")
        if data_fabric_supbase.empty:
            print("❌ Không tìm thấy dữ liệu fabric_list")
            return
        data_result = data.merge(data_fabric_supbase, how="left", on="PO_Item").rename(columns={
            "PO_NO_x": "PO_NO", "JO NO": "JO_NO", "TRANS TYPE":"TRANS_TYPE"})
        data_result = data_result.drop(columns=["PO_NO_y", "id"])

        for col in data_result.select_dtypes(include=['object']).columns:
            data_result[col] = data_result[col].fillna('')
        for col in data_result.select_dtypes(include=['float64']).columns:
            data_result[col] = data_result[col].fillna(0)

        data_result["QTY"] = data_result["QTY"].apply(lambda x: abs(x) if pd.notnull(x) else x)
        data_result["QTY"] = data_result["QTY"].abs()
        data_result["TOTAL"] = data_result["QTY"].fillna(0) * data_result["Width"].fillna(0) * 0.9144 * 0.0254

        data_result["TRANS_DATE"] = data_result["TRANS_DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")        

        if self.supa_func.delete_data("fabric_trans", f' "SC_NO" IN ({jo_nos_str}) ') == True:
            print("✅ Xóa dữ liệu fabric_trans thành công")

            if self.supa_func.insert_data("fabric_trans", data_result.to_dict(orient="records")) == True:
                print("✅ Thêm dữ liệu fabric_trans thành công")
                return True
            else:
                print("❌ Lỗi khi thêm dữ liệu fabric_trans")
                return False
            