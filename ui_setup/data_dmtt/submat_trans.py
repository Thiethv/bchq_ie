
import pandas as pd
from database.connect_sqlserver import ConnectSQLServer
from database.connect_supabase import SupabaseFunctions

class SubmatTrans():
    def __init__(self, from_date):
        self.sql_query = ConnectSQLServer()
        self.supa_func = SupabaseFunctions()
        self.from_date = from_date

        self.process_data()

    def get_table(self):
        data_go = self.supa_func.get_data("list_go", "*")
        if data_go.empty:
            print("❌ Không tìm thấy dữ liệu list_go")
            return
        jo_nos = data_go["SC_NO"].unique().tolist()
        jo_nos_str = ','.join(f"'{jo_no}'" for jo_no in jo_nos)

        query = f'''
            SELECT * FROM [dbo].[V_Submat_Trans_Summary_EHV]
            WHERE [SC_NO] IN ({jo_nos_str})
            AND [TRANS_DATE] >= '{self.from_date}'
        '''
        data = self.sql_query.getData(query)
        return data, jo_nos_str
    
    def process_data(self):
        data, jo_nos_str = self.get_table()
        if data.empty:
            print("❌ Không tìm thấy dữ liệu submat_trans")
            return
        
        cols = [0, 3, 4, 5, 6, 7, 15, 17, 19, 20]
        data = data.iloc[:, cols]
        data.columns = ["STORE_CODE", "SC_NO", "JO_NO", "TRANS_DATE", "TRANS_CD", "ITEM_CODE", "PRODUCT_GROUP_NAME", "PRODUCT_CLASS", "TRANS_UOM", "QTY"]
        split_item_code = data["ITEM_CODE"].str.split(".", n=1, expand=True)
        data["PRODUCT_CODE"] = split_item_code[0]
        data["SUB_CODE"] = split_item_code[1]

        data_trims_list = self.supa_func.get_data("trims_list", "*")
        if data_trims_list.empty:
            print("❌ Không tìm thấy dữ liệu trims_list")
            return
        
        data_result = data.merge(data_trims_list, how="left", left_on ="PRODUCT_CODE", right_on="THV_CODE")
        data_result = data_result.drop(columns=["id", "THV_CODE"])
        
        data_result["QTY"] = data_result["QTY"].apply(lambda x: abs(x) if pd.notnull(x) else x)
        data_result["QTY"] = data_result["QTY"].abs()
        data_result["TOTAL"] = data_result["QTY"].fillna(0) * data_result["CONVERT"].fillna(0)

        data_result["TRANS_DATE"] = data_result["TRANS_DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")
        
        for col in data_result.select_dtypes(include=['object']).columns:
            data_result[col] = data_result[col].fillna('')
        for col in data_result.select_dtypes(include=['float64']).columns:
            data_result[col] = data_result[col].fillna(0)

        if self.supa_func.delete_data("submat_trans", f' "SC_NO" IN ({jo_nos_str}) ') == True:
            print("✅ Xóa dữ liệu submat_trans thành công")
            if self.supa_func.insert_data("submat_trans", data_result.to_dict(orient="records")) == True:
                print("✅ Thêm dữ liệu submat_trans thành công")
                return True
            else:
                print("❌ Lỗi khi thêm dữ liệu submat_trans")
                return False