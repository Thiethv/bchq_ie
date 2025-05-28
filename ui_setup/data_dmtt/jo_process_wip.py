
from database.connect_supabase import SupabaseFunctions
from database.connect_sqlserver import ConnectSQLServer

class JoProcessWip():
    def __init__(self):
        self.sql_query = ConnectSQLServer()
        self.supa_func = SupabaseFunctions()

    def get_table(self):
        data_go = self.supa_func.get_data("list_go", "*")
        if data_go.empty:
            print("❌ Không tìm thấy dữ liệu list_go")
            return
        jo_nos = data_go["SC_NO"].str[1:].unique().tolist()
        jo_nos_str = ','.join(f"'{jo_no}'" for jo_no in jo_nos)

        query = f'''
            SELECT * FROM [dbo].[V_JO_Process_WIP_EHV]
            WHERE LEFT([JO NO], 8) IN ({jo_nos_str})
        '''
        data = self.sql_query.getData(query)

        return data, jo_nos_str
    
    def process_wip(self):
        data, jo_nos_str = self.get_table()
        if data.empty:
            print("❌ Không tìm thấy dữ liệu jo_process_wip")
            return
        
        cols = [1, 2, 3, 4, 8, 9, 10, 11, 12]
        
        data = data.iloc[:, cols]

        data["SC_NO"] = "S"+ data["JO NO"].str[:8]

        data.columns = ["JO_NO", "Color_Code","Size_Code", "Process_Code", "In_Qty", "Output_Qty", "Pull_In_Qty", "Discrepancy_Qty", "Wip", "SC_NO"]
        for col in data.select_dtypes(include=['object']).columns:
            data[col] = data[col].fillna('')
        for col in data.select_dtypes(include=['float64']).columns:
            data[col] = data[col].fillna(0)

        if self.supa_func.delete_data("process_wip", f' LEFT("JO_NO", 8) IN ({jo_nos_str}) ') == True:
            print("✅ Xóa dữ liệu process_wip thanh cong")

            if self.supa_func.insert_data("process_wip", data.to_dict(orient="records")) == True:
                print("✅ Thêm dữ liệu process_wip thành công")
                return True
            else:
                print("❌ Lỗi khi thêm dữ liệu process_wip")
                return False