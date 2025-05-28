import os
import pandas as pd
from database.connect_supabase import SupabaseFunctions

class MasterList:
    def __init__(self):
        self.supabase = SupabaseFunctions()
    
    def insert_list_trims_to_supabase(self, data_file, check_delete):
        '''
            Lấy danh sách trims từ file excel và đẩy lên supabase
        '''
        if data_file.empty:
            print("❌ Không có dữ liệu trims")
            return

        # Lấy dữ liệu từ supabase
        data_supa = self.supabase.get_data("trims_list", "*")
        if check_delete:
            if self.supabase.truncate_table("trims_list") == True:
                print("✅ Đã xóa dữ liệu trims")
            else:
                print("❌ Lỗi khi xóa dữ liệu trims")
                return
            data = data_file
        else:
            data = data_file[~data_file["THV_CODE"].isin(data_supa["THV_CODE"])] if not data_supa.empty else data_file

            if not data_supa.empty:
                code_custom_convert_map = data_supa.set_index("THV_CODE")["CONVERT"].to_dict()
                data_file["Convet_in_data"] = data_file["THV_CODE"].map(code_custom_convert_map)

                df_check = data_file[(data_file["THV_CODE"].isin(data_supa["THV_CODE"])) & (data_file["CONVERT"] != data_file["Convet_in_data"])] # Lọc các THV_CODE có CONVERT khác Convet_in_data
                if not df_check.empty:
                    codes = df_check["THV_CODE"].unique().tolist()
                    conditions = f' "THV_CODE" IN {tuple(codes)} ' if len(codes) > 1 else f' "THV_CODE" = {codes[0]} '
                    self.supabase.delete_data("trims_list", conditions)

                    data = pd.concat([data, df_check], ignore_index=True)

                    df_change = df_check[["THV_CODE"]].rename(columns={"THV_CODE": "Item_change"})
                    df_change["Remark"] = "Trims list"

                    self.supabase.insert_data("data_change", df_change.to_dict('records'))
                    
                data = data.drop("Convet_in_data", axis=1)

        data_trims_list = data.copy()
        if data_trims_list.empty:
            print("❌ Không có dữ liệu trims")
            return

        data_trims_list["CONVERT"] = pd.to_numeric(data_trims_list["CONVERT"], errors='coerce')

        data_trims_list = data_trims_list.dropna()
        data_trims_list["CONVERT"] = data_trims_list["CONVERT"].astype(float)

        data_trims_list = data_trims_list.drop_duplicates()

        data_json = data_trims_list.to_dict('records')
        if self.supabase.insert_data("trims_list", data_json) == True:
            print(f"✅ Đã đưa dữ liệu trims: {len(data_trims_list)} dòng")
            return True
        else:
            print("❌ Lỗi khi lấy dữ liệu trims")
            return False
    
    def insert_list_fabric_to_supabase(self, data_file, check_delete):
        '''
            Lấy danh sách fabric từ file excel và đẩy lên supabase
        '''
        if data_file.empty:
            print("❌ Không có dữ liệu fabric")
            return        

        # Lý dữ liệu từ supabase
        data_supa = self.supabase.get_data("fabric_list", "*")
        if check_delete:
            if self.supabase.truncate_table("fabric_list") == True:
                print("✅ Đã xóa dữ liệu fabric")
            else:
                print("❌ Lỗi khi xóa dữ liệu fabric")
                return
            data = data_file
        else:
            data = data_file[~data_file["PO_NO"].isin(data_supa["PO_NO"])] if not data_supa.empty else data_file
            if not data_supa.empty:
                # Tạo dict ánh xạ PO_NO -> Width từ data_supa
                code_custom_convert_map = (
                    data_supa.dropna(subset=["PO_NO", "Width"])
                    .groupby("PO_NO")["Width"]
                    .first()
                    .to_dict()
                )

                data_file["Convet_in_data"] = data_file["PO_NO"].map(code_custom_convert_map)

                df_check = data_file[(data_file["PO_NO"].isin(data_supa["PO_NO"]))
                                     & (data_file["Width"] != data_file["Convet_in_data"])] # Lọc các PO_NO có Width khác Convet_in_data
                if not df_check.empty:
                    codes = df_check["PO_NO"].unique().tolist()
                    conditions = f' "PO_NO" IN {tuple(codes)} ' if len(codes) > 1 else f' "PO_NO" = {codes[0]} '
                    self.supabase.delete_data("fabric_list", conditions)

                    data = pd.concat([data, df_check], ignore_index=True)

                    df_change = df_check[["PO_NO", "Convet_in_data"]].copy()
                    df_change["Item_change"] = df_check["PO_NO"].astype(str) + "-" + df_check["Convet_in_data"].astype(str)
                    df_change["Remark"] = "Fabric list"

                    df_change.columns = ["Item_change", "Remark"]

                    self.supabase.insert_data("data_change", df_change.to_dict('records'))
                    
                data = data.drop("Convet_in_data", axis=1)

        data_fabric = data.copy()
        if data_fabric.empty:
            print("❌ Không có dữ liệu fabric")
            return

        data_fabric["Width"] = pd.to_numeric(data_fabric["Width"], errors='coerce')

        data_fabric = data_fabric.dropna()
        data_fabric["Width"] = data_fabric["Width"].astype(float)

        data_fabric = data_fabric.drop_duplicates()

        data_json = data_fabric.to_dict('records')
        if self.supabase.insert_data("fabric_list", data_json) == True:
             print(f"✅ Đã đưa dữ liệu fabric master: {len(data_fabric)} dòng")
             return True
        else:
            print("❌ Lỗi khi lấy dữ liệu fabric master")
            return False
    
    def insert_range_demand_to_supabase(self, data_file, check_delete):
        '''
            Đẩy dữ liệu range định mức lên supabase
        '''
        if data_file.empty:
            print("❌ Không có dữ liệu range")
            return

        # Lý dữ liệu từ supabase
        data_supa = self.supabase.get_data("range_dm", "*")
        if check_delete:

            if self.supabase.truncate_table("range_dm") == True:
                print("✅ Đã xóa dữ liệu fabric")
            else:
                print("❌ Lỗi khi xóa dữ liệu fabric")
                return
            data = data_file
        else:
            data = data_file[~data_file["CODE"].isin(data_supa["CODE"])] if not data_supa.empty else data_file

        # Đọc dữ liệu từ file Excel
        df = data.copy()
        df.columns = ["CODE", "MIN", "MAX", "CODE_NAME", "UNITS", "RANGE"]
        df["MIN"] = pd.to_numeric(df["MIN"], errors='coerce')
        df['MAX'] = pd.to_numeric(df['MAX'], errors='coerce')

        df = df.dropna()
        df["MIN"] = df["MIN"].astype(float)
        df['MAX'] = df['MAX'].astype(float)

        df = df.drop_duplicates()

        if check_delete and not data.empty:
            df = df[~df["CODE"].isin(data["CODE"])]  # Lọc ra những dòng không có trong supabase

        if df.empty:
            print("❌ Không tìm thấy dữ liệu range_dm")
            return
        data_json = df.to_dict('records')
        if self.supabase.insert_data("range_dm", data_json) == True:
            print(f"✅ Đã lấy dữ liệu range_dm: {len(df)} dòng")
            return True
        else:
            print("❌ Lỗi khi lấy dữ liệu range_dm")
            return False

    def insert_list_go(self, data_file, check_delete):
        """
        Đẩy dữ liệu list_go lên supabase
        """
        if data_file.empty:
            print("❌ Không có dữ liệu list_go")
            return

        data_supabase = self.supabase.get_data("list_go", "*")

        if check_delete:
            if self.supabase.truncate_table("list_go"):
                print("✅ Đã xóa dữ liệu list_go")
            else:
                print("❌ Lỗi khi xóa dữ liệu list_go")
                return
            data = data_file
        else:
            data = data_file if data_supabase.empty else data_file[~data_file["SC_NO"].isin(data_supabase["SC_NO"])]

        data = data.dropna()
        if data.empty:
            print("❌ Không có dữ liệu list_go")
            return

        data_json = data.to_dict('records')
        if self.supabase.insert_data("list_go", data_json):
            print(f"✅ Đã lấy dữ liệu list_go: {len(data)} dòng")
            return True
        else:
            print("❌ Lỗi khi lấy dữ liệu list_go")
            return False