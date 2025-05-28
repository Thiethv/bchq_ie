import pandas as pd
from database.connect_supabase import SupabaseFunctions

class DemandTechnical():
    def __init__ (self):
        self.supabase = SupabaseFunctions()

    def process_to_technical(self, list_go):
        try:
            condition = f' "GO" IN {tuple(list_go)} ' if len(list_go) > 1 else f' "GO" = \'{list_go[0]}\' '

            df_submat = self.supabase.get_data("submat_demand", ' "GO", "CODE_HQ", "TOTAL_SUB_USED" ', condition )
            df_fabric = self.supabase.get_data("cutting_forecast", ' "GO", "CODE_CUSTOMS", "TOTAL_FB_USED", "Plan_Cut_Qty" ', condition )

            df_submat = df_submat.rename(columns={"GO": "SC_NO", "CODE_HQ": "CODE_CUSTOMS", "TOTAL_SUB_USED": "TOTAL"})
            df_fabric = df_fabric.rename(columns={"GO": "SC_NO", "CODE_CUSTOMS": "CODE_CUSTOMS", "TOTAL_FB_USED": "TOTAL", "Plan_Cut_Qty": "TOTAL_PCS"})

            df_demand = pd.concat([df_submat, df_fabric], ignore_index=True)

            if df_demand.empty:
                print("❌ Không tìm thấy dữ liệu technical_demand")
                return
            
            df_demand["TOTAL"] = df_demand["TOTAL"].fillna(0)
            df_demand["CODE_CUSTOMS"] = df_demand["CODE_CUSTOMS"].fillna("")
            df_demand["TOTAL_PCS"] = df_demand["TOTAL_PCS"].fillna(0)
            
            df_group = df_demand.groupby(["SC_NO", "CODE_CUSTOMS"]).agg(TOTAL=("TOTAL", "sum"), TOTAL_PCS=("TOTAL_PCS", "sum")).reset_index()

            if self.supabase.insert_data("dm_technical", df_group.to_dict('records')) == False:
                print("❌ Lỗi khi đưa dữ liệu sang dm_technical")
                return

        except Exception as e:
            print(f"❌ Lỗi khi update dữ liệu demand: {e}")

    def process_submat_demand(self, data_change):
        try:
            # Gọi hàm update trong supbase để cập nhật submat_demand
            if self.supabase.update_submat_demand() == False:
                print("❌ Lỗi khi update dữ liệu submat_demand")
                return
            # Kiểm tra sự thay đổi để cập nhật lại dữ liệu
            if data_change.empty:
                return

            trims_change = data_change[data_change["Remark"] == "Trims list"]
            if trims_change.empty:
                return
                
            item_change = trims_change["Item_change"].unique().tolist()

            if item_change:
                
                conditions = f' "Product_Code" IN {tuple(item_change)} ' if len(item_change) > 1 else f' "Product_Code" = \'{item_change[0]}\' '

                # Lấy các dòng submat_demand liên quan đến item_change
                data_submat = self.supabase.get_data("submat_demand", "*", conditions)

            if data_submat.empty:
                print("❌ Không CÓ dữ liệu submat_demand có CODE_HQ IS NULL")
                return
            
            data_trims = self.supabase.get_data("trims_list", "*")

            # Nối với trims_list theo Product_Code = THV_CODE
            merged = data_submat.merge(
                data_trims[["THV_CODE", "CODE_CUSTOMS", "CONVERT"]],
                left_on="Product_Code",
                right_on="THV_CODE",
                how="inner"
            )

            # Gán giá trị mới
            merged["CODE_HQ"] = merged["CODE_CUSTOMS"]
            merged["CONVERT_RATE"] = merged["CONVERT"]
            merged["TOTAL_SUB_USED"] = merged["Required_Qty"] * merged["CONVERT"]

            # Chỉ lấy các cột cần update
            update_cols = ["CODE_HQ", "CONVERT_RATE", "TOTAL_SUB_USED"]
            key_cols = ["Product_Code"]  # giả sử Product_Code là khóa

            # Chuẩn bị dữ liệu update
            update_json = merged[["Product_Code"] + update_cols].to_dict("records")

            # Update batch lên supabase
            if self.supabase.update_batch("submat_demand", update_cols, key_cols, update_json, False):
                self.supabase.delete_data("data_change", f' "Remark" = \'Trims list\' ')
                print(f"✅ Đã update {len(update_json)} dòng submat_demand")
        except Exception as e:
            print(f"Lỗi khi xử lý submat demand: {e}")

    def process_fabric_demand(self, data_change):
        try:
            data_cutting_forecast = self.supabase.get_data("cutting_forecast", "*", ' "CODE_CUSTOMS" IS NULL ')

            data_fabric_list = self.supabase.get_data("fabric_list", "*")

            if data_fabric_list.empty:
                print("❌ Không có hoặc không tìm thấy dữ liệu fabric_list")
                return

            # Tạo dict PO_NO -> CODE_CUSTOMS (nối bằng dấu , nếu có nhiều)
            customs_map = (
                data_fabric_list.groupby("PO_NO")["CODE_CUSTOMS"]
                .apply(lambda x: ",".join(sorted(set(x.dropna().astype(str)))))
                .to_dict()
            )

            width_map = (
                data_fabric_list.dropna(subset=["PO_NO", "Width"])
                .groupby("PO_NO")["Width"]
                .first()
                .to_dict()
            )

            # Nếu có thay đổi fabric list thì lấy thêm các dòng liên quan
            data_item = pd.DataFrame()
            if not data_change.empty:
                fabric_change = data_change[data_change["Remark"] == "Fabric list"]

                if not fabric_change.empty:
                    split_df = fabric_change["Item_change"].str.split("-", expand=True)
                    ppo_list = split_df[0].str.strip().tolist()
                    width_list = split_df[1].str.strip().tolist()
                    condition = f' "PPO_No" IN {tuple(ppo_list)} AND "Width" IN {tuple(width_list)} ' \
                    if len(ppo_list) > 1 else f' "PPO_No" = \'{ppo_list[0]}\' AND "Width" = \'{width_list[0]}\' '

                    # Lấy các dòng cutting_forecast liên quan đến item_change
                    data_item = self.supabase.get_data("cutting_forecast", "*", condition)
                    # Gộp với data_cutting_forecast, tránh trùng lặp
                    data_cutting_forecast = pd.concat([data_cutting_forecast, data_item], ignore_index=True) if not data_cutting_forecast.empty else data_item

            if data_cutting_forecast.empty:
                print("❌ Không có hoặc tìm thấy dữ liệu cutting_forecast mới để cập nhật")
                return
            
            # Tính CODE_CUSTOMS cho từng dòng của cutting_forecast
            data_cutting_forecast["CODE_CUSTOMS"] = data_cutting_forecast["PPO_No"].map(customs_map).fillna("")
            data_cutting_forecast["Width"] = data_cutting_forecast["PPO_No"].map(width_map).fillna(0)
            data_cutting_forecast["Marker_YY"] = data_cutting_forecast["Marker_YY"].fillna(0)
            data_cutting_forecast["Plan_Cut_Qty"] = data_cutting_forecast["Plan_Cut_Qty"].fillna(0)

            value_1 = 0.9144
            value_2 = 0.0254

            data_cutting_forecast["TOTAL_FB_USED"] = data_cutting_forecast["Marker_YY"] * data_cutting_forecast["Width"] * data_cutting_forecast["Plan_Cut_Qty"] * value_1 * value_2

            # Update to supabase
            table_name = "cutting_forecast"
            item_values = ["CODE_CUSTOMS", "Width", "TOTAL_FB_USED"]
            conditions = ["id", "PPO_No"]

            data_update = data_cutting_forecast.to_dict('records')
            update_json = [dict(t) for t in data_update]

            if self.supabase.update_batch(table_name, item_values, conditions, update_json, False):
                self.supabase.delete_data("data_change", ' "Remark" = \'Fabric list\' ')
                print(f"✅ Đã update dữ liệu fabric: {len(data_cutting_forecast)} dòng")

        except Exception as e:
            print(f"❌ Lỗi khi update dữ liệu fabric: {e}")

    def process_update_technical(self):
        try:
            # Update trên supabase bằng function
            if self.supabase.update_dm_technical() == False:
                print("❌ Lỗi khi update dữ liệu TOTAL PCS & DEMAND của dm_technical")
                return
            
            df_demand = self.supabase.get_data("dm_technical", "*")

            df = df_demand[(~df_demand["DEMAND"].isnull()) & (df_demand["CODE_CUSTOMS"].isin(["CA", "CB", "CST"]))]

            # Tính DEMAND.CA cho từng SC_NO
            ca = df[df['CODE_CUSTOMS'] == 'CA'][['SC_NO', 'DEMAND', 'TOTAL']]
            ca = ca.rename(columns={'DEMAND': 'DEMAND_CA', 'TOTAL': 'TOTAL_CA'})

            # Gộp DEMAND_CA và TOTAL_CA vào df theo SC_NO
            df = df.merge(ca[['SC_NO', 'DEMAND_CA', 'TOTAL_CA']], on='SC_NO', how='left')

            # Tính DEMAND mới cho CB
            mask_cb = df['CODE_CUSTOMS'] == 'CB'
            df.loc[mask_cb, 'DEMAND'] = df.loc[mask_cb, 'DEMAND_CA'] * df.loc[mask_cb, 'TOTAL'] / df.loc[mask_cb, 'TOTAL_CA']

            # Tính DEMAND mới cho CST
            mask_cst = df['CODE_CUSTOMS'].str.startswith('CST')
            df.loc[mask_cst, 'DEMAND'] = df.loc[mask_cst, 'DEMAND_CA'] * 3

            df['DEMAND'] = df['DEMAND'].fillna(0)

            # Update to supabse (dm_submat)
            data_update = df.loc[df['CODE_CUSTOMS'] != 'CA', ['SC_NO', 'CODE_CUSTOMS', 'DEMAND']].to_dict('records')
            update_json = [dict(t) for t in data_update]

            if self.supabase.update_batch("dm_technical", ["DEMAND"], ["SC_NO", "CODE_CUSTOMS"], update_json, False):
                print(f"✅ Đã update dữ liệu demand: {len(df)} dòng")

            self.update_note_check_technical()

        except Exception as e:
            print(f"❌ Lỗi khi update dữ liệu demand: {e}")
    
    def update_note_check_technical(self):
        try:
            # Lấy toàn bộ dữ liệu dm_technical
            df = self.supabase.get_data("dm_technical", "*")

            # Lấy dữ liệu range_dm để join
            range_dm = self.supabase.get_data("range_dm", "*")
            if range_dm.empty:
                print("❌ Không tìm thấy dữ liệu range_dm")
                return

            # Merge để lấy MIN, MAX, CODE_NAME, RANGE cho từng CODE_CUSTOMS
            df = df.merge(
                range_dm[["CODE", "MIN", "MAX", "CODE_NAME", "RANGE"]],
                left_on="CODE_CUSTOMS", right_on="CODE", how="left"
            )

            def check_note(row):
                if pd.notnull(row["DEMAND"]) and pd.notnull(row["MIN"]) and pd.notnull(row["MAX"]):
                    if row["MIN"] < row["DEMAND"] < row["MAX"]:
                        return row["CODE_NAME"]
                return None

            # Tính NOTE
            df["NOTE"] = df.apply(lambda row: check_note(row), axis=1)

            required_codes = {"CA", "CST", "IN", "THR", "PB", "W-FAB"}

            # Tạo hàm kiểm tra xuất hiện bất kỳ chuỗi nào trong required_codes
            def find_codes_in_row(codes):
                found = set()
                for req in required_codes:
                    for code in codes:
                        if pd.notnull(code) and req in str(code):
                            found.add(req)
                return found

            # Lấy các CODE_CUSTOMS thực tế xuất hiện cho từng SC_NO (theo logic mới)
            go_codes = df.groupby("SC_NO")["CODE_CUSTOMS"].apply(lambda codes: find_codes_in_row(codes))

            # Tìm các code còn thiếu
            missing_codes = go_codes.apply(lambda found: required_codes - found)

            # Tạo dict tra cứu GO thiếu CODE_CUSTOMS
            missing_dict = missing_codes.to_dict()

            def check_dm(row):
                # CHECK_DM: chỉ ghi "Không {row['RANGE']}" nếu NOTE bị thiếu
                if pd.isnull(row["NOTE"]) and row["DEMAND"] > 0:
                    return f"Không {row['RANGE']}" if pd.notnull(row["RANGE"]) else None
                
                return None
            
            def remark_dm(row, missing_dict):
                miss = missing_dict.get(row["SC_NO"], set())
                # REMARK: chỉ ghi GO thiếu hoặc thiếu nếu miss không rỗng
                if pd.isnull(row["NOTE"]) and pd.isnull(row["CHECK_DM"]):
                    return "Kiểm lại PPO_No hoặc Product_code"
                
                elif row["DEMAND"] == 0 and (row["CODE_CUSTOMS"].startswith("CB") or row["CODE_CUSTOMS"].startswith("CST")):
                    return "GO không có CA để tính CB và CST"
                
                elif miss and row["CODE_CUSTOMS"] != '':
                    return f"GO thiếu: {', '.join(sorted(miss))}"               

                return None

            df["CHECK_DM"] = df.apply(check_dm, axis=1)
            df["REMARK"] = df.apply(lambda row: remark_dm(row, missing_dict), axis=1)

            df = df.drop(columns=["id"])

            df.to_excel("check_dm.xlsx", index=False)
            
            df = df.drop(columns=["CODE","MIN", "MAX", "CODE_NAME", "RANGE"])

            # pdate lại lên supabase
            update_cols = ["NOTE", "CHECK_DM", "REMARK"]
            key_cols = ["SC_NO", "CODE_CUSTOMS"]
            update_json = df[["SC_NO", "CODE_CUSTOMS", "NOTE", "CHECK_DM", "REMARK"]].to_dict("records")

            if self.supabase.update_batch("dm_technical", update_cols, key_cols, update_json, False):
                print(f"✅ Cập nhật và truy vấn dữ liệu Technical thành công: {len(df)} dòng")

        except Exception as e:
            print(f"❌ Lỗi khi update dữ liệu demand: {e}")

    def get_results_dm_technical(self):
        
        data_go = self.supabase.get_data("list_go", "*")
        data_change = self.supabase.get_data("data_change", "*")
        
        if data_go.empty:
            print("❌ Không tìm thấy dữ liệu list GO cần tính demand")
            return
        
        list_go = data_go["SC_NO"].unique().tolist()

        self.process_submat_demand(data_change)
        self.process_fabric_demand(data_change)

        if self.supabase.delete_data("dm_technical", f' "SC_NO" IN {tuple(list_go)} '):
            self.process_to_technical(list_go)
            self.process_update_technical()
            return True
        else:
            print("❌ Lỗi khi xóa dữ liệu dm_technical")
            return False