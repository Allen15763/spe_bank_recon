import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from typing import Optional, Dict, Any


class GoogleSheetsManager:
    """
    Google Sheets 資料管理類別
    
    用於簡化 Google Sheets 的讀取、寫入和更新操作
    
    Attributes:
        service: gspread Spreadsheet 物件
        scopes: Google API 權限範圍列表
    """
    
    def __init__(self, credentials_path: str, spreadsheet_url: str):
        """
        初始化 GoogleSheetsManager
        
        Args:
            credentials_path: Service Account JSON 金鑰檔案的路徑
            spreadsheet_url: Google Sheets 試算表的完整 URL
        
        Raises:
            FileNotFoundError: 當 credentials 檔案不存在時
            gspread.exceptions.SpreadsheetNotFound: 當試算表 URL 無效時
        """
        self.scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # 認證
        self.creds = Credentials.from_service_account_file(
            credentials_path,
            scopes=self.scopes
        )
        
        # 建立 gspread 客戶端並開啟試算表
        gc = gspread.authorize(self.creds)
        self.service = gc.open_by_url(spreadsheet_url)
    
    def get_data(self, 
                 sheet_name: str = 'Sheet1',
                 range_name: Optional[str] = None) -> pd.DataFrame:
        """
        從指定的工作表讀取資料
        
        Args:
            sheet_name: 工作表名稱，預設為 'Sheet1'
            range_name: 指定範圍（例如 'A1:D10'），若為 None 則讀取所有資料
        
        Returns:
            包含工作表資料的 pandas DataFrame
        
        Examples:
            >>> manager = GoogleSheetsManager('credentials.json', 'sheet_url')
            >>> df = manager.get_data('Sheet1')  # 讀取所有資料
            >>> df = manager.get_data('Sheet1', 'A1:D10')  # 讀取指定範圍
        """
        worksheet = self.service.worksheet(sheet_name)
        
        if range_name:
            # 取得指定範圍
            data = worksheet.get(range_name)
            
            # 將範圍資料轉換為 DataFrame
            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])
            else:
                df = pd.DataFrame(data)
        else:
            # 取得所有資料
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
        
        return df
    
    def write_data(self,
                   df: pd.DataFrame,
                   sheet_name: str = 'Sheet1',
                   is_append: bool = False,
                   clear_range: Optional[str] = None) -> Dict[str, Any]:
        """
        將 DataFrame 寫入到指定的工作表
        
        Args:
            df: 要寫入的 pandas DataFrame
            sheet_name: 工作表名稱，預設為 'Sheet1'
            is_append: 是否為追加模式（True: 追加，False: 覆寫），預設為 False
            clear_range: 在覆寫模式下，要清除的範圍（例如 'A1:Z100'），若為 None 則清除整個工作表
        
        Returns:
            操作結果的字典
        
        Examples:
            >>> manager = GoogleSheetsManager('credentials.json', 'sheet_url')
            >>> df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
            >>> manager.write_data(df, 'Sheet1')  # 覆寫整個工作表
            >>> manager.write_data(df, 'Sheet1', is_append=True)  # 追加資料
        """
        sheet = self.service.worksheet(sheet_name)
        
        if is_append:
            # 追加模式：處理 NaN 並轉換類型
            rows = df.fillna('').astype(object).values.tolist()
            result = sheet.append_rows(rows, value_input_option='RAW')
        else:
            # 覆寫模式：先清除現有資料
            if clear_range:
                sheet.batch_clear([clear_range])
            else:
                sheet.clear()
            
            # 寫入 DataFrame（包含標題和資料）
            result = sheet.update(
                [df.columns.values.tolist()] + df.values.tolist()
            )
        
        return result
    
    def recreate_and_write(self,
                           df: pd.DataFrame,
                           sheet_name_old: str,
                           sheet_name_new: str,
                           rows: Optional[int] = None,
                           cols: Optional[int] = None) -> Dict[str, Any]:
        """
        刪除舊工作表並建立新工作表來寫入資料
        
        此方法會完全刪除指定的工作表，然後建立一個新的工作表並寫入資料。
        適用於需要重置工作表設定或結構的情況。
        
        Args:
            df: 要寫入的 pandas DataFrame
            sheet_name_old: 要刪除的舊工作表名稱
            sheet_name_new: 要建立的新工作表名稱
            rows: 新工作表的列數，預設為 None（自動調整）
            cols: 新工作表的欄數,預設為 None（自動調整）
        
        Returns:
            操作結果的字典
        
        Examples:
            >>> manager = GoogleSheetsManager('credentials.json', 'sheet_url')
            >>> df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
            >>> manager.recreate_and_write(df, 'OldSheet', 'NewSheet', rows=100, cols=10)
        """
        # 刪除舊工作表
        sheet_old = self.service.worksheet(sheet_name_old)
        self.service.del_worksheet(sheet_old)
        
        # 建立新工作表
        new_sheet = self.service.add_worksheet(
            title=sheet_name_new,
            rows=rows,
            cols=cols
        )
        
        # 寫入資料（包含標題和資料）
        result = new_sheet.update(
            [df.columns.values.tolist()] + df.values.tolist()
        )
        
        return result
    
    def get_all_worksheets(self) -> list:
        """
        取得試算表中所有工作表的名稱
        
        Returns:
            工作表名稱的列表
        """
        return [worksheet.title for worksheet in self.service.worksheets()]
    
    def create_worksheet(self, 
                         sheet_name: str,
                         rows: int = 1000,
                         cols: int = 26) -> gspread.Worksheet:
        """
        建立新的工作表
        
        Args:
            sheet_name: 新工作表的名稱
            rows: 列數，預設為 1000
            cols: 欄數，預設為 26
        
        Returns:
            新建立的工作表物件
        """
        return self.service.add_worksheet(
            title=sheet_name,
            rows=rows,
            cols=cols
        )
    
    def delete_worksheet(self, sheet_name: str) -> None:
        """
        刪除指定的工作表
        
        Args:
            sheet_name: 要刪除的工作表名稱
        """
        worksheet = self.service.worksheet(sheet_name)
        self.service.del_worksheet(worksheet)


# 使用範例
# if __name__ == "__main__":
#     # 初始化管理器
#     manager = GoogleSheetsManager(
#         credentials_path='credentials.json',
#         spreadsheet_url='https://docs.google.com/spreadsheets/d/17puiAmAhM2dAm9BR7Sck1E2fwsf0v76CkpiLkVJPlxE/edit?gid=0#gid=0'
#     )
    
#     # 讀取資料
#     df = manager.get_data('Sheet1')
#     print(df.head())
    
#     # 寫入資料
#     new_df = pd.DataFrame({
#         'Column1': [1, 2, 3],
#         'Column2': ['A', 'B', 'C']
#     })
#     manager.write_data(new_df, 'Sheet1')
    
#     # 追加資料
#     append_df = pd.DataFrame({
#         'Column1': [4, 5],
#         'Column2': ['D', 'E']
#     })
#     manager.write_data(append_df, 'Sheet1', is_append=True)
    
#     # 取得所有工作表名稱
#     sheets = manager.get_all_worksheets()
#     print(f"所有工作表: {sheets}")