import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import warnings
from typing import Optional, Dict, Any
from .base import DataSource
from .config import DataSourceConfig
from src.utils.logging import get_logger


class GoogleSheetsManager(DataSource):
    """
    Google Sheets 資料管理類別（繼承自 DataSource 基類）

    用於簡化 Google Sheets 的讀取、寫入和更新操作

    Attributes:
        service: gspread Spreadsheet 物件
        scopes: Google API 權限範圍列表
        credentials_path: Service Account JSON 金鑰檔案路徑
        spreadsheet_url: Google Sheets 試算表 URL
        default_sheet: 預設工作表名稱
    """

    def __init__(self, config: Optional[DataSourceConfig] = None,
                 credentials_path: Optional[str] = None,
                 spreadsheet_url: Optional[str] = None):
        """
        初始化 GoogleSheetsManager

        支援兩種初始化方式:
        1. 使用 DataSourceConfig (推薦，符合 DataSource 規範)
        2. 使用 credentials_path 和 spreadsheet_url (向後兼容)

        Args:
            config: DataSourceConfig 配置物件
                   connection_params 需包含:
                   - credentials_path: Service Account JSON 路徑
                   - spreadsheet_url: Google Sheets URL
                   - default_sheet: 預設工作表名稱（可選，預設為 'Sheet1'）
            credentials_path: Service Account JSON 金鑰檔案的路徑（向後兼容）
            spreadsheet_url: Google Sheets 試算表的完整 URL（向後兼容）

        Raises:
            ValueError: 當參數不正確時
            FileNotFoundError: 當 credentials 檔案不存在時
            gspread.exceptions.SpreadsheetNotFound: 當試算表 URL 無效時
        """
        # 向後兼容: 如果沒有提供 config，使用舊方式初始化
        if config is None:
            if credentials_path is None or spreadsheet_url is None:
                raise ValueError("必須提供 config 或 (credentials_path 和 spreadsheet_url)")

            # 創建默認配置
            config = DataSourceConfig(
                source_type='google_sheets',
                connection_params={
                    'credentials_path': credentials_path,
                    'spreadsheet_url': spreadsheet_url,
                    'default_sheet': 'Sheet1'
                },
                cache_enabled=False
            )

            warnings.warn(
                "使用 credentials_path 和 spreadsheet_url 初始化的方式已過時，"
                "建議使用 DataSourceConfig 初始化",
                DeprecationWarning,
                stacklevel=2
            )

        # 調用父類初始化
        super().__init__(config)

        # 從配置中提取參數
        self.credentials_path = config.connection_params.get('credentials_path')
        self.spreadsheet_url = config.connection_params.get('spreadsheet_url')
        self.default_sheet = config.connection_params.get('default_sheet', 'Sheet1')

        if not self.credentials_path or not self.spreadsheet_url:
            raise ValueError("connection_params 必須包含 'credentials_path' 和 'spreadsheet_url'")

        # 設置 Google API 權限範圍
        self.scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        # 初始化連接
        self._init_connection()

    def _init_connection(self):
        """初始化 Google Sheets 連接"""
        try:
            # 認證
            self.creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=self.scopes
            )

            # 建立 gspread 客戶端並開啟試算表
            gc = gspread.authorize(self.creds)
            self.service = gc.open_by_url(self.spreadsheet_url)

            self.logger.info(f"成功連接到 Google Sheets: {self.spreadsheet_url}")

        except Exception as e:
            self.logger.error(f"連接 Google Sheets 失敗: {e}")
            raise

    def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        讀取 Google Sheets 數據（符合 DataSource 規範）

        Args:
            query: 工作表名稱（可選，預設使用配置的 default_sheet）
            **kwargs:
                - sheet_name: 工作表名稱（優先於 query）
                - range_name: 指定範圍（例如 'A1:D10'）

        Returns:
            包含工作表資料的 pandas DataFrame

        Examples:
            >>> config = DataSourceConfig(
            ...     source_type='google_sheets',
            ...     connection_params={
            ...         'credentials_path': 'credentials.json',
            ...         'spreadsheet_url': 'https://docs.google.com/...',
            ...         'default_sheet': 'Sheet1'
            ...     }
            ... )
            >>> manager = GoogleSheetsManager(config)
            >>> df = manager.read()  # 讀取預設工作表
            >>> df = manager.read(sheet_name='Sheet2')  # 讀取指定工作表
            >>> df = manager.read(sheet_name='Sheet1', range_name='A1:D10')  # 讀取指定範圍
        """
        sheet_name = kwargs.get('sheet_name') or query or self.default_sheet
        range_name = kwargs.get('range_name')

        try:
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

            self.logger.info(f"成功從工作表 '{sheet_name}' 讀取 {len(df)} 行資料")
            return df

        except Exception as e:
            self.logger.error(f"讀取工作表 '{sheet_name}' 失敗: {e}")
            raise

    def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        寫入數據到 Google Sheets（符合 DataSource 規範）

        Args:
            data: 要寫入的 pandas DataFrame
            **kwargs:
                - sheet_name: 工作表名稱（預設使用 default_sheet）
                - is_append: 是否為追加模式（True: 追加，False: 覆寫），預設為 False
                - clear_range: 在覆寫模式下，要清除的範圍（例如 'A1:Z100'），若為 None 則清除整個工作表

        Returns:
            bool: 操作是否成功

        Examples:
            >>> df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
            >>> manager.write(df)  # 覆寫預設工作表
            >>> manager.write(df, sheet_name='Sheet2')  # 寫入指定工作表
            >>> manager.write(df, sheet_name='Sheet1', is_append=True)  # 追加資料
        """
        sheet_name = kwargs.get('sheet_name', self.default_sheet)
        is_append = kwargs.get('is_append', False)
        clear_range = kwargs.get('clear_range')

        try:
            sheet = self.service.worksheet(sheet_name)

            if is_append:
                # 追加模式：處理 NaN 並轉換類型
                rows = data.fillna('').astype(object).values.tolist()
                result = sheet.append_rows(rows, value_input_option='RAW')
                self.logger.info(f"成功追加 {len(data)} 行資料到工作表 '{sheet_name}'")
            else:
                # 覆寫模式：先清除現有資料
                if clear_range:
                    sheet.batch_clear([clear_range])
                else:
                    sheet.clear()

                # 寫入 DataFrame（包含標題和資料）
                result = sheet.update(
                    [data.columns.values.tolist()] + data.values.tolist()
                )
                self.logger.info(f"成功寫入 {len(data)} 行資料到工作表 '{sheet_name}'")

            return True

        except Exception as e:
            self.logger.error(f"寫入工作表 '{sheet_name}' 失敗: {e}")
            return False

    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取 Google Sheets 資料源的元數據（符合 DataSource 規範）

        Returns:
            包含資料源元數據的字典
        """
        try:
            worksheets = self.service.worksheets()
            return {
                'source_type': 'google_sheets',
                'spreadsheet_url': self.spreadsheet_url,
                'spreadsheet_title': self.service.title,
                'credentials_path': self.credentials_path,
                'default_sheet': self.default_sheet,
                'available_sheets': [ws.title for ws in worksheets],
                'total_sheets': len(worksheets)
            }
        except Exception as e:
            self.logger.error(f"獲取元數據失敗: {e}")
            return {
                'source_type': 'google_sheets',
                'error': str(e)
            }

    # ==================== 向後兼容方法（已廢棄） ====================

    def get_data(self,
                 sheet_name: str = 'Sheet1',
                 range_name: Optional[str] = None) -> pd.DataFrame:
        """
        [已廢棄] 從指定的工作表讀取資料

        此方法已廢棄，請使用 read() 代替:
        - get_data('Sheet1') → read(sheet_name='Sheet1')
        - get_data('Sheet1', 'A1:D10') → read(sheet_name='Sheet1', range_name='A1:D10')

        Args:
            sheet_name: 工作表名稱，預設為 'Sheet1'
            range_name: 指定範圍（例如 'A1:D10'），若為 None 則讀取所有資料

        Returns:
            包含工作表資料的 pandas DataFrame
        """
        warnings.warn(
            "get_data() 已廢棄，請使用 read(sheet_name=..., range_name=...) 代替",
            DeprecationWarning,
            stacklevel=2
        )
        return self.read(sheet_name=sheet_name, range_name=range_name)

    def write_data(self,
                   df: pd.DataFrame,
                   sheet_name: str = 'Sheet1',
                   is_append: bool = False,
                   clear_range: Optional[str] = None) -> Dict[str, Any]:
        """
        [已廢棄] 將 DataFrame 寫入到指定的工作表

        此方法已廢棄，請使用 write() 代替:
        - write_data(df, 'Sheet1') → write(df, sheet_name='Sheet1')
        - write_data(df, 'Sheet1', is_append=True) → write(df, sheet_name='Sheet1', is_append=True)

        Args:
            df: 要寫入的 pandas DataFrame
            sheet_name: 工作表名稱，預設為 'Sheet1'
            is_append: 是否為追加模式（True: 追加，False: 覆寫），預設為 False
            clear_range: 在覆寫模式下，要清除的範圍（例如 'A1:Z100'），若為 None 則清除整個工作表

        Returns:
            操作結果的字典（向後兼容，實際返回簡化結果）
        """
        warnings.warn(
            "write_data() 已廢棄，請使用 write(data, sheet_name=..., is_append=...) 代替",
            DeprecationWarning,
            stacklevel=2
        )
        success = self.write(df, sheet_name=sheet_name, is_append=is_append, clear_range=clear_range)
        return {'success': success}

    # ==================== 其他輔助方法 ====================

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
            >>> df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
            >>> manager.recreate_and_write(df, 'OldSheet', 'NewSheet', rows=100, cols=10)
        """
        try:
            # 刪除舊工作表
            sheet_old = self.service.worksheet(sheet_name_old)
            self.service.del_worksheet(sheet_old)
            self.logger.info(f"已刪除舊工作表: {sheet_name_old}")

            # 建立新工作表
            new_sheet = self.service.add_worksheet(
                title=sheet_name_new,
                rows=rows,
                cols=cols
            )
            self.logger.info(f"已建立新工作表: {sheet_name_new}")

            # 寫入資料（包含標題和資料）
            result = new_sheet.update(
                [df.columns.values.tolist()] + df.values.tolist()
            )
            self.logger.info(f"已寫入 {len(df)} 行資料到新工作表 '{sheet_name_new}'")

            return {'success': True, 'result': result}

        except Exception as e:
            self.logger.error(f"recreate_and_write 失敗: {e}")
            return {'success': False, 'error': str(e)}

    def get_all_worksheets(self) -> list:
        """
        取得試算表中所有工作表的名稱

        Returns:
            工作表名稱的列表
        """
        try:
            worksheets = [worksheet.title for worksheet in self.service.worksheets()]
            self.logger.debug(f"獲取到 {len(worksheets)} 個工作表")
            return worksheets
        except Exception as e:
            self.logger.error(f"獲取工作表列表失敗: {e}")
            return []

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
        try:
            worksheet = self.service.add_worksheet(
                title=sheet_name,
                rows=rows,
                cols=cols
            )
            self.logger.info(f"已建立新工作表: {sheet_name} ({rows}x{cols})")
            return worksheet
        except Exception as e:
            self.logger.error(f"建立工作表 '{sheet_name}' 失敗: {e}")
            raise

    def delete_worksheet(self, sheet_name: str) -> None:
        """
        刪除指定的工作表

        Args:
            sheet_name: 要刪除的工作表名稱
        """
        try:
            worksheet = self.service.worksheet(sheet_name)
            self.service.del_worksheet(worksheet)
            self.logger.info(f"已刪除工作表: {sheet_name}")
        except Exception as e:
            self.logger.error(f"刪除工作表 '{sheet_name}' 失敗: {e}")
            raise


# 使用範例
# if __name__ == "__main__":
#     from .config import DataSourceConfig
#
#     # 新方式：使用 DataSourceConfig（推薦）
#     config = DataSourceConfig(
#         source_type='google_sheets',
#         connection_params={
#             'credentials_path': 'credentials.json',
#             'spreadsheet_url': 'https://docs.google.com/spreadsheets/d/17puiAmAhM2dAm9BR7Sck1E2fwsf0v76CkpiLkVJPlxE/',
#             'default_sheet': 'Sheet1'
#         },
#         cache_enabled=True
#     )
#     manager = GoogleSheetsManager(config)
#
#     # 使用新 API
#     df = manager.read()  # 讀取預設工作表
#     df = manager.read(sheet_name='Sheet2')  # 讀取指定工作表
#
#     # 寫入資料
#     new_df = pd.DataFrame({
#         'Column1': [1, 2, 3],
#         'Column2': ['A', 'B', 'C']
#     })
#     manager.write(new_df)
#
#     # 獲取元數據
#     metadata = manager.get_metadata()
#     print(metadata)
#
#     # 舊方式：向後兼容（會顯示 DeprecationWarning）
#     # manager_old = GoogleSheetsManager(
#     #     credentials_path='credentials.json',
#     #     spreadsheet_url='https://docs.google.com/spreadsheets/d/...'
#     # )
#     # df_old = manager_old.get_data('Sheet1')  # 舊 API，仍可用但已廢棄
