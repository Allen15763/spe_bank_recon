"""
DuckDB Manager 自定義異常模組
"""


class DuckDBManagerError(Exception):
    """DuckDB Manager 基礎異常類"""

    pass


class ConnectionError(DuckDBManagerError):
    """資料庫連線錯誤"""

    def __init__(self, db_path: str, message: str = None):
        self.db_path = db_path
        self.message = message or f"無法連線到資料庫: {db_path}"
        super().__init__(self.message)


class TableError(DuckDBManagerError):
    """表格操作錯誤的基礎類"""

    def __init__(self, table_name: str, message: str = None):
        self.table_name = table_name
        self.message = message or f"表格操作錯誤: {table_name}"
        super().__init__(self.message)


class TableExistsError(TableError):
    """表格已存在錯誤"""

    def __init__(self, table_name: str):
        super().__init__(
            table_name,
            f"表格 '{table_name}' 已存在"
        )


class TableNotFoundError(TableError):
    """表格不存在錯誤"""

    def __init__(self, table_name: str):
        super().__init__(
            table_name,
            f"表格 '{table_name}' 不存在"
        )


class QueryError(DuckDBManagerError):
    """查詢執行錯誤"""

    def __init__(self, query: str, original_error: Exception = None):
        self.query = query
        self.original_error = original_error
        truncated_query = query[:200] + "..." if len(query) > 200 else query
        message = f"查詢執行失敗: {truncated_query}"
        if original_error:
            message += f"\n原始錯誤: {original_error}"
        super().__init__(message)


class DataValidationError(DuckDBManagerError):
    """資料驗證錯誤"""

    def __init__(self, column_name: str, expected_type: str, invalid_count: int):
        self.column_name = column_name
        self.expected_type = expected_type
        self.invalid_count = invalid_count
        super().__init__(
            f"欄位 '{column_name}' 有 {invalid_count} 筆資料"
            f"無法轉換為 {expected_type}"
        )


class TransactionError(DuckDBManagerError):
    """事務處理錯誤"""

    def __init__(self, operation_index: int, message: str = None):
        self.operation_index = operation_index
        self.message = message or f"事務在第 {operation_index} 個操作失敗"
        super().__init__(self.message)


class ConfigurationError(DuckDBManagerError):
    """配置錯誤"""

    def __init__(self, config_key: str, message: str = None):
        self.config_key = config_key
        self.message = message or f"配置錯誤: {config_key}"
        super().__init__(self.message)
