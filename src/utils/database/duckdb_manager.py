"""
DuckDB Manager 向後相容層

此模組已重構為獨立可移植模組 `src.utils.duckdb_manager`。
為保持向後相容性，此檔案從新模組重新導出所有公開接口。

新專案請直接使用:
    from src.utils.duckdb_manager import DuckDBManager, DuckDBConfig

此檔案將在未來版本移除。
"""

import warnings
from typing import Optional
import pandas as pd

# 從新模組導入 (使用別名避免命名衝突)
from src.utils.duckdb_manager import (
    DuckDBManager as _BaseDuckDBManager,
    DuckDBConfig,
    DuckDBManagerError,
    ConnectionError,
    TableError,
    TableExistsError,
    TableNotFoundError,
    QueryError,
    DataValidationError,
    TransactionError,
    ConfigurationError,
)

# 為專案整合提供帶專案日誌的包裝
def _get_project_logger():
    """嘗試獲取專案日誌器"""
    try:
        from src.utils.logging import get_logger
        return get_logger('database.duckdb')
    except ImportError:
        return None


class DuckDBManager(_BaseDuckDBManager):
    """
    向後相容的 DuckDBManager

    支援舊版的 db_path 關鍵字參數，同時整合專案日誌系統。
    此類是對新版 DuckDBManager 的包裝，提供完全相同的接口。

    新專案建議使用:
        from src.utils.duckdb_manager import DuckDBManager
    """

    def __init__(self, db_path: str = ":memory:"):
        """
        初始化 DuckDB 管理器

        Args:
            db_path: 資料庫路徑，默認為內存模式 ":memory:"
        """
        # 嘗試獲取專案日誌器
        project_logger = _get_project_logger()

        # 建立配置
        config = DuckDBConfig(
            db_path=db_path,
            logger=project_logger,
            timezone="Asia/Taipei",
        )

        # 調用父類初始化
        super().__init__(config)


# 為向後相容保留別名
ProjectDuckDBManager = DuckDBManager

# ========== 向後相容的便利函數 (已棄用) ==========

def create_table(
    table_name: str,
    df: pd.DataFrame,
    db_path: str = "bank_statements.duckdb",
    _log_file: str = "duckdb_operations.log",  # unused, kept for compatibility
    _log_level: str = "DEBUG"  # unused, kept for compatibility
) -> Optional[dict]:
    """
    建立表格的便利函數

    .. deprecated::
        此函數已棄用，請直接使用 DuckDBManager 類。

    Example:
        with DuckDBManager(db_path) as db:
            db.create_table_from_df(table_name, df)
    """
    warnings.warn(
        "create_table() 函數已棄用，請使用 DuckDBManager 類。"
        "Example: with DuckDBManager(db_path) as db: db.create_table_from_df(...)",
        DeprecationWarning,
        stacklevel=2
    )

    with DuckDBManager(db_path) as db_manager:
        success = db_manager.create_table_from_df(table_name, df)
        if success:
            info = db_manager.get_table_info(table_name)
            print(f"\n📋 表格 {table_name}:")
            print(f"   記錄數: {info.get('row_count', 0):,}")
            print(f"   欄位數: {len(info.get('columns', []))}")
            return info
        return None


def insert_table(
    table_name: str,
    df: pd.DataFrame,
    db_path: str = "bank_statements.duckdb",
    _log_file: str = "duckdb_operations.log",  # unused, kept for compatibility
    _log_level: str = "DEBUG"  # unused, kept for compatibility
) -> Optional[dict]:
    """
    插入資料的便利函數

    .. deprecated::
        此函數已棄用，請直接使用 DuckDBManager 類。
    """
    warnings.warn(
        "insert_table() 函數已棄用，請使用 DuckDBManager 類。",
        DeprecationWarning,
        stacklevel=2
    )

    with DuckDBManager(db_path) as db_manager:
        success = db_manager.insert_df_into_table(table_name, df)
        if success:
            info = db_manager.get_table_info(table_name)
            print(f"\n📋 表格 {table_name}:")
            print(f"   記錄數: {info.get('row_count', 0):,}")
            print(f"   欄位數: {len(info.get('columns', []))}")
            return info
        return None


def alter_column_dtype(
    table_name: str,
    column_name: str,
    new_type: str = "BIGINT",
    db_path: str = "bank_statements.duckdb",
    _log_file: str = "duckdb_operations.log",  # unused, kept for compatibility
    _log_level: str = "DEBUG"  # unused, kept for compatibility
) -> None:
    """
    修改欄位類型的便利函數

    .. deprecated::
        此函數已棄用，請直接使用 DuckDBManager 類。
    """
    warnings.warn(
        "alter_column_dtype() 函數已棄用，請使用 DuckDBManager 類。",
        DeprecationWarning,
        stacklevel=2
    )

    with DuckDBManager(db_path) as db_manager:
        print("=== Step 1: Preview current data ===")
        db_manager.preview_column_values(
            table_name=table_name,
            column_name=column_name,
            limit=10,
            show_unique=True
        )

        print("\n=== Step 2: Preview cleaning ===")
        db_manager.clean_numeric_column(
            table_name=table_name,
            column_name=column_name,
            remove_chars=[','],
            preview_only=True
        )

        print("\n=== Step 3: Clean and convert ===")
        success = db_manager.clean_and_convert_column(
            table_name=table_name,
            column_name=column_name,
            target_type=new_type,
            remove_chars=[','],
            handle_empty_as_null=True
        )

        if success:
            print("🎉 Success! Let's verify the result:")
            schema = db_manager.describe_table(table_name)
            if schema is not None:
                print(schema[schema['column_name'] == column_name])


def drop_table(
    table_name: str,
    db_path: str = "bank_statements.duckdb",
    _log_file: str = "duckdb_operations.log",  # unused, kept for compatibility
    _log_level: str = "DEBUG"  # unused, kept for compatibility
) -> None:
    """
    刪除表格的便利函數

    .. deprecated::
        此函數已棄用，請直接使用 DuckDBManager 類。
    """
    warnings.warn(
        "drop_table() 函數已棄用，請使用 DuckDBManager 類。",
        DeprecationWarning,
        stacklevel=2
    )

    with DuckDBManager(db_path) as db_manager:
        db_manager.drop_table(table_name)


def backup_table(
    table_name: str,
    db_path: str = "bank_statements.duckdb",
    _log_file: str = "duckdb_operations.log",  # unused, kept for compatibility
    _log_level: str = "DEBUG",  # unused, kept for compatibility
    backup_format: str = 'parquet',
    backup_path: str = None
) -> None:
    """
    備份表格的便利函數

    .. deprecated::
        此函數已棄用，請直接使用 DuckDBManager 類。
    """
    warnings.warn(
        "backup_table() 函數已棄用，請使用 DuckDBManager 類。",
        DeprecationWarning,
        stacklevel=2
    )

    with DuckDBManager(db_path) as db_manager:
        db_manager.backup_table(
            table_name=table_name,
            backup_format=backup_format,
            backup_path=backup_path
        )


# ========== 導出列表 ==========

__all__ = [
    # 新模組類
    "DuckDBManager",
    "DuckDBConfig",
    "ProjectDuckDBManager",
    # 異常類
    "DuckDBManagerError",
    "ConnectionError",
    "TableError",
    "TableExistsError",
    "TableNotFoundError",
    "QueryError",
    "DataValidationError",
    "TransactionError",
    "ConfigurationError",
    # 棄用函數 (向後相容)
    "create_table",
    "insert_table",
    "alter_column_dtype",
    "drop_table",
    "backup_table",
]


if __name__ == "__main__":
    print("DuckDB Manager 向後相容層")
    print("建議使用: from src.utils.duckdb_manager import DuckDBManager")
