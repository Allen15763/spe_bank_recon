"""
DuckDB Manager Mixin 基類

定義所有操作 Mixin 的基礎介面和共用方法。
"""

from typing import TYPE_CHECKING, Optional
import pandas as pd

if TYPE_CHECKING:
    import duckdb
    from ..config import DuckDBConfig


class OperationMixin:
    """
    操作 Mixin 基類

    所有操作 Mixin 都應繼承此類。
    此類定義了 Mixin 所需的屬性類型提示，
    這些屬性由主類 DuckDBManager 提供。

    Attributes:
        conn: DuckDB 連線物件
        config: DuckDBConfig 配置物件
        logger: 日誌器實例
    """

    # 類型提示 - 這些屬性由 DuckDBManager 提供
    conn: Optional["duckdb.DuckDBPyConnection"]
    config: "DuckDBConfig"
    logger: any  # 可以是 logging.Logger 或任何符合 LoggerProtocol 的物件

    def _table_exists(self, table_name: str) -> bool:
        """
        檢查表格是否存在

        Args:
            table_name: 表格名稱

        Returns:
            bool: 表格是否存在
        """
        existing_tables = self.conn.sql("SHOW TABLES").df()
        return (
            table_name in existing_tables['name'].values
            if not existing_tables.empty else False
        )

    def _execute_sql(self, sql: str, description: str = None) -> pd.DataFrame:
        """
        執行 SQL 並返回 DataFrame

        Args:
            sql: SQL 語句
            description: 操作描述（用於日誌）

        Returns:
            pd.DataFrame: 查詢結果
        """
        if self.config.enable_query_logging and description:
            self.logger.debug(f"{description}: {sql[:100]}...")
        return self.conn.sql(sql).df()

    def _execute_sql_no_return(self, sql: str, description: str = None) -> None:
        """
        執行 SQL 無返回值

        Args:
            sql: SQL 語句
            description: 操作描述（用於日誌）
        """
        if self.config.enable_query_logging and description:
            self.logger.debug(f"{description}: {sql[:100]}...")
        self.conn.sql(sql)
