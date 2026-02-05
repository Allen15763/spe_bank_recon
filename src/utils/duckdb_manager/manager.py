"""
DuckDB Manager 核心模組

高可用、可移植的 DuckDB 管理器，支援:
- 多種配置方式 (DuckDBConfig, dict, str 路徑, TOML)
- 可插拔日誌系統
- 完整的 CRUD 操作
- 資料清理與轉換
- 事務處理
"""

import duckdb
import pandas as pd
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
from pathlib import Path

from .config import DuckDBConfig
from .utils.logging import get_logger
from .utils.type_mapping import get_duckdb_dtype
from .exceptions import (
    ConnectionError,
    TableExistsError,
    TableNotFoundError,
    QueryError,
    DataValidationError,
    TransactionError,
)


class DuckDBManager:
    """
    高可用 DuckDB 管理器

    支援多種配置方式，可作為獨立模組移植到其他專案。

    Example:
        # 方式 1: 直接傳路徑
        with DuckDBManager("./data.duckdb") as db:
            df = db.query_to_df("SELECT * FROM users")

        # 方式 2: 使用配置物件
        config = DuckDBConfig(
            db_path="./data.duckdb",
            timezone="Asia/Taipei",
            log_level="DEBUG"
        )
        with DuckDBManager(config) as db:
            db.create_table_from_df("users", df)

        # 方式 3: 使用字典配置
        with DuckDBManager({"db_path": "./data.duckdb"}) as db:
            db.insert_df_into_table("users", new_users)
    """

    def __init__(
        self,
        config: Union[DuckDBConfig, Dict[str, Any], str, Path, None] = None
    ):
        """
        初始化 DuckDB 管理器

        Args:
            config: 配置，可以是:
                - DuckDBConfig 實例
                - dict 配置字典
                - str 或 Path 資料庫路徑
                - None 使用預設 :memory: 模式
        """
        self.config = self._resolve_config(config)
        self.logger = self._setup_logger()
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

        # 設定時區
        self._setup_timezone()

        # 建立連線
        self._connect()

    def _resolve_config(
        self,
        config: Union[DuckDBConfig, Dict[str, Any], str, Path, None]
    ) -> DuckDBConfig:
        """解析配置"""
        if config is None:
            return DuckDBConfig()
        if isinstance(config, DuckDBConfig):
            return config
        if isinstance(config, dict):
            return DuckDBConfig.from_dict(config)
        if isinstance(config, (str, Path)):
            return DuckDBConfig(db_path=str(config))
        raise TypeError(f"不支援的配置類型: {type(config)}")

    def _setup_logger(self):
        """設定日誌器"""
        return get_logger(
            name="duckdb_manager",
            level=self.config.log_level,
            external_logger=self.config.logger,
        )

    def _setup_timezone(self):
        """設定時區 (使用配置中的時區)"""
        import os
        import time

        timezone = self.config.timezone
        os.environ['TZ'] = timezone

        # 只在 Unix 系統上調用 tzset
        if hasattr(time, 'tzset'):
            try:
                time.tzset()
            except Exception:
                # 某些 Windows 環境可能不支援
                pass

    def _connect(self):
        """建立資料庫連線"""
        try:
            self.conn = duckdb.connect(
                self.config.db_path,
                read_only=self.config.read_only,
            )
            self.logger.info(f"成功連接到 DuckDB: {self.config.db_path}")
        except Exception as e:
            self.logger.error(f"連接資料庫失敗: {e}")
            raise ConnectionError(self.config.db_path, str(e))

    # ========== CRUD 操作 ==========

    def create_table_from_df(
        self,
        table_name: str,
        df: pd.DataFrame,
        if_exists: str = 'fail'
    ) -> bool:
        """
        從 DataFrame 建立表格

        Args:
            table_name: 表格名稱
            df: pandas DataFrame
            if_exists: 'fail' (報錯), 'replace' (替換), 'append' (附加)

        Returns:
            bool: 是否成功建立
        """
        try:
            if self.config.enable_query_logging:
                self.logger.info(f"開始建立表格 '{table_name}'，模式: {if_exists}")

            # 檢查表格是否已存在
            table_exists = self._table_exists(table_name)

            if table_exists:
                self.logger.debug(f"表格 '{table_name}' 已存在")
                if if_exists == 'fail':
                    raise TableExistsError(table_name)
                elif if_exists == 'replace':
                    self.logger.warning(f"替換現有表格 '{table_name}'")
                    self.conn.sql(f'DROP TABLE IF EXISTS "{table_name}"')
                elif if_exists == 'append':
                    self.logger.info(f"將資料附加到現有表格 '{table_name}'")
                    return self.insert_df_into_table(table_name, df)

            # 建立欄位定義
            columns_with_types = []
            for col in df.columns:
                dtype_str = str(df[col].dtype)
                duckdb_dtype = get_duckdb_dtype(dtype_str)
                columns_with_types.append(f'"{col}" {duckdb_dtype}')
                self.logger.debug(f"欄位 '{col}': {dtype_str} -> {duckdb_dtype}")

            columns_sql = ", ".join(columns_with_types)

            # 建立表格
            self.conn.sql(f'CREATE TABLE "{table_name}" ({columns_sql})')
            self.logger.debug(f"表格結構建立完成: {columns_sql}")

            # 插入資料
            self.conn.sql(f'INSERT INTO "{table_name}" SELECT * FROM df')

            self.logger.info(
                f"成功建立表格 '{table_name}'，插入 {len(df):,} 筆資料"
            )
            return True

        except TableExistsError:
            raise
        except Exception as e:
            self.logger.error(f"建立表格 '{table_name}' 失敗: {e}")
            return False

    def insert_df_into_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """插入資料到現有表格"""
        try:
            if not self._table_exists(table_name):
                raise TableNotFoundError(table_name)

            self.conn.sql(f'INSERT INTO "{table_name}" SELECT * FROM df')
            self.logger.info(f"成功插入 {len(df):,} 筆資料到 '{table_name}'")
            return True

        except TableNotFoundError:
            raise
        except Exception as e:
            self.logger.error(f"插入資料到 '{table_name}' 失敗: {e}")
            return False

    def upsert_df_into_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        key_columns: List[str]
    ) -> bool:
        """
        更新或插入資料 (upsert)

        Args:
            table_name: 表格名稱
            df: 要插入的資料
            key_columns: 用於判斷重複的欄位
        """
        try:
            self.logger.info(
                f"開始 upsert 操作到 '{table_name}'，使用鍵: {key_columns}"
            )

            # 先刪除重複的記錄
            key_conditions = []
            for key_col in key_columns:
                unique_values = df[key_col].unique()
                if len(unique_values) > 0:
                    values_str = "', '".join(str(v) for v in unique_values)
                    key_conditions.append(f'"{key_col}" IN (\'{values_str}\')')

            if key_conditions:
                where_clause = " AND ".join(key_conditions)
                deleted_result = self.conn.sql(
                    f'SELECT COUNT(*) as count FROM "{table_name}" '
                    f'WHERE {where_clause}'
                ).df()
                deleted_count = (
                    deleted_result.iloc[0]['count']
                    if not deleted_result.empty else 0
                )

                self.conn.sql(f'DELETE FROM "{table_name}" WHERE {where_clause}')
                self.logger.info(f"刪除了 {deleted_count} 筆重複記錄")

            # 插入新資料
            result = self.insert_df_into_table(table_name, df)
            if result:
                self.logger.info("Upsert 操作完成")
            return result

        except Exception as e:
            self.logger.error(f"Upsert 操作失敗: {e}")
            return False

    def query_to_df(self, query: str) -> Optional[pd.DataFrame]:
        """
        執行查詢並返回 DataFrame

        Args:
            query: SQL 查詢語句

        Returns:
            DataFrame 或 None (查詢失敗時)
        """
        try:
            if self.config.enable_query_logging:
                self.logger.debug(f"執行查詢: {query[:100]}...")
            result = self.conn.sql(query).df()
            self.logger.debug(f"查詢返回 {len(result)} 筆記錄")
            return result
        except Exception as e:
            self.logger.error(f"查詢失敗: {e}")
            return None

    def delete_data(self, query: str) -> bool:
        """
        執行 DELETE 語句

        Args:
            query: DELETE SQL 語句

        Returns:
            bool: 是否成功
        """
        try:
            if self.config.enable_query_logging:
                self.logger.debug(f"執行刪除: {query[:100]}...")
            self.conn.sql(query)
            self.logger.debug("成功刪除資料")
            return True
        except Exception as e:
            self.logger.error(f"刪除失敗: {e}")
            return False

    # ========== 表格管理 ==========

    def show_tables(self) -> Optional[pd.DataFrame]:
        """顯示所有表格"""
        self.logger.debug("獲取所有表格列表")
        return self.query_to_df("SHOW TABLES")

    def describe_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """描述表格結構"""
        self.logger.debug(f"獲取表格 '{table_name}' 的結構")
        return self.query_to_df(f'DESCRIBE "{table_name}"')

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """獲取表格詳細資訊"""
        try:
            self.logger.debug(f"獲取表格 '{table_name}' 的詳細資訊")

            row_count = self.conn.sql(
                f'SELECT COUNT(*) as count FROM "{table_name}"'
            ).df().iloc[0]['count']
            schema = self.describe_table(table_name)

            info = {
                'table_name': table_name,
                'row_count': row_count,
                'columns': (
                    schema['column_name'].tolist()
                    if schema is not None else []
                ),
                'schema': schema
            }

            self.logger.info(
                f"表格 '{table_name}' 包含 {row_count:,} 筆記錄，"
                f"{len(info['columns'])} 個欄位"
            )
            return info

        except Exception as e:
            self.logger.error(f"獲取表格 '{table_name}' 資訊失敗: {e}")
            return {}

    def drop_table(
        self,
        table_name: str,
        if_exists: bool = True,
        confirm: bool = True
    ) -> bool:
        """
        刪除表格

        Args:
            table_name: 表格名稱
            if_exists: 如果為 True，表格不存在時不報錯
            confirm: 是否記錄確認日誌
        """
        try:
            table_exists = self._table_exists(table_name)

            if not table_exists and not if_exists:
                self.logger.error(f"表格 '{table_name}' 不存在")
                return False
            elif not table_exists and if_exists:
                self.logger.warning(f"表格 '{table_name}' 不存在，無需刪除")
                return True

            # 獲取表格資訊用於日誌
            table_info = self.get_table_info(table_name)
            row_count = table_info.get('row_count', 0)

            if confirm:
                self.logger.warning(
                    f"即將刪除表格 '{table_name}' (包含 {row_count:,} 筆資料)"
                )

            # 執行刪除
            drop_sql = (
                f'DROP TABLE {"IF EXISTS " if if_exists else ""}"{table_name}"'
            )
            self.conn.sql(drop_sql)

            self.logger.info(
                f"成功刪除表格 '{table_name}' (原有 {row_count:,} 筆資料)"
            )
            return True

        except Exception as e:
            self.logger.error(f"刪除表格 '{table_name}' 失敗: {e}")
            return False

    def truncate_table(self, table_name: str) -> bool:
        """清空表格資料但保留結構"""
        try:
            if not self._table_exists(table_name):
                self.logger.error(f"表格 '{table_name}' 不存在")
                return False

            row_count = self.conn.sql(
                f'SELECT COUNT(*) as count FROM "{table_name}"'
            ).df().iloc[0]['count']

            self.conn.sql(f'DELETE FROM "{table_name}"')

            self.logger.info(
                f"成功清空表格 '{table_name}' (刪除了 {row_count:,} 筆資料)"
            )
            return True

        except Exception as e:
            self.logger.error(f"清空表格 '{table_name}' 失敗: {e}")
            return False

    def backup_table(
        self,
        table_name: str,
        backup_format: str = 'parquet',
        backup_path: str = None
    ) -> bool:
        """
        備份表格資料

        Args:
            table_name: 表格名稱
            backup_format: 備份格式 ('parquet', 'csv', 'json')
            backup_path: 備份檔案路徑
        """
        try:
            if backup_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f"{table_name}_backup_{timestamp}.{backup_format}"

            if not self._table_exists(table_name):
                self.logger.error(f"表格 '{table_name}' 不存在")
                return False

            # 執行備份
            if backup_format.lower() == 'parquet':
                self.conn.sql(
                    f"COPY (SELECT * FROM \"{table_name}\") "
                    f"TO '{backup_path}' (FORMAT PARQUET)"
                )
            elif backup_format.lower() == 'csv':
                self.conn.sql(
                    f"COPY (SELECT * FROM \"{table_name}\") "
                    f"TO '{backup_path}' (FORMAT CSV, HEADER)"
                )
            elif backup_format.lower() == 'json':
                self.conn.sql(
                    f"COPY (SELECT * FROM \"{table_name}\") "
                    f"TO '{backup_path}' (FORMAT JSON)"
                )
            else:
                raise ValueError(f"不支援的備份格式: {backup_format}")

            table_info = self.get_table_info(table_name)
            row_count = table_info.get('row_count', 0)

            self.logger.info(
                f"成功備份表格 '{table_name}' 到 '{backup_path}' "
                f"({row_count:,} 筆資料)"
            )
            return True

        except Exception as e:
            self.logger.error(f"備份表格 '{table_name}' 失敗: {e}")
            return False

    # ========== 資料清理與轉換 ==========

    def alter_column_type(
        self,
        table_name: str,
        column_name: str,
        new_type: str,
        validate_conversion: bool = True
    ) -> bool:
        """
        修改表格欄位的資料型態

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            new_type: 新的資料型態 (如 'BIGINT', 'VARCHAR', 'DOUBLE' 等)
            validate_conversion: 是否先驗證資料能否轉換
        """
        try:
            self.logger.info(
                f"開始修改表格 '{table_name}' 的欄位 '{column_name}' "
                f"型態為 {new_type}"
            )

            if validate_conversion:
                self.logger.debug(
                    f"驗證 '{column_name}' 欄位資料是否能轉換為 {new_type}"
                )

                if new_type.upper() in ['BIGINT', 'INTEGER', 'DOUBLE', 'REAL']:
                    validation_query = f"""
                    SELECT COUNT(*) as invalid_count
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    AND TRY_CAST("{column_name}" AS {new_type}) IS NULL
                    """

                    invalid_result = self.conn.sql(validation_query).df()
                    invalid_count = (
                        invalid_result.iloc[0]['invalid_count']
                        if not invalid_result.empty else 0
                    )

                    if invalid_count > 0:
                        sample_query = f"""
                        SELECT "{column_name}" as invalid_value
                        FROM "{table_name}"
                        WHERE "{column_name}" IS NOT NULL
                        AND TRY_CAST("{column_name}" AS {new_type}) IS NULL
                        LIMIT 5
                        """
                        samples = self.conn.sql(sample_query).df()
                        self.logger.error(
                            f"發現 {invalid_count} 筆無法轉換的資料，"
                            f"範例: {samples['invalid_value'].tolist()}"
                        )
                        return False

                    self.logger.info(f"所有資料都能成功轉換為 {new_type}")

            # 執行欄位型態修改
            alter_query = (
                f'ALTER TABLE "{table_name}" '
                f'ALTER COLUMN "{column_name}" TYPE {new_type}'
            )
            self.conn.sql(alter_query)

            self.logger.info(f"成功修改欄位 '{column_name}' 型態為 {new_type}")

            # 驗證修改結果
            schema = self.describe_table(table_name)
            if schema is not None:
                column_info = schema[schema['column_name'] == column_name]
                if not column_info.empty:
                    actual_type = column_info.iloc[0]['column_type']
                    self.logger.info(
                        f"確認: 欄位 '{column_name}' 目前型態為 {actual_type}"
                    )

            return True

        except Exception as e:
            self.logger.error(f"修改欄位型態失敗: {e}")
            return False

    def clean_numeric_column(
        self,
        table_name: str,
        column_name: str,
        remove_chars: List[str] = None,
        preview_only: bool = False
    ) -> bool:
        """
        清理數字欄位中的非數字字符

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            remove_chars: 要移除的字符列表
            preview_only: 僅預覽清理結果，不實際執行更新
        """
        try:
            if remove_chars is None:
                # 常見的千分位符號和貨幣符號
                remove_chars = [',', '$', '€', '¥', ' ', '￥', '₩', '£']

            self.logger.info(f"開始清理表格 '{table_name}' 的欄位 '{column_name}'")
            self.logger.debug(f"將移除字符: {remove_chars}")

            # 檢查需要清理的資料數量
            check_conditions = [
                f'"{column_name}" LIKE \'%{char}%\''
                for char in remove_chars
            ]

            check_query = f"""
            SELECT COUNT(*) as dirty_count
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            """

            dirty_result = self.conn.sql(check_query).df()
            dirty_count = (
                dirty_result.iloc[0]['dirty_count']
                if not dirty_result.empty else 0
            )

            if dirty_count == 0:
                self.logger.info(f"欄位 '{column_name}' 無需清理")
                return True

            self.logger.info(f"發現 {dirty_count} 筆需要清理的資料")

            # 建立清理邏輯
            cleaned_expression = f'"{column_name}"'
            for char in remove_chars:
                cleaned_expression = (
                    f"REPLACE({cleaned_expression}, '{char}', '')"
                )

            # 預覽範例
            sample_query = f"""
            SELECT
                "{column_name}" as original_value,
                {cleaned_expression} as cleaned_value
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            LIMIT 10
            """

            sample_result = self.conn.sql(sample_query).df()

            self.logger.info("清理範例:")
            for _, row in sample_result.iterrows():
                self.logger.info(
                    f"  '{row['original_value']}' → '{row['cleaned_value']}'"
                )

            if preview_only:
                self.logger.info("預覽模式：未執行實際更新")
                return True

            # 執行清理
            update_query = f"""
            UPDATE "{table_name}"
            SET "{column_name}" = {cleaned_expression}
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            """

            self.conn.sql(update_query)

            # 驗證清理結果
            verify_query = f"""
            SELECT COUNT(*) as remaining_dirty
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            """

            verify_result = self.conn.sql(verify_query).df()
            remaining_dirty = (
                verify_result.iloc[0]['remaining_dirty']
                if not verify_result.empty else 0
            )

            if remaining_dirty == 0:
                self.logger.info(f"成功清理 {dirty_count} 筆資料")
            else:
                self.logger.warning(
                    f"清理完成，但仍有 {remaining_dirty} 筆資料"
                    "可能需要額外處理"
                )

            return True

        except Exception as e:
            self.logger.error(f"清理數據失敗: {e}")
            return False

    def clean_and_convert_column(
        self,
        table_name: str,
        column_name: str,
        target_type: str,
        remove_chars: List[str] = None,
        handle_empty_as_null: bool = True
    ) -> bool:
        """
        清理並轉換欄位型態的一站式方法

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            target_type: 目標資料型態
            remove_chars: 要移除的字符列表
            handle_empty_as_null: 是否將空字串轉換為 NULL
        """
        try:
            self.logger.info(
                f"開始清理並轉換欄位 '{column_name}' 為 {target_type}"
            )

            # Step 1: 清理數據
            clean_success = self.clean_numeric_column(
                table_name=table_name,
                column_name=column_name,
                remove_chars=remove_chars,
                preview_only=False
            )

            if not clean_success:
                return False

            # Step 2: 處理空字串
            if handle_empty_as_null:
                empty_query = f"""
                UPDATE "{table_name}"
                SET "{column_name}" = NULL
                WHERE "{column_name}" = '' OR "{column_name}" = ' '
                """
                self.conn.sql(empty_query)
                self.logger.debug("已將空字串轉換為 NULL")

            # Step 3: 最終驗證
            validation_success = self._validate_conversion(
                table_name, column_name, target_type
            )
            if not validation_success:
                return False

            # Step 4: 執行型態轉換
            conversion_success = self.alter_column_type(
                table_name=table_name,
                column_name=column_name,
                new_type=target_type,
                validate_conversion=False  # 已經驗證過了
            )

            if conversion_success:
                self.logger.info(
                    f"成功完成清理和轉換！"
                    f"欄位 '{column_name}' 現在是 {target_type} 型態"
                )

            return conversion_success

        except Exception as e:
            self.logger.error(f"清理和轉換過程失敗: {e}")
            return False

    def _validate_conversion(
        self,
        table_name: str,
        column_name: str,
        target_type: str
    ) -> bool:
        """內部方法：驗證清理後的資料是否能成功轉換"""
        try:
            if target_type.upper() in ['BIGINT', 'INTEGER', 'DOUBLE', 'REAL']:
                validation_query = f"""
                SELECT COUNT(*) as invalid_count
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                AND TRY_CAST("{column_name}" AS {target_type}) IS NULL
                """

                invalid_result = self.conn.sql(validation_query).df()
                invalid_count = (
                    invalid_result.iloc[0]['invalid_count']
                    if not invalid_result.empty else 0
                )

                if invalid_count > 0:
                    sample_query = f"""
                    SELECT "{column_name}" as problematic_value
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    AND TRY_CAST("{column_name}" AS {target_type}) IS NULL
                    LIMIT 5
                    """
                    samples = self.conn.sql(sample_query).df()
                    self.logger.error(
                        f"清理後仍有 {invalid_count} 筆無法轉換的資料"
                    )
                    self.logger.error(
                        f"範例: {samples['problematic_value'].tolist()}"
                    )
                    return False

                self.logger.info(
                    f"清理後所有資料都能成功轉換為 {target_type}"
                )

            return True

        except Exception as e:
            self.logger.error(f"驗證轉換失敗: {e}")
            return False

    def preview_column_values(
        self,
        table_name: str,
        column_name: str,
        limit: int = 20,
        show_unique: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        預覽欄位的值，用於了解資料格式

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            limit: 顯示筆數限制
            show_unique: 是否只顯示唯一值
        """
        try:
            if show_unique:
                query = f"""
                SELECT DISTINCT "{column_name}" as value, COUNT(*) as count
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                GROUP BY "{column_name}"
                ORDER BY count DESC
                LIMIT {limit}
                """
            else:
                query = f"""
                SELECT "{column_name}" as value
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                LIMIT {limit}
                """

            result = self.conn.sql(query).df()
            self.logger.info(f"欄位 '{column_name}' 的範例資料:")
            return result

        except Exception as e:
            self.logger.error(f"預覽資料失敗: {e}")
            return None

    # ========== 事務處理 ==========

    def execute_transaction(self, operations: List[str]) -> bool:
        """
        執行事務操作

        Args:
            operations: SQL 操作列表

        Returns:
            bool: 是否成功執行所有操作
        """
        try:
            self.logger.info(f"開始執行事務操作 (共 {len(operations)} 個操作)")

            # 開始事務
            self.conn.sql("BEGIN TRANSACTION")

            for i, operation in enumerate(operations, 1):
                try:
                    self.logger.debug(
                        f"執行操作 {i}/{len(operations)}: {operation[:100]}..."
                    )
                    self.conn.sql(operation)
                except Exception as e:
                    self.logger.error(f"操作 {i} 失敗: {e}")
                    self.conn.sql("ROLLBACK")
                    self.logger.error("事務已回滾")
                    raise TransactionError(i, str(e))

            # 提交事務
            self.conn.sql("COMMIT")
            self.logger.info(f"成功執行所有 {len(operations)} 個操作")
            return True

        except TransactionError:
            raise
        except Exception as e:
            self.logger.error(f"事務執行失敗: {e}")
            try:
                self.conn.sql("ROLLBACK")
                self.logger.error("事務已回滾")
            except Exception:
                pass
            return False

    def validate_data_integrity(
        self,
        table_name: str,
        checks: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        驗證資料完整性

        Args:
            table_name: 表格名稱
            checks: 自定義檢查規則 (名稱: SQL)
        """
        try:
            self.logger.info(f"開始驗證表格 '{table_name}' 的資料完整性")

            results = {
                'table_name': table_name,
                'total_rows': 0,
                'null_counts': {},
                'duplicate_rows': 0,
                'data_types': {},
                'custom_checks': {}
            }

            # 基本統計
            total_rows = self.conn.sql(
                f'SELECT COUNT(*) as count FROM "{table_name}"'
            ).df().iloc[0]['count']
            results['total_rows'] = total_rows

            # 檢查每個欄位的NULL值
            schema = self.describe_table(table_name)
            if schema is not None:
                for _, col_info in schema.iterrows():
                    col_name = col_info['column_name']
                    null_count = self.conn.sql(
                        f'SELECT COUNT(*) as count FROM "{table_name}" '
                        f'WHERE "{col_name}" IS NULL'
                    ).df().iloc[0]['count']
                    results['null_counts'][col_name] = null_count
                    results['data_types'][col_name] = col_info['column_type']

            # 檢查重複行
            duplicate_count = self.conn.sql(f'''
                SELECT COUNT(*) as count FROM (
                    SELECT COUNT(*) as row_count
                    FROM "{table_name}"
                    GROUP BY *
                    HAVING COUNT(*) > 1
                )
            ''').df().iloc[0]['count']
            results['duplicate_rows'] = duplicate_count

            # 自定義檢查
            if checks:
                for check_name, check_sql in checks.items():
                    try:
                        check_result = self.conn.sql(
                            check_sql.format(table_name=table_name)
                        ).df()
                        results['custom_checks'][check_name] = (
                            check_result.to_dict('records')
                        )
                    except Exception as e:
                        results['custom_checks'][check_name] = f"Error: {e}"

            self.logger.info("完成資料完整性驗證")
            return results

        except Exception as e:
            self.logger.error(f"資料完整性驗證失敗: {e}")
            return {}

    # ========== 輔助方法 ==========

    def _table_exists(self, table_name: str) -> bool:
        """檢查表格是否存在"""
        existing_tables = self.conn.sql("SHOW TABLES").df()
        return (
            table_name in existing_tables['name'].values
            if not existing_tables.empty else False
        )

    def close(self):
        """關閉資料庫連接"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("資料庫連接已關閉")

    def __enter__(self):
        """Context manager 入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 出口"""
        self.close()

    def __repr__(self) -> str:
        return f"DuckDBManager(db_path='{self.config.db_path}')"
