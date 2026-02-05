import duckdb
import pandas as pd
from typing import Optional, Dict, Any
from datetime import datetime
import time
import os

from src.utils.logging import get_logger


class DuckDBManager:
    """
    DuckDB 資料庫管理器

    使用項目統一的日誌系統進行日誌記錄
    """

    def __init__(
        self,
        db_path: str = ":memory:"
    ):
        """
        初始化 DuckDB 管理器

        Args:
            db_path: 資料庫路徑，默認為內存模式 ":memory:"
        """
        os.environ['TZ'] = 'Asia/Taipei'
        if hasattr(time, 'tzset'):
            os.environ['TZ'] = 'America/New_York'  # Example timezone
            time.tzset()
        else:
            print("time.tzset() is not available on this platform. Timezone changes may not take effect.")

        self.db_path = db_path
        self.conn = None

        # 使用項目統一的日誌系統
        self.logger = get_logger('database.duckdb')

        self._connect()

    def _connect(self):
        """建立資料庫連接"""
        try:
            self.conn = duckdb.connect(self.db_path)
            self.logger.info(f"成功連接到 DuckDB: {self.db_path}")
        except Exception as e:
            self.logger.error(f"連接資料庫失敗: {e}")
            raise

    def _get_duckdb_dtype(self, pandas_dtype: str) -> str:
        """更完整的型態映射"""
        dtype_mapping = {
            'object': 'VARCHAR',
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'float64': 'DOUBLE',
            'float32': 'REAL',
            'datetime64[ns]': 'TIMESTAMP',
            'timedelta64[ns]': 'INTERVAL',
            'bool': 'BOOLEAN',
            'category': 'VARCHAR',
            'string': 'VARCHAR'
        }

        # 處理複雜的 datetime 格式
        if 'datetime64' in pandas_dtype:
            return 'TIMESTAMP'

        return dtype_mapping.get(pandas_dtype, 'VARCHAR')

    def create_table_from_df(self, table_name: str, df: pd.DataFrame,
                             if_exists: str = 'fail') -> bool:
        """
        從 DataFrame 建立表格

        Args:
            table_name: 表格名稱
            df: pandas DataFrame
            if_exists: 'fail', 'replace', 'append'

        Returns:
            bool: 是否成功建立
        """
        try:
            # 記錄開始操作
            self.logger.info(f"開始建立表格 '{table_name}'，模式: {if_exists}")

            # 檢查表格是否已存在
            existing_tables = self.conn.sql("SHOW TABLES").df()
            table_exists = table_name in existing_tables['name'].values if not existing_tables.empty else False

            if table_exists:
                self.logger.debug(f"表格 '{table_name}' 已存在")
                if if_exists == 'fail':
                    raise ValueError(f"表格 {table_name} 已存在")
                elif if_exists == 'replace':
                    self.logger.warning(f"替換現有表格 '{table_name}'")
                    self.conn.sql(f'DROP TABLE IF EXISTS "{table_name}"')
                elif if_exists == 'append':
                    # 直接插入資料到現有表格
                    self.logger.info(f"將資料附加到現有表格 '{table_name}'")
                    return self.insert_df_into_table(table_name, df)

            # 建立欄位定義
            columns_with_types = []
            for col in df.columns:
                dtype_str = str(df[col].dtype)
                duckdb_dtype = self._get_duckdb_dtype(dtype_str)
                columns_with_types.append(f'"{col}" {duckdb_dtype}')
                self.logger.debug(f"欄位 '{col}': {dtype_str} -> {duckdb_dtype}")

            columns_sql = ", ".join(columns_with_types)

            # 建立表格
            self.conn.sql(f'CREATE TABLE "{table_name}" ({columns_sql})')
            self.logger.debug(f"表格結構建立完成: {columns_sql}")

            # 插入資料
            self.conn.sql(f'INSERT INTO "{table_name}" SELECT * FROM df')

            self.logger.info(f"✅ 成功建立表格 '{table_name}'，插入 {len(df):,} 筆資料")
            return True

        except Exception as e:
            self.logger.error(f"❌ 建立表格 '{table_name}' 失敗: {e}")
            return False

    def insert_df_into_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """插入資料到現有表格"""
        try:
            # 檢查表格是否存在
            existing_tables = self.conn.sql("SHOW TABLES").df()
            if existing_tables.empty or table_name not in existing_tables['name'].values:
                raise ValueError(f"表格 {table_name} 不存在")

            self.conn.sql(f'INSERT INTO "{table_name}" SELECT * FROM df')
            self.logger.info(f"✅ 成功插入 {len(df):,} 筆資料到 '{table_name}'")
            return True

        except Exception as e:
            self.logger.error(f"❌ 插入資料到 '{table_name}' 失敗: {e}")
            return False

    def upsert_df_into_table(self, table_name: str, df: pd.DataFrame,
                             key_columns: list) -> bool:
        """
        更新或插入資料 (upsert)

        Args:
            table_name: 表格名稱
            df: 要插入的資料
            key_columns: 用於判斷重複的欄位
        """
        try:
            self.logger.info(f"開始 upsert 操作到 '{table_name}'，使用鍵: {key_columns}")

            # 先刪除重複的記錄
            key_conditions = []
            for key_col in key_columns:
                unique_values = df[key_col].unique()
                if len(unique_values) > 0:
                    values_str = "', '".join(str(v) for v in unique_values)
                    key_conditions.append(f'"{key_col}" IN (\'{values_str}\')')

            if key_conditions:
                where_clause = " AND ".join(key_conditions)
                deleted_result = \
                    self.conn.sql(f'SELECT COUNT(*) as count FROM "{table_name}" WHERE {where_clause}').df()
                deleted_count = deleted_result.iloc[0]['count'] if not deleted_result.empty else 0

                self.conn.sql(f'DELETE FROM "{table_name}" WHERE {where_clause}')
                self.logger.info(f"刪除了 {deleted_count} 筆重複記錄")

            # 插入新資料
            result = self.insert_df_into_table(table_name, df)
            if result:
                self.logger.info("✅ Upsert 操作完成")
            return result

        except Exception as e:
            self.logger.error(f"❌ Upsert 操作失敗: {e}")
            return False

    def query_to_df(self, query: str) -> Optional[pd.DataFrame]:
        """執行查詢並返回 DataFrame"""
        try:
            self.logger.debug(f"執行查詢: {query[:100]}...")
            result = self.conn.sql(query).df()
            self.logger.debug(f"查詢返回 {len(result)} 筆記錄")
            return result
        except Exception as e:
            self.logger.error(f"❌ 查詢失敗: {e}")
            return None

    def delete_data(self, query: str):
        """執行DELETE"""
        try:
            self.logger.debug(f"執行查詢: {query[:100]}...")
            self.conn.sql(query)
            self.logger.debug("Successfuly deleted")
        except Exception as e:
            self.logger.error(f"❌ Failed to delete: {e}")

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

            row_count = self.conn.sql(f'SELECT COUNT(*) as count FROM "{table_name}"').df().iloc[0]['count']
            schema = self.describe_table(table_name)

            info = {
                'table_name': table_name,
                'row_count': row_count,
                'columns': schema['column_name'].tolist() if schema is not None else [],
                'schema': schema
            }

            self.logger.info(f"表格 '{table_name}' 包含 {row_count:,} 筆記錄，{len(info['columns'])} 個欄位")
            return info

        except Exception as e:
            self.logger.error(f"❌ 獲取表格 '{table_name}' 資訊失敗: {e}")
            return {}

    def alter_column_type(self, table_name: str, column_name: str, new_type: str,
                          validate_conversion: bool = True) -> bool:
        """
        修改表格欄位的資料型態

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            new_type: 新的資料型態 (如 'BIGINT', 'VARCHAR', 'DOUBLE' 等)
            validate_conversion: 是否先驗證資料能否轉換

        Returns:
            bool: 是否成功修改
        """
        try:
            self.logger.info(f"開始修改表格 '{table_name}' 的欄位 '{column_name}' 型態為 {new_type}")

            # 先驗證資料是否能轉換 (如果要求的話)
            if validate_conversion:
                self.logger.debug(f"驗證 '{column_name}' 欄位資料是否能轉換為 {new_type}")

                # 檢查是否有無法轉換的資料
                if new_type.upper() in ['BIGINT', 'INTEGER', 'DOUBLE', 'REAL']:
                    validation_query = f"""
                    SELECT COUNT(*) as invalid_count
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    AND TRY_CAST("{column_name}" AS {new_type}) IS NULL
                    """

                    invalid_result = self.conn.sql(validation_query).df()
                    invalid_count = invalid_result.iloc[0]['invalid_count'] if not invalid_result.empty else 0

                    if invalid_count > 0:
                        # 顯示一些無法轉換的範例
                        sample_query = f"""
                        SELECT "{column_name}" as invalid_value
                        FROM "{table_name}"
                        WHERE "{column_name}" IS NOT NULL
                        AND TRY_CAST("{column_name}" AS {new_type}) IS NULL
                        LIMIT 5
                        """
                        samples = self.conn.sql(sample_query).df()
                        self.logger.error(f"❌ 發現 {invalid_count} 筆無法轉換的資料，範例: {samples['invalid_value'].tolist()}")
                        return False

                    self.logger.info(f"✅ 所有資料都能成功轉換為 {new_type}")

            # 執行欄位型態修改
            alter_query = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {new_type}'
            self.conn.sql(alter_query)

            self.logger.info(f"✅ 成功修改欄位 '{column_name}' 型態為 {new_type}")

            # 驗證修改結果
            schema = self.describe_table(table_name)
            if schema is not None:
                column_info = schema[schema['column_name'] == column_name]
                if not column_info.empty:
                    actual_type = column_info.iloc[0]['column_type']
                    self.logger.info(f"確認: 欄位 '{column_name}' 目前型態為 {actual_type}")

            return True

        except Exception as e:
            self.logger.error(f"❌ 修改欄位型態失敗: {e}")
            return False

    def clean_numeric_column(self, table_name: str, column_name: str,
                             remove_chars: list = None,
                             preview_only: bool = False) -> bool:
        """
        清理數字欄位中的非數字字符

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            remove_chars: 要移除的字符列表，預設為 [',', '$', '€', '¥', ' ']
            preview_only: 僅預覽清理結果，不實際執行更新

        Returns:
            bool: 是否成功清理
        """
        try:
            if remove_chars is None:
                remove_chars = [',', '$', '€', '¥', ' ', '￥', '₩', '£']  # 常見的千分位符號和貨幣符號

            self.logger.info(f"開始清理表格 '{table_name}' 的欄位 '{column_name}'")
            self.logger.debug(f"將移除字符: {remove_chars}")

            # 首先檢查需要清理的資料數量
            check_conditions = []
            for char in remove_chars:
                check_conditions.append(f'"{column_name}" LIKE \'%{char}%\'')

            check_query = f"""
            SELECT COUNT(*) as dirty_count
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            """

            dirty_result = self.conn.sql(check_query).df()
            dirty_count = dirty_result.iloc[0]['dirty_count'] if not dirty_result.empty else 0

            if dirty_count == 0:
                self.logger.info(f"✅ 欄位 '{column_name}' 無需清理")
                return True

            self.logger.info(f"發現 {dirty_count} 筆需要清理的資料")

            # 顯示清理前後的範例
            sample_query = f"""
            SELECT
                "{column_name}" as original_value,
            """

            # 建立清理邏輯 - 逐步移除每個字符
            cleaned_expression = f'"{column_name}"'
            for char in remove_chars:
                cleaned_expression = f"REPLACE({cleaned_expression}, '{char}', '')"

            sample_query += f"""
                {cleaned_expression} as cleaned_value
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            AND ({' OR '.join(check_conditions)})
            LIMIT 10
            """

            sample_result = self.conn.sql(sample_query).df()

            self.logger.info("清理範例:")
            for _, row in sample_result.iterrows():
                self.logger.info(f"  '{row['original_value']}' → '{row['cleaned_value']}'")

            if preview_only:
                self.logger.info("📋 預覽模式：未執行實際更新")
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
            remaining_dirty = verify_result.iloc[0]['remaining_dirty'] if not verify_result.empty else 0

            if remaining_dirty == 0:
                self.logger.info(f"✅ 成功清理 {dirty_count} 筆資料")
            else:
                self.logger.warning(f"⚠️ 清理完成，但仍有 {remaining_dirty} 筆資料可能需要額外處理")

            return True

        except Exception as e:
            self.logger.error(f"❌ 清理數據失敗: {e}")
            return False

    def clean_and_convert_column(self, table_name: str, column_name: str,
                                 target_type: str,
                                 remove_chars: list = None,
                                 handle_empty_as_null: bool = True) -> bool:
        """
        清理並轉換欄位型態的一站式方法

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            target_type: 目標資料型態
            remove_chars: 要移除的字符列表
            handle_empty_as_null: 是否將空字串轉換為 NULL

        Returns:
            bool: 是否成功完成清理和轉換
        """
        try:
            self.logger.info(f"🧹 開始清理並轉換欄位 '{column_name}' 為 {target_type}")

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
            validation_success = self._validate_conversion(table_name, column_name, target_type)
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
                self.logger.info(f"🎉 成功完成清理和轉換！欄位 '{column_name}' 現在是 {target_type} 型態")

            return conversion_success

        except Exception as e:
            self.logger.error(f"❌ 清理和轉換過程失敗: {e}")
            return False

    def _validate_conversion(self, table_name: str, column_name: str, target_type: str) -> bool:
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
                invalid_count = invalid_result.iloc[0]['invalid_count'] if not invalid_result.empty else 0

                if invalid_count > 0:
                    # 顯示仍然無法轉換的資料
                    sample_query = f"""
                    SELECT "{column_name}" as problematic_value
                    FROM "{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    AND TRY_CAST("{column_name}" AS {target_type}) IS NULL
                    LIMIT 5
                    """
                    samples = self.conn.sql(sample_query).df()
                    self.logger.error(f"❌ 清理後仍有 {invalid_count} 筆無法轉換的資料")
                    self.logger.error(f"範例: {samples['problematic_value'].tolist()}")
                    return False

                self.logger.info(f"✅ 清理後所有資料都能成功轉換為 {target_type}")

            return True

        except Exception as e:
            self.logger.error(f"❌ 驗證轉換失敗: {e}")
            return False

    def preview_column_values(self, table_name: str, column_name: str,
                              limit: int = 20, show_unique: bool = True) -> Optional[pd.DataFrame]:
        """
        預覽欄位的值，用於了解資料格式

        Args:
            table_name: 表格名稱
            column_name: 欄位名稱
            limit: 顯示筆數限制
            show_unique: 是否只顯示唯一值

        Returns:
            DataFrame: 包含範例資料
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
            print(result)
            return result

        except Exception as e:
            self.logger.error(f"❌ 預覽資料失敗: {e}")
            return None

    def close(self):
        """關閉資料庫連接"""
        if self.conn:
            self.conn.close()
            self.logger.info("🔐 資料庫連接已關閉")

    def __enter__(self):
        """Context manager 入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 出口"""
        self.close()

    def drop_table(self, table_name: str,
                   if_exists: bool = True,
                   confirm: bool = True) -> bool:
        """
        刪除表格

        Args:
            table_name: 表格名稱
            if_exists: 如果為 True 使用 DROP TABLE IF EXISTS，避免表格不存在時報錯
            confirm: 是否需要確認操作（安全機制）

        Returns:
            bool: 是否成功刪除
        """
        try:
            # 檢查表格是否存在
            existing_tables = self.conn.sql("SHOW TABLES").df()
            table_exists = table_name in existing_tables['name'].values if not existing_tables.empty else False

            if not table_exists and not if_exists:
                self.logger.error(f"❌ 表格 '{table_name}' 不存在")
                return False
            elif not table_exists and if_exists:
                self.logger.warning(f"⚠️ 表格 '{table_name}' 不存在，無需刪除")
                return True

            # 獲取表格資訊用於日誌
            table_info = self.get_table_info(table_name)
            row_count = table_info.get('row_count', 0)

            # 確認機制
            if confirm:
                self.logger.warning(f"⚠️ 即將刪除表格 '{table_name}' (包含 {row_count:,} 筆資料)")
                # 在生產環境中，您可能想要實作更強的確認機制

            # 執行刪除
            drop_sql = f'DROP TABLE {"IF EXISTS " if if_exists else ""}"{table_name}"'
            self.conn.sql(drop_sql)

            self.logger.info(f"✅ 成功刪除表格 '{table_name}' (原有 {row_count:,} 筆資料)")
            return True

        except Exception as e:
            self.logger.error(f"❌ 刪除表格 '{table_name}' 失敗: {e}")
            return False

    def truncate_table(self, table_name: str) -> bool:
        """
        清空表格資料但保留結構

        Args:
            table_name: 表格名稱

        Returns:
            bool: 是否成功清空
        """
        try:
            # 檢查表格是否存在
            existing_tables = self.conn.sql("SHOW TABLES").df()
            if existing_tables.empty or table_name not in existing_tables['name'].values:
                self.logger.error(f"❌ 表格 '{table_name}' 不存在")
                return False

            # 獲取清空前的記錄數
            row_count = self.conn.sql(f'SELECT COUNT(*) as count FROM "{table_name}"').df().iloc[0]['count']

            # 清空表格
            self.conn.sql(f'DELETE FROM "{table_name}"')

            self.logger.info(f"✅ 成功清空表格 '{table_name}' (刪除了 {row_count:,} 筆資料)")
            return True

        except Exception as e:
            self.logger.error(f"❌ 清空表格 '{table_name}' 失敗: {e}")
            return False

    def backup_table(self, table_name: str, backup_format: str = 'parquet',
                     backup_path: str = None) -> bool:
        """
        備份表格資料

        Args:
            table_name: 表格名稱
            backup_format: 備份格式 ('parquet', 'csv', 'json')
            backup_path: 備份檔案路徑，如果為 None 則自動生成

        Returns:
            bool: 是否成功備份
        """
        try:

            if backup_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f"{table_name}_backup_{timestamp}.{backup_format}"

            # 檢查表格是否存在
            existing_tables = self.conn.sql("SHOW TABLES").df()
            if existing_tables.empty or table_name not in existing_tables['name'].values:
                self.logger.error(f"❌ 表格 '{table_name}' 不存在")
                return False

            # 執行備份
            if backup_format.lower() == 'parquet':
                self.conn.sql(f'COPY (SELECT * FROM "{table_name}") TO \'{backup_path}\' (FORMAT PARQUET)')
            elif backup_format.lower() == 'csv':
                self.conn.sql(f'COPY (SELECT * FROM "{table_name}") TO \'{backup_path}\' (FORMAT CSV, HEADER)')
            elif backup_format.lower() == 'json':
                self.conn.sql(f'COPY (SELECT * FROM "{table_name}") TO \'{backup_path}\' (FORMAT JSON)')
            else:
                raise ValueError(f"不支援的備份格式: {backup_format}")

            # 獲取備份資訊
            table_info = self.get_table_info(table_name)
            row_count = table_info.get('row_count', 0)

            self.logger.info(f"✅ 成功備份表格 '{table_name}' 到 '{backup_path}' ({row_count:,} 筆資料)")
            return True

        except Exception as e:
            self.logger.error(f"❌ 備份表格 '{table_name}' 失敗: {e}")
            return False

    def execute_transaction(self, operations: list) -> bool:
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
                    self.logger.debug(f"執行操作 {i}/{len(operations)}: {operation[:100]}...")
                    self.conn.sql(operation)
                except Exception as e:
                    self.logger.error(f"❌ 操作 {i} 失敗: {e}")
                    self.conn.sql("ROLLBACK")
                    self.logger.error("🔄 事務已回滾")
                    return False

            # 提交事務
            self.conn.sql("COMMIT")
            self.logger.info(f"✅ 成功執行所有 {len(operations)} 個操作")
            return True

        except Exception as e:
            self.logger.error(f"❌ 事務執行失敗: {e}")
            try:
                self.conn.sql("ROLLBACK")
                self.logger.error("🔄 事務已回滾")
            except Exception as err:
                pass
            return False

    def validate_data_integrity(self, table_name: str,
                                checks: dict = None) -> dict:
        """
        驗證資料完整性

        Args:
            table_name: 表格名稱
            checks: 自定義檢查規則

        Returns:
            dict: 驗證結果
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
            total_rows = self.conn.sql(f'SELECT COUNT(*) as count FROM "{table_name}"').df().iloc[0]['count']
            results['total_rows'] = total_rows

            # 檢查每個欄位的NULL值
            schema = self.describe_table(table_name)
            if schema is not None:
                for _, col_info in schema.iterrows():
                    col_name = col_info['column_name']
                    null_count = (self.conn.sql(
                        f'SELECT COUNT(*) as count FROM "{table_name}" WHERE "{col_name}" IS NULL')
                        .df()
                        .iloc[0]['count']
                    )
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
                        check_result = self.conn.sql(check_sql.format(table_name=table_name)).df()
                        results['custom_checks'][check_name] = check_result.to_dict('records')
                    except Exception as e:
                        results['custom_checks'][check_name] = f"Error: {e}"

            self.logger.info("✅ 完成資料完整性驗證")
            return results

        except Exception as e:
            self.logger.error(f"❌ 資料完整性驗證失敗: {e}")
            return {}


def create_table(table_name: str, 
                 df: pd.DataFrame, 
                 db_path="bank_statements.duckdb", 
                 log_file="duckdb_operations.log", 
                 log_level="DEBUG"):
    """使用範例：基本操作"""

    db_path = db_path
    log_file = log_file

    # 建立DuckDB管理器，同時輸出到terminal和檔案
    with DuckDBManager(
        db_path=db_path
    ) as db_manager:

        # 建立表格（正確的方式，只會插入一次）
        success1 = db_manager.create_table_from_df(
            table_name,
            df,
        )

        if success1:
            # 顯示詳細資訊
            info = db_manager.get_table_info(table_name)
            print(f"\n📋 表格 {table_name}:")
            print(f"   記錄數: {info.get('row_count', 0):,}")
            print(f"   欄位數: {len(info.get('columns', []))}")
            return info
        else:
            return None

def insert_table(table_name: str, 
                 df: pd.DataFrame, 
                 db_path="bank_statements.duckdb", 
                 log_file="duckdb_operations.log", 
                 log_level="DEBUG"):
    """使用範例：基本操作"""

    db_path = db_path
    log_file = log_file

    # 建立DuckDB管理器，同時輸出到terminal和檔案
    with DuckDBManager(
        db_path=db_path
    ) as db_manager:

        # 建立表格（正確的方式，只會插入一次）
        success1 = db_manager.insert_df_into_table(
            table_name,
            df,
        )

        if success1:
            # 顯示詳細資訊
            info = db_manager.get_table_info(table_name)
            print(f"\n📋 表格 {table_name}:")
            print(f"   記錄數: {info.get('row_count', 0):,}")
            print(f"   欄位數: {len(info.get('columns', []))}")
            return info
        else:
            return None

def alter_column_dtype(table_name: str, 
                       column_name: str, 
                       new_type: str = "BIGINT", 
                       db_path: str = "bank_statements.duckdb", 
                       log_file: str = "duckdb_operations.log", 
                       log_level: str = "DEBUG"):

    with DuckDBManager(
        db_path=db_path
    ) as db_manager:

        # Method 1: Preview the data first to understand the format
        print("=== Step 1: Preview current data ===")
        db_manager.preview_column_values(
            table_name=table_name,
            column_name=column_name,
            limit=10,
            show_unique=True
        )

        # Method 2: Preview cleaning (without actually changing data)
        print("\n=== Step 2: Preview cleaning ===")
        db_manager.clean_numeric_column(
            table_name=table_name,
            column_name=column_name,
            remove_chars=[','],  # Only remove commas
            preview_only=True
        )

        # Method 3: Actually clean and convert in one go
        print("\n=== Step 3: Clean and convert ===")
        success = db_manager.clean_and_convert_column(
            table_name=table_name,
            column_name=column_name,
            target_type=new_type,
            remove_chars=[','],  # Remove commas
            handle_empty_as_null=True
        )

        if success:
            print("🎉 Success! Let's verify the result:")

            # Verify the schema change
            schema = db_manager.describe_table(table_name)
            print(schema[schema['column_name'] == column_name])

            # Check some sample data
            sample_data = db_manager.query_to_df(f"""
            SELECT {column_name}
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT 5
            """)
            print("\nSample converted data:")
            print(sample_data)

def drop_table(table_name: str, db_path="bank_statements.duckdb", log_file="duckdb_operations.log", log_level="DEBUG"):
    with DuckDBManager(
        db_path=db_path
    ) as db_manager:

        query = \
            f"""
            DROP TABLE IF EXISTS {table_name}
            """

        db_manager.drop_table(table_name)

def backup_table(table_name: str, 
                 db_path="bank_statements.duckdb", 
                 log_file="duckdb_operations.log", 
                 log_level="DEBUG", 
                 backup_format: str = 'parquet', 
                 backup_path: str = None):
    with DuckDBManager(
        db_path=db_path
    ) as db_manager:

        db_manager.backup_table(
            table_name=table_name,
            backup_format=backup_format,
            backup_path=backup_path
        )

if __name__ == "__main__":
    DB_PATH = "bank_statements.duckdb"
    LOG_FILE = "duckdb_operations.log"
    print(1)
    
