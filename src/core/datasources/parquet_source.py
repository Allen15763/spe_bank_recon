"""
Parquet 數據源實現
"""

import pandas as pd
from typing import Dict, Optional, Any, List
from pathlib import Path

from .base import DataSource
from .config import DataSourceConfig, DataSourceType


class ParquetSource(DataSource):
    """Parquet 文件數據源"""
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化 Parquet 數據源
        
        Args:
            config: 數據源配置
        """
        super().__init__(config)
        self.file_path = Path(config.connection_params['file_path'])
        self.columns = config.connection_params.get('columns')
        self.filters = config.connection_params.get('filters')
        
        # 驗證 pyarrow 是否安裝
        try:
            import pyarrow
            self._pyarrow_available = True
        except ImportError:
            self._pyarrow_available = False
            self.logger.warning("pyarrow not installed, some features may not work")
    
    def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        讀取 Parquet 文件
        
        Args:
            query: 查詢條件（使用 pandas query 語法）
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        try:
            self.logger.info(f"Reading Parquet file: {self.file_path}")
            
            # 獲取參數
            columns = kwargs.get('columns', self.columns)
            filters = kwargs.get('filters', self.filters)
            
            # 構建讀取參數
            read_kwargs = {}
            if columns:
                read_kwargs['columns'] = columns
            if filters and self._pyarrow_available:
                read_kwargs['filters'] = filters
            
            # 讀取數據
            df = pd.read_parquet(self.file_path, **read_kwargs)
            
            # 應用查詢條件
            if query:
                df = self._apply_query(df, query)
            
            self.logger.info(f"Successfully read {len(df)} rows from Parquet")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading Parquet file: {str(e)}")
            raise
    
    def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        寫入 Parquet 文件
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        try:
            self.logger.info(f"Writing {len(data)} rows to Parquet: {self.file_path}")
            
            # 獲取參數
            compression = kwargs.get('compression', 'snappy')
            index = kwargs.get('index', False)
            partition_cols = kwargs.get('partition_cols')
            
            # 確保目錄存在
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 構建寫入參數
            write_kwargs = {
                'compression': compression,
                'index': index
            }
            if partition_cols:
                write_kwargs['partition_cols'] = partition_cols
            
            # 寫入數據
            data.to_parquet(self.file_path, **write_kwargs)
            
            self.logger.info("Successfully wrote data to Parquet")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing Parquet file: {str(e)}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取 Parquet 文件元數據
        
        Returns:
            Dict[str, Any]: 元數據信息
        """
        metadata = {
            'file_path': str(self.file_path),
            'file_size': self.file_path.stat().st_size if self.file_path.exists() else 0,
            'file_modified': self.file_path.stat().st_mtime if self.file_path.exists() else None,
        }
        
        try:
            if self._pyarrow_available and self.file_path.exists():
                import pyarrow.parquet as pq
                
                # 讀取 Parquet 元數據
                parquet_file = pq.ParquetFile(self.file_path)
                schema = parquet_file.schema_arrow
                
                metadata['num_columns'] = len(schema)
                metadata['column_names'] = schema.names
                metadata['num_rows'] = parquet_file.metadata.num_rows
                metadata['num_row_groups'] = parquet_file.metadata.num_row_groups
                
                # 獲取列類型
                metadata['column_types'] = {
                    field.name: str(field.type) 
                    for field in schema
                }
                
        except Exception as e:
            self.logger.warning(f"Could not read Parquet metadata: {str(e)}")
            metadata['num_columns'] = 0
            metadata['column_names'] = []
            metadata['num_rows'] = 0
        
        return metadata
    
    def get_schema(self) -> Optional[Dict[str, str]]:
        """
        獲取 Parquet Schema
        
        Returns:
            Optional[Dict[str, str]]: 列名到類型的映射
        """
        if not self._pyarrow_available:
            self.logger.warning("pyarrow not available, cannot get schema")
            return None
        
        try:
            import pyarrow.parquet as pq
            
            parquet_file = pq.ParquetFile(self.file_path)
            schema = parquet_file.schema_arrow
            
            return {
                field.name: str(field.type) 
                for field in schema
            }
            
        except Exception as e:
            self.logger.error(f"Error getting schema: {str(e)}")
            return None
    
    def read_row_group(self, row_group: int) -> pd.DataFrame:
        """
        讀取特定的 Row Group
        
        Args:
            row_group: Row Group 索引
            
        Returns:
            pd.DataFrame: 該 Row Group 的數據
        """
        if not self._pyarrow_available:
            raise ImportError("pyarrow is required for reading row groups")
        
        try:
            import pyarrow.parquet as pq
            
            parquet_file = pq.ParquetFile(self.file_path)
            table = parquet_file.read_row_group(row_group)
            
            return table.to_pandas()
            
        except Exception as e:
            self.logger.error(f"Error reading row group: {str(e)}")
            raise
    
    def _apply_query(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """應用查詢條件"""
        try:
            return df.query(query)
        except Exception as e:
            self.logger.warning(f"Could not apply query '{query}': {str(e)}")
            return df
    
    @classmethod
    def create_from_file(cls, file_path: str, **kwargs) -> 'ParquetSource':
        """
        便捷方法：從檔案路徑創建 Parquet 數據源
        
        Args:
            file_path: Parquet 檔案路徑
            **kwargs: 其他配置參數
            
        Returns:
            ParquetSource: Parquet 數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.PARQUET,
            connection_params={
                'file_path': file_path,
                **kwargs
            }
        )
        return cls(config)
