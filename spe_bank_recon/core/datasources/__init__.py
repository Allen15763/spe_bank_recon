"""
數據源模組
提供統一的數據源抽象層
"""

from .config import DataSourceConfig, DataSourceType
from .base import DataSource
from .csv_source import CSVSource
from .excel_source import ExcelSource
from .parquet_source import ParquetSource
from .google_sheet_source import GoogleSheetsManager
from .factory import DataSourceFactory, DataSourcePool, create_quick_source

__all__ = [
    # 配置
    'DataSourceConfig',
    'DataSourceType',
    # 基類
    'DataSource',
    # 實現
    'CSVSource',
    'ExcelSource',
    'ParquetSource',
    'GoogleSheetsManager',
    # 工廠
    'DataSourceFactory',
    'DataSourcePool',
    'create_quick_source',
]
