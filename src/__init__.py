"""
SPE - SPE資料處理任務框架

提供統一的數據源管理和 Pipeline 處理流程，
支援 Excel、CSV、Parquet 等多種資料格式。

主要模組：
- core.datasources: 統一的資料源抽象層
- core.pipeline: 資料處理流程管理
- utils: 日誌、配置等工具函數
"""

__version__ = "1.0.0"
__author__ = "SEA Team"

# from .core import datasources, pipeline
from .utils import get_logger, get_structured_logger, config_manager

__all__ = [
    # 版本
    '__version__',
    # 模組
    # 'datasources',
    # 'pipeline',
    # 工具
    'get_logger',
    'get_structured_logger',
    'config_manager',
]
