"""
數據源基礎類
定義數據源的抽象接口和通用功能（同步版本）
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime

from spe_bank_recon.utils.logging import get_logger
from .config import DataSourceConfig, DataSourceType


class DataSource(ABC):
    """數據源抽象基類"""
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化數據源
        
        Args:
            config: 數據源配置
        """
        self.config = config
        self.logger = get_logger(f"datasource.{self.__class__.__name__}")
        self._cache = None
        self._metadata = {}
        
    @abstractmethod
    def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        讀取數據
        
        Args:
            query: 查詢條件
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        pass
    
    @abstractmethod
    def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        寫入數據
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        pass
    
    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        獲取數據源元數據
        
        Returns:
            Dict[str, Any]: 元數據信息
        """
        pass
    
    def read_with_cache(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        帶快取的讀取
        
        Args:
            query: 查詢條件
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 數據
        """
        if self.config.cache_enabled and self._cache is not None:
            self.logger.debug("Returning cached data")
            return self._cache.copy()
        
        data = self.read(query, **kwargs)
        
        if self.config.cache_enabled:
            self._cache = data.copy()
            
        return data
    
    def clear_cache(self):
        """清除快取"""
        self._cache = None
        self.logger.debug("Cache cleared")
    
    def validate_connection(self) -> bool:
        """
        驗證連接是否有效
        
        Returns:
            bool: 連接是否有效
        """
        try:
            test_data = self.read(nrows=1)
            return test_data is not None
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False
    
    def get_row_count(self) -> int:
        """獲取數據行數"""
        try:
            data = self.read_with_cache()
            return len(data)
        except Exception as e:
            self.logger.error(f"Failed to get row count: {str(e)}")
            return 0
    
    def get_column_names(self) -> List[str]:
        """獲取列名"""
        try:
            data = self.read_with_cache()
            return data.columns.tolist()
        except Exception as e:
            self.logger.error(f"Failed to get column names: {str(e)}")
            return []
    
    def close(self):
        """關閉數據源連接"""
        self.clear_cache()
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.config.source_type.value})"
    
    def __enter__(self):
        """上下文管理器進入"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()
