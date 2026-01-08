"""
數據源基礎類
定義數據源的抽象接口和通用功能（同步版本）

迭代 2 更新: 增強快取機制，支持 TTL 和 LRU。
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import json

from src.utils.logging import get_logger
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

        # 迭代 2 更新: 增強的快取機制
        # 快取結構: {cache_key: (DataFrame, timestamp)}
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self._cache_ttl = timedelta(seconds=config.cache_ttl_seconds)
        self._cache_max_size = config.cache_max_items

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
        帶快取的讀取（增強版 - 迭代 2）

        支持功能:
        - 基於 query 和參數的多級快取
        - TTL (Time-To-Live) 自動過期
        - LRU (Least Recently Used) 驅逐策略

        Args:
            query: 查詢條件
            **kwargs: 額外參數

        Returns:
            pd.DataFrame: 數據
        """
        # 如果快取未啟用，直接讀取
        if not self.config.cache_enabled:
            return self.read(query, **kwargs)

        # 生成快取鍵（基於 query 和參數）
        cache_key = self._generate_cache_key(query, kwargs)

        # 檢查快取是否存在
        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]

            # 檢查是否過期
            if datetime.now() - timestamp < self._cache_ttl:
                self.logger.debug(f"快取命中: {cache_key[:16]}...")
                return data.copy()
            else:
                self.logger.debug(f"快取過期，重新讀取: {cache_key[:16]}...")
                del self._cache[cache_key]

        # 快取未命中或已過期，讀取數據
        self.logger.debug(f"快取未命中，從源讀取: {cache_key[:16]}...")
        data = self.read(query, **kwargs)

        # 保存到快取
        self._cache[cache_key] = (data.copy(), datetime.now())

        # LRU: 如果快取超出大小限制，移除最舊的條目
        if len(self._cache) > self._cache_max_size:
            # 找到時間戳最舊的條目
            oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]
            self.logger.debug(f"快取已滿，移除最舊條目: {oldest_key[:16]}...")

        return data

    def _generate_cache_key(self, query: Optional[str], kwargs: Dict[str, Any]) -> str:
        """
        生成快取鍵（基於 query 和參數的 MD5 hash）

        Args:
            query: 查詢條件
            kwargs: 額外參數

        Returns:
            str: MD5 hash 字符串
        """
        # 構建鍵數據（排除 logger 等非數據參數）
        key_data = {
            'query': query,
            'kwargs': {k: v for k, v in sorted(kwargs.items())
                       if k not in ['logger', 'log_level']}
        }

        # 序列化為 JSON（使用 default=str 處理特殊類型）
        try:
            key_json = json.dumps(key_data, sort_keys=True, default=str)
        except (TypeError, ValueError) as e:
            # 如果 JSON 序列化失敗，使用字符串表示
            self.logger.warning(f"JSON 序列化失敗，使用 repr: {e}")
            key_json = repr(key_data)

        # 計算 MD5 hash
        return hashlib.md5(key_json.encode('utf-8')).hexdigest()
    
    def clear_cache(self):
        """清除所有快取條目（迭代 2 更新）"""
        count = len(self._cache)
        self._cache.clear()
        if count > 0:
            self.logger.info(f"已清除 {count} 個快取條目")
        else:
            self.logger.debug("快取已為空")
    
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
