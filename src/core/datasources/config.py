"""
數據源配置類
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from enum import Enum


class DataSourceType(Enum):
    """數據源類型"""
    EXCEL = "excel"
    CSV = "csv"
    PARQUET = "parquet"  # 預留擴展


@dataclass
class DataSourceConfig:
    """
    數據源配置

    迭代 2 更新: 增強快取配置，支持 TTL 和 LRU。

    Attributes:
        source_type: 數據源類型
        connection_params: 連接參數
        cache_enabled: 是否啟用快取
        encoding: 編碼
        chunk_size: 分塊大小
        cache_ttl_seconds: 快取過期時間（秒），默認 300 秒（5 分鐘）
        cache_max_items: 最大快取條目數，默認 10 個
        cache_eviction_policy: 快取清理策略，默認 'lru' (Least Recently Used)
    """
    source_type: DataSourceType
    connection_params: Dict[str, Any]
    cache_enabled: bool = True
    encoding: str = 'utf-8'
    chunk_size: Optional[int] = None

    # 迭代 2 新增: 增強的快取配置
    cache_ttl_seconds: int = 300  # 5 分鐘過期
    cache_max_items: int = 10     # 最多 10 個快取條目
    cache_eviction_policy: str = "lru"  # lru, lfu, fifo
    
    def validate(self) -> tuple:
        """
        驗證配置有效性
        
        Returns:
            tuple[bool, List[str]]: (是否有效, 錯誤訊息列表)
        """
        errors = []
        
        required_params = {
            DataSourceType.EXCEL: ['file_path'],
            DataSourceType.CSV: ['file_path'],
            DataSourceType.PARQUET: ['file_path'],
        }
        
        required = required_params.get(self.source_type, [])
        for param in required:
            if param not in self.connection_params:
                errors.append(f"Missing required parameter: {param}")
        
        # 驗證檔案路徑是否存在
        if self.source_type in [DataSourceType.EXCEL, DataSourceType.CSV, DataSourceType.PARQUET]:
            file_path = self.connection_params.get('file_path')
            if file_path:
                from pathlib import Path
                if not Path(file_path).exists():
                    errors.append(f"File not found: {file_path}")
        
        return len(errors) == 0, errors
    
    def copy(self) -> 'DataSourceConfig':
        """創建配置的副本"""
        return DataSourceConfig(
            source_type=self.source_type,
            connection_params=self.connection_params.copy(),
            cache_enabled=self.cache_enabled,
            encoding=self.encoding,
            chunk_size=self.chunk_size,
            cache_ttl_seconds=self.cache_ttl_seconds,
            cache_max_items=self.cache_max_items,
            cache_eviction_policy=self.cache_eviction_policy
        )
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DataSourceConfig':
        """從字典創建配置"""
        source_type = config_dict.get('source_type')
        if isinstance(source_type, str):
            source_type = DataSourceType(source_type)

        return cls(
            source_type=source_type,
            connection_params=config_dict.get('connection_params', {}),
            cache_enabled=config_dict.get('cache_enabled', True),
            encoding=config_dict.get('encoding', 'utf-8'),
            chunk_size=config_dict.get('chunk_size'),
            cache_ttl_seconds=config_dict.get('cache_ttl_seconds', 300),
            cache_max_items=config_dict.get('cache_max_items', 10),
            cache_eviction_policy=config_dict.get('cache_eviction_policy', 'lru')
        )

    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return {
            'source_type': self.source_type.value,
            'connection_params': self.connection_params,
            'cache_enabled': self.cache_enabled,
            'encoding': self.encoding,
            'chunk_size': self.chunk_size,
            'cache_ttl_seconds': self.cache_ttl_seconds,
            'cache_max_items': self.cache_max_items,
            'cache_eviction_policy': self.cache_eviction_policy
        }
