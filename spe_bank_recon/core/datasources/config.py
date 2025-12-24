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
    
    Attributes:
        source_type: 數據源類型
        connection_params: 連接參數
        cache_enabled: 是否啟用快取
        encoding: 編碼
        chunk_size: 分塊大小
    """
    source_type: DataSourceType
    connection_params: Dict[str, Any]
    cache_enabled: bool = True
    encoding: str = 'utf-8'
    chunk_size: Optional[int] = None
    
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
            chunk_size=self.chunk_size
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
            chunk_size=config_dict.get('chunk_size')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return {
            'source_type': self.source_type.value,
            'connection_params': self.connection_params,
            'cache_enabled': self.cache_enabled,
            'encoding': self.encoding,
            'chunk_size': self.chunk_size
        }
