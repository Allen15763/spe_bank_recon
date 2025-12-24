"""
數據源工廠類
用於創建不同類型的數據源實例
"""

from typing import Dict, Type, Optional, Any
import logging
from pathlib import Path

from .base import DataSource
from .config import DataSourceConfig, DataSourceType
from .excel_source import ExcelSource
from .csv_source import CSVSource
from .parquet_source import ParquetSource


class DataSourceFactory:
    """數據源工廠"""
    
    # 註冊的數據源類型
    _sources: Dict[DataSourceType, Type[DataSource]] = {
        DataSourceType.EXCEL: ExcelSource,
        DataSourceType.CSV: CSVSource,
        DataSourceType.PARQUET: ParquetSource,
    }
    
    logger = logging.getLogger("DataSourceFactory")
    
    @classmethod
    def create(cls, config: DataSourceConfig) -> DataSource:
        """
        創建數據源實例
        
        Args:
            config: 數據源配置
            
        Returns:
            DataSource: 數據源實例
            
        Raises:
            ValueError: 配置無效
            NotImplementedError: 數據源類型未實現
        """
        # 驗證配置
        is_valid, errors = config.validate()
        if not is_valid:
            error_msg = f"Invalid configuration: {', '.join(errors)}"
            cls.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 獲取對應的數據源類
        source_class = cls._sources.get(config.source_type)
        if not source_class:
            raise NotImplementedError(f"Data source {config.source_type} not implemented")
        
        cls.logger.info(f"Creating {config.source_type.value} data source")
        return source_class(config)
    
    @classmethod
    def create_from_file(cls, file_path: str, **kwargs) -> DataSource:
        """
        根據文件擴展名自動創建數據源
        
        Args:
            file_path: 文件路徑
            **kwargs: 額外配置參數
            
        Returns:
            DataSource: 數據源實例
            
        Raises:
            ValueError: 不支援的文件類型
        """
        path = Path(file_path)
        extension = path.suffix.lower()
        
        # 根據擴展名判斷類型
        if extension in ['.xlsx', '.xls']:
            source_type = DataSourceType.EXCEL
        elif extension == '.csv':
            source_type = DataSourceType.CSV
        elif extension == '.parquet':
            source_type = DataSourceType.PARQUET
        else:
            raise ValueError(f"Unsupported file type: {extension}")
        
        config = DataSourceConfig(
            source_type=source_type,
            connection_params={
                'file_path': file_path,
                **kwargs
            }
        )
        
        return cls.create(config)
    
    @classmethod
    def register_source(cls, source_type: DataSourceType, 
                        source_class: Type[DataSource]):
        """
        註冊新的數據源類型
        
        Args:
            source_type: 數據源類型
            source_class: 數據源類
        """
        cls._sources[source_type] = source_class
        cls.logger.info(f"Registered new data source type: {source_type.value}")
    
    @classmethod
    def get_supported_types(cls) -> list:
        """獲取支援的數據源類型"""
        return list(cls._sources.keys())
    
    @classmethod
    def create_batch(cls, configs: list) -> Dict[str, DataSource]:
        """
        批量創建數據源
        
        Args:
            configs: 配置列表，每個元素是(名稱, 配置)元組
            
        Returns:
            Dict[str, DataSource]: 名稱到數據源的映射
        """
        sources = {}
        
        for name, config in configs:
            try:
                sources[name] = cls.create(config)
                cls.logger.info(f"Created data source: {name}")
            except Exception as e:
                cls.logger.error(f"Failed to create data source {name}: {str(e)}")
        
        return sources


class DataSourcePool:
    """
    數據源連接池（用於管理多個數據源）
    """
    
    def __init__(self):
        self.sources: Dict[str, DataSource] = {}
        self.logger = logging.getLogger("DataSourcePool")
    
    def add_source(self, name: str, source: DataSource):
        """添加數據源到池"""
        self.sources[name] = source
        self.logger.info(f"Added data source to pool: {name}")
    
    def get_source(self, name: str) -> Optional[DataSource]:
        """獲取數據源"""
        return self.sources.get(name)
    
    def remove_source(self, name: str) -> bool:
        """移除數據源"""
        if name in self.sources:
            source = self.sources[name]
            source.close()
            del self.sources[name]
            self.logger.info(f"Removed data source from pool: {name}")
            return True
        return False
    
    def close_all(self):
        """關閉所有數據源"""
        for name, source in self.sources.items():
            try:
                source.close()
                self.logger.info(f"Closed data source: {name}")
            except Exception as e:
                self.logger.error(f"Failed to close data source {name}: {str(e)}")
        self.sources.clear()
    
    def list_sources(self) -> list:
        """列出所有數據源"""
        return list(self.sources.keys())
    
    def __del__(self):
        """析構函數"""
        self.close_all()


def create_quick_source(file_path: str, **kwargs) -> DataSource:
    """
    快速創建數據源的便捷函數
    
    Args:
        file_path: 文件路徑
        **kwargs: 額外配置參數
        
    Returns:
        DataSource: 數據源實例
    """
    return DataSourceFactory.create_from_file(file_path, **kwargs)
