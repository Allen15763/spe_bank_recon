"""
配置管理器
提供統一的配置加載和管理功能（TOML 版本）
"""

import os
import sys
import logging
import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ImportError:
    import tomli as tomllib  # Python 3.10 及以下需要安裝 tomli


def get_project_root() -> Path:
    """
    獲取專案根目錄
    
    Returns:
        Path: 專案根目錄路徑
    """
    # 從當前檔案位置向上查找，直到找到包含 config 目錄的層級
    current = Path(__file__).parent
    while current.parent != current:
        if (current / 'config').exists():
            return current
        current = current.parent
    
    # 如果找不到，使用當前工作目錄
    return Path.cwd()


class ConfigManager:
    """配置管理器，單例模式"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._config_data: Dict[str, Any] = {}
        self._simple_logger = None
        self._setup_simple_logger()
        self._load_config()
        self._initialized = True
    
    def _setup_simple_logger(self) -> None:
        """設置簡單的日誌記錄器，避免循環導入"""
        self._simple_logger = logging.getLogger('config_manager')
        self._simple_logger.setLevel(logging.INFO)
        
        if not self._simple_logger.handlers:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self._simple_logger.addHandler(console_handler)
            self._simple_logger.propagate = False
    
    def _load_config(self) -> None:
        """加載配置檔案"""
        try:
            # 確定配置檔案路徑
            project_root = get_project_root()
            possible_paths = [
                project_root / 'config' / 'config.toml',
                Path(__file__).parent.parent.parent / 'config' / 'config.toml',
                Path.cwd() / 'config' / 'config.toml',
                Path.cwd() / 'offline_tasks' / 'config' / 'config.toml',
            ]
            
            config_path = None
            for path in possible_paths:
                if path.exists() and path.is_file():
                    config_path = path
                    break
            
            if not config_path:
                self._log_warning(f"配置檔案不存在，使用預設配置。嘗試路徑: {[str(p) for p in possible_paths]}")
                self._set_default_config()
                return
            
            # 加載 TOML 配置
            with open(config_path, 'rb') as f:
                self._config_data = tomllib.load(f)
            
            self._log_info(f"成功載入配置檔案: {config_path}")
            
        except Exception as e:
            self._log_error(f"載入配置檔案時出錯: {e}")
            self._set_default_config()

    def _log_info(self, message: str) -> None:
        """記錄資訊訊息"""
        if self._simple_logger:
            self._simple_logger.info(message)
        else:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sys.stdout.write(f"[{timestamp}] INFO: {message}\n")
            sys.stdout.flush()

    def _log_warning(self, message: str) -> None:
        """記錄警告訊息"""
        if self._simple_logger:
            self._simple_logger.warning(message)
        else:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sys.stderr.write(f"[{timestamp}] WARNING: {message}\n")

    def _log_error(self, message: str) -> None:
        """記錄錯誤訊息"""
        if self._simple_logger:
            self._simple_logger.error(message)
        else:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] ERROR: {message}", file=sys.stderr)
    
    def _set_default_config(self) -> None:
        """設定預設配置"""
        self._config_data = {
            'general': {
                'project_name': 'offline_tasks',
                'version': '1.0.0',
            },
            'logging': {
                'level': 'INFO',
                'detailed': True,
                'color': True,
                'max_file_size_mb': 10,
                'backup_count': 5,
                'log_to_file': True,
                'log_to_console': True,
            },
            'paths': {
                'log_path': './logs',
                'output_path': './output',
                'input_path': './input',
                'temp_path': './temp',
            },
            'datasource': {
                'default_encoding': 'utf-8',
                'excel_engine': 'openpyxl',
                'csv_separator': ',',
                'cache_enabled': True,
                'chunk_size': 10000,
            },
            'pipeline': {
                'stop_on_error': True,
                'log_level': 'INFO',
                'max_retries': 3,
                'retry_delay': 2,
            },
            'task': {
                'default_type': 'transform',
                'timeout': 300,
                'save_intermediate': False,
            }
        }
    
    def get(self, section: str, key: str = None, fallback: Any = None) -> Any:
        """
        獲取配置值
        
        支援兩種調用方式：
        - get('section', 'key') - 獲取 section 下的 key
        - get('section.key') - 使用點號分隔的路徑
        
        Args:
            section: 配置段落名稱或完整路徑
            key: 配置鍵名（可選）
            fallback: 預設值
            
        Returns:
            Any: 配置值
        """
        try:
            # 支援點號分隔的路徑
            if key is None and '.' in section:
                parts = section.split('.')
                value = self._config_data
                for part in parts:
                    value = value.get(part, {})
                return value if value != {} else fallback
            
            # 傳統的 section, key 方式
            if key is None:
                return self._config_data.get(section, fallback)
            
            section_data = self._config_data.get(section, {})
            return section_data.get(key, fallback)
            
        except Exception:
            return fallback
    
    def get_int(self, section: str, key: str = None, fallback: int = 0) -> int:
        """獲取整數配置值"""
        try:
            value = self.get(section, key)
            return int(value) if value is not None else fallback
        except (ValueError, TypeError):
            return fallback
    
    def get_float(self, section: str, key: str = None, fallback: float = 0.0) -> float:
        """獲取浮點數配置值"""
        try:
            value = self.get(section, key)
            return float(value) if value is not None else fallback
        except (ValueError, TypeError):
            return fallback
    
    def get_boolean(self, section: str, key: str = None, fallback: bool = False) -> bool:
        """獲取布林配置值"""
        try:
            value = self.get(section, key)
            if value is None:
                return fallback
            if isinstance(value, bool):
                return value
            return str(value).lower() in ('true', '1', 'yes', 'on')
        except (AttributeError, TypeError):
            return fallback
    
    def get_list(self, section: str, key: str = None, fallback: List = None) -> List:
        """獲取列表配置值"""
        if fallback is None:
            fallback = []
            
        try:
            value = self.get(section, key)
            if value is None:
                return fallback
            if isinstance(value, list):
                return value
            # 如果是字串，嘗試用逗號分隔
            if isinstance(value, str):
                return [item.strip() for item in value.split(',') if item.strip()]
            return fallback
        except (AttributeError, TypeError):
            return fallback
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """獲取整個配置段落"""
        return self._config_data.get(section, {})
    
    def has_section(self, section: str) -> bool:
        """檢查是否存在配置段落"""
        return section in self._config_data
    
    def has_option(self, section: str, key: str) -> bool:
        """檢查是否存在配置選項"""
        return section in self._config_data and key in self._config_data[section]
    
    def set_config(self, section: str, key: str, value: Any) -> None:
        """設定配置值（運行時配置）"""
        if section not in self._config_data:
            self._config_data[section] = {}
        self._config_data[section][key] = value
    
    def get_path(self, section: str, key: str = None, fallback: str = None) -> Optional[Path]:
        """
        獲取路徑配置值，自動轉為 Path 物件
        
        Args:
            section: 配置段落
            key: 配置鍵
            fallback: 預設值
            
        Returns:
            Optional[Path]: 路徑物件
        """
        path_str = self.get(section, key, fallback)
        if path_str:
            return Path(path_str)
        return None
    
    def get_nested(self, *keys: str, fallback: Any = None) -> Any:
        """
        獲取嵌套配置值
        
        Example:
            config.get_nested('datasource', 'excel', 'engine')
        
        Args:
            *keys: 嵌套的鍵名
            fallback: 預設值
            
        Returns:
            Any: 配置值
        """
        try:
            value = self._config_data
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return fallback
    
    def reload_config(self) -> None:
        """重新加載配置"""
        self._initialized = False
        self._config_data = {}
        self._load_config()
        self._initialized = True
    
    def to_dict(self) -> Dict[str, Any]:
        """返回完整的配置字典"""
        return self._config_data.copy()
    
    def __repr__(self) -> str:
        return f"ConfigManager(sections={list(self._config_data.keys())})"


# 全域配置管理器實例
config_manager = ConfigManager()


# 便利函數
def get_config(section: str, key: str = None, fallback: Any = None) -> Any:
    """獲取配置值的便利函數"""
    return config_manager.get(section, key, fallback)


def get_path(section: str, key: str = None, fallback: str = None) -> Optional[Path]:
    """獲取路徑配置值的便利函數"""
    return config_manager.get_path(section, key, fallback)
