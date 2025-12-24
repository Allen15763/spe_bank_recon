"""
日誌處理模組
提供統一的日誌記錄功能，支援多種輸出目標（同步版本）
"""

import os
import sys
import logging
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler

from ..config.config_manager import config_manager


# ANSI 顏色代碼
class ColorCodes:
    """終端顏色代碼"""
    GREY = '\033[90m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD_RED = '\033[1;91m'
    RESET = '\033[0m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'


class ColoredFormatter(logging.Formatter):
    """彩色日誌格式化器"""
    
    COLORS = {
        logging.DEBUG: ColorCodes.GREY,
        logging.INFO: ColorCodes.GREEN,
        logging.WARNING: ColorCodes.YELLOW,
        logging.ERROR: ColorCodes.RED,
        logging.CRITICAL: ColorCodes.BOLD_RED
    }
    
    def __init__(self, fmt: str = None, datefmt: str = None, use_color: bool = True):
        super().__init__(fmt, datefmt)
        self.use_color = use_color and self._supports_color()
    
    def _supports_color(self) -> bool:
        """檢測終端是否支援顏色"""
        if sys.platform == "win32":
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
                return True
            except Exception:
                return False
        return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
    
    def format(self, record: logging.LogRecord) -> str:
        """格式化日誌記錄"""
        if self.use_color:
            original_levelname = record.levelname
            original_name = record.name
            
            color = self.COLORS.get(record.levelno, ColorCodes.RESET)
            record.levelname = f"{color}{original_levelname}{ColorCodes.RESET}"
            record.name = f"{ColorCodes.CYAN}{original_name}{ColorCodes.RESET}"
            
            result = super().format(record)
            
            record.levelname = original_levelname
            record.name = original_name
            
            return result
        else:
            return super().format(record)


class Logger:
    """
    日誌處理器，單例模式（線程安全）
    """
    
    _instance = None
    _initialized = False
    _lock = threading.Lock()
    _logger_lock = threading.Lock()
    
    DETAILED_FORMAT = (
        '%(asctime)s | %(levelname)-8s | '
        '%(name)s | '
        '%(funcName)s:%(lineno)d | '
        '%(message)s'
    )
    
    SIMPLE_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
    
    FILE_FORMAT = (
        '%(asctime)s | %(levelname)-8s | '
        '%(name)s | '
        '%(module)s.%(funcName)s:%(lineno)d | '
        '%(process)d-%(thread)d | '
        '%(message)s'
    )
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._loggers: Dict[str, logging.Logger] = {}
        self._handlers: Dict[str, logging.Handler] = {}
        self._setup_logging()
        self._initialized = True
    
    def _setup_logging(self) -> None:
        """設置日誌系統"""
        try:
            # 從 TOML 配置讀取（使用小寫鍵名）
            log_level = config_manager.get('logging', 'level', 'INFO')
            use_detailed = config_manager.get_boolean('logging', 'detailed', True)
            console_format = self.DETAILED_FORMAT if use_detailed else self.SIMPLE_FORMAT
            use_color = config_manager.get_boolean('logging', 'color', True)
            log_to_console = config_manager.get_boolean('logging', 'log_to_console', True)
            log_to_file = config_manager.get_boolean('logging', 'log_to_file', True)
            
            self._setup_root_logger(log_level, console_format, use_color, log_to_console, log_to_file)
            
        except Exception as e:
            self._setup_fallback_logger()
            sys.stderr.write(f"日誌設置失敗，使用預設配置: {e}\n")
    
    def _setup_root_logger(self, log_level: str, console_format: str, 
                           use_color: bool = True,
                           log_to_console: bool = True,
                           log_to_file: bool = True) -> None:
        """設置根日誌記錄器"""
        if 'root' not in self._loggers:
            root_logger = logging.getLogger('spe_bank_recon')
            root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            root_logger.propagate = False
            self._loggers['root'] = root_logger
        else:
            root_logger = self._loggers['root']
        
        # ✅ 無論第一次(root in logger or not)還是後續，都先清理所有 handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()
        
        # 清理記錄的 handlers
        if 'console' in self._handlers:
            try:
                self._handlers['console'].close()
            except Exception as err:
                pass
            del self._handlers['console']
        
        if 'file' in self._handlers:
            try:
                self._handlers['file'].close()
            except Exception as err:
                pass
            del self._handlers['file']
        
        # 然後才添加新的 handlers
        # 控制台處理器
        if log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            
            console_formatter = ColoredFormatter(
                fmt=console_format,
                datefmt='%Y-%m-%d %H:%M:%S',
                use_color=use_color
            )
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)
            self._handlers['console'] = console_handler
        
        # 檔案處理器
        if log_to_file:
            log_path = config_manager.get('paths', 'log_path')
            if log_path:
                try:
                    self._setup_file_handler(root_logger, log_path)
                except Exception as e:
                    sys.stderr.write(f"設置檔案日誌處理器失敗: {e}\n")
    
    def _setup_file_handler(self, logger: logging.Logger, log_path: str) -> None:
        """設置檔案處理器"""
        try:
            log_dir = Path(log_path)
            log_dir.mkdir(parents=True, exist_ok=True)
            
            tz_offset = timezone(timedelta(hours=8))
            timestamp = datetime.now(tz_offset).strftime('%Y%m%d_%H%M%S')
            log_filename = f"spe_bank_recon_{timestamp}.log"
            log_file_path = log_dir / log_filename
            
            # 從配置讀取檔案大小和備份數量
            max_bytes = config_manager.get_int('logging', 'max_file_size_mb', 10) * 1024 * 1024
            backup_count = config_manager.get_int('logging', 'backup_count', 5)
            
            file_handler = RotatingFileHandler(
                log_file_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
            
            file_handler.setLevel(logging.DEBUG)
            
            file_formatter = logging.Formatter(
                self.FILE_FORMAT,
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            
            logger.addHandler(file_handler)
            self._handlers['file'] = file_handler
            
        except Exception as e:
            sys.stderr.write(f"創建檔案處理器失敗: {e}\n")
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        獲取日誌記錄器（線程安全）
        
        Args:
            name: 日誌記錄器名稱
            
        Returns:
            logging.Logger: 日誌記錄器
        """
        if name is None:
            name = 'root'
        
        with Logger._logger_lock:
            if name not in self._loggers:
                if name == 'root':
                    if 'root' not in self._loggers:
                        self._loggers['root'] = logging.getLogger('spe_bank_recon')
                    return self._loggers['root']
                else:
                    logger = logging.getLogger(f'spe_bank_recon.{name}')
                    logger.setLevel(logging.DEBUG)
                    self._loggers[name] = logger
            
            return self._loggers[name]
    
    def set_level(self, level: str, logger_name: Optional[str] = None) -> None:
        """設置日誌級別"""
        log_level = getattr(logging, level.upper(), logging.INFO)
        
        with Logger._logger_lock:
            if logger_name:
                if logger_name in self._loggers:
                    self._loggers[logger_name].setLevel(log_level)
            else:
                for logger in self._loggers.values():
                    logger.setLevel(log_level)
    
    def _setup_fallback_logger(self) -> None:
        """設置備用日誌配置"""
        with Logger._logger_lock:
            if 'root' not in self._loggers:
                root_logger = logging.getLogger('spe_bank_recon')
                root_logger.setLevel(logging.INFO)
                
                for handler in root_logger.handlers[:]:
                    root_logger.removeHandler(handler)
                    handler.close()
                
                root_logger.propagate = False
                
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(logging.INFO)
                formatter = ColoredFormatter(
                    fmt=self.SIMPLE_FORMAT,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    use_color=True
                )
                console_handler.setFormatter(formatter)
                root_logger.addHandler(console_handler)
                
                self._loggers['root'] = root_logger
                self._handlers['console'] = console_handler
    
    def cleanup(self) -> None:
        """清理日誌資源"""
        with Logger._logger_lock:
            for handler in self._handlers.values():
                try:
                    handler.close()
                except Exception as e:
                    sys.stderr.write(f"關閉日誌處理器失敗: {e}\n")
            
            self._handlers.clear()
            self._loggers.clear()
            self._initialized = False


class StructuredLogger:
    """
    結構化日誌記錄器
    """
    
    def __init__(self, logger_name: str = None):
        self.logger = Logger().get_logger(logger_name)
    
    def log_operation_start(self, operation: str, **kwargs) -> None:
        """記錄操作開始"""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        msg = f"▶ 開始執行: {operation}"
        if details:
            msg += f" | {details}"
        self.logger.info(msg)
    
    def log_operation_end(self, operation: str, success: bool = True, **kwargs) -> None:
        """記錄操作結束"""
        status = "✓ 成功" if success else "✗ 失敗"
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        msg = f"{status}: {operation}"
        if details:
            msg += f" | {details}"
        
        if success:
            self.logger.info(msg)
        else:
            self.logger.error(msg)
    
    def log_data_processing(self, data_type: str, record_count: int, 
                            processing_time: float = None, **kwargs) -> None:
        """記錄數據處理信息"""
        time_info = f"耗時 {processing_time:.2f}s" if processing_time else ""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"📊 處理 {data_type} 數據: {record_count:,} 筆記錄"
        if time_info:
            msg += f" | {time_info}"
        if details:
            msg += f" | {details}"
        
        self.logger.info(msg)
    
    def log_file_operation(self, operation: str, file_path: str, 
                           success: bool = True, **kwargs) -> None:
        """記錄檔案操作"""
        status = "✓" if success else "✗"
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"{status} 檔案{operation}: {file_path}"
        if details:
            msg += f" | {details}"
        
        if success:
            self.logger.info(msg)
        else:
            self.logger.error(msg)
    
    def log_error(self, error: Exception, context: str = None, **kwargs) -> None:
        """記錄錯誤信息"""
        context_info = f"[{context}] " if context else ""
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"❌ {context_info}錯誤: {str(error)}"
        if details:
            msg += f" | {details}"
        
        self.logger.error(msg, exc_info=True)
    
    def log_progress(self, current: int, total: int, operation: str = "", **kwargs) -> None:
        """記錄進度信息"""
        percentage = (current / total * 100) if total > 0 else 0
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        msg = f"⏳ 進度: {current}/{total} ({percentage:.1f}%)"
        if operation:
            msg += f" | {operation}"
        if details:
            msg += f" | {details}"
        
        self.logger.info(msg)
    
    def log_step_result(self, step_name: str, status: str, 
                        duration: float = None, **kwargs) -> None:
        """記錄步驟執行結果"""
        status_icons = {
            'success': '✅',
            'failed': '❌',
            'skipped': '⏭️',
            'pending': '⏳',
            'running': '🔄'
        }
        
        icon = status_icons.get(status.lower(), '•')
        msg = f"{icon} 步驟 [{step_name}]: {status}"
        
        if duration is not None:
            msg += f" (耗時: {duration:.2f}s)"
        
        details = ' '.join([f"{k}={v}" for k, v in kwargs.items()])
        if details:
            msg += f" | {details}"
        
        if status.lower() == 'failed':
            self.logger.error(msg)
        else:
            self.logger.info(msg)


# 全域日誌管理器實例
logger_manager = Logger()


# 便利函數
def get_logger(name: str = None) -> logging.Logger:
    """
    獲取日誌記錄器
    
    Args:
        name: 日誌記錄器名稱
        
    Returns:
        logging.Logger: 日誌記錄器
    """
    return logger_manager.get_logger(name)


def get_structured_logger(name: str = None) -> StructuredLogger:
    """
    獲取結構化日誌記錄器
    
    Args:
        name: 日誌記錄器名稱
        
    Returns:
        StructuredLogger: 結構化日誌記錄器
    """
    return StructuredLogger(name)
