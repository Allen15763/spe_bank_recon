"""
處理上下文
在Pipeline步驟間傳遞數據和狀態
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd
import logging


@dataclass
class ValidationResult:
    """驗證結果"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, error: str):
        """添加錯誤"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """添加警告"""
        self.warnings.append(warning)


@dataclass
class ContextMetadata:
    """上下文元數據"""
    task_name: str  # 任務名稱
    task_type: str  # 任務類型：transform, compare, report
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def update(self):
        """更新時間戳"""
        self.updated_at = datetime.now()


class ProcessingContext:
    """
    處理上下文
    在整個Pipeline執行過程中傳遞數據和狀態
    """
    
    def __init__(self,
                 data: pd.DataFrame = None,
                 task_name: str = "default_task",
                 task_type: str = "transform"):
        """
        初始化處理上下文
        
        Args:
            data: 主要數據
            task_name: 任務名稱
            task_type: 任務類型 (transform/compare/report)
        """
        self.data = data if data is not None else pd.DataFrame()
        self.metadata = ContextMetadata(
            task_name=task_name,
            task_type=task_type
        )
        
        # 輔助數據存儲
        self._auxiliary_data: Dict[str, pd.DataFrame] = {}
        
        # 共享變量存儲
        self._variables: Dict[str, Any] = {}
        
        # 錯誤和警告
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
        # 步驟執行歷史
        self._history: List[Dict[str, Any]] = []
        
        # 驗證結果
        self._validations: Dict[str, ValidationResult] = {}
        
        self.logger = logging.getLogger(f"Context.{task_name}")
    
    # === 主數據操作 ===
    
    def update_data(self, data: pd.DataFrame):
        """更新主數據"""
        self.data = data
        self.metadata.update()
    
    def get_data_copy(self) -> pd.DataFrame:
        """獲取數據副本"""
        return self.data.copy()
    
    # === 輔助數據操作 ===
    
    def add_auxiliary_data(self, name: str, data: pd.DataFrame):
        """添加輔助數據"""
        self._auxiliary_data[name] = data
        self.logger.debug(f"Added auxiliary data: {name} ({len(data)} rows)")
    
    def get_auxiliary_data(self, name: str) -> Optional[pd.DataFrame]:
        """獲取輔助數據"""
        return self._auxiliary_data.get(name)
    
    def has_auxiliary_data(self, name: str) -> bool:
        """檢查是否有指定的輔助數據"""
        return name in self._auxiliary_data
    
    def list_auxiliary_data(self) -> List[str]:
        """列出所有輔助數據名稱"""
        return list(self._auxiliary_data.keys())
    
    # === 變量存儲 ===
    
    def set_variable(self, key: str, value: Any):
        """設置共享變量"""
        self._variables[key] = value
    
    def get_variable(self, key: str, default: Any = None) -> Any:
        """獲取共享變量"""
        return self._variables.get(key, default)
    
    def has_variable(self, key: str) -> bool:
        """檢查是否有指定變量"""
        return key in self._variables
    
    # === 錯誤和警告 ===
    
    def add_error(self, error: str):
        """添加錯誤"""
        self.errors.append(error)
        self.logger.error(error)
    
    def add_warning(self, warning: str):
        """添加警告"""
        self.warnings.append(warning)
        self.logger.warning(warning)
    
    def has_errors(self) -> bool:
        """是否有錯誤"""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """是否有警告"""
        return len(self.warnings) > 0
    
    def clear_errors(self):
        """清除錯誤"""
        self.errors.clear()
    
    def clear_warnings(self):
        """清除警告"""
        self.warnings.clear()
    
    # === 驗證管理 ===
    
    def add_validation(self, name: str, result: ValidationResult):
        """添加驗證結果"""
        self._validations[name] = result
        
        for error in result.errors:
            self.add_error(f"[{name}] {error}")
        for warning in result.warnings:
            self.add_warning(f"[{name}] {warning}")
    
    def get_validation(self, name: str) -> Optional[ValidationResult]:
        """獲取驗證結果"""
        return self._validations.get(name)
    
    def is_valid(self) -> bool:
        """檢查所有驗證是否通過"""
        return all(v.is_valid for v in self._validations.values())
    
    # === 歷史記錄 ===
    
    def add_history(self, step_name: str, status: str, **kwargs):
        """添加步驟執行歷史"""
        record = {
            'step': step_name,
            'status': status,
            'timestamp': datetime.now(),
            **kwargs
        }
        self._history.append(record)
    
    def get_history(self) -> List[Dict[str, Any]]:
        """獲取執行歷史"""
        return self._history.copy()
    
    def get_last_step(self) -> Optional[Dict[str, Any]]:
        """獲取最後執行的步驟"""
        return self._history[-1] if self._history else None
    
    # === 實用方法 ===
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return {
            'task_name': self.metadata.task_name,
            'task_type': self.metadata.task_type,
            'data_shape': self.data.shape if self.data is not None else (0, 0),
            'auxiliary_data': list(self._auxiliary_data.keys()),
            'variables': list(self._variables.keys()),
            'errors': len(self.errors),
            'warnings': len(self.warnings),
            'validations': list(self._validations.keys()),
            'history_steps': len(self._history)
        }
    
    def __repr__(self) -> str:
        task = self.metadata.task_name
        task_type = self.metadata.task_type
        return f"ProcessingContext(task={task}, type={task_type})"
