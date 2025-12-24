"""
Pipeline 步驟基類
定義所有處理步驟的抽象接口（同步版本）
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Any, Dict, List, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
import time

from offline_tasks.utils.logging import get_logger
from .context import ProcessingContext


class StepStatus(Enum):
    """步驟執行狀態"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"


@dataclass
class StepResult:
    """步驟執行結果"""
    step_name: str
    status: StepStatus
    data: Optional[pd.DataFrame] = None
    error: Optional[Exception] = None
    message: Optional[str] = None
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_success(self) -> bool:
        return self.status == StepStatus.SUCCESS
    
    @property
    def is_failed(self) -> bool:
        return self.status == StepStatus.FAILED
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return {
            'step_name': self.step_name,
            'status': self.status.value,
            'message': self.message,
            'duration': self.duration,
            'metadata': self.metadata,
            'error': str(self.error) if self.error else None
        }


T = TypeVar('T')


class PipelineStep(ABC, Generic[T]):
    """
    Pipeline 步驟基類（同步版本）
    所有處理步驟必須繼承此類
    """
    
    def __init__(self, 
                 name: str,
                 description: str = "",
                 required: bool = True,
                 retry_count: int = 0,
                 timeout: Optional[float] = None):
        """
        初始化步驟
        
        Args:
            name: 步驟名稱
            description: 步驟描述
            required: 是否必需（失敗時是否停止Pipeline）
            retry_count: 重試次數
            timeout: 超時時間（秒）- 同步版本僅作記錄
        """
        self.name = name
        self.description = description
        self.required = required
        self.retry_count = retry_count
        self.timeout = timeout
        self.logger = get_logger(f"pipeline.{name}")
        self._prerequisites = []
        self._post_actions = []
    
    @abstractmethod
    def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行步驟邏輯
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 執行結果
        """
        pass
    
    def validate_input(self, context: ProcessingContext) -> bool:
        """
        驗證輸入是否符合要求（可選覆寫）
        
        Args:
            context: 處理上下文
            
        Returns:
            bool: 是否通過驗證
        """
        return True
    
    def rollback(self, context: ProcessingContext, error: Exception):
        """
        回滾操作（可選實現）
        
        Args:
            context: 處理上下文
            error: 觸發回滾的錯誤
        """
        self.logger.warning(f"Rollback not implemented for {self.name}")
    
    def __call__(self, context: ProcessingContext) -> StepResult:
        """
        使步驟可調用，包含完整的執行流程
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 執行結果
        """
        start_time = time.time()
        
        try:
            # 執行前置檢查
            if not self.validate_input(context):
                if self.required:
                    raise ValueError(f"Input validation failed for step {self.name}")
                else:
                    self.logger.warning(f"Skipping step {self.name} due to validation failure")
                    return StepResult(
                        step_name=self.name,
                        status=StepStatus.SKIPPED,
                        message="Input validation failed"
                    )
            
            # 執行前置動作
            for action in self._prerequisites:
                action(context)
            
            # 執行主邏輯（支援重試）
            result = None
            last_error = None
            
            for attempt in range(self.retry_count + 1):
                try:
                    result = self.execute(context)
                    break
                    
                except Exception as e:
                    last_error = e
                    if attempt < self.retry_count:
                        self.logger.warning(f"Step {self.name} failed, retrying... ({attempt + 1}/{self.retry_count})")
                        time.sleep(2 ** attempt)  # 指數退避
                    else:
                        self.logger.error(f"Step {self.name} failed after {self.retry_count + 1} attempts")
            
            # 如果所有重試都失敗
            if result is None:
                if self.required:
                    self.rollback(context, last_error)
                    raise last_error
                else:
                    result = StepResult(
                        step_name=self.name,
                        status=StepStatus.FAILED,
                        error=last_error,
                        message=str(last_error)
                    )
            
            # 執行後置動作
            for action in self._post_actions:
                action(context)
            
            # 計算執行時間
            duration = time.time() - start_time
            result.duration = duration
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e),
                duration=duration
            )
    
    def add_prerequisite(self, action: Callable):
        """添加前置動作"""
        self._prerequisites.append(action)
        return self
    
    def add_post_action(self, action: Callable):
        """添加後置動作"""
        self._post_actions.append(action)
        return self
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"


class FunctionStep(PipelineStep):
    """
    函數式步驟：用函數定義處理邏輯
    適合簡單的處理邏輯
    """
    
    def __init__(self,
                 name: str,
                 func: Callable[[ProcessingContext], pd.DataFrame],
                 **kwargs):
        """
        初始化函數式步驟
        
        Args:
            name: 步驟名稱
            func: 處理函數，接受 context 返回 DataFrame
            **kwargs: 其他參數
        """
        super().__init__(name, **kwargs)
        self.func = func
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行函數"""
        try:
            result_data = self.func(context)
            
            if result_data is not None:
                context.update_data(result_data)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=result_data,
                message="Function executed successfully"
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Function execution failed: {str(e)}"
            )


class ConditionalStep(PipelineStep):
    """
    條件步驟：根據條件決定是否執行
    """
    
    def __init__(self,
                 name: str,
                 condition: Callable[[ProcessingContext], bool],
                 true_step: PipelineStep,
                 false_step: Optional[PipelineStep] = None,
                 **kwargs):
        """
        初始化條件步驟
        
        Args:
            name: 步驟名稱
            condition: 條件函數
            true_step: 條件為真時執行的步驟
            false_step: 條件為假時執行的步驟（可選）
        """
        super().__init__(name, **kwargs)
        self.condition = condition
        self.true_step = true_step
        self.false_step = false_step
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行條件步驟"""
        try:
            condition_result = self.condition(context)
            
            if condition_result:
                self.logger.info(f"Condition met, executing {self.true_step.name}")
                return self.true_step(context)
            elif self.false_step:
                self.logger.info(f"Condition not met, executing {self.false_step.name}")
                return self.false_step(context)
            else:
                self.logger.info("Condition not met, skipping")
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SKIPPED,
                    message="Condition not met, no false step defined"
                )
                
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Conditional step failed: {str(e)}"
            )


class SequentialStep(PipelineStep):
    """
    順序步驟：依序執行多個步驟
    """
    
    def __init__(self,
                 name: str,
                 steps: List[PipelineStep],
                 stop_on_failure: bool = True,
                 **kwargs):
        """
        初始化順序步驟
        
        Args:
            name: 步驟名稱
            steps: 要順序執行的步驟列表
            stop_on_failure: 失敗時是否停止
        """
        super().__init__(name, **kwargs)
        self.steps = steps
        self.stop_on_failure = stop_on_failure
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """順序執行所有步驟"""
        completed_steps = []
        
        try:
            for step in self.steps:
                self.logger.info(f"Executing step {step.name}")
                result = step(context)
                
                completed_steps.append(result)
                
                if result.is_failed and self.stop_on_failure:
                    return StepResult(
                        step_name=self.name,
                        status=StepStatus.FAILED,
                        message=f"Step {step.name} failed",
                        metadata={
                            'failed_at': step.name,
                            'completed': [s.step_name for s in completed_steps]
                        }
                    )
            
            failed_steps = [s.step_name for s in completed_steps if s.is_failed]
            
            if failed_steps:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message=f"Some steps failed: {', '.join(failed_steps)}",
                    metadata={
                        'failed': failed_steps,
                        'total': len(self.steps)
                    }
                )
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"All {len(self.steps)} steps completed successfully",
                metadata={'completed_steps': [s.step_name for s in completed_steps]}
            )
            
        except Exception as e:
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"Sequential execution failed: {str(e)}",
                metadata={'completed_steps': [s.step_name for s in completed_steps]}
            )
    
    def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return len(self.steps) > 0
