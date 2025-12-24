"""
Pipeline 模組
提供數據處理流程管理
"""

from .base import (
    PipelineStep,
    StepResult,
    StepStatus,
    FunctionStep,
    ConditionalStep,
    SequentialStep,
)
from .context import ProcessingContext, ValidationResult, ContextMetadata
from .pipeline import Pipeline, PipelineBuilder, PipelineConfig, PipelineExecutor
from .steps import (
    # 通用步驟
    DataLoadingStep,
    DataExportStep,
    DataCleaningStep,
    DataTransformStep,
    DataMergeStep,
    DataValidationStep,
    DataFilterStep,
)
from .checkpoint import (
    CheckpointManager,
    PipelineWithCheckpoint,
    execute_with_checkpoint,
    resume_from_checkpoint,
    list_available_checkpoints,
    quick_test_step,
)

__all__ = [
    # 基類
    'PipelineStep',
    'StepResult',
    'StepStatus',
    'FunctionStep',
    'ConditionalStep',
    'SequentialStep',
    # 上下文
    'ProcessingContext',
    'ValidationResult',
    'ContextMetadata',
    # Pipeline
    'Pipeline',
    'PipelineBuilder',
    'PipelineConfig',
    'PipelineExecutor',
    # 通用步驟
    'DataLoadingStep',
    'DataExportStep',
    'DataCleaningStep',
    'DataTransformStep',
    'DataMergeStep',
    'DataValidationStep',
    'DataFilterStep',
    # Checkpoint
    'CheckpointManager',
    'PipelineWithCheckpoint',
    'execute_with_checkpoint',
    'resume_from_checkpoint',
    'list_available_checkpoints',
    'quick_test_step',
]
