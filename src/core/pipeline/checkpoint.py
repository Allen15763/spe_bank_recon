"""
Pipeline Checkpoint 系統
提供 pipeline 執行狀態的儲存和恢復功能（同步版本）

功能:
1. 儲存 pipeline 執行的中間狀態
2. 從指定步驟恢復執行
3. 快速測試後續步驟

使用方式:
    # 首次執行 - 自動儲存 checkpoint
    result = execute_with_checkpoint(pipeline, context, save_checkpoints=True)
    
    # 從特定步驟恢復
    result = resume_from_checkpoint(
        checkpoint_name="task_202501_after_Clean_Data",
        start_from_step="Transform_Data",
        pipeline=pipeline
    )
"""

import json
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime

import pandas as pd

from .context import ProcessingContext
from .pipeline import Pipeline
from .base import StepResult, StepStatus
from src.utils import get_logger, config_manager


class CheckpointManager:
    """Pipeline Checkpoint 管理器"""
    
    def __init__(self, checkpoint_dir: str = None):
        """
        初始化 Checkpoint 管理器
        
        Args:
            checkpoint_dir: checkpoint 儲存目錄，預設從配置讀取
        """
        if checkpoint_dir is None:
            checkpoint_dir = config_manager.get('paths', 'temp_path', './checkpoints')
        
        self.checkpoint_dir = Path(checkpoint_dir) / 'checkpoints'
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("checkpoint")
    
    def save_checkpoint(
        self,
        context: ProcessingContext,
        step_name: str,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        儲存 checkpoint
        
        Args:
            context: 處理上下文
            step_name: 步驟名稱
            metadata: 額外的元數據
            
        Returns:
            str: checkpoint 名稱
        """
        # 生成 checkpoint 名稱
        task_name = context.metadata.task_name or "unknown"
        task_type = context.metadata.task_type or "unknown"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        checkpoint_name = f"{task_name}_{task_type}_after_{step_name}"
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        checkpoint_path.mkdir(parents=True, exist_ok=True)
        
        # 儲存主數據
        if context.data is not None and not context.data.empty:
            data_path = checkpoint_path / "data.parquet"
            try:
                context.data.to_parquet(data_path, index=False)
            except Exception as e:
                # 如果 parquet 失敗，嘗試 pickle
                self.logger.warning(f"Parquet 儲存失敗，嘗試 pickle: {e}")
                data_path = checkpoint_path / "data.pkl"
                context.data.to_pickle(data_path)
        
        # 儲存輔助數據
        aux_data_dir = checkpoint_path / "auxiliary_data"
        aux_data_dir.mkdir(exist_ok=True)
        
        for aux_name in context.list_auxiliary_data():
            aux_data = context.get_auxiliary_data(aux_name)
            if aux_data is not None and not aux_data.empty:
                try:
                    aux_path = aux_data_dir / f"{aux_name}.parquet"
                    aux_data.to_parquet(aux_path, index=False)
                except Exception as e:
                    self.logger.warning(f"輔助數據 {aux_name} parquet 儲存失敗: {e}")
                    try:
                        aux_path = aux_data_dir / f"{aux_name}.pkl"
                        aux_data.to_pickle(aux_path)
                    except Exception as e2:
                        self.logger.error(f"輔助數據 {aux_name} 儲存失敗: {e2}")
        
        # 儲存變數和元數據（序列化安全處理）
        safe_variables = {}
        for k, v in context._variables.items():
            try:
                json.dumps(v)  # 測試是否可序列化
                safe_variables[k] = v
            except (TypeError, ValueError):
                safe_variables[k] = str(v)  # 轉為字串
        
        checkpoint_info = {
            'step_name': step_name,
            'task_name': context.metadata.task_name,
            'task_type': context.metadata.task_type,
            'variables': safe_variables,
            'warnings': context.warnings,
            'errors': context.errors,
            'timestamp': timestamp,
            'auxiliary_data_list': context.list_auxiliary_data(),
            'data_shape': list(context.data.shape) if context.data is not None else [0, 0],
            'metadata': metadata or {}
        }
        
        with open(checkpoint_path / "checkpoint_info.json", 'w', encoding='utf-8') as f:
            json.dump(checkpoint_info, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"✅ Checkpoint 已儲存: {checkpoint_name}")
        return checkpoint_name
    
    def load_checkpoint(self, checkpoint_name: str) -> ProcessingContext:
        """
        載入 checkpoint
        
        Args:
            checkpoint_name: checkpoint 名稱
            
        Returns:
            ProcessingContext: 恢復的上下文
        """
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Checkpoint 不存在: {checkpoint_name}")
        
        # 載入元數據
        with open(checkpoint_path / "checkpoint_info.json", 'r', encoding='utf-8') as f:
            info = json.load(f)
        
        # 載入主數據
        data_parquet = checkpoint_path / "data.parquet"
        data_pkl = checkpoint_path / "data.pkl"
        
        if data_parquet.exists():
            data = pd.read_parquet(data_parquet)
        elif data_pkl.exists():
            data = pd.read_pickle(data_pkl)
        else:
            data = pd.DataFrame()
        
        # 創建上下文
        context = ProcessingContext(
            data=data,
            task_name=info['task_name'],
            task_type=info['task_type']
        )
        
        # 恢復變數
        for key, value in info['variables'].items():
            context.set_variable(key, value)
        
        # 恢復警告和錯誤
        context.warnings = info.get('warnings', [])
        context.errors = info.get('errors', [])
        
        # 恢復輔助數據
        aux_data_dir = checkpoint_path / "auxiliary_data"
        if aux_data_dir.exists():
            for aux_file in aux_data_dir.glob("*.parquet"):
                aux_name = aux_file.stem
                aux_data = pd.read_parquet(aux_file)
                context.add_auxiliary_data(aux_name, aux_data)
            
            for aux_file in aux_data_dir.glob("*.pkl"):
                aux_name = aux_file.stem
                if not context.has_auxiliary_data(aux_name):  # 避免重複
                    aux_data = pd.read_pickle(aux_file)
                    context.add_auxiliary_data(aux_name, aux_data)
        
        self.logger.info(f"✅ Checkpoint 已載入: {checkpoint_name}")
        self.logger.info(f"   - 主數據: {len(context.data)} 行")
        self.logger.info(f"   - 輔助數據: {len(context.list_auxiliary_data())} 個")
        self.logger.info(f"   - 變數: {len(context._variables)} 個")
        
        return context
    
    def list_checkpoints(self, filter_task: str = None) -> List[Dict]:
        """
        列出所有可用的 checkpoint
        
        Args:
            filter_task: 過濾特定任務名稱
            
        Returns:
            List[Dict]: checkpoint 資訊列表
        """
        checkpoints = []
        
        for checkpoint_path in self.checkpoint_dir.iterdir():
            if checkpoint_path.is_dir():
                info_file = checkpoint_path / "checkpoint_info.json"
                if info_file.exists():
                    try:
                        with open(info_file, 'r', encoding='utf-8') as f:
                            info = json.load(f)
                        
                        # 過濾任務
                        if filter_task and info.get('task_name') != filter_task:
                            continue
                        
                        checkpoints.append({
                            'name': checkpoint_path.name,
                            'step': info['step_name'],
                            'task_name': info.get('task_name', 'unknown'),
                            'task_type': info.get('task_type', 'unknown'),
                            'timestamp': info['timestamp'],
                            'data_shape': info.get('data_shape', [0, 0])
                        })
                    except Exception as e:
                        self.logger.warning(f"讀取 checkpoint 資訊失敗: {checkpoint_path.name}, {e}")
        
        return sorted(checkpoints, key=lambda x: x['timestamp'], reverse=True)
    
    def delete_checkpoint(self, checkpoint_name: str) -> bool:
        """
        刪除指定的 checkpoint
        
        Args:
            checkpoint_name: checkpoint 名稱
            
        Returns:
            bool: 是否成功刪除
        """
        checkpoint_path = self.checkpoint_dir / checkpoint_name
        if checkpoint_path.exists():
            shutil.rmtree(checkpoint_path)
            self.logger.info(f"✅ Checkpoint 已刪除: {checkpoint_name}")
            return True
        return False
    
    def cleanup_old_checkpoints(self, keep_last: int = 5, task_name: str = None):
        """
        清理舊的 checkpoint，保留最近的 N 個
        
        Args:
            keep_last: 保留最近的數量
            task_name: 指定任務名稱，None 表示所有
        """
        checkpoints = self.list_checkpoints(filter_task=task_name)
        
        if len(checkpoints) > keep_last:
            to_delete = checkpoints[keep_last:]
            for cp in to_delete:
                self.delete_checkpoint(cp['name'])
            
            self.logger.info(f"清理了 {len(to_delete)} 個舊 checkpoint")


class PipelineWithCheckpoint:
    """
    帶 Checkpoint 功能的 Pipeline 執行器
    """
    
    def __init__(self, pipeline: Pipeline, checkpoint_manager: CheckpointManager = None):
        """
        初始化
        
        Args:
            pipeline: Pipeline 實例
            checkpoint_manager: Checkpoint 管理器，None 則自動創建
        """
        self.pipeline = pipeline
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.logger = get_logger("pipeline.checkpoint")
    
    def execute_with_checkpoint(
        self,
        context: ProcessingContext,
        save_after_each_step: bool = True,
        start_from_step: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        執行 Pipeline 並自動儲存 checkpoint
        
        Args:
            context: 處理上下文
            save_after_each_step: 是否在每個步驟後儲存 checkpoint
            start_from_step: 從哪個步驟開始執行 (None = 從頭開始)
            
        Returns:
            Dict: 執行結果
        """
        start_time = datetime.now()
        
        # 找到起始步驟的索引
        start_index = 0
        if start_from_step:
            for i, step in enumerate(self.pipeline.steps):
                if step.name == start_from_step:
                    start_index = i
                    self.logger.info(f"🔄 從步驟 '{start_from_step}' 開始執行 (跳過前 {i} 個步驟)")
                    break
            else:
                raise ValueError(f"找不到步驟: {start_from_step}")
        
        # 執行步驟
        results = []
        for i, step in enumerate(self.pipeline.steps[start_index:], start=start_index):
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"執行步驟 {i+1}/{len(self.pipeline.steps)}: {step.name}")
            self.logger.info(f"{'='*60}")
            
            # 執行步驟
            result = step(context)
            results.append(result)
            
            # 記錄到上下文歷史
            context.add_history(step.name, result.status.value)
            
            # 儲存 checkpoint
            if save_after_each_step and result.is_success:
                self.checkpoint_manager.save_checkpoint(
                    context=context,
                    step_name=step.name,
                    metadata={
                        'step_index': i,
                        'step_status': result.status.value,
                        'step_message': result.message
                    }
                )
            
            # 如果失敗且設定為遇錯即停
            if not result.is_success and self.pipeline.config.stop_on_error:
                self.logger.error(f"❌ 步驟失敗，停止執行: {result.message}")
                break
        
        # 彙總結果
        end_time = datetime.now()
        successful = sum(1 for r in results if r.is_success)
        failed = sum(1 for r in results if r.is_failed)
        skipped = sum(1 for r in results if r.status == StepStatus.SKIPPED)
        
        return {
            'success': failed == 0,
            'pipeline': self.pipeline.config.name,
            'start_time': start_time,
            'end_time': end_time,
            'duration': (end_time - start_time).total_seconds(),
            'total_steps': len(self.pipeline.steps),
            'executed_steps': len(results),
            'successful_steps': successful,
            'failed_steps': failed,
            'skipped_steps': skipped,
            'results': [r.to_dict() for r in results],
            'context': context
        }


# =============================================================================
# 便捷函數
# =============================================================================

def execute_with_checkpoint(
    pipeline: Pipeline,
    context: ProcessingContext,
    checkpoint_dir: str = None,
    save_checkpoints: bool = True
) -> Dict[str, Any]:
    """
    執行 pipeline 並自動儲存 checkpoint
    
    Args:
        pipeline: Pipeline 實例
        context: 處理上下文
        checkpoint_dir: checkpoint 儲存目錄
        save_checkpoints: 是否儲存 checkpoint
        
    Returns:
        Dict: 執行結果
    """
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    
    return executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints
    )


def resume_from_checkpoint(
    checkpoint_name: str,
    start_from_step: str,
    pipeline: Pipeline,
    checkpoint_dir: str = None,
    save_checkpoints: bool = True
) -> Dict[str, Any]:
    """
    從 checkpoint 恢復並從指定步驟開始執行
    
    Args:
        checkpoint_name: checkpoint 名稱
        start_from_step: 從哪個步驟開始
        pipeline: Pipeline 實例
        checkpoint_dir: checkpoint 目錄
        save_checkpoints: 是否儲存新的 checkpoint
        
    Returns:
        Dict: 執行結果
    """
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    context = checkpoint_manager.load_checkpoint(checkpoint_name)
    
    executor = PipelineWithCheckpoint(pipeline, checkpoint_manager)
    
    return executor.execute_with_checkpoint(
        context=context,
        save_after_each_step=save_checkpoints,
        start_from_step=start_from_step
    )


def list_available_checkpoints(
    checkpoint_dir: str = None,
    task_name: str = None
) -> List[Dict]:
    """
    列出可用的 checkpoint
    
    Args:
        checkpoint_dir: checkpoint 目錄
        task_name: 過濾特定任務
        
    Returns:
        List[Dict]: checkpoint 資訊列表
    """
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    return checkpoint_manager.list_checkpoints(filter_task=task_name)


def quick_test_step(
    checkpoint_name: str,
    step_to_test: str,
    pipeline: Pipeline,
    checkpoint_dir: str = None
) -> Dict[str, Any]:
    """
    快速測試單一步驟 (從上一個 checkpoint 恢復)
    
    Args:
        checkpoint_name: checkpoint 名稱
        step_to_test: 要測試的步驟名稱
        pipeline: Pipeline 實例
        checkpoint_dir: checkpoint 目錄
        
    Returns:
        Dict: 執行結果
    """
    return resume_from_checkpoint(
        checkpoint_name=checkpoint_name,
        start_from_step=step_to_test,
        pipeline=pipeline,
        checkpoint_dir=checkpoint_dir,
        save_checkpoints=False  # 測試時不儲存
    )
