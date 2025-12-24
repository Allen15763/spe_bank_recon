"""
Pipeline 步驟工具模組
提供標準化的 metadata 構建和錯誤處理工具
"""

import traceback
from typing import Dict, Any, Optional
from datetime import datetime


class StepMetadataBuilder:
    """
    StepResult metadata 構建器
    提供標準化的 metadata 結構和鏈式 API
    
    使用範例:
        metadata = (StepMetadataBuilder()
                    .set_row_counts(1000, 950)
                    .set_process_counts(processed=950, skipped=50)
                    .set_time_info(start_time, end_time)
                    .add_custom('filter_pattern', 'SPX')
                    .build())
    """
    
    def __init__(self):
        self.metadata: Dict[str, Any] = {
            # 基本統計
            'input_rows': 0,
            'output_rows': 0,
            'rows_changed': 0,
            
            # 處理統計
            'records_processed': 0,
            'records_skipped': 0,
            'records_failed': 0,
            
            # 時間資訊
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0.0,
        }
    
    def set_row_counts(self, input_rows: int, output_rows: int) -> 'StepMetadataBuilder':
        """
        設置行數統計
        
        Args:
            input_rows: 輸入行數
            output_rows: 輸出行數
        """
        self.metadata['input_rows'] = int(input_rows)
        self.metadata['output_rows'] = int(output_rows)
        self.metadata['rows_changed'] = int(output_rows - input_rows)
        return self
    
    def set_process_counts(self, 
                           processed: int = 0, 
                           skipped: int = 0, 
                           failed: int = 0) -> 'StepMetadataBuilder':
        """
        設置處理計數
        
        Args:
            processed: 成功處理的記錄數
            skipped: 跳過的記錄數
            failed: 失敗的記錄數
        """
        self.metadata['records_processed'] = int(processed)
        self.metadata['records_skipped'] = int(skipped)
        self.metadata['records_failed'] = int(failed)
        return self
    
    def set_time_info(self, 
                      start_time: datetime, 
                      end_time: datetime) -> 'StepMetadataBuilder':
        """
        設置時間資訊
        
        Args:
            start_time: 開始時間
            end_time: 結束時間
        """
        self.metadata['start_time'] = start_time.isoformat()
        self.metadata['end_time'] = end_time.isoformat()
        self.metadata['duration_seconds'] = (end_time - start_time).total_seconds()
        return self
    
    def set_duration(self, duration: float) -> 'StepMetadataBuilder':
        """
        直接設置執行時長
        
        Args:
            duration: 執行時長（秒）
        """
        self.metadata['duration_seconds'] = duration
        return self
    
    def set_file_info(self, 
                      input_file: str = None, 
                      output_file: str = None) -> 'StepMetadataBuilder':
        """
        設置檔案資訊
        
        Args:
            input_file: 輸入檔案路徑
            output_file: 輸出檔案路徑
        """
        if input_file:
            self.metadata['input_file'] = str(input_file)
        if output_file:
            self.metadata['output_file'] = str(output_file)
        return self
    
    def set_data_info(self, 
                      columns: int = None, 
                      column_names: list = None) -> 'StepMetadataBuilder':
        """
        設置資料資訊
        
        Args:
            columns: 欄位數
            column_names: 欄位名稱列表
        """
        if columns is not None:
            self.metadata['columns'] = int(columns)
        if column_names is not None:
            self.metadata['column_names'] = column_names[:20]  # 只保留前20個
        return self
    
    def add_custom(self, key: str, value: Any) -> 'StepMetadataBuilder':
        """
        添加自定義 metadata
        
        Args:
            key: 鍵名
            value: 值
        """
        self.metadata[key] = value
        return self
    
    def add_multiple(self, **kwargs) -> 'StepMetadataBuilder':
        """
        批量添加自定義 metadata
        
        Args:
            **kwargs: 鍵值對
        """
        self.metadata.update(kwargs)
        return self
    
    def build(self) -> Dict[str, Any]:
        """
        構建並返回 metadata 字典
        
        Returns:
            Dict[str, Any]: metadata 字典
        """
        # 移除值為 None 或 0 的項目（可選）
        return {k: v for k, v in self.metadata.items() if v is not None}


def create_error_metadata(
    error: Exception, 
    context: 'ProcessingContext', 
    step_name: str,
    include_traceback: bool = True,
    include_data_snapshot: bool = True,
    **kwargs
) -> Dict[str, Any]:
    """
    創建增強的錯誤 metadata
    
    Args:
        error: 發生的異常
        context: 處理上下文
        step_name: 步驟名稱
        include_traceback: 是否包含 traceback
        include_data_snapshot: 是否包含數據快照
        **kwargs: 額外的上下文資訊
    
    Returns:
        Dict[str, Any]: 錯誤 metadata 字典
        
    使用範例:
        error_metadata = create_error_metadata(
            e, context, self.name,
            stage='data_loading',
            file_path=file_path
        )
    """
    error_metadata = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'step_name': step_name,
        'timestamp': datetime.now().isoformat(),
    }
    
    # 添加 traceback
    if include_traceback:
        error_metadata['error_traceback'] = traceback.format_exc()
    
    # 添加數據快照
    if include_data_snapshot and context is not None:
        if context.data is not None and not context.data.empty:
            error_metadata['data_snapshot'] = {
                'total_rows': len(context.data),
                'total_columns': len(context.data.columns),
                'columns': list(context.data.columns)[:20],
                'dtypes': {str(k): str(v) for k, v in context.data.dtypes.items()}
            }
        else:
            error_metadata['data_snapshot'] = {'status': 'no_data_or_empty'}
        
        # 添加輔助數據摘要
        aux_data_names = context.list_auxiliary_data()
        if aux_data_names:
            error_metadata['auxiliary_data'] = aux_data_names
        
        # 添加上下文變量（限制長度）
        if hasattr(context, '_variables') and context._variables:
            error_metadata['context_variables'] = {
                k: str(v)[:100] for k, v in context._variables.items()
            }
    
    # 添加額外資訊
    error_metadata.update(kwargs)
    
    return error_metadata


def create_success_metadata(
    input_rows: int,
    output_rows: int,
    duration: float,
    **kwargs
) -> Dict[str, Any]:
    """
    快速創建成功的 metadata
    
    Args:
        input_rows: 輸入行數
        output_rows: 輸出行數
        duration: 執行時長（秒）
        **kwargs: 額外資訊
        
    Returns:
        Dict[str, Any]: metadata 字典
    """
    metadata = {
        'input_rows': input_rows,
        'output_rows': output_rows,
        'rows_changed': output_rows - input_rows,
        'duration_seconds': round(duration, 3),
        'processing_speed': f"{input_rows / duration:.0f} rows/s" if duration > 0 else "N/A",
    }
    metadata.update(kwargs)
    return metadata


def format_duration(seconds: float) -> str:
    """
    格式化時長為人類可讀格式
    
    Args:
        seconds: 秒數
        
    Returns:
        str: 格式化的時長字串
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
