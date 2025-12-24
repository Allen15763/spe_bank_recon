"""
通用處理步驟
提供常用的數據處理步驟模板
"""

import pandas as pd
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path

from ..base import PipelineStep, StepResult, StepStatus
from ..context import ProcessingContext, ValidationResult
from offline_tasks.utils.logging import get_logger


class DataLoadingStep(PipelineStep):
    """
    數據載入步驟
    從文件載入數據到上下文
    """
    
    def __init__(self,
                 name: str,
                 file_path: str,
                 file_type: str = 'auto',
                 target: str = 'main',  # 'main' 或輔助數據名稱
                 read_params: Dict[str, Any] = None,
                 **kwargs):
        """
        初始化數據載入步驟
        
        Args:
            name: 步驟名稱
            file_path: 文件路徑
            file_type: 文件類型 ('auto', 'excel', 'csv')
            target: 載入目標 ('main' 為主數據，其他為輔助數據名稱)
            read_params: 讀取參數
        """
        super().__init__(name, **kwargs)
        self.file_path = Path(file_path)
        self.file_type = file_type
        self.target = target
        self.read_params = read_params or {}
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據載入"""
        try:
            self.logger.info(f"Loading data from: {self.file_path}")
            
            # 自動識別文件類型
            if self.file_type == 'auto':
                ext = self.file_path.suffix.lower()
                if ext in ['.xlsx', '.xls']:
                    file_type = 'excel'
                elif ext == '.csv':
                    file_type = 'csv'
                else:
                    raise ValueError(f"Unsupported file type: {ext}")
            else:
                file_type = self.file_type
            
            # 讀取數據
            if file_type == 'excel':
                df = pd.read_excel(self.file_path, **self.read_params)
            elif file_type == 'csv':
                df = pd.read_csv(self.file_path, **self.read_params)
            else:
                raise ValueError(f"Unknown file type: {file_type}")
            
            # 存儲數據
            if self.target == 'main':
                context.update_data(df)
            else:
                context.add_auxiliary_data(self.target, df)
            
            self.logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Loaded {len(df)} rows from {self.file_path.name}",
                metadata={'rows': len(df), 'columns': len(df.columns)}
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load data: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def validate_input(self, context: ProcessingContext) -> bool:
        """驗證文件是否存在"""
        return self.file_path.exists()


class DataExportStep(PipelineStep):
    """
    數據導出步驟
    將數據導出到文件
    """
    
    def __init__(self,
                 name: str,
                 output_path: str,
                 source: str = 'main',  # 'main' 或輔助數據名稱
                 file_type: str = 'auto',
                 write_params: Dict[str, Any] = None,
                 **kwargs):
        """
        初始化數據導出步驟
        
        Args:
            name: 步驟名稱
            output_path: 輸出路徑
            source: 數據來源 ('main' 為主數據，其他為輔助數據名稱)
            file_type: 文件類型 ('auto', 'excel', 'csv')
            write_params: 寫入參數
        """
        super().__init__(name, **kwargs)
        self.output_path = Path(output_path)
        self.source = source
        self.file_type = file_type
        self.write_params = write_params or {'index': False}
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據導出"""
        try:
            # 獲取數據
            if self.source == 'main':
                df = context.data
            else:
                df = context.get_auxiliary_data(self.source)
                if df is None:
                    raise ValueError(f"Auxiliary data '{self.source}' not found")
            
            self.logger.info(f"Exporting {len(df)} rows to: {self.output_path}")
            
            # 確保目錄存在
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 自動識別文件類型
            if self.file_type == 'auto':
                ext = self.output_path.suffix.lower()
                if ext in ['.xlsx', '.xls']:
                    file_type = 'excel'
                elif ext == '.csv':
                    file_type = 'csv'
                else:
                    file_type = 'excel'  # 默認 Excel
            else:
                file_type = self.file_type
            
            # 寫入數據
            if file_type == 'excel':
                df.to_excel(self.output_path, **self.write_params)
            elif file_type == 'csv':
                df.to_csv(self.output_path, **self.write_params)
            
            # 記錄輸出路徑到上下文
            context.set_variable(f'{self.name}_output_path', str(self.output_path))
            
            self.logger.info(f"Exported successfully to: {self.output_path}")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"Exported {len(df)} rows to {self.output_path.name}",
                metadata={'output_path': str(self.output_path), 'rows': len(df)}
            )
            
        except Exception as e:
            self.logger.error(f"Failed to export data: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )


class DataCleaningStep(PipelineStep):
    """
    數據清洗步驟
    執行常見的數據清洗操作
    """
    
    def __init__(self,
                 name: str,
                 drop_duplicates: bool = False,
                 drop_na_columns: List[str] = None,
                 fill_na_values: Dict[str, Any] = None,
                 strip_columns: List[str] = None,
                 rename_columns: Dict[str, str] = None,
                 drop_columns: List[str] = None,
                 **kwargs):
        """
        初始化數據清洗步驟
        
        Args:
            name: 步驟名稱
            drop_duplicates: 是否刪除重複行
            drop_na_columns: 需要刪除 NA 的列
            fill_na_values: NA 填充值字典
            strip_columns: 需要去除空白的列
            rename_columns: 列重命名映射
            drop_columns: 需要刪除的列
        """
        super().__init__(name, **kwargs)
        self.drop_duplicates = drop_duplicates
        self.drop_na_columns = drop_na_columns or []
        self.fill_na_values = fill_na_values or {}
        self.strip_columns = strip_columns or []
        self.rename_columns = rename_columns or {}
        self.drop_columns = drop_columns or []
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據清洗"""
        try:
            df = context.data.copy()
            original_rows = len(df)
            
            # 刪除重複行
            if self.drop_duplicates:
                df = df.drop_duplicates()
                self.logger.info(f"Dropped {original_rows - len(df)} duplicate rows")
            
            # 刪除指定列的 NA
            for col in self.drop_na_columns:
                if col in df.columns:
                    before = len(df)
                    df = df.dropna(subset=[col])
                    self.logger.info(f"Dropped {before - len(df)} rows with NA in '{col}'")
            
            # 填充 NA 值
            for col, value in self.fill_na_values.items():
                if col in df.columns:
                    df[col] = df[col].fillna(value)
                    self.logger.info(f"Filled NA in '{col}' with '{value}'")
            
            # 去除字符串列的空白
            for col in self.strip_columns:
                if col in df.columns and df[col].dtype == 'object':
                    df[col] = df[col].str.strip()
                    self.logger.info(f"Stripped whitespace from '{col}'")
            
            # 重命名列
            if self.rename_columns:
                df = df.rename(columns=self.rename_columns)
                self.logger.info(f"Renamed columns: {self.rename_columns}")
            
            # 刪除列
            cols_to_drop = [c for c in self.drop_columns if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)
                self.logger.info(f"Dropped columns: {cols_to_drop}")
            
            context.update_data(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Cleaned data: {original_rows} -> {len(df)} rows",
                metadata={'original_rows': original_rows, 'cleaned_rows': len(df)}
            )
            
        except Exception as e:
            self.logger.error(f"Data cleaning failed: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )


class DataTransformStep(PipelineStep):
    """
    數據轉換步驟
    使用自定義函數轉換數據
    """
    
    def __init__(self,
                 name: str,
                 transform_func: Callable[[pd.DataFrame, ProcessingContext], pd.DataFrame],
                 **kwargs):
        """
        初始化數據轉換步驟
        
        Args:
            name: 步驟名稱
            transform_func: 轉換函數，接受 DataFrame 和 Context，返回 DataFrame
        """
        super().__init__(name, **kwargs)
        self.transform_func = transform_func
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據轉換"""
        try:
            df = context.data.copy()
            original_shape = df.shape
            
            result_df = self.transform_func(df, context)
            
            context.update_data(result_df)
            
            self.logger.info(f"Transformed data: {original_shape} -> {result_df.shape}")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=result_df,
                message=f"Transformed data successfully",
                metadata={
                    'original_shape': original_shape,
                    'result_shape': result_df.shape
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data transformation failed: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )


class DataMergeStep(PipelineStep):
    """
    數據合併步驟
    合併主數據與輔助數據
    """
    
    def __init__(self,
                 name: str,
                 auxiliary_name: str,
                 on: Optional[List[str]] = None,
                 left_on: Optional[List[str]] = None,
                 right_on: Optional[List[str]] = None,
                 how: str = 'left',
                 suffixes: tuple = ('', '_aux'),
                 **kwargs):
        """
        初始化數據合併步驟
        
        Args:
            name: 步驟名稱
            auxiliary_name: 輔助數據名稱
            on: 合併鍵（左右相同時使用）
            left_on: 左側合併鍵
            right_on: 右側合併鍵
            how: 合併方式 ('left', 'right', 'inner', 'outer')
            suffixes: 重複列名後綴
        """
        super().__init__(name, **kwargs)
        self.auxiliary_name = auxiliary_name
        self.on = on
        self.left_on = left_on
        self.right_on = right_on
        self.how = how
        self.suffixes = suffixes
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據合併"""
        try:
            main_df = context.data
            aux_df = context.get_auxiliary_data(self.auxiliary_name)
            
            if aux_df is None:
                raise ValueError(f"Auxiliary data '{self.auxiliary_name}' not found")
            
            original_rows = len(main_df)
            
            # 執行合併
            merge_kwargs = {'how': self.how, 'suffixes': self.suffixes}
            if self.on:
                merge_kwargs['on'] = self.on
            else:
                if self.left_on:
                    merge_kwargs['left_on'] = self.left_on
                if self.right_on:
                    merge_kwargs['right_on'] = self.right_on
            
            result_df = main_df.merge(aux_df, **merge_kwargs)
            
            context.update_data(result_df)
            
            self.logger.info(f"Merged data: {original_rows} -> {len(result_df)} rows")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=result_df,
                message=f"Merged with '{self.auxiliary_name}': {original_rows} -> {len(result_df)} rows",
                metadata={
                    'original_rows': original_rows,
                    'result_rows': len(result_df),
                    'merge_type': self.how
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data merge failed: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def validate_input(self, context: ProcessingContext) -> bool:
        """驗證輸入"""
        return context.has_auxiliary_data(self.auxiliary_name)


class DataValidationStep(PipelineStep):
    """
    數據驗證步驟
    驗證數據是否符合預期
    """
    
    def __init__(self,
                 name: str,
                 required_columns: List[str] = None,
                 unique_columns: List[str] = None,
                 not_null_columns: List[str] = None,
                 custom_validators: List[Callable[[pd.DataFrame], tuple]] = None,
                 **kwargs):
        """
        初始化數據驗證步驟
        
        Args:
            name: 步驟名稱
            required_columns: 必需的列
            unique_columns: 需要唯一的列
            not_null_columns: 不允許空值的列
            custom_validators: 自定義驗證器列表，每個返回 (is_valid, message)
        """
        super().__init__(name, **kwargs)
        self.required_columns = required_columns or []
        self.unique_columns = unique_columns or []
        self.not_null_columns = not_null_columns or []
        self.custom_validators = custom_validators or []
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據驗證"""
        try:
            df = context.data
            validation_result = ValidationResult(is_valid=True)
            
            # 檢查必需列
            for col in self.required_columns:
                if col not in df.columns:
                    validation_result.add_error(f"Missing required column: {col}")
            
            # 檢查唯一性
            for col in self.unique_columns:
                if col in df.columns:
                    duplicates = df[col].duplicated().sum()
                    if duplicates > 0:
                        validation_result.add_warning(f"Column '{col}' has {duplicates} duplicates")
            
            # 檢查空值
            for col in self.not_null_columns:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    if null_count > 0:
                        validation_result.add_error(f"Column '{col}' has {null_count} null values")
            
            # 執行自定義驗證
            for validator in self.custom_validators:
                is_valid, message = validator(df)
                if not is_valid:
                    validation_result.add_error(message)
                else:
                    self.logger.info(f"Custom validation passed: {message}")
            
            # 添加到上下文
            context.add_validation(self.name, validation_result)
            
            if validation_result.is_valid:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.SUCCESS,
                    message="All validations passed",
                    metadata={
                        'errors': validation_result.errors,
                        'warnings': validation_result.warnings
                    }
                )
            else:
                return StepResult(
                    step_name=self.name,
                    status=StepStatus.FAILED,
                    message=f"Validation failed: {', '.join(validation_result.errors)}",
                    metadata={
                        'errors': validation_result.errors,
                        'warnings': validation_result.warnings
                    }
                )
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )


class DataFilterStep(PipelineStep):
    """
    數據過濾步驟
    根據條件過濾數據
    """
    
    def __init__(self,
                 name: str,
                 query: str = None,
                 filter_func: Callable[[pd.DataFrame], pd.DataFrame] = None,
                 **kwargs):
        """
        初始化數據過濾步驟
        
        Args:
            name: 步驟名稱
            query: pandas query 字符串
            filter_func: 自定義過濾函數
        """
        super().__init__(name, **kwargs)
        self.query = query
        self.filter_func = filter_func
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """執行數據過濾"""
        try:
            df = context.data.copy()
            original_rows = len(df)
            
            if self.query:
                df = df.query(self.query)
                self.logger.info(f"Applied query: {self.query}")
            
            if self.filter_func:
                df = self.filter_func(df)
                self.logger.info("Applied custom filter function")
            
            context.update_data(df)
            
            filtered_count = original_rows - len(df)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                data=df,
                message=f"Filtered {filtered_count} rows ({original_rows} -> {len(df)})",
                metadata={
                    'original_rows': original_rows,
                    'filtered_rows': len(df),
                    'removed_rows': filtered_count
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data filtering failed: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
