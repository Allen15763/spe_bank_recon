"""
Excel數據源實現（同步版本）
"""

import pandas as pd
from typing import Dict, Optional, Any, List
from pathlib import Path

from .base import DataSource
from .config import DataSourceConfig, DataSourceType


class ExcelSource(DataSource):
    """Excel文件數據源"""
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化Excel數據源
        
        Args:
            config: 數據源配置
        """
        super().__init__(config)
        self.file_path = Path(config.connection_params['file_path'])
        self.sheet_name = config.connection_params.get('sheet_name', 0)
        self.header = config.connection_params.get('header', 0)
        self.usecols = config.connection_params.get('usecols')
        self.dtype = config.connection_params.get('dtype')
        self.na_values = config.connection_params.get('na_values')
        self.parse_dates = config.connection_params.get('parse_dates')
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")
    
    def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        讀取Excel文件
        
        Args:
            query: pandas query 條件
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        sheet_name = kwargs.get('sheet_name', self.sheet_name)
        header = kwargs.get('header', self.header)
        usecols = kwargs.get('usecols', self.usecols)
        dtype = kwargs.get('dtype', self.dtype)
        nrows = kwargs.get('nrows')
        skiprows = kwargs.get('skiprows')
        
        try:
            self.logger.info(f"Reading Excel file: {self.file_path}")
            
            read_kwargs = {
                'sheet_name': sheet_name,
                'header': header,
                'engine': 'openpyxl'
            }
            
            if usecols is not None:
                read_kwargs['usecols'] = usecols
            if dtype is not None:
                read_kwargs['dtype'] = dtype
            if self.na_values is not None:
                read_kwargs['na_values'] = self.na_values
            if self.parse_dates is not None:
                read_kwargs['parse_dates'] = self.parse_dates
            if nrows is not None:
                read_kwargs['nrows'] = nrows
            if skiprows is not None:
                read_kwargs['skiprows'] = skiprows
            
            df = pd.read_excel(self.file_path, **read_kwargs)
            
            # 應用查詢條件
            if query:
                df = self._apply_query(df, query)
            
            self.logger.info(f"Successfully read {len(df)} rows from Excel")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading Excel file: {str(e)}")
            raise
    
    def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        寫入Excel文件
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        sheet_name = kwargs.get('sheet_name', 'Sheet1')
        index = kwargs.get('index', False)
        mode = kwargs.get('mode', 'w')
        if_sheet_exists = kwargs.get('if_sheet_exists', 'replace')
        output_path = kwargs.get('output_path', self.file_path)
        
        try:
            self.logger.info(f"Writing {len(data)} rows to Excel: {output_path}")
            
            output_path = Path(output_path)
            
            if mode == 'a' and output_path.exists():
                with pd.ExcelWriter(
                    output_path, 
                    mode='a',
                    engine='openpyxl',
                    if_sheet_exists=if_sheet_exists
                ) as writer:
                    data.to_excel(writer, sheet_name=sheet_name, index=index)
            else:
                with pd.ExcelWriter(
                    output_path,
                    engine='openpyxl'
                ) as writer:
                    data.to_excel(writer, sheet_name=sheet_name, index=index)
            
            self.logger.info("Successfully wrote data to Excel")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing Excel file: {str(e)}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """獲取Excel文件元數據"""
        metadata = {
            'file_path': str(self.file_path),
            'file_size': self.file_path.stat().st_size if self.file_path.exists() else 0,
            'file_modified': self.file_path.stat().st_mtime if self.file_path.exists() else None
        }
        
        try:
            with pd.ExcelFile(self.file_path, engine='openpyxl') as excel_file:
                metadata['sheet_names'] = excel_file.sheet_names
                metadata['num_sheets'] = len(excel_file.sheet_names)
        except Exception as e:
            self.logger.warning(f"Could not read sheet information: {str(e)}")
            metadata['sheet_names'] = []
            metadata['num_sheets'] = 0
        
        return metadata
    
    def get_sheet_names(self) -> List[str]:
        """獲取所有工作表名稱"""
        try:
            with pd.ExcelFile(self.file_path, engine='openpyxl') as excel_file:
                return excel_file.sheet_names
        except Exception as e:
            self.logger.error(f"Error getting sheet names: {str(e)}")
            return []
    
    def read_all_sheets(self) -> Dict[str, pd.DataFrame]:
        """讀取所有工作表"""
        sheet_names = self.get_sheet_names()
        result = {}
        
        for sheet_name in sheet_names:
            try:
                df = self.read(sheet_name=sheet_name)
                result[sheet_name] = df
            except Exception as e:
                self.logger.warning(f"Could not read sheet {sheet_name}: {str(e)}")
        
        return result
    
    def _apply_query(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """應用查詢條件"""
        try:
            return df.query(query)
        except Exception as e:
            self.logger.warning(f"Could not apply query '{query}': {str(e)}")
            return df
    
    def append_data(self, data: pd.DataFrame, sheet_name: str = None) -> bool:
        """追加數據到現有Excel文件"""
        if not self.file_path.exists():
            return self.write(data, sheet_name=sheet_name or 'Sheet1')
        
        existing_data = self.read(sheet_name=sheet_name)
        combined_data = pd.concat([existing_data, data], ignore_index=True)
        return self.write(combined_data, sheet_name=sheet_name or 'Sheet1')
    
    def write_multiple_sheets(self, data_dict: Dict[str, pd.DataFrame], 
                              output_path: str = None, index: bool = False) -> bool:
        """
        寫入多個工作表
        
        Args:
            data_dict: 工作表名稱到DataFrame的映射
            output_path: 輸出路徑
            index: 是否包含索引
            
        Returns:
            bool: 是否成功
        """
        output_path = Path(output_path) if output_path else self.file_path
        
        try:
            self.logger.info(f"Writing {len(data_dict)} sheets to Excel: {output_path}")
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet_name, df in data_dict.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=index)
            
            self.logger.info("Successfully wrote multiple sheets to Excel")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing multiple sheets: {str(e)}")
            return False
    
    @classmethod
    def create_from_file(cls, file_path: str, **kwargs) -> 'ExcelSource':
        """
        便捷方法：從檔案路徑創建Excel數據源
        
        Args:
            file_path: Excel檔案路徑
            **kwargs: 其他配置參數
            
        Returns:
            ExcelSource: Excel數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.EXCEL,
            connection_params={
                'file_path': file_path,
                **kwargs
            }
        )
        return cls(config)
