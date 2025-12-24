"""
CSV數據源實現（同步版本）
"""

import pandas as pd
from typing import Dict, Optional, Any, List
from pathlib import Path

from .base import DataSource
from .config import DataSourceConfig, DataSourceType


class CSVSource(DataSource):
    """CSV文件數據源"""
    
    def __init__(self, config: DataSourceConfig):
        """
        初始化CSV數據源
        
        Args:
            config: 數據源配置
        """
        super().__init__(config)
        self.file_path = Path(config.connection_params['file_path'])
        self.encoding = config.encoding or 'utf-8'
        self.sep = config.connection_params.get('sep', ',')
        self.header = config.connection_params.get('header', 'infer')
        self.dtype = config.connection_params.get('dtype')
        self.na_values = config.connection_params.get('na_values')
        self.parse_dates = config.connection_params.get('parse_dates')
        self.usecols = config.connection_params.get('usecols')
        self.chunk_size = config.chunk_size
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
    
    def read(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        讀取CSV文件
        
        Args:
            query: pandas query 條件
            **kwargs: 額外參數
            
        Returns:
            pd.DataFrame: 讀取的數據
        """
        nrows = kwargs.get('nrows')
        skiprows = kwargs.get('skiprows')
        chunksize = kwargs.get('chunksize', self.chunk_size)
        
        try:
            self.logger.info(f"Reading CSV file: {self.file_path}")
            
            # 構建讀取參數
            read_kwargs = {
                'sep': self.sep,
                'encoding': self.encoding,
                'header': self.header
            }
            
            if self.dtype is not None:
                read_kwargs['dtype'] = self.dtype
            if self.na_values is not None:
                read_kwargs['na_values'] = self.na_values
            if self.parse_dates is not None:
                read_kwargs['parse_dates'] = self.parse_dates
            if self.usecols is not None:
                read_kwargs['usecols'] = self.usecols
            if nrows is not None:
                read_kwargs['nrows'] = nrows
            if skiprows is not None:
                read_kwargs['skiprows'] = skiprows
            
            # 分塊讀取
            if chunksize:
                read_kwargs['chunksize'] = chunksize
                chunks = []
                for chunk in pd.read_csv(self.file_path, **read_kwargs):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_csv(self.file_path, **read_kwargs)
            
            # 應用查詢條件
            if query:
                df = self._apply_query(df, query)
            
            self.logger.info(f"Successfully read {len(df)} rows from CSV")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            raise
    
    def write(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        寫入CSV文件
        
        Args:
            data: 要寫入的數據
            **kwargs: 額外參數
            
        Returns:
            bool: 是否成功
        """
        index = kwargs.get('index', False)
        mode = kwargs.get('mode', 'w')
        header = kwargs.get('header', True)
        output_path = kwargs.get('output_path', self.file_path)
        
        try:
            self.logger.info(f"Writing {len(data)} rows to CSV: {output_path}")
            
            data.to_csv(
                output_path,
                sep=self.sep,
                encoding=self.encoding,
                index=index,
                mode=mode,
                header=header
            )
            
            self.logger.info("Successfully wrote data to CSV")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing CSV file: {str(e)}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """獲取CSV文件元數據"""
        metadata = {
            'file_path': str(self.file_path),
            'file_size': self.file_path.stat().st_size if self.file_path.exists() else 0,
            'file_modified': self.file_path.stat().st_mtime if self.file_path.exists() else None,
            'encoding': self.encoding,
            'separator': self.sep
        }
        
        try:
            sample_df = pd.read_csv(
                self.file_path,
                sep=self.sep,
                encoding=self.encoding,
                nrows=5
            )
            metadata['num_columns'] = len(sample_df.columns)
            metadata['column_names'] = sample_df.columns.tolist()
            
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                metadata['num_rows'] = sum(1 for _ in f) - 1
                
        except Exception as e:
            self.logger.warning(f"Could not read CSV metadata: {str(e)}")
            metadata['num_columns'] = 0
            metadata['column_names'] = []
            metadata['num_rows'] = 0
        
        return metadata
    
    def read_in_chunks(self, chunk_size: int = 10000) -> List[pd.DataFrame]:
        """
        分塊讀取CSV文件
        
        Args:
            chunk_size: 每塊的行數
            
        Returns:
            List[pd.DataFrame]: 數據塊列表
        """
        chunks = []
        try:
            for chunk in pd.read_csv(
                self.file_path,
                sep=self.sep,
                encoding=self.encoding,
                chunksize=chunk_size,
                dtype=self.dtype,
                na_values=self.na_values,
                parse_dates=self.parse_dates
            ):
                chunks.append(chunk)
            
            self.logger.info(f"Read {len(chunks)} chunks from CSV")
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error reading CSV in chunks: {str(e)}")
            raise
    
    def append_data(self, data: pd.DataFrame) -> bool:
        """追加數據到現有CSV文件"""
        if self.file_path.exists():
            return self.write(data, mode='a', header=False)
        else:
            return self.write(data)
    
    def _apply_query(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """應用查詢條件"""
        try:
            return df.query(query)
        except Exception as e:
            self.logger.warning(f"Could not apply query '{query}': {str(e)}")
            return df
    
    @classmethod
    def create_from_file(cls, file_path: str, sep: str = ',', 
                         encoding: str = 'utf-8', **kwargs) -> 'CSVSource':
        """
        便捷方法：從檔案路徑創建CSV數據源
        
        Args:
            file_path: CSV檔案路徑
            sep: 分隔符
            encoding: 編碼
            **kwargs: 其他配置參數
            
        Returns:
            CSVSource: CSV數據源實例
        """
        config = DataSourceConfig(
            source_type=DataSourceType.CSV,
            connection_params={
                'file_path': file_path,
                'sep': sep,
                **kwargs
            },
            encoding=encoding
        )
        return cls(config)
