"""
銀行資料處理基類
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd

from src.utils import get_logger
from ..models import BankDataContainer


class BankProcessor(ABC):
    """
    銀行資料處理基類
    
    所有銀行處理器都應繼承此類並實作抽象方法
    """
    
    def __init__(self, bank_code: str, bank_name: str, config: Dict[str, Any]):
        """
        初始化處理器
        
        Args:
            bank_code: 銀行代碼
            bank_name: 銀行名稱
            config: 配置字典
        """
        self.bank_code = bank_code
        self.bank_name = bank_name
        self.config = config
        self.logger = get_logger(f"bank_processor.{bank_code}")
    
    @abstractmethod
    def load_data(self, db_manager, beg_date: str, end_date: str) -> pd.DataFrame:
        """
        載入銀行資料
        
        Args:
            db_manager: DuckDB 管理器
            beg_date: 開始日期
            end_date: 結束日期
            
        Returns:
            pd.DataFrame: 載入的資料
        """
        pass
    
    @abstractmethod
    def calculate_recon_amounts(
        self, 
        data: pd.DataFrame,
        beg_date: str,
        end_date: str,
        last_beg_date: str,
        last_end_date: str
    ) -> Dict[str, Any]:
        """
        計算對帳金額
        
        Args:
            data: 原始資料
            beg_date: 當期開始日期
            end_date: 當期結束日期
            last_beg_date: 前期開始日期
            last_end_date: 前期結束日期
            
        Returns:
            Dict: 計算結果字典，包含各項金額
        """
        pass
    
    def process(
        self,
        db_manager,
        beg_date: str,
        end_date: str,
        last_beg_date: str,
        last_end_date: str
    ) -> BankDataContainer:
        """
        完整處理流程
        
        Args:
            db_manager: DuckDB 管理器
            beg_date: 當期開始日期
            end_date: 當期結束日期
            last_beg_date: 前期開始日期
            last_end_date: 前期結束日期
            
        Returns:
            BankDataContainer: 處理結果容器
        """
        self.logger.info(f"開始處理 {self.bank_name} ({self.config.get('category', 'default')})")
        
        # 1. 載入資料
        data = self.load_data(db_manager, beg_date, end_date)
        self.logger.info(f"載入資料: {len(data)} 筆")
        
        # 2. 計算金額
        amounts = self.calculate_recon_amounts(
            data, beg_date, end_date, last_beg_date, last_end_date
        )
        
        # 3. 建立 Container
        container = BankDataContainer(
            bank_code=self.bank_code,
            bank_name=self.bank_name,
            raw_data=data,
            **amounts
        )
        
        # 4. 驗證
        if not self.validate_container(container):
            self.logger.warning(f"Container 驗證失敗: {self.bank_name}")
        
        self.logger.info(f"處理完成 - 對帳金額: {container.recon_amount:,}, 手續費: {container.recon_service_fee:,}")
        
        return container
    
    def validate_container(self, container: BankDataContainer) -> bool:
        """
        驗證 Container 資料
        
        Args:
            container: 資料容器
            
        Returns:
            bool: 是否通過驗證
        """
        # 基本驗證
        if not container.validate():
            self.logger.error("Container 基本驗證失敗")
            return False
        
        # 金額合理性檢查
        if container.recon_amount < 0:
            self.logger.warning("請款金額為負數")
        
        if container.recon_service_fee < 0:
            self.logger.warning("手續費為負數")
        
        return True
    
    def get_query(self, table_name: str, date_column: str = 'disbursement_date') -> str:
        """
        生成查詢 SQL
        
        Args:
            table_name: 表名
            date_column: 日期欄位名
            
        Returns:
            str: SQL 查詢語句
        """
        return f"""
        SELECT * FROM {table_name}
        WHERE {date_column} IS NOT NULL
        """
