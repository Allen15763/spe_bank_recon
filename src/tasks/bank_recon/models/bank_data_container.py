"""
銀行對帳資料容器模型
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import pandas as pd


@dataclass
class BankDataContainer:
    """
    銀行對帳資料容器
    
    用於統一存儲各銀行的對帳資料和計算結果
    """
    # 基本資訊
    bank_code: str  # 銀行代碼 (cub, ctbc, nccc, ub, taishi)
    bank_name: str  # 銀行名稱
    category: str   # 類別 (individual/nonindividual/installment/noninstallment/recon/default)
    
    # 原始資料
    raw_data: pd.DataFrame
    aggregated_data: Optional[pd.DataFrame] = None
    
    # 對帳金額
    recon_amount: int = 0  # 當期請款金額
    amount_claimed_last_period_paid_by_current: int = 0  # 前期發票當期撥款
    recon_amount_for_trust_account_fee: int = 0  # Trust Account Fee 金額
    
    # 手續費
    recon_service_fee: int = 0  # 當期手續費
    service_fee_claimed_last_period_paid_by_current: int = 0  # 前期手續費
    adj_service_fee: int = 0  # 調整手續費
    
    # 發票金額
    invoice_amount_claimed: int = 0
    invoice_service_fee: Optional[int] = None
    
    # 元數據
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """
        轉為摘要字典
        
        Returns:
            Dict: 摘要資料
        """
        return {
            '銀行': f"{self.bank_name}_{self.category}" if self.category != 'default' else self.bank_name,
            '對帳_請款金額_當期': self.recon_amount,
            '對帳_請款金額_Trust_Account_Fee': self.recon_amount_for_trust_account_fee,
            '對帳_手續費_當期': self.recon_service_fee,
            '對帳_調整金額': self.adj_service_fee,
            '發票_請款金額': self.invoice_amount_claimed,
            '發票_手續費': self.invoice_service_fee
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """
        轉為完整字典（不包含 DataFrame）
        
        Returns:
            Dict: 完整資料
        """
        return {
            'bank_code': self.bank_code,
            'bank_name': self.bank_name,
            'category': self.category,
            'recon_amount': self.recon_amount,
            'amount_claimed_last_period_paid_by_current': self.amount_claimed_last_period_paid_by_current,
            'recon_amount_for_trust_account_fee': self.recon_amount_for_trust_account_fee,
            'recon_service_fee': self.recon_service_fee,
            'service_fee_claimed_last_period_paid_by_current': self.service_fee_claimed_last_period_paid_by_current,
            'adj_service_fee': self.adj_service_fee,
            'invoice_amount_claimed': self.invoice_amount_claimed,
            'invoice_service_fee': self.invoice_service_fee,
            'metadata': self.metadata,
            'data_shape': self.raw_data.shape if self.raw_data is not None else (0, 0)
        }
    
    def get_total_service_fee(self) -> int:
        """
        計算總手續費
        
        Returns:
            int: 總手續費 (當期 + 前期 - 調整)
        """
        return (self.recon_service_fee + 
                self.service_fee_claimed_last_period_paid_by_current - 
                abs(self.adj_service_fee))
    
    def validate(self) -> bool:
        """
        驗證資料完整性
        
        Returns:
            bool: 是否通過驗證
        """
        # 基本欄位檢查
        if not self.bank_code or not self.bank_name:
            return False
        
        # DataFrame 檢查
        if self.raw_data is None or self.raw_data.empty:
            return False
        
        return True
    
    def __repr__(self) -> str:
        return f"BankDataContainer(bank={self.bank_name}, category={self.category}, amount={self.recon_amount:,})"


@dataclass
class InstallmentReportData:
    """分期報表資料容器"""
    bank_code: str
    bank_name: str
    transaction_type: str  # 3期, 6期, 12期, 24期, normal
    
    total_claimed: float = 0.0  # 請款金額
    total_service_fee: float = 0.0  # 手續費
    total_paid: float = 0.0  # 實付金額
    
    service_fee_rate: Optional[float] = None  # 手續費率
    calculated_service_fee: Optional[float] = None  # 計算的手續費
    
    raw_data: Optional[pd.DataFrame] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """轉為字典"""
        return {
            'bank_code': self.bank_code,
            'bank_name': self.bank_name,
            'transaction_type': self.transaction_type,
            'total_claimed': self.total_claimed,
            'total_service_fee': self.total_service_fee,
            'total_paid': self.total_paid,
            'service_fee_rate': self.service_fee_rate,
            'calculated_service_fee': self.calculated_service_fee,
            'metadata': self.metadata
        }