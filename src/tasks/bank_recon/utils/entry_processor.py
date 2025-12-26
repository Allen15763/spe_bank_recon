"""
會計分錄處理器
處理會計分錄的生成、驗證和輸出
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

from src.utils import get_logger

logger = get_logger("entry_processor")


class AccountingEntryProcessor:
    """
    會計分錄處理器
    
    負責:
    1. 處理寬格式分錄轉為長格式
    2. 加入期初/期末調整
    3. 驗證借貸平衡
    4. 生成報告
    """
    
    def __init__(self, year: int, month: int, config: Dict[str, Any] = None):
        """
        初始化處理器
        
        Args:
            year: 年份
            month: 月份
            config: 配置字典
        """
        self.year = year
        self.month = month
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)
        
        # 從配置載入交易類型排序
        self.type_order = self.config.get('transaction_type_order', {
            '期初數': '00.期初數',
            '內扣_ctbc_cc_匯費': '01.內扣_ctbc_cc_匯費',
            'received_ctbc_spt': '02.received_ctbc_spt',
            'received_ctbc_spt_退匯': '03.received_ctbc_spt_退匯',
            'out_ctbc_spt': '04.out_ctbc_spt',
            'other': '05.other',
            'other_利息': '05.other_利息',
            'spt': '06.spt',
            'spe_withdrawal': '07.spe_withdrawal',
            'spl手續費調整': '08.spl手續費調整',
            '發票已開立沖轉': '09.發票已開立沖轉',
        })
        
        # 排除零值驗證的交易類型
        self.val_zero_excludes = self.config.get('exclude_zero_check', [
            '00.期初數',
            '09.發票已開立沖轉',
        ])
    
    def process(self, df_entry_temp: pd.DataFrame) -> pd.DataFrame:
        """
        處理分錄（寬格式 -> 長格式）
        
        Args:
            df_entry_temp: 寬格式分錄 DataFrame
            
        Returns:
            pd.DataFrame: 長格式分錄
        """
        from .entry_transformer import AccountingEntryTransformer
        
        transformer = AccountingEntryTransformer(
            accounts_config=self.config.get('accounts'),
            accounts_detail_config=self.config.get('accounts_detail')
        )
        
        df_entry_long = transformer.transform(df_entry_temp)
        
        # 加入期間資訊
        df_entry_long['period'] = f"{self.year}-{self.month:02d}"
        
        self.logger.info(f"分錄處理完成: {len(df_entry_long)} 筆")
        return df_entry_long
    
    def add_beginning_balance(self, df_entry_long: pd.DataFrame,
                              beginning_balances: Dict[str, float]) -> pd.DataFrame:
        """
        加入期初數
        
        Args:
            df_entry_long: 長格式分錄
            beginning_balances: 期初餘額 {'104171': 1000000, ...}
            
        Returns:
            pd.DataFrame: 含期初數的分錄
        """
        beginning_entries = []
        
        for account_no, balance in beginning_balances.items():
            if balance != 0:
                beginning_entries.append({
                    'accounting_date': None,
                    'account_no': account_no,
                    'account_desc': f'Beginning Balance - {account_no}',
                    'transaction_type': '期初數',
                    'amount': balance,
                    'period': f"{self.year}-{self.month:02d}",
                })
        
        if beginning_entries:
            df_beginning = pd.DataFrame(beginning_entries)
            df_entry_long = pd.concat([df_beginning, df_entry_long], ignore_index=True)
            self.logger.info(f"已加入 {len(beginning_entries)} 筆期初數")
        
        return df_entry_long
    
    def apply_type_order(self, df_entry_long: pd.DataFrame) -> pd.DataFrame:
        """
        套用交易類型排序
        
        Args:
            df_entry_long: 長格式分錄
            
        Returns:
            pd.DataFrame: 排序後的分錄
        """
        df_copy = df_entry_long.copy()
        df_copy['transaction_type_sort'] = df_copy['transaction_type'].map(self.type_order)
        df_copy['transaction_type_sort'] = df_copy['transaction_type_sort'].fillna(df_copy['transaction_type'])
        
        return df_copy
    
    def save_result(self, df_entry_long: pd.DataFrame, 
                    output_dir: str = './output') -> str:
        """
        儲存結果到檔案
        
        Args:
            df_entry_long: 長格式分錄
            output_dir: 輸出目錄
            
        Returns:
            str: 輸出檔案路徑
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        filename = f"entries_{self.year}{self.month:02d}.xlsx"
        filepath = output_path / filename
        
        df_entry_long.to_excel(filepath, index=False)
        self.logger.info(f"分錄已儲存: {filepath}")
        
        return str(filepath)
    
    def generate_report(self, df_entry_long: pd.DataFrame) -> Dict[str, Any]:
        """
        生成分錄報告
        
        Args:
            df_entry_long: 長格式分錄
            
        Returns:
            Dict: 報告資訊
        """
        report = {
            'period': f"{self.year}-{self.month:02d}",
            'total_entries': len(df_entry_long),
            'total_amount': df_entry_long['amount'].sum(),
            'accounts_summary': {},
            'type_summary': {},
        }
        
        # 按科目統計
        accounts_summary = df_entry_long.groupby('account_no')['amount'].sum()
        report['accounts_summary'] = accounts_summary.to_dict()
        
        # 按交易類型統計
        type_summary = df_entry_long.groupby('transaction_type')['amount'].sum()
        report['type_summary'] = type_summary.to_dict()
        
        # 驗證借貸平衡
        report['is_balanced'] = abs(report['total_amount']) < 1
        
        self.logger.info(f"分錄報告生成完成: {report['total_entries']} 筆, 平衡: {report['is_balanced']}")
        return report


def calculate_daily_balance(df: pd.DataFrame,
                            beg_amt: float,
                            movement_col: str = 'daily_movement',
                            balance_col: str = 'balance_entry') -> pd.DataFrame:
    """
    計算每日餘額（基於累計變動）
    
    Args:
        df: 包含每日變動欄位的資料框
        beg_amt: 期初金額
        movement_col: 每日變動欄位名稱
        balance_col: 輸出餘額欄位名稱
        
    Returns:
        pd.DataFrame: 新增餘額欄位的資料框
    """
    df_result = df.copy()
    
    # 資料清理與轉換
    daily_movement_series = (
        df_result[movement_col]
        .replace(['-', '', ' '], np.nan)
    )
    
    daily_movement_series = (
        daily_movement_series
        .astype(str)
        .str.replace(',', '', regex=False)
        .str.strip()
        .replace('nan', np.nan)
    )
    
    daily_movement_clean = pd.to_numeric(
        daily_movement_series,
        errors='coerce'
    ).fillna(0)
    
    # 計算累計餘額
    df_result[balance_col] = beg_amt + daily_movement_clean.cumsum()
    
    # 差異檢核
    if 'balance_dfr' in df_result.columns:
        df_result['check'] = df_result['balance_dfr'] - df_result[balance_col]
        df_result['check_pct'] = (
            df_result['check'] / df_result['balance_dfr'] * 100
        ).round(2)
    
    logger.info(f"每日餘額計算完成: 期初 {beg_amt:,.0f}, 期末 {df_result[balance_col].iloc[-1]:,.0f}")
    return df_result


def dfr_balance_check(df_entry_long: pd.DataFrame,
                      df_result_dfr: pd.DataFrame,
                      account_no: str = '104171') -> pd.DataFrame:
    """
    DFR 餘額核對
    
    Args:
        df_entry_long: 長格式分錄
        df_result_dfr: DFR 處理結果
        account_no: 要核對的科目代號
        
    Returns:
        pd.DataFrame: 核對結果
    """
    # 篩選指定科目
    df_account = df_entry_long[df_entry_long['account_no'] == account_no].copy()
    
    # 按日期彙總
    daily_summary = df_account.groupby('accounting_date')['amount'].sum().reset_index()
    daily_summary.columns = ['Date', 'entry_amount']
    
    # 與 DFR 合併
    if 'Date' in df_result_dfr.columns:
        df_check = df_result_dfr[['Date']].merge(daily_summary, on='Date', how='left')
        df_check['entry_amount'] = df_check['entry_amount'].fillna(0)
    else:
        df_check = daily_summary
    
    logger.info(f"DFR 餘額核對完成: 科目 {account_no}")
    return df_check


def summarize_balance_check(df_check: pd.DataFrame) -> pd.DataFrame:
    """
    彙總餘額核對結果
    
    Args:
        df_check: 核對結果 DataFrame
        
    Returns:
        pd.DataFrame: 彙總結果
    """
    summary = pd.DataFrame({
        'metric': ['total_entry_amount', 'days_with_diff', 'max_diff', 'total_diff'],
        'value': [
            df_check['entry_amount'].sum() if 'entry_amount' in df_check.columns else 0,
            (df_check['check'].abs() >= 1).sum() if 'check' in df_check.columns else 0,
            df_check['check'].abs().max() if 'check' in df_check.columns else 0,
            df_check['check'].sum() if 'check' in df_check.columns else 0,
        ]
    })
    
    return summary


def create_big_entry_pivot(df_entry_long: pd.DataFrame,
                           type_order: Dict[str, str] = None) -> pd.DataFrame:
    """
    建立大 Entry pivot 表
    
    Args:
        df_entry_long: 長格式分錄
        type_order: 交易類型排序
        
    Returns:
        pd.DataFrame: Pivot 格式的大 Entry
    """
    df_copy = df_entry_long.copy()
    
    # 套用排序
    if type_order:
        df_copy['transaction_type'] = df_copy['transaction_type'].map(type_order).fillna(df_copy['transaction_type'])
    
    # 處理空白日期
    df_copy['accounting_date'] = df_copy['accounting_date'].fillna('期末會計調整')
    
    # 建立 pivot
    df_big_entry = df_copy.pivot_table(
        index=['account_no', 'account_desc', 'transaction_type'],
        columns='accounting_date',
        values='amount',
        aggfunc='sum',
        margins=True,
        margins_name='Total'
    ).reset_index()
    
    logger.info(f"大 Entry pivot 建立完成: {len(df_big_entry)} 行")
    return df_big_entry


def validate_result(df_entry_long: pd.DataFrame) -> Dict[str, Any]:
    """
    驗證分錄結果
    
    Args:
        df_entry_long: 長格式分錄
        
    Returns:
        Dict: 驗證結果
    """
    result = {
        'total_entries': len(df_entry_long),
        'unique_accounts': df_entry_long['account_no'].nunique(),
        'unique_types': df_entry_long['transaction_type'].nunique(),
        'total_amount': df_entry_long['amount'].sum(),
        'is_balanced': abs(df_entry_long['amount'].sum()) < 1,
    }
    
    # 按科目統計
    account_summary = df_entry_long.groupby('account_no')['amount'].agg(['sum', 'count'])
    result['account_summary'] = account_summary.to_dict()
    
    if result['is_balanced']:
        logger.info("分錄驗證通過: 借貸平衡")
    else:
        logger.warning(f"分錄驗證失敗: 差額 {result['total_amount']:,.2f}")
    
    return result
