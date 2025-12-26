"""
會計分錄轉換器
將每日匯總的寬格式資料轉換為標準的長格式會計分錄
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from datetime import datetime

from src.utils import get_logger

logger = get_logger("entry_transformer")


@dataclass
class EntryMapping:
    """分錄映射定義"""
    source_column: str
    account_no: str
    transaction_type: str
    account_desc_key: Optional[str] = None


class AccountingEntryTransformer:
    """
    會計分錄轉換器
    
    將每日匯總的寬格式資料轉換為標準的長格式會計分錄
    """
    
    def __init__(self, accounts_config: Dict[str, Any] = None, 
                 accounts_detail_config: Dict[str, Dict[str, str]] = None):
        """
        初始化轉換器
        
        Args:
            accounts_config: 會計科目映射 {'101150': 'Cash in Bank...', ...}
            accounts_detail_config: 有子分類的科目 {'460103': {'APCC_ACH': '...', ...}, ...}
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # 預設會計科目名稱映射
        self.account_descriptions = accounts_config or {
            '200208': 'Receive on behalf of Shopee',
            '200701': 'Amount due to SPTTW-Escrow',
            '101150': 'Cash in Bank - Fubon TWD 2087',
            '104171': 'Escrow Bank - CTBC TWD 4935',
            '999995': 'Cash Clearing',
            '111301': 'Tax Receivable - GST/VAT',
            '111302': 'Tax Receivable - WHT',
            '112001': 'Amount due from IC-APYTW',
            '112002': 'Unbilled Receivables-RC-SPTTW',
            '113101': 'Receivables from payment gateway',
            '200601': 'Tax Payables - GST/VAT/WHT',
            '440001': 'Interest Income',
        }
        
        # 有子分類的科目
        self.account_details = accounts_detail_config or {
            '530006': {
                '收單_SPE': 'Bank transaction fee(Remittance fee)-收單-SPE',
                '回饋金': 'Bank transaction fee(Remittance fee)-CTBC收單手續費回饋金',
                '內扣CTBCCC匯費': 'Bank transaction fee(Remittance fee)-收單轉帳匯費',
                'COD匯費': 'Bank transaction fee(Remittance fee)-COD匯費',
            },
            '460103': {
                'APCC_ACH': 'Commission charge-RC-SPTTW-ACH/eACH/EDI',
                'APCC_手續費': 'Commission charge-RC-SPTTW-APCC手續費',
            }
        }
        
        # 定義分錄映射規則
        self.entry_mappings = self._build_entry_mappings()
    
    def _build_entry_mappings(self) -> List[EntryMapping]:
        """建立分錄映射規則"""
        return [
            # 200208科目 - received_ctbc_spt
            EntryMapping('acc_200208_ReceivedCTBCSPT_negative', '200208', 'received_ctbc_spt'),
            EntryMapping('acc_200208_ReceivedCTBCSPT_positive', '200208', 'received_ctbc_spt'),
            
            # 200701科目 - out, received, 退匯
            EntryMapping('acc_200701_OutCTBCSPT', '200701', 'out_ctbc_spt'),
            EntryMapping('acc_200701_ReceivedCTBCSPT_negative', '200701', 'received_ctbc_spt'),
            EntryMapping('acc_200701_ReceivedCTBCSPT退匯', '200701', 'received_ctbc_spt_退匯'),
            
            # 104171科目 - 中信信託帳戶
            EntryMapping('acc_104171_內扣CTBCCC匯費', '104171', '內扣_ctbc_cc_匯費'),
            EntryMapping('acc_104171_ReceivedCTBCSPT', '104171', 'received_ctbc_spt'),
            EntryMapping('acc_104171_ReceivedCTBCSPT退匯', '104171', 'received_ctbc_spt_退匯'),
            EntryMapping('acc_104171_OutCTBCSPT', '104171', 'out_ctbc_spt'),
            EntryMapping('acc_104171_others', '104171', 'other'),
            
            # 530006科目 - 銀行手續費
            EntryMapping('acc_530006__內扣CTBCCC匯費', '530006', '內扣_ctbc_cc_匯費', '內扣CTBCCC匯費'),
            EntryMapping('acc_530006_收單_SPE', '530006', 'received_ctbc_spt', '收單_SPE'),
            
            # 999995科目 - Cash Clearing
            EntryMapping('acc_999995_others', '999995', 'other'),
            
            # 101150科目 - 富邦銀行
            EntryMapping('acc_101150_Received_CTBC_SPT', '101150', 'received_ctbc_spt'),
            
            # 440001科目 - 利息收入
            EntryMapping('acc_440001_interest', '440001', 'other_利息'),
        ]
    
    def get_account_description(self, account_no: str, detail_key: str = None) -> str:
        """
        取得會計科目說明
        
        Args:
            account_no: 科目代號
            detail_key: 子分類 key（若有）
            
        Returns:
            str: 科目說明
        """
        if detail_key and account_no in self.account_details:
            return self.account_details[account_no].get(detail_key, f'Unknown-{account_no}')
        return self.account_descriptions.get(account_no, f'Unknown-{account_no}')
    
    def transform(self, df_entry_temp: pd.DataFrame) -> pd.DataFrame:
        """
        轉換寬格式為長格式分錄
        
        Args:
            df_entry_temp: 寬格式分錄 DataFrame
            
        Returns:
            pd.DataFrame: 長格式分錄
        """
        records = []
        
        for _, row in df_entry_temp.iterrows():
            date = row.get('Date')
            
            for mapping in self.entry_mappings:
                if mapping.source_column in df_entry_temp.columns:
                    amount = row.get(mapping.source_column, 0)
                    
                    if pd.notna(amount) and amount != 0:
                        records.append({
                            'accounting_date': date,
                            'account_no': mapping.account_no,
                            'account_desc': self.get_account_description(
                                mapping.account_no, 
                                mapping.account_desc_key
                            ),
                            'transaction_type': mapping.transaction_type,
                            'amount': amount,
                        })
        
        df_long = pd.DataFrame(records)
        
        if len(df_long) > 0:
            self.logger.info(f"轉換完成: {len(df_long)} 筆分錄")
        else:
            self.logger.warning("轉換後無分錄資料")
        
        return df_long
    
    def validate_balance(self, df_long: pd.DataFrame) -> Tuple[bool, float]:
        """
        驗證借貸是否平衡
        
        Args:
            df_long: 長格式分錄 DataFrame
            
        Returns:
            Tuple[bool, float]: (是否平衡, 差額)
        """
        if 'amount' not in df_long.columns:
            return False, np.nan
        
        total = df_long['amount'].sum()
        is_balanced = abs(total) < 1  # 允許 1 元誤差
        
        if is_balanced:
            self.logger.info("借貸平衡驗證: 通過")
        else:
            self.logger.warning(f"借貸平衡驗證: 不平衡，差額 {total:,.2f}")
        
        return is_balanced, total


def process_accounting_entries(df_dfr_wp: pd.DataFrame,
                               cub_rebate: pd.DataFrame,
                               received_ctbc_spt: pd.DataFrame,
                               interest: pd.Series,
                               beg_date: str,
                               end_date: str) -> pd.DataFrame:
    """
    處理會計分錄，將原始資料整理成寬表格格式
    
    Args:
        df_dfr_wp: DFR 工作底稿 DataFrame
        cub_rebate: 國泰回饋金 DataFrame
        received_ctbc_spt: 中信 SPT 入款 DataFrame
        interest: 利息 Series
        beg_date: 開始日期
        end_date: 結束日期
        
    Returns:
        pd.DataFrame: 寬格式分錄 DataFrame
    """
    # 1. 建立日期範圍的基礎 DataFrame
    df_entry_temp = pd.DataFrame(
        data=pd.date_range(beg_date, end_date, freq='D'),
        columns=['Date']
    ).assign(Date=lambda x: x['Date'].dt.date)
    
    # 2. 預處理 df_dfr_wp
    df_wp_clean = df_dfr_wp.set_index('Date').drop('Total', errors='ignore')
    
    # 3. 提取基礎欄位數據
    remittance_fee = df_wp_clean['remittance_fee'].values if 'remittance_fee' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    inbound = df_wp_clean['Inbound'].values if 'Inbound' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    unsuccessful_ach = df_wp_clean['Unsuccessful_ACH'].values if 'Unsuccessful_ACH' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    outbound = df_wp_clean['Outbound'].values if 'Outbound' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    handing_fee = df_wp_clean['handing_fee'].values if 'handing_fee' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    
    # 4. 提取 cub_rebate 和 received_ctbc_spt 數據
    cub_rebate_amount = cub_rebate['amount'].values if 'amount' in cub_rebate.columns else np.zeros(len(df_entry_temp))
    received_spt_amount = received_ctbc_spt['amount'].values if 'amount' in received_ctbc_spt.columns else np.zeros(len(df_entry_temp))
    interest_values = interest.values if isinstance(interest, pd.Series) else np.zeros(len(df_entry_temp))
    
    # 5. 驗證數據長度一致性
    expected_length = len(df_entry_temp)
    
    def pad_or_truncate(arr, length):
        if len(arr) < length:
            return np.pad(arr, (0, length - len(arr)), mode='constant', constant_values=0)
        return arr[:length]
    
    remittance_fee = pad_or_truncate(remittance_fee, expected_length)
    inbound = pad_or_truncate(inbound, expected_length)
    unsuccessful_ach = pad_or_truncate(unsuccessful_ach, expected_length)
    outbound = pad_or_truncate(outbound, expected_length)
    handing_fee = pad_or_truncate(handing_fee, expected_length)
    cub_rebate_amount = pad_or_truncate(cub_rebate_amount, expected_length)
    received_spt_amount = pad_or_truncate(received_spt_amount, expected_length)
    interest_values = pad_or_truncate(interest_values, expected_length)
    
    # 6. 計算中間值
    inbound_net_of_remittance_fee = inbound - remittance_fee
    received_ctbc_spt_104171 = inbound_net_of_remittance_fee - cub_rebate_amount
    
    # 7. 建立所有欄位
    df_entry_temp = df_entry_temp.assign(
        # 科目 101150 - 富邦銀行
        acc_101150_Received_CTBC_SPT=received_spt_amount * -1,
        
        # 科目 104171 - 中信信託帳戶
        acc_104171_內扣CTBCCC匯費=remittance_fee,
        acc_104171_ReceivedCTBCSPT=received_ctbc_spt_104171,
        acc_104171_ReceivedCTBCSPT退匯=unsuccessful_ach,
        acc_104171_OutCTBCSPT=outbound,
        acc_104171_others=cub_rebate_amount,
        
        # 科目 999995 - Cash Clearing
        acc_999995_others=cub_rebate_amount * -1,
        
        # 科目 530006 - 銀行手續費
        acc_530006__內扣CTBCCC匯費=remittance_fee * -1,
        acc_530006_收單_SPE=handing_fee,
        
        # 科目 200701 - 應付帳款
        acc_200701_ReceivedCTBCSPT退匯=unsuccessful_ach * -1,
        acc_200701_OutCTBCSPT=outbound * -1,
        
        # 科目 440001 - 利息收入
        acc_440001_interest=interest_values * -1,
    )
    
    # 8. 計算 200208 科目
    df_entry_temp['acc_200208_ReceivedCTBCSPT_positive'] = (
        df_entry_temp['acc_530006_收單_SPE'] +
        df_entry_temp['acc_104171_ReceivedCTBCSPT'] +
        df_entry_temp['acc_101150_Received_CTBC_SPT']
    )
    
    df_entry_temp['acc_200208_ReceivedCTBCSPT_negative'] = (
        df_entry_temp['acc_200208_ReceivedCTBCSPT_positive'] * -1
    )
    
    df_entry_temp['acc_200701_ReceivedCTBCSPT_negative'] = (
        df_entry_temp['acc_104171_ReceivedCTBCSPT'] * -1
    )
    
    logger.info(f"會計分錄整理完成: {len(df_entry_temp)} 天")
    return df_entry_temp


def validate_accounting_balance(df_entry_temp: pd.DataFrame) -> Dict[str, Any]:
    """
    驗證會計分錄借貸平衡
    
    Args:
        df_entry_temp: 寬格式分錄 DataFrame
        
    Returns:
        Dict: 驗證結果
    """
    # 取得所有 acc_ 開頭的欄位
    acc_cols = [col for col in df_entry_temp.columns if col.startswith('acc_')]
    
    # 計算每日總額
    daily_totals = df_entry_temp[acc_cols].sum(axis=1)
    
    # 計算總差額
    total_diff = daily_totals.sum()
    
    result = {
        'is_balanced': abs(total_diff) < 1,
        'total_diff': total_diff,
        'daily_max_diff': daily_totals.abs().max(),
        'unbalanced_days': (daily_totals.abs() >= 1).sum()
    }
    
    if result['is_balanced']:
        logger.info("會計平衡驗證通過")
    else:
        logger.warning(f"會計平衡驗證失敗: 總差額 {total_diff:,.2f}")
    
    return result
