"""
會計分錄轉換器
將每日匯總的寬格式資料轉換為標準的長格式會計分錄

重構說明:
- 配置驅動：會計科目映射、分錄映射規則從 TOML 配置讀取
- 新增 ConfigurableEntryConfig 類取代硬編碼的 MonthlyConfig
- 支援月度配置檔案獨立管理
"""

from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import pandas as pd
import numpy as np

from src.utils import get_logger, load_toml

logger = get_logger("entry_transformer")


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
    remittance_fee = (
        df_wp_clean['remittance_fee'].values 
        if 'remittance_fee' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    )
    inbound = df_wp_clean['Inbound'].values if 'Inbound' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    unsuccessful_ach = (
        df_wp_clean['Unsuccessful_ACH'].values 
        if 'Unsuccessful_ACH' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    )
    outbound = df_wp_clean['Outbound'].values if 'Outbound' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    handing_fee = (
        df_wp_clean['handing_fee'].values 
        if 'handing_fee' in df_wp_clean.columns else np.zeros(len(df_entry_temp))
    )
    
    # 4. 提取 cub_rebate 和 received_ctbc_spt 數據
    cub_rebate_amount = cub_rebate['amount'].values if 'amount' in cub_rebate.columns else np.zeros(len(df_entry_temp))
    received_spt_amount = (
        received_ctbc_spt['amount'].values 
        if 'amount' in received_ctbc_spt.columns else np.zeros(len(df_entry_temp))
    )
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
        acc_104171_OutCTBCSPT=outbound + interest_values * .1,
        acc_104171_others=cub_rebate_amount,
        
        # 科目 999995 - Cash Clearing
        acc_999995_others=cub_rebate_amount * -1,
        
        # 科目 530006 - 銀行手續費
        acc_530006_內扣CTBCCC匯費=remittance_fee * -1,
        acc_530006_收單_SPE=handing_fee,
        
        # 科目 200701 - 應付帳款
        acc_200701_ReceivedCTBCSPT退匯=unsuccessful_ach * -1,
        acc_200701_OutCTBCSPT=outbound * -1 - interest_values * .1,
        
        # 科目 440001 - 利息收入
        acc_440001_interest=interest_values * -1,
        acc_104171_others_利息=interest_values * .9,
        acc_111302_interest=interest_values * .1,
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
        df_entry_temp['acc_200208_ReceivedCTBCSPT_negative']
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


def get_easyfund_adj_service_fee_for_SPT(df, beg_date: str) -> tuple:
    """從仲信手續費檔案取得折讓金額"""
    idx_discount = df.loc[df.號碼.str.contains(f"/{beg_date[5:7]}.*折讓總計", na=False, regex=True)].index[-1]

    acc_111301 = df.iloc[idx_discount, df.columns.get_loc('VAT')]
    acc_200701 = df.iloc[idx_discount, df.columns.get_loc('含稅')] * -1

    return acc_111301, acc_200701


def get_easyfund_service_fee_for_999995(df, beg_date: str) -> float:
    """從仲信手續費檔案取得服務費金額"""
    mask1 = df.開立日期.astype('string').str.contains(f"{beg_date[5:7]}", na=False, regex=True)
    mask2 = df.號碼.str.contains("[A-Z][A-Z]\\d{8}", na=False, regex=True)
    mask3 = df.開立日期.astype('string').str.contains(f"{beg_date[:4]}", na=False, regex=True)
    idx_service_fee = df.loc[mask1 & mask2 & mask3, :].index[-1]
    return df.iloc[idx_service_fee, df.columns.get_loc('含稅')]


class AccountingEntryTransformer:
    """
    會計分錄轉換器 (配置驅動版本)

    將每日匯總的寬格式資料轉換為標準的長格式會計分錄
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化轉換器
        
        Args:
            config: 從 TOML 讀取的配置，包含 accounts、accounts_detail、entry_mapping
        """
        self.config = config or {}
        self.logger = get_logger("AccountingEntryTransformer")
        
        # 從配置讀取會計科目描述
        self.account_descriptions = self._build_account_descriptions()
        
        # 從配置讀取分錄映射規則
        self.entry_mapping = self._build_entry_mapping()
        
        self.logger.debug(f"初始化完成: {len(self.account_descriptions)} 個科目, {len(self.entry_mapping)} 個映射規則")

    def _build_account_descriptions(self) -> Dict[str, Any]:
        """從配置建立會計科目描述映射"""
        accounts = self.config.get('accounts', {})
        accounts_detail = self.config.get('accounts_detail', {})
        
        # 合併基本科目和有子分類的科目
        result = dict(accounts)
        
        for account_no, details in accounts_detail.items():
            result[account_no] = details
        
        # 如果配置為空，使用預設值
        if not result:
            self.logger.warning("未找到配置，使用預設會計科目映射")
            result = {
                '200208': 'Receive on behalf of Shopee',
                '200701': 'Amount due to SPTTW-Escrow',
                '530006': {
                    '收單_SPE': 'Bank transaction fee(Remittance fee)-收單-SPE',
                    '回饋金': 'Bank transaction fee(Remittance fee)-CTBC收單手續費回饋金',
                    '內扣CTBCCC匯費': 'Bank transaction fee(Remittance fee)-收單轉帳匯費',
                    'COD匯費': 'Bank transaction fee(Remittance fee)-COD匯費',
                },
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
                '460103': {
                    'APCC_ACH': 'Commission charge-RC-SPTTW-ACH/eACH/EDI',
                    'APCC_手續費': 'Commission charge-RC-SPTTW-APCC手續費',
                }
            }
        
        return result

    def _build_entry_mapping(self) -> List[Tuple]:
        """從配置建立分錄映射規則"""
        mapping_config = self.config.get('entry_mapping', [])
        
        result = []
        for item in mapping_config:
            column = item.get('column')
            account_no = item.get('account_no')
            transaction_type = item.get('transaction_type')
            desc_key = item.get('desc_key')
            
            if column and account_no and transaction_type:
                result.append((column, account_no, transaction_type, desc_key))
        
        # 如果配置為空，使用預設值
        if not result:
            self.logger.warning("未找到配置，使用預設分錄映射規則")
            result = [
                ('acc_200208_ReceivedCTBCSPT_negative', '200208', 'received_ctbc_spt', None),
                ('acc_200208_ReceivedCTBCSPT_positive', '200208', 'received_ctbc_spt', None),
                ('acc_200701_OutCTBCSPT', '200701', 'out_ctbc_spt', None),
                ('acc_200701_ReceivedCTBCSPT_negative', '200701', 'received_ctbc_spt', None),
                ('acc_200701_ReceivedCTBCSPT退匯', '200701', 'received_ctbc_spt_退匯', None),
                ('acc_530006_收單_SPE', '530006', 'received_ctbc_spt', '收單_SPE'),
                ('acc_530006_內扣CTBCCC匯費', '530006', '內扣_ctbc_cc_匯費', '內扣CTBCCC匯費'),
                ('acc_101150_Received_CTBC_SPT', '101150', 'received_ctbc_spt', None),
                ('acc_104171_OutCTBCSPT', '104171', 'out_ctbc_spt', None),
                ('acc_104171_ReceivedCTBCSPT', '104171', 'received_ctbc_spt', None),
                ('acc_104171_ReceivedCTBCSPT退匯', '104171', 'received_ctbc_spt_退匯', None),
                ('acc_104171_內扣CTBCCC匯費', '104171', '內扣_ctbc_cc_匯費', None),
                ('acc_104171_others', '104171', 'other', None),
                ('acc_104171_others_利息', '104171', 'other_利息', None),
                ('acc_999995_others', '999995', 'other', None),
                ('acc_440001_interest', '440001', 'other_利息', None),
                ('acc_111302_interest', '111302', 'other_利息', None),
            ]
        
        return result

    def get_account_description(self, account_no: str, desc_key: str = None) -> str:
        """
        取得會計科目描述

        Args:
            account_no: 會計科目編號
            desc_key: 科目描述的key (適用於有多個描述的科目如530006)

        Returns:
            會計科目描述
        """
        desc = self.account_descriptions.get(account_no, '')

        # 如果描述是字典，根據desc_key取得具體描述
        if isinstance(desc, dict):
            return desc.get(desc_key, '')

        return desc

    def transform(self, df_entry_temp: pd.DataFrame) -> pd.DataFrame:
        """
        轉換寬格式資料為長格式會計分錄

        Args:
            df_entry_temp: 寬格式的會計資料，每行代表一天的各科目金額

        Returns:
            長格式的會計分錄資料
        """
        entries_list = []

        # 確保Date欄位是日期格式
        if df_entry_temp['Date'].dtype != 'datetime64[ns]':
            df_entry_temp = df_entry_temp.copy()
            df_entry_temp['Date'] = pd.to_datetime(df_entry_temp['Date'])

        # 遍歷每一天的資料
        for idx, row in df_entry_temp.iterrows():
            date = row['Date']

            # 格式化日期為 YYYY/MM/DD
            accounting_date = date.strftime('%Y/%m/%d')

            # 取得期間 (YYYY-MM格式)
            period = date.strftime('%Y-%m')

            # 根據entry_mapping產生分錄
            for column_name, account_no, transaction_type, desc_key in self.entry_mapping:
                # 檢查欄位是否存在
                if column_name not in row:
                    continue

                amount = row[column_name]

                # 處理NaN值，轉換為0.0
                if pd.isna(amount):
                    amount = 0.0

                # 取得會計科目描述
                account_desc = self.get_account_description(account_no, desc_key)

                # 建立分錄
                entry = {
                    'accounting_date': accounting_date,
                    'transaction_type': transaction_type,
                    'account_no': account_no,
                    'account_desc': account_desc,
                    'amount': amount,
                    'period': period
                }

                entries_list.append(entry)

        # 轉換為DataFrame
        df_result = pd.DataFrame(entries_list)

        return df_result

    def add_special_entries(self, df_result: pd.DataFrame,
                            special_entries: List[Dict]) -> pd.DataFrame:
        """
        新增特殊分錄 (如other_利息、期初數、調整後期末餘額等)

        Args:
            df_result: 已轉換的分錄資料
            special_entries: 特殊分錄列表，每個元素為包含分錄資訊的字典

        Returns:
            包含特殊分錄的完整資料
        """
        df_special = pd.DataFrame(special_entries)
        df_combined = pd.concat([df_result, df_special], ignore_index=True)

        return df_combined

    def add_summary_entries(self, period: str,
                            summary_data: Dict[str, List[Dict]]) -> pd.DataFrame:
        """
        新增匯總分錄 (期初數、調整後期末餘額等)

        Args:
            period: 期間 (YYYY-MM格式)
            summary_data: 匯總資料，格式為 {account_no: [entry_dict1, entry_dict2, ...]}

        Returns:
            匯總分錄的DataFrame
        """
        summary_list = []

        for account_no, entries in summary_data.items():
            for entry in entries:
                # 取得科目描述
                # 優先順序: 1. 配置中的account_desc  2. 配置中的desc_key  3. 預設
                if 'account_desc' in entry:
                    # 方式1: 配置中明確指定 account_desc
                    account_desc = entry['account_desc']
                elif 'desc_key' in entry:
                    # 方式2: 使用 desc_key 從 account_descriptions 取得
                    account_desc = self.get_account_description(account_no, entry['desc_key'])
                else:
                    # 方式3: 使用預設描述
                    account_desc = self.get_account_description(account_no)

                summary_entry = {
                    'accounting_date': np.nan,  # 匯總分錄沒有具體日期
                    'transaction_type': entry['transaction_type'],
                    'account_no': account_no,
                    'account_desc': account_desc,
                    'amount': entry['amount'],
                    'period': period
                }
                summary_list.append(summary_entry)

        return pd.DataFrame(summary_list)


class ConfigurableEntryConfig:
    """
    配置驅動的 Entry 配置類
    
    從 TOML 配置檔讀取期初數、特殊分錄等月度調整參數，
    取代原本硬編碼的 MonthlyConfig
    """

    def __init__(self, 
                 year: int, 
                 month: int,
                 entry_config: Dict[str, Any],
                 monthly_config: Dict[str, Any],
                 runtime_params: Dict[str, Any]):
        """
        初始化配置驅動的 Entry 配置
        
        Args:
            year: 年份
            month: 月份
            entry_config: 從主配置檔讀取的 [entry] 區段
            monthly_config: 從月度配置檔讀取的配置
            runtime_params: 運行時參數 (從 context 取得的動態計算結果)
        """
        self.year = year
        self.month = month
        self.period = f"{year}-{month:02d}"
        self.entry_config = entry_config
        self.monthly_config = monthly_config
        self.runtime_params = runtime_params
        self.logger = get_logger("ConfigurableEntryConfig")
        
        self.logger.info(f"初始化配置: {self.period}")

    def get_special_dates_config(self) -> Dict[str, List[Dict]]:
        """
        取得特殊日期的分錄配置
        
        Returns:
            字典，key為日期 (YYYY-MM-DD格式)，value為該日期的特殊分錄列表
        """
        special_dates = self.monthly_config.get('special_dates', {})
        
        # 將 TOML 格式轉換為內部格式
        result = {}
        for date_str, entries in special_dates.items():
            if isinstance(entries, list):
                result[date_str] = entries
        
        self.logger.debug(f"載入特殊日期配置: {len(result)} 個日期")
        return result

    def get_summary_data(self) -> Dict[str, List[Dict]]:
        """
        取得月底匯總分錄資料
        
        從配置檔讀取期初數，結合運行時參數生成完整的匯總資料
        
        Returns:
            字典，key為科目編號，value為該科目的匯總分錄列表
        """
        opening_balances = self.monthly_config.get('opening_balances', {})
        reversal_amounts = self.monthly_config.get('reversal_amounts', {})
        
        # 從運行時參數取得計算結果
        df_easyfund = self.runtime_params.get('df_easyfund')
        beg_date = self.runtime_params.get('beg_date')
        apcc_acquiring = self.runtime_params.get('apcc_acquiring_charge', 0)
        ach_exps = self.runtime_params.get('ach_exps', 0)
        cod_remittance_fee = self.runtime_params.get('cod_remittance_fee', 0)
        ctbc_rebate_amt = self.runtime_params.get('ctbc_rebate_amt', 0)
        
        # 計算仲信手續費相關金額
        adj_service_fee = (0, 0)
        service_fee_999995 = 0
        
        if df_easyfund is not None and beg_date:
            try:
                adj_service_fee = get_easyfund_adj_service_fee_for_SPT(df_easyfund, beg_date)
                service_fee_999995 = get_easyfund_service_fee_for_999995(df_easyfund, beg_date)
            except Exception as e:
                self.logger.warning(f"計算仲信手續費金額失敗: {e}")
        
        summary_data = {
            # ===== 資產類科目 =====
            '111301': [
                {'transaction_type': 'spe_withdrawal', 'amount': 0.0},
                {'transaction_type': 'spl手續費調整', 'amount': adj_service_fee[0]},
                {'transaction_type': '期初數', 'amount': opening_balances.get('111301', 0)},
            ],

            '111302': [
                {'transaction_type': '期初數', 'amount': opening_balances.get('111302', 0)},
            ],

            '112001': [
                {'transaction_type': '期初數', 'amount': opening_balances.get('112001', 0)},
            ],

            '112002': [
                {'transaction_type': 'spt', 'amount': apcc_acquiring + ach_exps},
                {'transaction_type': '期初數', 'amount': opening_balances.get('112002', 0)},
                {'transaction_type': '發票已開立沖轉', 'amount': reversal_amounts.get(
                    '112002_reversal', -opening_balances.get('112002', 0))},
            ],

            '113101': [
                {'transaction_type': '期初數', 'amount': opening_balances.get('113101', 0)},
            ],

            '101150': [
                {'transaction_type': '期初數', 'amount': opening_balances.get('101150', 0)}
            ],

            '104171': [
                {'transaction_type': '期初數', 'amount': opening_balances.get('104171', 0)},
            ],

            # ===== 負債類科目 =====
            '200208': [
                {'transaction_type': 'spt', 'amount': 0.0},
                {'transaction_type': 'spt', 'amount': 0.0},
                {'transaction_type': '期初數', 'amount': opening_balances.get('200208_credit', 0)},
                {'transaction_type': '期初數', 'amount': opening_balances.get('200208_debit', 0)},
            ],

            '200601': [
                {'transaction_type': '期初數', 'amount': opening_balances.get('200601', 0)},
            ],

            '200701': [
                {'transaction_type': 'spe_withdrawal', 'amount': 0.0},
                {'transaction_type': 'spl手續費調整', 'amount': adj_service_fee[1]},
                {'transaction_type': 'spt', 'amount': -service_fee_999995 - cod_remittance_fee - ctbc_rebate_amt * -1},
                {'transaction_type': '期初數', 'amount': opening_balances.get('200701', 0)},
            ],

            # ===== 收入類科目 =====
            '440001': [
                {'transaction_type': '期初數', 'amount': opening_balances.get('440001', 0)},
            ],

            '460103': [
                {'transaction_type': 'spt', 'amount': ach_exps * -1, 'desc_key': 'APCC_ACH'},
                {'transaction_type': 'spt', 'amount': apcc_acquiring * -1, 'desc_key': 'APCC_手續費'},
            ],

            # ===== 費用類科目 =====
            '530006': [
                {'transaction_type': 'spt', 'amount': ctbc_rebate_amt * -1, 'desc_key': '回饋金'},
                {'transaction_type': 'spt', 'amount': cod_remittance_fee, 'desc_key': 'COD匯費'},
            ],

            # ===== 清算科目 =====
            '999995': [
                {'transaction_type': 'spe_withdrawal', 'amount': 0.0},
                {'transaction_type': 'spl手續費調整', 'amount': -adj_service_fee[0] - adj_service_fee[1]},
                {'transaction_type': 'spt', 'amount': service_fee_999995},
            ]
        }

        return summary_data

    def get_business_rules(self) -> Dict:
        """
        取得業務規則配置
        
        Returns:
            包含各種業務規則的字典
        """
        validation_config = self.entry_config.get('validation', {})
        
        rules = {
            'skip_zero_amount': validation_config.get('skip_zero_amount', False),
            'amount_decimal_places': validation_config.get('amount_decimal_places', 2),
        }

        return rules

    def validate_config(self) -> bool:
        """
        驗證配置的完整性和正確性
        
        Returns:
            True if valid, False otherwise
        """
        try:
            # 檢查期初數配置
            opening_balances = self.monthly_config.get('opening_balances', {})
            if not opening_balances:
                self.logger.warning("期初數配置為空")
            
            # 檢查特殊日期配置格式
            special_dates = self.get_special_dates_config()
            for date, entries in special_dates.items():
                if not isinstance(entries, list):
                    raise ValueError(f"特殊日期 {date} 的分錄必須是列表")
                for entry in entries:
                    if 'account_no' not in entry:
                        raise ValueError(f"特殊日期 {date} 的分錄缺少 account_no")
                    if 'transaction_type' not in entry:
                        raise ValueError(f"特殊日期 {date} 的分錄缺少 transaction_type")
                    if 'amount' not in entry:
                        raise ValueError(f"特殊日期 {date} 的分錄缺少 amount")
            
            self.logger.info(f"✓ {self.period} 配置驗證通過")
            return True

        except Exception as e:
            self.logger.error(f"✗ {self.period} 配置驗證失敗: {str(e)}")
            return False


# 保留 MonthlyConfig 作為 ConfigurableEntryConfig 的別名，確保向後相容
MonthlyConfig = ConfigurableEntryConfig
