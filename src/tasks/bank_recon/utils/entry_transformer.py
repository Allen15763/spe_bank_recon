"""
會計分錄轉換器
將每日匯總的寬格式資料轉換為標準的長格式會計分錄
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np

from src.utils import get_logger

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
        acc_104171_OutCTBCSPT=outbound,
        acc_104171_others=cub_rebate_amount,
        
        # 科目 999995 - Cash Clearing
        acc_999995_others=cub_rebate_amount * -1,
        
        # 科目 530006 - 銀行手續費
        acc_530006_內扣CTBCCC匯費=remittance_fee * -1,
        acc_530006_收單_SPE=handing_fee,
        
        # 科目 200701 - 應付帳款
        acc_200701_ReceivedCTBCSPT退匯=unsuccessful_ach * -1,
        acc_200701_OutCTBCSPT=outbound * -1,
        
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
    idx_discount = df.loc[df.號碼.str.contains(f"/{beg_date[5:7]}.*折讓總計", na=False, regex=True)].index[-1]

    acc_111301 = df.iloc[idx_discount, df.columns.get_loc('VAT')]
    acc_200701 = df.iloc[idx_discount, df.columns.get_loc('含稅')] * -1

    return acc_111301, acc_200701


def get_easyfund_service_fee_for_999995(df, beg_date: str) -> float:
    mask1 = df.開立日期.astype('string').str.contains(f"{beg_date[5:7]}", na=False, regex=True)
    mask2 = df.號碼.str.contains("[A-Z][A-Z]\d{8}", na=False, regex=True)
    mask3 = df.開立日期.astype('string').str.contains(f"{beg_date[:4]}", na=False, regex=True)
    idx_service_fee = df.loc[mask1 & mask2 & mask3, :].index[-1]
    return df.iloc[idx_service_fee, df.columns.get_loc('含稅')]


class AccountingEntryTransformer:
    """
    會計分錄轉換器

    將每日匯總的寬格式資料轉換為標準的長格式會計分錄
    """

    def __init__(self):
        """初始化轉換器，設定會計科目與交易類型的映射關係"""

        # 會計科目名稱映射
        self.account_descriptions = {
            # 可根據實際需求修改
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

        # 定義會計分錄的轉換規則
        # 格式: (原始欄位名稱, 會計科目, 交易類型, 科目描述key)
        self.entry_mapping = [
            # 200208科目 - 兩筆received_ctbc_spt (negative和positive)
            ('acc_200208_ReceivedCTBCSPT_negative', '200208', 'received_ctbc_spt', None),
            ('acc_200208_ReceivedCTBCSPT_positive', '200208', 'received_ctbc_spt', None),

            # 200701科目 - out, received, 退匯
            ('acc_200701_OutCTBCSPT', '200701', 'out_ctbc_spt', None),
            ('acc_200701_ReceivedCTBCSPT_negative', '200701', 'received_ctbc_spt', None),
            ('acc_200701_ReceivedCTBCSPT退匯', '200701', 'received_ctbc_spt_退匯', None),

            # 530006科目 - 收單SPE和內扣匯費
            ('acc_530006_收單_SPE', '530006', 'received_ctbc_spt', '收單_SPE'),
            ('acc_530006_內扣CTBCCC匯費', '530006', '內扣_ctbc_cc_匯費', '內扣CTBCCC匯費'),

            # 101150科目 - received
            ('acc_101150_Received_CTBC_SPT', '101150', 'received_ctbc_spt', None),

            # 104171科目 - out, received, 退匯, 內扣匯費
            ('acc_104171_OutCTBCSPT', '104171', 'out_ctbc_spt', None),
            ('acc_104171_ReceivedCTBCSPT', '104171', 'received_ctbc_spt', None),
            ('acc_104171_ReceivedCTBCSPT退匯', '104171', 'received_ctbc_spt_退匯', None),
            ('acc_104171_內扣CTBCCC匯費', '104171', '內扣_ctbc_cc_匯費', None),
            ('acc_104171_others', '104171', 'other', None),
            ('acc_104171_others_利息', '104171', 'other_利息', None),

            # 999995科目 - others
            ('acc_999995_others', '999995', 'other', None),

            # 440001 - interest
            ('acc_440001_interest', '440001', 'other_利息', None),
            ('acc_111302_interest', '111302', 'other_利息', None),
        ]

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

        Example:
            special_entries = [
                {
                    'accounting_date': '2025/11/21',
                    'transaction_type': 'other_利息',
                    'account_no': '104171',
                    'account_desc': 'Escrow Bank - CTBC TWD 4935',
                    'amount': 0.0,
                    'period': '2025-11'
                }
            ]
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

        說明:
            每個 entry_dict 可以包含以下欄位:
            - transaction_type: 交易類型 (必填)
            - amount: 金額 (必填)
            - account_desc: 科目描述 (選填，不填則使用預設)
            - desc_key: 描述key (選填，適用於有多個子描述的科目)

        Example:
            summary_data = {
                '111301': [
                    {'transaction_type': 'spe_withdrawal', 'amount': 0.0},
                    {'transaction_type': '期初數', 'amount': -12315.0, 'account_desc': '自訂描述'}
                ],
                '530006': [
                    {'transaction_type': 'spt', 'amount': 5000.0, 'desc_key': '收單_SPE'}
                ]
            }
        """
        summary_list = []

        for account_no, entries in summary_data.items():
            for entry in entries:
                # 取得科目描述 - 支援三種方式
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


class MonthlyConfig:
    """
    月度配置類別
    集中管理每月可能變動的參數
    """

    """
    月度會計分錄配置檔
    管理每月可能變動的業務規則和參數

    使用方式:
    1. 每月檢查完配置檔案，再來改這邊需要新增的分錄 e.g.利息
    2. 根據當月實際情況調整參數
    3. 在主程式中匯入對應月份的配置
    """

    def __init__(self, year: int, month: int, 
                 df_easyfund,
                 apcc_acquiring_charge,
                 ach_exps,
                 cod_remittance_fee,
                 df_ctbc_rebate_amt,
                 beg_date,
                 ):
        """
        初始化月度配置

        Args:
            year: 年份 (例如: 2025)
            month: 月份 (1-12)
        """
        self.year = year
        self.month = month
        self.period = f"{year}-{month:02d}"
        self.args = {
            'easyfund': df_easyfund,
            'apcc_acquiring': apcc_acquiring_charge,
            'ach_exps': ach_exps,
            'cod_remittance_fee': cod_remittance_fee,
            'ctbc_rebate_amt': df_ctbc_rebate_amt,
            'beg_date': beg_date,
        }

    def get_special_dates_config(self) -> Dict[str, List[Dict]]:
        """
        取得特殊日期的分錄配置

        Returns:
            字典，key為日期 (YYYY-MM-DD格式)，value為該日期的特殊分錄列表

        說明:
        - 某些日期可能有特殊的交易類型 (如利息、調整等)
        - 每月根據實際發生情況調整

        Note:
        - Key是日期，可以一天內含多筆紀錄
        """
        special_dates = {
            # 範例: 06/21有利息收入
            # '2025-10-21': [
            #     {
            #         'transaction_type': 'other_利息',
            #         'account_no': '104171',
            #         'amount': interest_income * .9  # 根據銀行對帳單的90%
            #     },
            #     {
            #         'transaction_type': 'other_利息',
            #         'account_no': '111302',
            #         'amount': interest_income * .1  # 根據銀行對帳單的10%
            #     },
            #     {
            #         'transaction_type': 'other_利息',
            #         'account_no': '440001',
            #         'amount': interest_income * -1  # 根據銀行對帳單填入
            #     }
            # ],

            # # 範例: 某些日期有other類型調整
            # '2025-11-06': [
            #     {
            #         'transaction_type': 'other',
            #         'account_no': '200701',
            #         'amount': 0.0
            #     }
            # ],
            # '2025-11-19': [
            #     {
            #         'transaction_type': 'other',
            #         'account_no': '200701',
            #         'amount': 0.0
            #     }
            # ],
            # '2025-11-20': [
            #     {
            #         'transaction_type': 'other',
            #         'account_no': '104171',
            #         'amount': 0.0
            #     }
            # ],

            # # 範例: 11/23開始有999995的清算分錄
            # '2025-11-23': [
            #     {
            #         'transaction_type': 'other',
            #         'account_no': '999995',
            #         'amount': 0.0
            #     }
            # ],
            # '2025-11-24': [
            #     {
            #         'transaction_type': 'other',
            #         'account_no': '999995',
            #         'amount': 0.0
            #     }
            # ],

            # 範例: 11/25有104171的other類型和999995的清算
            # '2025-11-25': [
            #     {
            #         'transaction_type': 'other',
            #         'account_no': '104171',
            #         'amount': 13951257.0
            #     },
            #     {
            #         'transaction_type': 'other',
            #         'account_no': '999995',
            #         'amount': -13951257.0
            #     }
            # ]
        }

        # # 自動為11/23之後的每一天添加999995的清算分錄
        # # (如果該日期尚未在special_dates中定義)
        # for day in range(26, 31):
        #     date_str = f'2025-11-{day:02d}'
        #     if date_str not in special_dates:
        #         special_dates[date_str] = []

        #     # 檢查是否已有999995的分錄
        #     has_999995 = any(
        #         entry['account_no'] == '999995'
        #         for entry in special_dates[date_str]
        #     )

        #     if not has_999995:
        #         special_dates[date_str].append({
        #             'transaction_type': 'other',
        #             'account_no': '999995',
        #             'amount': 0.0
        #         })

        return special_dates

    def get_summary_data(self) -> Dict[str, List[Dict]]:
        """
        取得月底匯總分錄資料

        Returns:
            字典，key為科目編號，value為該科目的匯總分錄列表

        說明:
        - 這些分錄通常包含期初數、期末餘額、各種調整項目
        - 金額需要根據實際對帳結果填入
        """
        summary_data = {
            # ===== 資產類科目 =====

            # 稅金應收款 - GST/VAT
            '111301': [
                {'transaction_type': 'spe_withdrawal', 'amount': 0.0},
                {'transaction_type': 'spl手續費調整', 'amount': get_easyfund_adj_service_fee_for_SPT(
                    self.args['easyfund'], self.args['beg_date'])[0]},
                {'transaction_type': '期初數', 'amount': -11_536.0},  # -12315.0
            ],

            # 稅金應收款 - WHT
            '111302': [
                {'transaction_type': '期初數', 'amount': 2_174_601.0},
            ],

            # 關係人往來 - APYTW
            '112001': [
                {'transaction_type': '期初數', 'amount': 0.0},
            ],

            # UB - RC-SPTTW
            '112002': [
                {'transaction_type': 'spt', 'amount': self.args['apcc_acquiring'] + self.args['ach_exps']},
                {'transaction_type': '期初數', 'amount': 181_118_860.0},  # 175819228.0
                {'transaction_type': '發票已開立沖轉', 'amount': -181_118_860.0},
            ],

            # VAT
            '113101': [
                {'transaction_type': '期初數', 'amount': 0.0},
            ],

            # 銀行存款 - 富邦
            '101150': [
                {'transaction_type': '期初數', 'amount': 0.0}
            ],

            # 託管銀行 - 中信
            '104171': [
                {'transaction_type': '期初數', 'amount': 1_394_478_080.0},  # 2739021556.0
            ],

            # ===== 負債類科目 =====

            # 代收代付 - Shopee
            '200208': [
                {'transaction_type': 'spt', 'amount': 0.0},
                {'transaction_type': 'spt', 'amount': 0.0},
                {'transaction_type': '期初數', 'amount': -990_741_706_660.0},  # -1014347397798.587
                {'transaction_type': '期初數', 'amount': 990_741_706_660.0},
            ],

            # 稅金應付款
            '200601': [
                {'transaction_type': '期初數', 'amount': 0.0},
            ],

            # 應付 - SPTTW託管
            '200701': [
                {'transaction_type': 'spe_withdrawal', 'amount': 0.0},
                {'transaction_type': 'spl手續費調整', 'amount': get_easyfund_adj_service_fee_for_SPT(
                    self.args['easyfund'], self.args['beg_date'])[1]},
                {'transaction_type': 'spt', 'amount': -get_easyfund_service_fee_for_999995(
                    self.args['easyfund'], self.args['beg_date']
                ) - self.args['cod_remittance_fee'] - self.args['ctbc_rebate_amt'] * -1},
                {'transaction_type': '期初數', 'amount': -1_431_762_212.0},  # -2770078192.5866
            ],

            # ===== 收入類科目 =====

            # 利息收入
            '440001': [
                {'transaction_type': '期初數', 'amount': 0},
            ],

            # 佣金支出
            '460103': [
                {'transaction_type': 'spt', 'amount': self.args['ach_exps'] * -1, 'desc_key': 'APCC_ACH', },
                {'transaction_type': 'spt', 'amount': self.args['apcc_acquiring'] * -1, 'desc_key': 'APCC_手續費'},
            ],

            # ===== 費用類科目 =====

            # 銀行手續費
            '530006': [
                # CTBC回饋金  df_ctbc_rebate['Actual received amount'].iloc[-1] * -1
                {'transaction_type': 'spt', 'amount': self.args['ctbc_rebate_amt'] * -1, 'desc_key': '回饋金'},
                # COD匯費
                {'transaction_type': 'spt', 'amount': self.args['cod_remittance_fee'], 'desc_key': 'COD匯費'},
            ],

            # ===== 清算科目 =====

            # 現金清算
            '999995': [
                {'transaction_type': 'spe_withdrawal', 'amount': 0.0},
                {'transaction_type': 'spl手續費調整', 'amount': -get_easyfund_adj_service_fee_for_SPT(
                    self.args['easyfund'], self.args['beg_date'])[0] - 
                    get_easyfund_adj_service_fee_for_SPT(self.args['easyfund'], self.args['beg_date'])[1]
                 },
                {'transaction_type': 'spt', 'amount': get_easyfund_service_fee_for_999995(self.args['easyfund'], 
                                                                                          self.args['beg_date'])},
            ]
        }

        return summary_data

    def get_business_rules(self) -> Dict:
        """
        取得業務規則配置

        Returns:
            包含各種業務規則的字典

        說明:
        - 定義某些科目的特殊處理邏輯
        - 金額計算的特殊規則
        """
        rules = {
            # 是否啟用999995清算分錄 (從某日期開始)
            # 'enable_999995_from_date': '2025-11-23',

            # 是否啟用104471科目 (others)
            # 'enable_104471': False,

            # 特殊調整項目
            'adjustments': {
                # 範例: 某個科目需要額外調整
                # '104171': {'date': '2025-11-25', 'amount': 13951257.0}
            },

            # 跳過金額為0的分錄 (可選)
            'skip_zero_amount': False,

            # 金額小數位數
            'amount_decimal_places': 2
        }

        return rules

    def validate_config(self) -> bool:
        """
        驗證配置的完整性和正確性

        Returns:
            True if valid, False otherwise
        """
        try:
            # 檢查特殊日期配置
            special_dates = self.get_special_dates_config()
            for date, entries in special_dates.items():
                assert isinstance(entries, list), f"特殊日期 {date} 的分錄必須是列表"
                for entry in entries:
                    assert 'transaction_type' in entry, "分錄必須包含 transaction_type"
                    assert 'account_no' in entry, "分錄必須包含 account_no"
                    assert 'amount' in entry, "分錄必須包含 amount"

                    # 驗證 account_desc 和 desc_key 不能同時存在
                    if 'account_desc' in entry and 'desc_key' in entry:
                        print(f"⚠️  警告: 日期 {date} 的分錄同時包含 account_desc 和 desc_key，將優先使用 account_desc")

            # 檢查匯總資料配置
            summary_data = self.get_summary_data()
            for account_no, entries in summary_data.items():
                assert isinstance(entries, list), f"科目 {account_no} 的匯總分錄必須是列表"
                for entry in entries:
                    assert 'transaction_type' in entry, "匯總分錄必須包含 transaction_type"
                    assert 'amount' in entry, "匯總分錄必須包含 amount"

                    # 驗證 account_desc 和 desc_key 不能同時存在
                    if 'account_desc' in entry and 'desc_key' in entry:
                        print(f"⚠️  警告: 科目 {account_no} 的匯總分錄同時包含 account_desc 和 desc_key，將優先使用 account_desc")

            print(f"✓ {self.period} 配置驗證通過")
            return True

        except AssertionError as e:
            print(f"✗ {self.period} 配置驗證失敗: {str(e)}")
            return False


