"""
會計分錄處理器
處理會計分錄的生成、驗證和輸出
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np

from src.utils import get_logger
from .entry_transformer import AccountingEntryTransformer, MonthlyConfig

logger = get_logger("entry_processor")


class AccountingEntryProcessor:
    """
    會計分錄處理器
    整合轉換器和配置，提供完整的處理流程

    會計分錄處理系統 - 主程式
    整合所有模組，提供完整的會計分錄轉換流程

    使用流程:
    1. 準備原始寬格式資料 (df_entry_temp)
    2. 設定當月配置 (monthly_config.py)
    3. 執行轉換流程
    4. 驗證並輸出結果
    """

    def __init__(self, year: int, month: int, config_obj: MonthlyConfig):
        """
        初始化處理器

        Args:
            year: 年份
            month: 月份
        """
        self.year = year
        self.month = month
        self.period = f"{year}-{month:02d}"

        # 初始化轉換器
        self.transformer = AccountingEntryTransformer()

        # 載入月度配置
        self.config = config_obj

        # 驗證配置
        if not self.config.validate_config():
            raise ValueError(f"配置驗證失敗: {self.period}")

        print("=== 會計分錄處理器已初始化 ===")
        print(f"處理期間: {self.period}")
        print("配置驗證: ✓ 通過\n")

    def process(self, df_entry_temp: pd.DataFrame) -> pd.DataFrame:
        """
        執行完整的會計分錄處理流程

        Args:
            df_entry_temp: 原始寬格式資料

        Returns:
            處理完成的長格式會計分錄
        """
        print("=== 開始處理會計分錄 ===\n")

        # ===== 步驟1: 基本轉換 =====
        print("步驟1: 執行基本會計分錄轉換...")
        df_entries = self.transformer.transform(df_entry_temp)
        print(f"✓ 完成，產生 {len(df_entries)} 筆基本分錄\n")

        # ===== 步驟2: 新增特殊日期分錄 =====
        print("步驟2: 新增特殊日期分錄...")
        df_entries = self._add_special_entries(df_entries, df_entry_temp)
        print(f"✓ 完成，目前共 {len(df_entries)} 筆分錄\n")

        # ===== 步驟3: 新增月底匯總分錄 =====
        print("步驟3: 新增月底匯總分錄...")
        df_summary = self._create_summary_entries()
        print(f"✓ 完成，產生 {len(df_summary)} 筆匯總分錄\n")

        # ===== 步驟4: 合併所有分錄 =====
        print("步驟4: 合併所有分錄...")
        df_final = pd.concat([df_entries, df_summary], ignore_index=True)
        print(f"✓ 完成，最終共 {len(df_final)} 筆分錄\n")

        # ===== 步驟5: 應用業務規則 =====
        print("步驟5: 應用業務規則...")
        df_final = self._apply_business_rules(df_final)
        print("✓ 完成\n")

        print("=== 處理完成 ===\n")
        return df_final

    def _add_special_entries(self, df_entries: pd.DataFrame,
                             df_entry_temp: pd.DataFrame) -> pd.DataFrame:
        """
        新增特殊日期的分錄

        Args:
            df_entries: 現有的分錄資料
            df_entry_temp: 原始資料 (用於檢查日期是否存在)

        Returns:
            包含特殊分錄的資料
        """
        special_dates_config = self.config.get_special_dates_config()
        special_entries = []

        # 將df_entry_temp的日期轉換為字串格式以便比對
        existing_dates = df_entry_temp['Date'].dt.strftime('%Y-%m-%d').values

        for date_str, entries in special_dates_config.items():
            # 檢查該日期是否存在於原始資料中
            if date_str not in existing_dates:
                continue

            # 為該日期的每個特殊分錄建立完整的分錄記錄
            for entry in entries:
                account_no = entry['account_no']
                transaction_type = entry['transaction_type']
                amount = entry['amount']

                # 取得科目描述 - 支援三種方式

                # 優先順序: 1. 配置中的account_desc  2. 配置中的desc_key  3. 預設
                if 'account_desc' in entry:
                    # 方式1: 配置中明確指定 account_desc
                    account_desc = entry['account_desc']
                elif 'desc_key' in entry:
                    # 方式2: 使用 desc_key 從 account_descriptions 取得
                    account_desc = self.transformer.get_account_description(
                        account_no,
                        entry['desc_key']
                    )
                else:
                    # 方式3: 使用預設描述
                    account_desc = self.transformer.get_account_description(account_no)

                # 建立分錄
                special_entry = {
                    'accounting_date': date_str.replace('-', '/'),
                    'transaction_type': transaction_type,
                    'account_no': account_no,
                    'account_desc': account_desc,
                    'amount': amount,
                    'period': self.period
                }

                special_entries.append(special_entry)

        if special_entries:
            df_special = pd.DataFrame(special_entries)
            df_combined = pd.concat([df_entries, df_special], ignore_index=True)
            print(f"  新增了 {len(special_entries)} 筆特殊分錄")
            return df_combined
        else:
            print("  本月無特殊分錄")
            return df_entries

    def _create_summary_entries(self) -> pd.DataFrame:
        """
        建立月底匯總分錄

        Returns:
            匯總分錄的DataFrame
        """
        summary_data = self.config.get_summary_data()
        return self.transformer.add_summary_entries(self.period, summary_data)

    def _apply_business_rules(self, df_final: pd.DataFrame) -> pd.DataFrame:
        """
        應用業務規則

        Args:
            df_final: 完整的分錄資料

        Returns:
            套用規則後的資料
        """
        rules = self.config.get_business_rules()

        # 規則1: 是否跳過金額為0的分錄
        if rules.get('skip_zero_amount', False):
            original_count = len(df_final)
            df_final = df_final[df_final['amount'] != 0].reset_index(drop=True)
            removed_count = original_count - len(df_final)
            if removed_count > 0:
                print(f"  移除了 {removed_count} 筆金額為0的分錄")

        # 規則2: 金額小數位數處理
        decimal_places = rules.get('amount_decimal_places', None)
        if decimal_places is not None:
            df_final['amount'] = df_final['amount'].round(decimal_places)
            print(f"  金額已四捨五入至小數點後 {decimal_places} 位")

        # 規則3: 排序 (按日期和科目)
        df_final = df_final.sort_values(
            by=['accounting_date', 'account_no'],
            na_position='last'
        ).reset_index(drop=True)

        return df_final

    def generate_report(self, df_final: pd.DataFrame):
        """
        產生處理報告

        Args:
            df_final: 最終的分錄資料
        """
        print("=== 處理報告 ===\n")

        # 基本統計
        print(f"處理期間: {self.period}")
        print(f"總分錄數: {len(df_final)}")
        # print(f"涵蓋日期: {df_final['accounting_date'].min()} 至 {df_final['accounting_date'].max()}")
        print(f"會計科目數: {df_final['account_no'].nunique()}")
        print(f"交易類型數: {df_final['transaction_type'].nunique()}\n")

        # 交易類型分布
        print("=== 交易類型分布 ===")
        type_counts = df_final['transaction_type'].value_counts()
        for trans_type, count in type_counts.items():
            print(f"{trans_type:30s}: {count:6d} 筆")

        # 科目分布 (只顯示前10個)
        print("\n=== 主要科目分布 (Top 10) ===")
        account_counts = df_final['account_no'].value_counts().head(10)
        for account, count in account_counts.items():
            desc = df_final[df_final['account_no'] == account]['account_desc'].iloc[0]
            print(f"{account} - {desc[:40]:40s}: {count:6d} 筆")

        # 金額統計
        daily_types = ['內扣_ctbc_cc_匯費', 'received_ctbc_spt', 
                       'received_ctbc_spt_退匯', 'out_ctbc_spt', 'other', 'other_利息']
        mask_movement = df_final['transaction_type'].isin(daily_types)
        mask_dr = df_final['amount'] > 0
        mask_cr = df_final['amount'] < 0
        print("\n=== 金額統計 ===")
        print(f"總借方金額: {df_final[df_final['amount'] > 0]['amount'].sum():,.2f}")
        print(f"總貸方金額: {df_final[df_final['amount'] < 0]['amount'].sum():,.2f}")

        print(f"movement借方金額: {df_final.loc[mask_dr & mask_movement, 'amount'].sum():,.2f}")
        print(f"movement貸方金額: {df_final.loc[mask_cr & mask_movement, 'amount'].sum():,.2f}")

        temp_dict = df_final.loc[mask_movement, :].groupby(['account_no']).amount.sum().map(lambda x: int(x)).to_dict()
        for k, v in temp_dict.items():
            print(f"\t movement by account: {k}, {v:,.0f}")

        print(f"最大金額: {df_final['amount'].max():,.2f}")
        print(f"最小金額: {df_final['amount'].min():,.2f}")

        print("\n" + "=" * 60 + "\n")


def calculate_daily_balance(
    df: pd.DataFrame,
    beg_amt: float,
    movement_col: str = 'daily_movement',
    balance_col: str = 'balance_entry'
) -> pd.DataFrame:
    """
    計算每日餘額（基於累計變動）

    計算邏輯：
    - 每日餘額 = 期初金額 + 當日累計變動
    - 自動處理空值('-', '', NaN)和千分位逗號格式

    Parameters:
    -----------
    df : pd.DataFrame
        包含每日變動欄位的資料框
    beg_amt : float
        期初金額（起始餘額）
    movement_col : str, default='daily_movement'
        每日變動欄位名稱
    balance_col : str, default='balance_entry'
        輸出餘額欄位名稱

    Returns:
    --------
    pd.DataFrame
        新增餘額欄位及差異檢核欄位的資料框

    Examples:
    ---------
    >>> df = pd.DataFrame({
    ...     'accounting_date': ['2025/11/1', '2025/11/2'],
    ...     'daily_movement': ['-', '100,000']
    ... })
    >>> result = calculate_daily_balance(df, beg_amt=1000000)
    """
    # 步驟 1: 複製原始資料（避免 SettingWithCopyWarning）
    df_result = df.copy()

    # 步驟 2: 資料清理與轉換
    # 2.1 將特殊字元 ('-', '') 替換為 NaN
    daily_movement_series = (
        df_result[movement_col]
        .replace(['-', '', ' '], np.nan)  # 處理空值符號
    )

    # 2.2 轉換為字串並移除千分位逗號
    daily_movement_series = (
        daily_movement_series
        .astype(str)
        .str.replace(',', '', regex=False)  # 移除千分位
        .str.strip()  # 移除前後空白
        .replace('nan', np.nan)  # 處理字串 'nan'
    )

    # 2.3 轉換為數值型態，無法轉換的設為 NaN，最後填充 0
    daily_movement_clean = pd.to_numeric(
        daily_movement_series,
        errors='coerce'  # 轉換失敗時設為 NaN
    ).fillna(0)

    # 步驟 3: 計算累計餘額
    # 公式: 餘額 = 期初金額 + Σ(每日變動)
    df_result[balance_col] = beg_amt + daily_movement_clean.cumsum()

    # 步驟 4: 差異檢核（如果有參考餘額欄位）
    if 'balance_dfr' in df_result.columns:
        df_result['check'] = df_result['balance_dfr'] - df_result[balance_col]
        df_result['check_pct'] = (
            df_result['check'] / df_result['balance_dfr'] * 100
        ).round(2)

    logger.info(f"每日銀行存款餘額計算完成: 期初 {beg_amt:,.0f}, 期末 {df_result[balance_col].iloc[-1]:,.0f}")
    return df_result


def dfr_balance_check(
    df_transactions: pd.DataFrame,
    df_dfr_balance: pd.DataFrame,
    account_no: str = '104171',
    transaction_types: str = '0[1-5]',
    beg_transaction_type: str = '00.期初數'
) -> pd.DataFrame:
    """
    DFR 帳戶餘額檢核功能

    功能說明：
    1. 從交易明細中篩選指定帳戶的特定交易類型
    2. 按日期彙總每日變動金額
    3. 計算每日餘額並與 DFR 系統餘額比對

    Parameters:
    -----------
    df_transactions : pd.DataFrame
        交易明細資料，需包含欄位：
        - account_no: 帳號
        - transaction_type: 交易類型
        - accounting_date: 會計日期
        - amount: 金額
    df_dfr_balance : pd.DataFrame
        DFR 系統餘額資料，需包含欄位：
        - Balance: 餘額
    account_no : str, default='104171'
        要檢核的帳號
    transaction_types : str, default='0[1-5]'
        交易類型篩選條件（支援正則表達式）
    beg_transaction_type : str, default='00.期初數'
        期初數的交易類型代碼

    Returns:
    --------
    pd.DataFrame
        包含以下欄位的資料框：
        - accounting_date: 會計日期
        - daily_movement: 每日變動金額
        - balance_dfr: DFR 系統餘額
        - balance_entry: 計算餘額
        - check: 差異金額
        - check_pct: 差異百分比

    Examples:
    ---------
    >>> result = dfr_balance_check(df_trans, df_dfr, account_no='104171')
    >>> # 檢視差異超過 1% 的日期
    >>> result[result['check_pct'].abs() > 1]
    """
    # ========== 步驟 1: 資料驗證 ==========
    # 確認必要欄位存在
    required_cols_trans = ['account_no', 'transaction_type', 'accounting_date', 'amount']
    missing_cols = [col for col in required_cols_trans if col not in df_transactions.columns]
    if missing_cols:
        raise ValueError(f"交易資料缺少必要欄位: {missing_cols}")

    if 'balance' not in df_dfr_balance.columns:
        raise ValueError("DFR 資料缺少 'balance' 欄位")

    # ========== 步驟 2: 篩選交易資料 ==========
    # 2.1 建立查詢條件
    query_condition = (
        f"account_no == '{account_no}' and "
        f"transaction_type.str.contains('{transaction_types}', regex=True)"
    )

    # 2.2 篩選符合條件的交易
    df_filtered = df_transactions.query(query_condition).copy()

    if df_filtered.empty:
        raise ValueError(f"查無符合條件的交易資料: {query_condition}")

    # ========== 步驟 3: 計算每日變動金額 ==========
    df_daily_movement = (
        df_filtered
        .groupby('accounting_date', as_index=False)['amount']
        .sum()  # 按日期彙總金額
        .rename(columns={'amount': 'daily_movement'})  # 重新命名欄位
    )

    # ========== 步驟 4: 合併 DFR 系統餘額 ==========
    # 確保兩個資料框的長度一致
    if len(df_daily_movement) != len(df_dfr_balance):
        print(f"⚠️ 警告: 交易筆數({len(df_daily_movement)}) 與 DFR 餘額筆數({len(df_dfr_balance)}) 不一致")

    # 直接賦值 DFR 餘額（假設順序一致）
    df_daily_movement['balance_dfr'] = df_dfr_balance['balance'].values

    # ========== 步驟 5: 取得期初金額 ==========
    beg_query = (
        f"account_no == '{account_no}' and "
        f"transaction_type == '{beg_transaction_type}'"
    )
    df_beg = df_transactions.query(beg_query)

    if df_beg.empty:
        raise ValueError(f"查無期初數資料: {beg_query}")

    beginning_amount = df_beg['amount'].values[0]

    # ========== 步驟 6: 計算每日餘額並進行檢核 ==========
    df_result = calculate_daily_balance(
        df_daily_movement,
        beg_amt=beginning_amount
    )

    return df_result


def summarize_balance_check(df_result: pd.DataFrame, tolerance: float = 0.01) -> dict:
    """
    彙總餘額檢核結果

    Parameters:
    -----------
    df_result : pd.DataFrame
        calculate_daily_balance 的輸出結果
    tolerance : float, default=0.01
        容許誤差比例（0.01 = 1%）

    Returns:
    --------
    dict
        檢核統計摘要
    """
    if 'check' not in df_result.columns:
        return {"message": "無檢核欄位"}

    summary = {
        "總筆數": len(df_result),
        "完全相符筆數": (df_result['check'] == 0).sum(),
        "有差異筆數": (df_result['check'] != 0).sum(),
        "最大差異金額": df_result['check'].abs().max(),
        "平均差異金額": df_result['check'].abs().mean(),
    }

    if 'check_pct' in df_result.columns:
        summary["超過容許誤差筆數"] = (df_result['check_pct'].abs() > tolerance * 100).sum()

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


def validate_result(df_entry_long: pd.DataFrame):
    """
    驗證轉換結果

    檢查項目:
    1. 必要欄位是否完整
    2. 金額欄位是否為數值
    3. 日期格式是否正確
    4. 是否有異常值
    """
    print("\n=== 資料驗證 ===")

    # 檢查必要欄位
    required_columns = ['accounting_date', 'transaction_type', 'account_no',
                        'account_desc', 'amount', 'period']
    missing_columns = set(required_columns) - set(df_entry_long.columns)
    if missing_columns:
        logger.warning(f"⚠️  缺少欄位: {missing_columns}")
    else:
        logger.info("✓ 所有必要欄位完整")

    # 檢查金額欄位
    if df_entry_long['amount'].dtype in ['float64', 'int64']:
        logger.info("✓ 金額欄位格式正確")
    else:
        logger.warning(f"⚠️  金額欄位格式異常: {df_entry_long['amount'].dtype}")

    # 檢查是否有空值 (accounting_date允許為空，因為有匯總分錄)
    null_counts = df_entry_long[['transaction_type', 'account_no', 'amount']].isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"⚠️  發現空值:\n{null_counts[null_counts > 0]}")
    else:
        logger.info("✓ 主要欄位無空值")

    # 檢查金額異常值
    if (df_entry_long['amount'].abs() > 1e12).any():
        logger.warning("⚠️  發現異常大的金額 (>1兆)")
        logger.warning(df_entry_long[df_entry_long['amount'].abs() > 1e12][['accounting_date', 'account_no', 'amount']])
    else:
        logger.info("✓ 金額範圍正常")

    logger.info("\n驗證完成！")
