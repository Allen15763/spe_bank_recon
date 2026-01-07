"""
APCC 手續費計算工具
計算各銀行收單手續費及 SPE 服務費
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

from src.utils import get_logger

logger = get_logger("apcc_calculator")


def reformat_df_wp(df: pd.DataFrame, is_claimed_only: bool = True) -> pd.DataFrame:
    """
    重新格式化工作底稿 DataFrame
    
    Args:
        df: 原始 DataFrame (通常是 trust_account_fee)
        is_claimed_only: 是否只取請款欄位
        
    Returns:
        pd.DataFrame: 重新格式化的 DataFrame
    """
    df_copy = df.copy()
    
    # 處理多層欄位索引
    if isinstance(df_copy.columns, pd.MultiIndex):
        idx = df_copy.columns.to_flat_index()
        df_copy.columns = ['_'.join([str(i[1]), str(i[0])]) for i in idx]
    
    df_copy = df_copy.fillna(0).astype('Float64')
    df_copy = df_copy.reset_index()
    
    if is_claimed_only:
        df_copy = df_copy.filter(regex='claimed|transaction_type')
    
    logger.info(f"工作底稿重新格式化完成: {len(df_copy)} 筆")
    return df_copy


def get_apcc_service_fee_charged(df: pd.DataFrame, 
                                 charge_rates: List[float]) -> pd.DataFrame:
    """
    計算 APCC 手續費
    
    Args:
        df: 請款資料 DataFrame
        charge_rates: 各交易類型的費率清單 [normal, 3期, 6期, 12期, 24期, 小計]
        
    Returns:
        pd.DataFrame: 含手續費計算的 DataFrame
    """
    df_copy = df.copy()
    
    # 計算各欄位小計
    numeric_cols = df_copy.select_dtypes(include='number').columns
    df_copy['subtotal'] = df_copy[numeric_cols].sum(axis=1)
    
    # 確保費率清單長度正確
    if len(charge_rates) < len(df_copy):
        charge_rates = charge_rates + [0] * (len(df_copy) - len(charge_rates))
    elif len(charge_rates) > len(df_copy):
        charge_rates = charge_rates[:len(df_copy)]
    
    df_copy['charge_rate'] = charge_rates
    
    # 計算手續費
    df_copy['commission_fee'] = df_copy['subtotal'] * df_copy['charge_rate']
    df_copy['commission_fee'] = df_copy['commission_fee'].round(0)
    # Subtotal
    df_copy.iloc[-1, df_copy.columns.get_loc('commission_fee')] = df_copy['commission_fee'].sum()
    
    logger.info("APCC 手續費計算完成")
    return df_copy


def apply_ops_adjustment(df: pd.DataFrame, 
                         ops_adj_amt: float,
                         normal_row_index: int = 0,
                         subtotal_row_index: int = -1,
                         adj_idx: int = 0) -> pd.DataFrame:
    """
    套用營運調整 (調扣加回)
    
    Args:
        df: DataFrame
        ops_adj_amt: 調整金額
        normal_row_index: normal 行的索引
        subtotal_row_index: 小計行的索引
        adj_idx: 預設0，調整台新的調扣。
            - 1: NCCC
            - 2: CUB
            - 3: CTBC
            - 4: UB
        
    Returns:
        pd.DataFrame: 調整後的 DataFrame
    """
    df_copy = df.copy()
    
    # 找到 claimed 相關欄位
    claimed_cols = [col for col in df_copy.columns if 'claimed' in col.lower()]
    
    if claimed_cols:
        first_claimed_col = claimed_cols[adj_idx]
        bank = df_copy.iloc[:, df_copy.columns.get_loc(first_claimed_col)].name.split('_')[0]

        logger.info(f"""
            \t\t\t調整調扣銀行: {bank}
            \t\t\t調整調扣前Normal: {df_copy.iloc[normal_row_index, df_copy.columns.get_loc(first_claimed_col)]:,.2f}
            \t\t\t調整調扣前3期: {df_copy.iloc[normal_row_index + 1, df_copy.columns.get_loc(first_claimed_col)]:,.2f}
            \t\t\t調整調扣前SubTotal: {df_copy.iloc[subtotal_row_index, df_copy.columns.get_loc(first_claimed_col)]:,.2f}
        """)
        
        if bank != 'NCCC':
            # 調整 normal 行
            df_copy.iloc[normal_row_index, df_copy.columns.get_loc(first_claimed_col)] += ops_adj_amt
        else:
            # 調整 3期 行
            df_copy.iloc[normal_row_index + 1, df_copy.columns.get_loc(first_claimed_col)] += ops_adj_amt
            
        # 調整小計行
        df_copy.iloc[subtotal_row_index, df_copy.columns.get_loc(first_claimed_col)] += ops_adj_amt

        logger.info(f"""
            \t\t\t調整調扣後Normal: {df_copy.iloc[normal_row_index, df_copy.columns.get_loc(first_claimed_col)]:,.2f}
            \t\t\t調整調扣前3期: {df_copy.iloc[normal_row_index + 1, df_copy.columns.get_loc(first_claimed_col)]:,.2f}
            \t\t\t調整調扣後SubTotal: {df_copy.iloc[subtotal_row_index, df_copy.columns.get_loc(first_claimed_col)]:,.2f}
        """)
    
    logger.info(f"已套用OPS調扣調整: {ops_adj_amt:,.0f}")
    return df_copy


def apply_rounding_adjustment(df: pd.DataFrame,
                              bank_name: str,
                              rounding_amount: float,
                              fee_column_index: int,
                              normal_row_index: int = 0,
                              subtotal_row_index: int = -1) -> pd.DataFrame:
    """
    套用手續費尾差調整
    
    Args:
        df: DataFrame
        bank_name: 銀行名稱（用於日誌）
        rounding_amount: 尾差金額
        fee_column_index: 手續費欄位索引
        normal_row_index: normal 行的索引
        subtotal_row_index: 小計行的索引
        
    Returns:
        pd.DataFrame: 調整後的 DataFrame
    """
    if rounding_amount == 0:
        return df
    
    df_copy = df.copy()
    logger.info(f"""
        \t\t\t調整Rounding前Normal: {df_copy.iloc[normal_row_index, fee_column_index]:,.2f}
        \t\t\t調整Rounding前SubTotal: {df_copy.iloc[subtotal_row_index, fee_column_index]:,.2f}
    """)
    
    # 調整 normal 行
    df_copy.iloc[normal_row_index, fee_column_index] += rounding_amount
    # 調整小計行
    df_copy.iloc[subtotal_row_index, fee_column_index] += rounding_amount
    
    logger.info(f"已套用 {bank_name} 手續費尾差調整: {rounding_amount:,.2f}")
    logger.info(f"""
        \t\t\t調整Rounding後Normal: {df_copy.iloc[normal_row_index, fee_column_index]:,.2f}
        \t\t\t調整Rounding後SubTotal: {df_copy.iloc[subtotal_row_index, fee_column_index]:,.2f}
    """)
    return df_copy


def calculate_trust_account_validation(df_trust_account_fee: pd.DataFrame,
                                       df_escrow_inv: pd.DataFrame) -> pd.DataFrame:
    """
    計算 Trust Account Fee 與 Escrow Invoice 的差異
    
    Args:
        df_trust_account_fee: Trust Account Fee DataFrame
        df_escrow_inv: Escrow Invoice DataFrame
        
    Returns:
        pd.DataFrame: 驗證結果
    """
    try:
        # 取得 Trust Account Fee 小計
        trust_fee = df_trust_account_fee.loc['小計', 'total_service_fee']
        
        # 取得 Escrow Invoice 手續費
        escrow_fee = df_escrow_inv.loc['小計', 'total_service_fee']
        
        # 計算差異
        validation = pd.DataFrame({
            'trust_account_fee的小計': trust_fee,
            'escrow_inv的手續費': escrow_fee,
            'diff': trust_fee - escrow_fee
        })
        
        logger.info("Trust Account 驗證計算完成")
        return validation
        
    except Exception as e:
        logger.warning(f"Trust Account 驗證計算失敗: {e}")
        return pd.DataFrame()


def validate_apcc_vs_frr(df_apcc: pd.DataFrame, 
                         df_frr_net_billing: pd.DataFrame) -> pd.DataFrame:
    """
    驗證 APCC 與 FRR 請款金額
    
    Args:
        df_apcc: APCC 手續費 DataFrame
        df_frr_net_billing: FRR 請款 DataFrame
        
    Returns:
        pd.DataFrame: 驗證結果
    """
    bank_mapping = {'台新': 'TSPG', '國泰': 'CUB', '聯邦': 'UBOT', 'CTBC': 'CTBC', 'NCCC': 'NCCC'}
    
    try:
        # 取得 APCC 小計
        apcc_subtotal = df_apcc.loc[df_apcc['transaction_type'] == '小計'].copy()
        apcc_subtotal = apcc_subtotal.reset_index(drop=True)
        
        # 整理 APCC 資料
        claimed_cols = [col for col in apcc_subtotal.columns if 'claimed' in col.lower()]
        apcc_data = []
        for col in claimed_cols:
            bank = col.split('_')[0]
            amount = apcc_subtotal[col].values[0] if len(apcc_subtotal) > 0 else 0
            apcc_data.append({'bank_wp': bank, 'subtotal_wp': amount})
        
        df_wp = pd.DataFrame(apcc_data)
        df_wp['bank_code'] = df_wp['bank_wp'].map(bank_mapping)
        
        # 取得 FRR 資料
        frr_subtotal = df_frr_net_billing.loc['Grand Total'].iloc[:-1].reset_index()
        frr_subtotal.columns = ['bank_frr', 'subtotal_frr']
        
        # 合併比對
        df_validate = df_wp.merge(frr_subtotal, left_on='bank_code', right_on='bank_frr', how='outer')
        df_validate['diff'] = df_validate['subtotal_wp'] - df_validate['subtotal_frr']
        
        # 轉換數值型態
        numeric_cols = df_validate.select_dtypes(include='number').columns
        for col in numeric_cols:
            df_validate[col] = df_validate[col].astype('Float64')
        
        logger.info("APCC vs FRR 驗證完成")
        return df_validate
        
    except Exception as e:
        logger.warning(f"APCC vs FRR 驗證失敗: {e}")
        return pd.DataFrame()


def get_spe_charge_with_tax(df_apcc: pd.DataFrame, tax_rate: float = 0.05) -> pd.DataFrame:
    """
    計算含稅的 SPE 服務費
    
    Args:
        df_apcc: APCC 手續費 DataFrame
        tax_rate: 稅率 (預設 5%)
        
    Returns:
        pd.DataFrame: 含稅服務費
    """
    df_spe_charge = df_apcc[['commission_fee', 'charge_rate']].copy()
    df_spe_charge['commission_fee'] = round(df_spe_charge['commission_fee'] * (1 + tax_rate), 0)
    df_spe_charge.columns = ['SPE Charge', 'charge_rate']
    
    logger.info(f"SPE 含稅服務費計算完成 (稅率 {tax_rate*100:.0f}%)")
    return df_spe_charge


def reformat_df_summary(df: pd.DataFrame, df_val: pd.DataFrame) -> pd.DataFrame:
    """
    重新格式化 Summary DataFrame
    
    Args:
        df: trust_account_fee_with_adj DataFrame
        df_val: trust_account_validation DataFrame
        
    Returns:
        pd.DataFrame: 格式化後的 Summary
    """
    df_copy = df.copy()
    types = ['3期', '6期', '12期', '24期']
    
    try:
        cub_subtotal = df_val.loc['國泰', 'escrow_inv的手續費']
        ubot_subtotal = df_val.loc['聯邦', 'escrow_inv的手續費']
        
        is_sub_total = df_copy['transaction_type'] == '小計'
        is_normal = df_copy['transaction_type'] == 'normal'
        is_installment = df_copy['transaction_type'].isin(types)
        
        # 設定小計
        df_copy.loc[is_sub_total, '國泰_total_service_fee'] = cub_subtotal
        df_copy.loc[is_sub_total, '聯邦_total_service_fee'] = ubot_subtotal
        
        # 計算 normal（小計減去分期）
        cub_installment_amt = df_copy.loc[is_installment, '國泰_total_service_fee'].sum()
        df_copy.loc[is_normal, '國泰_total_service_fee'] = cub_subtotal - cub_installment_amt
        
        ubot_installment_amt = df_copy.loc[is_installment, '聯邦_total_service_fee'].sum()
        df_copy.loc[is_normal, '聯邦_total_service_fee'] = ubot_subtotal - ubot_installment_amt
        
        logger.info("Summary 重新格式化完成")
        return df_copy
        
    except Exception as e:
        logger.warning(f"Summary 格式化失敗: {e}")
        return df


def transpose_df_summary(df: pd.DataFrame, end_date: str) -> pd.DataFrame:
    """
    將 Summary 從寬格式轉換為長格式
    
    Args:
        df: Summary DataFrame
        end_date: 期間結束日期
        
    Returns:
        pd.DataFrame: 長格式 Summary
    """
    df_copy = df.copy()
    
    df_copy = pd.melt(
        df_copy, 
        id_vars=['transaction_type'], 
        var_name='bank_and_amt_type', 
        value_name='amount'
    )
    
    # 處理欄位名稱
    df_copy['bank_and_amt_type'] = df_copy['bank_and_amt_type'].str.replace('_total', '')
    df_copy['bank_and_amt_type'] = df_copy['bank_and_amt_type'].str.replace('service', 'acquiring')
    
    # 分離銀行和金額類型
    df_copy['bank'] = df_copy['bank_and_amt_type'].str.split('_').str[0]
    df_copy['amt_type'] = df_copy['bank_and_amt_type'].str.split('_').str[1]
    
    # 加入期間
    df_copy['period'] = end_date
    
    logger.info("Summary 轉置為長格式完成")
    return df_copy


def transform_payment_data(df, end_date: str):
    """
    Transform long-format payment data into wide-format table with banks as columns.

    This function takes transaction data with separate rows for each bank/transaction type
    combination and pivots it into a wide table where:
    - Rows represent different transaction types (normal payments and installment periods)
    - Columns represent different banks with their claimed amounts and acquiring fees
    - Includes subtotal (小計), and rate (OCP), and percentage rows
    - Calculates total transaction amounts and acquiring fees

    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe with the following columns:
        - transaction_type: Type of transaction ('normal', '3期', '6期', '12期', '24期', '小計')
        - amount: Transaction amount
        - bank: Bank name ('台新', 'NCCC', '國泰', 'CTBC', '聯邦')
        - amt_type: Amount type ('claimed' or 'acquiring')
        - period: Transaction period (e.g., '2025-11-30')
        - charge_rate: Charge rate (optional, only for normal transactions)

    Returns:
    --------
    pandas.DataFrame
        Wide-format table with:
        - Index: Transaction types
        - Columns: Bank amounts, acquiring fees, totals, and calculated rates

    Example:
    --------
    >>> import pandas as pd
    >>> df = pd.read_csv('payment_data.csv')
    >>> result_df = transform_payment_data(df)
    >>> result_df.to_excel('payment_report.xlsx')
    """
    # Filter df with end_date
    df = df.loc[df.period == end_date, :].copy().reset_index(drop=True)

    # Define transaction type order and display names
    TRANSACTION_ORDER = ['normal', '3期', '6期', '12期', '24期', '小計']
    TRANSACTION_DISPLAY = {
        'normal': 'Non-installment-Payment',
        '3期': 'Installment-Payment-3M',
        '6期': 'Installment-Payment-6M',
        '12期': 'Installment-Payment-12M',
        '24期': 'Installment-Payment-24M',
        '小計': '小計'
    }

    # Define bank order and display names
    BANKS = ['台新', 'NCCC', '國泰', 'CTBC', '聯邦']
    BANK_DISPLAY = {
        '台新': 'TSB',  # Special name for first bank
        'NCCC': 'NCCC',
        '國泰': 'Cathay',
        'CTBC': 'CTBC',
        '聯邦': 'UBOT'
    }

    # Filter out subtotal rows (will be calculated)
    df_working = df[df['transaction_type'] != '小計'].copy()

    # Build result data row by row
    result_data = []

    for trans_type in TRANSACTION_ORDER[:-1]:  # Exclude '小計'
        row = {'transaction_type': TRANSACTION_DISPLAY[trans_type]}

        for bank in BANKS:
            # Extract claimed amount
            claimed_filter = (
                (df_working['transaction_type'] == trans_type) &
                (df_working['bank'] == bank) &
                (df_working['amt_type'] == 'claimed')
            )
            claimed_amt = df_working[claimed_filter]['amount'].values
            claimed_amt = claimed_amt[0] if len(claimed_amt) > 0 else 0

            # Extract acquiring amount
            acquiring_filter = (
                (df_working['transaction_type'] == trans_type) &
                (df_working['bank'] == bank) &
                (df_working['amt_type'] == 'acquiring')
            )
            acquiring_amt = df_working[acquiring_filter]['amount'].values
            acquiring_amt = acquiring_amt[0] if len(acquiring_amt) > 0 else 0

            # Add to row with appropriate column names
            if bank == '台新':
                row['TSB'] = claimed_amt
                row['TSB Acquiring fee'] = acquiring_amt
            elif bank == '國泰':
                row['Cathay'] = claimed_amt
                row['Cathay Acquiring fee'] = acquiring_amt
                row['Rebate from Cathay'] = 0  # Placeholder for special column
            else:
                display_name = BANK_DISPLAY[bank]
                row[display_name] = claimed_amt
                row[f'{display_name} Acquiring fee'] = acquiring_amt

        result_data.append(row)

    # Create DataFrame from collected data
    result_df = pd.DataFrame(result_data)

    # Calculate subtotal row (小計)
    subtotal = {'transaction_type': '小計'}
    for col in result_df.columns:
        if col not in ['transaction_type', 'Rebate from Cathay']:
            subtotal[col] = result_df[col].sum()
        elif col == 'Rebate from Cathay':
            subtotal[col] = 0

    result_df = pd.concat([result_df, pd.DataFrame([subtotal])], ignore_index=True)

    # Calculate total transaction column (sum of all claimed amounts)
    claimed_columns = ['TSB', 'NCCC', 'Cathay', 'CTBC', 'UBOT']
    result_df['Total Transaction'] = result_df[claimed_columns].sum(axis=1)

    # Calculate total acquiring fee column (sum of all acquiring fees)
    acquiring_columns = [
        'TSB Acquiring fee', 'NCCC Acquiring fee', 'Cathay Acquiring fee',
        'CTBC Acquiring fee', 'UBOT Acquiring fee'
    ]
    result_df['Total Acquiring Fee'] = result_df[acquiring_columns].sum(axis=1)

    # Calculate additional metrics
    result_df['rate_charge'] = (
        result_df['Total Acquiring Fee'] / result_df['Total Transaction']
    )

    # Add OCP row with charge rates
    ocp_row = {'transaction_type': 'OCP'}

    # Calculate charge rates for each bank
    for bank in BANKS:
        # Get original charge rate from data
        rate_filter = (
            (df['bank'] == bank) &
            (df['amt_type'] == 'acquiring') &
            (df['charge_rate'].notna())
        )
        charge_rate_data = df[rate_filter]['charge_rate'].values
        charge_rate = charge_rate_data[0] if len(charge_rate_data) > 0 else np.nan

        # Calculate actual acquiring rate from subtotal row
        if bank == '台新':
            ocp_row['TSB Acquiring fee'] = charge_rate
            subtotal_claimed = result_df.loc[5, 'TSB']
            subtotal_acquiring = result_df.loc[5, 'TSB Acquiring fee']
        elif bank == '國泰':
            ocp_row['Cathay Acquiring fee'] = charge_rate
            subtotal_claimed = result_df.loc[5, 'Cathay']
            subtotal_acquiring = result_df.loc[5, 'Cathay Acquiring fee']
            ocp_row['Rebate from Cathay'] = np.nan
        else:
            display_name = BANK_DISPLAY[bank]
            ocp_row[f'{display_name} Acquiring fee'] = charge_rate
            subtotal_claimed = result_df.loc[5, display_name]
            subtotal_acquiring = result_df.loc[5, f'{display_name} Acquiring fee']

    # Calculate overall acquiring rate
    total_claimed = result_df.loc[5, 'Total Transaction']
    total_acquiring = result_df.loc[5, 'Total Acquiring Fee']
    ocp_row['Total Transaction'] = np.nan
    ocp_row['Total Acquiring Fee'] = (
        total_acquiring / total_claimed if total_claimed > 0 else np.nan
    )
    ocp_row['rate_charge'] = np.nan

    # Append OCP row
    result_df = pd.concat([result_df, pd.DataFrame([ocp_row])], ignore_index=True)

    # Add Percentage row (Bank Subtotal / Total Transaction)
    percentage_row = {'transaction_type': 'Percentage'}

    # Get total transaction from subtotal row
    total_transaction = result_df.loc[5, 'Total Transaction']

    # Calculate percentage for each bank
    for bank in BANKS:
        if bank == '台新':
            bank_subtotal = result_df.loc[5, 'TSB']
            percentage_row['TSB'] = (bank_subtotal / total_transaction) if total_transaction > 0 else np.nan
            percentage_row['TSB Acquiring fee'] = np.nan
        elif bank == '國泰':
            bank_subtotal = result_df.loc[5, 'Cathay']
            percentage_row['Cathay'] = (bank_subtotal / total_transaction) if total_transaction > 0 else np.nan
            percentage_row['Cathay Acquiring fee'] = np.nan
            percentage_row['Rebate from Cathay'] = np.nan
        else:
            display_name = BANK_DISPLAY[bank]
            bank_subtotal = result_df.loc[5, display_name]
            percentage_row[display_name] = (bank_subtotal / total_transaction) if total_transaction > 0 else np.nan
            percentage_row[f'{display_name} Acquiring fee'] = np.nan

    # Set other columns to NaN for percentage row
    percentage_row['Total Transaction'] = np.nan
    percentage_row['Total Acquiring Fee'] = np.nan
    percentage_row['rate_charge'] = np.nan

    # Append Percentage row
    result_df = pd.concat([result_df, pd.DataFrame([percentage_row])], ignore_index=True)

    # Set transaction_type as index
    result_df = result_df.set_index('transaction_type')
    result_df.index.name = None

    return result_df

def calculate_charge_rate(df):
    """
    Calculate charge rate (acquiring/claimed) for normal transactions by bank.

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with columns: transaction_type, amount, bank, amt_type

    Returns:
    --------
    pandas.DataFrame
        Original DataFrame with a new 'charge_rate' column added.
        The charge_rate is only populated for rows where transaction_type is 'normal'.

    Example:
    --------
    >>> df_with_rate = calculate_charge_rate(df)
    """
    # Make a copy to avoid modifying the original
    df = df.copy()

    # Filter for normal transactions only
    df_normal = df[df['transaction_type'] == 'normal'].copy()

    # Pivot to get acquiring and claimed side by side
    df_pivot = df_normal.pivot_table(
        index=['bank'],
        columns='amt_type',
        values='amount',
        aggfunc='first'
    ).reset_index()

    # Calculate charge rate
    df_pivot['charge_rate'] = df_pivot['acquiring'] / df_pivot['claimed']

    # Create a mapping dictionary for charge rates
    charge_rate_map = df_pivot.set_index('bank')['charge_rate'].to_dict()

    # Add charge_rate column to original dataframe
    # Only populate for normal transactions
    df['charge_rate'] = df.apply(
        lambda row: round(charge_rate_map.get(row['bank'], pd.NA), 4)
        if (row['transaction_type'] == 'normal') and (row['amt_type'] == 'acquiring') else pd.NA,
        axis=1
    )

    return df


def get_df_cc_rev(df_acquiring, df_commission) -> pd.DataFrame:
    """
    計算cc_net_revenue

    從 Google Sheets 讀取
        - df_acquiring: acquiring_charge_raw
        - df_commission: APCC 手續費

    Returns:
        pd.DataFrame: 包含 period 和 cc_net_revenue 兩欄的 DataFrame
    """
    # 讀取原始資料

    # 處理收單手續費資料
    df_acquiring_processed = (
        df_acquiring
        .loc[df_acquiring.amt_type == 'acquiring', :]
        .groupby(['period', 'transaction_type'], as_index=False)
        .agg({'amount': 'sum'})
        .assign(period=lambda x: x['period'].str[:7])
    )

    # 處理特約商店手續費資料
    df_commission_processed = (
        df_commission[['transaction_type', 'charge_rate', 'commission_fee', 'end_date']]
        .assign(
            period=lambda x: x['end_date'].str[:7],
            commission_fee_inc_tax=lambda x: round(x['commission_fee'] * 1.05, 0)
        )
        .drop(['end_date', 'commission_fee'], axis=1)
    )

    # 合併資料並計算淨收入
    df_result = (
        pd.merge(
            df_acquiring_processed,
            df_commission_processed,
            on=['period', 'transaction_type'],
            how='inner'
        )
        .assign(
            cc_net_revenue=lambda x: np.where(
                x['transaction_type'] == '小計',
                x['commission_fee_inc_tax'] - x['amount'] - 150000,
                0
            )
        )
        .loc[lambda x: x['transaction_type'] == '小計', ['period', 'cc_net_revenue']]
    )

    return df_result


def calculate_spe_transaction_percentage(df):
    """
    Calculate percentage for each transaction type based on '小計' (subtotal) within each end_date block.

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame containing the transaction data

    Returns:
    --------
    pd.DataFrame
        Processed dataframe with percentage column added
    """
    # Create a copy to avoid modifying the original
    result_df = df.copy()

    # Create a new column for percentage
    result_df['percentage'] = 0.0

    # Group by end_date
    for end_date in result_df['end_date'].unique():
        # Get all rows for this end_date
        date_mask = result_df['end_date'] == end_date

        # Find the 小計 row for this end_date
        subtotal_mask = (result_df['end_date'] == end_date) & (result_df['transaction_type'] == '小計')

        if subtotal_mask.any():
            # Get the subtotal commission_fee (denominator)
            subtotal_commission = result_df.loc[subtotal_mask, 'commission_fee'].values[0]

            # Calculate percentage for all rows in this end_date block
            result_df.loc[date_mask, 'percentage'] = (
                result_df.loc[date_mask, 'commission_fee'] / subtotal_commission  # * 100
            ).round(4)

    # Select only required columns
    result_df = result_df.assign(
        commission_fee_inc_tax=lambda x: round(x['commission_fee'] * 1.05, 0)
    )
    result = result_df[
        [
            'transaction_type', 
            'commission_fee', 
            'commission_fee_inc_tax', 
            'charge_rate', 
            'end_date', 
            'percentage'
        ]].copy()
    return result

def calculate_transaction_percentage(data):
    """
    Calculate percentage for each transaction type based on acquiring/claimed ratio.

    Parameters:
    -----------
    data : pd.DataFrame or str
        Input data as DataFrame or CSV string/file path

    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: transaction_type, amt_type, amount, period, percentage
    """

    # Load data if it's a string (CSV content or file path)
    if isinstance(data, str):
        from io import StringIO
        try:
            # Try as CSV content first
            df = pd.read_csv(StringIO(data))
        except Exception as err:
            # Try as file path
            df = pd.read_csv(data)
    else:
        df = data.copy()

    # Group by period and transaction_type, sum amounts for each amt_type
    grouped = df.groupby(['period', 'transaction_type', 'amt_type'])['amount'].sum().reset_index()

    # Pivot to get claimed and acquiring as separate columns
    pivot = grouped.pivot_table(
        index=['period', 'transaction_type'],
        columns='amt_type',
        values='amount',
        fill_value=0
    ).reset_index()

    # Calculate percentage: (acquiring / claimed)
    # Handle division by zero
    pivot['percentage'] = pivot.apply(
        lambda row: (row['acquiring'] / row['claimed']) if row['claimed'] != 0 else 0,
        axis=1
    )

    # Create result dataframe with claimed rows
    result_claimed = pivot[['transaction_type', 'period', 'claimed', 'percentage']].copy()
    result_claimed['amt_type'] = 'claimed'
    result_claimed.rename(columns={'claimed': 'amount'}, inplace=True)

    # Create result dataframe with acquiring rows
    result_acquiring = pivot[['transaction_type', 'period', 'acquiring', 'percentage']].copy()
    result_acquiring['amt_type'] = 'acquiring'
    result_acquiring.rename(columns={'acquiring': 'amount'}, inplace=True)

    # Combine both
    result = pd.concat([result_claimed, result_acquiring], ignore_index=True)

    # Reorder columns as requested
    result = result[['transaction_type', 'amt_type', 'amount', 'period', 'percentage']]

    # Sort by period and transaction_type for better readability
    # Define custom order for transaction_type
    transaction_order = ['normal', '3期', '6期', '12期', '24期', '小計']
    result['sort_key'] = result['transaction_type'].apply(
        lambda x: transaction_order.index(x) if x in transaction_order else 999
    )
    result = result.sort_values(['period', 'sort_key', 'amt_type'],
                                ascending=[False, True, False])
    result = result.drop('sort_key', axis=1)

    # Round percentage to 2 decimal places
    result['percentage'] = result['percentage'].round(4)

    return result.reset_index(drop=True)


def convert_flatIndex_to_multiIndex(df):
    """
    將扁平的欄位名稱轉換為 MultiIndex columns
    
    Parameters:
    -----------
    df : pd.DataFrame
        輸入的 DataFrame，欄位格式為 '銀行名稱_total_claimed' 或 '銀行名稱_total_service_fee'
        且第一欄為 'transaction_type'
    
    Returns:
    --------
    pd.DataFrame
        具有 MultiIndex columns 的 DataFrame
        - Level 0: metric (total_claimed, total_service_fee)
        - Level 1: bank (銀行名稱)，names=['bank']
        - Index: transaction_type
    
    Examples:
    ---------
    >>> df = 
    >>> df_multi = convert_flatIndex_to_multiIndex(df)
    >>> 
    >>> # 存取特定銀行的資料
    >>> df_multi[('total_claimed', '台新')]
    >>> 
    >>> # 選取所有 total_claimed 欄位
    >>> df_multi['total_claimed']
    >>> 
    >>> # 選取特定交易類型
    >>> df_multi.loc['normal']
    """
    # 複製 DataFrame 避免修改原始資料
    df = df.copy()
    
    # 設定 transaction_type 為 index
    df = df.set_index('transaction_type')
    
    # 建立 MultiIndex
    multi_columns = []
    
    for col in df.columns:
        # 解析欄位名稱
        if '_total_claimed' in col:
            metric = 'total_claimed'
            bank = col.replace('_total_claimed', '')
        elif '_total_service_fee' in col:
            metric = 'total_service_fee'
            bank = col.replace('_total_service_fee', '')
        else:
            # 如果格式不符合，保留原樣
            metric = col
            bank = ''
        
        multi_columns.append((metric, bank))
    
    # 建立 MultiIndex
    df.columns = pd.MultiIndex.from_tuples(
        multi_columns,
        names=[None, 'bank']
    )
    
    # 將 index 名稱設定為 'transaction_type'
    df.index.name = 'transaction_type'
    
    return df
