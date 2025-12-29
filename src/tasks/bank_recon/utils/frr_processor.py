"""
FRR (財務部) 資料處理工具
處理財務部 Excel 檔案的讀取、清理和轉換
"""

from typing import Dict, Any
import pandas as pd
import numpy as np

from src.utils import get_logger

logger = get_logger("frr_processor")


def quick_clean_financial_data(df: pd.DataFrame, columns_config: Dict[str, Any]) -> pd.DataFrame:
    """
    快速清理財務資料
    
    Args:
        df: 原始 DataFrame
        columns_config: 欄位配置
        
    Returns:
        pd.DataFrame: 清理後的 DataFrame
    """
    df_clean = df.copy()
    
    # 建立新欄位名稱
    new_columns = [columns_config.get('date_col', 'Date')]
    
    # TSPG (4 columns)
    new_columns.extend(columns_config.get('tspg_cols', [
        'TSPG_Net_Billing', 'TSPG_Handling_Fee', 'TSPG_Adjustment', 'TSPG_Net_Disbursement'
    ]))
    
    # CTBC (4 columns)
    new_columns.extend(columns_config.get('ctbc_cols', [
        'CTBC_Net_Billing', 'CTBC_Handling_Fee', 'CTBC_Adjustment', 'CTBC_Net_Disbursement'
    ]))
    
    # NCCC (4 columns)
    new_columns.extend(columns_config.get('nccc_cols', [
        'NCCC_Net_Billing', 'NCCC_Handling_Fee', 'NCCC_Adjustment', 'NCCC_Net_Disbursement'
    ]))
    
    # CUB (5 columns)
    new_columns.extend(columns_config.get('cub_cols', [
        'CUB_Net_Billing', 'CUB_Handling_Fee', 'CUB_Adjustment', 'CUB_Remittance_Fee', 'CUB_Net_Disbursement'
    ]))
    
    # UBOT (5 columns)
    new_columns.extend(columns_config.get('ubot_cols', [
        'UBOT_Net_Billing', 'UBOT_Handling_Fee', 'UBOT_Remittance_Fee', 'UBOT_Adjustment', 'UBOT_Net_Disbursement'
    ]))
    
    # 套用新欄位名稱
    if len(new_columns) <= len(df_clean.columns):
        df_clean.columns = new_columns + list(df_clean.columns[len(new_columns):])
    else:
        logger.warning(f"欄位數量不符: 預期 {len(new_columns)}, 實際 {len(df_clean.columns)}")
        df_clean.columns = new_columns[:len(df_clean.columns)]
    
    # 移除空白行
    df_clean = df_clean.dropna(subset=['Date'], how='all')
    
    # 轉換日期欄位
    if 'Date' in df_clean.columns:
        df_clean['Date'] = pd.to_datetime(df_clean['Date'], errors='coerce')
    
    # 轉換數值欄位
    numeric_cols = [col for col in df_clean.columns if col != 'Date']
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    
    # 移除Date空值(總計或原始底稿預留空列)
    df_clean = df_clean.dropna(subset=['Date'], how='all')
    logger.info(f"FRR 資料清理完成: {len(df_clean)} 筆")
    return df_clean


def create_complete_date_range(df: pd.DataFrame, beg_date: str, end_date: str) -> pd.DataFrame:
    """
    建立完整的日期範圍，填補缺失日期
    
    Args:
        df: 原始 DataFrame
        beg_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
        
    Returns:
        pd.DataFrame: 完整日期範圍的 DataFrame
    """
    # 建立完整日期範圍
    full_date_range = pd.date_range(start=beg_date, end=end_date, freq='D')
    df_full = pd.DataFrame({'Date': full_date_range})
    
    # 確保原始資料的 Date 欄位為 datetime
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    
    # 合併資料
    df_merged = df_full.merge(df, on='Date', how='left')
    
    # 填補缺失值為 0
    numeric_cols = df_merged.select_dtypes(include=[np.number]).columns
    df_merged[numeric_cols] = df_merged[numeric_cols].fillna(0)

    # 把Date欄位從datatime轉回date
    df_merged['Date'] = pd.to_datetime(df_merged['Date']).dt.date
    
    logger.info(f"日期範圍補齊完成: {beg_date} ~ {end_date}, 共 {len(df_merged)} 天/筆")
    return df_merged


def convert_to_long_format(df: pd.DataFrame, bank_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    將寬格式轉換為長格式
    
    Args:
        df: 寬格式 DataFrame
        bank_mapping: 銀行代碼對應名稱 {'TSPG': '台新', 'CTBC': 'CTBC', ...}
        
    Returns:
        pd.DataFrame: 長格式 DataFrame
    """
    records = []
    
    for _, row in df.iterrows():
        date = row['Date']
        
        for bank_code, bank_name in bank_mapping.items():
            # 取得各欄位值；該銀行沒有那個欄位則補0
            net_billing = row.get(f'{bank_code}_Net_Billing', 0)
            handling_fee = row.get(f'{bank_code}_Handling_Fee', 0)
            adjustment = row.get(f'{bank_code}_Adjustment', 0)
            net_disbursement = row.get(f'{bank_code}_Net_Disbursement', 0)
            remittance_fee = row.get(f'{bank_code}_Remittance_Fee', 0)
            
            records.append({
                'Date': date,
                'Bank': bank_code,
                'Bank_Name': bank_name,
                'Net_Billing': net_billing,
                'Handling_Fee': handling_fee,
                'Adjustment': adjustment,
                'Remittance_Fee': remittance_fee,
                'Net_Disbursement': net_disbursement
            })
    
    df_long = pd.DataFrame(records)
    logger.info(f"轉換為長格式: {len(df_long)} 筆")
    return df_long


def calculate_frr_handling_fee(long_format_df: pd.DataFrame, beg_date: str, end_date: str) -> pd.DataFrame:
    """
    計算 FRR 手續費 pivot table
    
    Args:
        long_format_df: 長格式 DataFrame
        beg_date: 開始日期
        end_date: 結束日期
        
    Returns:
        pd.DataFrame: 手續費 pivot table
    """
    # 確保日期範圍完整
    long_format_df = create_complete_date_range(long_format_df, beg_date, end_date)
    
    df_pivot = long_format_df.pivot_table(
        index='Date',
        columns='Bank',
        values='Handling_Fee',
        aggfunc='sum',
        fill_value=0,
        margins=True,
        margins_name='Grand Total'
    ).map(lambda x: abs(x))
    
    logger.info("FRR 手續費計算完成")
    return df_pivot


def calculate_frr_remittance_fee(long_format_df: pd.DataFrame, beg_date: str, end_date: str) -> pd.DataFrame:
    """
    計算 FRR 匯費 pivot table
    
    Args:
        long_format_df: 長格式 DataFrame
        beg_date: 開始日期
        end_date: 結束日期
        
    Returns:
        pd.DataFrame: 匯費 pivot table
    """
    long_format_df = create_complete_date_range(long_format_df, beg_date, end_date)
    
    df_pivot = long_format_df.pivot_table(
        index='Date',
        columns='Bank',
        values='Remittance_Fee',
        aggfunc='sum',
        fill_value=0,
        margins=True,
        margins_name='Grand Total'
    )
    
    logger.info("FRR 匯費計算完成")
    return df_pivot


def calculate_frr_net_billing(long_format_df: pd.DataFrame, beg_date: str, end_date: str) -> pd.DataFrame:
    """
    計算 FRR 請款 pivot table
    
    Args:
        long_format_df: 長格式 DataFrame
        beg_date: 開始日期
        end_date: 結束日期
        
    Returns:
        pd.DataFrame: 請款 pivot table
    """
    long_format_df = create_complete_date_range(long_format_df, beg_date, end_date)
    
    df_pivot = long_format_df.pivot_table(
        index='Date',
        columns='Bank',
        values='Net_Billing',
        aggfunc='sum',
        fill_value=0,
        margins=True,
        margins_name='Grand Total'
    )
    
    logger.info("FRR 請款計算完成")
    return df_pivot


def validate_frr_handling_fee(df_frr_handling_fee: pd.DataFrame, 
                              df_summary_escrow_inv: pd.DataFrame) -> pd.DataFrame:
    """
    驗證 FRR 手續費與 Escrow Invoice 是否一致
    
    Args:
        df_frr_handling_fee: FRR 手續費 pivot table
        df_summary_escrow_inv: Escrow Invoice 摘要
        
    Returns:
        pd.DataFrame: 驗證結果
    """
    # 銀行名稱映射
    bank_mapping = {'台新': 'TSPG', '國泰': 'CUB', '聯邦': 'UBOT', 'CTBC': 'CTBC', 'NCCC': 'NCCC'}
    
    try:
        # 取得 Escrow Invoice 手續費
        escrow_fees = df_summary_escrow_inv.loc['小計'].loc['total_service_fee'].reset_index()
        escrow_fees.columns = ['bank', 'escrow_fee']
        escrow_fees['bank_code'] = escrow_fees['bank'].map(bank_mapping)
        
        # 取得 FRR 手續費
        frr_fees = df_frr_handling_fee.loc['Grand Total'].iloc[:-1].reset_index()
        frr_fees.columns = ['Bank', 'frr_fee']
        
        # 合併比對
        df_validate = escrow_fees.merge(frr_fees, left_on='bank_code', right_on='Bank', how='outer')
        df_validate['diff'] = df_validate['escrow_fee'] - df_validate['frr_fee']
        
        logger.info("FRR 手續費驗證完成")
        return df_validate
        
    except Exception as e:
        logger.warning(f"FRR 手續費驗證失敗: {e}")
        return pd.DataFrame()


def validate_frr_net_billing(df_frr_net_billing: pd.DataFrame, 
                             df_summary_escrow_inv: pd.DataFrame) -> pd.DataFrame:
    """
    驗證 FRR 請款與 Escrow Invoice 是否一致
    
    Args:
        df_frr_net_billing: FRR 請款 pivot table
        df_summary_escrow_inv: Escrow Invoice 摘要
        
    Returns:
        pd.DataFrame: 驗證結果
    """
    bank_mapping = {'台新': 'TSPG', '國泰': 'CUB', '聯邦': 'UBOT', 'CTBC': 'CTBC', 'NCCC': 'NCCC'}
    
    try:
        # 取得 Escrow Invoice 請款
        escrow_billing = df_summary_escrow_inv.loc['小計'].loc['total_claimed'].reset_index()
        escrow_billing.columns = ['bank', 'escrow_billing']
        escrow_billing['bank_code'] = escrow_billing['bank'].map(bank_mapping)
        
        # 取得 FRR 請款
        frr_billing = df_frr_net_billing.loc['Grand Total'].iloc[:-1].reset_index()
        frr_billing.columns = ['Bank', 'frr_billing']
        
        # 合併比對
        df_validate = escrow_billing.merge(frr_billing, left_on='bank_code', right_on='Bank', how='outer')
        df_validate['diff'] = df_validate['escrow_billing'] - df_validate['frr_billing']
        
        # 轉換數值型態
        numeric_cols = df_validate.select_dtypes(include='number').columns
        for col in numeric_cols:
            df_validate[col] = df_validate[col].astype('Float64')
        
        logger.info("FRR 請款驗證完成")
        return df_validate
        
    except Exception as e:
        logger.warning(f"FRR 請款驗證失敗: {e}")
        return pd.DataFrame()
