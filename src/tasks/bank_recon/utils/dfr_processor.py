"""
DFR (TW Bank Balance) 資料處理工具
處理銀行餘額 Excel 檔案的讀取、清理和計算
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

from src.utils import get_logger

logger = get_logger("dfr_processor")


def validate_dfr_columns(df: pd.DataFrame, 
                         inbound_validation_cols: List[str],
                         outbound_validation_cols: List[str]) -> Dict[str, bool]:
    """
    驗證 DFR 欄位是否符合預期
    
    Args:
        df: DFR DataFrame
        inbound_validation_cols: 預期的 Inbound 欄位清單
        outbound_validation_cols: 預期的 Outbound 欄位清單
        
    Returns:
        Dict[str, bool]: 驗證結果
    """
    result = {
        'inbound_valid': True,
        'outbound_valid': True,
        'inbound_missing': [],
        'outbound_missing': [],
    }
    
    # 檢查 Inbound 欄位
    for col in inbound_validation_cols:
        if col not in df.columns:
            result['inbound_valid'] = False
            result['inbound_missing'].append(col)
    
    # 檢查 Outbound 欄位
    for col in outbound_validation_cols:
        if col not in df.columns:
            result['outbound_valid'] = False
            result['outbound_missing'].append(col)
    
    if result['inbound_valid']:
        logger.info("DFR Inbound 欄位驗證通過")
    else:
        logger.warning(f"DFR Inbound 欄位缺失: {result['inbound_missing']}")
    
    if result['outbound_valid']:
        logger.info("DFR Outbound 欄位驗證通過")
    else:
        logger.warning(f"DFR Outbound 欄位缺失: {result['outbound_missing']}")
    
    return result


def get_column_range_indices(df: pd.DataFrame, 
                             start_col: str, 
                             end_col: str,
                             extra_cols: List[str] = None) -> List[int]:
    """
    取得欄位範圍的索引
    
    Args:
        df: DataFrame
        start_col: 起始欄位名稱
        end_col: 結束欄位名稱
        extra_cols: 額外的欄位名稱清單
        
    Returns:
        List[int]: 欄位索引清單
    """
    try:
        start_idx = df.columns.get_loc(start_col)
        end_idx = df.columns.get_loc(end_col) + 1
        indices = list(range(start_idx, end_idx))
        
        # 加入額外欄位
        if extra_cols:
            for col in extra_cols:
                if col in df.columns:
                    col_idx = df.columns.get_loc(col)
                    if col_idx not in indices:
                        indices.append(col_idx)
        
        return indices
        
    except KeyError as e:
        logger.error(f"欄位不存在: {e}")
        return []


def process_dfr_data(df: pd.DataFrame, 
                     beg_date: str, 
                     end_date: str,
                     columns_config: Dict[str, Any]) -> pd.DataFrame:
    """
    處理 DFR 資料，計算各項金額
    
    Args:
        df: DFR DataFrame
        beg_date: 開始日期
        end_date: 結束日期
        columns_config: 欄位配置
        
    Returns:
        pd.DataFrame: 處理後的 DataFrame
    """
    date_col = columns_config.get('date_col', 'Date')
    
    # 過濾日期範圍
    df_filtered = df.query(f"{date_col}.between(@beg_date, @end_date)").copy()
    
    if len(df_filtered) == 0:
        logger.warning(f"DFR 無資料在日期範圍內: {beg_date} ~ {end_date}")
        return pd.DataFrame()
    
    # 計算 Inbound
    inbound_start = columns_config.get('inbound_start_col', 'BT')
    inbound_end = columns_config.get('inbound_end_col', 'Collection Shop')
    inbound_extra = columns_config.get('inbound_extra_cols', ['offline transfer'])
    
    inbound_indices = get_column_range_indices(df, inbound_start, inbound_end, inbound_extra)
    if inbound_indices:
        inbound = df_filtered.iloc[:, inbound_indices].sum(axis=1)
    else:
        inbound = pd.Series([0] * len(df_filtered))
    
    # 計算 Unsuccessful ACH
    ach_start = columns_config.get('unsuccessful_ach_start_col', 'Unsuccessful\n(ACH)')
    ach_end = columns_config.get('unsuccessful_ach_end_col', 'Unsuccessful (ACH)\nCOD RR')
    ach_indices = get_column_range_indices(df, ach_start, ach_end)
    if ach_indices:
        unsuccessful_ach = df_filtered.iloc[:, ach_indices].sum(axis=1)
    else:
        unsuccessful_ach = pd.Series([0] * len(df_filtered))
    
    # 計算 Outbound
    outbound_cols = columns_config.get('outbound_cols', [])
    outbound_extra = columns_config.get('outbound_extra_cols', [])
    all_outbound_cols = outbound_cols + outbound_extra
    
    outbound_indices = []
    for col in all_outbound_cols:
        if col in df.columns:
            outbound_indices.append(df.columns.get_loc(col))
    
    if outbound_indices:
        outbound = df_filtered.iloc[:, outbound_indices].sum(axis=1)
    else:
        outbound = pd.Series([0] * len(df_filtered))
    
    # 取得其他特殊欄位
    def get_single_column_sum(col_name: str) -> pd.Series:
        if col_name in df.columns:
            col_idx = df.columns.get_loc(col_name)
            return df_filtered.iloc[:, col_idx:col_idx + 1].sum(axis=1)
        return pd.Series([0] * len(df_filtered))
    
    spl = get_single_column_sum(columns_config.get('spl_col', '企網自行轉帳\n20680100228850'))
    interest = get_single_column_sum(columns_config.get('interest_col', '存款息'))
    interbank = get_single_column_sum(columns_config.get('interbank_col', 'Interbank\ntransfer'))
    offline_transfer = get_single_column_sum(columns_config.get('offline_transfer', 'offline transfer'))

    adj = get_single_column_sum(columns_config.get('adj_col', '調撥'))
    # 信託戶手續費提領
    withdraw_service_fee = get_single_column_sum(columns_config.get('withdraw_service_fee_col', '手續費'))
    balance = get_single_column_sum(columns_config.get('balance_col', 'Balance.1'))
    
    # 建立結果 DataFrame
    df_result = pd.DataFrame({
        'Date': df_filtered[date_col].values,
        'Inbound': inbound.values,
        'Unsuccessful_ACH': unsuccessful_ach.values,
        'Outbound': outbound.values,
        'spl': spl.values,
        'interest': interest.values,
        'interbank': interbank.values,
        'offline_transfer': offline_transfer.values,
        'adj': adj.values,
        'withdraw_service_fee': withdraw_service_fee.values,
        'balance': balance.values,
    })
    
    logger.info(f"DFR 資料處理完成: {len(df_result)} 筆")
    return df_result


def create_dfr_wp(df_result_dfr: pd.DataFrame,
                  service_fee_col: str = 'withdraw_service_fee',
                  handing_fee_col: str = 'handing_fee',
                  remittance_fee_col: str = 'remittance_fee') -> pd.DataFrame:
    """
    建立 DFR 工作底稿格式 (4 欄格式)
    
    Args:
        df_result_dfr: 處理後的 DFR DataFrame
        withdraw_service_fee: 服務費欄位名稱
        handing_fee_col: 手續費欄位名稱
        remittance_fee_col: 匯費欄位名稱
        
    Returns:
        pd.DataFrame: 工作底稿格式 DataFrame
    """
    df_dfr_wp = pd.DataFrame({
        'Date': df_result_dfr['Date'],
        'remittance_fee': df_result_dfr.get(remittance_fee_col, 0),
        'Inbound': df_result_dfr['Inbound'].add(
            df_result_dfr.get(service_fee_col, 0), fill_value=0
        ).add(
            df_result_dfr.get('spl', 0), fill_value=0
        ),
        'Unsuccessful_ACH': df_result_dfr['Unsuccessful_ACH'],
        'Outbound': df_result_dfr['Outbound'],
        'handing_fee': df_result_dfr.get(handing_fee_col, 0),
    })
    
    # 加入 Total 行
    total_row = pd.DataFrame({
        'Date': ['Total'],
        'remittance_fee': [df_dfr_wp['remittance_fee'].sum()],
        'Inbound': [df_dfr_wp['Inbound'].sum()],
        'Unsuccessful_ACH': [df_dfr_wp['Unsuccessful_ACH'].sum()],
        'Outbound': [df_dfr_wp['Outbound'].sum()],
        'handing_fee': [df_dfr_wp['handing_fee'].sum()],
    })
    
    df_dfr_wp = pd.concat([df_dfr_wp, total_row], ignore_index=True)
    
    logger.info("DFR 工作底稿格式建立完成")
    return df_dfr_wp


def calculate_daily_movement(df_dfr: pd.DataFrame, 
                             inbound_col: str = 'Inbound',
                             outbound_col: str = 'Outbound',
                             unsuccessful_ach_col: str = 'Unsuccessful_ACH') -> pd.Series:
    """
    計算每日變動金額
    
    Args:
        df_dfr: DFR DataFrame
        inbound_col: 入款欄位
        outbound_col: 出款欄位
        unsuccessful_ach_col: 退匯欄位
        
    Returns:
        pd.Series: 每日變動金額
    """
    inbound = df_dfr.get(inbound_col, 0)
    outbound = df_dfr.get(outbound_col, 0)
    unsuccessful = df_dfr.get(unsuccessful_ach_col, 0)
    
    # # 每日變動 = Inbound - Outbound + Unsuccessful_ACH
    # daily_movement = inbound - outbound + unsuccessful

    # 取DFR Result的Inbound到Balance欄位前所有數字的加總
    daily_movement = df_dfr.iloc[
        :, df_dfr.columns.get_loc(inbound_col):df_dfr.columns.get_loc('balance')
    ].sum(axis=1)
    
    return daily_movement


def calculate_running_balance(df_dfr: pd.DataFrame,
                              beginning_balance: float,
                              daily_movement: pd.Series = None) -> pd.DataFrame:
    """
    計算累計餘額
    
    Args:
        df_dfr: DFR DataFrame
        beginning_balance: 期初餘額
        daily_movement: 每日變動金額 (若為 None 則自動計算)
        
    Returns:
        pd.DataFrame: 含累計餘額的 DataFrame
    """
    df_result = df_dfr.copy()
    
    if daily_movement is None:
        daily_movement = calculate_daily_movement(df_dfr)
    
    # 計算累計餘額
    df_result['daily_movement'] = daily_movement
    df_result['running_balance'] = beginning_balance + daily_movement.cumsum()
    
    logger.info(f"累計餘額計算完成: 期初 {beginning_balance:,.0f}, 期末 {df_result['running_balance'].iloc[-1]:,.0f}")
    return df_result
