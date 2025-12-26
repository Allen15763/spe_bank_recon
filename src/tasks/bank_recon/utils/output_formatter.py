"""
輸出格式化工具
"""

import pandas as pd
from typing import List, Dict, Any
from datetime import datetime


def create_summary_dataframe(containers_and_names: List[tuple]) -> pd.DataFrame:
    """
    創建銀行摘要的 DataFrame
    
    Args:
        containers_and_names: List of tuples (BankDataContainer, display_name)
        
    Returns:
        pd.DataFrame: 摘要 DataFrame
    """
    summary_data = []
    
    for container, bank_name in containers_and_names:
        # 基礎數據
        data = {
            '銀行': bank_name,
            '對帳_請款金額_當期': container.recon_amount,
            '對帳_請款金額_Trust_Account_Fee': container.recon_amount_for_trust_account_fee,
            '對帳_手續費_當期': container.recon_service_fee,
            '對帳_調整金額': container.adj_service_fee,
        }
        
        # 根據銀行類型添加特定欄位
        if 'cub' in bank_name.lower():
            data.update({
                '對帳_退貨金額': container.amount_claimed_last_period_paid_by_current,
                '對帳_手續費_前期': container.service_fee_claimed_last_period_paid_by_current,
                '發票_手續費': container.invoice_service_fee,
                '發票_請款金額': container.invoice_amount_claimed,
            })
            data['對帳_手續費_總計'] = (
                container.recon_service_fee +
                container.service_fee_claimed_last_period_paid_by_current
            )
        
        elif any(['ctbc' in bank_name.lower(), 'nccc' in bank_name.lower()]):
            data.update({
                '對帳_請款金額_前期發票當期撥款': container.amount_claimed_last_period_paid_by_current,
                '對帳_手續費_前期': container.service_fee_claimed_last_period_paid_by_current,
                '發票_請款金額': container.invoice_amount_claimed,
            })
            data['對帳_手續費_總計'] = (
                container.recon_service_fee +
                container.service_fee_claimed_last_period_paid_by_current
            )
            
            if 'nccc' in bank_name.lower():
                data['發票_手續費'] = container.invoice_service_fee
                data['對帳_手續費_總計'] = container.recon_service_fee
            else:
                data['發票_手續費'] = container.invoice_service_fee
        
        elif 'ub' in bank_name.lower():
            data.update({
                '對帳_請款金額_前期發票當期撥款': container.amount_claimed_last_period_paid_by_current,
                '對帳_手續費_前期': container.service_fee_claimed_last_period_paid_by_current,
            })
            data['對帳_手續費_總計'] = container.recon_service_fee - container.adj_service_fee
            data['對帳_手續費_當期'] = (
                container.recon_service_fee -
                container.service_fee_claimed_last_period_paid_by_current
            )
        
        elif 'taishi' in bank_name.lower():
            data.update({
                '對帳_稅額調整': container.service_fee_claimed_last_period_paid_by_current,
            })
            data['對帳_手續費_總計'] = container.recon_service_fee
        
        summary_data.append(data)
    
    df = pd.DataFrame(summary_data)
    
    # 重新排列欄位
    cols = ['銀行'] + [col for col in df.columns if col != '銀行']
    df = df[cols]
    
    return df


def format_excel_output(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
    sheet_name: str,
    freeze_panes: tuple = (1, 1),
    column_widths: Dict[str, int] = None
):
    """
    格式化 Excel 輸出
    
    Args:
        writer: ExcelWriter 物件
        df: DataFrame
        sheet_name: Sheet 名稱
        freeze_panes: 凍結窗格位置
        column_widths: 欄位寬度字典
    """
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # 設定凍結窗格
    if freeze_panes:
        worksheet.freeze_panes(*freeze_panes)
    
    # 設定欄寬
    if column_widths:
        for col_name, width in column_widths.items():
            col_idx = df.columns.get_loc(col_name)
            worksheet.set_column(col_idx, col_idx, width)


def reorder_bank_summary(df: pd.DataFrame, bank_order: List[str]) -> pd.DataFrame:
    """
    重新排序銀行摘要
    
    Args:
        df: DataFrame
        bank_order: 銀行順序列表
        
    Returns:
        pd.DataFrame: 排序後的 DataFrame
    """
    df_copy = df.copy()
    df_copy = df_copy.set_index('銀行')
    
    # 過濾出存在的銀行
    existing_banks = [bank for bank in bank_order if bank in df_copy.index]
    
    # 重新排序
    df_reordered = df_copy.reindex(existing_banks)

    return df_reordered.reset_index()


def add_timestamp_to_filename(filename: str, timestamp: datetime = None) -> str:
    """
    在檔名中添加時間戳
    
    Args:
        filename: 原始檔名
        timestamp: 時間戳，None 則使用當前時間
        
    Returns:
        str: 帶時間戳的檔名
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    
    # 分離檔名和副檔名
    if '.' in filename:
        name, ext = filename.rsplit('.', 1)
        return f"{name}_{timestamp_str}.{ext}"
    else:
        return f"{filename}_{timestamp_str}"


def format_number_columns(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """
    格式化數字欄位（千分位）
    
    Args:
        df: DataFrame
        columns: 要格式化的欄位列表，None 則格式化所有數字欄位
        
    Returns:
        pd.DataFrame: 格式化後的 DataFrame
    """
    df_copy = df.copy()
    
    if columns is None:
        columns = df_copy.select_dtypes(include=['number']).columns
    
    for col in columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(
                lambda x: f"{x:,}" if pd.notna(x) else "N/A"
            )
    
    return df_copy
