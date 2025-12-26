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
    
    logger.info("APCC 手續費計算完成")
    return df_copy


def apply_ops_adjustment(df: pd.DataFrame, 
                         ops_adj_amt: float,
                         normal_row_index: int = 0,
                         subtotal_row_index: int = -1) -> pd.DataFrame:
    """
    套用營運調整 (調扣加回)
    
    Args:
        df: DataFrame
        ops_adj_amt: 調整金額
        normal_row_index: normal 行的索引
        subtotal_row_index: 小計行的索引
        
    Returns:
        pd.DataFrame: 調整後的 DataFrame
    """
    df_copy = df.copy()
    
    # 找到 claimed 相關欄位
    claimed_cols = [col for col in df_copy.columns if 'claimed' in col.lower()]
    
    if claimed_cols:
        first_claimed_col = claimed_cols[0]
        # 調整 normal 行
        df_copy.iloc[normal_row_index, df_copy.columns.get_loc(first_claimed_col)] += ops_adj_amt
        # 調整小計行
        df_copy.iloc[subtotal_row_index, df_copy.columns.get_loc(first_claimed_col)] += ops_adj_amt
    
    logger.info(f"已套用營運調整: {ops_adj_amt:,.0f}")
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
    
    # 調整 normal 行
    df_copy.iloc[normal_row_index, fee_column_index] += rounding_amount
    # 調整小計行
    df_copy.iloc[subtotal_row_index, fee_column_index] += rounding_amount
    
    logger.info(f"已套用 {bank_name} 手續費尾差調整: {rounding_amount:,.2f}")
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


def transform_payment_data(df: pd.DataFrame, end_date: str) -> pd.DataFrame:
    """
    轉換支付資料格式（用於顯示）
    
    Args:
        df: 長格式 Summary DataFrame
        end_date: 期間結束日期
        
    Returns:
        pd.DataFrame: 轉換後的顯示格式
    """
    # 重新 pivot 為顯示格式
    df_display = df.pivot_table(
        index='transaction_type',
        columns=['bank', 'amt_type'],
        values='amount',
        aggfunc='sum'
    ).reset_index()
    
    logger.info("支付資料轉換完成")
    return df_display
