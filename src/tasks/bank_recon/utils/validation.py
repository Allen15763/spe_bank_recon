"""
驗證工具模組
"""

import pandas as pd
from typing import Optional, Tuple
from src.utils import get_logger

logger = get_logger("validation")


def validate_amount(
    amount: float,
    tolerance: float = 1.0,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None
) -> Tuple[bool, str]:
    """
    驗證金額
    
    Args:
        amount: 待驗證金額
        tolerance: 容差
        min_value: 最小值
        max_value: 最大值
        
    Returns:
        Tuple[bool, str]: (是否通過, 訊息)
    """
    if pd.isna(amount):
        return False, "金額為空值"
    
    if min_value is not None and amount < min_value:
        return False, f"金額 {amount:,} 小於最小值 {min_value:,}"
    
    if max_value is not None and amount > max_value:
        return False, f"金額 {amount:,} 大於最大值 {max_value:,}"
    
    return True, "驗證通過"


def compare_amounts(
    amount1: float,
    amount2: float,
    tolerance: float = 1.0,
    label1: str = "金額1",
    label2: str = "金額2"
) -> Tuple[bool, str, float]:
    """
    比對兩個金額
    
    Args:
        amount1: 金額1
        amount2: 金額2
        tolerance: 容差
        label1: 金額1標籤
        label2: 金額2標籤
        
    Returns:
        Tuple[bool, str, float]: (是否一致, 訊息, 差異)
    """
    diff = abs(amount1 - amount2)
    
    if diff <= tolerance:
        return True, f"{label1} 與 {label2} 一致", diff
    else:
        message = f"{label1}({amount1:,}) 與 {label2}({amount2:,}) 差異: {diff:,}"
        return False, message, diff


def validate_dataframe(
    df: pd.DataFrame,
    required_columns: list,
    min_rows: int = 1
) -> Tuple[bool, str]:
    """
    驗證 DataFrame
    
    Args:
        df: 待驗證的 DataFrame
        required_columns: 必要欄位列表
        min_rows: 最小行數
        
    Returns:
        Tuple[bool, str]: (是否通過, 訊息)
    """
    if df is None:
        return False, "DataFrame 為 None"
    
    if df.empty:
        return False, "DataFrame 為空"
    
    if len(df) < min_rows:
        return False, f"行數 {len(df)} 小於最小要求 {min_rows}"
    
    # 檢查必要欄位
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return False, f"缺少必要欄位: {', '.join(missing_columns)}"
    
    return True, "驗證通過"


def validate_date_range(
    beg_date: str,
    end_date: str,
    date_format: str = "%Y-%m-%d"
) -> Tuple[bool, str]:
    """
    驗證日期範圍
    
    Args:
        beg_date: 開始日期
        end_date: 結束日期
        date_format: 日期格式
        
    Returns:
        Tuple[bool, str]: (是否通過, 訊息)
    """
    try:
        start = pd.to_datetime(beg_date, format=date_format)
        end = pd.to_datetime(end_date, format=date_format)
        
        if start > end:
            return False, f"開始日期 {beg_date} 晚於結束日期 {end_date}"
        
        return True, "驗證通過"
    
    except Exception as e:
        return False, f"日期格式錯誤: {str(e)}"


def log_validation_result(result: Tuple[bool, str], level: str = "info"):
    """
    記錄驗證結果
    
    Args:
        result: 驗證結果 (is_valid, message)
        level: 日誌級別
    """
    is_valid, message = result
    
    if is_valid:
        logger.info(f"✓ {message}")
    else:
        if level == "error":
            logger.error(f"✗ {message}")
        else:
            logger.warning(f"✗ {message}")
