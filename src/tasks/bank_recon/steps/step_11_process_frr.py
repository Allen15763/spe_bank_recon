"""
Step 11: 處理 FRR (財務部) 資料
讀取並處理財務部 Excel 檔案
"""

from typing import Dict, Any
import pandas as pd
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger

from ..utils import (
    quick_clean_financial_data,
    create_complete_date_range,
    convert_to_long_format,
    calculate_frr_handling_fee,
    calculate_frr_remittance_fee,
    calculate_frr_net_billing,
)


class ProcessFRRStep(PipelineStep):
    """
    處理 FRR 步驟
    
    功能:
    1. 讀取財務部 Excel
    2. 清理資料格式
    3. 轉換為長格式
    4. 計算手續費、匯費、請款 pivot tables
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("ProcessFRRStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始處理 FRR (財務部) 資料")
            self.logger.info("=" * 60)
            
            # 取得參數
            frr_path = context.get_variable('frr_path')
            frr_sheet = context.get_variable('frr_sheet')
            frr_header_row = context.get_variable('frr_header_row', 0)
            frr_columns = context.get_variable('frr_columns', {})
            frr_bank_mapping = context.get_variable('frr_bank_mapping', {})
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            
            # =================================================================
            # 1. 讀取 Excel
            # =================================================================
            self.logger.info(f"讀取 FRR: {frr_path}, Sheet: {frr_sheet}")
            
            df_raw = pd.read_excel(
                frr_path,
                sheet_name=frr_sheet,
                header=frr_header_row
            )
            
            self.logger.info(f"原始資料: {len(df_raw)} 行, {len(df_raw.columns)} 欄")
            
            # =================================================================
            # 2. 清理資料
            # =================================================================
            df_clean = quick_clean_financial_data(df_raw, frr_columns)
            
            # =================================================================
            # 3. 補齊日期範圍
            # =================================================================
            df_complete = create_complete_date_range(df_clean, beg_date, end_date)
            
            # =================================================================
            # 4. 轉換為長格式
            # =================================================================
            long_format_df = convert_to_long_format(df_complete, frr_bank_mapping)
            
            # 儲存長格式資料
            context.add_auxiliary_data('frr_long_format', long_format_df)
            
            # =================================================================
            # 5. 計算各種 Pivot Tables
            # =================================================================
            
            # 5.1 手續費 Pivot
            df_frr_handling_fee = calculate_frr_handling_fee(long_format_df, beg_date, end_date)
            context.add_auxiliary_data('frr_handling_fee', df_frr_handling_fee)
            
            handling_fee_total = (
                df_frr_handling_fee.loc[
                    'Grand Total', 'Grand Total'
                ] if 'Grand Total' in df_frr_handling_fee.index else 0
            )
            self.logger.info(f"FRR 手續費總額: {handling_fee_total:,.0f}")
            
            # 5.2 匯費 Pivot
            df_frr_remittance_fee = calculate_frr_remittance_fee(long_format_df, beg_date, end_date)
            context.add_auxiliary_data('frr_remittance_fee', df_frr_remittance_fee)
            
            remittance_fee_total = (
                df_frr_remittance_fee.loc[
                    'Grand Total', 'Grand Total'
                ] if 'Grand Total' in df_frr_remittance_fee.index else 0
            )
            self.logger.info(f"FRR 匯費總額: {remittance_fee_total:,.0f}")
            
            # 5.3 請款 Pivot
            df_frr_net_billing = calculate_frr_net_billing(long_format_df, beg_date, end_date)
            context.add_auxiliary_data('frr_net_billing', df_frr_net_billing)
            
            net_billing_total = (
                df_frr_net_billing.loc[
                    'Grand Total', 'Grand Total'
                ] if 'Grand Total' in df_frr_net_billing.index else 0
            )
            self.logger.info(f"FRR 請款總額: {net_billing_total:,.0f}")
            
            # =================================================================
            # 6. 摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("FRR 處理完成")
            self.logger.info(f"  日期範圍: {beg_date} ~ {end_date}")
            self.logger.info(f"  長格式資料: {len(long_format_df)} 筆")
            self.logger.info(f"  手續費總額: {handling_fee_total:,.0f}")
            self.logger.info(f"  匯費總額: {remittance_fee_total:,.0f}")
            self.logger.info(f"  請款總額: {net_billing_total:,.0f}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="FRR 處理完成",
                metadata={
                    'records': len(long_format_df),
                    'handling_fee_total': handling_fee_total,
                    'remittance_fee_total': remittance_fee_total,
                    'net_billing_total': net_billing_total,
                    'processed_at': datetime.now().isoformat()
                }
            )
            
        except FileNotFoundError as e:
            self.logger.error(f"FRR 檔案不存在: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"FRR 檔案不存在: {e}"
            )
            
        except Exception as e:
            self.logger.error(f"處理 FRR 失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
