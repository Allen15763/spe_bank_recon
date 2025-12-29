"""
Step 12: 處理 DFR (TW Bank Balance) 資料
讀取並處理銀行餘額 Excel 檔案
"""

from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger

from ..utils import (
    validate_dfr_columns,
    process_dfr_data,
    create_dfr_wp,
    calculate_running_balance,
)


class ProcessDFRStep(PipelineStep):
    """
    處理 DFR 步驟
    
    功能:
    1. 讀取 TW Bank Balance Excel
    2. 驗證欄位（完全配置化）
    3. 計算 inbound/outbound/unsuccessful_ach
    4. 生成 df_result_dfr 和 df_dfr_wp
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("ProcessDFRStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始處理 DFR (TW Bank Balance) 資料")
            self.logger.info("=" * 60)
            
            # 取得參數
            dfr_path = context.get_variable('dfr_path')
            dfr_sheet = context.get_variable('dfr_sheet')
            dfr_header_row = context.get_variable('dfr_header_row', 5)
            dfr_columns = context.get_variable('dfr_columns', {})
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            
            inbound_validation_cols = context.get_variable('dfr_inbound_validation_cols', [])
            outbound_validation_cols = context.get_variable('dfr_outbound_validation_cols', [])
            
            # =================================================================
            # 1. 讀取 Excel
            # =================================================================
            self.logger.info(f"讀取 DFR: {dfr_path}, Sheet: {dfr_sheet}")
            
            df_raw = pd.read_excel(
                dfr_path,
                sheet_name=dfr_sheet,
                header=dfr_header_row
            )
            
            self.logger.info(f"原始資料: {len(df_raw)} 行, {len(df_raw.columns)} 欄")
            
            # 儲存原始 DFR 資料
            context.add_auxiliary_data('dfr_raw', df_raw)
            
            # =================================================================
            # 2. 驗證欄位
            # =================================================================
            validation_result = validate_dfr_columns(
                df_raw,
                inbound_validation_cols,
                outbound_validation_cols
            )
            
            if not validation_result['inbound_valid']:
                context.add_warning(f"DFR Inbound 欄位缺失: {validation_result['inbound_missing']}")
                self.logger.warning(f"Inbound 欄位缺失: {validation_result['inbound_missing']}")
            
            if not validation_result['outbound_valid']:
                context.add_warning(f"DFR Outbound 欄位缺失: {validation_result['outbound_missing']}")
                self.logger.warning(f"Outbound 欄位缺失: {validation_result['outbound_missing']}")
            
            # =================================================================
            # 3. 處理 DFR 資料
            # =================================================================
            df_result_dfr = process_dfr_data(df_raw, beg_date, end_date, dfr_columns)
            
            if len(df_result_dfr) == 0:
                raise ValueError(f"DFR 無資料在日期範圍內: {beg_date} ~ {end_date}")
            
            self.logger.info(f"DFR 處理結果: {len(df_result_dfr)} 筆")
            self.logger.info(f"  Inbound 總額: {df_result_dfr['Inbound'].sum():,.0f}")
            self.logger.info(f"  Outbound 總額: {df_result_dfr['Outbound'].sum():,.0f}")
            self.logger.info(f"  Unsuccessful ACH 總額: {df_result_dfr['Unsuccessful_ACH'].sum():,.0f}")
            
            # =================================================================
            # 4. 取得手續費和匯費資料
            # =================================================================
            # 從原始 DFR 取得 handing_fee_col 和 remittance_fee 欄位
            date_col = dfr_columns.get('date_col', 'Date')
            remittance_fee_col = dfr_columns.get('remittance_fee_col', 'remittance fee')
            handing_fee_col = dfr_columns.get('handing_fee_col', 'handing_fee')

            # 過濾日期範圍
            frr_handling_fee = (
                context.get_auxiliary_data('frr_handling_fee')
                .reset_index()
                .assign(Date=lambda row: row[date_col].astype('string'))
                .query(f"`{date_col}`.between(@beg_date, @end_date)")
                .copy()
            )
            frr_remittance_fee = (
                context.get_auxiliary_data('frr_remittance_fee')
                .reset_index()
                .assign(Date=lambda row: row[date_col].astype('string'))
                .query(f"`{date_col}`.between(@beg_date, @end_date)")
                .copy()
            )
            
            # 提取手續費相關欄位
            if remittance_fee_col in frr_remittance_fee.columns:
                df_result_dfr['remittance_fee'] = frr_remittance_fee[remittance_fee_col].values
            else:
                df_result_dfr['remittance_fee'] = 0
                
            if handing_fee_col in frr_handling_fee.columns:
                df_result_dfr['handing_fee'] = frr_handling_fee[handing_fee_col].values
            else:
                df_result_dfr['handing_fee'] = 0
                
            context.add_auxiliary_data('dfr_result', df_result_dfr)
            
            # =================================================================
            # 5. 建立 DFR 工作底稿
            # =================================================================
            df_dfr_wp = create_dfr_wp(df_result_dfr)
            context.add_auxiliary_data('dfr_wp', df_dfr_wp)
            
            self.logger.info("DFR 工作底稿建立完成")
            
            # =================================================================
            # 6. 取得期初餘額並計算累計餘額
            # =================================================================
            # 從 DFR 取得期初餘額（前一日的餘額）；在OPS的DFR底稿有兩個balance欄位第二個才是銀行餘額!
            balance_col = 'Balance.1'
            if balance_col in df_raw.columns:
                # 取得期間開始前一天的餘額
                df_before = df_raw[df_raw[date_col] < beg_date]
                if len(df_before) > 0:
                    beginning_balance = df_before[balance_col].iloc[-1]
                else:
                    beginning_balance = 0
            else:
                beginning_balance = 0
            
            context.set_variable('dfr_beginning_balance', beginning_balance)
            self.logger.info(f"期初餘額: {beginning_balance:,.0f}")
            
            # 計算累計餘額
            df_with_balance = calculate_running_balance(df_result_dfr, beginning_balance)
            context.add_auxiliary_data('dfr_with_balance', df_with_balance)
            
            ending_balance = df_with_balance['running_balance'].iloc[-1]
            self.logger.info(f"期末餘額: {ending_balance:,.0f}")
            
            # =================================================================
            # 7. 摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("DFR 處理完成")
            self.logger.info(f"  日期範圍: {beg_date} ~ {end_date}")
            self.logger.info(f"  資料筆數: {len(df_result_dfr)} 筆")
            self.logger.info(f"  期初餘額: {beginning_balance:,.0f}")
            self.logger.info(f"  期末餘額: {ending_balance:,.0f}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="DFR 處理完成",
                metadata={
                    'records': len(df_result_dfr),
                    'inbound_total': df_result_dfr['Inbound'].sum(),
                    'outbound_total': df_result_dfr['Outbound'].sum(),
                    'beginning_balance': beginning_balance,
                    'ending_balance': ending_balance,
                    'processed_at': datetime.now().isoformat()
                }
            )
            
        except FileNotFoundError as e:
            self.logger.error(f"DFR 檔案不存在: {e}")
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"DFR 檔案不存在: {e}"
            )
            
        except Exception as e:
            self.logger.error(f"處理 DFR 失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
