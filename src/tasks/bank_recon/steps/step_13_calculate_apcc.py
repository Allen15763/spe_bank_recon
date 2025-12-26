"""
Step 13: 計算 APCC 手續費
計算各銀行收單手續費及 SPE 服務費
"""

from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger

from ..utils import (
    reformat_df_wp,
    get_apcc_service_fee_charged,
    apply_ops_adjustment,
    apply_rounding_adjustment,
    calculate_trust_account_validation,
    validate_apcc_vs_frr,
    get_spe_charge_with_tax,
    reformat_df_summary,
    transpose_df_summary,
)


class CalculateAPCCStep(PipelineStep):
    """
    計算 APCC 手續費步驟
    
    功能:
    1. 從 Trust Account Fee 取得請款資料
    2. 計算 APCC 手續費
    3. 套用調扣調整
    4. 套用尾差調整
    5. 生成 df_apcc_acquiring_charge
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("CalculateAPCCStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始計算 APCC 手續費")
            self.logger.info("=" * 60)
            
            # 取得參數
            charge_rates = context.get_variable('charge_rates', [])
            ops_adj_amt = context.get_variable('ops_adj_amt', 0)
            taishi_rounding = context.get_variable('taishi_service_fee_rounding', 0)
            ctbc_rounding = context.get_variable('ctbc_service_fee_rounding', 0)
            end_date = context.get_variable('end_date')
            
            # =================================================================
            # 1. 取得 Trust Account Fee 資料
            # =================================================================
            # 從 Step 9 取得 trust_account_fee
            df_trust_account = context.get_auxiliary_data('trust_account_fee')
            
            if df_trust_account is None or len(df_trust_account) == 0:
                self.logger.warning("Trust Account Fee 資料不存在，嘗試從 Escrow 資料重建")
                # 嘗試從 Escrow Summary 取得
                df_escrow_summary = context.get_auxiliary_data('escrow_summary')
                if df_escrow_summary is None:
                    raise ValueError("無法取得 Trust Account Fee 或 Escrow Summary 資料")
                df_trust_account = df_escrow_summary
            
            self.logger.info(f"Trust Account Fee 資料: {len(df_trust_account)} 行")
            
            # =================================================================
            # 2. 重新格式化工作底稿（只取 claimed 欄位）
            # =================================================================
            df_wp = reformat_df_wp(df_trust_account, is_claimed_only=True)
            
            self.logger.info(f"工作底稿格式化完成: {len(df_wp)} 行")
            
            # =================================================================
            # 3. 套用調扣調整
            # =================================================================
            if ops_adj_amt != 0:
                df_wp = apply_ops_adjustment(df_wp, ops_adj_amt)
                self.logger.info(f"已套用調扣調整: {ops_adj_amt:,.0f}")
            
            # =================================================================
            # 4. 計算 APCC 手續費
            # =================================================================
            # 預設費率（如果沒有從 Google Sheets 載入）
            if not charge_rates:
                charge_rates = [0.0185, 0.0185, 0.018, 0.017, 0.015, 0.0185]  # 預設費率
                self.logger.warning("使用預設手續費率")
            
            df_apcc = get_apcc_service_fee_charged(df_wp, charge_rates)
            
            self.logger.info("APCC 手續費計算完成")
            for _, row in df_apcc.iterrows():
                if 'transaction_type' in df_apcc.columns:
                    self.logger.info(f"  {row.get('transaction_type', 'N/A')}: {row.get('commission_fee', 0):,.0f}")
            
            # =================================================================
            # 5. 套用手續費尾差調整
            # =================================================================
            if taishi_rounding != 0:
                # 找到 commission_fee 欄位的索引
                if 'commission_fee' in df_apcc.columns:
                    fee_col_idx = df_apcc.columns.get_loc('commission_fee')
                    df_apcc = apply_rounding_adjustment(
                        df_apcc, '台新', taishi_rounding, fee_col_idx
                    )
            
            if ctbc_rounding != 0:
                if 'commission_fee' in df_apcc.columns:
                    fee_col_idx = df_apcc.columns.get_loc('commission_fee')
                    df_apcc = apply_rounding_adjustment(
                        df_apcc, 'CTBC', ctbc_rounding, fee_col_idx
                    )
            
            # =================================================================
            # 6. 計算含稅 SPE 服務費
            # =================================================================
            df_spe_charge = get_spe_charge_with_tax(df_apcc, tax_rate=0.05)
            
            # 合併到 APCC DataFrame
            df_apcc['SPE_Charge_with_Tax'] = df_spe_charge['SPE Charge'].values
            
            # 儲存 APCC 結果
            context.add_auxiliary_data('apcc_acquiring_charge', df_apcc)
            
            total_commission = df_apcc['commission_fee'].sum()
            total_spe_charge = df_apcc['SPE_Charge_with_Tax'].sum()
            
            self.logger.info(f"手續費總額: {total_commission:,.0f}")
            self.logger.info(f"SPE 服務費(含稅): {total_spe_charge:,.0f}")
            
            # =================================================================
            # 7. 驗證與 FRR 的一致性
            # =================================================================
            df_frr_net_billing = context.get_auxiliary_data('frr_net_billing')
            
            if df_frr_net_billing is not None:
                df_validate = validate_apcc_vs_frr(df_apcc, df_frr_net_billing)
                context.add_auxiliary_data('apcc_validate_frr', df_validate)
                
                if 'diff' in df_validate.columns:
                    total_diff = df_validate['diff'].sum()
                    if abs(total_diff) > 1:
                        context.add_warning(f"APCC 與 FRR 請款差異: {total_diff:,.0f}")
                        self.logger.warning(f"APCC 與 FRR 請款差異: {total_diff:,.0f}")
                    else:
                        self.logger.info("APCC 與 FRR 請款驗證通過")
            
            # =================================================================
            # 8. 建立 Summary
            # =================================================================
            # 取得 Escrow Invoice 資料
            df_escrow_inv = context.get_auxiliary_data('escrow_invoice')
            
            if df_trust_account is not None and df_escrow_inv is not None:
                df_validation = calculate_trust_account_validation(df_trust_account, df_escrow_inv)
                context.add_auxiliary_data('trust_account_validation', df_validation)
                
                # 重新格式化 Summary
                df_summary = reformat_df_summary(df_apcc, df_validation)
                
                # 轉置為長格式
                df_summary_long = transpose_df_summary(df_summary, end_date)
                
                context.add_auxiliary_data('apcc_summary', df_summary)
                context.add_auxiliary_data('apcc_summary_long', df_summary_long)
            
            # =================================================================
            # 9. 摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("APCC 手續費計算完成")
            self.logger.info(f"  手續費總額: {total_commission:,.0f}")
            self.logger.info(f"  SPE 服務費(含稅): {total_spe_charge:,.0f}")
            self.logger.info(f"  調扣金額: {ops_adj_amt:,.0f}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="APCC 手續費計算完成",
                metadata={
                    'total_commission': total_commission,
                    'total_spe_charge': total_spe_charge,
                    'ops_adj_amt': ops_adj_amt,
                    'calculated_at': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            self.logger.error(f"計算 APCC 失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
