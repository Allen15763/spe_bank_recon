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
    calculate_charge_rate,
    transform_payment_data,
    get_df_cc_rev,
    calculate_spe_transaction_percentage,
    calculate_transaction_percentage,
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

    - Step 13，APCC手續費與Summary分析表:
        - 完成apcc_acquiring_charge(APCC手續費)，為SPE對SPT的charge之估算。
        - context.get_auxiliary_data('apcc_acquiring_charge_DW') >> **DW資料(APCC 手續費)，確認後上傳至雲表。**
        - 完成df_apcc_summary_fin，為APCC手續費的請款數加上trust_account_fee的service fee(topup後)與相關手續費比率的整合分析表
        - context.get_auxiliary_data('apcc_summary_long') >> **DW資料(acquiring_charge_raw)，確認後上傳至雲表。**
        - context.get_auxiliary_data('apcc_summary')，topuped trust_account_fee
        - context.get_auxiliary_data('df_summary_long_without_spe_charge', df_summary_long_without_spe_charge)，
            沒有SPE資訊的summary
        - context.get_auxiliary_data('df_apcc_summary_fin', df_apcc_summary_fin)，含SPE資訊的summary
        - net_cc_rev、spe_charge_proportion、acquiring_proportion，雲表樞紐的分析資料源，更新DW資料(APCC 手續費、acquiring_charge_raw)後自動刷新

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
            ops_taishi_adj_amt = context.get_variable('ops_taishi_adj_amt', 0)
            ops_cub_adj_amt = context.get_variable('ops_cub_adj_amt', 0)
            ops_ctbc_adj_amt = context.get_variable('ops_ctbc_adj_amt', 0)
            ops_nccc_adj_amt = context.get_variable('ops_nccc_adj_amt', 0)
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
            df_wp_with_service_fee = reformat_df_wp(df_trust_account, is_claimed_only=False)
            
            self.logger.info(f"工作底稿格式化完成: {len(df_wp)} 行")
            
            # =================================================================
            # 3. 套用調扣調整；~~只有NCCC調在3期，其他調在Normal~~ 
            # NCCC是不調的 Ref 202408-APCC 手續費 、CTBC應該也是
            # =================================================================
            if ops_taishi_adj_amt != 0:
                df_wp = apply_ops_adjustment(df_wp, ops_taishi_adj_amt)
                df_wp_with_service_fee = apply_ops_adjustment(df_wp_with_service_fee, ops_taishi_adj_amt)
                self.logger.info(f"Taishi已套用調扣調整: {ops_taishi_adj_amt:,.0f}")

            if ops_cub_adj_amt != 0:
                df_wp = apply_ops_adjustment(df_wp, ops_cub_adj_amt, adj_idx=2)
                df_wp_with_service_fee = apply_ops_adjustment(df_wp_with_service_fee, ops_cub_adj_amt, adj_idx=2)
                self.logger.info(f"CUB已套用調扣調整: {ops_cub_adj_amt:,.0f}")

            if ops_ctbc_adj_amt != 0:
                df_wp = apply_ops_adjustment(df_wp, ops_ctbc_adj_amt, adj_idx=3)
                df_wp_with_service_fee = apply_ops_adjustment(df_wp_with_service_fee, ops_ctbc_adj_amt, adj_idx=3)
                self.logger.info(f"CTBC已套用調扣調整: {ops_ctbc_adj_amt:,.0f}")

            if ops_nccc_adj_amt != 0:
                df_wp = apply_ops_adjustment(df_wp, ops_nccc_adj_amt, adj_idx=1)
                df_wp_with_service_fee = apply_ops_adjustment(df_wp_with_service_fee, ops_nccc_adj_amt, adj_idx=1)
                self.logger.info(f"NCCC已套用調扣調整: {ops_nccc_adj_amt:,.0f}")
            
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
            # 5. 套用手續費尾差調整；調Escrow_Inv(trust_account_fee)的手續費尾差
            # =================================================================
            if taishi_rounding != 0:
                # 找到欄位的索引
                regex = "(?=.*service_fee)(?=.*台)"
                if len(df_wp_with_service_fee.filter(regex=regex).columns) != 0:
                    fee_col_idx = df_wp_with_service_fee.columns.get_loc(
                        df_wp_with_service_fee.filter(regex=regex).columns[0]
                    )
                    df_wp_with_service_fee = apply_rounding_adjustment(
                        df_wp_with_service_fee, '台新', taishi_rounding, fee_col_idx
                    )
            
            if ctbc_rounding != 0:
                regex = "(?=.*service_fee)(?=.*CTBC)"
                if len(df_wp_with_service_fee.filter(regex=regex).columns) != 0:
                    fee_col_idx = df_wp_with_service_fee.columns.get_loc(
                        df_wp_with_service_fee.filter(regex=regex).columns[0]
                    )
                    df_wp_with_service_fee = apply_rounding_adjustment(
                        df_wp_with_service_fee, 'CTBC', ctbc_rounding, fee_col_idx
                    )

            # =================================================================
            # 6. 計算含稅 SPE 服務費；SPE向SPT收
            # =================================================================
            df_spe_charge = get_spe_charge_with_tax(df_apcc, tax_rate=0.05)
            
            # 合併到 APCC DataFrame
            df_apcc['SPE_Charge_with_Tax'] = df_spe_charge['SPE Charge'].values
            
            # 儲存 APCC 結果
            context.add_auxiliary_data('apcc_acquiring_charge', df_apcc)
            context.add_auxiliary_data(
                'apcc_acquiring_charge_DW', 
                df_apcc.drop('SPE_Charge_with_Tax', axis=1).assign(end_date=end_date)
            )
            
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
            # 取得 Escrow Invoice 驗證資料
            df_escrow_inv = context.get_auxiliary_data('trust_account_validation').loc['total_service_fee'].iloc[:, 3:]

            if df_wp_with_service_fee is not None and df_escrow_inv is not None: 
                # 重新格式化 Summary；補齊Normal數字
                df_summary = reformat_df_summary(df_wp_with_service_fee, df_escrow_inv)
                
                # 轉置為長格式 & 在normal的acquring上標記費率
                df_summary_long = transpose_df_summary(df_summary, end_date)
                df_summary_long = calculate_charge_rate(df_summary_long)
                
                context.add_auxiliary_data('apcc_summary', df_summary)
                context.add_auxiliary_data('apcc_summary_long', df_summary_long)

                # 暫時紀錄 等於df_summary_wp_transposed_without_spe_charge
                df_summary_long_without_spe_charge = transform_payment_data(df_summary_long, end_date)
                context.add_auxiliary_data('df_summary_long_without_spe_charge', df_summary_long_without_spe_charge)

                df_apcc_summary_fin = pd.concat(
                    [df_summary_long_without_spe_charge.reset_index(), df_spe_charge], 
                    axis=1
                )
                context.add_auxiliary_data('df_apcc_summary_fin', df_apcc_summary_fin)
            
            # =================================================================
            # 8.1 累計分析表原始資料倉儲
            # =================================================================
            df_summary_wp_transposed_history = context.get_auxiliary_data('acquiring_charge_history')
            df_spe_charge_history = context.get_auxiliary_data('apcc_history')

            # CC Net Revenue
            df_cc_rev = get_df_cc_rev(df_summary_wp_transposed_history,
                                      df_spe_charge_history)

            # SPE 交易類型手續費占比; (Commission fee)當期每個類型除當期小計
            result_spe_proportion = calculate_spe_transaction_percentage(df_spe_charge_history)

            # SUMMARY 交易類型手續費占比; (acquiring所有銀行總計)當期每個類型除當期小計
            result_wp_proportion = calculate_transaction_percentage(df_summary_wp_transposed_history)

            context.add_auxiliary_data('net_cc_rev', df_cc_rev)
            context.add_auxiliary_data('spe_charge_proportion', result_spe_proportion)
            context.add_auxiliary_data('acquiring_proportion', result_wp_proportion)
            
            # =================================================================
            # 9. 摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("APCC 手續費計算完成")
            self.logger.info(f"  手續費總額: {total_commission:,.0f}")
            self.logger.info(f"  SPE 服務費(含稅): {total_spe_charge:,.0f}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="APCC 手續費計算完成",
                metadata={
                    'total_commission': total_commission,
                    'total_spe_charge': total_spe_charge,
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

