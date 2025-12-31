"""
Step 15: 準備會計分錄
整理會計科目，將每日資料轉換為分錄格式
"""

from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger

from ..utils import (
    process_accounting_entries,
    validate_accounting_balance,
    MonthlyConfig,
    AccountingEntryProcessor,
    validate_result,
    dfr_balance_check,
    summarize_balance_check,
)


class PrepareEntriesStep(PipelineStep):
    """
    準備會計分錄步驟
    
    功能:
    1. 整理會計科目
    2. 處理國泰/中信回饋金
    3. 生成 df_entry_temp（寬格式）
    4. 驗證會計平衡
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("PrepareEntriesStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始準備會計分錄")
            self.logger.info("=" * 60)
            
            # 取得參數
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            cod_remittance_fee = context.get_variable('cod_remittance_fee', 0)
            ach_exps = context.get_variable('ach_exps', 0)
            ctbc_rebate_amt = context.get_variable('ctbc_rebate_amt', 0)
            
            # 取得資料
            df_dfr_wp = context.get_auxiliary_data('dfr_wp')
            df_result_dfr = context.get_auxiliary_data('dfr_result')
            cub_rebate = context.get_auxiliary_data('cub_rebate')
            received_ctbc_spt = context.get_auxiliary_data('received_ctbc_spt')
            df_easyfund = pd.read_excel(context.get_variable('easyfund_path'), 
                                        usecols=context.get_variable('easyfund_usecols'))
            
            if df_dfr_wp is None:
                raise ValueError("缺少 DFR 工作底稿資料")
            
            # =================================================================
            # 1. 準備回饋金資料
            # =================================================================
            # 確保 cub_rebate 存在
            if cub_rebate is None:
                self.logger.warning("無國泰回饋金資料，使用零值")
                date_range = pd.date_range(beg_date, end_date, freq='D')
                cub_rebate = pd.DataFrame({
                    'Date': date_range.strftime('%Y-%m-%d'),
                    'amount': 0
                })
            
            cub_rebate_total = cub_rebate['amount'].sum()
            self.logger.info(f"國泰回饋金總額: {cub_rebate_total:,.0f}")
            
            # 確保 received_ctbc_spt 存在
            if received_ctbc_spt is None:
                self.logger.warning("無中信 SPT 入款資料，使用零值")
                date_range = pd.date_range(beg_date, end_date, freq='D')
                received_ctbc_spt = pd.DataFrame({
                    'Date': date_range.strftime('%Y-%m-%d'),
                    'amount': 0
                })
            
            received_spt_total = received_ctbc_spt['amount'].sum()
            self.logger.info(f"中信 SPT 入款總額: {received_spt_total:,.0f}")
            
            # =================================================================
            # 2. 取得利息資料
            # =================================================================
            if df_result_dfr is not None and 'interest' in df_result_dfr.columns:
                interest = df_result_dfr['interest']
            else:
                interest = pd.Series([0] * len(pd.date_range(beg_date, end_date, freq='D')))
            
            interest_total = interest.sum()
            self.logger.info(f"利息總額: {interest_total:,.0f}")
            
            # =================================================================
            # 3. 整理會計分錄（寬格式）
            # =================================================================
            df_entry_temp = process_accounting_entries(
                df_dfr_wp,
                cub_rebate,
                received_ctbc_spt,
                interest,
                beg_date,
                end_date
            )
            
            context.add_auxiliary_data('entry_temp', df_entry_temp)
            
            self.logger.info(f"會計分錄整理完成: {len(df_entry_temp)} 天")
            
            # =================================================================
            # 4. 加入額外調整項目
            # =================================================================
            # COD 匯費
            if cod_remittance_fee != 0:
                self.logger.info(f"COD 匯費: {cod_remittance_fee:,.0f}")
            
            # 中信 ACH eACH EDI 費用
            if ach_exps != 0:
                self.logger.info(f"ACH 費用: {ach_exps:,.0f}")
            
            # 中信回饋金
            if ctbc_rebate_amt != 0:
                self.logger.info(f"中信回饋金: {ctbc_rebate_amt:,.0f}")
            
            # 儲存調整項目
            context.set_variable('entry_adjustments', {
                'cod_remittance_fee': cod_remittance_fee,
                'ach_exps': ach_exps,
                'ctbc_rebate_amt': ctbc_rebate_amt,
            })
            
            # =================================================================
            # 5. 驗證會計平衡
            # =================================================================
            validation = validate_accounting_balance(df_entry_temp)
            
            if validation['is_balanced']:
                self.logger.info("會計平衡驗證通過")
            else:
                self.logger.warning(f"會計平衡驗證失敗: 差額 {validation['total_diff']:,.2f}")
                context.add_warning(f"會計平衡差額: {validation['total_diff']:,.2f}")
            
            context.add_auxiliary_data('entry_validation', validation)
            
            # =================================================================
            # 6. 顯示分錄摘要
            # =================================================================
            # 計算各科目總額
            acc_cols = [col for col in df_entry_temp.columns if col.startswith('acc_')]
            
            self.logger.info("\n分錄摘要:")
            for col in acc_cols:
                total = df_entry_temp[col].sum()
                if abs(total) > 0:
                    self.logger.info(f"  {col}: {total:,.0f}")

            print("初始化處理器...")

            acquiring_amt: float | int = (
                context.get_auxiliary_data('apcc_acquiring_charge')
                .query("transaction_type=='小計'").commission_fee.values[0])

            config_obj = MonthlyConfig(
                2025, 10,
                df_easyfund=df_easyfund,
                apcc_acquiring_charge=acquiring_amt,
                ach_exps=ach_exps,
                cod_remittance_fee=cod_remittance_fee,
                df_ctbc_rebate_amt=ctbc_rebate_amt,
                beg_date=beg_date
            )

            processor = AccountingEntryProcessor(2025, 10, config_obj)  # 在實例化時導入config

            # 執行完整處理
            print("\n開始處理...")
            df_entry_long = processor.process(df_entry_temp)

            # 產生報告
            processor.generate_report(df_entry_long)

            # 驗證結果
            print("\n驗證結果...")
            # from example_usage import validate_result
            validate_result(df_entry_long)

            df_entry_long['accounting_date'] = df_entry_long['accounting_date'].fillna('期末會計調整')

            type_order = {
                '期初數': '00.期初數',
                '內扣_ctbc_cc_匯費': '01.內扣_ctbc_cc_匯費',
                'received_ctbc_spt': '02.received_ctbc_spt',
                'received_ctbc_spt_退匯': '03.received_ctbc_spt_退匯',
                'out_ctbc_spt': '04.out_ctbc_spt',
                'other': '05.other',
                'other_利息': '05.other_利息',
                'spt': '06.spt',
                'spe_withdrawal': '07.spe_withdrawal',
                'spl手續費調整': '08.spl手續費調整',
                '發票已開立沖轉': '09.發票已開立沖轉',
            }
            val_zero_excludes = [
                '00.期初數',
                '09.發票已開立沖轉',
            ]

            df_entry_long_temp = df_entry_long.copy()
            df_entry_long_temp['transaction_type'] = (df_entry_long_temp['transaction_type']
                                                      .map(type_order)
                                                      .fillna(df_entry_long['transaction_type'])
                                                      )

            result_check_dfr = dfr_balance_check(df_entry_long_temp, df_result_dfr)
            summary_check_dfr = summarize_balance_check(result_check_dfr)

            result_check_gategory = df_entry_long_temp.query("~transaction_type.isin(@val_zero_excludes)").pivot_table(
                index=['accounting_date'], 
                columns='transaction_type', values='amount', aggfunc='sum', margins=True, margins_name='Total'
            ).reset_index()

            df_big_entry = df_entry_long_temp.pivot_table(
                index=['account_no', 'account_desc', 'transaction_type'], 
                columns='accounting_date', values='amount', aggfunc='sum', margins=True, margins_name='Total'
            ).reset_index()
            
            # =================================================================
            # 7. 摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("會計分錄準備完成")
            self.logger.info(f"  分錄天數: {len(df_entry_temp)}")
            self.logger.info(f"  科目數量: {len(acc_cols)}")
            self.logger.info(f"  國泰回饋金: {cub_rebate_total:,.0f}")
            self.logger.info(f"  利息收入: {interest_total:,.0f}")
            self.logger.info(f"  會計平衡: {'是' if validation['is_balanced'] else '否'}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="會計分錄準備完成",
                metadata={
                    'days': len(df_entry_temp),
                    'account_count': len(acc_cols),
                    'cub_rebate_total': cub_rebate_total,
                    'interest_total': interest_total,
                    'is_balanced': validation['is_balanced'],
                    'total_diff': validation['total_diff'],
                    'prepared_at': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            self.logger.error(f"準備會計分錄失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
