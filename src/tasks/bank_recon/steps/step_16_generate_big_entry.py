"""
Step 16: 生成大 Entry
將寬格式分錄轉換為長格式，並生成 pivot 報表
"""

from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger

from ..utils import (
    AccountingEntryTransformer,
    AccountingEntryProcessor,
    calculate_daily_balance,
    dfr_balance_check,
    create_big_entry_pivot,
    validate_result,
)


class GenerateBigEntryStep(PipelineStep):
    """
    生成大 Entry 步驟
    
    功能:
    1. 使用 AccountingEntryTransformer 轉換分錄
    2. 生成 df_entry_long（長格式）
    3. 執行 DFR 餘額核對
    4. 生成 df_big_entry（pivot）
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("GenerateBigEntryStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始生成大 Entry")
            self.logger.info("=" * 60)
            
            # 取得參數
            year = int(context.get_variable('current_month')[:4])
            month = int(context.get_variable('current_month')[4:])
            accounts_config = context.get_variable('accounts_config', {})
            accounts_detail = context.get_variable('accounts_detail', {})
            type_order = context.get_variable('transaction_type_order', {})
            
            dfr_beginning_balance = context.get_variable('dfr_beginning_balance', 0)
            
            # 取得資料
            df_entry_temp = context.get_auxiliary_data('entry_temp')
            df_result_dfr = context.get_auxiliary_data('dfr_result')
            
            if df_entry_temp is None:
                raise ValueError("缺少分錄資料 (entry_temp)")
            
            # =================================================================
            # 1. 建立 AccountingEntryTransformer
            # =================================================================
            transformer = AccountingEntryTransformer(
                accounts_config=accounts_config,
                accounts_detail_config=accounts_detail
            )
            
            # =================================================================
            # 2. 轉換為長格式
            # =================================================================
            df_entry_long = transformer.transform(df_entry_temp)
            
            if len(df_entry_long) == 0:
                self.logger.warning("轉換後無分錄資料")
            else:
                self.logger.info(f"轉換完成: {len(df_entry_long)} 筆分錄")
            
            context.add_auxiliary_data('entry_long', df_entry_long)
            
            # =================================================================
            # 3. 驗證借貸平衡
            # =================================================================
            is_balanced, balance_diff = transformer.validate_balance(df_entry_long)
            
            if not is_balanced:
                context.add_warning(f"長格式分錄借貸不平衡: {balance_diff:,.2f}")
            
            # =================================================================
            # 4. 使用 AccountingEntryProcessor 處理
            # =================================================================
            processor = AccountingEntryProcessor(
                year=year,
                month=month,
                config={
                    'accounts': accounts_config,
                    'accounts_detail': accounts_detail,
                    'transaction_type_order': type_order,
                }
            )
            
            # 套用交易類型排序
            df_entry_long = processor.apply_type_order(df_entry_long)
            
            # =================================================================
            # 5. DFR 餘額核對
            # =================================================================
            if df_result_dfr is not None:
                self.logger.info("執行 DFR 餘額核對...")
                
                df_balance_check = dfr_balance_check(df_entry_long, df_result_dfr, '104171')
                context.add_auxiliary_data('dfr_balance_check', df_balance_check)
                
                # 計算每日餘額
                if len(df_balance_check) > 0:
                    df_with_balance = calculate_daily_balance(
                        df_balance_check,
                        dfr_beginning_balance,
                        'entry_amount',
                        'balance_entry'
                    )
                    context.add_auxiliary_data('entry_with_balance', df_with_balance)
                    
                    # 檢查差異
                    if 'check' in df_with_balance.columns:
                        max_diff = df_with_balance['check'].abs().max()
                        if max_diff > 1:
                            context.add_warning(f"DFR 餘額核對最大差異: {max_diff:,.0f}")
                            self.logger.warning(f"DFR 餘額核對最大差異: {max_diff:,.0f}")
                        else:
                            self.logger.info("DFR 餘額核對通過")
            
            # =================================================================
            # 6. 生成大 Entry Pivot
            # =================================================================
            df_big_entry = create_big_entry_pivot(df_entry_long, type_order)
            context.add_auxiliary_data('big_entry', df_big_entry)
            
            self.logger.info(f"大 Entry 生成完成: {len(df_big_entry)} 行")
            
            # =================================================================
            # 7. 驗證結果
            # =================================================================
            validation = validate_result(df_entry_long)
            context.add_auxiliary_data('entry_long_validation', validation)
            
            self.logger.info(f"分錄驗證: {validation['total_entries']} 筆, "
                             f"科目 {validation['unique_accounts']} 個, "
                             f"平衡: {validation['is_balanced']}")
            
            # =================================================================
            # 8. 生成報告
            # =================================================================
            report = processor.generate_report(df_entry_long)
            context.add_auxiliary_data('entry_report', report)
            
            # 顯示科目摘要
            self.logger.info("\n科目摘要:")
            for account_no, amount in report['accounts_summary'].items():
                if abs(amount) > 0:
                    self.logger.info(f"  {account_no}: {amount:,.0f}")
            
            # =================================================================
            # 9. 摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("大 Entry 生成完成")
            self.logger.info(f"  分錄筆數: {len(df_entry_long)}")
            self.logger.info(f"  科目數量: {validation['unique_accounts']}")
            self.logger.info(f"  交易類型: {validation['unique_types']}")
            self.logger.info(f"  總金額: {validation['total_amount']:,.0f}")
            self.logger.info(f"  借貸平衡: {'是' if validation['is_balanced'] else '否'}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="大 Entry 生成完成",
                metadata={
                    'entry_count': len(df_entry_long),
                    'unique_accounts': validation['unique_accounts'],
                    'unique_types': validation['unique_types'],
                    'total_amount': validation['total_amount'],
                    'is_balanced': validation['is_balanced'],
                    'generated_at': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            self.logger.error(f"生成大 Entry 失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
