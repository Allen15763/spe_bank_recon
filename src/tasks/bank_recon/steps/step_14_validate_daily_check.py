"""
Step 14: 驗證 Daily Check 資料
驗證 FRR 手續費、請款金額與 Escrow Invoice 的一致性
"""

from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger

from ..utils import (
    validate_frr_handling_fee,
    validate_frr_net_billing,
    convert_flatIndex_to_multiIndex,
)


class ValidateDailyCheckStep(PipelineStep):
    """
    驗證 Daily Check 步驟
        從trust_account_fee再topup調扣跟尾差(if any)後的驗證
    
    功能:
    1. 驗證 FRR 手續費與 Escrow Invoice
    2. 驗證 FRR 請款金額
    3. 生成驗證報告
    4. 記錄警告/錯誤
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("ValidateDailyCheckStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始驗證 Daily Check 資料")
            self.logger.info("=" * 60)
            
            # 取得資料
            df_frr_handling_fee = context.get_auxiliary_data('frr_handling_fee')
            df_frr_net_billing = context.get_auxiliary_data('frr_net_billing')
            df_escrow_summary = context.get_auxiliary_data('apcc_summary')
            # 轉換成MutiIndex
            df_escrow_summary = convert_flatIndex_to_multiIndex(df_escrow_summary)
            
            validation_results = {
                'handling_fee_valid': True,
                'net_billing_valid': True,
                'warnings': [],
                'errors': []
            }
            
            # =================================================================
            # 1. 驗證 FRR 手續費
            # =================================================================
            if df_frr_handling_fee is not None and df_escrow_summary is not None:
                self.logger.info("驗證 FRR 手續費...")
                
                df_validate_handling = validate_frr_handling_fee(
                    df_frr_handling_fee, 
                    df_escrow_summary
                )
                
                context.add_auxiliary_data('validate_frr_handling_fee', df_validate_handling)
                
                if len(df_validate_handling) > 0 and 'diff' in df_validate_handling.columns:
                    # 檢查差異
                    diff_rows = df_validate_handling[df_validate_handling['diff'].abs() > 1]
                    
                    if len(diff_rows) > 0:
                        validation_results['handling_fee_valid'] = False
                        for _, row in diff_rows.iterrows():
                            bank = row.get('bank', 'Unknown')
                            diff = row.get('diff', 0)
                            msg = f"FRR 手續費差異 - {bank}: {diff:,.0f}"
                            validation_results['warnings'].append(msg)
                            self.logger.warning(msg)
                    else:
                        self.logger.info("FRR 手續費驗證通過")
            else:
                self.logger.warning("無法驗證 FRR 手續費: 缺少必要資料")
                validation_results['warnings'].append("無法驗證 FRR 手續費: 缺少必要資料")
            
            # =================================================================
            # 2. 驗證 FRR 請款金額
            # =================================================================
            if df_frr_net_billing is not None and df_escrow_summary is not None:
                self.logger.info("驗證 FRR 請款金額...")
                
                df_validate_billing = validate_frr_net_billing(
                    df_frr_net_billing, 
                    df_escrow_summary
                )
                
                context.add_auxiliary_data('validate_frr_net_billing', df_validate_billing)
                
                if len(df_validate_billing) > 0 and 'diff' in df_validate_billing.columns:
                    # 檢查差異
                    diff_rows = df_validate_billing[df_validate_billing['diff'].abs() > 1]
                    
                    if len(diff_rows) > 0:
                        validation_results['net_billing_valid'] = False
                        for _, row in diff_rows.iterrows():
                            bank = row.get('bank', 'Unknown')
                            diff = row.get('diff', 0)
                            msg = f"FRR 請款差異 - {bank}: {diff:,.0f}"
                            validation_results['warnings'].append(msg)
                            self.logger.warning(msg)
                    else:
                        self.logger.info("FRR 請款驗證通過")
            else:
                self.logger.warning("無法驗證 FRR 請款: 缺少必要資料")
                validation_results['warnings'].append("無法驗證 FRR 請款: 缺少必要資料")
            
            # =================================================================
            # 3. 建立驗證摘要報告
            # =================================================================
            validation_summary = pd.DataFrame({
                'validation_item': [
                    'FRR 手續費',
                    'FRR 請款',
                ],
                'status': [
                    '通過' if validation_results['handling_fee_valid'] else '有差異',
                    '通過' if validation_results['net_billing_valid'] else '有差異',
                ],
                'notes': [
                    '',
                    '',
                ]
            })
            
            context.add_auxiliary_data('validation_summary', validation_summary)
            
            # 加入警告到 context
            for warning in validation_results['warnings']:
                context.add_warning(warning)
            
            # =================================================================
            # 5. 摘要
            # =================================================================
            all_valid = (
                validation_results['handling_fee_valid'] and 
                validation_results['net_billing_valid'] and 
                len(validation_results['errors']) == 0
            )
            
            self.logger.info("\n" + "=" * 60)
            self.logger.info("Daily Check 驗證完成")
            self.logger.info(f"  整體狀態: {'通過' if all_valid else '有差異'}")
            self.logger.info(f"  警告數量: {len(validation_results['warnings'])}")
            self.logger.info(f"  錯誤數量: {len(validation_results['errors'])}")
            self.logger.info("=" * 60 + "\n")
            
            # 即使有警告，也視為成功（警告不阻擋流程）
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="Daily Check 驗證完成",
                metadata={
                    'all_valid': all_valid,
                    'handling_fee_valid': validation_results['handling_fee_valid'],
                    'net_billing_valid': validation_results['net_billing_valid'],
                    'warning_count': len(validation_results['warnings']),
                    'error_count': len(validation_results['errors']),
                    'validated_at': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            self.logger.error(f"驗證 Daily Check 失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
