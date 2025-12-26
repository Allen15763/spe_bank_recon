"""
Step 9: 生成 Trust Account Fee 工作底稿
"""

import pandas as pd
import numpy as np
from pathlib import Path

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger


class GenerateTrustAccountStep(PipelineStep):
    """生成 Trust Account Fee 工作底稿"""
    
    TRANSACTION_TYPE_MAPPING = {
        '03': '3期', '06': '6期', '12': '12期', '24': '24期',
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("GenerateTrustAccountStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始生成 Trust Account Fee 工作底稿")
            self.logger.info("=" * 60)
            
            # ===============================================================
            # 1. 收集所有分期數據
            # ===============================================================
            bank_data = [
                (context.get_auxiliary_data('ub_installment'), '聯邦'),
                (self.merge_cub(context), '國泰'),
                (context.get_auxiliary_data('nccc_installment'), 'NCCC'),
                (context.get_auxiliary_data('taishi_installment'), '台新'),
                (context.get_auxiliary_data('ctbc_installment'), 'CTBC'),
            ]
            
            dfs = []
            for df, bank in bank_data:
                df = df[['transaction_type', 'total_claimed', 'total_service_fee']].copy()
                df['bank'] = bank
                dfs.append(df)
            
            df_all = pd.concat(dfs, ignore_index=True)
            
            # 標準化 transaction_type
            df_all['transaction_type'] = df_all['transaction_type'].apply(
                lambda x: x if x in self.TRANSACTION_TYPE_MAPPING.values() else 'normal'
            )
            
            # 聚合並透視
            df_pivot = df_all.groupby(['bank', 'transaction_type']).sum().unstack(0)
            
            # ===============================================================
            # 2. 更新 normal 金額 (國泰和聯邦)
            # ===============================================================
            df_escrow = context.get_auxiliary_data('df_summary_escrow_inv')
            
            # 國泰 normal
            total_cub = df_escrow.query("銀行.str.contains('cub')")['對帳_請款金額_Trust_Account_Fee'].sum()
            existing_cub = df_pivot.loc[df_pivot.index != 'normal', ('total_claimed', '國泰')].sum()
            df_pivot.loc['normal', ('total_claimed', '國泰')] = total_cub - existing_cub
            
            # 聯邦 normal
            _query = "銀行.isin(['ub_noninstallment', 'ub_installment'])"
            total_ub = df_escrow.query(_query)['對帳_請款金額_Trust_Account_Fee'].sum()
            existing_ub = df_pivot.loc[df_pivot.index != 'normal', ('total_claimed', '聯邦')].sum()
            df_pivot.loc['normal', ('total_claimed', '聯邦')] = total_ub - existing_ub
            
            # ===============================================================
            # 3. 重新排序
            # ===============================================================
            df_pivot = self.reorder(df_pivot)
            
            # ===============================================================
            # 4. 新增小計行
            # ===============================================================
            df_with_subtotal = self.add_subtotal(df_pivot)
            
            # ===============================================================
            # 5. 驗證
            # ===============================================================
            validation = self.validate(df_with_subtotal, df_escrow)
            
            # ===============================================================
            # 6. 輸出 Excel
            # ===============================================================
            output_path = Path(context.get_variable('output_path', './output/'))
            filename = context.get_variable('trust_account_filename', 
                                            'filing data for Trust Account Fee Accrual-SPETW.xlsx')
            output_file = output_path / filename
            
            output_path.mkdir(parents=True, exist_ok=True)
            
            with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
                df_with_subtotal.to_excel(writer, sheet_name='trust_account_fee')
                df_escrow.to_excel(writer, sheet_name='escrow_inv', index=False)
                context.get_auxiliary_data('invoice_summary').to_excel(
                    writer, sheet_name='invoice_summary', index=False)
                validation.to_excel(writer, sheet_name='trust_account_validation')
                
                # 各銀行分期明細
                for (df, bank) in bank_data:
                    df.to_excel(writer, sheet_name=bank, index=False)
            
            self.logger.info(f"成功輸出: {output_file}")
            
            # ===============================================================
            # 7. 儲存到 Context
            # ===============================================================
            context.add_auxiliary_data('trust_account_fee', df_with_subtotal)
            context.add_auxiliary_data('trust_account_validation', validation)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"成功生成 Trust Account Fee 工作底稿: {filename}",
                metadata={'output_file': str(output_file)}
            )
            
        except Exception as e:
            self.logger.error(f"生成 Trust Account Fee 失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return StepResult(step_name=self.name, status=StepStatus.FAILED, error=e, message=str(e))
    
    def merge_cub(self, context):
        """合併國泰個人和法人"""
        ind = context.get_auxiliary_data('cub_individual_installment')
        nonind = context.get_auxiliary_data('cub_nonindividual_installment')
        
        merged = pd.merge(ind, nonind, on='transaction_type', how='outer', 
                          suffixes=['_ind', '_nonind']).fillna(0)
        merged['total_claimed'] = merged['total_claimed_ind'] + merged['total_claimed_nonind']
        merged['total_service_fee'] = merged['total_service_fee_ind'] + merged['total_service_fee_nonind']
        
        return merged[['transaction_type', 'total_claimed', 'total_service_fee']]
    
    def reorder(self, df):
        """重新排序行和列"""
        # 行順序
        row_order = ['normal', '3期', '6期', '12期', '24期']
        df = df.reindex(row_order)
        
        # 列順序
        bank_order = ['台新', 'NCCC', '國泰', 'CTBC', '聯邦']
        level_0 = df.columns.get_level_values(0).unique()
        
        new_cols = []
        for l0 in level_0:
            for bank in bank_order:
                if (l0, bank) in df.columns:
                    new_cols.append((l0, bank))
        
        return df[new_cols]
    
    def add_subtotal(self, df):
        """新增小計行"""
        subtotal = df.select_dtypes(include=['number']).sum()
        subtotal_df = pd.DataFrame([subtotal], index=['小計'])
        subtotal_df.index.name = df.index.name
        return pd.concat([df, subtotal_df])
    
    def validate(self, df, df_escrow):
        """驗證 Trust Account Fee"""
        # 驗證請款金額
        val_amt = pd.DataFrame(df.T['小計'].iloc[:5])
        val_amt.columns = ['trust_account_fee的小計']
        val_amt['escrow_inv的對帳_請款金額'] = [
            df_escrow.query("銀行.str.contains('taishi')")['對帳_請款金額_當期'].sum(),
            df_escrow.query("銀行.str.contains('nccc')")['對帳_請款金額_Trust_Account_Fee'].sum(),
            df_escrow.query("銀行.str.contains('cub')")['對帳_請款金額_Trust_Account_Fee'].sum(),
            df_escrow.query("銀行.str.contains('ctbc')")['對帳_請款金額_Trust_Account_Fee'].sum(),
            df_escrow.query("銀行.isin(['ub_noninstallment', 'ub_installment'])")['對帳_請款金額_Trust_Account_Fee'].sum()
        ]
        val_amt['diff'] = val_amt.iloc[:, 0] - val_amt.iloc[:, 1]
        
        # 驗證手續費
        val_fee = pd.DataFrame(df.T['小計'].iloc[5:])
        val_fee.columns = ['trust_account_fee的小計手續費']
        val_fee['escrow_inv的手續費'] = [
            df_escrow.query("銀行.str.contains('taishi')")['對帳_手續費_總計'].sum(),
            df_escrow.query("銀行.str.contains('nccc')")['對帳_手續費_總計'].sum(),
            df_escrow.query("銀行.str.contains('cub')")['對帳_手續費_總計'].sum(),
            df_escrow.query("銀行.str.contains('ctbc')")['對帳_手續費_總計'].sum(),
            df_escrow.query("銀行.isin(['ub_noninstallment', 'ub_installment'])")['對帳_手續費_總計'].sum()
        ]
        val_fee['diff'] = val_fee.iloc[:, 0] - val_fee.iloc[:, 1]
        
        return pd.concat([val_amt, val_fee], axis=1)


if __name__ == "__main__":
    print("請在完整 Pipeline 中執行此步驟")
