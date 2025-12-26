"""
Step 7: 匯總 Escrow 對帳資料
收集所有銀行 Container 並生成 Escrow Invoice Excel
"""

from typing import List, Tuple
import pandas as pd
import numpy as np
from pathlib import Path

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger, config_manager, DuckDBManager

from ..models import BankDataContainer
from ..utils import create_summary_dataframe, reorder_bank_summary


class AggregateEscrowStep(PipelineStep):
    """
    匯總 Escrow 對帳資料步驟
    
    功能:
    1. 收集所有銀行的 Container
    2. 建立摘要 DataFrame
    3. 讀取發票資料並映射
    4. 驗證金額
    5. 輸出 Escrow Excel 檔案
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("AggregateEscrowStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始匯總 Escrow 對帳資料")
            self.logger.info("=" * 60)
            
            # ===================================================================
            # 1. 收集所有銀行 Container
            # ===================================================================
            all_containers = []
            containers_and_names = []
            
            # 國泰 (2個類別)
            cub_containers = context.get_auxiliary_data('cub_containers')
            for container in cub_containers:
                all_containers.append(container)
                name = f"cub_{container.category}"
                containers_and_names.append((container, name))
            
            # 中信 (2個類別)
            ctbc_containers = context.get_auxiliary_data('ctbc_containers')
            for container in ctbc_containers:
                all_containers.append(container)
                name = f"ctbc_{container.category}"
                containers_and_names.append((container, name))
            
            # NCCC
            nccc_container = context.get_auxiliary_data('nccc_container')
            all_containers.append(nccc_container)
            containers_and_names.append((nccc_container, 'nccc'))
            
            # 聯邦 (2個類別)
            ub_containers = context.get_auxiliary_data('ub_containers')
            for container in ub_containers:
                all_containers.append(container)
                name = f"ub_{container.category}"
                containers_and_names.append((container, name))
            
            # 台新
            taishi_container = context.get_auxiliary_data('taishi_container')
            all_containers.append(taishi_container)
            containers_and_names.append((taishi_container, 'taishi'))
            
            self.logger.info(f"收集了 {len(all_containers)} 個銀行類別的資料")
            
            # ===================================================================
            # 2. 建立摘要 DataFrame
            # ===================================================================
            df_summary = create_summary_dataframe(containers_and_names)
            self.logger.info(f"建立摘要 DataFrame: {len(df_summary)} 行")
            
            # ===================================================================
            # 3. 讀取發票資料
            # ===================================================================
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            db_path = context.get_variable('db_path')
            log_file = context.get_variable('log_file')
            
            with DuckDBManager(db_path=db_path, log_file=log_file, log_level="DEBUG") as db_manager:
                query = """
                SELECT * FROM invoice_details
                """
                df_invoice = db_manager.query_to_df(query)
            
            self.logger.info(f"讀取發票資料: {len(df_invoice)} 筆")
            
            # 處理發票日期
            df_invoice['invoice_date'] = pd.to_datetime(
                df_invoice['invoice_date'].str.replace('-', ''), 
                format='%Y%m%d'
            )
            df_invoice['is_handling_fee'] = df_invoice['invoice_description'].str.contains("手續費", na=False)
            
            # 篩選當期發票
            mask = df_invoice['invoice_date'].between(beg_date, end_date)
            
            # 發票摘要
            invoice_summary = df_invoice[mask].groupby(['category', 'is_handling_fee'], as_index=False).agg(
                total_amount_excl_tax=pd.NamedAgg(column='amount_excl_tax', aggfunc='sum'),
                total_tax_amount=pd.NamedAgg(column='tax_amount', aggfunc='sum'),
                total_amount_incl_tax=pd.NamedAgg(column='amount_incl_tax', aggfunc='sum'),
            )
            
            self.logger.info(f"發票摘要: {len(invoice_summary)} 筆")
            
            # ===================================================================
            # 4. 映射 UB 發票手續費
            # ===================================================================
            # 從 UB 發票映射回 UB 的"發票_手續費"欄位
            try:
                ub_noninstall_fee = invoice_summary.query(
                    "category=='ub_noninstallment' and is_handling_fee==True"
                )['total_amount_incl_tax'].values[0]
                df_summary.loc[df_summary['銀行'] == 'ub_noninstallment', '發票_手續費'] = ub_noninstall_fee
                
                ub_install_fee = invoice_summary.query(
                    "category=='ub_installment' and is_handling_fee==True"
                )['total_amount_incl_tax'].values[0]
                df_summary.loc[df_summary['銀行'] == 'ub_installment', '發票_手續費'] = ub_install_fee
                
                self.logger.info("成功映射 UB 發票手續費")
            except Exception as e:
                self.logger.warning(f"映射 UB 發票手續費失敗: {str(e)}")
            
            # ===================================================================
            # 5. 合併發票稅額資料
            # ===================================================================
            df_summary = df_summary.merge(
                invoice_summary.loc[
                    invoice_summary['is_handling_fee'] == True, 
                    ['category', 'total_tax_amount', 'total_amount_incl_tax']
                ],
                left_on='銀行',
                right_on='category',
                how='left'
            ).drop(columns=['category'])
            
            # ===================================================================
            # 6. 驗證發票金額
            # ===================================================================
            df_summary['check_invoice_amt\n發票_手續費(對帳單) - total_amount_incl_tax(AI)'] = np.where(
                df_summary['銀行'] != 'taishi', 
                df_summary['發票_手續費'] - df_summary['total_amount_incl_tax'],
                df_summary['對帳_手續費_當期'] - df_summary['total_amount_incl_tax']
            )
            
            # ===================================================================
            # 7. 調整欄位順序
            # ===================================================================
            # 把發票類的放到後面
            for col in ['發票_請款金額', '發票_手續費']:
                if col in df_summary.columns:
                    a = df_summary.pop(col)
                    df_summary.insert(
                        df_summary.columns.get_loc('total_tax_amount'), 
                        col, 
                        a
                    )
            
            # ===================================================================
            # 8. 排序銀行
            # ===================================================================
            bank_order = context.get_variable('escrow_bank_order', [
                'taishi',
                'nccc',
                'cub_nonindividual',
                'cub_individual',
                'ctbc_noninstallment',
                'ctbc_installment',
                'ub_noninstallment',
                'ub_installment',
            ])
            
            df_summary = reorder_bank_summary(df_summary, bank_order)
            
            # 添加銀行代碼欄位
            df_summary['bank'] = df_summary['銀行'].str.split('_').str[0]
            
            # ===================================================================
            # 9. 輸出 Excel
            # ===================================================================
            output_path = Path(context.get_variable('output_path', './output/'))
            escrow_filename = context.get_variable('escrow_filename', 'Escrow_recon_renew.xlsx')
            output_file = output_path / escrow_filename
            
            # 確保輸出目錄存在
            output_path.mkdir(parents=True, exist_ok=True)
            
            with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
                # Sheet 1: 摘要
                df_summary.to_excel(writer, sheet_name='summary', index=False)
                
                # Sheet 2: 發票摘要
                invoice_summary.to_excel(writer, sheet_name='invoice_summary', index=False)
                
                # Sheet 3: 發票明細
                df_invoice.to_excel(writer, sheet_name='invoice_details', index=False)
            
            self.logger.info(f"成功輸出 Escrow Excel: {output_file}")
            
            # ===================================================================
            # 10. 儲存到 Context
            # ===================================================================
            context.add_auxiliary_data('df_summary_escrow_inv', df_summary)
            context.add_auxiliary_data('invoice_summary', invoice_summary)
            context.add_auxiliary_data('df_invoice', df_invoice)
            
            # ===================================================================
            # 11. 記錄統計資訊
            # ===================================================================
            total_recon_amount = df_summary['對帳_請款金額_Trust_Account_Fee'].sum()
            total_service_fee = df_summary['對帳_手續費_總計'].sum() if '對帳_手續費_總計' in df_summary.columns else 0
            
            self.logger.info(f"\n{'=' * 60}")
            self.logger.info("Escrow 匯總統計:")
            self.logger.info(f"  總 Trust Account Fee: {total_recon_amount:,}")
            self.logger.info(f"  總手續費: {total_service_fee:,}")
            self.logger.info(f"  輸出檔案: {output_file}")
            self.logger.info(f"{'=' * 60}\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"成功匯總 Escrow 資料並輸出至 {escrow_filename}",
                metadata={
                    'total_banks': len(all_containers),
                    'total_recon_amount': float(total_recon_amount),
                    'total_service_fee': float(total_service_fee),
                    'output_file': str(output_file)
                }
            )
            
        except Exception as e:
            self.logger.error(f"匯總 Escrow 資料失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )


if __name__ == "__main__":
    from src.core.pipeline.context import ProcessingContext
    
    # 注意: 此步驟需要前面的步驟先執行完成
    print("此步驟需要前面的步驟先執行，無法獨立測試")
    print("請在完整 Pipeline 中執行")
