"""
Step 6: 處理台新銀行對帳
"""

from typing import Dict, Any
import pandas as pd

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger, config_manager, DuckDBManager

from ..models import BankDataContainer
from ..utils import BankProcessor


class TaishiProcessor(BankProcessor):
    """
    台新銀行處理器
    
    關鍵特色:
    1. 使用 disbursement_amount 作為 Trust Account Fee（不是 request_amount）
    2. 需要扣除 TSPG System Service Fees（disbursement_amount==0 的部分）
    3. 有稅額調整
    4. 手續費用 invoice_amount（不是 handling_fee）
    """
    
    def load_data(self, db_manager, beg_date: str, end_date: str) -> pd.DataFrame:
        """載入台新資料"""
        table_name = self.config.get('table_name', 'taishi_recon_statement')
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE disbursement_date IS NOT NULL
        """
        
        data = db_manager.query_to_df(query)
        self.logger.info(f"載入 {table_name} 資料: {len(data)} 筆")
        
        return data
    
    def calculate_recon_amounts(
        self, 
        data: pd.DataFrame,
        beg_date: str,
        end_date: str,
        last_beg_date: str,
        last_end_date: str
    ) -> Dict[str, Any]:
        """
        計算台新對帳金額
        
        台新計算邏輯:
        1. 當期請款金額 = 撥款日期在期間內的 request_amount 總和
        2. Trust Account Fee = 撥款日期在期間內的 disbursement_amount 總和
        3. 手續費 = invoice_amount 總和（扣除 disbursement_amount==0 的部分）
        4. 調整金額 = disbursement_amount==0 的 invoice_amount（TSPG System Service Fees）
        5. 稅額調整 = disbursement_amount==0 的 tax_amount
        """
        # 建立 Mask
        mask_current_payout = pd.to_datetime(data['disbursement_date']).between(beg_date, end_date)
        
        # 篩選當期資料
        df = data[mask_current_payout]
        
        # 1. 當期請款金額
        recon_amount = int(df['request_amount'].sum())
        
        # 2. Trust Account Fee（使用 disbursement_amount）
        recon_claimed_amount = int(df['disbursement_amount'].sum())
        
        # 3. 手續費（invoice_amount 總和）
        service_fee_amount = int(df['invoice_amount'].sum())
        
        # 4. 調整金額（需扣除的 TSPG System Service Fees）
        adj_amt = int(df.query("disbursement_amount==0")['invoice_amount'].sum())
        
        # 5. 稅額調整
        adj_tax_amt = int(df.query("disbursement_amount==0")['tax_amount'].sum())
        
        # 6. 實際手續費（扣除調整後）
        actual_service_fee = service_fee_amount - adj_amt
        
        self.logger.info("計算結果:")
        self.logger.info(f"  當期請款金額: {recon_amount:,}")
        self.logger.info(f"  請/付款金額(Trust Account Fee): {recon_claimed_amount:,}")
        self.logger.info(f"  手續費(原始): {service_fee_amount:,}")
        self.logger.info(f"  TSPG System Service Fees: {adj_amt:,}")
        self.logger.info(f"  手續費(實際): {actual_service_fee:,}")
        self.logger.info(f"  稅額調整: {adj_tax_amt:,}")
        
        return {
            'category': 'default',
            'recon_amount': recon_amount,
            'amount_claimed_last_period_paid_by_current': 0,
            'recon_amount_for_trust_account_fee': recon_claimed_amount,
            'recon_service_fee': actual_service_fee,
            'service_fee_claimed_last_period_paid_by_current': adj_tax_amt,  # 暫代稅額調整
            'adj_service_fee': adj_amt,  # 暫代發票額調整
            'invoice_amount_claimed': actual_service_fee,
            'invoice_service_fee': None
        }


class ProcessTaishiStep(PipelineStep):
    """處理台新銀行對帳步驟"""
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config.get('banks', {}).get('taishi')
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            # 取得參數
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            last_beg_date = context.get_variable('last_beg_date')
            last_end_date = context.get_variable('last_end_date')
            db_path = context.get_variable('db_path')
            log_file = context.get_variable('log_file')
            
            self.logger.info("=" * 60)
            self.logger.info("處理台新銀行")
            self.logger.info("=" * 60)
            
            # 創建處理器
            processor = TaishiProcessor(
                bank_code='taishi',
                bank_name='台新',
                config={
                    'table_name': self.config['tables']['recon']
                }
            )
            
            # 處理資料
            with DuckDBManager(db_path=db_path) as db_manager:
                container = processor.process(
                    db_manager=db_manager,
                    beg_date=beg_date,
                    end_date=end_date,
                    last_beg_date=last_beg_date,
                    last_end_date=last_end_date
                )
            
            # 記錄摘要
            self.print_summary(container)
            
            # 儲存到 Context
            context.add_auxiliary_data('taishi_container', container)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="成功處理台新銀行",
                metadata={
                    'recon_amount': container.recon_amount,
                    'trust_account_fee': container.recon_amount_for_trust_account_fee,
                    'service_fee': container.recon_service_fee
                }
            )
            
        except Exception as e:
            self.logger.error(f"處理台新銀行失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def print_summary(self, container: BankDataContainer):
        """列印摘要"""
        self.logger.info("\n--- 台新 摘要 ---")
        self.logger.info(f"對帳 請款金額(當期): {container.recon_amount:,}")
        self.logger.info(f"對帳 手續費(當期): {container.recon_service_fee:,}")
        self.logger.info(f"對帳 請/付款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}")
        self.logger.info("-" * 20)
        self.logger.info(f"對帳 TSPG System Service Fees: {container.adj_service_fee:,}")
        self.logger.info(f"對帳 稅額調整: {container.service_fee_claimed_last_period_paid_by_current:,}\n")


if __name__ == "__main__":
    from src.core.pipeline.context import ProcessingContext
    
    context = ProcessingContext(task_name="test_taishi", task_type="transform")
    context.set_variable('beg_date', '2025-10-01')
    context.set_variable('end_date', '2025-10-31')
    context.set_variable('last_beg_date', '2025-09-01')
    context.set_variable('last_end_date', '2025-09-30')
    context.set_variable('db_path', './db/bank_statements.duckdb')
    context.set_variable('log_file', './logs/duckdb_operations.log')
    
    step = ProcessTaishiStep(name="Process_Taishi", description="處理台新銀行對帳")
    result = step(context)
    
    print(f"\n執行結果: {result.status.value}")
    if result.is_success:
        container = context.get_auxiliary_data('taishi_container')
        print(f"請款金額: {container.recon_amount:,}")
        print(f"Trust Account Fee: {container.recon_amount_for_trust_account_fee:,}")
