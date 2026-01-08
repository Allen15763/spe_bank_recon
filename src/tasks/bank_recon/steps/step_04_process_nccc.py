"""
Step 4: 處理 NCCC 銀行對帳

重構後使用 BaseBankProcessStep 基類，大幅簡化代碼。
"""

from typing import Dict, Any
import pandas as pd

from ..utils import BankProcessor
from .base_bank_step import BaseBankProcessStep


class NCCCProcessor(BankProcessor):
    """
    NCCC 銀行處理器
    
    關鍵差異:
    1. 使用 nccc_recon_statement（不是 payment）
    2. 當期請款金額 = 撥款日期在期間內的 request_amount
    3. 前期發票當期撥款 = 撥款日期在期間內但請款日期不在期間內
    4. 手續費(前期) = 請款日期在前期且撥款日期在當期
    """
    
    def load_data(self, db_manager, beg_date: str, end_date: str) -> pd.DataFrame:
        """載入 NCCC 資料"""
        table_name = self.config.get('table_name', 'nccc_recon_statement')
        
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
        計算 NCCC 對帳金額
        
        NCCC 計算邏輯:
        1. 當期請款金額 = 撥款日期在期間內的 request_amount
        2. 前期發票當期撥款 = 撥款在當期 BUT 請款不在當期
        3. 當期手續費 = 撥款日期在期間內的 handling_fee
        4. 前期手續費 = 請款在前期且撥款在當期
        5. 發票手續費 = 請款日期在期間內的 handling_fee
        """
        # 建立 Mask
        mask_current_payout = data['disbursement_date'].between(beg_date, end_date)
        mask_current_request = data['request_date'].between(beg_date, end_date)
        mask_last_payout = data['disbursement_date'].between(last_beg_date, last_end_date)
        mask_last_request = data['request_date'].between(last_beg_date, last_end_date)
        
        # 1. 當期請款金額（撥款在期間內）
        claimed_amount = int(data[mask_current_payout]['request_amount'].sum())
        
        # 2. 前期發票當期撥款（撥款在當期 BUT 請款不在當期）
        previous_claimed = int(
            data[(~mask_current_request) & mask_current_payout]['request_amount'].sum()
        )
        
        # 3. 當期手續費（撥款在期間內）
        service_fee_amount = int(data[mask_current_payout]['handling_fee'].sum())
        
        # 4. 前期手續費（請款在前期且撥款在當期）
        previous_service_fee = int(
            data[mask_last_request & mask_current_payout]['handling_fee'].sum()
        )
        
        # 5. 發票手續費（請款日期在期間內）
        invoice_service_fee = int(data[mask_current_request]['handling_fee'].sum())
        
        # 6. 發票請款金額（請款日期在期間內）
        invoice_amount = int(data[mask_current_request]['request_amount'].sum())
        
        self.logger.info("計算結果:")
        self.logger.info(f"  當期請款金額: {claimed_amount:,}")
        self.logger.info(f"  前期發票當期撥款: {previous_claimed:,}")
        self.logger.info(f"  Trust Account Fee: {claimed_amount:,}")
        self.logger.info(f"  當期手續費: {service_fee_amount:,}")
        self.logger.info(f"  前期手續費: {previous_service_fee:,}")
        self.logger.info(f"  發票手續費: {invoice_service_fee:,}")
        
        return {
            'category': 'recon',
            'recon_amount': claimed_amount,
            'amount_claimed_last_period_paid_by_current': previous_claimed,
            'recon_amount_for_trust_account_fee': claimed_amount,
            'recon_service_fee': service_fee_amount,
            'service_fee_claimed_last_period_paid_by_current': previous_service_fee,
            'adj_service_fee': 0,
            'invoice_amount_claimed': invoice_amount,
            'invoice_service_fee': invoice_service_fee
        }


class ProcessNCCCStep(BaseBankProcessStep):
    """
    處理 NCCC 銀行對帳步驟

    重構後的簡化版本，僅需實現兩個抽象方法。
    所有共同邏輯由 BaseBankProcessStep 處理。
    """

    def get_bank_code(self) -> str:
        """返回銀行代碼"""
        return 'nccc'

    def get_processor_class(self):
        """返回對應的 Processor 類"""
        return NCCCProcessor


if __name__ == "__main__":
    from src.core.pipeline.context import ProcessingContext
    from src.utils.config import ConfigManager

    # 載入配置
    config = ConfigManager().config

    context = ProcessingContext(task_name="test_nccc", task_type="transform")
    context.set_variable('beg_date', '2025-10-01')
    context.set_variable('end_date', '2025-10-31')
    context.set_variable('last_beg_date', '2025-09-01')
    context.set_variable('last_end_date', '2025-09-30')
    context.set_variable('db_path', './db/bank_statements.duckdb')
    context.set_variable('log_file', './logs/duckdb_operations.log')

    step = ProcessNCCCStep(
        name="Process_NCCC",
        description="處理 NCCC 銀行對帳",
        config=config
    )
    result = step(context)
    
    print(f"\n執行結果: {result.status.value}")
    print(f"訊息: {result.message}")
    
    if result.is_success:
        container = context.get_auxiliary_data('nccc_container')
        print(f"請款金額: {container.recon_amount:,}")
