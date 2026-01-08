"""
Step 3: 處理中國信託銀行對帳

重構後使用 BaseBankProcessStep 基類，大幅簡化代碼。
"""

from typing import Dict, Any
import pandas as pd

from ..utils import BankProcessor
from .base_bank_step import BaseBankProcessStep


class CTBCProcessor(BankProcessor):
    """
    中國信託銀行處理器
    
    處理中信分期和非分期兩個類別的對帳資料
    
    關鍵邏輯:
    1. 當期請款金額 = 當月請款 AND 當月撥款的 request_amount
    2. 前期發票當期撥款 = 前期請款 BUT 非前期撥款的 request_amount
    3. 調整金額 = adjustment_amount
    4. 手續費 = handling_fee + adjustment_handling_fee
    """
    
    def load_data(self, db_manager, beg_date: str, end_date: str) -> pd.DataFrame:
        """
        載入中信資料
        
        Args:
            db_manager: DuckDB 管理器
            beg_date: 開始日期（不使用，查詢全表）
            end_date: 結束日期（不使用，查詢全表）
            
        Returns:
            pd.DataFrame: 載入的資料
        """
        table_name = self.config['table_name']
        
        query = f"""
        SELECT * FROM {table_name}
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
        計算中信對帳金額
        
        中信的計算邏輯:
        1. 當期請款金額 = 請款日期和撥款日期都在當期的 request_amount 總和
        2. 前期發票當期撥款 = 請款日期在前期 BUT 撥款日期不在前期的 request_amount
        3. Trust Account Fee = 當期請款 + 前期發票當期撥款 + 調整金額
        4. 當期手續費 = 當月請款且當月撥款的 handling_fee + adjustment_handling_fee
        5. 前期手續費 = 前期請款但非前期撥款的 handling_fee
        6. 發票金額 = 當月請款的 invoice_amount 總和
        
        Args:
            data: 原始資料
            beg_date: 當期開始日期
            end_date: 當期結束日期
            last_beg_date: 前期開始日期
            last_end_date: 前期結束日期
            
        Returns:
            Dict: 計算結果
        """
        # 計算當期月份字串
        current_month = beg_date.replace('-', '')[:6]
        
        # ===================================================================
        # 建立 Mask (布林遮罩)
        # ===================================================================
        
        # 當月撥款
        mask_current_month_payout = (
            data['disbursement_date'].dt.strftime('%Y%m').str.contains(current_month)
        )
        
        # 當月請款
        mask_current_month_request = (
            data['request_date'].dt.strftime('%Y%m').str.contains(current_month)
        )
        
        # 前期請款
        mask_last_period_request = (
            data['request_date'].between(last_beg_date, last_end_date)
        )
        
        # 前期撥款
        mask_last_period_payout = (
            data['disbursement_date'].between(last_beg_date, last_end_date)
        )
        
        # ===================================================================
        # 計算各項金額
        # ===================================================================
        
        # 1. 當期請款金額 (當月請款 AND 當月撥款)
        current_period_recon_amount = int(
            data.loc[
                (mask_current_month_payout & mask_current_month_request),
                'request_amount'
            ].sum()
        )
        
        # 2. 前期發票當期撥款 (前期請款 BUT 非前期撥款)
        previous_claimed = int(
            data.loc[
                (mask_last_period_request & ~mask_last_period_payout),
                'request_amount'
            ].sum()
        )
        
        # 3. 調整金額 (當月請款且當月撥款)
        adjustment_amount = int(
            data.loc[
                (mask_current_month_payout & mask_current_month_request),
                'adjustment_amount'
            ].sum()
        )
        
        # 4. Trust Account Fee = 當期 + 前期 + 調整
        trust_account_fee_amount = (
            current_period_recon_amount + 
            previous_claimed + 
            adjustment_amount
        )
        
        # 5. 當期手續費 (當月撥款且當月請款)
        current_period_service_fee = int(
            data.loc[
                (mask_current_month_payout & mask_current_month_request),
                'handling_fee'
            ].sum()
        )
        
        # 6. 調整手續費
        adjustment_handling_fee = int(
            data.loc[
                (mask_current_month_payout & mask_current_month_request),
                'adjustment_handling_fee'
            ].sum()
        )
        
        # 7. 前期手續費 (前期請款但非前期撥款)
        previous_claimed_service_fee = int(
            data.loc[
                (mask_last_period_request & ~mask_last_period_payout),
                'handling_fee'
            ].sum()
        )
        
        # 8. 發票金額 (當月請款)
        invoice_amount = int(
            data.loc[mask_current_month_request, 'invoice_amount'].sum()
        )
        
        # 9. 發票請款金額 (當月請款的 request_amount)
        invoice_request_amount = int(
            data.loc[mask_current_month_request, 'request_amount'].sum()
        )
        
        # ===================================================================
        # 記錄日誌
        # ===================================================================
        self.logger.info("計算結果:")
        self.logger.info(f"  當期請款金額: {current_period_recon_amount:,}")
        self.logger.info(f"  前期發票當期撥款: {previous_claimed:,}")
        self.logger.info(f"  調整金額: {adjustment_amount:,}")
        self.logger.info(f"  Trust Account Fee: {trust_account_fee_amount:,}")
        self.logger.info(f"  當期手續費: {current_period_service_fee:,}")
        self.logger.info(f"  調整手續費: {adjustment_handling_fee:,}")
        self.logger.info(f"  前期手續費: {previous_claimed_service_fee:,}")
        self.logger.info(f"  發票金額: {invoice_amount:,}")
        
        # ===================================================================
        # 返回結果
        # ===================================================================
        return {
            'category': self.config['category'],
            'recon_amount': current_period_recon_amount,
            'amount_claimed_last_period_paid_by_current': previous_claimed,
            'recon_amount_for_trust_account_fee': trust_account_fee_amount,
            'recon_service_fee': current_period_service_fee + adjustment_handling_fee,
            'service_fee_claimed_last_period_paid_by_current': previous_claimed_service_fee,
            'adj_service_fee': adjustment_amount,
            'invoice_amount_claimed': invoice_request_amount,
            'invoice_service_fee': invoice_amount
        }


class ProcessCTBCStep(BaseBankProcessStep):
    """
    處理中國信託銀行對帳步驟

    重構後的簡化版本，僅需實現兩個抽象方法。
    所有共同邏輯由 BaseBankProcessStep 處理。
    """

    def get_bank_code(self) -> str:
        """返回銀行代碼"""
        return 'ctbc'

    def get_processor_class(self):
        """返回對應的 Processor 類"""
        return CTBCProcessor


# =============================================================================
# 使用範例
# =============================================================================

if __name__ == "__main__":
    """
    獨立測試此步驟
    """
    from src.core.pipeline.context import ProcessingContext
    from src.utils.config import ConfigManager

    # 載入配置
    config = ConfigManager().config

    # 創建測試 Context
    context = ProcessingContext(
        task_name="test_ctbc",
        task_type="transform"
    )

    # 設定參數
    context.set_variable('beg_date', '2025-10-01')
    context.set_variable('end_date', '2025-10-31')
    context.set_variable('last_beg_date', '2025-09-01')
    context.set_variable('last_end_date', '2025-09-30')
    context.set_variable('db_path', './db/bank_statements.duckdb')
    context.set_variable('log_file', './logs/duckdb_operations.log')

    # 執行步驟 (使用新的簡化版本)
    step = ProcessCTBCStep(
        name="Process_CTBC",
        description="處理中國信託銀行對帳",
        config=config
    )

    result = step(context)

    # 檢查結果
    print(f"\n執行結果: {result.status.value}")
    print(f"訊息: {result.message}")
    print(f"耗時: {result.duration:.2f} 秒")

    if result.is_success:
        # 取得處理結果
        containers = context.get_auxiliary_data('ctbc_containers')
        print(f"\n處理了 {len(containers)} 個類別:")
        for container in containers:
            print(f"  - {container.category}:")
            print(f"    請款金額: {container.recon_amount:,}")
            print(f"    Trust Account Fee: {container.recon_amount_for_trust_account_fee:,}")
            print(f"    手續費: {container.recon_service_fee:,}")
