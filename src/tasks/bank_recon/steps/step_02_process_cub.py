"""
Step 2: 處理國泰世華銀行對帳
完整示範實作
"""

from typing import Dict, Any
import pandas as pd

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger, config_manager, DuckDBManager

from ..models import BankDataContainer
from ..utils import BankProcessor


class CUBProcessor(BankProcessor):
    """
    國泰世華銀行處理器
    
    處理國泰個人和法人兩個類別的對帳資料
    """
    
    def load_data(self, db_manager, beg_date: str, end_date: str) -> pd.DataFrame:
        """
        載入國泰資料
        
        Args:
            db_manager: DuckDB 管理器
            beg_date: 開始日期
            end_date: 結束日期
            
        Returns:
            pd.DataFrame: 載入的資料
        """
        table_name = self.config['table_name']
        
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
        計算對帳金額
        
        國泰的計算邏輯:
        1. 當期請款金額 = 撥款日期在期間內的 request_amount 總和
        2. 退貨金額 = 撥款日期在期間內的 return_amount 總和（取絕對值）
        3. Trust Account Fee = 請款金額 - 退貨金額 - 調整金額
        4. 手續費 = 撥款日期在期間內的 handling_fee 總和
        
        Args:
            data: 原始資料
            beg_date: 當期開始日期
            end_date: 當期結束日期
            last_beg_date: 前期開始日期
            last_end_date: 前期結束日期
            
        Returns:
            Dict: 計算結果
        """
        # 篩選當期資料
        mask = data['disbursement_date'].between(beg_date, end_date)
        current_data = data[mask]
        
        # 計算各項金額
        claimed_amount = int(current_data['request_amount'].sum())
        refunded_amount = abs(int(current_data['return_amount'].sum()))
        service_fee = int(current_data['handling_fee'].sum())
        adj_amount = 0  # 國泰通常無調整
        
        self.logger.info("計算結果:")
        self.logger.info(f"  當期請款: {claimed_amount:,}")
        self.logger.info(f"  退貨金額: {refunded_amount:,}")
        self.logger.info(f"  手續費: {service_fee:,}")
        
        return {
            'category': self.config['category'],
            'recon_amount': claimed_amount,
            'amount_claimed_last_period_paid_by_current': refunded_amount,
            'recon_amount_for_trust_account_fee': claimed_amount - refunded_amount - adj_amount,
            'recon_service_fee': service_fee,
            'service_fee_claimed_last_period_paid_by_current': 0,
            'adj_service_fee': adj_amount,
            'invoice_amount_claimed': claimed_amount - refunded_amount - adj_amount,
            'invoice_service_fee': service_fee
        }


class ProcessCUBStep(PipelineStep):
    """
    處理國泰世華銀行對帳步驟
    
    處理流程:
    1. 從配置讀取國泰設定
    2. 處理個人類別
    3. 處理法人類別  
    4. 將結果存入 Context
    5. 記錄摘要日誌
    """
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        # 從配置檔讀取國泰設定
        self.config = config.get('banks').get('cub')
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行步驟
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 執行結果
        """
        try:
            # 從 Context 取得參數
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            last_beg_date = context.get_variable('last_beg_date')
            last_end_date = context.get_variable('last_end_date')
            db_path = context.get_variable('db_path')
            log_file = context.get_variable('log_file')
            
            self.logger.info(f"處理期間: {beg_date} ~ {end_date}")
            
            containers = []
            
            # 處理個人與法人兩個類別
            for category in self.config['categories']:
                self.logger.info("=" * 60)
                self.logger.info(f"處理國泰 {category}")
                self.logger.info("=" * 60)
                
                # 創建處理器
                processor = CUBProcessor(
                    bank_code='cub',
                    bank_name='國泰世華',
                    config={
                        'table_name': self.config['tables'][category],
                        'category': category
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
                
                containers.append(container)
                
                # 記錄摘要
                self.print_summary(container, category)
            
            # 儲存到 Context
            context.add_auxiliary_data('cub_containers', containers)
            
            # 計算總計
            total_amount = sum(c.recon_amount for c in containers)
            total_fee = sum(c.recon_service_fee for c in containers)
            
            self.logger.info(f"\n{'=' * 60}")
            self.logger.info("國泰總計:")
            self.logger.info(f"  總請款金額: {total_amount:,}")
            self.logger.info(f"  總手續費: {total_fee:,}")
            self.logger.info(f"{'=' * 60}\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"成功處理國泰銀行 {len(containers)} 個類別",
                metadata={
                    'categories_processed': [c.category for c in containers],
                    'total_amount': total_amount,
                    'total_service_fee': total_fee
                }
            )
            
        except Exception as e:
            self.logger.error(f"處理國泰銀行失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def print_summary(self, container: BankDataContainer, category: str):
        """
        列印摘要
        
        Args:
            container: 資料容器
            category: 類別名稱
        """
        self.logger.info(f"\n--- 國泰 {category} 摘要 ---")
        self.logger.info(f"對帳 請款金額(當期): {container.recon_amount:,}")
        self.logger.info(f"對帳 退貨金額: {container.amount_claimed_last_period_paid_by_current:,}")
        self.logger.info(f"對帳 調整金額: {container.adj_service_fee}")
        self.logger.info(f"對帳 請款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}")
        self.logger.info("-" * 20)
        self.logger.info(f"對帳 手續費(當期): {container.recon_service_fee:,}")
        self.logger.info(f"對帳 手續費(前期): {container.service_fee_claimed_last_period_paid_by_current:,}")
        
        total_service_fee = (
            container.recon_service_fee +
            container.service_fee_claimed_last_period_paid_by_current
        )
        self.logger.info(f"對帳 手續費(前期+當期): {total_service_fee:,}")
        self.logger.info("-" * 20)
        self.logger.info(f"發票 請款金額: {container.invoice_amount_claimed:,}")
        self.logger.info(f"發票 手續費: {container.invoice_service_fee:,}\n")


# =============================================================================
# 使用範例
# =============================================================================

if __name__ == "__main__":
    """
    獨立測試此步驟
    """
    from src.core.pipeline.context import ProcessingContext
    
    # 創建測試 Context
    context = ProcessingContext(
        task_name="test_cub",
        task_type="transform"
    )
    
    # 設定參數
    context.set_variable('beg_date', '2025-10-01')
    context.set_variable('end_date', '2025-10-31')
    context.set_variable('last_beg_date', '2025-09-01')
    context.set_variable('last_end_date', '2025-09-30')
    context.set_variable('db_path', './db/bank_statements.duckdb')
    context.set_variable('log_file', './logs/duckdb_operations.log')
    
    # 執行步驟
    step = ProcessCUBStep(
        name="Process_CUB",
        description="處理國泰世華銀行對帳"
    )
    
    result = step(context)
    
    # 檢查結果
    print(f"\n執行結果: {result.status.value}")
    print(f"訊息: {result.message}")
    print(f"耗時: {result.duration:.2f} 秒")
    
    if result.is_success:
        # 取得處理結果
        containers = context.get_auxiliary_data('cub_containers')
        print(f"\n處理了 {len(containers)} 個類別:")
        for container in containers:
            print(f"  - {container.category}: {container.recon_amount:,}")
