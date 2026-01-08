"""
Step 5: 處理聯邦銀行對帳
包含分期與非分期兩個類別，特殊聚合邏輯
"""

from typing import Dict, Any
import pandas as pd

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger, config_manager, DuckDBManager

from ..models import BankDataContainer
from ..utils import BankProcessor


class UBProcessor(BankProcessor):
    """
    聯邦銀行處理器
    
    關鍵特色:
    1. 需要聚合特定商店（他行卡、自行卡、自行卡分期內含）
    2. 手續費的計算較複雜，需減去調整
    3. 發票請款金額取最後一筆記錄
    """
    
    # 常數定義
    RECON_AGG_TARGET_STORES = ['他行卡', '自行卡', '自行卡分期內含']
    MAIN_COMPANY_NAME = '樂購蝦皮股份有限公司'
    ADJUSTMENT_ROW_IDENTIFIER = '調整 -- 退貨'
    
    def load_data(self, db_manager, beg_date: str, end_date: str) -> pd.DataFrame:
        """載入聯邦資料"""
        table_name = self.config['table_name']
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE disbursement_date IS NOT NULL
        AND request_date IS NOT NULL
        """
        
        data = db_manager.query_to_df(query)
        self.logger.info(f"載入 {table_name} 資料: {len(data)} 筆")
        
        return data
    
    def get_aggregate_recon_df(self, df: pd.DataFrame, current_month: str) -> pd.DataFrame:
        """
        對對帳單資料進行篩選和匯總
        
        聚合目標商店: 他行卡、自行卡、自行卡分期內含
        """
        mask_current_month = df['disbursement_date'].str.contains(current_month, na=False)
        mask_type = df['store_name'].isin(self.RECON_AGG_TARGET_STORES)
        
        # 篩選出當月且符合商店類型的資料
        filtered_df = df.loc[mask_current_month & mask_type]
        
        # 進行分組與聚合
        df_agg = filtered_df.groupby('store_name', as_index=False).agg(
            total_claimed=pd.NamedAgg(column='request_amount', aggfunc='sum'),
            total_service_fee=pd.NamedAgg('handling_fee', 'sum'),
            total_ub_service_fee=pd.NamedAgg('local_handling_fee', 'sum'),
            total_paid=pd.NamedAgg('disbursement_amount', 'sum'),
        )
        
        self.logger.info("聚合後資料:")
        for _, row in df_agg.iterrows():
            self.logger.info(f"  {row['store_name']}: {row['total_claimed']:,}")
        
        return df_agg
    
    def calculate_recon_amounts(
        self, 
        data: pd.DataFrame,
        beg_date: str,
        end_date: str,
        last_beg_date: str,
        last_end_date: str
    ) -> Dict[str, Any]:
        """
        計算聯邦對帳金額
        
        聯邦計算邏輯:
        1. 當期請款金額 = 請款且撥款都在當期 AND 商店名稱包含"行卡"
        2. 前期發票當期撥款 = 撥款在當期 AND 請款在前期
        3. Trust Account Fee = 聚合資料的總請款金額
        4. 手續費 = 商店名稱為主公司且撥款在當期
        5. 調整手續費 = 調整行且請款在當期
        6. 發票請款金額 = 最後一筆記錄的請款金額
        """
        # 計算期間字串
        current_month = beg_date.replace('-', '')[:6]
        
        # 建立 Mask
        mask_current_payout = data['disbursement_date'].between(
            beg_date.replace('-', ''), 
            end_date.replace('-', '')
        )
        mask_current_request = data['request_date'].between(
            beg_date.replace('-', ''), 
            end_date.replace('-', '')
        )
        mask_last_payout = data['disbursement_date'].between(
            last_beg_date.replace('-', ''), 
            last_end_date.replace('-', '')
        )
        mask_last_request = data['request_date'].between(
            last_beg_date.replace('-', ''), 
            last_end_date.replace('-', '')
        )
        
        # 1. 聚合資料（Trust Account Fee）
        agg_df = self.get_aggregate_recon_df(data, current_month)
        trust_account_fee_amount = int(agg_df['total_claimed'].sum())
        
        # 2. 當期請款金額（請款且撥款都在當期 AND 店名包含"行卡"）
        current_period_recon_amount = int(
            data[
                (mask_current_request & mask_current_payout) & 
                data['store_name'].str.contains('行卡', na=False)
            ]['request_amount'].sum()
        )
        
        # 3. 前期發票當期撥款
        previous_claimed = int(
            data[(mask_current_payout & mask_last_request)]['request_amount'].sum()
        )
        
        # 驗證
        validate_1 = current_period_recon_amount + previous_claimed - trust_account_fee_amount
        if validate_1 != 0:
            self.logger.warning(f"驗證失敗: 差異 {validate_1}")
        else:
            self.logger.info(f"驗證通過: 差異 {validate_1}")
        
        # 4. 前期手續費
        previous_service_fee = int(
            data[(mask_current_payout & mask_last_request)]['handling_fee'].sum()
        )
        
        # 5. 當期手續費（商店名稱為主公司且撥款在當期）
        current_period_service_fee = int(
            data[
                mask_current_payout & 
                (data['store_name'] == self.MAIN_COMPANY_NAME)
            ]['handling_fee'].sum()
        )
        
        # 6. 調整手續費
        adjustment_service_fee = int(
            data[
                (data['store_name'] == self.ADJUSTMENT_ROW_IDENTIFIER) & 
                mask_current_request
            ]['handling_fee'].sum()
        )
        
        # 7. 發票請款金額（取最後一筆記錄）
        invoice_amount = int(data.at[data.index[-1], 'request_amount'])
        
        self.logger.info("計算結果:")
        self.logger.info(f"  當期請款金額: {current_period_recon_amount:,}")
        self.logger.info(f"  前期發票當期撥款: {previous_claimed:,}")
        self.logger.info(f"  Trust Account Fee: {trust_account_fee_amount:,}")
        self.logger.info(f"  當期手續費: {current_period_service_fee:,}")
        self.logger.info(f"  前期手續費: {previous_service_fee:,}")
        self.logger.info(f"  調整手續費: {adjustment_service_fee:,}")
        
        return {
            'category': self.config['category'],
            'aggregated_data': agg_df,
            'recon_amount': current_period_recon_amount,
            'amount_claimed_last_period_paid_by_current': previous_claimed,
            'recon_amount_for_trust_account_fee': trust_account_fee_amount,
            'recon_service_fee': current_period_service_fee,
            'service_fee_claimed_last_period_paid_by_current': previous_service_fee,
            'adj_service_fee': adjustment_service_fee,
            'invoice_amount_claimed': invoice_amount,
            'invoice_service_fee': 0  # 對帳單明細無法篩選，需從發票檔擷取
        }


class ProcessUBStep(PipelineStep):
    """處理聯邦銀行對帳步驟"""
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config.get('banks', {}).get('ub')
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            # 取得參數
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            last_beg_date = context.get_variable('last_beg_date')
            last_end_date = context.get_variable('last_end_date')
            db_path = context.get_variable('db_path')
            log_file = context.get_variable('log_file')
            
            self.logger.info(f"處理期間: {beg_date} ~ {end_date}")
            
            containers = []
            
            # 處理非分期與分期兩個類別
            for category in self.config['categories']:
                self.logger.info("=" * 60)
                self.logger.info(f"處理聯邦 {category}")
                self.logger.info("=" * 60)
                
                # 創建處理器
                processor = UBProcessor(
                    bank_code='ub',
                    bank_name='聯邦',
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
                
                # 特別處理: 設定 aggregated_data
                if 'aggregated_data' in processor.calculate_recon_amounts.__code__.co_names:
                    amounts = processor.calculate_recon_amounts(
                        container.raw_data, beg_date, end_date, last_beg_date, last_end_date
                    )
                    container.aggregated_data = amounts.get('aggregated_data')
                
                containers.append(container)
                
                # 記錄摘要
                self.print_summary(container, category)
            
            # 儲存到 Context
            context.add_auxiliary_data('ub_containers', containers)
            
            # 計算總計
            total_amount = sum(c.recon_amount for c in containers)
            total_trust_account = sum(c.recon_amount_for_trust_account_fee for c in containers)
            total_fee = sum(c.recon_service_fee - c.adj_service_fee for c in containers)
            
            self.logger.info(f"\n{'=' * 60}")
            self.logger.info("聯邦總計:")
            self.logger.info(f"  總當期請款金額: {total_amount:,}")
            self.logger.info(f"  總 Trust Account Fee: {total_trust_account:,}")
            self.logger.info(f"  總手續費: {total_fee:,}")
            self.logger.info(f"{'=' * 60}\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"成功處理聯邦銀行 {len(containers)} 個類別",
                metadata={
                    'categories_processed': [c.category for c in containers],
                    'total_amount': total_amount,
                    'total_trust_account_fee': total_trust_account,
                    'total_service_fee': total_fee
                }
            )
            
        except Exception as e:
            self.logger.error(f"處理聯邦銀行失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def print_summary(self, container: BankDataContainer, category: str):
        """列印摘要"""
        self.logger.info(f"\n--- 聯邦 {category} 摘要 ---")
        self.logger.info(f"對帳 請款金額(當期): {container.recon_amount:,}")
        self.logger.info(f"對帳 請款金額(前期發票當期撥款): {container.amount_claimed_last_period_paid_by_current:,}")
        self.logger.info(f"對帳 請款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}")
        self.logger.info("-" * 20)
        self.logger.info(f"對帳 手續費(當期+前期於本期撥款): {container.recon_service_fee:,}")
        self.logger.info(f"對帳 手續費(調整): {container.adj_service_fee:,}")
        
        total_service_fee = container.recon_service_fee - container.adj_service_fee
        self.logger.info(f"對帳 手續費(總計): {total_service_fee:,}\n")


if __name__ == "__main__":
    from src.core.pipeline.context import ProcessingContext
    
    context = ProcessingContext(task_name="test_ub", task_type="transform")
    context.set_variable('beg_date', '2025-10-01')
    context.set_variable('end_date', '2025-10-31')
    context.set_variable('last_beg_date', '2025-09-01')
    context.set_variable('last_end_date', '2025-09-30')
    context.set_variable('db_path', './db/bank_statements.duckdb')
    context.set_variable('log_file', './logs/duckdb_operations.log')
    
    step = ProcessUBStep(name="Process_UB", description="處理聯邦銀行對帳")
    result = step(context)
    
    print(f"\n執行結果: {result.status.value}")
    if result.is_success:
        containers = context.get_auxiliary_data('ub_containers')
        for container in containers:
            print(f"{container.category}: {container.recon_amount:,}")
