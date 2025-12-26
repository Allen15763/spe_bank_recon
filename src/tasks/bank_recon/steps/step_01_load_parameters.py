"""
Step 1: 載入參數
從配置檔載入所有必要的執行參數到 Context
"""

from typing import Dict, Any
import pandas as pd
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger


class LoadParametersStep(PipelineStep):
    """
    載入參數步驟
    
    功能:
    1. 從配置檔讀取日期範圍
    2. 讀取資料庫路徑
    3. 讀取輸出路徑
    4. 讀取分期報表路徑
    5. 設定所有參數到 Context
    """
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.logger = get_logger("LoadParametersStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行參數載入
        
        Args:
            context: 處理上下文
            
        Returns:
            StepResult: 執行結果
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始載入參數")
            self.logger.info("=" * 60)
            
            # =================================================================
            # 1. 載入日期範圍
            # =================================================================
            dates_config = self.config.get('dates')
            
            beg_date = dates_config.get('current_period_start')
            end_date = dates_config.get('current_period_end')
            last_beg_date = dates_config.get('last_period_start')
            last_end_date = dates_config.get('last_period_end')
            
            # 計算期間字串（用於檔名）
            current_month = beg_date.replace('-', '')[:6]  # 202510
            last_month = last_beg_date.replace('-', '')[:6]  # 202509
            
            # 設定日期變數
            context.set_variable('beg_date', beg_date)
            context.set_variable('end_date', end_date)
            context.set_variable('last_beg_date', last_beg_date)
            context.set_variable('last_end_date', last_end_date)
            context.set_variable('current_month', current_month)
            context.set_variable('last_month', last_month)
            
            self.logger.info(f"當期範圍: {beg_date} ~ {end_date}")
            self.logger.info(f"前期範圍: {last_beg_date} ~ {last_end_date}")
            
            # =================================================================
            # 2. 載入資料庫配置
            # =================================================================
            db_config = self.config.get('database')
            
            db_path = db_config.get('path', './db/bank_statements.duckdb')
            log_file = db_config.get('log_file', './logs/duckdb_operations.log')
            log_level = db_config.get('log_level', 'DEBUG')
            
            context.set_variable('db_path', db_path)
            context.set_variable('log_file', log_file)
            context.set_variable('log_level', log_level)
            
            self.logger.info(f"資料庫路徑: {db_path}")
            
            # =================================================================
            # 3. 載入輸出配置
            # =================================================================
            output_config = self.config.get('output')
            
            output_path = output_config.get('path', './output/')
            escrow_filename = output_config.get(
                'escrow_filename', 
                'Escrow_recon_{period}_renew.xlsx'.replace('{period}', current_month)
            ).replace('{period}', current_month)
            trust_account_filename = output_config.get('trust_account_filename', 
                                                       'filing data for Trust Account Fee Accrual-SPETW.xlsx')
            
            # 替換檔名中的期間變數
            escrow_filename = escrow_filename.replace('{period}', current_month)
            
            context.set_variable('output_path', output_path)
            context.set_variable('escrow_filename', escrow_filename)
            context.set_variable('trust_account_filename', trust_account_filename)
            
            self.logger.info(f"輸出路徑: {output_path}")
            self.logger.info(f"Escrow 檔名: {escrow_filename}")
            self.logger.info(f"Trust Account 檔名: {trust_account_filename}")
            
            # =================================================================
            # 4. 載入分期報表路徑
            # =================================================================
            installment_config = self.config.get('installment')['reports']
            
            # 替換路徑中的期間變數
            installment_reports = {}
            for bank, path_template in installment_config.items():
                path = path_template.replace('{period}', current_month)
                installment_reports[bank] = path
                self.logger.info(f"{bank} 分期報表: {path}")
            
            context.set_variable('installment_reports', installment_reports)
            
            # =================================================================
            # 5. 載入其他配置
            # =================================================================
            
            # 驗證規則
            validation_config = self.config.get('validation')
            context.set_variable('validation_tolerance', validation_config.get('tolerance', 1))
            context.set_variable('validation_strict_mode', validation_config.get('enable_strict_mode', False))
            
            # Google Sheets 配置
            installment_google_sheets = self.config.get('installment').get('google_sheets_enabled', True)
            service_fee_sheet_name = self.config.get('installment').get('service_fee_sheet_name', 'service_fee_rate')
            context.set_variable('use_google_sheets', installment_google_sheets)
            context.set_variable('service_fee_sheet_name', service_fee_sheet_name)
            
            # 銀行順序配置
            bank_order = self.config.get('output').get('bank_order')
            context.set_variable('escrow_bank_order', bank_order.get('escrow', []))
            context.set_variable('trust_account_bank_order', bank_order.get('trust_account', []))
            
            # =================================================================
            # 6. 顯示摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("參數載入完成")
            self.logger.info("=" * 60)
            self.logger.info(f"處理期間: {current_month}")
            self.logger.info(f"日期範圍: {beg_date} ~ {end_date}")
            self.logger.info(f"資料庫: {db_path}")
            self.logger.info(f"輸出目錄: {output_path}")
            self.logger.info(f"分期報表數量: {len(installment_reports)}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="成功載入所有參數",
                metadata={
                    'period': current_month,
                    'date_range': f"{beg_date}~{end_date}",
                    'loaded_at': datetime.now().isoformat(),
                    'parameters_count': len(context._variables),
                    'installment_reports_count': len(installment_reports)
                }
            )
            
        except Exception as e:
            self.logger.error(f"載入參數失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=f"參數載入失敗: {str(e)}"
            )


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
        task_name="test_load_parameters",
        task_type="transform"
    )
    
    # 執行步驟
    step = LoadParametersStep(
        name="Load_Parameters",
        description="載入執行參數"
    )
    
    result = step(context)
    
    # 檢查結果
    print(f"\n執行結果: {result.status.value}")
    print(f"訊息: {result.message}")
    print(f"耗時: {result.duration:.2f} 秒")
    
    if result.is_success:
        print("\n已載入的參數:")
        for key, value in context.variables.items():
            if isinstance(value, dict):
                print(f"  {key}: (字典, {len(value)} 項)")
            elif isinstance(value, list):
                print(f"  {key}: (列表, {len(value)} 項)")
            else:
                print(f"  {key}: {value}")
