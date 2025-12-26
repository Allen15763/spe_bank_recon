"""
Step 17: 輸出工作底稿
輸出 Daily Check Excel、Entry Excel，並寫入 Google Sheets
"""

from typing import Dict, Any
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.core.datasources import GoogleSheetsManager
from src.utils import get_logger, config_manager


class OutputWorkpaperStep(PipelineStep):
    """
    輸出工作底稿步驟
    
    功能:
    1. 輸出 Daily Check Excel
    2. 輸出 Entry Excel
    3. 寫入 Google Sheets（acquiring_charge_raw）
    4. 寫入 Google Sheets（大entry_raw）
    """
    
    def __init__(self, config: Dict[str, Any] = None, **kwargs):
        super().__init__(**kwargs)
        self.config = config or {}
        self.logger = get_logger("OutputWorkpaperStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始輸出工作底稿")
            self.logger.info("=" * 60)
            
            # 取得參數
            output_path = self.config.get('output', {}).get('path', './output/')
            daily_check_filename = context.get_variable('daily_check_filename')
            entry_filename = context.get_variable('entry_filename')
            current_month = context.get_variable('current_month')
            
            # 確保輸出目錄存在
            output_dir = Path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            output_files = []
            
            # =================================================================
            # 1. 輸出 Daily Check Excel
            # =================================================================
            daily_check_path = output_dir / daily_check_filename
            self.logger.info(f"輸出 Daily Check: {daily_check_path}")
            
            with pd.ExcelWriter(daily_check_path, engine='openpyxl') as writer:
                # DFR
                df_dfr = context.get_auxiliary_data('dfr_raw')
                if df_dfr is not None:
                    df_dfr.to_excel(writer, sheet_name='dfr', index=False)
                
                # DFR_WP
                df_dfr_wp = context.get_auxiliary_data('dfr_wp')
                if df_dfr_wp is not None:
                    df_dfr_wp.to_excel(writer, sheet_name='dfr_wp', index=False)
                
                # FRR Handling Fee
                df_frr_handling = context.get_auxiliary_data('frr_handling_fee')
                if df_frr_handling is not None:
                    df_frr_handling.to_excel(writer, sheet_name='frr_handling_fee')
                
                # FRR Remittance Fee
                df_frr_remittance = context.get_auxiliary_data('frr_remittance_fee')
                if df_frr_remittance is not None:
                    df_frr_remittance.to_excel(writer, sheet_name='frr_remittance_fee')
                
                # APCC Acquiring Charge
                df_apcc = context.get_auxiliary_data('apcc_acquiring_charge')
                if df_apcc is not None:
                    df_apcc.to_excel(writer, sheet_name='apcc_acquiring_charge', index=False)
                
                # APCC Validate FRR
                df_apcc_validate = context.get_auxiliary_data('apcc_validate_frr')
                if df_apcc_validate is not None:
                    df_apcc_validate.to_excel(writer, sheet_name='apcc_validate_frr', index=False)
                
                # Summary
                df_summary = context.get_auxiliary_data('apcc_summary')
                if df_summary is not None:
                    df_summary.to_excel(writer, sheet_name='summary_01', index=False)
                
                # Validate FRR Handling Fee
                df_validate_handling = context.get_auxiliary_data('validate_frr_handling_fee')
                if df_validate_handling is not None:
                    df_validate_handling.to_excel(writer, sheet_name='validate_frr_handling_fee', index=False)
                
                # Validate FRR Net Billing
                df_validate_billing = context.get_auxiliary_data('validate_frr_net_billing')
                if df_validate_billing is not None:
                    df_validate_billing.to_excel(writer, sheet_name='validate_frr_net_billing', index=False)
                
                # FRR Long Format
                df_frr_long = context.get_auxiliary_data('frr_long_format')
                if df_frr_long is not None:
                    df_frr_long.to_excel(writer, sheet_name='extracted_from_frr', index=False)
            
            output_files.append(str(daily_check_path))
            self.logger.info("Daily Check Excel 輸出完成")
            
            # =================================================================
            # 2. 輸出 Entry Excel
            # =================================================================
            entry_path = output_dir / entry_filename
            self.logger.info(f"輸出 Entry: {entry_path}")
            
            with pd.ExcelWriter(entry_path, engine='openpyxl') as writer:
                # DFR 驗證
                df_balance_check = context.get_auxiliary_data('dfr_balance_check')
                if df_balance_check is not None:
                    df_balance_check.to_excel(writer, sheet_name='DFR驗證', index=False)
                
                # 分類驗證
                df_entry_validation = context.get_auxiliary_data('entry_long_validation')
                if df_entry_validation is not None:
                    pd.DataFrame([df_entry_validation]).to_excel(writer, sheet_name='分類驗證', index=False)
                
                # 大 Entry
                df_big_entry = context.get_auxiliary_data('big_entry')
                if df_big_entry is not None:
                    df_big_entry.to_excel(writer, sheet_name='大entry', index=False)
                
                # Summary
                df_summary = context.get_auxiliary_data('apcc_summary')
                if df_summary is not None:
                    df_summary.to_excel(writer, sheet_name='summary_01', index=False)
                
                # Entry Temp
                df_entry_temp = context.get_auxiliary_data('entry_temp')
                if df_entry_temp is not None:
                    df_entry_temp.to_excel(writer, sheet_name='entry_temp', index=False)
                
                # Entry Long
                df_entry_long = context.get_auxiliary_data('entry_long')
                if df_entry_long is not None:
                    df_entry_long.to_excel(writer, sheet_name='entry_long_temp', index=False)
                
                # DFR_WP
                if df_dfr_wp is not None:
                    df_dfr_wp.to_excel(writer, sheet_name='dfr_wp', index=False)
                
                # APCC 手續費
                if df_apcc is not None:
                    df_apcc.to_excel(writer, sheet_name='APCC手續費', index=False)
            
            output_files.append(str(entry_path))
            self.logger.info("Entry Excel 輸出完成")
            
            # =================================================================
            # 3. 寫入 Google Sheets
            # =================================================================
            gs_config = self.config.get('google_sheets', {})
            
            if gs_config.get('enabled', True):
                try:
                    cred_path = config_manager.get('general', 'cred_path')
                    spreadsheet_url = gs_config.get('spreadsheet_url')
                    
                    gs_manager = GoogleSheetsManager(
                        credentials_path=cred_path,
                        spreadsheet_url=spreadsheet_url
                    )
                    
                    output_config = gs_config.get('output', {})
                    modes_config = gs_config.get('output', {}).get('modes', {})
                    
                    # 3.1 寫入 acquiring_charge_raw (append)
                    acquiring_sheet = output_config.get('acquiring_charge_raw_sheet', 'acquiring_charge_raw')
                    df_apcc_summary_long = context.get_auxiliary_data('apcc_summary_long')
                    
                    if df_apcc_summary_long is not None:
                        is_append = modes_config.get('acquiring_charge_raw', 'append') == 'append'
                        
                        # 準備資料
                        df_to_write = df_apcc_summary_long.copy()
                        df_to_write['period'] = current_month
                        df_to_write['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        gs_manager.write_data(df_to_write, acquiring_sheet, is_append=is_append)
                        self.logger.info(f"已寫入 Google Sheets: {acquiring_sheet}")
                    
                    # 3.2 寫入 大entry_raw (append)
                    big_entry_sheet = output_config.get('big_entry_raw_sheet', '大entry_raw')
                    df_entry_long = context.get_auxiliary_data('entry_long')
                    
                    if df_entry_long is not None:
                        is_append = modes_config.get('big_entry_raw', 'append') == 'append'
                        
                        # 準備資料
                        df_to_write = df_entry_long.copy()
                        df_to_write['period'] = current_month
                        df_to_write['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        gs_manager.write_data(df_to_write, big_entry_sheet, is_append=is_append)
                        self.logger.info(f"已寫入 Google Sheets: {big_entry_sheet}")
                    
                    # 3.3 寫入 acquiring_charge_sum_display (overwrite)
                    display_sheet = output_config.get('acquiring_charge_sum_display_sheet', 
                                                      'acquiring_charge_sum_display')
                    df_summary = context.get_auxiliary_data('apcc_summary')
                    
                    if df_summary is not None:
                        is_append = modes_config.get('acquiring_charge_sum_display', 'overwrite') == 'append'
                        gs_manager.write_data(df_summary, display_sheet, is_append=is_append)
                        self.logger.info(f"已寫入 Google Sheets: {display_sheet}")
                    
                except Exception as e:
                    self.logger.warning(f"寫入 Google Sheets 失敗: {e}")
                    context.add_warning(f"Google Sheets 寫入失敗: {e}")
            
            # =================================================================
            # 4. 摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("工作底稿輸出完成")
            self.logger.info(f"  輸出檔案數: {len(output_files)}")
            for f in output_files:
                self.logger.info(f"  - {f}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="工作底稿輸出完成",
                metadata={
                    'output_files': output_files,
                    'daily_check_path': str(daily_check_path),
                    'entry_path': str(entry_path),
                    'google_sheets_written': gs_config.get('enabled', True),
                    'output_at': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            self.logger.error(f"輸出工作底稿失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
