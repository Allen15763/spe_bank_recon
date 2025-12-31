"""
Step 10: 載入 Daily Check 參數
從配置檔和 Google Sheets 載入所有必要的參數
"""

from typing import Dict, Any
import pandas as pd
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.core.datasources import GoogleSheetsManager
from src.utils import get_logger, config_manager


class LoadDailyCheckParamsStep(PipelineStep):
    """
    載入 Daily Check 參數步驟
    
    功能:
    1. 載入 FRR/DFR 檔案路徑和欄位配置
    2. 從 Google Sheets 載入手續費率
    3. 從 Google Sheets 載入國泰/中信回饋金
    4. 載入業務規則參數
    """
    
    def __init__(self, config: Dict[str, Any] = None, **kwargs):
        super().__init__(**kwargs)
        self.config = config or {}
        self.logger = get_logger("LoadDailyCheckParamsStep")
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始載入 Daily Check 參數")
            self.logger.info("=" * 60)
            
            # 取得日期範圍（已在 Step 1 載入）
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            current_month = context.get_variable('current_month')
            
            # =================================================================
            # 1. 載入 FRR 配置
            # =================================================================
            frr_config = self.config.get('daily_check', {}).get('frr', {})
            
            frr_path = frr_config.get('path', './input/財務部-{period}.xlsx')
            frr_path = frr_path.replace('{period}', current_month)
            frr_sheet = frr_config.get('sheet_name', '匯入款組成-樂購')
            frr_header_row = frr_config.get('header_row', 0)
            frr_columns = frr_config.get('columns', {})
            frr_bank_mapping = frr_config.get('bank_mapping', {
                'TSPG': '台新', 'CTBC': 'CTBC', 'NCCC': 'NCCC', 
                'CUB': '國泰', 'UBOT': '聯邦'
            })
            
            context.set_variable('frr_path', frr_path)
            context.set_variable('frr_sheet', frr_sheet)
            context.set_variable('frr_header_row', frr_header_row)
            context.set_variable('frr_columns', frr_columns)
            context.set_variable('frr_bank_mapping', frr_bank_mapping)
            
            self.logger.info(f"FRR 路徑: {frr_path}")
            self.logger.info(f"FRR Sheet: {frr_sheet}")
            
            # =================================================================
            # 2. 載入 DFR 配置
            # =================================================================
            dfr_config = self.config.get('daily_check', {}).get('dfr', {})
            
            dfr_path = dfr_config.get('path', './input/TW Bank Balance(NEW).xlsx')
            dfr_sheet = dfr_config.get('sheet_name', ' CTBC SINCE 202108')
            dfr_header_row = dfr_config.get('header_row', 5)
            dfr_columns = dfr_config.get('columns', {})
            
            context.set_variable('dfr_path', dfr_path)
            context.set_variable('dfr_sheet', dfr_sheet)
            context.set_variable('dfr_header_row', dfr_header_row)
            context.set_variable('dfr_columns', dfr_columns)
            
            # 載入驗證欄位清單
            inbound_validation_cols = dfr_columns.get('inbound_validation_cols', [])
            outbound_validation_cols = dfr_columns.get('outbound_validation_cols', [])
            
            context.set_variable('dfr_inbound_validation_cols', inbound_validation_cols)
            context.set_variable('dfr_outbound_validation_cols', outbound_validation_cols)
            
            self.logger.info(f"DFR 路徑: {dfr_path}")
            self.logger.info(f"DFR Sheet: {dfr_sheet}")
            
            # =================================================================
            # 3. 從 Google Sheets 載入手續費率
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
                    
                    # 載入手續費率
                    input_sheets = gs_config.get('input', {})
                    spe_rate_sheet = input_sheets.get('spe_rate_sheet', 'spe_rate')
                    df_spe_rate = gs_manager.get_data(spe_rate_sheet)
                    
                    # 轉換為費率清單
                    charge_rates = df_spe_rate['charge_rate'].tolist() if 'charge_rate' in df_spe_rate.columns else []
                    context.set_variable('charge_rates', charge_rates)
                    
                    self.logger.info(f"已載入手續費率: {len(charge_rates)} 筆")
                    
                    # =================================================================
                    # 4. 從 Google Sheets 載入國泰回饋金
                    # =================================================================
                    cub_rebate_sheet = input_sheets.get('cub_rebate_sheet', '國泰回饋金')
                    df_cub_rebate = gs_manager.get_data(cub_rebate_sheet)
                    
                    # 處理國泰回饋金
                    cub_rebate = self._process_cub_rebate(df_cub_rebate, beg_date, end_date)
                    context.add_auxiliary_data('cub_rebate', cub_rebate)
                    
                    self.logger.info(f"已載入國泰回饋金: {cub_rebate['amount'].sum():,.0f}")
                    
                    # =================================================================
                    # 5. 從 Google Sheets 載入中信回饋金
                    # =================================================================
                    ctbc_rebate_sheet = input_sheets.get('ctbc_rebate_sheet', '中信回饋金')
                    df_ctbc_rebate = gs_manager.get_data(ctbc_rebate_sheet)
                    
                    context.add_auxiliary_data('ctbc_rebate_raw', df_ctbc_rebate)
                    
                    # 從配置取得當月中信回饋金金額（不從 Google Sheets 取最後一筆，因為沒有實際內扣日期）
                    ctbc_rebate_amt = self.config.get('business_rules', {}).get('ctbc_rebate_amt', 0)
                    # if ctbc_rebate_amt == 0 and len(df_ctbc_rebate) > 0:
                    #     if 'Actual received amount' in df_ctbc_rebate.columns:
                    #         ctbc_rebate_amt = df_ctbc_rebate['Actual received amount'].iloc[-1]
                    
                    context.set_variable('ctbc_rebate_amt', ctbc_rebate_amt)
                    self.logger.info(f"中信回饋金金額: {ctbc_rebate_amt:,.0f}")

                    # =================================================================
                    # 6. 從 Google Sheets 載入acquiring_charge_raw、APCC 手續費
                    # =================================================================
                    acquiring_charge_history_sheet = input_sheets.get('acquiring_charge_history_sheet', 
                                                                      'acquiring_charge_raw')
                    df_acquiring_charge_history = gs_manager.get_data(acquiring_charge_history_sheet)
                    
                    context.add_auxiliary_data('acquiring_charge_history', df_acquiring_charge_history)
                    self.logger.info(f"載入acquiring_charge_raw: {df_acquiring_charge_history.shape}")

                    apcc_history_sheet = input_sheets.get('apcc_history_sheet', 'APCC 手續費')
                    df_apcc_history = gs_manager.get_data(apcc_history_sheet)
                    
                    context.add_auxiliary_data('apcc_history', df_apcc_history)
                    self.logger.info(f"載入APCC 手續費: {df_apcc_history.shape}")
                    
                except Exception as e:
                    self.logger.warning(f"載入 Google Sheets 資料失敗: {e}")
                    context.set_variable('charge_rates', [])
                    context.add_warning(f"Google Sheets 載入失敗: {e}")
            
            # =================================================================
            # 6. 載入業務規則參數
            # =================================================================
            business_rules = self.config.get('business_rules', {})
            
            ops_taishi_adj_amt = business_rules.get('ops_taishi_adj_amt', 0)
            ops_cub_adj_amt = business_rules.get('ops_cub_adj_amt', 0)
            ops_ctbc_adj_amt = business_rules.get('ops_ctbc_adj_amt', 0)
            cod_remittance_fee = business_rules.get('cod_remittance_fee', 0)
            ach_exps = business_rules.get('ach_exps', 0)
            taishi_rounding = business_rules.get('taishi_service_fee_rounding', 0)
            ctbc_rounding = business_rules.get('ctbc_service_fee_rounding', 0)
            
            context.set_variable('ops_taishi_adj_amt', ops_taishi_adj_amt)
            context.set_variable('ops_cub_adj_amt', ops_cub_adj_amt)
            context.set_variable('ops_ctbc_adj_amt', ops_ctbc_adj_amt)
            context.set_variable('cod_remittance_fee', cod_remittance_fee)
            context.set_variable('ach_exps', ach_exps)
            context.set_variable('taishi_service_fee_rounding', taishi_rounding)
            context.set_variable('ctbc_service_fee_rounding', ctbc_rounding)
            
            self.logger.info(f"調扣金額: {ops_taishi_adj_amt:,.0f}")
            self.logger.info(f"COD 匯費: {cod_remittance_fee:,.0f}")
            self.logger.info(f"ACH 費用: {ach_exps:,.0f}")
            
            # =================================================================
            # 7. 載入特殊日期交易配置
            # =================================================================
            special_transactions = business_rules.get('special_transactions', {})
            received_ctbc_spt = self._build_received_ctbc_spt(special_transactions, beg_date, end_date)
            context.add_auxiliary_data('received_ctbc_spt', received_ctbc_spt)
            
            if received_ctbc_spt['amount'].sum() > 0:
                self.logger.info(f"特殊交易金額: {received_ctbc_spt['amount'].sum():,.0f}")
            
            # =================================================================
            # 8. 載入 Entry 配置
            # =================================================================
            entry_config = self.config.get('entry', {})
            
            easyfund_path = entry_config.get('easyfund', {}).get('path', './input/仲信手續費_2025.xlsx')
            easyfund_usecols = entry_config.get('easyfund', {}).get('usecols')
            accounts_config = entry_config.get('accounts', {})
            accounts_detail = entry_config.get('accounts_detail', {})
            type_order = entry_config.get('transaction_type_order', {})
            
            context.set_variable('easyfund_path', easyfund_path)
            context.set_variable('easyfund_usecols', easyfund_usecols)
            context.set_variable('accounts_config', accounts_config)
            context.set_variable('accounts_detail', accounts_detail)
            context.set_variable('transaction_type_order', type_order)
            
            self.logger.info(f"仲信手續費路徑: {easyfund_path}")
            
            # =================================================================
            # 9. 載入輸出配置
            # =================================================================
            output_config = self.config.get('output', {})
            
            daily_check_output = output_config.get('daily_check', {})
            daily_check_filename = daily_check_output.get('filename', 'SPETW_daily_check_{period}_中信.xlsx')
            daily_check_filename = daily_check_filename.replace('{period}', current_month)
            
            entry_output = output_config.get('entry', {})
            entry_filename = entry_output.get('filename', 'TW_SPE_entries_{period}.xlsx')
            entry_filename = entry_filename.replace('{period}', current_month)
            
            context.set_variable('daily_check_filename', daily_check_filename)
            context.set_variable('entry_filename', entry_filename)
            
            self.logger.info(f"Daily Check 檔名: {daily_check_filename}")
            self.logger.info(f"Entry 檔名: {entry_filename}")
            
            # =================================================================
            # 10. 顯示摘要
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("Daily Check 參數載入完成")
            self.logger.info("=" * 60)
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="成功載入 Daily Check 參數",
                metadata={
                    'period': current_month,
                    'frr_path': frr_path,
                    'dfr_path': dfr_path,
                    'charge_rates_count': len(context.get_variable('charge_rates', [])),
                    'ops_taishi_adj_amt': ops_taishi_adj_amt,
                    'loaded_at': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            self.logger.error(f"載入 Daily Check 參數失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _process_cub_rebate(self, df: pd.DataFrame, beg_date: str, end_date: str) -> pd.DataFrame:
        """處理國泰回饋金資料"""
        # 建立日期範圍
        date_range = pd.DataFrame(
            pd.date_range(beg_date, end_date, freq='D'),
            columns=['Date']
        )
        date_range['Date'] = date_range['Date'].dt.strftime('%Y-%m-%d')
        
        # 處理原始資料
        if 'Actual received date' in df.columns and 'Actual received amount' in df.columns:
            df_rebate = df[['Actual received date', 'Actual received amount']].copy()
            df_rebate['Date'] = pd.to_datetime(df_rebate['Actual received date']).dt.strftime('%Y-%m-%d')
            rebate_dict = df_rebate.set_index('Date')['Actual received amount'].to_dict()
        else:
            rebate_dict = {}
        
        # 合併回饋金
        date_range['amount'] = date_range['Date'].map(rebate_dict).fillna(0)
        
        return date_range
    
    def _build_received_ctbc_spt(self, special_transactions: Dict[str, Any], 
                                 beg_date: str, end_date: str) -> pd.DataFrame:
        """建立中信 SPT 入款資料"""
        date_range = pd.DataFrame(
            pd.date_range(beg_date, end_date, freq='D'),
            columns=['Date']
        )
        date_range['Date'] = date_range['Date'].dt.strftime('%Y-%m-%d')
        date_range['amount'] = date_range['Date'].map(special_transactions).fillna(0)
        
        return date_range
