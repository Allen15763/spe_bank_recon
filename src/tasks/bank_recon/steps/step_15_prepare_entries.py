"""
Step 15: 準備會計分錄
整理會計科目，將每日資料轉換為分錄格式

重構說明:
- 配置驅動：從 TOML 配置檔讀取參數
- 統一日誌：使用 get_logger 而非 print
- 動態年月：從 context 取得年月，不再硬編碼
- 結果輸出：所有結果存入 context
"""

from typing import Dict, Any
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_structured_logger, load_toml

from ..utils import (
    process_accounting_entries,
    validate_accounting_balance,
    ConfigurableEntryConfig,
    AccountingEntryProcessor,
    AccountingEntryTransformer,
    validate_result,
    dfr_balance_check,
    summarize_balance_check,
)


class PrepareEntriesStep(PipelineStep):
    """
    準備會計分錄步驟
    
    功能:
    1. 整理會計科目
    2. 處理國泰/中信回饋金
    3. 生成 df_entry_temp（寬格式）
    4. 驗證會計平衡
    5. 生成完整分錄表（長格式）
    6. 生成大 Entry pivot 表
    """
    
    def __init__(self, config: Dict[str, Any] = None, **kwargs):
        """
        初始化步驟
        
        Args:
            config: 從 TOML 讀取的配置
            **kwargs: 傳遞給父類的參數
        """
        super().__init__(**kwargs)
        self.config = config or {}
        self.logger = get_structured_logger("PrepareEntriesStep").logger
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始準備會計分錄")
            self.logger.info("=" * 60)
            
            # =================================================================
            # 0. 取得參數和配置
            # =================================================================
            # 從 context 取得年月（動態）
            beg_date = context.get_variable('beg_date')
            end_date = context.get_variable('end_date')
            year = int(beg_date[:4])
            month = int(beg_date[5:7])
            
            self.logger.info(f"處理期間: {year}-{month:02d}")
            
            # 從 context 取得業務參數
            cod_remittance_fee = context.get_variable('cod_remittance_fee', 0)
            ach_exps = context.get_variable('ach_exps', 0)
            ctbc_rebate_amt = context.get_variable('ctbc_rebate_amt', 0)
            
            # 從配置取得 entry 相關設定
            entry_config = self.config.get('entry', {})
            
            # 載入月度配置檔
            monthly_config = self._load_monthly_config(entry_config)
            
            # =================================================================
            # 1. 取得來源資料
            # =================================================================
            df_dfr_wp = context.get_auxiliary_data('dfr_wp')
            df_result_dfr = context.get_auxiliary_data('dfr_result')
            cub_rebate = context.get_auxiliary_data('cub_rebate')
            received_ctbc_spt = context.get_auxiliary_data('received_ctbc_spt')
            
            if df_dfr_wp is None:
                raise ValueError("缺少 DFR 工作底稿資料")
            
            # 載入仲信手續費檔案
            easyfund_path = context.get_variable('easyfund_path')
            easyfund_usecols = context.get_variable('easyfund_usecols')
            
            df_easyfund = pd.read_excel(easyfund_path, usecols=easyfund_usecols)
            self.logger.info(f"已載入仲信手續費: {easyfund_path}")
            
            # =================================================================
            # 2. 準備回饋金資料
            # =================================================================
            cub_rebate = self._prepare_rebate_data(cub_rebate, beg_date, end_date, '國泰回饋金')
            received_ctbc_spt = self._prepare_rebate_data(received_ctbc_spt, beg_date, end_date, '中信 SPT 入款')
            
            cub_rebate_total = cub_rebate['amount'].sum()
            received_spt_total = received_ctbc_spt['amount'].sum()
            
            self.logger.info(f"國泰回饋金總額: {cub_rebate_total:,.0f}")
            self.logger.info(f"中信 SPT 入款總額: {received_spt_total:,.0f}")
            
            # =================================================================
            # 3. 取得利息資料
            # =================================================================
            if df_result_dfr is not None and 'interest' in df_result_dfr.columns:
                interest = df_result_dfr['interest']
            else:
                interest = pd.Series([0] * len(pd.date_range(beg_date, end_date, freq='D')))
            
            interest_total = interest.sum()
            self.logger.info(f"利息總額: {interest_total:,.0f}")
            
            # =================================================================
            # 4. 整理會計分錄（寬格式）
            # =================================================================
            df_entry_temp = process_accounting_entries(
                df_dfr_wp,
                cub_rebate,
                received_ctbc_spt,
                interest,
                beg_date,
                end_date
            )
            
            context.add_auxiliary_data('entry_temp', df_entry_temp)
            self.logger.info(f"會計分錄整理完成: {len(df_entry_temp)} 天")
            
            # =================================================================
            # 5. 驗證會計平衡
            # =================================================================
            validation = validate_accounting_balance(df_entry_temp)
            
            if validation['is_balanced']:
                self.logger.info("會計平衡驗證通過")
            else:
                self.logger.warning(f"會計平衡驗證失敗: 差額 {validation['total_diff']:,.2f}")
                context.add_warning(f"會計平衡差額: {validation['total_diff']:,.2f}")
            
            context.add_auxiliary_data('entry_validation', validation)
            
            # =================================================================
            # 6. 取得 APCC 收單手續費
            # =================================================================
            apcc_acquiring_charge = context.get_auxiliary_data('apcc_acquiring_charge')
            acquiring_amt = 0
            
            if apcc_acquiring_charge is not None:
                try:
                    acquiring_amt = (
                        apcc_acquiring_charge
                        .query("transaction_type=='小計'")
                        .commission_fee.values[0]
                    )
                except (IndexError, KeyError) as e:
                    self.logger.warning(f"取得 APCC 手續費失敗: {e}")
            
            self.logger.info(f"APCC 收單手續費: {acquiring_amt:,.0f}")
            
            # =================================================================
            # 7. 初始化配置驅動的 Entry 配置
            # =================================================================
            self.logger.info("初始化會計分錄處理器...")
            
            # 建立運行時參數
            runtime_params = {
                'df_easyfund': df_easyfund,
                'beg_date': beg_date,
                'apcc_acquiring_charge': acquiring_amt,
                'ach_exps': ach_exps,
                'cod_remittance_fee': cod_remittance_fee,
                'ctbc_rebate_amt': ctbc_rebate_amt,
            }
            
            # 儲存調整項目
            context.set_variable('entry_adjustments', {
                'cod_remittance_fee': cod_remittance_fee,
                'ach_exps': ach_exps,
                'ctbc_rebate_amt': ctbc_rebate_amt,
                'apcc_acquiring_charge': acquiring_amt,
            })
            
            # 建立配置驅動的 Entry 配置
            config_obj = ConfigurableEntryConfig(
                year=year,
                month=month,
                entry_config=entry_config,
                monthly_config=monthly_config,
                runtime_params=runtime_params
            )
            
            # 建立處理器
            processor = AccountingEntryProcessor(
                year=year,
                month=month,
                config=config_obj,
                entry_config=entry_config
            )
            
            # =================================================================
            # 8. 執行完整處理
            # =================================================================
            self.logger.info("開始處理...")
            df_entry_long = processor.process(df_entry_temp)
            
            # 產生報告
            processor.generate_report(df_entry_long)
            
            # 驗證結果
            self.logger.info("驗證結果...")
            validate_result(df_entry_long)
            
            # 存入 context
            context.add_auxiliary_data('entry_long', df_entry_long)
            
            # =================================================================
            # 9. 處理交易類型排序並生成報表
            # =================================================================
            # 從配置讀取交易類型排序
            type_order = entry_config.get('transaction_type_order', {})
            val_zero_excludes = entry_config.get('validation', {}).get('exclude_zero_check', [])
            
            df_entry_long['accounting_date'] = df_entry_long['accounting_date'].fillna('期末會計調整')
            
            df_entry_long_temp = df_entry_long.copy()
            df_entry_long_temp['transaction_type'] = (
                df_entry_long_temp['transaction_type']
                .map(type_order)
                .fillna(df_entry_long['transaction_type'])
            )
            
            context.add_auxiliary_data('entry_long_temp', df_entry_long_temp)
            
            # =================================================================
            # 10. DFR 餘額驗證
            # =================================================================
            try:
                result_check_dfr = dfr_balance_check(df_entry_long_temp, df_result_dfr)
                summary_check_dfr = summarize_balance_check(result_check_dfr)
                
                context.add_auxiliary_data('dfr_balance_check', result_check_dfr)
                context.add_auxiliary_data('dfr_balance_summary', summary_check_dfr)
                
                self.logger.info(f"DFR 餘額驗證: {summary_check_dfr}")
            except Exception as e:
                self.logger.warning(f"DFR 餘額驗證失敗: {e}")
            
            # =================================================================
            # 11. 分類驗證
            # =================================================================
            result_check_category = (
                df_entry_long_temp
                .query("~transaction_type.isin(@val_zero_excludes)")
                .pivot_table(
                    index=['accounting_date'], 
                    columns='transaction_type', 
                    values='amount', 
                    aggfunc='sum', 
                    margins=True, 
                    margins_name='Total'
                )
                .reset_index()
            )
            
            context.add_auxiliary_data('category_validation', result_check_category)
            
            # =================================================================
            # 12. 生成大 Entry pivot 表
            # =================================================================
            df_big_entry = (
                df_entry_long_temp
                .pivot_table(
                    index=['account_no', 'account_desc', 'transaction_type'], 
                    columns='accounting_date', 
                    values='amount', 
                    aggfunc='sum', 
                    margins=True, 
                    margins_name='Total'
                )
                .reset_index()
            )
            
            context.add_auxiliary_data('big_entry', df_big_entry)
            self.logger.info(f"大 Entry pivot 表: {len(df_big_entry)} 行")
            
            # =================================================================
            # 13. 顯示分錄摘要
            # =================================================================
            acc_cols = [col for col in df_entry_temp.columns if col.startswith('acc_')]
            
            self.logger.info("\n分錄摘要:")
            for col in acc_cols:
                total = df_entry_temp[col].sum()
                if abs(total) > 0:
                    self.logger.info(f"  {col}: {total:,.0f}")
            
            # =================================================================
            # 14. 完成
            # =================================================================
            self.logger.info("\n" + "=" * 60)
            self.logger.info("會計分錄準備完成")
            self.logger.info(f"  分錄天數: {len(df_entry_temp)}")
            self.logger.info(f"  科目數量: {len(acc_cols)}")
            self.logger.info(f"  國泰回饋金: {cub_rebate_total:,.0f}")
            self.logger.info(f"  利息收入: {interest_total:,.0f}")
            self.logger.info(f"  會計平衡: {'是' if validation['is_balanced'] else '否'}")
            self.logger.info("=" * 60 + "\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="會計分錄準備完成",
                metadata={
                    'days': len(df_entry_temp),
                    'account_count': len(acc_cols),
                    'cub_rebate_total': cub_rebate_total,
                    'interest_total': interest_total,
                    'is_balanced': validation['is_balanced'],
                    'total_diff': validation['total_diff'],
                    'entry_long_count': len(df_entry_long),
                    'big_entry_count': len(df_big_entry),
                    'prepared_at': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            self.logger.error(f"準備會計分錄失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    def _load_monthly_config(self, entry_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        載入月度配置檔
        
        Args:
            entry_config: entry 配置區段
            
        Returns:
            Dict: 月度配置
        """
        monthly_config_path = entry_config.get('monthly_config_path', 
                                               './src/config/bank_recon_entry_monthly.toml')
        
        try:
            monthly_config = load_toml(monthly_config_path)
            self.logger.info(f"已載入月度配置: {monthly_config_path}")
            return monthly_config
        except FileNotFoundError:
            self.logger.warning(f"月度配置檔不存在: {monthly_config_path}，使用預設值")
            return {}
        except Exception as e:
            self.logger.error(f"載入月度配置失敗: {e}")
            return {}
    
    def _prepare_rebate_data(self, 
                             data: pd.DataFrame, 
                             beg_date: str, 
                             end_date: str,
                             name: str) -> pd.DataFrame:
        """
        準備回饋金/入款資料，確保資料存在且格式正確
        
        Args:
            data: 原始資料
            beg_date: 開始日期
            end_date: 結束日期
            name: 資料名稱（用於日誌）
            
        Returns:
            pd.DataFrame: 處理後的資料
        """
        if data is None:
            self.logger.warning(f"無{name}資料，使用零值")
            date_range = pd.date_range(beg_date, end_date, freq='D')
            return pd.DataFrame({
                'Date': date_range.strftime('%Y-%m-%d'),
                'amount': 0
            })
        
        return data
