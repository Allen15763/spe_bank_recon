"""
Step 8: 載入分期報表
處理各銀行的分期報表數據
"""

from typing import Dict, Any
import re
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.utils import get_logger, config_manager, DuckDBManager
from src.core.datasources import GoogleSheetsManager

from ..models import InstallmentReportData


class LoadInstallmentStep(PipelineStep):
    """
    載入分期報表步驟
    
    處理各銀行的分期報表:
    1. CUB (國泰): 個人 + 法人
    2. CTBC (中信): 處理多個 sheet
    3. NCCC: 期數處理
    4. UB (聯邦): 手續費率計算
    5. Taishi (台新): 一般卡 + voucher
    """
    
    # 期數映射
    TRANSACTION_TYPE_MAPPING = {
        '03': '3期',
        '06': '6期',
        '12': '12期',
        '24': '24期',
    }
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger("LoadInstallmentStep")
        self.service_fee_rate = {}
    
    def execute(self, context: ProcessingContext) -> StepResult:
        try:
            self.logger.info("=" * 60)
            self.logger.info("開始載入分期報表")
            self.logger.info("=" * 60)
            
            # 取得參數
            installment_reports = context.get_variable('installment_reports')
            use_google_sheets = context.get_variable('use_google_sheets', True)
            
            # ===================================================================
            # 1. 載入手續費率（從 Google Sheets）
            # ===================================================================
            if use_google_sheets:
                self.load_service_fee_rate(context)
            
            # ===================================================================
            # 2. 處理各銀行分期報表
            # ===================================================================
            
            # 2.1 國泰 (CUB)
            cub_individual_agg, cub_nonindividual_agg = self.process_cub_installment(
                installment_reports.get('cub_individual'),
                installment_reports.get('cub_nonindividual')
            )
            
            # 2.2 中信 (CTBC)
            ctbc_agg = self.process_ctbc_installment(
                installment_reports.get('ctbc'),
                context
            )
            
            # 2.3 NCCC
            nccc_agg = self.process_nccc_installment(
                installment_reports.get('nccc')
            )
            
            # 2.4 聯邦 (UB)
            ub_agg = self.process_ub_installment(
                installment_reports.get('ub')
            )
            
            # 2.5 台新 (Taishi)
            taishi_agg = self.process_taishi_installment(
                installment_reports.get('taishi')
            )
            
            # ===================================================================
            # 3. 儲存結果到 Context
            # ===================================================================
            context.add_auxiliary_data('cub_individual_installment', cub_individual_agg)
            context.add_auxiliary_data('cub_nonindividual_installment', cub_nonindividual_agg)
            context.add_auxiliary_data('ctbc_installment', ctbc_agg)
            context.add_auxiliary_data('nccc_installment', nccc_agg)
            context.add_auxiliary_data('ub_installment', ub_agg)
            context.add_auxiliary_data('taishi_installment', taishi_agg)
            
            # ===================================================================
            # 4. 記錄統計
            # ===================================================================
            self.logger.info(f"\n{'=' * 60}")
            self.logger.info("分期報表載入完成:")
            self.logger.info(f"  國泰個人: {len(cub_individual_agg)} 筆")
            self.logger.info(f"  國泰法人: {len(cub_nonindividual_agg)} 筆")
            self.logger.info(f"  中信: {len(ctbc_agg)} 筆")
            self.logger.info(f"  NCCC: {len(nccc_agg)} 筆")
            self.logger.info(f"  聯邦: {len(ub_agg)} 筆")
            self.logger.info(f"  台新: {len(taishi_agg)} 筆")
            self.logger.info(f"{'=' * 60}\n")
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message="成功載入所有分期報表",
                metadata={
                    'reports_loaded': 6,
                    'cub_records': len(cub_individual_agg) + len(cub_nonindividual_agg),
                    'ctbc_records': len(ctbc_agg),
                    'nccc_records': len(nccc_agg),
                    'ub_records': len(ub_agg),
                    'taishi_records': len(taishi_agg)
                }
            )
            
        except Exception as e:
            self.logger.error(f"載入分期報表失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )
    
    # =========================================================================
    # 輔助方法
    # =========================================================================
    
    def load_service_fee_rate(self, context: ProcessingContext):
        """從 Google Sheets 載入手續費率"""
        try:
            cred_path = config_manager.get('general', 'cred_path')
            spreadsheet_url = config_manager.get(
                'general', 'spreadsheet_url', 
                'https://docs.google.com/spreadsheets/d/17puiAmAhM2dAm9BR7Sck1E2fwsf0v76CkpiLkVJPlxE/edit?gid=0#gid=0')
            
            manager = GoogleSheetsManager(
                credentials_path=cred_path,
                spreadsheet_url=spreadsheet_url
            )
            
            sheet_name = context.get_variable('service_fee_sheet_name', 'service_fee_rate')
            df_rate = manager.get_data(sheet_name)
            
            # 轉換為巢狀字典
            self.service_fee_rate = self.dataframe_to_nested_dict(df_rate)
            
            self.logger.info(f"成功載入手續費率: {len(self.service_fee_rate)} 個銀行")
            
        except Exception as e:
            self.logger.warning(f"載入手續費率失敗: {str(e)}")
            # 使用預設值
            self.service_fee_rate = {}
    
    def dataframe_to_nested_dict(self, df: pd.DataFrame) -> Dict:
        """將 DataFrame 轉換為巢狀字典"""
        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)
        
        df = df.set_index('index')
        result = {}
        
        for col in df.columns:
            result[col] = {}
            for key in df.index:
                value = df.loc[key, col]
                if pd.notna(value) and value != '' and str(value).strip() != '':
                    formatted_key = self.format_key(key)
                    try:
                        result[col][formatted_key] = float(value)
                    except (ValueError, TypeError):
                        result[col][formatted_key] = value
        
        return result
    
    def format_key(self, key):
        """格式化 key"""
        if isinstance(key, (int, float)):
            num = int(key)
            return f'{num:02d}' if num < 10 else str(num)
        
        if isinstance(key, str):
            if key.isdigit():
                num = int(key)
                return f'{num:02d}' if num < 10 else str(num)
            return key
        
        return str(key)
    
    # =========================================================================
    # 各銀行分期報表處理
    # =========================================================================
    
    def process_cub_installment(self, reports):
        """處理國泰分期報表"""
        self.logger.info("處理國泰分期報表...")
        
        def read_cub(url):
            df = pd.read_excel(url, header=3, dtype=str, sheet_name='B2B_TimesM')
            df = df.rename(columns={'交易\n類別': '交易類別', '請款\n商店代號': '請款商店代號'})
            df = df.query("~交易類別.isna() and 交易類別 != '小計'")
            for c in ['請款月', '分期數', '請款商店代號', '請款商店名稱']:
                df[c] = df[c].fillna(method='ffill')
            df['金額'] = df['金額'].astype(float)
            df['手續費'] = df['手續費'].astype(float)
            return df.groupby('分期數').agg(
                total_claimed=('金額', 'sum'),
                total_service_fee=('手續費', 'sum')
            ).reset_index().rename(columns={'分期數': 'transaction_type'})
        
        cub_individual_agg = read_cub(reports['cub_individual'])
        cub_nonindividual_agg = read_cub(reports['cub_nonindividual'])

        a = f"  國泰個人: {len(cub_individual_agg)} 筆, 總額: {cub_individual_agg['total_claimed'].sum():,.0f}"
        b = f"  國泰法人: {len(cub_nonindividual_agg)} 筆, 總額: {cub_nonindividual_agg['total_claimed'].sum():,.0f}"
        self.logger.info(a)
        self.logger.info(b)
        return cub_individual_agg, cub_nonindividual_agg
    
    def process_ctbc_installment(self, reports, context):
        """處理中信"""
        self.logger.info("處理中信...")
        
        beg = context.get_variable('beg_date')
        end = context.get_variable('end_date')
        
        # 讀取並處理 Excel sheets
        dfs_install = []
        with pd.ExcelFile(reports['ctbc']) as xls:
            for sheet in xls.sheet_names:
                if re.search(r'\d{4}', sheet) and '分' in sheet:
                    df = xls.parse(sheet, usecols='A:I', header=2)
                    df['source'] = sheet
                    dfs_install.append(df)
        
        df_install = pd.concat(dfs_install, ignore_index=True)
        
        # 讀取日期範圍
        with DuckDBManager(
            db_path=context.get_variable('db_path'),
            log_file=context.get_variable('log_file'),
            log_level="DEBUG"
        ) as db:
            req_dates = db.query_to_df(
                f"SELECT request_date FROM ctbc_installment WHERE request_date BETWEEN '{beg}' AND '{end}'"
            ).iloc[:, 0].dt.strftime('%m%d').tolist()
        
        # 計算分期數據
        df_install['source_clean'] = df_install['source'].str.replace('分-', '')
        results = {}
        for period in [3, 6, 12, 24]:
            mask = (df_install['期數'] == period) & df_install['source_clean'].isin(req_dates)
            results[f'{period}期'] = {
                'total_claimed': df_install.loc[mask, '請/調金額'].sum(),
                'total_service_fee': df_install.loc[mask, '實際手續費'].sum()
            }
        
        # 調整
        mask_adj = df_install['產品別'].str.contains('調', na=False) & df_install['source_clean'].isin(req_dates)
        results['3期']['total_claimed'] += df_install.loc[mask_adj, '請/調金額'].sum()
        results['3期']['total_service_fee'] += df_install.loc[mask_adj, '實際手續費'].sum()
        
        # normal (非分期)
        results['normal'] = {'total_claimed': 0, 'total_service_fee': 0}
        
        return pd.DataFrame([
            {'transaction_type': k, **v} for k, v in results.items()
        ])
    
    def process_nccc_installment(self, reports):
        """處理 NCCC"""
        self.logger.info("處理 NCCC...")
        df = pd.read_excel(reports['nccc'], header=4, dtype=str)
        df = df.query("~期數.isna() and ~期數.isin(['小計', '合計', '總計'])")
        
        for c in ['特店代號', '處理日', '類別', '卡別']:
            df[c] = df[c].fillna(method='ffill')
        
        df['金額'] = df['金額'].astype(float)
        df['手續費'] = df['手續費'].astype(float)
        
        return df.groupby('期數').agg(
            total_claimed=('金額', 'sum'),
            total_service_fee=('手續費', 'sum')
        ).reset_index().rename(columns={'期數': 'transaction_type'}).assign(
            transaction_type=lambda x: x['transaction_type'].map(self.TRANSACTION_TYPE_MAPPING)
        )
    
    def process_ub_installment(self, reports):
        """處理聯邦"""
        self.logger.info("處理聯邦...")
        df = pd.read_excel(reports['ub'], header=3, dtype=str)
        df = df.query("~商店名稱.isna() and ~交易類別.isna()").reset_index(drop=True)
        
        df['金額'] = df['金額'].astype(int)
        df['手續費'] = df['手續費'].astype(int)
        
        agg = df.groupby('分期期數').agg(
            total_claimed=('金額', 'sum'),
            total_service_fee=('手續費', 'sum')
        ).reset_index().assign(
            service_fee_rate=lambda x: x['分期期數'].map(self.service_fee_rate.get('ub', {}))
        )
        agg['calculated_service_fee'] = (agg['total_claimed'] * agg['service_fee_rate']).round(2)
        
        return agg.rename(columns={'分期期數': 'transaction_type'}).assign(
            transaction_type=lambda x: x['transaction_type'].map(self.TRANSACTION_TYPE_MAPPING)
        )
    
    def process_taishi_installment(self, reports):
        """處理台新"""
        self.logger.info("處理台新...")
        
        def read_taishi(sheet_idx, is_voucher=False):
            df = pd.read_excel(reports['taishi'], header=2, dtype=str, sheet_name=sheet_idx)
            df = df.iloc[:df.query("卡別=='總筆數'").index[0], :]
            df.columns = ['卡別', 'transaction_type', 'count_and_amount', 'Visa', 'M/C', 
                          'JCB', 'CUP', 'Discover', 'S/P', 'U/C', '跨境', '小計']
            
            for c in ['卡別', 'transaction_type', 'count_and_amount']:
                df[c] = df[c].fillna(method='ffill')
            
            for c in df.columns[3:]:
                df[c] = df[c].astype(int)
            
            df['transaction_type'] = df['transaction_type'].str.replace(' ', '')
            df = df.query("~transaction_type.str.contains('小計')")
            df['卡別'] = df['卡別'].str.replace('\n|\|', '', regex=True)
            
            df_amt = df.query("count_and_amount=='金額'").copy()
            rate_map = self.service_fee_rate.get('taishi_voucher' if is_voucher else 'taishi', {})
            df_amt['service_fee_rate'] = df_amt['transaction_type'].map(rate_map)
            df_amt['service_fee'] = (df_amt['小計'] * df_amt['service_fee_rate']).round(2)
            
            return df_amt[['transaction_type', '小計', 'service_fee']]
        
        normal = read_taishi(0, False)
        voucher = read_taishi(1, True)
        
        merged = pd.merge(normal, voucher, on='transaction_type', how='outer', 
                          suffixes=['_normal', '_voucher']).fillna(0)
        merged['total_claimed'] = merged['小計_normal'] + merged['小計_voucher'].astype(int)
        merged['total_service_fee'] = merged['service_fee_normal'] + merged['service_fee_voucher']
        
        return merged[['transaction_type', 'total_claimed', 'total_service_fee']]
