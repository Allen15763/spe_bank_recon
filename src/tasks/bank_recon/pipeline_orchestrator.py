"""
Bank Recon Pipeline 定義
組裝所有步驟形成完整的處理流程

主要修復：
1. 不用 PipelineBuilder.build()，使用 Pipeline 直接構造
2. 統一配置管理
3. 擴展支援 Daily Check & Entry 步驟 (Step 10-17)
"""
from pathlib import Path
from typing import Dict, Any, Optional, List
import tomllib

from src.core.pipeline import Pipeline, ProcessingContext, PipelineConfig
from src.core.pipeline.checkpoint import (
    CheckpointManager,
    PipelineWithCheckpoint,
    resume_from_checkpoint,
    list_available_checkpoints
)
from src.utils import get_logger, get_structured_logger

from .steps import (
    # Original Steps (1-9)
    LoadParametersStep,
    ProcessCUBStep,
    ProcessCTBCStep,
    ProcessNCCCStep,
    ProcessUBStep,
    ProcessTaishiStep,
    AggregateEscrowStep,
    LoadInstallmentStep,
    GenerateTrustAccountStep,
    # Daily Check & Entry Steps (10-17)
    LoadDailyCheckParamsStep,
    ProcessFRRStep,
    ProcessDFRStep,
    CalculateAPCCStep,
    ValidateDailyCheckStep,
    PrepareEntriesStep,
    OutputWorkpaperStep,
)


"""
SPE Bank Recon Task 類 - 完整實現示例
參考 Offline Tasks 框架設計

這是一個完整的 Task 類實現，展示如何將參考框架的設計應用到 SPE Bank Recon 專案中。
"""

logger = get_logger("bank_recon.pipeline")


class BankReconTask:
    
    """
    SPE 銀行對帳主任務類（重構版）
    
    完整的處理流程：
    1. Escrow 對帳（國泰、中信、NCCC、聯邦、台新）
    2. 分期報表處理
    3. Trust Account Fee 生成
    4. Daily Check（FRR/DFR 處理、APCC 手續費計算）
    5. Entry 生成（會計分錄）
    
    功能：
    - 完整的配置管理
    - Pipeline 構建與執行
    - Checkpoint 管理
    - 輸入驗證
    - 錯誤處理
    - Resume 支持
    
    Example:
        >>> # 基本使用（完整流程）
        >>> task = BankReconTask()
        >>> result = task.execute()
        >>> 
        >>> # 從 checkpoint 恢復
        >>> result = task.resume(
        ...     checkpoint_name='bank_recon_after_Process_CTBC',
        ...     start_from_step='Process_NCCC'
        ... )
        >>> 
        >>> # 僅執行 Escrow 對帳
        >>> result = task.execute(mode='escrow')
        >>> 
        >>> # 僅執行 Daily Check & Entry
        >>> result = task.execute(mode='daily_check')
    """
    
    # 支持的 Pipeline 模式
    SUPPORTED_MODES = ['full', 'escrow', 'installment', 'daily_check', 'entry', 'full_with_entry']
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        初始化任務
        
        Args:
            config_path: 配置文件路徑（TOML）
            config: 配置字典（如果不使用文件）
        """
        self.logger = get_structured_logger(self.__class__.__name__).logger
        
        # 載入配置（只載入一次）
        if config_path:
            self.config = self._load_config(config_path)
        elif config:
            self.config = config
        else:
            # 使用預設配置路徑
            default_config_path = (
                Path(__file__).parent.parent.parent / 
                'config' / 'bank_recon_config.toml'
            )
            self.config = self._load_config(default_config_path)
        
        # 從配置獲取任務信息
        self.name = self.config.get('task', {}).get('name', 'BankRecon')
        self.description = self.config.get('task', {}).get(
            'description', 
            'SPE 銀行對帳與分期報表處理'
        )
        
        # 初始化 checkpoint 管理器
        checkpoint_dir = self.config.get('pipeline', {}).get(
            'checkpoint_dir', 
            './temp/checkpoints'
        )
        self.checkpoint_manager = CheckpointManager(checkpoint_dir)
        
        self.logger.info(f"BankReconTask 已初始化: {self.name}")
    
    # ========================================================================
    # 配置管理方法
    # ========================================================================
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        載入 TOML 配置文件
        
        Args:
            config_path: 配置文件路徑
            
        Returns:
            Dict[str, Any]: 配置字典
            
        Raises:
            FileNotFoundError: 配置文件不存在
            tomllib.TOMLDecodeError: 配置文件格式錯誤
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        
        try:
            with open(config_path, 'rb') as f:
                config = tomllib.load(f)
            self.logger.info(f"成功載入配置文件: {config_path}")
            return config
        except Exception as e:
            self.logger.error(f"載入配置文件失敗: {e}")
            raise
    
    def _get_pipeline_config(self) -> PipelineConfig:
        """
        從任務配置創建 Pipeline 配置
        
        使用已載入的 self.config，避免重複載入。
        
        Returns:
            PipelineConfig: Pipeline 配置對象
        """
        return PipelineConfig(
            name=self.config.get('task', {}).get('name', 'bank_recon'),
            description=self.description,
            task_type="transform",
            stop_on_error=self.config.get('pipeline', {}).get('stop_on_error', True),
            log_level=self.config.get('logging', {}).get('level', 'INFO')
        )
    
    # ========================================================================
    # Pipeline 創建方法（私有）
    # ========================================================================
    
    def _create_full_pipeline(self) -> Pipeline:
        """
        創建完整的銀行對帳 Pipeline（不含 Daily Check & Entry）
        
        包含 9 個步驟:
        1. LoadParametersStep - 載入參數
        2. ProcessCUBStep - 處理國泰
        3. ProcessCTBCStep - 處理中信
        4. ProcessNCCCStep - 處理 NCCC
        5. ProcessUBStep - 處理聯邦
        6. ProcessTaishiStep - 處理台新
        7. AggregateEscrowStep - 匯總 Escrow
        8. LoadInstallmentStep - 載入分期報表
        9. GenerateTrustAccountStep - 生成 Trust Account Fee
        
        Returns:
            Pipeline: 完整的處理 Pipeline
        """
        self.logger.info("創建完整的銀行對帳 Pipeline")
        
        # 使用統一的配置創建 Pipeline
        pipeline_config = self._get_pipeline_config()
        pipeline = Pipeline(pipeline_config)
        
        # 添加所有步驟
        self._add_parameter_step(pipeline)
        self._add_bank_processing_steps(pipeline)
        self._add_escrow_aggregation_step(pipeline)
        self._add_installment_steps(pipeline)
        
        self.logger.info(f"完整 Pipeline 創建完成: {len(pipeline.steps)} 個步驟")
        return pipeline
    
    def _create_full_with_entry_pipeline(self) -> Pipeline:
        """
        創建完整的銀行對帳 Pipeline（包含 Daily Check & Entry）
        
        包含 17 個步驟 (Step 1-17)
        
        Returns:
            Pipeline: 完整的處理 Pipeline
        """
        self.logger.info("創建完整的銀行對帳 Pipeline（含 Daily Check & Entry）")
        
        pipeline_config = self._get_pipeline_config()
        pipeline = Pipeline(pipeline_config)
        
        # 添加所有步驟 (Step 1-9)
        self._add_parameter_step(pipeline)
        self._add_bank_processing_steps(pipeline)
        self._add_escrow_aggregation_step(pipeline)
        self._add_installment_steps(pipeline)
        
        # 添加 Daily Check & Entry 步驟 (Step 10-17)
        self._add_daily_check_steps(pipeline)
        self._add_entry_steps(pipeline)
        
        self.logger.info(f"完整 Pipeline (含 Entry) 創建完成: {len(pipeline.steps)} 個步驟")
        return pipeline
    
    def _create_escrow_pipeline(self) -> Pipeline:
        """
        創建僅執行 Escrow 對帳的 Pipeline
        
        包含步驟 1-7
        
        Returns:
            Pipeline: Escrow 對帳 Pipeline
        """
        self.logger.info("創建 Escrow Only Pipeline")
        
        pipeline_config = self._get_pipeline_config()
        pipeline = Pipeline(pipeline_config)
        
        # 添加 Escrow 相關步驟
        self._add_parameter_step(pipeline)
        self._add_bank_processing_steps(pipeline)
        self._add_escrow_aggregation_step(pipeline)
        
        self.logger.info(f"Escrow Pipeline 創建完成: {len(pipeline.steps)} 個步驟")
        return pipeline
    
    def _create_installment_pipeline(self) -> Pipeline:
        """
        創建僅執行分期報表處理的 Pipeline
        
        包含步驟 1, 8, 9
        
        Returns:
            Pipeline: 分期報表處理 Pipeline
        """
        self.logger.info("創建 Installment Only Pipeline")
        
        pipeline_config = self._get_pipeline_config()
        pipeline = Pipeline(pipeline_config)
        
        # 添加分期報表相關步驟
        self._add_parameter_step(pipeline)
        self._add_installment_steps(pipeline)
        
        self.logger.info(f"Installment Pipeline 創建完成: {len(pipeline.steps)} 個步驟")
        return pipeline
    
    def _create_daily_check_pipeline(self) -> Pipeline:
        """
        創建 Daily Check Pipeline
        
        包含步驟 1, 10-14（需要先有 Escrow 資料）
        
        注意: 此 Pipeline 需要在 Escrow 完成後執行，
              或從已有的 checkpoint 恢復。
        
        Returns:
            Pipeline: Daily Check Pipeline
        """
        self.logger.info("創建 Daily Check Pipeline")
        
        pipeline_config = self._get_pipeline_config()
        pipeline = Pipeline(pipeline_config)
        
        # 添加參數載入步驟
        self._add_parameter_step(pipeline)
        
        # 添加 Daily Check 步驟
        self._add_daily_check_steps(pipeline)
        
        self.logger.info(f"Daily Check Pipeline 創建完成: {len(pipeline.steps)} 個步驟")
        return pipeline
    
    def _create_entry_pipeline(self) -> Pipeline:
        """
        創建 Entry Pipeline
        
        包含步驟 1, 10-17（完整的 Daily Check + Entry）
        
        Returns:
            Pipeline: Entry Pipeline
        """
        self.logger.info("創建 Entry Pipeline")
        
        pipeline_config = self._get_pipeline_config()
        pipeline = Pipeline(pipeline_config)
        
        # 添加參數載入步驟
        self._add_parameter_step(pipeline)
        
        # 添加 Daily Check & Entry 步驟
        self._add_daily_check_steps(pipeline)
        self._add_entry_steps(pipeline)
        
        self.logger.info(f"Entry Pipeline 創建完成: {len(pipeline.steps)} 個步驟")
        return pipeline
    
    # ========================================================================
    # Pipeline 步驟添加輔助方法（私有）
    # ========================================================================
    
    def _add_parameter_step(self, pipeline: Pipeline):
        """添加參數載入步驟"""
        pipeline.add_step(LoadParametersStep(
            name="Load_Parameters",
            description="載入日期範圍、DB路徑等參數",
            config=self.config
        ))
    
    def _add_bank_processing_steps(self, pipeline: Pipeline):
        """添加各銀行處理步驟"""
        # 國泰世華
        pipeline.add_step(ProcessCUBStep(
            name="Process_CUB",
            description="處理國泰世華銀行對帳",
            config=self.config
        ))
        
        # 中國信託
        pipeline.add_step(ProcessCTBCStep(
            name="Process_CTBC",
            description="處理中國信託銀行對帳",
            config=self.config
        ))
        
        # NCCC
        pipeline.add_step(ProcessNCCCStep(
            name="Process_NCCC",
            description="處理 NCCC 銀行對帳",
            config=self.config
        ))
        
        # 聯邦銀行
        pipeline.add_step(ProcessUBStep(
            name="Process_UB",
            description="處理聯邦銀行對帳",
            config=self.config
        ))
        
        # 台新銀行
        pipeline.add_step(ProcessTaishiStep(
            name="Process_Taishi",
            description="處理台新銀行對帳",
            config=self.config
        ))
    
    def _add_escrow_aggregation_step(self, pipeline: Pipeline):
        """添加 Escrow 匯總步驟"""
        pipeline.add_step(AggregateEscrowStep(
            name="Aggregate_Escrow",
            description="匯總所有銀行對帳資料並生成 Escrow Invoice",
        ))
    
    def _add_installment_steps(self, pipeline: Pipeline):
        """添加分期報表處理步驟"""
        # 載入分期報表
        pipeline.add_step(LoadInstallmentStep(
            name="Load_Installment",
            description="載入並處理各銀行分期報表",
        ))
        
        # 生成 Trust Account Fee
        pipeline.add_step(GenerateTrustAccountStep(
            name="Generate_Trust_Account",
            description="生成 Trust Account Fee 工作底稿",
        ))
    
    def _add_daily_check_steps(self, pipeline: Pipeline):
        """添加 Daily Check 步驟 (Step 10-14)"""
        # Step 10: 載入 Daily Check 參數
        pipeline.add_step(LoadDailyCheckParamsStep(
            name="Load_Daily_Check_Params",
            description="載入 FRR/DFR 配置、手續費率、回饋金等參數",
            config=self.config
        ))
        
        # Step 11: 處理 FRR
        pipeline.add_step(ProcessFRRStep(
            name="Process_FRR",
            description="處理財務部 Excel (FRR)",
        ))
        
        # Step 12: 處理 DFR
        pipeline.add_step(ProcessDFRStep(
            name="Process_DFR",
            description="處理 TW Bank Balance Excel (DFR)",
        ))
        
        # Step 13: 計算 APCC
        pipeline.add_step(CalculateAPCCStep(
            name="Calculate_APCC",
            description="計算 APCC 手續費",
        ))
        
        # Step 14: 驗證 Daily Check
        pipeline.add_step(ValidateDailyCheckStep(
            name="Validate_Daily_Check",
            description="驗證 FRR 手續費與請款",
        ))
    
    def _add_entry_steps(self, pipeline: Pipeline):
        """添加 Entry 步驟 (Step 15-17)"""
        # Step 15: 準備會計分錄
        pipeline.add_step(PrepareEntriesStep(
            name="Prepare_Entries",
            description="整理會計科目、處理回饋金、生成寬格式分錄、轉換為長格式分錄、DFR 餘額核對、生成大 Entry",
            config=self.config
        ))
        
        # Step 16: 輸出工作底稿
        pipeline.add_step(OutputWorkpaperStep(
            name="Output_Workpaper",
            description="輸出 Daily Check Excel、Entry Excel、寫入 Google Sheets",
            config=self.config
        ))
    
    # ========================================================================
    # Pipeline 構建方法（公開）
    # ========================================================================
    
    def build_pipeline(self, mode: str = 'full') -> Pipeline:
        """
        構建處理 Pipeline
        
        根據模式選擇對應的 Pipeline 創建方法。
        
        Args:
            mode: Pipeline 模式
                - 'full': 完整流程（Escrow + Installment，不含 Entry）
                - 'full_with_entry': 完整流程（包含 Entry）
                - 'escrow': 僅 Escrow 對帳
                - 'installment': 僅分期報表
                - 'daily_check': Daily Check（Step 10-14）
                - 'entry': Daily Check + Entry（Step 10-17）
                
        Returns:
            Pipeline: 構建好的 Pipeline
            
        Raises:
            ValueError: 無效的 mode 參數
        """
        self.logger.info(f"開始構建 Pipeline (mode={mode})")
        
        # Pipeline 創建方法映射
        pipeline_creators = {
            'full': self._create_full_pipeline,
            'full_with_entry': self._create_full_with_entry_pipeline,
            'escrow': self._create_escrow_pipeline,
            'installment': self._create_installment_pipeline,
            'daily_check': self._create_daily_check_pipeline,
            'entry': self._create_entry_pipeline,
        }
        
        # 驗證 mode
        if mode not in pipeline_creators:
            raise ValueError(
                f"無效的 mode: {mode}. "
                f"可用選項: {', '.join(pipeline_creators.keys())}"
            )
        
        # 調用對應的創建方法
        pipeline = pipeline_creators[mode]()
        
        self.logger.info(
            f"Pipeline 構建完成: {pipeline.config.name} "
            f"({len(pipeline.steps)} 個步驟)"
        )
        
        return pipeline
    
    # ========================================================================
    # Context 準備方法
    # ========================================================================
    
    def prepare_context(self, **kwargs) -> ProcessingContext:
        """
        準備處理上下文
        
        Args:
            **kwargs: 額外的上下文變量
            
        Returns:
            ProcessingContext: 處理上下文
        """
        context = ProcessingContext(
            task_name="bank_recon",
            task_type="transform"
        )
        
        # 從配置設置變量
        context.set_variable('output_dir', self.config.get('output', {}).get('path'))
        context.set_variable('db_path', self.config.get('database', {}).get('path'))
        
        # 設置額外變量
        for key, value in kwargs.items():
            context.set_variable(key, value)
        
        self.logger.info("Context 準備完成")
        return context
    
    # ========================================================================
    # 任務執行方法
    # ========================================================================
    
    def execute(
        self,
        mode: str = 'full',
        save_checkpoints: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        執行任務
        
        Args:
            mode: Pipeline 模式 ('full', 'full_with_entry', 'escrow', 'installment', 'daily_check', 'entry')
            save_checkpoints: 是否保存 checkpoint
            **kwargs: 其他上下文變量
            
        Returns:
            Dict[str, Any]: 執行結果
                {
                    'success': bool,
                    'duration': float,
                    'total_steps': int,
                    'successful_steps': int,
                    'failed_step': Optional[str],
                    'error': Optional[str],
                    'context': ProcessingContext
                }
                
        Example:
            >>> task = BankReconTask()
            >>> result = task.execute()
            >>> if result['success']:
            ...     print(f"成功完成 {result['successful_steps']} 個步驟")
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"開始執行任務: {self.name}")
            self.logger.info(f"執行模式: {mode}")
            self.logger.info("=" * 80)
            
            # 驗證輸入
            validation = self.validate_inputs(mode=mode)
            if not validation['is_valid']:
                self.logger.error("輸入驗證失敗:")
                for error in validation['errors']:
                    self.logger.error(f"  - {error}")
                return {
                    'success': False,
                    'error': 'Input validation failed',
                    'validation': validation
                }
            
            # 顯示警告
            if validation['warnings']:
                self.logger.warning("輸入驗證警告:")
                for warning in validation['warnings']:
                    self.logger.warning(f"  - {warning}")
            
            # 準備上下文
            context = self.prepare_context(**kwargs)
            
            # 構建 Pipeline
            pipeline = self.build_pipeline(mode=mode)
            
            # 執行 Pipeline（帶 checkpoint）
            if save_checkpoints:
                executor = PipelineWithCheckpoint(
                    pipeline, 
                    self.checkpoint_manager
                )
                result = executor.execute_with_checkpoint(
                    context=context,
                    save_after_each_step=True
                )
            else:
                result = pipeline.execute(context)
            
            # 處理結果
            if result['success']:
                self.logger.info("=" * 80)
                self.logger.info("✅ 任務執行成功！")
                self.logger.info("=" * 80)
                self.logger.info(f"執行時間: {result['duration']:.2f} 秒")
                self.logger.info(
                    f"成功步驟: {result['successful_steps']}/{result['total_steps']}"
                )
                
                # 獲取輸出文件信息
                self._log_output_files(context)
                
                # 獲取警告
                if context.has_warnings():
                    warnings = context.warnings
                    if warnings:
                        self.logger.warning(f"執行過程中有 {len(warnings)} 個警告:")
                        for i, warning in enumerate(warnings, 1):
                            self.logger.warning(f"  {i}. {warning}")
            else:
                self.logger.error("=" * 80)
                self.logger.error("❌ 任務執行失敗！")
                self.logger.error("=" * 80)
                
                if result.get('error'):
                    self.logger.error(f"錯誤: {result['error']}")
                if result.get('failed_step'):
                    self.logger.error(f"失敗步驟: {result['failed_step']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"任務執行異常: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'message': '任務執行異常',
                'exception': e
            }
    
    def resume(
        self,
        checkpoint_name: str,
        start_from_step: str,
        save_checkpoints: bool = True
    ) -> Dict[str, Any]:
        """
        從 checkpoint 恢復執行
        
        Args:
            checkpoint_name: Checkpoint 名稱
            start_from_step: 從哪個步驟開始執行
            save_checkpoints: 是否保存新的 checkpoint
            
        Returns:
            Dict[str, Any]: 執行結果
            
        Example:
            >>> task = BankReconTask()
            >>> result = task.resume(
            ...     checkpoint_name='bank_recon_after_Process_CTBC',
            ...     start_from_step='Process_NCCC'
            ... )
        """
        self.logger.info("=" * 80)
        self.logger.info("從 checkpoint 恢復執行")
        self.logger.info(f"Checkpoint: {checkpoint_name}")
        self.logger.info(f"起始步驟: {start_from_step}")
        self.logger.info("=" * 80)
        
        try:
            # 構建 Pipeline（使用 full_with_entry 以包含所有步驟）
            pipeline = self.build_pipeline(mode='full_with_entry')
            
            # 從 checkpoint 恢復
            result = resume_from_checkpoint(
                checkpoint_name=checkpoint_name,
                start_from_step=start_from_step,
                pipeline=pipeline,
                checkpoint_dir=self.checkpoint_manager.checkpoint_dir.parent,
                save_checkpoints=save_checkpoints
            )
            
            if result.get('success'):
                self.logger.info("=" * 80)
                self.logger.info("✅ 恢復執行成功！")
                self.logger.info("=" * 80)
            else:
                self.logger.error("=" * 80)
                self.logger.error("❌ 恢復執行失敗")
                self.logger.error("=" * 80)
            
            return result
            
        except Exception as e:
            self.logger.error(f"恢復執行異常: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'message': f'恢復執行失敗: {str(e)}',
                'exception': e
            }
    
    # ========================================================================
    # 輔助方法
    # ========================================================================
    
    def validate_inputs(self, mode: str = 'full') -> Dict[str, Any]:
        """
        驗證輸入參數和文件
        
        Args:
            mode: Pipeline 模式
        
        Returns:
            Dict[str, Any]: 驗證結果
                {
                    'is_valid': bool,
                    'errors': List[str],
                    'warnings': List[str]
                }
        """
        errors = []
        warnings = []
        
        # 檢查配置
        
        # 檢查 DB 路徑
        db_path = self.config.get('database', {}).get('path')
        if db_path:
            db_path_obj = Path(db_path)
            if not db_path_obj.exists():
                warnings.append(f"DB 路徑不存在: {db_path}")
        else:
            warnings.append("未設置 DB 路徑")
        
        # 檢查輸出目錄
        output_dir = self.config.get('output', {}).get('path')
        output_path = Path(output_dir)
        if output_path.exists() and not output_path.is_dir():
            errors.append(f"輸出路徑存在但不是目錄: {output_dir}")
        
        # 檢查輸入文件（如果配置中有指定）
        input_files = self.config.get('installment', {}).get('reports')
        current_period = self.config.get('dates').get('current_period_start')[:7].replace('-', '')
        
        if input_files:
            for file_type, file_path in input_files.items():
                if file_path and not isinstance(file_path, dict):
                    file_path = file_path.replace('{period}', current_period)
                    file_path_obj = Path(file_path)
                    if not file_path_obj.exists():
                        warnings.append(f"{file_type} 文件不存在: {file_path}")
                elif file_path and isinstance(file_path, dict):
                    for cate, path in file_path.items():
                        path = path.replace('{period}', current_period)
                        path_obj = Path(path)
                        if not path_obj.exists():
                            warnings.append(f"{cate} 文件不存在: {path}")
        
        # 如果是 daily_check 或 entry 模式，檢查 FRR/DFR 文件
        if mode in ['daily_check', 'entry', 'full_with_entry']:
            daily_check_config = self.config.get('daily_check', {})
            
            # 檢查 FRR 文件
            frr_path = daily_check_config.get('frr', {}).get('path', '')
            if frr_path:
                frr_path = frr_path.replace('{period}', current_period)
                if not Path(frr_path).exists():
                    warnings.append(f"FRR 文件不存在: {frr_path}")
            
            # 檢查 DFR 文件
            dfr_path = daily_check_config.get('dfr', {}).get('path', '')
            if dfr_path:
                if not Path(dfr_path).exists():
                    warnings.append(f"DFR 文件不存在: {dfr_path}")
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    def list_checkpoints(self) -> List[Dict]:
        """
        列出可用的 checkpoint
        
        Returns:
            List[Dict]: Checkpoint 資訊列表
                [
                    {
                        'name': str,
                        'timestamp': str,
                        'task_name': str,
                        'last_step': str
                    },
                    ...
                ]
        """
        return list_available_checkpoints(
            checkpoint_dir=self.checkpoint_manager.checkpoint_dir.parent,
            task_name="bank_recon"
        )
    
    def get_pipeline_steps(self, mode: str = 'full') -> List[str]:
        """
        獲取 Pipeline 步驟列表
        
        Args:
            mode: Pipeline 模式
            
        Returns:
            List[str]: 步驟名稱列表
        """
        pipeline = self.build_pipeline(mode=mode)
        return [step.name for step in pipeline.steps]
    
    def _log_output_files(self, context: ProcessingContext):
        """記錄輸出文件信息"""
        output_dir = context.get_variable('output_dir', './output')
        
        # Escrow 文件
        escrow_file = context.get_variable('escrow_filename', '')
        if escrow_file:
            self.logger.info(f"Escrow 文件: {output_dir}/{escrow_file}")
        
        # Trust Account 文件
        trust_file = context.get_variable('trust_account_filename', '')
        if trust_file:
            self.logger.info(f"Trust Account 文件: {output_dir}/{trust_file}")
        
        # Daily Check 文件
        daily_check_file = context.get_variable('daily_check_filename', '')
        if daily_check_file:
            self.logger.info(f"Daily Check 文件: {output_dir}/{daily_check_file}")
        
        # Entry 文件
        entry_file = context.get_variable('entry_filename', '')
        if entry_file:
            self.logger.info(f"Entry 文件: {output_dir}/{entry_file}")


# ============================================================================
# 便捷函數（保持向後兼容）
# ============================================================================

def run_bank_recon(
    config_path: Optional[str] = None,
    mode: str = 'full',
    save_checkpoints: bool = True,
    **kwargs
) -> Dict[str, Any]:
    """
    便捷函數：執行銀行對帳任務
    
    Args:
        config_path: 配置文件路徑（可選）
        mode: 執行模式 ('full', 'full_with_entry', 'escrow', 'installment', 'daily_check', 'entry')
        save_checkpoints: 是否保存 checkpoint
        **kwargs: 其他參數
        
    Returns:
        Dict[str, Any]: 執行結果
        
    Example:
        >>> from spe_bank_recon.tasks.bank_recon import run_bank_recon
        >>> 
        >>> # 執行完整流程
        >>> result = run_bank_recon()
        >>> 
        >>> # 執行完整流程（含 Entry）
        >>> result = run_bank_recon(mode='full_with_entry')
        >>> 
        >>> # 僅執行 Daily Check & Entry
        >>> result = run_bank_recon(mode='entry')
    """
    task = BankReconTask(config_path=config_path)
    return task.execute(
        mode=mode,
        save_checkpoints=save_checkpoints,
        **kwargs
    )


def resume_bank_recon(
    checkpoint_name: str,
    start_from_step: str,
    config_path: Optional[str] = None,
    save_checkpoints: bool = True
) -> Dict[str, Any]:
    """
    便捷函數：從 checkpoint 恢復任務
    
    Args:
        checkpoint_name: Checkpoint 名稱
        start_from_step: 從哪個步驟開始
        config_path: 配置文件路徑（可選）
        save_checkpoints: 是否保存新的 checkpoint
        
    Returns:
        Dict[str, Any]: 執行結果
        
    Example:
        >>> from spe_bank_recon.tasks.bank_recon import resume_bank_recon
        >>> 
        >>> result = resume_bank_recon(
        ...     checkpoint_name='bank_recon_after_Generate_Trust_Account',
        ...     start_from_step='Load_Daily_Check_Params'
        ... )
    """
    task = BankReconTask(config_path=config_path)
    return task.resume(
        checkpoint_name=checkpoint_name,
        start_from_step=start_from_step,
        save_checkpoints=save_checkpoints
    )


def list_bank_recon_checkpoints(config_path: Optional[str] = None) -> List[Dict]:
    """
    便捷函數：列出可用的 checkpoint
    
    Args:
        config_path: 配置文件路徑（可選）
        
    Returns:
        List[Dict]: Checkpoint 列表
        
    Example:
        >>> from spe_bank_recon.tasks.bank_recon import list_bank_recon_checkpoints
        >>> 
        >>> checkpoints = list_bank_recon_checkpoints()
        >>> for cp in checkpoints:
        ...     print(f"{cp['name']} - {cp['timestamp']}")
    """
    task = BankReconTask(config_path=config_path)
    return task.list_checkpoints()


def get_pipeline_by_name(name: str, config_path: Optional[str] = None) -> Pipeline:
    """
    便捷函數：根據名稱獲取 Pipeline
    
    Args:
        name: Pipeline 名稱 ('full', 'full_with_entry', 'escrow', 'installment', 'daily_check', 'entry')
        config_path: 配置文件路徑（可選）
        
    Returns:
        Pipeline: 對應的 Pipeline
        
    Raises:
        ValueError: 未知的 Pipeline 名稱
        
    Example:
        >>> pipeline = get_pipeline_by_name('full_with_entry')
        >>> print(f"Pipeline: {pipeline.config.name}")
    """
    task = BankReconTask(config_path=config_path)
    return task.build_pipeline(mode=name)


# ============================================================================
# 模塊級別常量和工具
# ============================================================================


if __name__ == "__main__":
    # 示例：創建並執行任務
    task = BankReconTask()
    
    # 顯示支持的模式
    print(f"支持的模式: {BankReconTask.SUPPORTED_MODES}")
    
    # 獲取 Pipeline 步驟
    for mode in BankReconTask.SUPPORTED_MODES:
        steps = task.get_pipeline_steps(mode=mode)
        print(f"\n{mode.upper()} 模式步驟 ({len(steps)}):")
        for i, step in enumerate(steps, 1):
            print(f"  {i}. {step}")
