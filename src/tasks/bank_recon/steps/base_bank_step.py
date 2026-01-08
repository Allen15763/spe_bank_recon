"""
BaseBankProcessStep - 銀行處理步驟基礎類

提取所有銀行處理步驟的共同邏輯，消除代碼重複。
使用模板方法模式定義統一的處理流程。
"""

from abc import abstractmethod
from typing import Dict, Any, List

from src.core.pipeline import PipelineStep, StepResult, StepStatus
from src.core.pipeline.context import ProcessingContext
from src.tasks.bank_recon.models import BankDataContainer
from src.utils.database import DuckDBManager


class BaseBankProcessStep(PipelineStep):
    """
    銀行處理步驟基礎類 - 提取共同邏輯

    使用模板方法模式定義標準處理流程:
    1. 提取參數
    2. 處理所有類別
    3. 存儲結果
    4. 記錄總計

    子類只需實現:
    - get_bank_code() - 返回銀行代碼
    - get_processor_class() - 返回對應的 Processor 類
    """

    def __init__(self, config: Dict[str, Any], **kwargs):
        """
        初始化銀行處理步驟

        Args:
            config: 完整的任務配置字典
            **kwargs: 傳遞給 PipelineStep 的其他參數
        """
        super().__init__(**kwargs)
        # 提取當前銀行的配置
        self.bank_config = config.get('banks', {}).get(self.get_bank_code())

        if not self.bank_config:
            raise ValueError(f"找不到銀行配置: {self.get_bank_code()}")

    @abstractmethod
    def get_bank_code(self) -> str:
        """
        返回銀行代碼

        Returns:
            str: 銀行代碼 (cub, ctbc, nccc, ub, taishi)
        """
        pass

    @abstractmethod
    def get_processor_class(self):
        """
        返回對應的 Processor 類

        Returns:
            Type[BankProcessor]: Processor 類
        """
        pass

    def get_categories(self) -> List[str]:
        """
        返回需要處理的類別列表

        從銀行配置中讀取 categories 列表。
        如果配置中沒有 categories，則返回 ['default']。

        Returns:
            List[str]: 類別列表
        """
        return self.bank_config.get('categories', ['default'])

    def execute(self, context: ProcessingContext) -> StepResult:
        """
        執行銀行處理步驟 (模板方法)

        定義統一的處理流程:
        1. 提取公共參數
        2. 處理所有類別
        3. 存儲結果到 context
        4. 計算並記錄總計

        Args:
            context: 處理上下文

        Returns:
            StepResult: 執行結果
        """
        try:
            # 1. 提取公共參數
            params = self._extract_parameters(context)

            self.logger.info(f"處理期間: {params['beg_date']} ~ {params['end_date']}")

            # 2. 處理所有類別
            containers = self._process_categories(params)

            # 3. 存儲結果
            self._store_results(context, containers)

            # 4. 計算並記錄總計
            self._log_totals(containers)

            return StepResult(
                step_name=self.name,
                status=StepStatus.SUCCESS,
                message=f"成功處理 {self.bank_config['name']} 銀行 {len(containers)} 個類別",
                metadata={
                    'bank_code': self.get_bank_code(),
                    'bank_name': self.bank_config['name'],
                    'categories_processed': [c.category for c in containers],
                    'total_amount': sum(c.recon_amount for c in containers),
                    'total_service_fee': sum(c.recon_service_fee for c in containers)
                }
            )

        except Exception as e:
            self.logger.error(f"處理 {self.bank_config['name']} 銀行失敗: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

            return StepResult(
                step_name=self.name,
                status=StepStatus.FAILED,
                error=e,
                message=str(e)
            )

    def _extract_parameters(self, context: ProcessingContext) -> Dict[str, Any]:
        """
        從 context 提取公共參數

        Args:
            context: 處理上下文

        Returns:
            Dict[str, Any]: 參數字典
        """
        return {
            'beg_date': context.get_variable('beg_date'),
            'end_date': context.get_variable('end_date'),
            'last_beg_date': context.get_variable('last_beg_date'),
            'last_end_date': context.get_variable('last_end_date'),
            'db_path': context.get_variable('db_path'),
            'log_file': context.get_variable('log_file')
        }

    def _process_categories(self, params: Dict[str, Any]) -> List[BankDataContainer]:
        """
        處理所有類別

        Args:
            params: 參數字典

        Returns:
            List[BankDataContainer]: 處理結果容器列表
        """
        containers = []
        categories = self.get_categories()

        for category in categories:
            # 記錄類別標題（只在多類別時顯示）
            self._log_category_header(category)

            # 創建 processor
            processor = self._create_processor(category)

            # 處理單個類別
            with DuckDBManager(db_path=params['db_path']) as db_manager:
                container = processor.process(
                    db_manager=db_manager,
                    beg_date=params['beg_date'],
                    end_date=params['end_date'],
                    last_beg_date=params['last_beg_date'],
                    last_end_date=params['last_end_date']
                )

            containers.append(container)

            # 列印摘要
            self._print_summary(container, category)

        return containers

    def _create_processor(self, category: str):
        """
        創建 processor 實例

        Args:
            category: 類別名稱

        Returns:
            BankProcessor: 處理器實例
        """
        ProcessorClass = self.get_processor_class()

        # 獲取對應的表名
        # 如果配置中有該類別的表，使用該表
        # 否則使用 recon 表作為默認值
        tables = self.bank_config.get('tables', {})
        table_name = tables.get(category, tables.get('recon', f"{self.get_bank_code()}_recon"))

        return ProcessorClass(
            bank_code=self.get_bank_code(),
            bank_name=self.bank_config['name'],
            config={
                'table_name': table_name,
                'category': category
            }
        )

    def _log_category_header(self, category: str):
        """
        記錄類別標題

        只在有多個類別或非 default 類別時記錄。

        Args:
            category: 類別名稱
        """
        categories = self.get_categories()

        # 只在多類別或非 default 時顯示標題
        if len(categories) > 1 or category != 'default':
            self.logger.info("=" * 60)
            self.logger.info(f"處理 {self.bank_config['name']} - {category}")
            self.logger.info("=" * 60)

    def _store_results(self, context: ProcessingContext, containers: List[BankDataContainer]):
        """
        存儲結果到 context

        單類別銀行: 存儲為 {bank_code}_container
        多類別銀行: 存儲為 {bank_code}_containers

        Args:
            context: 處理上下文
            containers: 結果容器列表
        """
        bank_code = self.get_bank_code()

        if len(containers) == 1:
            # 單類別銀行 (如 NCCC, Taishi)
            context.add_auxiliary_data(f'{bank_code}_container', containers[0])
        else:
            # 多類別銀行 (如 CUB, CTBC, UB)
            context.add_auxiliary_data(f'{bank_code}_containers', containers)

    def _log_totals(self, containers: List[BankDataContainer]):
        """
        記錄總計

        只在有多個類別時記錄總計。

        Args:
            containers: 結果容器列表
        """
        # 只在多類別時顯示總計
        if len(containers) <= 1:
            return

        total_amount = sum(c.recon_amount for c in containers)
        total_fee = sum(c.recon_service_fee for c in containers)

        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"{self.bank_config['name']} 總計:")
        self.logger.info(f"  總請款金額: {total_amount:,}")
        self.logger.info(f"  總手續費: {total_fee:,}")
        self.logger.info(f"{'=' * 60}\n")

    def _print_summary(self, container: BankDataContainer, category: str = None):
        """
        列印摘要 - 使用統一的格式化工具

        Args:
            container: 資料容器
            category: 類別名稱（可選）
        """
        # 導入摘要格式化工具
        from ..utils.summary_formatter import BankSummaryFormatter

        formatter = BankSummaryFormatter(self.logger)
        formatter.print_container_summary(container, category)
