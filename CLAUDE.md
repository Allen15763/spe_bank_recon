# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**SPE Bank Reconciliation Automation** - An accounting automation system for processing bank reconciliation and monthly closing data for SPE Taiwan. The system transforms raw banking data into accounting workpapers through a modular pipeline framework.

**Tech Stack:** Python 3.11+, Pandas, DuckDB, Google Sheets API, Streamlit (optional)

## Common Commands

### Running the Application

```bash
# Standard execution (escrow reconciliation + installment reports)
python main.py

# Full workflow including accounting entries
python new_main.py

# Programmatic execution with specific mode
python -c "from src.tasks.bank_recon import BankReconTask; task = BankReconTask(); task.execute(mode='full_with_entry')"
```

### Execution Modes

The `BankReconTask.execute(mode=...)` supports 6 modes:

- `'full'` - Steps 1-9: Escrow reconciliation + Installment (no accounting entries)；Main usage
- `'full_with_entry'` - Steps 1-16: Complete workflow including accounting entries；Main usage
- `'escrow'` - Steps 1-7: Bank reconciliation only
- `'installment'` - Steps 8-9: Installment reports only
- `'daily_check'` - Steps 10-14: Daily check validation only
- `'entry'` - Steps 10-16: Daily check + accounting entries

### Checkpoint Operations

```python
# Resume from saved checkpoint
from src.tasks.bank_recon import BankReconTask
task = BankReconTask()
result = task.resume(
    checkpoint_name='bank_recon_transform_after_Process_CTBC',
    start_from_step='Process_NCCC'
)

# List available checkpoints
checkpoints = task.list_checkpoints()
```

### Development Commands

```bash
# Install dependencies
python -m pip install -r requirements.txt

# Run from virtual environment (Windows PowerShell)
./venv/Scripts/activate
python main.py

# Type checking
python -m mypy src/

# Code formatting
python -m black src/

# Run unit tests
python run_all_tests.py

# Run specific test
python tests/utils/test_config_manager.py
python tests/core/datasources/test_datasource_base.py
python tests/utils/test_file_utils.py

# Run verification tests
python tests/verify_iteration1.py
python tests/verify_iteration2.py
python tests/verify_iteration3.py
```

## Recent Architecture Improvements (2026-01)

The codebase has undergone three iterations of systematic improvements to enhance maintainability, reliability, and extensibility:

### Iteration 1: Critical Fixes (P0) ✅

**Objective**: Fix stability issues affecting production reliability.

1. **Unified Logging Framework**
   - Migrated `DuckDBManager` from loguru to project's standard logging
   - All modules now use `get_logger()` from `src.utils.logging`
   - Consistent log format and centralized configuration

2. **Thread-Safe ConfigManager**
   - Implemented double-checked locking for singleton pattern
   - Added `threading.Lock()` to prevent race conditions
   - Safe for multi-threaded environments

3. **DataSource Compliance**
   - `GoogleSheetsManager` now inherits from `DataSource` base class
   - Standardized API: `read()` and `write()` methods
   - Supports caching and DataSourceFactory integration
   - Backward compatible with deprecated `get_data()` and `write_data()`

**Verification**: `python tests/verify_iteration1.py` (All tests pass)

### Iteration 2: Code Quality Improvements (P1) ✅

**Objective**: Eliminate code duplication, improve maintainability, enhance caching.

1. **Eliminated Bank Step Duplication (87.4% reduction)**
   - Created `BaseBankProcessStep` abstract class using template method pattern
   - Reduced bank processing steps from 120-390 lines to 15-20 lines each
   - Total code reduction: 450+ lines
   - Files:
     - [src/tasks/bank_recon/steps/base_bank_step.py](src/tasks/bank_recon/steps/base_bank_step.py) - Base class
     - [src/tasks/bank_recon/utils/summary_formatter.py](src/tasks/bank_recon/utils/summary_formatter.py) - Unified summary output

2. **Enhanced Error Handling & Logging**
   - Added comprehensive logging to `file_utils.py` (previously silent failures)
   - Functions now properly log: `validate_file_path()`, `ensure_directory_exists()`, `get_file_info()`, `copy_file_safely()`
   - Log levels: warning, info, debug, error

3. **Advanced Caching Mechanism**
   - Multi-level caching with MD5-based cache keys
   - TTL (Time-To-Live) automatic expiration
   - LRU (Least Recently Used) eviction strategy
   - Configuration in `DataSourceConfig`:
     - `cache_ttl_seconds` (default: 300s)
     - `cache_max_items` (default: 10)
     - `cache_eviction_policy` (default: "lru")

**Verification**: `python tests/verify_iteration2.py` (All tests pass)

### Iteration 3: Extensibility & Testing (P2) ✅

**Objective**: Prepare for future expansion, establish testing framework.

1. **Configuration-Driven Bank Steps**
   - Banks now configured in `[pipeline.bank_processing]` section
   - Dynamic step addition based on `enabled_banks` list
   - Easy to add/remove/reorder banks without code changes
   - Configuration: [src/config/bank_recon_config.toml](src/config/bank_recon_config.toml#L18-L30)

2. **Unit Test Coverage (28 tests)**
   - **ConfigManager**: 7 tests (singleton, thread safety, config reading)
   - **DataSource**: 8 tests (cache hit/miss, TTL, LRU eviction)
   - **file_utils**: 13 tests (validation, directory creation, file copy)
   - Test files:
     - [tests/utils/test_config_manager.py](tests/utils/test_config_manager.py)
     - [tests/core/datasources/test_datasource_base.py](tests/core/datasources/test_datasource_base.py)
     - [tests/utils/test_file_utils.py](tests/utils/test_file_utils.py)

**Verification**: `python tests/verify_iteration3.py` (All tests pass)

### Summary of Improvements

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Code Duplication (Bank Steps) | 87.4% | 0% | ✅ Eliminated |
| Lines of Code | Baseline | -450 lines | ✅ Reduced |
| Logging Coverage | Partial | Complete | ✅ All modules |
| Cache Mechanism | Simple | TTL + LRU + Multi-level | ✅ Enhanced |
| Thread Safety | No | Yes | ✅ Implemented |
| Unit Tests | 0 | 28 | ✅ Established |
| Configuration | Hardcoded | Driven | ✅ Flexible |

---

## Architecture Overview

### Three-Layer Pipeline Framework

**Layer 1: Core Pipeline Infrastructure** (`src/core/pipeline/`)
- `PipelineStep` - Abstract base class for all processing steps with retry, timeout, validation
- `Pipeline` - Orchestrates sequential step execution with error handling
- `ProcessingContext` - Central data carrier storing main data, auxiliary data, variables, errors, and execution history
- `Checkpoint` - Save/restore mechanism supporting Parquet and Pickle formats

**Layer 2: Task Orchestration** (`src/tasks/bank_recon/`)
- `BankReconTask` - Main task class managing the 17-step workflow
- `pipeline_orchestrator.py` - Builds pipelines based on execution mode
- `steps/` - 16 step implementations containing domain-specific business logic
- `utils/` - Bank processors, validators, calculators, formatters

**Layer 3: Data Sources** (`src/core/datasources/`)
- `DataSource` abstraction with implementations for Excel, CSV, Parquet, Google Sheets
- `DuckDBManager` - SQL query engine for complex data transformations
- `DataSourceFactory` - Factory pattern for creating appropriate data sources

### 17-Step Processing Pipeline

The complete workflow (`mode='full_with_entry'`) executes:

**Phase 1: Escrow Reconciliation (Steps 1-7)**
1. Load parameters from configuration
2. Process CUB (Cathay Bank - individual + corporate)
3. Process CTBC (Chinatrust - installment + non-installment)
4. Process NCCC (Credit card processing)
5. Process UB (Federal Bank - installment + non-installment)
6. Process Taishi (Taishin Bank)
7. Aggregate escrow invoice consolidation

**Phase 2: Installment Processing (Steps 8-9)**
8. Load installment reports from all banks
9. Generate trust account fee accrual calculations

**Phase 3: Daily Check & Validation (Steps 10-14)**
10. Load daily check parameters
11. Process FRR (Financial records)
12. Process DFR (Deferred payment records)
13. Calculate APCC fees
14. Validate daily check reconciliation

**Phase 4: Accounting Entries (Steps 15-16)**
15. Prepare journal entries
16. Output workpaper (Excel export with formatting)
17. Additional processing or final output

### ProcessingContext: The Data Pipeline Carrier

The `ProcessingContext` is the central object passed through all pipeline steps:

```python
context.update_data(df)                    # Set main DataFrame
context.add_auxiliary_data(name, df)       # Store named auxiliary data
context.get_auxiliary_data(name)           # Retrieve auxiliary data
context.set_variable(key, value)           # Store variables (strings, ints, etc.)
context.get_variable(key, default)         # Retrieve variables
context.add_error(msg)                     # Track errors
context.add_warning(msg)                   # Track warnings
```

**Key Design:** Each step receives the context, processes data, updates the context, and passes it to the next step. The context maintains execution history, validation results, and both main and auxiliary DataFrames.

### Checkpoint Mechanism

**Purpose:** Enable fault tolerance and workflow resumption from any step.

**Storage Structure:**
```
checkpoints/bank_recon_transform_after_Process_CTBC/
├── data.parquet                 # Main DataFrame
├── variables.json               # Context variables
├── metadata.json                # Execution metadata
└── auxiliary_data/              # Named auxiliary DataFrames
    ├── cub_containers.parquet
    ├── ctbc_containers.parquet
    └── ...
```

**Naming Convention:** `{task_name}_{task_type}_after_{step_name}`

**Usage:** Checkpoints are saved automatically after each step when `save_checkpoints=True` (default). Resume using `task.resume(checkpoint_name, start_from_step)`.

## Configuration System

### Configuration Files (`src/config/`)

- `bank_recon_config.toml` - Main task configuration (dates, banks, file paths)
- `bank_recon_entry_monthly.toml` - Monthly accounting entry rules
- `generate_spe_bank_recon_wp_config.toml` - Workpaper generation settings
- `config.toml` - Global system configuration

### Key Configuration Sections

**Pipeline Configuration (New in Iteration 3):**
```toml
[pipeline.bank_processing]
# 啟用的銀行列表 (按處理順序)
enabled_banks = ["cub", "ctbc", "nccc", "ub", "taishi"]

# 處理模式
processing_mode = "sequential"

# 如果設置為 true，只處理 enabled=true 的銀行
respect_bank_enabled_flag = true
```

**Usage Examples:**
```toml
# Temporarily disable a bank
enabled_banks = ["cub", "ctbc", "nccc", "taishi"]  # removed "ub"

# Change processing order
enabled_banks = ["ctbc", "cub", "nccc", "ub", "taishi"]  # process CTBC first

# Use bank enabled flag
[banks.cub]
enabled = false  # temporarily disable CUB
```

**Dates Configuration:**
```toml
[dates]
current_period_start = "2025-12-01"
current_period_end = "2025-12-31"
last_period_start = "2025-11-01"
last_period_end = "2025-11-30"
```

**Bank Configuration:** Each of 5 banks (CUB, CTBC, NCCC, UB, Taishi) has:
- Bank code and display name
- Categories (individual/corporate/installment)
- Table names for DuckDB storage
- File paths and formats
- `enabled` flag (can be used with `respect_bank_enabled_flag`)

**Output Configuration:**
```toml
[output]
path = "./output/"
escrow_filename = "Escrow_recon_{period}_renew.xlsx"
trust_account_filename = "filing data for Trust Account Fee Accrual-SPETW.xlsx"
```

**ConfigManager Usage:**
```python
from src.utils.config import ConfigManager

config = ConfigManager()  # Singleton
current_period = config.get('dates.current_period_start')
bank_name = config.get('banks.cub.name')
output_path = config.get('output.path')
```

## Key Design Patterns & Conventions

### BaseBankProcessStep Pattern (New in Iteration 2)

**Purpose**: Eliminate code duplication across bank processing steps using template method pattern.

**Architecture**:
```python
# Base class defines the standard flow
class BaseBankProcessStep(PipelineStep):
    @abstractmethod
    def get_bank_code(self) -> str:
        """Return bank code (cub, ctbc, nccc, ub, taishi)"""
        pass

    @abstractmethod
    def get_processor_class(self):
        """Return corresponding Processor class"""
        pass

    def _execute(self, context: ProcessingContext) -> StepResult:
        """Template method - defines processing flow"""
        # 1. Extract common parameters
        params = self._extract_parameters(context)
        # 2. Process all categories
        containers = self._process_categories(params)
        # 3. Store results
        self._store_results(context, containers)
        # 4. Calculate and log totals
        self._log_totals(containers)
        return StepResult(...)
```

**Implementation Example**:
```python
# Before: 280 lines of duplicated code
# After: 15 lines
class ProcessCUBStep(BaseBankProcessStep):
    """處理國泰世華銀行對帳步驟"""

    def get_bank_code(self) -> str:
        return 'cub'

    def get_processor_class(self):
        return CUBProcessor
```

**Benefits**:
- Reduced code from 120-390 lines to 15-20 lines per bank step
- Unified logging and error handling
- Easy to add new banks (just implement 2 methods)
- Consistent processing flow across all banks

**Location**: [src/tasks/bank_recon/steps/base_bank_step.py](src/tasks/bank_recon/steps/base_bank_step.py)

### BankDataContainer Model

Unified data structure for all bank processing results:

```python
@dataclass
class BankDataContainer:
    bank_code: str              # cub, ctbc, nccc, ub, taishi
    bank_name: str              # Display name
    category: str               # individual/nonindividual/installment/default
    raw_data: pd.DataFrame
    recon_amount: int           # Current period claim
    recon_service_fee: int      # Service fees
    trust_account_fee: int      # Trust account fees
    # ... additional fields
```

### Error Handling Strategy

1. Each step wraps processing in try-catch
2. Errors tracked in `ProcessingContext.errors`
3. Pipeline can continue or stop on error (configurable)
4. Detailed error messages with step name and context
5. Validation results stored in `ProcessingContext._validations`

### Logging System

Structured logging with colored console output and file rotation:

```python
from src.utils import get_logger

logger = get_logger(__name__)
logger.info("Processing started")
logger.warning("Missing data", extra={'step': 'Process_CUB'})
logger.error("Validation failed", extra={'errors': error_list})
```

**Log Format:** `[TIMESTAMP] | LEVEL | logger_name | function:line | message`

**Configuration:** Controlled via `[logging]` section in config.toml (level, colors, rotation, file/console output)

## Important Implementation Notes

### Type Hints Required

All functions must use Python 3.11+ type annotations:
```python
def process_data(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Process the data according to configuration."""
    ...
```

### Python 3.11 Features Used

- `match` statements for pattern matching
- Type parameter syntax
- Generic type hints (e.g., `PipelineStep[T]`)
- `|` union type syntax

### Module Organization

- **Steps:** Named `step_##_descriptive_name.py` (e.g., `step_02_process_cub.py`)
- **Classes:** Descriptive nouns (e.g., `ProcessCTBCStep`, `BankProcessor`)
- **Period Format:** YYYYMM (e.g., 202512 = December 2025)

### Data Source Factory Pattern

Create data sources using the factory:

```python
from src.core.datasources import DataSourceFactory

# Auto-detect type from extension
source = DataSourceFactory.create('path/to/file.xlsx')

# Explicit type
source = DataSourceFactory.create('path/to/file.csv', source_type='csv')

# Read data
df = source.read()

# Write data
source.write(df)
```

## Project Structure Context

- 核心抽象+任務分散
  - 核心抽象在 /project/src/core/pipeline,步驟在 tasks/
    - 統一的管道模組: "/project/src/core/pipeline"
    - checkpoits機制:"/project/src/core/pipeline/checkpoint.py"
  - 通用輔助系統如config_manager、logging使用通用模組，utils/config, utils/logging
  - 資料的輸入與輸出統一使core/datasources
    - 統一的資料源模組: "/project/src/core/datasources"

執行目錄： "/project/main.py"

project/
├── src/
│   ├── core/
│   │   ├── pipeline/          # Pipeline 核心框架
│   │   │   ├── base.py        # PipelineStep 基類、StepResult
│   │   │   ├── pipeline.py    # Pipeline、PipelineBuilder
│   │   │   ├── context.py     # ProcessingContext
│   │   │   └── checkpoint.py  # Checkpoint 機制，每個步驟設置斷點儲存context避免失敗後要重頭執行
│   │   └── datasources/       # 資料源抽象層
│   │       ├── base.py        # DataSource 基類
│   │       ├── excel_source.py
│   │       ├── google_sheet_source.py
│   │       ├── duckdb_manager.py  # DuckDBManager
│   │       └── factory.py     # DataSourceFactory
│   ├── tasks/
│   │   └── task_name/         # 架構下的某個任務
│   │       ├── pipeline_orchestrator.py  # 任務步驟的編排
│   │       ├── steps/         # 任務的處理步驟，包含詳細業務邏輯
│   │       ├── models/        # 任務的資料模型，如果有需要抽象化的物件時使用。
│   │       └── utils/         # 任務的工具函數
│   ├── config/
│   │   ├── config.toml        # 全域配置
│   │   ├── <task_name>_config.toml  # 任務專用配置
│   │   └── <task_nam>_special_config.toml  # 任務專用的特殊配置，當任務配置內容太多或有特殊調整配置時獨立出一個檔案
│   │── utils/
│   │   ├── config/            # ConfigManager
│   │   ├── logging/           # Logger, StructuredLogger
│   │   └── helpers/           # file_utils
│   └── ui/  # if any
└── main.py

### Core Directories

- `src/core/pipeline/` - Pipeline framework (PipelineStep, Pipeline, Context, Checkpoint)
- `src/core/datasources/` - Data source abstraction layer
- `src/tasks/bank_recon/` - Bank reconciliation task implementation
- `src/utils/` - Cross-cutting concerns (config, logging, database, file operations)
- `src/config/` - TOML configuration files
- `checkpoints/` - Pipeline checkpoint storage
- `output/` - Generated workpapers and reports
- `logs/` - Application logs

### When Adding New Steps

1. Inherit from `PipelineStep` in `src/core/pipeline/base.py`
2. Implement `_execute(context: ProcessingContext) -> StepResult`
3. Add step to pipeline builder in `pipeline_orchestrator.py`
4. Update configuration if needed
5. Follow naming convention: `step_##_descriptive_name.py`

### When Adding New Banks (Updated in Iteration 2 & 3)

**Step 1: Add Bank Configuration**
```toml
# In src/config/bank_recon_config.toml

# Add to pipeline configuration
[pipeline.bank_processing]
enabled_banks = ["cub", "ctbc", "nccc", "ub", "taishi", "new_bank"]  # Add your bank

# Define bank configuration
[banks.new_bank]
code = "new_bank"
name = "新銀行名稱"
enabled = true
categories = ["category1", "category2"]

[banks.new_bank.tables]
category1 = "new_bank_category1_statement"
category2 = "new_bank_category2_statement"

[banks.new_bank.fields]
field1 = "field1_column_name"
field2 = "field2_column_name"
```

**Step 2: Create Processor Class**
```python
# In src/tasks/bank_recon/utils/processors/new_bank_processor.py

from .base_processor import BankProcessor

class NewBankProcessor(BankProcessor):
    """新銀行處理器"""

    def process(self, db_manager, beg_date, end_date, last_beg_date, last_end_date):
        # Implement bank-specific processing logic
        # Return BankDataContainer
        pass
```

**Step 3: Create Step Class (Only 15 lines!)**
```python
# In src/tasks/bank_recon/steps/step_##_process_new_bank.py

from .base_bank_step import BaseBankProcessStep
from ..utils.processors import NewBankProcessor

class ProcessNewBankStep(BaseBankProcessStep):
    """處理新銀行對帳步驟"""

    def get_bank_code(self) -> str:
        return 'new_bank'

    def get_processor_class(self):
        return NewBankProcessor
```

**Step 4: Register in Pipeline (Automatic!)**

No code changes needed! The configuration-driven system will automatically add the step based on `enabled_banks` in the config.

**Step 5: Test**
```bash
# Add unit tests following the pattern
python tests/utils/test_new_bank_processor.py
```

**That's it!** The new bank will be automatically integrated into the pipeline.

---

## Testing Guide

### Unit Tests (New in Iteration 3)

The project now has comprehensive unit test coverage for core modules:

**Running Tests**:
```bash
# Run all tests
python run_all_tests.py

# Run specific test file
python tests/utils/test_config_manager.py
python tests/core/datasources/test_datasource_base.py
python tests/utils/test_file_utils.py

# Run verification tests
python tests/verify_iteration1.py
python tests/verify_iteration2.py
python tests/verify_iteration3.py
```

**Test Structure**:
```
tests/
├── utils/
│   ├── test_config_manager.py      # ConfigManager tests (7 tests)
│   └── test_file_utils.py          # file_utils tests (13 tests)
├── core/
│   └── datasources/
│       └── test_datasource_base.py # DataSource cache tests (8 tests)
├── verify_iteration1.py            # Iteration 1 verification
├── verify_iteration2.py            # Iteration 2 verification
└── verify_iteration3.py            # Iteration 3 verification
```

**Writing New Tests**:
```python
import unittest
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class TestYourFeature(unittest.TestCase):
    def test_something(self):
        # Your test code
        self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()
```

### Integration Testing

**End-to-End Test**:
```bash
# Test full workflow
python main.py

# Test with accounting entries
python new_main.py

# Test specific mode
python -c "from src.tasks.bank_recon import BankReconTask; task = BankReconTask(); task.execute(mode='escrow')"
```

**Checkpoint Testing**:
```bash
# Create checkpoint during execution
task = BankReconTask()
result = task.execute(mode='full', save_checkpoints=True)

# Resume from checkpoint
task.resume(checkpoint_name='bank_recon_transform_after_Process_CUB', start_from_step='Process_CTBC')
```


## Common Workflow Patterns

### Standard Execution Flow

```python
# 1. Initialize task
task = BankReconTask()

# 2. Validate inputs (optional but recommended)
validation = task.validate_inputs(mode='full_with_entry')
if not validation['is_valid']:
    raise Exception(validation['errors'])

# 3. Execute with checkpoint support
result = task.execute(mode='full_with_entry', save_checkpoints=True)

# 4. Check result
if result['success']:
    print(f"Completed {result['successful_steps']}/{result['total_steps']} steps")
else:
    print(f"Errors: {result['errors']}")
```

### Recovery from Failure

```python
# 1. List available checkpoints
checkpoints = task.list_checkpoints()

# 2. Resume from last successful step
result = task.resume(
    checkpoint_name='bank_recon_transform_after_step_06',
    start_from_step='Aggregate_Escrow'  # Step 7
)
```

### Custom Pipeline Building

```python
from src.core.pipeline import Pipeline, PipelineConfig, ProcessingContext
from src.tasks.bank_recon.steps import LoadParametersStep, ProcessCUBStep

# 1. 創建 Pipeline 配置
pipeline_config = PipelineConfig(
    name='custom_recon',
    description='Custom bank reconciliation pipeline',
    task_type='transform',
    stop_on_error=True,
    log_level='INFO'
)

# 2. 創建 Pipeline 實例
pipeline = Pipeline(pipeline_config)

# 3. 添加步驟
pipeline.add_step(LoadParametersStep(
    name="Load_Parameters",
    description="載入參數",
    config=config  # 從 TOML 載入的配置
))

pipeline.add_step(ProcessCUBStep(
    name="Process_CUB",
    description="處理國泰世華銀行對帳",
    config=config
))

# 4. 準備 Context 並執行
context = ProcessingContext(
    task_name="custom_recon",
    task_type="transform"
)
result = pipeline.execute(context)
```

**注意**: 實際專案中不使用 `PipelineBuilder`，而是直接使用 `Pipeline` 對象配合 `PipelineConfig` 來構建 pipeline。
