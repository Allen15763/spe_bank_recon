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
```

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

### When Adding New Banks

1. Add bank configuration to `bank_recon_config.toml` under `[banks.*]`
2. Create step in `src/tasks/bank_recon/steps/`
3. Implement `BankProcessor` subclass in `src/tasks/bank_recon/utils/`
4. Add to pipeline builder based on execution mode
5. Update `BankDataContainer` if new fields needed


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
