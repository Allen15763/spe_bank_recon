# Metadata Builder Plugin

髒資料處理工具類，用於處理高度不可控的源資料（特別是 Excel）。

## 特性

- **Bronze Layer**: 原樣落地，`dtype=str` 讀取，自動添加 metadata
- **Silver Layer**: 欄位映射（支援 regex）、安全類型轉換、Circuit Breaker
- **配置驅動**: SchemaConfig 定義欄位映射和驗證規則
- **Pipeline 友好**: 作為工具類被 Step 呼叫，不綁定執行流程
- **可移植**: 可複製到其他專案直接使用

## 安裝

此插件為獨立模組，無需額外安裝。確保專案已安裝:
- pandas
- (可選) pyyaml - 用於 YAML 配置

## 快速開始

### 基本使用

```python
from src.utils.metadata_builder import MetadataBuilder, SchemaConfig, ColumnSpec

# 定義 Schema
schema = SchemaConfig(columns=[
    ColumnSpec(source='交易日期', target='date', dtype='DATE', required=True),
    ColumnSpec(source='金額', target='amount', dtype='BIGINT'),
    ColumnSpec(source='.*備註.*', target='remarks', dtype='VARCHAR'),  # regex
])

# 使用 MetadataBuilder
builder = MetadataBuilder()
df = builder.build('./input/bank.xlsx', schema, sheet_name=0, header_row=2)
```

### 配合 DuckDBManager 使用

```python
from src.utils.metadata_builder import MetadataBuilder, SchemaConfig, ColumnSpec
from src.utils.duckdb_manager import DuckDBManager

builder = MetadataBuilder()

# Bronze: 讀取原始資料
df_raw = builder.extract(
    './input/bank.xlsx',
    sheet_name='B2B',
    header_row=2,
    add_metadata=True
)

# Silver: 清洗轉換
schema = SchemaConfig(columns=[
    ColumnSpec(source='交易日期', target='date', dtype='DATE', required=True),
    ColumnSpec(source='金額', target='amount', dtype='BIGINT'),
])
df_clean = builder.transform(df_raw, schema)

# 存入 DuckDB
with DuckDBManager('./db/data.duckdb') as db:
    db.create_table_from_df('bronze_bank', df_raw, if_exists='replace')
    db.create_table_from_df('silver_bank', df_clean, if_exists='replace')
```

### 在 Pipeline Step 中使用

```python
from src.core.pipeline import PipelineStep, StepResult
from src.utils.metadata_builder import MetadataBuilder, SchemaConfig
from src.utils.duckdb_manager import DuckDBManager

class ProcessBankStep(PipelineStep):
    def __init__(self, schema_config: SchemaConfig, **kwargs):
        super().__init__(name="ProcessBank", **kwargs)
        self.schema_config = schema_config

    def execute(self, context):
        file_path = context.get('file_path')
        db_path = context.get('db_path')

        builder = MetadataBuilder()
        
        # Bronze + Silver
        df_raw = builder.extract(file_path, add_metadata=True)
        df_clean = builder.transform(df_raw, self.schema_config)

        with DuckDBManager(db_path) as db:
            db.create_table_from_df('bank_data', df_clean, if_exists='replace')

        return StepResult(status='SUCCESS', data=df_clean)
```

## API 參考

### MetadataBuilder

| 方法 | 說明 |
|------|------|
| `extract(file_path, ...)` | Bronze: 讀取源檔案，全字串 |
| `transform(df, schema_config)` | Silver: 欄位映射、類型轉換 |
| `build(file_path, schema_config)` | Bronze + Silver 組合 |
| `extract_and_preview(file_path)` | 預覽檔案結構 |
| `get_excel_sheets(file_path)` | 取得 Sheet 列表 |

### SchemaConfig

| 屬性 | 類型 | 預設值 | 說明 |
|------|------|--------|------|
| `columns` | list[ColumnSpec] | [] | 欄位定義 |
| `circuit_breaker_threshold` | float | 0.3 | NULL 比例閾值 |
| `filter_empty_rows` | bool | True | 過濾空行 |
| `preserve_unmapped` | bool | False | 保留未映射欄位 |

### ColumnSpec

| 屬性 | 類型 | 預設值 | 說明 |
|------|------|--------|------|
| `source` | str | - | 來源欄位名 (支援 regex) |
| `target` | str | - | 目標欄位名 |
| `dtype` | str | "VARCHAR" | 目標類型 |
| `required` | bool | False | 是否必要 |
| `default` | Any | None | 預設值 |
| `date_format` | str | None | 日期格式 |

### 支援的類型

| dtype | 說明 |
|-------|------|
| `VARCHAR` | 字串 (預設) |
| `BIGINT` / `INTEGER` | 整數 |
| `DOUBLE` / `FLOAT` | 浮點數 |
| `DATE` | 日期 |
| `DATETIME` / `TIMESTAMP` | 日期時間 |
| `BOOLEAN` | 布林值 |

## 異常類

| 異常 | 說明 |
|------|------|
| `SourceFileError` | 源檔案讀取失敗 |
| `SheetNotFoundError` | Sheet 不存在 |
| `SchemaValidationError` | 必要欄位缺失 |
| `CircuitBreakerError` | NULL 比例超過閾值 |
| `ColumnMappingError` | 欄位映射失敗 |

## 模組結構

```
metadata_builder/
├── __init__.py          # 插件入口
├── config.py            # 配置類
├── exceptions.py        # 自定義異常
├── reader.py            # SourceReader
├── builder.py           # MetadataBuilder
├── processors/
│   ├── bronze.py        # BronzeProcessor
│   └── silver.py        # SilverProcessor
├── transformers/
│   ├── column_mapper.py # ColumnMapper
│   └── type_caster.py   # SafeTypeCaster
├── validation/
│   └── circuit_breaker.py
└── README.md
```

## 版本

- v1.0.0 - 初始版本
