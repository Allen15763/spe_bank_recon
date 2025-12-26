# DataSources 模組文檔

## 📋 概述

DataSources 模組提供了統一的數據源抽象層，支援多種數據格式，包括 Excel、CSV 和 Parquet。所有數據源實現相同的接口，便於在不同數據格式間切換。

## 🚀 快速開始

### 基本使用

```python
from src.core.datasources import DataSourceFactory, create_quick_source

# 方式一：使用工廠自動識別檔案類型
source = DataSourceFactory.create_from_file('data.xlsx')
df = source.read()

# 方式二：快速創建
source = create_quick_source('data.csv')
df = source.read()

# 方式三：使用上下文管理器
with DataSourceFactory.create_from_file('data.xlsx') as source:
    df = source.read()
# 自動關閉資源
```

## 🏗️ 架構設計

### 核心組件

```
datasources/
├── base.py           # DataSource 抽象基類
├── config.py         # 配置管理
├── factory.py        # 工廠模式實現
├── excel_source.py   # Excel 實現
├── csv_source.py     # CSV 實現
├── parquet_source.py # Parquet 實現
└── __init__.py       # 模組導出
```

### 類別圖

```
DataSource (ABC)
    ├── ExcelSource
    ├── CSVSource
    └── ParquetSource

DataSourceFactory
    └── create_from_file()
    └── create()
    └── register_source()

DataSourcePool
    └── add_source()
    └── get_source()
    └── close_all()
```

## 📊 支援的數據源

### Excel (`ExcelSource`)

**特點：**
- 支援多工作表
- 可指定列和資料類型
- 支援讀取和寫入

**使用範例：**
```python
from src.core.datasources import ExcelSource

# 從檔案創建
source = ExcelSource.create_from_file('data.xlsx', sheet_name='Sheet1')

# 讀取資料
df = source.read()

# 指定參數讀取
df = source.read(sheet_name='Sheet2', usecols=['A', 'B', 'C'])

# 獲取工作表列表
sheets = source.get_sheet_names()

# 讀取所有工作表
all_sheets = source.read_all_sheets()

# 寫入資料
source.write(df, sheet_name='Result', index=False)

# 寫入多個工作表
source.write_multiple_sheets({
    'Sheet1': df1,
    'Sheet2': df2
}, output_path='output.xlsx')
```

### CSV (`CSVSource`)

**特點：**
- 高效的文本格式
- 支援大檔案分塊處理
- 可自定義分隔符和編碼

**使用範例：**
```python
from src.core.datasources import CSVSource

# 從檔案創建
source = CSVSource.create_from_file('data.csv', sep=',', encoding='utf-8')

# 基本讀取
df = source.read()

# 帶條件讀取
df = source.read(query="amount > 1000")

# 分塊讀取大檔案
chunks = source.read_in_chunks(chunk_size=10000)
for chunk in chunks:
    process(chunk)

# 追加資料
source.append_data(new_df)
```

### Parquet (`ParquetSource`)

**特點：**
- 列式存儲，高壓縮比
- 保留資料類型
- 支援 Schema 管理

**使用範例：**
```python
from src.core.datasources import ParquetSource

# 從檔案創建
source = ParquetSource.create_from_file('data.parquet')

# 讀取資料
df = source.read()

# 只讀取特定列
df = source.read(columns=['id', 'amount'])

# 獲取 Schema
schema = source.get_schema()

# 寫入資料
source.write(df, compression='snappy')
```

## 🔧 進階用法

### 使用配置創建數據源

```python
from src.core.datasources import (
    DataSourceConfig, 
    DataSourceType, 
    DataSourceFactory
)

# 建立配置
config = DataSourceConfig(
    source_type=DataSourceType.EXCEL,
    connection_params={
        'file_path': 'data.xlsx',
        'sheet_name': 'Sheet1',
        'header': 0
    },
    cache_enabled=True,
    encoding='utf-8'
)

# 使用配置創建數據源
source = DataSourceFactory.create(config)
```

### 數據源池管理

```python
from src.core.datasources import DataSourcePool, DataSourceFactory

# 創建數據源池
pool = DataSourcePool()

# 添加數據源
source1 = DataSourceFactory.create_from_file('file1.xlsx')
source2 = DataSourceFactory.create_from_file('file2.csv')

pool.add_source('excel_data', source1)
pool.add_source('csv_data', source2)

# 使用數據源
excel_df = pool.get_source('excel_data').read()
csv_df = pool.get_source('csv_data').read()

# 列出所有數據源
print(pool.list_sources())

# 關閉所有連接
pool.close_all()
```

### 帶快取讀取

```python
# 第一次讀取會快取
df1 = source.read_with_cache()

# 第二次讀取直接返回快取
df2 = source.read_with_cache()

# 清除快取
source.clear_cache()
```

### 獲取元數據

```python
metadata = source.get_metadata()
print(f"檔案路徑: {metadata['file_path']}")
print(f"檔案大小: {metadata['file_size']} bytes")
print(f"列名: {metadata['column_names']}")
print(f"行數: {metadata['num_rows']}")
```

## 📝 最佳實踐

1. **選擇適合的數據源**
   - 小數據 + 需要編輯：Excel
   - 大數據 + 簡單格式：CSV
   - 長期存儲 + 高壓縮：Parquet

2. **使用上下文管理器**
   ```python
   with DataSourceFactory.create_from_file('data.xlsx') as source:
       df = source.read()
   # 自動關閉
   ```

3. **大檔案使用分塊**
   ```python
   # CSV 大檔案分塊讀取
   for chunk in source.read_in_chunks(chunk_size=10000):
       process(chunk)
   ```

4. **啟用快取避免重複讀取**
   ```python
   config = DataSourceConfig(
       source_type=DataSourceType.EXCEL,
       connection_params={'file_path': 'data.xlsx'},
       cache_enabled=True
   )
   ```

5. **錯誤處理**
   ```python
   try:
       df = source.read()
   except FileNotFoundError:
       logger.error("檔案不存在")
   except Exception as e:
       logger.error(f"讀取失敗: {e}")
   ```

## 🔌 擴展新數據源

```python
from src.core.datasources import DataSource, DataSourceConfig

class MySource(DataSource):
    """自定義數據源"""
    
    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        # 初始化邏輯
    
    def read(self, query=None, **kwargs):
        # 實現讀取邏輯
        pass
    
    def write(self, data, **kwargs):
        # 實現寫入邏輯
        pass
    
    def get_metadata(self):
        # 返回元數據
        return {}

# 註冊到工廠
from src.core.datasources import DataSourceFactory, DataSourceType

# 需要先在 DataSourceType 中添加類型
DataSourceFactory.register_source(DataSourceType.MY_TYPE, MySource)
```

## 📈 效能建議

| 場景 | 建議 |
|-----|------|
| 讀取大型 Excel | 只讀取需要的欄位 (`usecols`) |
| 處理大型 CSV | 使用分塊讀取 (`read_in_chunks`) |
| 頻繁讀取相同資料 | 啟用快取 (`cache_enabled=True`) |
| 存儲大量資料 | 使用 Parquet 格式 |

## 📧 支援

如有問題或建議，請聯繫開發團隊。

---

*最後更新：2025年1月*
