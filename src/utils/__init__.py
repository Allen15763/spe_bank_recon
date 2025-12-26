"""
工具模組
提供日誌、配置管理等通用工具
"""
from .config import (
    config_manager, 
    ConfigManager, 
    get_project_root,
    get_config,
    get_path,
)
from .logging import (
    get_logger, 
    get_structured_logger, 
    Logger, 
    StructuredLogger,
    logger_manager,
)
from .database import (
    DuckDBManager,
    create_table,
    insert_table,
    alter_column_dtype,
    drop_table,
    backup_table,
)

from .helpers import (
    get_resource_path,
    validate_file_path,
    validate_file_extension,
    get_file_extension,
    is_excel_file,
    is_csv_file,
    ensure_directory_exists,
    get_safe_filename,
    get_unique_filename,
    get_file_info,
    calculate_file_hash,
    copy_file_safely,
    move_file_safely,
    cleanup_temp_files,
    find_files_by_pattern,
    get_directory_size,
    load_toml
)

__all__ = [
    # 配置管理
    'config_manager',
    'ConfigManager',
    'get_project_root',
    'get_config',
    'get_path',
    # 日誌
    'get_logger',
    'get_structured_logger',
    'Logger',
    'StructuredLogger',
    'logger_manager',
    # DuckDB
    'DuckDBManager',
    'create_table',
    'insert_table',
    'alter_column_dtype',
    'drop_table',
    'backup_table',

    # file_utils
    'get_resource_path',
    'validate_file_path',
    'validate_file_extension',
    'get_file_extension',
    'is_excel_file',
    'is_csv_file',
    'ensure_directory_exists',
    'get_safe_filename',
    'get_unique_filename',
    'get_file_info',
    'calculate_file_hash',
    'copy_file_safely',
    'move_file_safely',
    'cleanup_temp_files',
    'find_files_by_pattern',
    'get_directory_size',
    'load_toml',
]
