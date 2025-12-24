"""
duckDB module
"""

from .duckdb_manager import (
    DuckDBManager,
    create_table,
    insert_table,
    alter_column_dtype,
    drop_table,
    backup_table,

)

__all__ = [
    'DuckDBManager',
    'create_table',
    'insert_table',
    'alter_column_dtype',
    'drop_table',
    'backup_table',
]
