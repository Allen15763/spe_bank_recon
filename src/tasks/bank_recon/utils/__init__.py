"""
工具模組
"""

from .bank_processor import BankProcessor
from .summary_formatter import BankSummaryFormatter
from .validation import (
    validate_amount,
    compare_amounts,
    validate_dataframe,
    validate_date_range,
    log_validation_result
)
from .output_formatter import (
    create_summary_dataframe,
    format_excel_output,
    reorder_bank_summary,
    add_timestamp_to_filename,
    format_number_columns
)

# Daily Check & Entry 新增模組
from .frr_processor import (
    quick_clean_financial_data,
    create_complete_date_range,
    convert_to_long_format,
    calculate_frr_handling_fee,
    calculate_frr_remittance_fee,
    calculate_frr_net_billing,
    validate_frr_handling_fee,
    validate_frr_net_billing,
)

from .dfr_processor import (
    validate_dfr_columns,
    get_column_range_indices,
    process_dfr_data,
    create_dfr_wp,
    calculate_daily_movement,
    calculate_running_balance,
)

from .apcc_calculator import (
    reformat_df_wp,
    get_apcc_service_fee_charged,
    apply_ops_adjustment,
    apply_rounding_adjustment,
    calculate_trust_account_validation,
    validate_apcc_vs_frr,
    get_spe_charge_with_tax,
    reformat_df_summary,
    transpose_df_summary,
    transform_payment_data,
    calculate_charge_rate,
    get_df_cc_rev,
    calculate_spe_transaction_percentage,
    calculate_transaction_percentage,
    convert_flatIndex_to_multiIndex
)

from .entry_transformer import (
    process_accounting_entries,
    validate_accounting_balance,
    AccountingEntryTransformer,
    ConfigurableEntryConfig,
    MonthlyConfig,  # 別名，向後相容
    get_easyfund_adj_service_fee_for_SPT,
    get_easyfund_service_fee_for_999995,
)

from .entry_processor import (
    AccountingEntryProcessor,
    calculate_daily_balance,
    dfr_balance_check,
    summarize_balance_check,
    create_big_entry_pivot,
    validate_result,
)

__all__ = [
    'BankProcessor',
    'BankSummaryFormatter',
    'validate_amount',
    'compare_amounts',
    'validate_dataframe',
    'validate_date_range',
    'log_validation_result',
    'create_summary_dataframe',
    'format_excel_output',
    'reorder_bank_summary',
    'add_timestamp_to_filename',
    'format_number_columns',
    
    # FRR Processor
    'quick_clean_financial_data',
    'create_complete_date_range',
    'convert_to_long_format',
    'calculate_frr_handling_fee',
    'calculate_frr_remittance_fee',
    'calculate_frr_net_billing',
    'validate_frr_handling_fee',
    'validate_frr_net_billing',
    
    # DFR Processor
    'validate_dfr_columns',
    'get_column_range_indices',
    'process_dfr_data',
    'create_dfr_wp',
    'calculate_daily_movement',
    'calculate_running_balance',
    
    # APCC Calculator
    'reformat_df_wp',
    'get_apcc_service_fee_charged',
    'apply_ops_adjustment',
    'apply_rounding_adjustment',
    'calculate_trust_account_validation',
    'validate_apcc_vs_frr',
    'get_spe_charge_with_tax',
    'reformat_df_summary',
    'transpose_df_summary',
    'transform_payment_data',
    'calculate_charge_rate',
    'get_df_cc_rev',
    'calculate_spe_transaction_percentage',
    'calculate_transaction_percentage',
    'convert_flatIndex_to_multiIndex',
    
    # Entry Transformer
    'process_accounting_entries',
    'validate_accounting_balance',
    'AccountingEntryTransformer',
    'ConfigurableEntryConfig',
    'MonthlyConfig',
    'get_easyfund_adj_service_fee_for_SPT',
    'get_easyfund_service_fee_for_999995',
    
    # Entry Processor
    'AccountingEntryProcessor',
    'calculate_daily_balance',
    'dfr_balance_check',
    'summarize_balance_check',
    'create_big_entry_pivot',
    'validate_result',
]
