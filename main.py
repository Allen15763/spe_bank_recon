# import spe_bank_recon.utils.database as db_tools

from src.utils import get_logger, get_structured_logger, config_manager
from src.utils import (DuckDBManager,
                                  create_table,
                                  insert_table,
                                  alter_column_dtype,
                                  drop_table,
                                  backup_table)
from src.core.datasources import GoogleSheetsManager


DB_PATH = config_manager.get('general', 'DB_PATH')
LOG_FILE = config_manager.get('general', 'LOG_FILE')



manager = GoogleSheetsManager(
    credentials_path=config_manager.get('general', 'cred_path'),
    spreadsheet_url='https://docs.google.com/spreadsheets/d/17puiAmAhM2dAm9BR7Sck1E2fwsf0v76CkpiLkVJPlxE/edit?gid=0#gid=0'
)




with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:
    tables = db_manager.show_tables()




from dataclasses import dataclass
from typing import Optional
import pandas as pd
import numpy as np


@dataclass
class BankDataContainer:
    """資料容器，用於存放處理後的銀行對帳資料"""
    raw_data: pd.DataFrame
    aggregated_data: pd.DataFrame
    recon_amount: int
    amount_claimed_last_period_paid_by_current: int
    recon_amount_for_trust_account_fee: int
    recon_service_fee: int
    service_fee_claimed_last_period_paid_by_current: int
    adj_service_fee: int
    invoice_amount_claimed: int

    invoice_service_fee: Optional[int] = None





beg_date = '2025-10-01'
end_date = '2025-10-31'

last_beg_date = '2025-09-01'
last_end_date = '2025-09-30'





with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    # tables = db_manager.describe_table('ub_remittance_fee_installment')

    query = \
    f"""
    SELECT * FROM cub_nonindividual_statement
    WHERE disbursement_date IS NOT NULL
    """
    data = db_manager.query_to_df(query)



def get_cub_container(data):
    # non-individual
    CUB_CLAIMED_AMOUNT = data[data['disbursement_date'].between(beg_date, end_date)].request_amount.sum()  # 1_780_857_981  期間內
    CUB_REFUNDED_AMOUNT = abs(data[data['disbursement_date'].between(beg_date, end_date)].return_amount.sum())  # 101_805_057   期間內
    CUB_ADJ_AMOUNT = 0
    CUB_SERVICE_FEE_AMOUNT = data[data['disbursement_date'].between(beg_date, end_date)].handling_fee.sum()  # 26_483_578     # 期間內


    container = BankDataContainer(
        raw_data=data[data['disbursement_date'].between(beg_date, end_date)],
        aggregated_data=None,
        recon_amount=CUB_CLAIMED_AMOUNT,
        amount_claimed_last_period_paid_by_current=CUB_REFUNDED_AMOUNT,
        recon_amount_for_trust_account_fee=CUB_CLAIMED_AMOUNT - CUB_REFUNDED_AMOUNT - CUB_ADJ_AMOUNT,
        recon_service_fee=CUB_SERVICE_FEE_AMOUNT,
        service_fee_claimed_last_period_paid_by_current=0,
        adj_service_fee=CUB_ADJ_AMOUNT,
        invoice_amount_claimed=CUB_CLAIMED_AMOUNT - CUB_REFUNDED_AMOUNT - CUB_ADJ_AMOUNT,
        invoice_service_fee=CUB_SERVICE_FEE_AMOUNT
    )
    return container

def print_summary_cub(container: BankDataContainer, title: str):
    """格式化輸出結果"""
    print(f"\n--- {title} 摘要 ---")
    print(f'對帳 請款金額(當期): {container.recon_amount:,}')
    print(f'對帳 退貨金額: {container.amount_claimed_last_period_paid_by_current:,}')
    print(f'對帳 調整金額: {container.adj_service_fee}')
    print(f'對帳 請款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}')
    print('-' * 20)
    print(f'對帳 手續費(當期): {container.recon_service_fee:,}')
    print(f'對帳 手續費(前期): {container.service_fee_claimed_last_period_paid_by_current:,}')

    total_service_fee = (container.recon_service_fee +
                         container.service_fee_claimed_last_period_paid_by_current)
    print(f'對帳 手續費(前期+當期-(調整)): {total_service_fee:,}')

    print('-' * 20)
    print(f'發票 請款金額: {container.invoice_amount_claimed:,}')
    print(f'發票 手續費: {container.invoice_service_fee:,}')


    """JULY
recon_amount請款金額
1_778_202_357

amount_claimed_last_period_paid_by_current替代RETURN_AMOUNT
122_984_194

1_655_218_163


手續費
26_163_697
"""

cub_nonindividual_container = get_cub_container(data)
print_summary_cub(cub_nonindividual_container, "法人 (Non-Individual)")


with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    # tables = db_manager.describe_table('ub_remittance_fee_installment')

    query = \
    f"""
    SELECT * FROM cub_individual_statement
    WHERE disbursement_date IS NOT NULL
    """
    data = db_manager.query_to_df(query)


"""JULY
recon_amount請款金額
2_472_490_061

amount_claimed_last_period_paid_by_current替代RETURN_AMOUNT
171_730_873

2_300_759_188


手續費
36_020_326
"""


cub_individual_container = get_cub_container(data)
print_summary_cub(cub_individual_container, "個人 (Individual)")


def process_reconciliation_data(
    recon_df: pd.DataFrame,
    previous_claimed: int,
    previous_claimed_service_fee: int,
    current_month: str=beg_date.replace('-', '')[:6]
) -> BankDataContainer:
    """
    核心處理函式：接收原始對帳單和前期資料，計算所有指標並回傳一個 BankDataContainer。
    """
    # --- 步驟 1: 預先計算，避免重複 ---
    # 計算一次聚合資料，並在後續重複使用
    # agg_df = get_aggregate_recon_df(recon_df, current_month)

    # 建立當月資料的布林遮罩 (mask)，供後續重複使用
    mask_current_month_payout = recon_df['disbursement_date'].dt.strftime('%Y%m').str.contains(current_month)
    mask_current_month_request = recon_df['request_date'].dt.strftime('%Y%m').str.contains(current_month)

    # --- 步驟 2: 分別計算各項指標，增加可讀性 ---
    # 當期請款金額 (來自聚合資料)
    current_period_recon_amount = int(recon_df.loc[(mask_current_month_payout&
                                                    mask_current_month_request), :].request_amount.sum())

    # 調整 (篩選調整項目)
    adjustment_service_fee = int(
        recon_df.loc[(mask_current_month_payout&
                      mask_current_month_request), 'adjustment_amount'].sum()
    )

    # Trust Account Fee 的總請款金額
    trust_account_fee_amount = current_period_recon_amount + previous_claimed + adjustment_service_fee

    # 當期手續費 (篩選當月撥付)
    current_period_service_fee = int(
        recon_df.loc[
            (mask_current_month_payout&
             mask_current_month_request), 'handling_fee'
        ].sum()
    )

    current_period_adj_service_fee = int(
        recon_df.loc[
            (mask_current_month_payout&
             mask_current_month_request), 'adjustment_handling_fee'
        ].sum()
    )

    # 發票金額
    invoice_amount = int(recon_df.loc[mask_current_month_request,
                                      'invoice_amount'].sum())

    # --- 步驟 3: 實例化並回傳 Data Container ---
    container = BankDataContainer(
        raw_data=recon_df,
        aggregated_data=None,
        recon_amount=current_period_recon_amount,
        amount_claimed_last_period_paid_by_current=previous_claimed,
        recon_amount_for_trust_account_fee=trust_account_fee_amount,
        recon_service_fee=current_period_service_fee + current_period_adj_service_fee,
        service_fee_claimed_last_period_paid_by_current=previous_claimed_service_fee,
        adj_service_fee=adjustment_service_fee,
        invoice_amount_claimed=recon_df[mask_current_month_request].request_amount.sum(),
        invoice_service_fee=invoice_amount
    )
    return container

def print_summary_ctbc(container: BankDataContainer, title: str):
    """格式化輸出結果"""
    print(f"\n--- {title} 摘要 ---")
    print(f'對帳 請款金額(當期): {container.recon_amount:,}')
    print(f'對帳 請款金額(前期發票當期撥款): {container.amount_claimed_last_period_paid_by_current:,}')
    print(f'對帳 調整金額: {container.adj_service_fee}')
    print(f'對帳 請款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}')
    print('-' * 20)
    print(f'對帳 手續費(當期): {container.recon_service_fee:,}')
    print(f'對帳 手續費(前期): {container.service_fee_claimed_last_period_paid_by_current:,}')

    total_service_fee = (container.recon_service_fee +
                         container.service_fee_claimed_last_period_paid_by_current)
    print(f'對帳 手續費(前期+當期-(調整)): {total_service_fee:,}')

    print('-' * 20)
    print(f'發票 請款金額: {container.invoice_amount_claimed:,}')
    print(f'發票 手續費: {container.invoice_service_fee:,}')





with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    query = \
    f"""
    SELECT
        *
    FROM ctbc_noninstallment

    """
    data = db_manager.query_to_df(query)


"""JULY

--- 非分期 (Non-Installment) 摘要 ---
對帳 請款金額(當期): 1,207,253,135
對帳 請款金額(前期發票當期撥款): 39,371,800
對帳 調整金額: 0
對帳 請款金額(Trust Account Fee): 1,246,624,935
--------------------
對帳 手續費(當期): 18,591,709
對帳 手續費(前期): 606,326
對帳 手續費(前期+當期-(調整)): 19,198,035
發票 手續費: 19,207,220

"""

ctbc_noninstall_container = process_reconciliation_data(
    recon_df=data,
    previous_claimed=data[(data['request_date'].between(last_beg_date, last_end_date))&(~data['disbursement_date'].between(last_beg_date, last_end_date))].request_amount.sum(),
    previous_claimed_service_fee=data[(data['request_date'].between(last_beg_date, last_end_date))&(~data['disbursement_date'].between(last_beg_date, last_end_date))].handling_fee.sum()
)

print_summary_ctbc(ctbc_noninstall_container, "非分期 (Non-Installment)")


"""JULY
--- 分期 (Installment) 摘要 ---
對帳 請款金額(當期): 135,896,381
對帳 請款金額(前期發票當期撥款): 12,233,257
對帳 調整金額: 0
對帳 請款金額(Trust Account Fee): 148,129,638
--------------------
對帳 手續費(當期): 2,703,358
對帳 手續費(前期): 240,699
對帳 手續費(前期+當期-(調整)): 2,944,057
--------------------
發票 手續費: 2,780,655
"""


with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    query = \
    f"""
    SELECT
        *
    FROM ctbc_installment

    """
    data = db_manager.query_to_df(query)



ctbc_install_container = process_reconciliation_data(
    recon_df=data,
    previous_claimed=data[(data['request_date'].between(last_beg_date, last_end_date))&(~data['disbursement_date'].between(last_beg_date, last_end_date))].request_amount.sum(),
    previous_claimed_service_fee=data[(data['request_date'].between(last_beg_date, last_end_date))&(~data['disbursement_date'].between(last_beg_date, last_end_date))].handling_fee.sum()
)

print_summary_ctbc(ctbc_install_container, "分期 (Installment)")


with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    query = \
    f"""
    SELECT * FROM nccc_payment_statement
    WHERE disbursement_date IS NOT NULL
    """
    data = db_manager.query_to_df(query)


with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    query = \
    f"""
    SELECT * FROM nccc_recon_statement
    WHERE disbursement_date IS NOT NULL
    """
    invoice_data = db_manager.query_to_df(query)



def get_nccc_container(data):

    mask_current_month_payout = (data['disbursement_date'].between(beg_date, end_date))
    mask_current_month_request =(data['request_date'].between(beg_date, end_date))

    mask_last_month_payout = (data['disbursement_date'].between(last_beg_date, last_end_date))
    mask_last_month_request = (data['request_date'].between(last_beg_date, last_end_date))


    CLAIMED_AMOUNT = data[mask_current_month_payout].request_amount.sum()

    # 當期發票手續費欄位
    invoice_service_fee = data[mask_current_month_request].handling_fee.sum()
    SERVICE_FEE_AMOUNT = data[mask_current_month_payout].handling_fee.sum()


    container = BankDataContainer(
        raw_data=data[mask_current_month_payout],
        aggregated_data=None,
        recon_amount=CLAIMED_AMOUNT,
        amount_claimed_last_period_paid_by_current=data[(~mask_current_month_request&
                                                         mask_current_month_payout)].request_amount.sum(),
        recon_amount_for_trust_account_fee=CLAIMED_AMOUNT,
        recon_service_fee=SERVICE_FEE_AMOUNT,
        service_fee_claimed_last_period_paid_by_current=data[(mask_last_month_request&
                                                              mask_current_month_payout)].handling_fee.sum(),
        adj_service_fee=0,
        invoice_amount_claimed=data[mask_current_month_request].request_amount.sum(),

        invoice_service_fee=invoice_service_fee
    )
    return container

def print_summary_nccc(container: BankDataContainer, title: str):
    """格式化輸出結果"""
    print(f"\n--- {title} 摘要 ---")
    print(f'對帳 請款金額(當期): {container.recon_amount:,}')
    print(f'對帳 請款金額(前期發票當期撥款): {container.amount_claimed_last_period_paid_by_current:,}')
    # print(f'對帳 調整金額: {container.adj_service_fee}')  # 通常不會有調整，暫時放著
    print(f'對帳 請款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}')
    print('-' * 20)
    print(f'對帳 手續費(當期): {container.recon_service_fee:,}')
    print(f'對帳 手續費(前期): {container.service_fee_claimed_last_period_paid_by_current:,}')

    total_service_fee = (container.recon_service_fee +
                         container.service_fee_claimed_last_period_paid_by_current)
    print(f'對帳 手續費(前期+當期-(調整)): {total_service_fee:,}')

    print('-' * 20)
    print(f'發票 請款金額: {container.invoice_amount_claimed:,}')
    print(f'發票 手續費: {container.invoice_service_fee:,}')


#  用nccc_recon_container >>  nccc_recon_statement
nccc_recon_container = get_nccc_container(invoice_data)
print_summary_nccc(nccc_recon_container, "對帳報表")



RECON_AGG_TARGET_STORES = ['他行卡', '自行卡', '自行卡分期內含']
RECON_STOP_ROW_IDENTIFIER = '商店總計'
MAIN_COMPANY_NAME = '樂購蝦皮股份有限公司'
ADJUSTMENT_ROW_IDENTIFIER = '調整 -- 退貨'


def get_aggregate_recon_df(df: pd.DataFrame, current_month: str) -> pd.DataFrame:
    """對對帳單資料進行篩選和匯總"""
    mask_current_month = df['disbursement_date'].str.contains(current_month, na=False)
    mask_type = df['store_name'].isin(RECON_AGG_TARGET_STORES)

    # 篩選出當月且符合商店類型的資料
    filtered_df = df.loc[mask_current_month & mask_type]

    # 進行分組與聚合
    df_agg = filtered_df.groupby('store_name', as_index=False).agg(
        total_claimed=pd.NamedAgg(column='request_amount', aggfunc='sum'),
        total_service_fee=pd.NamedAgg('handling_fee', 'sum'),
        total_ub_service_fee=pd.NamedAgg('local_handling_fee', 'sum'),
        total_paid=pd.NamedAgg('disbursement_amount', 'sum'),
    )
    return df_agg

def process_reconciliation_data(
    recon_df: pd.DataFrame,
    # previous_claimed: int,
    # previous_claimed_service_fee: int,
    current_month: str=beg_date.replace('-', '')[:6]
) -> BankDataContainer:
    """
    核心處理函式：接收原始對帳單和前期資料，計算所有指標並回傳一個 BankDataContainer。
    這個函式消除了主程式中的重複邏輯。
    """
    # --- 步驟 1: 預先計算，避免重複 ---
    # 計算一次聚合資料，並在後續重複使用
    agg_df = get_aggregate_recon_df(recon_df, current_month)

    # 建立當月資料的布林遮罩 (mask)，供後續重複使用
    # mask_current_month_payout = recon_df['disbursement_date'].str.contains(current_month, na=False)
    # mask_current_month_request = recon_df['request_date'].str.contains(current_month, na=False)

    mask_current_month_payout = (recon_df['disbursement_date'].between(beg_date.replace('-',''), end_date.replace('-','')))
    mask_current_month_request =(recon_df['request_date'].between(beg_date.replace('-',''), end_date.replace('-','')))

    mask_last_month_payout = (recon_df['disbursement_date'].between(last_beg_date.replace('-',''), last_end_date.replace('-','')))
    mask_last_month_request = (recon_df['request_date'].between(last_beg_date.replace('-',''), last_end_date.replace('-','')))

    # --- 步驟 2: 分別計算各項指標，增加可讀性 ---
    # 當期請款金額
    previous_claimed = recon_df[(mask_current_month_payout&
                                 mask_last_month_request)].request_amount.sum()
    current_period_recon_amount = recon_df[(mask_current_month_request & mask_current_month_payout) & recon_df['store_name'].str.contains('行卡')].request_amount.sum()

    # Trust Account Fee 的總請款金額(含上期請款當期付款)
    trust_account_fee_amount = int(agg_df.total_claimed.sum())

    # Validation
    validate_1 = current_period_recon_amount + previous_claimed - trust_account_fee_amount
    if validate_1 != 0:
        raise ValueError(f"Validation failed: {validate_1}")
    else:
        print(f"Validation_1 passed: {validate_1}")

    #   手續費 (篩選當月撥付 & 公司名稱) 含上期請款
    previous_claimed_service_fee = recon_df[(mask_current_month_payout&
                                 mask_last_month_request)].handling_fee.sum()

    current_period_service_fee = int(
        recon_df.loc[
            mask_current_month_payout & (recon_df['store_name'] == MAIN_COMPANY_NAME), 'handling_fee'
        ].sum()
    )


    # 調整手續費 (篩選調整項目)
    adjustment_service_fee = int(
        recon_df.loc[(recon_df['store_name'] == ADJUSTMENT_ROW_IDENTIFIER) & mask_current_month_request, 'handling_fee'].sum()
    )

    # 發票請款金額 (取最後一筆記錄的第5個欄位 '請款金額')
    # 使用 .at 或 .iat 搭配欄位名稱會比 iloc 更穩健
    invoice_amount = int(recon_df.at[recon_df.index[-1], 'request_amount'])

    # --- 步驟 3: 實例化並回傳 Data Container ---
    container = BankDataContainer(
        raw_data=recon_df,
        aggregated_data=agg_df,
        recon_amount=current_period_recon_amount,
        amount_claimed_last_period_paid_by_current=previous_claimed,
        recon_amount_for_trust_account_fee=trust_account_fee_amount,
        recon_service_fee=current_period_service_fee,
        service_fee_claimed_last_period_paid_by_current=previous_claimed_service_fee,  # Noted
        adj_service_fee=adjustment_service_fee,
        invoice_amount_claimed=invoice_amount,
        invoice_service_fee=0  # TBC 對帳單明細無法篩選出，只能從發票檔擷取
    )
    return container

def print_summary_ub(container: BankDataContainer, title: str):
    """格式化輸出結果"""
    print(f"\n--- {title} 摘要 ---")
    print(f'對帳 請款金額(當期): {container.recon_amount:,}')
    print(f'對帳 請款金額(前期發票當期撥款): {container.amount_claimed_last_period_paid_by_current:,}')
    print(f'對帳 請款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}')
    print('-' * 20)
    print(f'對帳 手續費(當期+前期於本期撥款): {container.recon_service_fee:,}')
    print(f'對帳 手續費(調整): {container.adj_service_fee}')

    # total_service_fee = (container.recon_service_fee +
    #                      container.service_fee_claimed_last_period_paid_by_current -
    #                      container.adj_service_fee)
    total_service_fee = (container.recon_service_fee -
                         container.adj_service_fee)
    print(f'對帳 手續費(前期+當期-(調整)): {total_service_fee:,}')


with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:


    query = \
    f"""
    SELECT * FROM ub_noninstallment_recon_statement
    WHERE disbursement_date IS NOT NULL
    AND request_date IS NOT NULL
    -- AND request_date <> 'nan'  -- 會把 store_name='樂購蝦皮股份有限公司'的排除
    """
    data = db_manager.query_to_df(query)




ub_noninstall_container = process_reconciliation_data(
    recon_df=data,
)

print_summary_ub(ub_noninstall_container, "非分期 (Non-Installment)")

with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    # tables = db_manager.describe_table('ub_remittance_fee_installment')

    query = \
    f"""
    SELECT * FROM ub_installment_recon_statement
    -- WHERE request_date IS NOT NULL and request_date != 'nan'

    WHERE disbursement_date IS NOT NULL
    AND request_date IS NOT NULL
    """
    data = db_manager.query_to_df(query)


ub_install_container = process_reconciliation_data(
    #分期的資料不要轉日期 recon_df=data[(pd.to_datetime(data['request_date']).between(beg_date, end_date))],
    recon_df=data

)

print_summary_ub(ub_install_container, "分期 (Installment)")



with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:

    # tables = db_manager.describe_table('ub_remittance_fee_installment')

    query = \
    f"""
    SELECT * FROM taishi_recon_statement
    WHERE disbursement_date IS NOT NULL
    """
    data = db_manager.query_to_df(query)



def get_taishi_container(data):
    mask_current_month_payout = (pd.to_datetime(data['disbursement_date']).between(beg_date, end_date))
    df = data[mask_current_month_payout]

    recon_amount = int(df['request_amount'].sum())
    recon_claimed_amount: int = int(df['disbursement_amount'].sum())
    service_fee_amount: int = int(df['invoice_amount'].sum())
    adj_amt = int(df.query("disbursement_amount==0").invoice_amount.sum())  # 須扣除的TSPG System Service Fees
    adj_tax_amt = int(df.query("disbursement_amount==0").tax_amount.sum())


    container = \
    BankDataContainer(
        raw_data=df,
        aggregated_data=None,
        recon_amount=recon_amount,
        amount_claimed_last_period_paid_by_current=0,
        recon_amount_for_trust_account_fee=recon_claimed_amount,
        recon_service_fee=service_fee_amount - adj_amt,
        service_fee_claimed_last_period_paid_by_current=adj_tax_amt,  # 用此欄位暫代稅額調整
        adj_service_fee=adj_amt,                                      # 用此欄位暫代發票額調整
        invoice_amount_claimed=service_fee_amount - adj_amt
    )
    return container

def print_summary_taishi(container: BankDataContainer, title: str):
    """格式化輸出結果"""
    print(f"\n--- {title} 摘要 ---")
    print(f'對帳 請款金額(當期): {container.recon_amount:,}')
    print(f'對帳 手續費(當期+前期於本期撥款): {container.recon_service_fee:,}')
    print(f'對帳 請/付款金額(Trust Account Fee): {container.recon_amount_for_trust_account_fee:,}')
    print('-' * 20)

    print(f'對帳 TSPG System Service Fees: {container.adj_service_fee:,}')
    print(f'對帳 稅額調整: {container.service_fee_claimed_last_period_paid_by_current:,}')



taishi_container = get_taishi_container(data)
print_summary_taishi(taishi_container, '台新')



import pandas as pd
from typing import List, Tuple, Dict, Any

def create_bank_summary_dataframe(containers_and_names: List[Tuple[Any, str]]) -> pd.DataFrame:
    """
    創建銀行摘要的 DataFrame

    Args:
        containers_and_names: List of tuples containing (BankDataContainer, bank_name)

    Returns:
        pd.DataFrame: 包含所有銀行摘要數據的 DataFrame
    """
    summary_data = []

    for container, bank_name in containers_and_names:
        # 基礎數據字典
        data = {
            '銀行': bank_name,
            '對帳_請款金額_當期': container.recon_amount,
            '對帳_請款金額_Trust_Account_Fee': getattr(container, 'recon_amount_for_trust_account_fee', 0),
            '對帳_手續費_當期': container.recon_service_fee,
            '對帳_調整金額': container.adj_service_fee,
        }

        # 根據不同銀行設定特定欄位
        if 'cub' in bank_name.lower():
            data.update({
                '對帳_退貨金額': container.amount_claimed_last_period_paid_by_current,
                '對帳_手續費_前期': container.service_fee_claimed_last_period_paid_by_current,
                '發票_手續費': container.invoice_service_fee,
                '發票_請款金額': container.invoice_amount_claimed,
            })
            # CUB 的總手續費計算
            data['對帳_手續費_總計'] = (container.recon_service_fee +
                                  container.service_fee_claimed_last_period_paid_by_current)

        elif any(['ctbc' in bank_name.lower(), 'nccc' in bank_name.lower()]):
            data.update({
                '對帳_請款金額_前期發票當期撥款': container.amount_claimed_last_period_paid_by_current,
                '對帳_手續費_前期': container.service_fee_claimed_last_period_paid_by_current,
                '發票_請款金額': container.invoice_amount_claimed,
            })
            # CTBC/NCCC 的總手續費計算
            data['對帳_手續費_總計'] = (container.recon_service_fee +
                                  container.service_fee_claimed_last_period_paid_by_current)

            if 'nccc' in bank_name.lower():
                data.update({
                    '發票_請款金額': container.invoice_amount_claimed,
                    '發票_手續費': container.invoice_service_fee,
                })
                # 用RECON的報表這邊的結果要用當期的手續費即可
                data['對帳_手續費_總計'] = (container.recon_service_fee)
            else:  # ctbc
                data['發票_手續費'] = container.invoice_service_fee

        elif 'ub' in bank_name.lower():
            data.update({
                '對帳_請款金額_前期發票當期撥款': container.amount_claimed_last_period_paid_by_current,
                # '對帳_手續費_當期_前期於本期撥款': container.recon_service_fee,
                '對帳_手續費_前期': container.service_fee_claimed_last_period_paid_by_current,
            })
            # UB 的總手續費計算（有調整金額的減法）
            data['對帳_手續費_總計'] = (container.recon_service_fee -
                                  container.adj_service_fee)
            data['對帳_手續費_當期'] = (container.recon_service_fee -
                                  container.service_fee_claimed_last_period_paid_by_current)

        elif 'taishi' in bank_name.lower():
            data.update({
                # '對帳_手續費_當期_前期於本期撥款': container.recon_service_fee,
                # '對帳_付款金額_Trust_Account_Fee': container.recon_amount_for_trust_account_fee,
                # '對帳_TSPG_System_Service_Fees': container.adj_service_fee,
                '對帳_稅額調整': container.service_fee_claimed_last_period_paid_by_current,
            })
            # 台新手續費皆使用當期
            data['對帳_手續費_總計'] = container.recon_service_fee

        summary_data.append(data)

    # 創建 DataFrame
    df = pd.DataFrame(summary_data)

    # 重新排列欄位順序，將銀行放在第一欄
    cols = ['銀行'] + [col for col in df.columns if col != '銀行']
    df = df[cols]

    return df

def display_summary_dataframe(df: pd.DataFrame, format_numbers: bool = True) -> None:
    """
    格式化顯示 DataFrame

    Args:
        df: 要顯示的 DataFrame
        format_numbers: 是否格式化數字（加千分位逗號）
    """
    if format_numbers:
        # 創建副本避免修改原始數據
        display_df = df.copy()

        # 格式化數值欄位
        numeric_columns = df.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:,}" if pd.notna(x) else "N/A"
            )

        print(display_df.to_string(index=False))
    else:
        print(df.to_string(index=False))




# 使用範例
def example_usage():
    """
    使用範例 - 需要替換為實際的 container 對象
    """
    containers_and_names = [
        (cub_nonindividual_container, 'cub_nonindividual'),
        (cub_individual_container, 'cub_individual'),
        (ctbc_noninstall_container, 'ctbc_noninstallment'),
        (ctbc_install_container, 'ctbc_installment'),
        # (nccc_payout_container, 'nccc'),
        (nccc_recon_container, 'nccc'),



        (ub_noninstall_container, 'ub_noninstallment'),
        (ub_install_container, 'ub_installment'),
        (taishi_container, 'taishi'),

    ]

    # 創建 DataFrame
    summary_df = create_bank_summary_dataframe(containers_and_names)

    # 顯示結果
    print("=== 銀行摘要 DataFrame ===")
    display_summary_dataframe(summary_df)

    # 也可以導出到 Excel 或 CSV
    # summary_df.to_excel('bank_summary.xlsx', index=False)
    # summary_df.to_csv('bank_summary.csv', index=False)

    return summary_df


df_summary_escrow_inv = example_usage()



dfs = []
for table in tables.name.tolist():
    print(f"quering {table}")

    if table != 'invoice_details':
        with DuckDBManager(
            db_path=DB_PATH,
            log_file=LOG_FILE,
            log_level="DEBUG") as db_manager:


            query = \
            f"""
            SELECT * FROM {table}
            """
            df = db_manager.query_to_df(query)
        df['source'] = table
        dfs.append(df)

df_full_raw = pd.concat(dfs, ignore_index=True)


with DuckDBManager(
    db_path=DB_PATH,
    log_file=LOG_FILE,
    log_level="DEBUG") as db_manager:


    query = \
    """
    SELECT * FROM invoice_details
    """
    df_invoice = db_manager.query_to_df(query)



df_invoice['invoice_date'] = pd.to_datetime(df_invoice['invoice_date'].str.replace('-',''), format='%Y%m%d')
df_invoice['is_handling_fee'] = df_invoice.invoice_description.str.contains("手續費")
mask = df_invoice.invoice_date.between(beg_date, end_date)


invoice_summary: pd.DataFrame = \
df_invoice[mask].groupby(['category', 'is_handling_fee'], as_index=False).agg(
    total_amount_excl_tax=pd.NamedAgg(column='amount_excl_tax', aggfunc='sum'),
    total_tax_amount=pd.NamedAgg(column='tax_amount', aggfunc='sum'),
    total_amount_incl_tax=pd.NamedAgg(column='amount_incl_tax', aggfunc='sum'),
)





# 從UB發票映射回UB的"發票_手續費"欄位
df_summary_escrow_inv.loc[df_summary_escrow_inv.銀行=='ub_noninstallment', '發票_手續費'] = invoice_summary.query("category=='ub_noninstallment' and is_handling_fee==True").total_amount_incl_tax.values[0]
df_summary_escrow_inv.loc[df_summary_escrow_inv.銀行=='ub_installment', '發票_手續費'] = invoice_summary.query("category=='ub_installment' and is_handling_fee==True").total_amount_incl_tax.values[0]

# 找回發票的稅額
df_summary_escrow_inv = (df_summary_escrow_inv.merge(
        invoice_summary.loc[invoice_summary.is_handling_fee==True, ['category', 'total_tax_amount', 'total_amount_incl_tax']],
        left_on='銀行',
        right_on='category',
        how='left')
    .drop(columns=['category'])
    )




df_summary_escrow_inv['check_invoice_amt\n發票_手續費(對帳單) - total_amount_incl_tax(AI)'] = np.where(
    df_summary_escrow_inv.銀行!='taishi', df_summary_escrow_inv.發票_手續費 - df_summary_escrow_inv.total_amount_incl_tax,
    df_summary_escrow_inv.對帳_手續費_當期 - df_summary_escrow_inv.total_amount_incl_tax
    )


# 把發票類的放到後面比較好看

for col in ['發票_請款金額', '發票_手續費']:
    a = df_summary_escrow_inv.pop(col)
    df_summary_escrow_inv.insert(df_summary_escrow_inv.columns.get_loc('total_tax_amount'), col, a)

# 排序銀行
def re_order(df):
    df_copy = df.copy()
    df_copy.set_index('銀行', inplace=True)

    # Define the desired order
    desired_order =  [
        'taishi',
        'nccc',
        'cub_nonindividual',
        'cub_individual',
        'ctbc_noninstallment',
        'ctbc_installment',
        'ub_noninstallment',
        'ub_installment',
    ]

    # Reorder the DataFrame
    df_reordered = df_copy.reindex(desired_order)
    return df_reordered.reset_index()

df_summary_escrow_inv = re_order(df_summary_escrow_inv)

df_summary_escrow_inv['bank'] = df_summary_escrow_inv['銀行'].str.split('_').str[0]
df_summary_escrow_inv


url_ub_installment_report = "/content/聯邦分期報表_202510.xlsx"
url_ctbc_installment_report = "/content/CTBC手續費_202510.xlsx"
url_cub_installment_report_individual = "/content/國泰分期報表_個人_202510.xlsx"
url_cub_installment_report_nonindividual = "/content/國泰分期報表_法人_202510.xls"
url_nccc_installment_report = "/content/NCCC分期報表_202510.xls"
url_taishi_installment_report = "/content/台新分期報表_202510.xls"



def dataframe_to_nested_dict(df):
    """
    將 DataFrame 轉換回巢狀字典
    智能處理 key 格式：
    - 3, 6 -> '03', '06'（小於10的數字加前導零）
    - 12, 24 -> '12', '24'（保持兩位數）
    - 其他保持原樣
    """
    # 移除 level_0 欄位
    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1)

    # 設定 index
    df = df.set_index('index')

    result = {}

    for col in df.columns:
        result[col] = {}

        for key in df.index:
            value = df.loc[key, col]

            # 過濾空值
            if pd.notna(value) and value != '' and str(value).strip() != '':
                # 處理 key 的格式
                formatted_key = format_key(key)

                try:
                    # 嘗試轉換為 float
                    result[col][formatted_key] = float(value)
                except (ValueError, TypeError):
                    result[col][formatted_key] = value

    return result

def format_key(key):
    """
    格式化 key：
    - 數字 < 10: 轉為兩位數字串（'03', '06'）
    - 數字 >= 10: 轉為字串（'12', '24'）
    - 其他: 保持原樣
    """
    # 處理數字型 key
    if isinstance(key, (int, float)):
        num = int(key)
        if num < 10:
            return f'{num:02d}'
        else:
            return str(num)

    # 處理字串型 key（可能是數字字串）
    if isinstance(key, str):
        # 如果是純數字字串
        if key.isdigit():
            num = int(key)
            if num < 10:
                return f'{num:02d}'
            else:
                return str(num)
        # 其他字串保持原樣
        return key

    # 其他類型轉為字串
    return str(key)

service_fee_rate = dataframe_to_nested_dict(manager.get_data('service_fee_rate'))

transaction_tyep_mapping = {
    '03': '3期',
    '06': '6期',
    '12': '12期',
    '24': '24期',
}


def get_cub_cc_service_fee_detail(url: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_excel(url, header=3, dtype=str, sheet_name='B2B_TimesM')
    df = df.rename(columns={'交易\n類別': '交易類別', '請款\n商店代號': '請款商店代號'})
    df = df.query("~交易類別.isna() and 交易類別 != '小計'")

    for column in ['請款月', '分期數', '請款商店代號', '請款商店名稱']:
        df[column] = df[column].fillna(method='ffill')

    columns = ['金額', '手續費']
    for column in columns:
        df[column] = df[column].astype(float)

    df_agg = \
    df.groupby('分期數').agg(
        total_claimed=pd.NamedAgg(column='金額', aggfunc='sum'),
        total_service_fee=pd.NamedAgg('手續費', 'sum'),
    ).reset_index().rename(columns={'分期數': 'transaction_type'})
    return df, df_agg


cub_individual_raw, cub_individual_agg = get_cub_cc_service_fee_detail(url_cub_installment_report_individual)
cub_nonindividual_raw, cub_nonindividual_agg = get_cub_cc_service_fee_detail(url_cub_installment_report_nonindividual)

cub_fin = pd.merge(cub_individual_agg, cub_nonindividual_agg, on='transaction_type', how='outer', suffixes=['_individual', '_nonindividual'])
cub_fin['total_claimed'] = cub_fin.total_claimed_individual + cub_fin.total_claimed_nonindividual
cub_fin['total_service_fee'] = cub_fin.total_service_fee_individual + cub_fin.total_service_fee_nonindividual


val_amt = cub_nonindividual_raw.金額.sum() + cub_individual_raw.金額.sum() - cub_fin.total_claimed.sum()
val_fee = cub_nonindividual_raw.手續費.sum() + cub_individual_raw.手續費.sum() - cub_fin.total_service_fee.sum()

if all([val_amt == 0, val_fee <= 1 and val_fee >= -1]):
    print("Validated Success")
else:
    raise ValueError("Validated Error")


import pandas as pd
import re
from typing import List, Tuple, Dict


def process_excel_sheets(url_ctbc_installment_report: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Efficiently process Excel sheets and categorize them into installment and non-installment data.
    """
    dfs_noninstallment = []
    dfs_installment = []

    with pd.ExcelFile(url_ctbc_installment_report) as excel_file:
        for sheet in excel_file.sheet_names:
            if re.search(r'\d{4}', sheet):
                # df = excel_file.parse(sheet, usecols='A:H', header=2)
                # df['source'] = sheet

                # if '分' in sheet:
                #     dfs_installment.append(df)
                # else:
                #     dfs_noninstallment.append(df)
                if '分' in sheet:
                    df = excel_file.parse(sheet, usecols='A:I', header=2)
                    df['source'] = sheet
                    dfs_installment.append(df)
                else:
                    df = excel_file.parse(sheet, usecols='B:I', header=2)
                    df['source'] = sheet
                    dfs_noninstallment.append(df)

    return (
        pd.concat(dfs_noninstallment, ignore_index=True) if dfs_noninstallment else pd.DataFrame(),
        pd.concat(dfs_installment, ignore_index=True) if dfs_installment else pd.DataFrame()
    )


def get_date_ranges(db_manager, table_name: str, beg_date: str, end_date: str) -> Tuple[List[str], List[str]]:
    """
    Get request date ranges for both request_date and disbursement_date in a single function.
    """
    query = f"""
    SELECT
        request_date as req_date,
        request_date as disb_date
    FROM {table_name}
    WHERE request_date BETWEEN '{beg_date}' AND '{end_date}'

    UNION ALL

    SELECT
        request_date as req_date,
        request_date as disb_date
    FROM {table_name}
    WHERE disbursement_date BETWEEN '{beg_date}' AND '{end_date}'
    """

    # Get request dates
    req_query = f"""
    SELECT request_date FROM {table_name}
    WHERE request_date BETWEEN '{beg_date}' AND '{end_date}'
    """

    # Get disbursement dates
    disb_query = f"""
    SELECT request_date FROM {table_name}
    WHERE disbursement_date BETWEEN '{beg_date}' AND '{end_date}'
    """

    request_dates = db_manager.query_to_df(req_query).iloc[:, 0].dt.strftime('%m%d').tolist()
    processed_dates = db_manager.query_to_df(disb_query).iloc[:, 0].dt.strftime('%m%d').tolist()

    print(f'Table: {table_name}')
    print(f"Request date range: {min(request_dates)} ~ {max(request_dates)}")
    print(f"Processed date range: {min(processed_dates)} ~ {max(processed_dates)}")

    return request_dates, processed_dates


def calculate_noninstallment_metrics(df: pd.DataFrame, recon_range: List[str],
                                   recon_range_processed: List[str]) -> Dict[str, float]:
    """
    Calculate non-installment metrics efficiently using vectorized operations.
    """
    # Define card type conditions
    conditions = {
        '本行卡': df['卡別'].isna(),
        '他行卡': df['卡別'] == '非本行國內',
        '調整': df['卡別'].isin(['帳務調整', '調手續費'])
    }

    results = {}

    # Calculate for both date ranges and both metrics
    for date_type, date_range in [('帳務日', recon_range), ('處理日', recon_range_processed)]:
        df_filtered = df[df['source'].isin(date_range)]

        for card_type, condition in conditions.items():
            # Handle special case for 處理日 調整 which should use recon_range
            if date_type == '處理日' and card_type == '調整':
                df_subset = df[df['source'].isin(recon_range) & condition]
            else:
                df_subset = df_filtered[condition]

            results[f'{date_type}_{card_type}_請款金額'] = df_subset['請款金額'].sum()
            results[f'{date_type}_{card_type}_手續費'] = df_subset['手續費'].sum()

    return results


def calculate_installment_metrics(df: pd.DataFrame, request_dates: List[str],
                                processed_dates: List[str]) -> Dict[str, float]:
    """
    Calculate installment metrics efficiently.
    """
    results = {}

    # Clean source column for installment data
    df_clean_source = df['source'].str.replace('分-', '')

    # Define installment periods and adjustment condition
    periods = [3, 6, 12, 24]
    adjustment_condition = df['產品別'].str.contains('調', na=False)

    for date_type, date_range in [('帳務日', request_dates), ('處理日', processed_dates)]:
        date_filter = df_clean_source.isin(date_range)

        # Calculate for each period
        for period in periods:
            condition = (df['期數'] == period) & date_filter
            results[f'{date_type}_{period}期_請款金額'] = df.loc[condition, '請/調金額'].sum()
            results[f'{date_type}_{period}期_手續費'] = df.loc[condition, '實際手續費'].sum()

        # Calculate for adjustments
        adj_condition = adjustment_condition & date_filter
        results[f'{date_type}_調整_請款金額'] = df.loc[adj_condition, '請/調金額'].sum()
        results[f'{date_type}_調整_手續費'] = df.loc[adj_condition, '實際手續費'].sum()

    return results


def create_summary_dataframe(noninstall_results: Dict[str, float],
                           install_results: Dict[str, float]) -> pd.DataFrame:
    """
    Create a comprehensive summary DataFrame from the calculated results.
    """
    # Prepare data for DataFrame
    summary_data = []

    # Non-installment data
    card_types = ['本行卡', '他行卡', '調整']
    date_types = ['帳務日', '處理日']
    metrics = ['請款金額', '手續費']

    for date_type in date_types:
        for card_type in card_types:
            for metric in metrics:
                key = f'{date_type}_{card_type}_{metric}'
                summary_data.append({
                    'category': 'non_installment',
                    'date_type': date_type,
                    'subcategory': card_type,
                    'metric': metric,
                    'value': noninstall_results.get(key, 0)
                })

    # Installment data
    periods = ['3期', '6期', '12期', '24期', '調整']

    for date_type in date_types:
        for period in periods:
            for metric in metrics:
                key = f'{date_type}_{period}_{metric}'
                summary_data.append({
                    'category': 'installment',
                    'date_type': date_type,
                    'subcategory': period,
                    'metric': metric,
                    'value': install_results.get(key, 0)
                })

    return pd.DataFrame(summary_data)


def reformat_pivot_table(pivot_df):
    """
    Reformat pivot table with custom ordering:
    1. non_installment before installment
    2. subcategory: 3期, 6期, 12期, 24期, 調整 (for installment); 本行卡, 他行卡, 調整 (for non_installment)
    3. metric: 請款金額 before 手續費
    """
    # Define custom sort orders
    category_order = ['non_installment', 'installment']

    subcategory_order = {
        'non_installment': ['本行卡', '他行卡', '調整'],
        'installment': ['3期', '6期', '12期', '24期', '調整']
    }

    metric_order = ['請款金額', '手續費']
    date_order = ['帳務日', '處理日']

    # Create custom sorting keys for index
    def get_category_sort_key(cat_subcat):
        category, subcategory = cat_subcat
        cat_idx = category_order.index(category)
        subcat_idx = subcategory_order[category].index(subcategory)
        return (cat_idx, subcat_idx)

    # Create custom sorting keys for columns
    def get_column_sort_key(date_metric):
        date_type, metric = date_metric
        date_idx = date_order.index(date_type)
        metric_idx = metric_order.index(metric)
        return (date_idx, metric_idx)

    # Sort index (rows)
    sorted_index = sorted(pivot_df.index, key=get_category_sort_key)

    # Sort columns
    sorted_columns = sorted(pivot_df.columns, key=get_column_sort_key)

    # Reindex the DataFrame
    return pivot_df.reindex(index=sorted_index, columns=sorted_columns)


def main_processing(url_ctbc_installment_report: str, DB_PATH: str, LOG_FILE: str,
                   beg_date: str, end_date: str) -> pd.DataFrame:
    """
    Main function to process CTBC installment and non-installment data.
    """
    # Process Excel sheets
    ctbc_noninstall_raw, ctbc_install_raw = process_excel_sheets(url_ctbc_installment_report)

    # Get date ranges using database manager
    with DuckDBManager(db_path=DB_PATH, log_file=LOG_FILE, log_level="DEBUG") as db_manager:
        # Get non-installment date ranges
        recon_range, recon_range_processed = get_date_ranges(
            db_manager, 'ctbc_noninstallment', beg_date, end_date
        )

        # Get installment date ranges
        request_date_installment, request_date_processed_installment = get_date_ranges(
            db_manager, 'ctbc_installment', beg_date, end_date
        )

    # Calculate metrics
    noninstall_results = calculate_noninstallment_metrics(
        ctbc_noninstall_raw, recon_range, recon_range_processed
    )

    install_results = calculate_installment_metrics(
        ctbc_install_raw, request_date_installment, request_date_processed_installment
    )

    # Create summary DataFrame
    summary_df = create_summary_dataframe(noninstall_results, install_results)

    # Optional: Create pivot table for better readability
    pivot_df = summary_df.pivot_table(
        index=['category', 'subcategory'],
        columns=['date_type', 'metric'],
        values='value',
        fill_value=0
    )

    pivot_df = reformat_pivot_table(pivot_df)

    return summary_df, pivot_df, ctbc_noninstall_raw, ctbc_install_raw


# Usage example:
summary_df, pivot_df, ctbc_noninstall_raw, ctbc_install_raw = main_processing(
    url_ctbc_installment_report, DB_PATH, LOG_FILE, beg_date, end_date
)


def validate_summary_data(db_manager, summary_df, beg_date, end_date):
    """
    Validate summary DataFrame against database totals.
    Returns tuple of differences: (帳務日請款金額差, 帳務日手續費差, 處理日請款金額差, 處理日手續費差)
    """
    # Single query to get all validation data
    query = f"""
    SELECT
        SUM(CASE WHEN request_date BETWEEN '{beg_date}' AND '{end_date}' THEN request_amount + adjustment_amount ELSE 0 END) as req_amount,
        SUM(CASE WHEN request_date BETWEEN '{beg_date}' AND '{end_date}' THEN invoice_amount ELSE 0 END) as req_fee,
        SUM(CASE WHEN disbursement_date BETWEEN '{beg_date}' AND '{end_date}' THEN request_amount + adjustment_amount ELSE 0 END) as disb_amount,
        SUM(CASE WHEN disbursement_date BETWEEN '{beg_date}' AND '{end_date}' THEN invoice_amount ELSE 0 END) as disb_fee
    FROM (
        SELECT * FROM ctbc_noninstallment
        UNION ALL
        SELECT * FROM ctbc_installment
    ) combined
    """

    db_totals = db_manager.query_to_df(query).iloc[0]

    return (
        summary_df.query("date_type=='帳務日' and metric=='請款金額'").value.sum() - db_totals['req_amount'],
        summary_df.query("date_type=='帳務日' and metric=='手續費'").value.sum() - db_totals['req_fee'],
        summary_df.query("date_type=='處理日' and metric=='請款金額'").value.sum() - db_totals['disb_amount'],
        summary_df.query("date_type=='處理日' and metric=='手續費'").value.sum() - db_totals['disb_fee']
    ), db_totals


with DuckDBManager(db_path=DB_PATH, log_file=LOG_FILE, log_level="DEBUG") as db_manager:
    validation, db_amount = validate_summary_data(db_manager, summary_df, beg_date, end_date)

db_amount.name = 'agg_amt'
db_amount = pd.DataFrame(db_amount)
db_amount['diff'] = validation

db_amount


def redefine_subcategory_for_recon(df, adj_type='3期'):
    df_copy = df.copy()
    df_copy = df_copy.query("date_type=='處理日'")

    mask1 = df_copy.subcategory=='調整'
    mask2 = df_copy.category=='installment'
    mask3 = df_copy.subcategory.str.contains('行卡')

    # 分期調整放，3期付款；會動態調整
    df_copy.loc[mask1 & mask2, 'subcategory'] = adj_type

    df_copy.loc[mask2 & mask3, 'subcategory'] = 'normal'

    # 要含調整才可以跟Trust Acc對上
    # df_agg = df_copy.loc[~mask1, :].groupby(['subcategory', 'metric'], as_index=False).value.sum()
    df_agg = df_copy.groupby(['subcategory', 'metric'], as_index=False).value.sum()

    result = pd.pivot_table(df_agg, index='subcategory', columns='metric', values='value', aggfunc='sum').reset_index()

    name_col = {
        'subcategory': 'transaction_type',
        '手續費': 'total_service_fee',
        '請款金額': 'total_claimed'
    }

    return result.rename(columns=name_col)


ctbc_agg = redefine_subcategory_for_recon(summary_df)


def get_nccc_cc_service_fee_detail(url: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_excel(url, header=4, dtype=str)
    df = df.query("~期數.isna() and ~期數.isin(['小計', '合計', '總計']) ")

    for column in ['特店代號', '處理日', '類別', '卡別']:
        df[column] = df[column].fillna(method='ffill')

    columns = ['金額', '手續費', '淨額']
    for column in columns:
        df[column] = df[column].astype(float)

    df_agg = \
    df.groupby('期數').agg(
        total_claimed=pd.NamedAgg(column='金額', aggfunc='sum'),
        total_service_fee=pd.NamedAgg('手續費', 'sum'),
        total_paid=pd.NamedAgg('淨額', 'sum'),
    ).reset_index().rename(columns={'期數': 'transaction_type'}).assign(transaction_type=lambda x: x['transaction_type'].map(transaction_tyep_mapping))
    return df, df_agg


nccc_raw, nccc_agg = get_nccc_cc_service_fee_detail(url_nccc_installment_report)


import pandas as pd
import warnings
warnings.filterwarnings('ignore')


def get_ub_cc_service_fee_detail(url: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_excel(url, header=3, dtype=str)
    df = df.query("~商店名稱.isna() and ~交易類別.isna()").reset_index(drop=True)

    columns = ['金額', '手續費', '淨額']
    for column in columns:
        df[column] = df[column].astype(int)

    df_agg = df.groupby('分期期數', as_index=False).agg(
        total_claimed=pd.NamedAgg(column='金額', aggfunc='sum'),
        total_service_fee=pd.NamedAgg('手續費', 'sum'),
        total_paid=pd.NamedAgg('淨額', 'sum'),
    ).assign(service_fee_rate=lambda x: x['分期期數'].map(service_fee_rate['ub']))

    df_agg['calculated_service_fee'] = df_agg.total_claimed * df_agg.service_fee_rate
    df_agg['calculated_service_fee'] = df_agg['calculated_service_fee'].apply(lambda x: round(x, 2))
    df_agg['inferred_rate'] = round(df_agg.total_service_fee / df_agg.total_claimed, 4)
    return df, df_agg.rename(columns={'分期期數': 'transaction_type'}).assign(transaction_type=lambda x: x['transaction_type'].map(transaction_tyep_mapping))


ub_raw, ub_agg = get_ub_cc_service_fee_detail(url_ub_installment_report)

if any([ub_raw.金額.sum() - ub_agg.total_claimed.sum() != 0, ub_raw.手續費.sum() - ub_agg.total_service_fee.sum() !=0]):
    raise ValueError("Validated Error")
else:
    print("Validated Success")




def get_taishi_cc_service_fee_detail(url: str, is_voucher=False) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not is_voucher:
        df = pd.read_excel(url, header=2, dtype=str, sheet_name=0)
        df = df.iloc[:df.query("卡別=='總筆數'").index[0], :]

        df.columns = ['卡別', 'transaction_type', 'count_and_amount', 'Visa', 'M/C', 'JCB', 'CUP',
            'Discover', 'S/P', 'U/C', '跨境', '小計']

        columns = ['卡別', 'transaction_type', 'count_and_amount']
        for column in columns:
            df[column] = df[column].fillna(method='ffill')

        for column in df.columns:
            if column not in columns:
                df[column] = df[column].astype(int)

        df['transaction_type'] = df['transaction_type'].str.replace(' ', '')
        df = df.query("~transaction_type.str.contains('小計')")
        df['卡別'] = df['卡別'].str.replace('\n|\|', '', regex=True)

        df_amount = df.query("count_and_amount=='金額'").copy()
        df_count = df.query("count_and_amount=='筆數'").copy()
        df_amount['service_fee_rate'] = df_amount.iloc[:, 1].map(service_fee_rate['taishi'])
        df_amount['service_fee'] = df_amount.小計 * df_amount.service_fee_rate
        df_amount['service_fee'] = df_amount['service_fee'].apply(lambda x: round(x, 2))
        return df, df_amount
    else:
        df = pd.read_excel(url, header=2, dtype=str, sheet_name=1)
        df = df.iloc[:df.query("卡別=='總筆數'").index[0], :]

        df.columns = ['卡別', 'transaction_type', 'count_and_amount', 'Visa', 'M/C', 'JCB', 'CUP',
            'Discover', 'S/P', 'U/C', '跨境', '小計']

        columns = ['卡別', 'transaction_type', 'count_and_amount']
        for column in columns:
            df[column] = df[column].fillna(method='ffill')

        for column in df.columns:
            if column not in columns:
                df[column] = df[column].astype(int)

        df['transaction_type'] = df['transaction_type'].str.replace(' ', '')
        df = df.query("~transaction_type.str.contains('小計')")
        df['卡別'] = df['卡別'].str.replace('\n|\|', '', regex=True)

        df_amount = df.query("count_and_amount=='金額'").copy()
        df_count = df.query("count_and_amount=='筆數'").copy()
        df_amount['service_fee_rate'] = df_amount.iloc[:, 1].map(service_fee_rate['taishi_voucher'])
        df_amount['service_fee'] = (df_amount.小計 * df_amount.service_fee_rate)
        df_amount['service_fee'] = df_amount['service_fee'].apply(lambda x: round(x, 2))
        return df, df_amount


taishi_raw, taishi_agg = get_taishi_cc_service_fee_detail(url_taishi_installment_report)
taishi_voucher_raw, taishi_voucher_agg = get_taishi_cc_service_fee_detail(url_taishi_installment_report, is_voucher=True)


taishi_fin = pd.merge(taishi_agg.loc[:, ['transaction_type', '小計', 'service_fee']],
                      taishi_voucher_agg.loc[:, ['transaction_type', '小計', 'service_fee']],
                      on='transaction_type',
                      how='outer',
                      suffixes=['_normal', '_voucher']).fillna(0)

taishi_fin = taishi_fin.assign(
    total_claimed=lambda x: x['小計_normal'] + x['小計_voucher'].astype(int),
    total_service_fee=lambda x: x['service_fee_normal'] + x['service_fee_voucher']
)



bank_data = [ub_agg, cub_fin, nccc_agg, taishi_fin, ctbc_agg]
banks = ['聯邦', '國泰', 'NCCC', '台新', 'CTBC']
dfs = []
for bank, df in zip(banks, bank_data):
    # print(df.columns)
    df = df[['transaction_type', 'total_claimed', 'total_service_fee']]
    df['bank'] = bank
    dfs.append(df)


df_trust_account_fee = pd.concat(dfs, ignore_index=True)
df_trust_account_fee['transaction_type'] = df_trust_account_fee.transaction_type.map(lambda x: x if x in transaction_tyep_mapping.values() else 'normal')

# 聚合長表格明細
df_trust_account_fee = df_trust_account_fee.groupby(['bank', 'transaction_type']).sum().unstack(0)


def update_normal(df, df_escrow_inv):
    """
    Update CUB and UB normal amount.
    """
    df_copy = df.copy()
    mask1 = (df_copy.index == 'normal')

    total_amount = df_escrow_inv.query("銀行.str.contains('cub')").對帳_請款金額_Trust_Account_Fee.sum()
    existing_amount = df_trust_account_fee[('total_claimed', '國泰')].sum()

    df_copy.loc[mask1, ('total_claimed', '國泰')] = total_amount - existing_amount

    total_amount = df_escrow_inv.query("銀行.isin(['ub_noninstallment', 'ub_installment'])").對帳_請款金額_Trust_Account_Fee.sum()
    existing_amount = df_trust_account_fee[('total_claimed', '聯邦')].sum()

    df_copy.loc[mask1, ('total_claimed', '聯邦')] = total_amount - existing_amount
    return df_copy


def re_index(df):
    # Define the desired order
    desired_order = ['normal', '3期', '6期', '12期', '24期']

    # Reorder the DataFrame
    df_reordered = df.reindex(desired_order)
    return df_reordered


def re_index_and_columns(df):
    # Define the desired row order
    desired_row_order = ['normal', '3期', '6期', '12期', '24期']

    # Define the desired column order for banks
    desired_bank_order = ['台新', 'NCCC', '國泰', 'CTBC', '聯邦']

    # Reorder the rows (index)
    df_reordered = df.reindex(desired_row_order)

    # Reorder the columns
    # Get the first level names (total_claimed, total_service_fee)
    level_0_names = df_reordered.columns.get_level_values(0).unique()

    # Create new column order: for each level 0, pair it with each bank in desired order
    new_columns = []
    for level_0 in level_0_names:
        for bank in desired_bank_order:
            if (level_0, bank) in df_reordered.columns:
                new_columns.append((level_0, bank))

    # Reorder columns
    df_final = df_reordered[new_columns]

    return df_final


def reorder_columns(df):
    """Reorder DataFrame columns by bank order"""
    desired_bank_order = ['台新', 'NCCC', '國泰', 'CTBC', '聯邦']

    # Get the first level names
    level_0_names = df.columns.get_level_values(0).unique()

    # Create new column order
    new_columns = []
    for level_0 in level_0_names:
        for bank in desired_bank_order:
            if (level_0, bank) in df.columns:
                new_columns.append((level_0, bank))

    return df[new_columns]


def add_subtotal_row(df, subtotal_label='小計'):
    """
    Add a subtotal row to the bottom of a MultiIndex DataFrame

    Parameters:
    df: DataFrame with MultiIndex columns
    subtotal_label: Label for the subtotal row (default: '小計')

    Returns:
    DataFrame with subtotal row added
    """
    # Create subtotal row by summing numeric columns
    subtotal_row = df.select_dtypes(include=['number']).sum()

    # For non-numeric columns, set to subtotal_label or empty string
    for col in df.columns:
        if col not in subtotal_row.index:
            if col[0] == 'transaction_type':  # Assuming first level indicates transaction_type
                subtotal_row[col] = subtotal_label
            else:
                subtotal_row[col] = ''

    # Reorder to match original column order
    subtotal_row = subtotal_row.reindex(df.columns)

    # Convert to DataFrame and set index name
    subtotal_df = pd.DataFrame([subtotal_row], index=[subtotal_label])
    subtotal_df.index.name = df.index.name

    # Concatenate with original DataFrame
    result_df = pd.concat([df, subtotal_df])

    return result_df

# Usage:
df_with_subtotal = add_subtotal_row(re_index_and_columns(update_normal(df_trust_account_fee, df_summary_escrow_inv)))


def validate_summary(df, df_escrow_inv):
    # 只驗"請款淨額"，手續費不驗
    df_val = pd.DataFrame(df.T.小計.iloc[:5])
    df_val['請款淨額'] = \
    [
     # 台新用當期請款
     df_escrow_inv.query("銀行.str.contains('taishi')").對帳_請款金額_當期.sum(),
     df_escrow_inv.query("銀行.str.contains('nccc')").對帳_請款金額_Trust_Account_Fee.sum(),
     df_escrow_inv.query("銀行.str.contains('cub')").對帳_請款金額_Trust_Account_Fee.sum(),
     df_escrow_inv.query("銀行.str.contains('ctbc')").對帳_請款金額_Trust_Account_Fee.sum(),
     df_escrow_inv.query("銀行.isin(['ub_noninstallment', 'ub_installment'])").對帳_請款金額_Trust_Account_Fee.sum()]
    df_val['diff'] = df_val['小計'] - df_val['請款淨額']

    rename_col = {
        '小計': 'trust_account_fee的小計',
        '請款淨額': 'escrow_inv的對帳_請款金額_Trust_Account_Fee(除了台新)',
    }

    df_val = df_val.rename(columns=rename_col)


    df_fee = pd.DataFrame(df.T.小計.iloc[5:])
    df_fee['escrow_inv的手續費'] = \
    [
    df_summary_escrow_inv.query("銀行.str.contains('taishi')").對帳_手續費_總計.sum(),
    df_summary_escrow_inv.query("銀行.str.contains('nccc')").對帳_手續費_總計.sum(),
    df_summary_escrow_inv.query("銀行.str.contains('cub')").對帳_手續費_總計.sum(),
    df_summary_escrow_inv.query("銀行.str.contains('ctbc')").對帳_手續費_總計.sum(),
    df_summary_escrow_inv.query("銀行.isin(['ub_noninstallment', 'ub_installment'])").對帳_手續費_總計.sum()]
    df_fee['diff'] = df_fee['小計'] - df_fee['escrow_inv的手續費']

    rename_col_fee = {
        '小計': 'trust_account_fee的小計手續費',
    }
    df_fee = df_fee.rename(columns=rename_col)



    return pd.concat([df_val, df_fee], axis=1)

trust_account_validation = validate_summary(df_with_subtotal, df_summary_escrow_inv)
trust_account_validation.map(lambda x: format(x, ','))





with pd.ExcelWriter('filing data for Trust Account Fee Accrual-SPETW.xlsx') as writer:
    df_with_subtotal.to_excel(writer, sheet_name='trust_account_fee')
    df_summary_escrow_inv.to_excel(writer, sheet_name='escrow_inv', index=False)
    invoice_summary.to_excel(writer, sheet_name='invoice_summary', index=False)
    df_full_raw.to_excel(writer, sheet_name='escrow_inv_raw', index=False)
    df_invoice.to_excel(writer, sheet_name='invoice_details', index=False)
    db_amount.to_excel(writer, sheet_name='ctbc_validation', index=False)
    trust_account_validation.to_excel(writer, sheet_name='trust_account_validation')

    bank_data = [ub_agg, cub_fin, nccc_agg, taishi_fin, ctbc_agg]
    banks = ['聯邦', '國泰', 'NCCC', '台新', 'CTBC']
    for bank, df in zip(banks, bank_data):
        df.to_excel(writer, sheet_name=bank, index=False)
