"""
BankSummaryFormatter - 統一的銀行摘要格式化工具

提供一致的摘要輸出格式，消除重複的列印代碼。
"""

from typing import Optional
import logging

from ..models import BankDataContainer


class BankSummaryFormatter:
    """
    統一的銀行摘要格式化工具

    用於格式化銀行處理結果的摘要輸出。
    所有銀行步驟使用統一的格式，提升可維護性。
    """

    def __init__(self, logger: logging.Logger):
        """
        初始化格式化工具

        Args:
            logger: 日誌記錄器
        """
        self.logger = logger

    def print_container_summary(
        self,
        container: BankDataContainer,
        category: Optional[str] = None
    ):
        """
        列印容器摘要

        自動適配不同銀行的特殊字段:
        - 所有銀行都有的基礎字段: recon_amount, recon_service_fee
        - CTBC 特有字段: trust_account_fee
        - 前期相關字段: last_recon_amount, last_recon_service_fee

        Args:
            container: 銀行資料容器
            category: 類別名稱（可選，用於多類別銀行）
        """
        # 構建標題
        category_str = f" [{category}]" if category else ""
        self.logger.info(f"\n--- {container.bank_name}{category_str} 摘要 ---")

        # 顯示當期請款金額
        self.logger.info(f"對帳 請款金額(當期): {container.recon_amount:,}")

        # 如果有前期金額，顯示前期金額（CUB, CTBC, UB 有此字段）
        if hasattr(container, 'amount_claimed_last_period_paid_by_current'):
            last_period_amount = container.amount_claimed_last_period_paid_by_current
            if last_period_amount and last_period_amount != 0:
                self.logger.info(f"對帳 請款金額(前期): {last_period_amount:,}")

        # 如果有調整金額，顯示調整金額
        if hasattr(container, 'adj_service_fee'):
            adj_amount = container.adj_service_fee
            if adj_amount and adj_amount != 0:
                self.logger.info(f"對帳 調整金額: {adj_amount:,}")

        # 如果有 Trust Account Fee，顯示（CUB, CTBC, UB 有此字段）
        if hasattr(container, 'recon_amount_for_trust_account_fee'):
            trust_fee = container.recon_amount_for_trust_account_fee
            if trust_fee and trust_fee != 0:
                self.logger.info(f"對帳 請款金額(Trust Account Fee): {trust_fee:,}")

        # 分隔線
        self.logger.info("-" * 20)

        # 顯示當期手續費
        self.logger.info(f"對帳 手續費(當期): {container.recon_service_fee:,}")

        # 如果有前期手續費，顯示前期手續費
        if hasattr(container, 'service_fee_claimed_last_period_paid_by_current'):
            last_period_fee = container.service_fee_claimed_last_period_paid_by_current
            if last_period_fee and last_period_fee != 0:
                self.logger.info(f"對帳 手續費(前期): {last_period_fee:,}")

                # 計算並顯示總手續費
                total_service_fee = container.recon_service_fee + last_period_fee
                self.logger.info(f"對帳 手續費(前期+當期): {total_service_fee:,}")

        # 分隔線
        self.logger.info("-" * 20)

        # 顯示發票金額（如果有）
        if hasattr(container, 'invoice_amount_claimed'):
            invoice_amount = container.invoice_amount_claimed
            if invoice_amount and invoice_amount != 0:
                self.logger.info(f"發票 請款金額: {invoice_amount:,}")

        if hasattr(container, 'invoice_service_fee'):
            invoice_fee = container.invoice_service_fee
            if invoice_fee and invoice_fee != 0:
                self.logger.info(f"發票 手續費: {invoice_fee:,}")

        # 結束空行
        self.logger.info("")

    def print_multiple_containers_summary(
        self,
        containers: list[BankDataContainer],
        bank_name: str
    ):
        """
        列印多個容器的總計摘要

        用於有多個類別的銀行（如 CUB, CTBC, UB）。

        Args:
            containers: 容器列表
            bank_name: 銀行名稱
        """
        if not containers:
            self.logger.warning(f"{bank_name} 沒有處理結果")
            return

        # 計算總計
        total_amount = sum(c.recon_amount for c in containers)
        total_fee = sum(c.recon_service_fee for c in containers)

        # 如果所有容器都有 Trust Account Fee，計算總計
        if all(hasattr(c, 'recon_amount_for_trust_account_fee') for c in containers):
            total_trust_fee = sum(c.recon_amount_for_trust_account_fee for c in containers)
            has_trust_fee = True
        else:
            has_trust_fee = False

        # 顯示總計
        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"{bank_name} 總計:")
        self.logger.info(f"  總請款金額: {total_amount:,}")

        if has_trust_fee:
            self.logger.info(f"  總 Trust Account Fee: {total_trust_fee:,}")

        self.logger.info(f"  總手續費: {total_fee:,}")
        self.logger.info(f"{'=' * 60}\n")
