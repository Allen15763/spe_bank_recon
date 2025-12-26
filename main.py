# import spe_bank_recon.utils.database as db_tools

"""
SPE Bank Recon 主程式 - 重構版本
使用 Task 類封裝，參考 Offline Tasks 框架

編程調用示例:
    from spe_bank_recon.tasks.bank_recon import run_bank_recon
    result = run_bank_recon()
"""

import sys
from pathlib import Path

# 添加 src 到路徑
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.tasks.bank_recon import BankReconTask
from src.utils import get_logger


if __name__ == "__main__":
    task = BankReconTask()
    
    result = task.execute()
