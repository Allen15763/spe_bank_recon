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

    # 拿到FRR前
    result = task.execute(mode='full')
    # 拿到FRR後
    result = task.execute(mode='full_with_entry')

# 200208其實不放期初期末數也沒差，入帳入200701的數字就好->分錄有平的情況下
# Step 13 調扣調整固定NCCC放3期其他放Normal，但實際可能異動
# 這期tax直接在outbound了，(預設已扣繳)以前是DFR上利息100，DR bank 90 Tax 10, CR Rev 100，
# 在DFR的tax有扣繳利息稅額數字時，按原本方法Tax那包被含進outbound所以movement不同了
# TBC upload form、平台check
