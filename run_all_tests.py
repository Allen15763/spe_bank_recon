"""
運行所有單元測試

此腳本用於在有完整依賴（pandas, duckdb）的環境中運行所有測試。
"""

import sys
import subprocess
from pathlib import Path

project_root = Path(__file__).parent

def run_test_file(test_file: str):
    """運行單個測試文件"""
    print(f"\n{'=' * 60}")
    print(f"運行測試: {test_file}")
    print('=' * 60)

    result = subprocess.run(
        [sys.executable, str(project_root / test_file)],
        capture_output=False,
        text=True
    )

    return result.returncode == 0

def main():
    """運行所有測試"""
    print("\n" + "=" * 60)
    print("運行所有單元測試")
    print("=" * 60)

    test_files = [
        'tests/utils/test_config_manager.py',
        'tests/core/datasources/test_datasource_base.py',
        'tests/utils/test_file_utils.py',
    ]

    results = {}
    for test_file in test_files:
        results[test_file] = run_test_file(test_file)

    print("\n" + "=" * 60)
    print("測試總結")
    print("=" * 60)

    passed = sum(1 for result in results.values() if result)
    total = len(results)

    for test_file, result in results.items():
        status = "[OK] 通過" if result else "[FAIL] 失敗"
        print(f"{test_file}: {status}")

    print(f"\n總計: {passed}/{total} 測試文件通過")

    return passed == total

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
