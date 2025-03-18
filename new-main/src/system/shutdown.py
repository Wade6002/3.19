# # ==== file: system/shutdown.py ====
# from typing import List

# from process_manager import EventHubManager
# # 全局变量保存需要关闭的管理器实例
# _managers: List[EventHubManager] = []

# def register_managers(managers: List[EventHubManager]) -> None:
#     """注册需要关闭的管理器"""
#     global _managers
#     _managers = managers

# def shutdown_all() -> None:
#     """关闭所有已注册的管理器"""
#     print("Initiating graceful shutdown...")
#     for manager in _managers:
#         manager.shutdown()
#     print("All managers have been shut down.")

# if __name__ == "__main__":
#     # 直接运行此文件时触发关闭（需确保已注册管理器）
#     shutdown_all()