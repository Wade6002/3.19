# ==== file: system/monitor.py ====
import time
from typing import Dict, List
from datetime import datetime
from managers.process_manager import EventHubManager 
class SystemMonitor:
    def __init__(self, managers: List[EventHubManager]):
        self.managers = managers
        self.start_time = datetime.now()
        
    def generate_report(self) -> Dict:
        """生成系统状态报告"""
        return {
            "uptime": str(datetime.now() - self.start_time),
            "hubs": [self._hub_status(m) for m in self.managers]
        }
        
    def _hub_status(self, manager) -> Dict:
        """单个事件中心状态"""
        status = manager.monitor()
        return {
            "name": status['hub_name'],
            "partitions": {
                "total": status['total'],
                "active": status['active'],
                "inactive": status['inactive']
            }
        }

class ConsoleReporter:
    def __init__(self, monitor: SystemMonitor):
        self.monitor = monitor
        
    def print_report(self):
        """控制台格式化输出"""
        report = self.monitor.generate_report()
        print(f"\n=== System Status (Uptime: {report['uptime']}) ===")
        for hub in report['hubs']:
            print(f"{hub['name']}:")
            print(f"  Partitions: {hub['partitions']['active']}/{hub['partitions']['total']} active")
            if hub['partitions']['inactive']:
                print(f"  Inactive: {len(hub['partitions']['inactive'])} partitions")