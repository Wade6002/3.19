# ==== file: system/controller.py ====
import sys
from typing import List
from signal import signal, SIGINT
from system.monitor import SystemMonitor, ConsoleReporter
from managers.process_manager import EventHubManager
import time



class ApplicationController:
    def __init__(self, managers: List[EventHubManager]):
        self.managers = managers
        self.monitor = SystemMonitor(managers)
        self.reporter = ConsoleReporter(self.monitor)
        self._setup_signal_handlers()
        
    def _setup_signal_handlers(self):
        """注册信号处理"""
        signal(SIGINT, self._handle_shutdown_signal)
        
    def _handle_shutdown_signal(self, signum, frame):
        """处理关闭信号"""
        print("\nReceived shutdown signal...")
        self.shutdown()
        sys.exit(0)
        
    def run(self):
        """启动主循环"""
        try:
            while True:
                self.reporter.print_report()
                time.sleep(60)
        except KeyboardInterrupt:
            self.shutdown()
            
    def shutdown(self):
        """执行优雅关闭"""
        print("Initiating graceful shutdown...")
        for manager in self.managers:
            manager.shutdown()