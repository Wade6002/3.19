# ==== file: managers/process_manager.py ====
import os
import time
from multiprocessing import Process
from typing import List, Dict
import asyncio
from services.eventhub_consumer import EventHubConsumer


class PartitionProcess(Process):
    def __init__(self, consumer_config: Dict):
        super().__init__()
        self.consumer_config = consumer_config
        self.daemon = True

    def run(self) -> None:
        """进程入口点"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            consumer = self._create_consumer()
            loop.run_until_complete(consumer.run())
        finally:
            loop.close()

    def _create_consumer(self):
        """创建消费者实例"""
        return EventHubConsumer(self.consumer_config)

class EventHubManager:
    def __init__(self, config: Dict):  # 修改为接收处理器实例
        self.config = config
        self.processes: List[Process] = []

    def start(self) -> None:
        """启动事件中心处理"""
        for partition_id in range(self.config['partitions']):
            process_config = self._build_process_config(partition_id)
            process = PartitionProcess(process_config)
            process.start()
            # process.run()
            self.processes.append(process)

    def shutdown(self) -> None:
        """关闭所有子进程"""
        for process in self.processes:
            if process.is_alive():
                process.terminate()
                process.join()

    def _build_process_config(self, partition_id: int) -> Dict:
        """构建进程配置"""
        return {
            **self.config,
            "partition_id": partition_id,
        }

    def monitor(self) -> Dict:
        """监控进程状态"""
        return {
            "hub_name": self.config['name'],
            "total": self.config['partitions'],
            "active": sum(1 for p in self.processes if p.is_alive()),
            "inactive": [p.name for p in self.processes if not p.is_alive()]
        }