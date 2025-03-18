# ==== file: services/eventhub_consumer.py ====
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from typing import Dict, Any
from local_checkpoint import FileCheckpointStore
from libs.logger import EventHubLogger
from libs.exceptions import error_handler, ProcessingError
from processor.custom_processor import CustomProcessor
from processor.event_processor import IEventProcessor  # 新增接口依赖

class EventHubConsumer:
    def __init__(self, config: Dict[str, Any]):
        """
        初始化事件中心消费者
        :param config: 配置字典
        :param processor: 事件处理器实例（必须实现 IEventProcessor 接口）
        """
        self.config = config
        self.logger = EventHubLogger.get_logger(f"consumer.{config['name']}")
        self._client = None
        self.processor = CustomProcessor()
        
        # 初始化本地检查点存储
        self.checkpoint_store = FileCheckpointStore(
            storage_dir=config['local_checkpoint_dir'],
            eventhub_name=config['name'],
            consumer_group=config['consumer_group']
        )

    @property
    def client(self) -> EventHubConsumerClient:
        """懒加载客户端实例"""
        if not self._client:
            self._client = EventHubConsumerClient.from_connection_string(
                conn_str=self.config['connection_str'],
                consumer_group=self.config['consumer_group'],
                eventhub_name=self.config['name']
            )
        return self._client

    @error_handler(max_retries=3)
    async def process_message(self, partition_context, event) -> None:
        print(f"Received event from partition {partition_context.partition_id}")

        """
        统一消息处理入口（由SDK自动调用）
        :param partition_context: 分区上下文对象
        :param event: 原始事件对象
        """
        try:
            # 1. 构建标准化事件格式
            # event_data = {
            #     "body": event.body_as_str(encoding="UTF-8"),
            #     "properties": event.properties,
            #     "offset": event.offset,
            #     "sequence_number": event.sequence_number,
            #     "partition_id": partition_context.partition_id,
            #     "enqueued_time": event.enqueued_time
            # }

            # 2. 调用处理器逻辑
            await self.processor.process(partition_context, event)

            # 3. 更新检查点（仅在处理成功后）
            await partition_context.update_checkpoint(event)
            
            # 4. 记录处理成功日志
            self.logger.debug(
                "Event processed successfully",
                extra={"partition": partition_context.partition_id}
            )

        except Exception as e:
            self.logger.error(
                "Event processing failed",
                extra={
                    "partition": partition_context.partition_id,
                    "error": str(e)
                },
                exc_info=True
            )
            raise ProcessingError(
                message="Event processing failed",
                context={
                    "partition": partition_context.partition_id,
                    "offset": event.offset
                }
            ) from e

    async def run(self) -> None:
        """
        启动消费者主循环
        """
        async with self.client:
            await self.client.receive(
                on_event=self.process_message
               )

    async def graceful_shutdown(self) -> None:
        """
        优雅关闭消费者
        """
        if self._client:
            await self._client.close()
            self.logger.info("Consumer client closed gracefully")