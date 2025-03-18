# ==== file: services/event_processor.py ====
from abc import ABC, abstractmethod
from typing import Dict, Any

class IEventProcessor(ABC):
    """
    事件处理接口定义（抽象基类）
    所有具体处理器必须实现 process 方法
    """
    @abstractmethod
    async def process(self, partition_context, event: Dict[str, Any]) -> None:
        """
        处理单个事件的抽象方法
        :param partition_context: 分区上下文对象
        :param event: 标准化事件字典，包含：
            - body: 消息体
            - properties: 消息属性
            - offset: 消息偏移量
            - sequence_number: 序列号
            - partition_id: 分区ID
            - enqueued_time: 入队时间
        """
        pass