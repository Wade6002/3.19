# ==== file: services/log_sender.py ====
import gzip
import json
import asyncio
from typing import NamedTuple, Generator, List, Dict, Any, Optional
from urllib.parse import urlparse
import aiohttp
# from libs.config_loader import AppConfig
from libs.logger import EventHubLogger

class LogBatch(NamedTuple):
    """日志批次数据容器"""
    serialized_batch: str
    number_of_logs: int

class LogSender:
    """Dynatrace日志发送处理器"""

    def __init__(self, config):
        self.config = config
        self.logger = EventHubLogger.get_logger("LogSender")
        
        # 初始化Dynatrace端点
        self._endpoint = self._build_endpoint_url()
        self._auth_header = f"Api-Token {self.config['dynatrace']['api_token']}"
        
        # 初始化SSL上下文
        self._ssl_context = self._create_ssl_context()


    def _build_endpoint_url(self) -> str:
        """构建完整的日志接入URL"""
        base = urlparse(self.config['dynatrace']['base_url'])
        return f"{base.scheme}://{base.netloc}{self.config['dynatrace']['log_ingest_endpoint']}"

    def _create_ssl_context(self) -> Optional[bool]:
        """创建SSL验证策略"""
        if not self.config['security']['ssl']['verify_certificate']:
            self.logger.warning("SSL certificate verification is DISABLED")
            return False
        return None  # 使用系统默认验证

    async def send(self, logs: List[Dict]) -> bool:
        """发送日志主入口"""
        try:
            batches = list(self._prepare_batches(logs))
            if not batches:
                self.logger.warning("No valid batches to send")
                return True

            semaphore = asyncio.Semaphore(
                self.config['performance']['http']['concurrency']
            )
            
            async with aiohttp.ClientSession() as session:
                tasks = [
                    self._process_batch(session, batch, semaphore)
                    for batch in batches
                ]
                results = await asyncio.gather(*tasks)
                
            return all(results)
        except Exception as e:
            self.logger.critical("Fatal error in log sending", exc_info=True)
            return False

    def _prepare_batches(self, logs: List[Dict]) -> Generator[LogBatch, None, None]:
        """生成符合要求的日志批次（生成器版本）"""
        current_batch = []
        current_size = 0
        max_size = self.config['log_ingestion']['max_request_size']
        max_events = self.config['log_ingestion']['max_events_per_request']

        for log in logs:
            serialized = json.dumps(log)
            entry_size = len(serialized.encode('utf-8'))
            
            # 跳过超限条目
            if entry_size > max_size:
                self.logger.warning(
                    "Oversized log entry skipped",
                    extra={"size": entry_size, "limit": max_size}
                )
                continue

            # 检查是否需要提交当前批次
            if (len(current_batch) >= max_events or 
                (current_size + entry_size) > max_size):
                yield self._create_batch(current_batch)
                current_batch.clear()
                current_size = 0

            current_batch.append(log)
            current_size += entry_size

        # 提交最后一批
        if current_batch:
            yield self._create_batch(current_batch)

    def _create_batch(self, logs: List[Dict]) -> LogBatch:
        """创建标准化批次对象"""
        return LogBatch(
            serialized_batch=json.dumps(logs),
            number_of_logs=len(logs)
        )

    async def _process_batch(self, 
                           session: aiohttp.ClientSession,
                           batch: LogBatch,
                           semaphore: asyncio.Semaphore) -> bool:
        """处理单个日志批次（含信号量控制）"""
        async with semaphore:
            for attempt in range(1, self.config['performance']['http']['max_retries'] + 1):
                try:
                    return await self._send_batch(session, batch)
                except Exception as e:
                    if attempt == self.config['performance']['http']['max_retries']:
                        self.logger.error(
                            "Final retry failed for batch",
                            extra={
                                "batch_size": batch.number_of_logs,
                                "attempts": attempt
                            },
                            exc_info=True
                        )
                        return False
                    await asyncio.sleep(attempt * 1.5)  # 指数退避

            return False  # 理论上不会执行到这里

    async def _send_batch(self, 
                        session: aiohttp.ClientSession,
                        batch: LogBatch) -> bool:
        """执行单批次发送操作"""
        compressed = self._compress_data(batch.serialized_batch)
        
        headers = {
            "Authorization": self._auth_header,
            "Content-Type": "application/json",
            "Content-Encoding": "gzip"
        }
        
        try:
            async with session.post(
                self._endpoint,
                data=compressed,
                headers=headers,
                ssl=self._ssl_context,
                timeout=aiohttp.ClientTimeout(
                    total=self.config['performance']['http']['timeout']
                )
            ) as response:
                return await self._handle_response(response, batch)
        except aiohttp.ClientError as e:
            self.logger.error(
                "Network error during send",
                extra={
                    "error_type": type(e).__name__,
                    "error_msg": str(e)
                }
            )
            return False

    def _compress_data(self, payload: str) -> bytes:
        """GZIP压缩处理"""
        compressed = gzip.compress(
            payload.encode('utf-8'),
            compresslevel=self.config['performance']['compression']['gzip_level']
        )
        
        # 记录压缩效率
        original_size = len(payload.encode('utf-8'))
        compressed_size = len(compressed)
        ratio = compressed_size / original_size if original_size > 0 else 0
        
        self.logger.debug(
            "Payload compression stats",
            extra={
                "original_size": original_size,
                "compressed_size": compressed_size,
                "compression_ratio": f"{ratio:.1%}"
            }
        )
        
        return compressed

    async def _handle_response(self, 
                             response: aiohttp.ClientResponse,
                             batch: LogBatch) -> bool:
        """处理HTTP响应"""
        if response.status < 300:
            self.logger.info(
                "Batch successfully sent",
                extra={
                    "batch_size": batch.number_of_logs,
                    "status": response.status
                }
            )
            return True
            
        response_text = await response.text()
        self.logger.error(
            "Failed to send batch",
            extra={
                "status": response.status,
                "reason": response.reason,
                "response_sample": response_text[:500],  # 安全截断
                "batch_size": batch.number_of_logs
            }
        )
        return False