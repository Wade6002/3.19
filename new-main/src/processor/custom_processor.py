# ==== file: processors/custom_processor.py ====
import json
import re
import os
import asyncio
from json import JSONDecodeError
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

from libs.config_loader import ConfigLoader
from libs.logger import EventHubLogger
from services.logs_sender import LogSender
from processor.event_processor import IEventProcessor
from processor.sub.metadata_engine import MetadataEngine
from processor.sub.mapping import (
    extract_resource_id_attributes,
    extract_severity,
    azure_properties_names
)
from processor.sub.monitored_entity_id import infer_monitored_entity_id

class CustomProcessor(IEventProcessor):
    def __init__(self):
        config_loader = ConfigLoader()
        config = config_loader.load()
        self.logger = EventHubLogger.get_logger("CustomProcessor")
        self.sender = LogSender(config)  # 注入配置初始化的发送器
        
        # 初始化处理参数
        self.attribute_limit = config["log_processing"]["attribute_value_length_limit"]
        self.content_limit = config["log_processing"]["content_length_limit"]
        self.truncated_mark = config["log_processing"]["content_truncated_mark"]
        
        self.metadata_engine = MetadataEngine()
        self._init_time_formats()

    def _init_time_formats(self):
        """预编译时间戳正则表达式"""
        self.time_patterns = [
            re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"),  # ISO格式
            re.compile(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}")  # 月/日/年格式
        ]

    async def process(self, partition_context, event: dict) -> None:
        """处理事件主流程"""
        try:
            logs = self.extract_logs(event)
            if logs:
                self.logger.info(
                    "Parsed %d valid logs from partition %s",
                    len(logs),
                    partition_context.partition_id,
                    extra={"partition": partition_context.partition_id}
                )
                await self._send_logs_safely(logs)
        except Exception as e:
            self._log_processing_error(partition_context, e)
            raise

    async def _send_logs_safely(self, logs: List[Dict]) -> None:
        """带异常处理的日志发送"""
        try:
            if not await self.sender.send(logs):
                self.logger.error("Partial logs failed to send")
        except Exception as e:
            self.logger.critical("Log sending aborted", exc_info=True)
            raise

    def extract_logs(self, event: dict) -> List[Dict]:
        """从原始事件提取结构化日志"""
        try:
            event_body = event.body_as_str(encoding="UTF-8")
            if not event_body:
                return []
                
            return [
                self._process_record(record)
                for record in self._parse_event_body(event_body)
                if record is not None
            ]
        except Exception as e:
            self._log_event_error(event, e)
            return []

    def _parse_event_body(self, body: str) -> List[Dict]:
        """解析事件体为JSON"""
        try:
            return json.loads(body).get("records", [])
        except JSONDecodeError as e:
            return self._attempt_fallback_parsing(body, e)

    def _attempt_fallback_parsing(self, body: str, initial_error: Exception) -> List[Dict]:
        """尝试多种方式解析异常格式JSON"""
        modified_body = body.replace("\n", "").replace("'", "\"")
        try:
            return json.loads(modified_body, strict=False).get("records", [])
        except Exception:
            self._log_json_parse_failure(body, initial_error)
            return []

    def _process_record(self, record: Dict) -> Optional[Dict]:
        """处理单条日志记录"""
        try:
            self._deserialize_properties(record)
            parsed = self._build_parsed_record(record)
            return self._apply_field_limits(parsed)
        except Exception as e:
            self._log_record_error(record, e)
            return None

    def _build_parsed_record(self, record: Dict) -> Dict:
        """构建结构化日志记录"""
        parsed = {"cloud.provider": "Azure"}
        
        # 提取核心字段
        extract_severity(record, parsed)
        self._extract_resource_info(record, parsed)
        
        # 应用元数据规则
        self.metadata_engine.apply(record, parsed)
        self._convert_timestamp(parsed)
        
        # 推断监控实体
        category = record.get("category", "").lower()
        infer_monitored_entity_id(category, parsed)
        
        return parsed

    def _extract_resource_info(self, record: Dict, parsed: Dict) -> None:
        """提取Azure资源信息"""
        if "resourceId" in record:
            extract_resource_id_attributes(parsed, record["resourceId"])

    def _convert_timestamp(self, record: Dict) -> None:
        """统一时间戳格式"""
        if "timestamp" in record:
            record["timestamp"] = self._parse_datetime(record["timestamp"])

    def _parse_datetime(self, timestamp: str) -> str:
        """智能时间戳解析"""
        for pattern in self.time_patterns:
            if pattern.match(timestamp):
                try:
                    dt = datetime.strptime(timestamp, self._get_format(pattern))
                    return dt.isoformat(timespec="milliseconds")
                except ValueError:
                    continue
        return datetime.now(timezone.utc).isoformat()

    def _get_format(self, pattern: re.Pattern) -> str:
        """根据正则匹配返回对应时间格式"""
        return "%Y-%m-%d %H:%M:%S" if "202" in pattern.pattern else "%m/%d/%Y %H:%M:%S"

    def _apply_field_limits(self, record: Dict) -> Dict:
        """应用字段长度限制"""
        return {
            key: self._truncate_value(key, value)
            for key, value in record.items()
        }

    def _truncate_value(self, key: str, value: Any) -> str:
        """字段值截断处理"""
        if key == "content":
            return self._truncate_content(value)
            
        if key in ["severity", "timestamp"]:
            return str(value)
            
        return self._truncate_generic_field(value)

    def _truncate_content(self, content: Any) -> str:
        """内容字段特殊处理"""
        content_str = json.dumps(content) if not isinstance(content, str) else content
        if len(content_str) > self.content_limit:
            return content_str[:self.content_limit - len(self.truncated_mark)] + self.truncated_mark
        return content_str

    def _truncate_generic_field(self, value: Any) -> str:
        """通用字段截断"""
        str_value = str(value)
        if len(str_value) > self.attribute_limit:
            return str_value[:self.attribute_limit - len(self.truncated_mark)] + self.truncated_mark
        return str_value

    def _deserialize_properties(self, record: Dict) -> None:
        """反序列化properties字段"""
        prop_key = next((k for k in azure_properties_names if k in record), None)
        if prop_key and isinstance(record.get(prop_key), str):
            try:
                record[prop_key] = json.loads(record[prop_key])
            except JSONDecodeError:
                pass

    def _log_json_parse_failure(self, body: str, error: Exception) -> None:
        """记录JSON解析失败日志"""
        sample = (body[:500] + "[...]") if len(body) > 500 else body
        self.logger.error(
            "Failed to parse event body",
            extra={
                "body_sample": sample,
                "error": str(error),
                "event_id": "json_parse_failure"
            }
        )

    def _log_record_error(self, record: Dict, error: Exception) -> None:
        """记录单条日志处理错误"""
        self.logger.warning(
            "Record processing failed",
            extra={
                "record_sample": str(record)[:200],
                "error": str(error),
                "event_id": "record_processing_error"
            }
        )

    def _log_event_error(self, event: Dict, error: Exception) -> None:
        """记录事件处理错误"""
        self.logger.error(
            "Event processing failed",
            extra={
                "event_offset": event.get("offset", "unknown"),
                "error": str(error),
                "event_id": "event_processing_error"
            }
        )

    def _log_processing_error(self, partition_context, error: Exception) -> None:
        """记录处理过程错误"""
        self.logger.critical(
            "Partition processing failed",
            extra={
                "partition": partition_context.partition_id,
                "error": str(error),
                "event_id": "partition_failure"
            },
            exc_info=True
        )