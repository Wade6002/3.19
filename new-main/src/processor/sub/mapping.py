import json
import os
from typing import Dict

from libs.logger import EventHubLogger

# 初始化日志记录器
logger = EventHubLogger.get_logger("AzureResourceProcessor")

# 常量定义
DEFAULT_SEVERITY_INFO = "Informational"
RESOURCE_ID_ATTRIBUTE = "azure.resource.id"
SUBSCRIPTION_ATTRIBUTE = "azure.subscription"
RESOURCE_GROUP_ATTRIBUTE = "azure.resource.group"
RESOURCE_TYPE_ATTRIBUTE = "azure.resource.type"
RESOURCE_NAME_ATTRIBUTE = "azure.resource.name"

log_level_to_severity_dict = {
    1: 'Critical',
    2: 'Error',
    3: 'Warning',
    4: 'Informational'
}

severity_to_log_level_dict = {v: k for k, v in log_level_to_severity_dict.items()}

azure_level_properties = ['Level', 'level']
azure_properties_names = ['properties', 'EventProperties']
activity_log_categories = ['alert', 'administrative', 'resourcehealth', 'servicehealth', 'security', 'policy', 'recommendation', 'autoscale']

# 加载ME类型映射
working_directory = os.path.dirname(os.path.realpath(__file__))
me_type_mapper_file_path = os.path.join(working_directory, "me_type_mapper.json")
dt_me_type_mapper = {}

try:
    with open(me_type_mapper_file_path) as me_type_mapper_file:
        me_type_mapper_json = json.load(me_type_mapper_file)
        # logger.info("Loading ME type mapping file", extra={"file_path": me_type_mapper_file_path})
        
        for resource_type_to_me_type in me_type_mapper_json:
            resource_type = resource_type_to_me_type["resourceType"].lower()
            category = resource_type_to_me_type.get("category", "").lower()
            key = ",".join(filter(None, [resource_type, category]))
            dt_me_type_mapper.update({key: resource_type_to_me_type["meType"]})
            
        logger.debug("ME type mapping loaded", extra={"mapping_count": len(dt_me_type_mapper)})
        
except Exception as e:
    logger.error(
        "Failed to load ME type mapping file",
        extra={
            "file_path": me_type_mapper_file_path,
            "error": str(e)
        },
        exc_info=True
    )
    dt_me_type_mapper = {}

def extract_resource_id_attributes(parsed_record: Dict, resource_id: str):
    """
    增强版资源ID解析（添加错误跟踪和调试信息）
    """
    try:
        parsed_record[RESOURCE_ID_ATTRIBUTE] = resource_id
        parts = resource_id.lstrip("/").split("/")

        logger.debug(
            "Processing resource ID",
            extra={"resource_id_sample": resource_id[:200]}
        )

        # 验证资源ID结构
        if len(parts) < 7:
            logger.warning(
                "Invalid resource ID structure",
                extra={
                    "reason": "insufficient_parts",
                    "part_count": len(parts),
                    "resource_id_sample": resource_id[:200]
                }
            )
            return
            
        if parts[0].casefold() != "subscriptions":
            logger.warning(
                "Invalid resource ID prefix",
                extra={
                    "expected": "subscriptions",
                    "actual": parts[0].lower(),
                    "resource_id_sample": resource_id[:200]
                }
            )
            return

        if parts[2].casefold() != "resourcegroups":
            logger.warning(
                "Missing resource groups section",
                extra={"resource_id_sample": resource_id[:200]}
            )
            return

        if parts[4].casefold() != "providers":
            logger.warning(
                "Missing providers section",
                extra={"resource_id_sample": resource_id[:200]}
            )
            return

        # 提取订阅信息
        parsed_record[SUBSCRIPTION_ATTRIBUTE] = parts[1]
        parsed_record[RESOURCE_GROUP_ATTRIBUTE] = parts[3]
        parsed_record[RESOURCE_NAME_ATTRIBUTE] = parts[-1]

        # 处理资源类型层级
        resource_type_parts_with_parent = parts[5:-1]
        resource_type_parts = [
            part for index, part 
            in enumerate(resource_type_parts_with_parent) 
            if (index == 0 or index % 2 != 0)
        ]
        parsed_record[RESOURCE_TYPE_ATTRIBUTE] = "/".join(resource_type_parts)
        
        logger.debug(
            "Successfully parsed resource ID",
            extra={
                "subscription": parts[1],
                "resource_group": parts[3],
                "resource_type": parsed_record[RESOURCE_TYPE_ATTRIBUTE]
            }
        )

    except Exception as e:
        logger.error(
            "Resource ID processing failed",
            extra={
                "resource_id_sample": resource_id[:200],
                "error": str(e)
            },
            exc_info=True
        )

def extract_severity(record: Dict, parsed_record: Dict):
    """增强版严重性提取"""
    try:
        level_property = next(
            (level for level in azure_level_properties 
             if level in record.keys()), 
            None
        )
        
        if level_property:
            map_to_severity(record, parsed_record, level_property)
        else:
            parsed_record["severity"] = DEFAULT_SEVERITY_INFO
            logger.debug(
                "Using default severity level",
                extra={"default_severity": DEFAULT_SEVERITY_INFO}
            )
            
    except Exception as e:
        logger.error(
            "Severity extraction failed",
            extra={"error": str(e)},
            exc_info=True
        )
        parsed_record["severity"] = DEFAULT_SEVERITY_INFO

def map_to_severity(record: Dict, parsed_record: Dict, level_property: str):
    """增强版严重性映射"""
    try:
        raw_value = record[level_property]
        
        if isinstance(raw_value, int):
            severity = log_level_to_severity_dict.get(raw_value, DEFAULT_SEVERITY_INFO)
            parsed_record["severity"] = severity
            
            logger.info(
                "Mapped integer severity level",
                extra={
                    "original_value": raw_value,
                    "mapped_severity": severity
                }
            )
        else:
            parsed_record["severity"] = raw_value
            logger.debug(
                "Using string severity value",
                extra={"severity_value": raw_value}
            )
            
        # 验证有效性
        if parsed_record["severity"] not in severity_to_log_level_dict:
            logger.warning(
                "Unrecognized severity value",
                extra={
                    "severity_value": parsed_record["severity"],
                    "allowed_values": list(severity_to_log_level_dict.keys())
                }
            )
            
    except KeyError:
        logger.warning(
            "Missing severity property",
            extra={"expected_properties": azure_level_properties}
        )
        parsed_record["severity"] = DEFAULT_SEVERITY_INFO
        
    except Exception as e:
        logger.error(
            "Severity mapping failed",
            extra={"error": str(e)},
            exc_info=True
        )
        parsed_record["severity"] = DEFAULT_SEVERITY_INFO