import json
import os
import re
from dataclasses import dataclass
from os import listdir
from os.path import isfile
from typing import Dict, List, Optional, Any
from pathlib import Path

import jmespath

from libs.logger import EventHubLogger
from .jmespath_custom import JMESPATH_OPTIONS
from .mapping import RESOURCE_TYPE_ATTRIBUTE

# 初始化日志记录器
logger = EventHubLogger.get_logger("MetadataEngine")

# region 比较器函数
def _eq_comparator(x, y) -> bool:
    """Case-insensitive equality check"""
    return str(x).casefold() == str(y).casefold()

def _in_comparator(x, y) -> bool:
    """Case-insensitive contains check"""
    return str(x).casefold() in str(y).casefold().split(',')

def _prefix_comparator(x, y) -> bool:
    """Case-insensitive prefix match"""
    return str(x).casefold().startswith(str(y).casefold())

def _contains_comparator(x, y) -> bool:
    """Case-insensitive substring match"""
    return str(y).casefold() in str(x).casefold()

_CONDITION_COMPARATOR_MAP = {
    "$eq".casefold(): _eq_comparator,
    "$in".casefold(): _in_comparator,
    "$prefix".casefold(): _prefix_comparator,
    "$contains".casefold(): _contains_comparator,
}
# endregion

# region 值提取函数
def _extract_resource_type(record: Dict, parsed_record: Dict) -> str:
    """Extract resource type from parsed record"""
    return parsed_record.get(RESOURCE_TYPE_ATTRIBUTE, "")

def _extract_category(record: Dict, parsed_record: Dict) -> str:
    """Extract category from original record"""
    return record.get("category", "")

_SOURCE_VALUE_EXTRACTOR_MAP = {
    "resourceType".casefold(): _extract_resource_type,
    "category".casefold(): _extract_category
}
# endregion

# region 数据类
@dataclass(frozen=True)
class Attribute:
    """Represents a metadata attribute mapping"""
    key: str
    pattern: str

@dataclass(frozen=True)
class ConfigRule:
    """Defines a complete metadata mapping rule"""
    entity_type_name: str
    source_matchers: List['SourceMatcher']
    attributes: List[Attribute]
# endregion

class SourceMatcher:
    """Handles source matching logic"""
    def __init__(self, source: str, condition: str):
        self.source = source
        self.condition = condition
        self.valid = True
        self._evaluator = None
        self._operand = None
        self._source_value_extractor = None

        # 解析条件表达式
        for key in _CONDITION_COMPARATOR_MAP:
            if condition.startswith(key):
                self._evaluator = _CONDITION_COMPARATOR_MAP[key]
                break

        # 提取操作数
        operands = re.findall(r"'(.*?)'", condition, re.DOTALL)
        self._operand = ','.join(operands) if operands else None
        
        # 获取值提取器
        self._source_value_extractor = _SOURCE_VALUE_EXTRACTOR_MAP.get(source.casefold())

        # 有效性验证
        if not self._source_value_extractor:
            logger.warning("Unsupported source type", 
                         extra={"source": source, "event_id": "unsupported-source-type-warning"})
            self.valid = False
            
        if not self._evaluator or not self._operand:
            logger.warning("Condition macro parsing failed",
                         extra={"expression": condition, "event_id": "condition-macro-parsing-warning"})
            self.valid = False

    def match(self, record: Dict, parsed_record: Dict) -> bool:
        """执行匹配逻辑"""
        if not self.valid:
            return False
        try:
            value = self._source_value_extractor(record, parsed_record)
            return self._evaluator(value, self._operand)
        except Exception as e:
            logger.error("Matching failed",
                       extra={"source": self.source, "error": str(e)},
                       exc_info=True)
            return False

class MetadataEngine:
    """Main metadata processing engine"""
    def __init__(self):
        self.rules: List[ConfigRule] = []
        self.default_rule: Optional[ConfigRule] = None
        self._load_configs()

    def _load_configs(self):
        """加载所有配置文件"""
        try:
            # 动态计算配置路径
            current_file = Path(__file__).absolute()
            project_root = current_file.parents[3]  # 根据实际目录结构调整
            config_dir = project_root / "config" / "config_rule"
            
            if not config_dir.exists():
                logger.error("Config directory missing", 
                           extra={"path": str(config_dir)})
                return

            config_files = [
                f for f in listdir(config_dir)
                if isfile(config_dir / f) and f.endswith(".json")
            ]
            
            for cfg_file in config_files:
                self._load_single_config(config_dir / cfg_file)
                
        except Exception as e:
            logger.error("Config loading failed",
                       extra={"error": str(e)},
                       exc_info=True)

    def _load_single_config(self, config_path: Path):
        """加载单个配置文件"""
        try:
            with open(config_path) as f:
                config = json.load(f)
                
                if config.get("name") == "default":
                    self.default_rule = self._create_config_rule(
                        entity_name="default",
                        rule_def=config
                    )
                else:
                    self.rules.extend(self._process_config(config))
                    
                logger.debug("Config loaded",
                           extra={"file": config_path.name})
                
        except Exception as e:
            logger.error("Config file error",
                       extra={"file": str(config_path), "error": str(e)},
                       exc_info=True)

    def apply(self, record: Dict, parsed_record: Dict):
        """应用元数据规则"""
        try:
            # 尝试匹配所有规则
            for rule in self.rules:
                if self._is_rule_applicable(rule, record, parsed_record):
                    self._apply_rule(rule, parsed_record, record)
                    return
            
            # 回退到默认规则
            if self.default_rule:
                self._apply_rule(self.default_rule, parsed_record, record)
                
        except Exception as e:
            logger.error("Metadata processing failed",
                       extra={"error": str(e)},
                       exc_info=True)

    def _is_rule_applicable(self, rule: ConfigRule, record: Dict, parsed_record: Dict) -> bool:
        """验证规则适用性"""
        return all(matcher.match(record, parsed_record) for matcher in rule.source_matchers)

    def _apply_rule(self, rule: ConfigRule, parsed_record: Dict, raw_record: Dict):
        """执行规则映射"""
        for attr in rule.attributes:
            try:
                value = jmespath.search(attr.pattern, raw_record, JMESPATH_OPTIONS)
                if value is not None:
                    parsed_record[attr.key] = value
            except Exception as e:
                logger.error("Attribute mapping failed",
                           extra={"attribute": attr.key, "pattern": attr.pattern, "error": str(e)},
                           exc_info=True)

    def _process_config(self, config: Dict) -> List[ConfigRule]:
        """处理单个配置文件"""
        return [
            self._create_config_rule(config.get("name", "unnamed"), rule_def)
            for rule_def in config.get("rules", [])
            if (rule := self._create_config_rule(config.get("name", "unnamed"), rule_def)) is not None
        ]

    def _create_config_rule(self, entity_name: str, rule_def: Dict) -> Optional[ConfigRule]:
        """创建配置规则实例"""
        try:
            sources = self._create_sources(rule_def.get("sources", []))
            attrs = self._create_attributes(rule_def.get("attributes", []))
            
            if entity_name != "default" and not sources:
                logger.warning("Invalid rule configuration",
                             extra={"entity": entity_name, "reason": "no_valid_sources"})
                return None
                
            return ConfigRule(
                entity_type_name=entity_name,
                source_matchers=sources,
                attributes=attrs
            )
        except Exception as e:
            logger.error("Rule creation failed",
                       extra={"entity": entity_name, "error": str(e)},
                       exc_info=True)
            return None

    def _create_sources(self, sources_def: List[Dict]) -> List[SourceMatcher]:
        """创建源匹配器集合"""
        valid_sources = []
        for src in sources_def:
            try:
                matcher = SourceMatcher(src["source"], src["condition"])
                if matcher.valid:
                    valid_sources.append(matcher)
            except KeyError:
                logger.warning("Invalid source definition",
                             extra={"definition": str(src)[:200]})
        return valid_sources

    def _create_attributes(self, attrs_def: List[Dict]) -> List[Attribute]:
        """创建属性映射集合"""
        valid_attrs = []
        for attr in attrs_def:
            try:
                valid_attrs.append(Attribute(attr["key"], attr["pattern"]))
            except KeyError:
                logger.warning("Invalid attribute definition",
                             extra={"definition": str(attr)[:200]})
        return valid_attrs