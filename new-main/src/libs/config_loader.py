from pathlib import Path
from typing import Dict, Any
import yaml

from libs.exceptions import ConfigValidationError



class ConfigLoader:
    def __init__(self, config_path: Path = None):
        self.config_path = config_path or Path(__file__).parent.parent.parent / "config" / "config.yml"
        self._config = None
        
    def load(self) -> Dict[str, Any]:
        """加载并验证配置文件"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                self._validate_structure(config)
                self._validate_values(config)
                self._config = config
                return config
        except FileNotFoundError:
            raise ConfigValidationError(f"Config file not found at {self.config_path}")
        except yaml.YAMLError as e:
            raise ConfigValidationError(f"YAML parsing error: {str(e)}")

    def _validate_structure(self, config: Dict) -> None:
        """验证配置结构"""
        required_sections = {'event_hubs'}
        missing = required_sections - config.keys()
        if missing:
            raise ConfigValidationError(f"Missing required sections: {', '.join(missing)}")

        for hub in config.get('event_hubs', []):
            required_fields = {'name', 'connection_str', 'consumer_group', 'partitions'}
            missing = required_fields - hub.keys()
            if missing:
                raise ConfigValidationError(f"Event hub {hub.get('name')} missing fields: {', '.join(missing)}")

    def _validate_values(self, config: Dict) -> None:
        """验证配置值有效性"""
        if not isinstance(config['event_hubs'], list):
            raise ConfigValidationError("event_hubs must be a list")
            
        for hub in config['event_hubs']:
            if hub['partitions'] <= 0:
                raise ConfigValidationError(f"Invalid partitions number for {hub['name']}")
            if 'local_checkpoint_dir' not in hub:
                raise ConfigValidationError(f"Missing local_checkpoint_dir for {hub['name']}")
            if not isinstance(hub['local_checkpoint_dir'], str):
                raise ConfigValidationError("local_checkpoint_dir must be string")