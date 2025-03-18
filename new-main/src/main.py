from pathlib import Path
import sys
from libs.config_loader import ConfigLoader, ConfigValidationError
from managers.process_manager import EventHubManager
from system.controller import ApplicationController


def main():
    try:
        # 初始化配置
        config_loader = ConfigLoader()
        config = config_loader.load()
        
        # 2. 初始化处理器和管理器
        managers = []
        for hub_config in config['event_hubs']:
            # 为每个 Event Hub 创建独立的处理器
            # processor = CustomProcessor(config)  # 🎯 初始化处理器
            
            # 创建管理器并关联处理器
            manager = EventHubManager(hub_config)  # 修改构造函数以接收处理器
            managers.append(manager)
        
        # 启动所有处理器
        for manager in managers:
            manager.start()
            
        # 启动控制循环
        controller = ApplicationController(managers)
        controller.run()
        
    except ConfigValidationError as e:
        print(f"Configuration error: {str(e)}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Critical failure: {str(e)}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
 
    main()