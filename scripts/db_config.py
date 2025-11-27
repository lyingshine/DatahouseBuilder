"""
数据库配置读取模块
从项目根目录的"数据库信息"文件读取配置
"""
import os
import re

# 获取项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_INFO_FILE = os.path.join(BASE_DIR, '数据库信息')


def read_db_config():
    """读取数据库配置文件"""
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '132014',
        'database': 'datas',
        'charset': 'utf8mb4'
    }
    
    if not os.path.exists(DB_INFO_FILE):
        print(f"警告: 数据库信息文件不存在，使用默认配置")
        return config
    
    try:
        with open(DB_INFO_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 解析配置
        patterns = {
            'host': r'数据库地址[：:]\s*(.+)',
            'port': r'端口[：:]\s*(\d+)',
            'database': r'数据库名[：:]\s*(.+)',
            'user': r'用户名[：:]\s*(.+)',
            'password': r'密码[：:]\s*(.+)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, content)
            if match:
                value = match.group(1).strip()
                if key == 'port':
                    config[key] = int(value)
                else:
                    config[key] = value
        
        print(f"✓ 已从配置文件读取数据库信息: {config['host']}:{config['port']}/{config['database']}")
        
    except Exception as e:
        print(f"警告: 读取数据库信息文件失败 ({e})，使用默认配置")
    
    return config


# 导出配置
DB_CONFIG = read_db_config()
