"""
一键搭建千万级数据仓库
自动执行所有初始化步骤
"""
import pymysql
import subprocess
import sys
import os
import json
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def print_step(step, message):
    """打印步骤信息"""
    print(f"\n{'='*60}")
    print(f"步骤 {step}: {message}")
    print(f"{'='*60}")

def get_db_config():
    """获取数据库配置"""
    print("\n请输入数据库配置:")
    
    host = input("数据库地址 [localhost]: ").strip() or 'localhost'
    port = input("端口 [3306]: ").strip() or '3306'
    user = input("用户名 [root]: ").strip() or 'root'
    password = input("密码: ").strip()
    database = input("数据库名 [datas]: ").strip() or 'datas'
    
    return {
        'host': host,
        'port': int(port),
        'user': user,
        'password': password,
        'database': database
    }

def test_connection(db_config):
    """测试数据库连接"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            charset='utf8mb4'
        )
        conn.close()
        print("✓ 数据库连接成功")
        return True
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        return False

def create_database(db_config):
    """创建数据库"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✓ 数据库 {db_config['database']} 已创建")
        return True
    except Exception as e:
        print(f"✗ 创建数据库失败: {e}")
        return False

def execute_sql_file(db_config, sql_file):
    """执行 SQL 文件"""
    try:
        sql_path = os.path.join(BASE_DIR, sql_file)
        
        if not os.path.exists(sql_path):
            print(f"✗ SQL 文件不存在: {sql_path}")
            return False
        
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割并执行 SQL 语句
        statements = sql_content.split(';')
        for statement in statements:
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    # 忽略某些错误（如表已存在）
                    if 'already exists' not in str(e).lower():
                        print(f"  警告: {str(e)[:100]}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ SQL 文件执行成功: {sql_file}")
        return True
        
    except Exception as e:
        print(f"✗ 执行 SQL 文件失败: {e}")
        return False

def apply_optimizations(db_config):
    """应用性能优化配置"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        optimizations = [
            ("SET GLOBAL local_infile = 1", "启用本地文件加载"),
            ("SET GLOBAL innodb_file_per_table = 1", "启用独立表空间"),
            ("SET GLOBAL slow_query_log = 1", "启用慢查询日志"),
            ("SET GLOBAL long_query_time = 2", "设置慢查询阈值"),
        ]
        
        for sql, desc in optimizations:
            try:
                cursor.execute(sql)
                print(f"  ✓ {desc}")
            except Exception as e:
                print(f"  ⚠ {desc} - {str(e)[:50]}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✓ 性能优化配置已应用")
        return True
        
    except Exception as e:
        print(f"✗ 应用优化配置失败: {e}")
        return False

def run_python_script(script_name, config):
    """运行 Python 脚本"""
    try:
        script_path = os.path.join(BASE_DIR, 'scripts', script_name)
        
        if not os.path.exists(script_path):
            print(f"✗ 脚本不存在: {script_path}")
            return False
        
        config_json = json.dumps(config)
        
        result = subprocess.run(
            [sys.executable, script_path, config_json],
            capture_output=True,
            text=True,
            cwd=BASE_DIR
        )
        
        print(result.stdout)
        
        if result.returncode == 0:
            print(f"✓ 脚本执行成功: {script_name}")
            return True
        else:
            print(f"✗ 脚本执行失败: {script_name}")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"✗ 运行脚本失败: {e}")
        return False

def main():
    """主函数"""
    print("="*60)
    print("千万级数据仓库一键搭建工具")
    print("="*60)
    
    # 步骤1: 获取配置
    print_step(1, "配置数据库连接")
    db_config = get_db_config()
    
    if not test_connection(db_config):
        print("\n✗ 数据库连接失败，请检查配置")
        return
    
    # 步骤2: 创建数据库
    print_step(2, "创建数据库")
    if not create_database(db_config):
        return
    
    # 步骤3: 创建表结构
    print_step(3, "创建优化后的表结构")
    
    choice = input("\n选择建表脚本:\n1. 优化版（推荐，支持分区）\n2. 标准版\n请选择 [1]: ").strip() or '1'
    
    if choice == '1':
        sql_file = 'sql/create_tables_optimized.sql'
    else:
        sql_file = 'sql/create_tables_refactored.sql'
    
    if not execute_sql_file(db_config, sql_file):
        print("\n⚠ 建表失败，但可以继续")
    
    # 步骤4: 应用性能优化
    print_step(4, "应用性能优化配置")
    apply_optimizations(db_config)
    
    # 步骤5: 生成测试数据
    print_step(5, "生成测试数据")
    
    generate = input("\n是否生成测试数据? (y/n) [y]: ").strip().lower() or 'y'
    
    if generate == 'y':
        order_count = input("订单数量 [50000]: ").strip() or '50000'
        print(f"\n正在生成 {order_count} 条订单数据...")
        print("提示: 可以在界面中配置更多参数")
        print("执行: node main.js")
    
    # 步骤6: 加载数据
    print_step(6, "加载数据到数据库")
    
    load = input("\n是否加载 ODS 层数据? (y/n) [y]: ").strip().lower() or 'y'
    
    if load == 'y':
        config = {
            'dbConfig': db_config,
            'layer': 'ods',
            'mode': 'full'
        }
        run_python_script('load_to_database.py', config)
    
    # 步骤7: 数据转换
    print_step(7, "数据转换（DIM → DWD → DWS）")
    
    transform = input("\n是否执行数据转换? (y/n) [y]: ").strip().lower() or 'y'
    
    if transform == 'y':
        config = {
            'dbConfig': db_config,
            'mode': 'full'
        }
        
        print("\n转换 DIM 层...")
        run_python_script('transform_dim.py', config)
        
        print("\n转换 DWD 层...")
        run_python_script('transform_dwd.py', config)
        
        print("\n转换 DWS 层...")
        run_python_script('transform_dws.py', config)
    
    # 步骤8: 添加分区
    print_step(8, "分区管理")
    
    partition = input("\n是否添加未来分区? (y/n) [y]: ").strip().lower() or 'y'
    
    if partition == 'y':
        config = {
            'dbConfig': db_config,
            'action': 'add',
            'months': 6
        }
        run_python_script('partition_manager.py', config)
    
    # 步骤9: 性能监控
    print_step(9, "性能监控")
    
    monitor = input("\n是否查看性能监控? (y/n) [y]: ").strip().lower() or 'y'
    
    if monitor == 'y':
        config = {'dbConfig': db_config}
        run_python_script('performance_monitor.py', config)
    
    # 完成
    print("\n" + "="*60)
    print("✓ 数据仓库搭建完成！")
    print("="*60)
    
    print("\n后续操作:")
    print("1. 查看文档: docs/千万级数仓搭建指南.md")
    print("2. 性能监控: python scripts/performance_monitor.py")
    print("3. 分区管理: python scripts/partition_manager.py")
    print("4. 启动界面: node main.js")
    
    print("\n数据库信息:")
    print(f"  地址: {db_config['host']}:{db_config['port']}")
    print(f"  数据库: {db_config['database']}")
    print(f"  用户: {db_config['user']}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户取消操作")
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
