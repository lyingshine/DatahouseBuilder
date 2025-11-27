"""
清空数据工具
支持清空本地CSV文件和数据库表
"""
import os
import sys
import json
import pymysql
import shutil

# 获取项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def clear_local_data():
    """清空本地CSV文件"""
    print("\n清空本地数据...")
    
    layers = ['ods', 'dwd', 'dws']
    total_deleted = 0
    
    for layer in layers:
        layer_path = os.path.join(DATA_DIR, layer)
        if os.path.exists(layer_path):
            csv_files = [f for f in os.listdir(layer_path) if f.endswith('.csv')]
            for csv_file in csv_files:
                file_path = os.path.join(layer_path, csv_file)
                try:
                    os.remove(file_path)
                    print(f"  ✓ 删除文件: {layer}/{csv_file}")
                    total_deleted += 1
                except Exception as e:
                    print(f"  ✗ 删除失败: {layer}/{csv_file} - {e}")
    
    print(f"\n本地数据清空完成，共删除 {total_deleted} 个文件")
    return total_deleted > 0


def clear_database_tables(db_config):
    """清空数据库表"""
    print("\n清空数据库表...")
    
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        # 获取所有表
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        if not tables:
            print("  数据库中没有表")
            cursor.close()
            conn.close()
            return True
        
        # 临时禁用外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        total_deleted = 0
        for table in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"  ✓ 删除表: {table}")
                total_deleted += 1
            except Exception as e:
                print(f"  ✗ 删除失败: {table} - {e}")
        
        # 恢复外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n数据库清空完成，共删除 {total_deleted} 张表")
        return True
        
    except Exception as e:
        print(f"\n数据库清空失败: {e}")
        return False


def main():
    """主函数"""
    # 读取配置参数
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    # 获取数据库配置
    db_config = config.get('dbConfig', {
        'host': 'localhost',
        'port': 3306,
        'database': 'datas',
        'user': 'root',
        'password': ''
    })
    
    clear_type = config.get('clearType', 'all')  # all, local, database
    
    print("="*60)
    print("数据清空工具")
    print("="*60)
    print(f"清空类型: {clear_type}")
    print(f"数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print("="*60)
    
    success = True
    
    if clear_type in ['all', 'local']:
        success = clear_local_data() and success
    
    if clear_type in ['all', 'database']:
        success = clear_database_tables(db_config) and success
    
    print("\n" + "="*60)
    if success:
        print("✓ 数据清空完成！")
    else:
        print("✗ 数据清空失败")
    print("="*60)
    
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()
