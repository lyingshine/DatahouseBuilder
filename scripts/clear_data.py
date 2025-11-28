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
    print("\n[进度] 开始清空本地数据...")
    sys.stdout.flush()
    
    layers = ['ods', 'dwd', 'dws']
    total_deleted = 0
    
    # 先统计总文件数
    total_files = 0
    for layer in layers:
        layer_path = os.path.join(DATA_DIR, layer)
        if os.path.exists(layer_path):
            csv_files = [f for f in os.listdir(layer_path) if f.endswith('.csv')]
            total_files += len(csv_files)
    
    if total_files == 0:
        print("[进度] 没有找到CSV文件")
        sys.stdout.flush()
        return True
    
    print(f"[进度] 找到 {total_files} 个CSV文件")
    sys.stdout.flush()
    
    # 删除文件
    for layer in layers:
        layer_path = os.path.join(DATA_DIR, layer)
        if os.path.exists(layer_path):
            csv_files = [f for f in os.listdir(layer_path) if f.endswith('.csv')]
            for csv_file in csv_files:
                file_path = os.path.join(layer_path, csv_file)
                try:
                    os.remove(file_path)
                    total_deleted += 1
                    progress = int((total_deleted / total_files) * 100)
                    print(f"[进度] ({total_deleted}/{total_files}) {progress}% - 删除文件: {layer}/{csv_file}")
                    sys.stdout.flush()
                except Exception as e:
                    print(f"[错误] 删除失败: {layer}/{csv_file} - {e}")
                    sys.stdout.flush()
    
    print(f"[完成] 本地数据清空完成，共删除 {total_deleted} 个文件")
    sys.stdout.flush()
    return True


def drop_and_recreate_database(db_config):
    """删除并重建整个数据库（最快方式）"""
    print("\n[进度] 开始删除并重建数据库（极速模式）...")
    sys.stdout.flush()
    
    try:
        database_name = db_config['database']
        
        # 连接到 MySQL（不指定数据库）
        print(f"[进度] 连接数据库服务器: {db_config['host']}:{db_config['port']}")
        sys.stdout.flush()
        
        conn = pymysql.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=str(db_config['password']),
            charset='utf8mb4',
            connect_timeout=30
        )
        cursor = conn.cursor()
        
        print("[进度] 连接成功 ✓")
        sys.stdout.flush()
        
        # 删除数据库
        print(f"[进度] 删除数据库: {database_name}")
        sys.stdout.flush()
        cursor.execute(f"DROP DATABASE IF EXISTS `{database_name}`")
        
        # 重建数据库
        print(f"[进度] 重建数据库: {database_name}")
        sys.stdout.flush()
        cursor.execute(f"CREATE DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        
        cursor.close()
        conn.close()
        
        print(f"[完成] 数据库 {database_name} 已重建 ✓")
        sys.stdout.flush()
        return True
        
    except pymysql.Error as e:
        print(f"[错误] 数据库错误: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"[错误] 操作失败: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False


def clear_database_tables(db_config):
    """清空数据库表（仅删除程序生成的表）"""
    print("\n[进度] 开始清空数据库表...")
    sys.stdout.flush()
    
    try:
        # 连接数据库
        print(f"[进度] 连接数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        print(f"[进度] 使用用户: {db_config['user']}")
        sys.stdout.flush()
        
        conn = pymysql.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=str(db_config['password']),
            database=db_config['database'],
            charset='utf8mb4',
            connect_timeout=30,
            read_timeout=600,
            write_timeout=600
        )
        cursor = conn.cursor()
        
        print("[进度] 数据库连接成功 ✓")
        sys.stdout.flush()
        
        # 获取所有表
        print("[进度] 正在查询数据库表...")
        sys.stdout.flush()
        
        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]
        
        if not all_tables:
            print("[进度] 数据库中没有表")
            sys.stdout.flush()
            cursor.close()
            conn.close()
            return True
        
        # 只删除程序生成的表（ods_*, dwd_*, dws_*, dim_*）
        program_tables = [
            table for table in all_tables 
            if table.startswith('ods_') or 
               table.startswith('dwd_') or 
               table.startswith('dws_') or 
               table.startswith('dim_')
        ]
        
        if not program_tables:
            print("[进度] 没有找到程序生成的表")
            sys.stdout.flush()
            cursor.close()
            conn.close()
            return True
        
        print(f"[进度] 找到 {len(program_tables)} 张程序生成的表")
        sys.stdout.flush()
        
        # 临时禁用外键检查和自动提交
        print("[进度] 禁用外键检查...")
        sys.stdout.flush()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("SET autocommit = 1")
        
        total_deleted = 0
        total_tables = len(program_tables)
        
        # 批量删除，使用 TRUNCATE 清空大表后再 DROP
        for i, table in enumerate(program_tables):
            try:
                # 对于大表，先 TRUNCATE 再 DROP（更快）
                if table.startswith('dwd_') or table.startswith('ods_order'):
                    print(f"[进度] 清空大表: {table}（可能需要较长时间）...")
                    sys.stdout.flush()
                    try:
                        cursor.execute(f"TRUNCATE TABLE `{table}`")
                    except:
                        pass  # 如果 TRUNCATE 失败，继续尝试 DROP
                
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                total_deleted += 1
                
                # 每10%或每5个表显示一次进度
                if (i + 1) % max(1, total_tables // 10) == 0 or (i + 1) % 5 == 0:
                    progress = int((total_deleted / total_tables) * 100)
                    print(f"[进度] {progress}% ({total_deleted}/{total_tables})")
                    sys.stdout.flush()
            except Exception as e:
                print(f"[错误] 删除失败: {table} - {e}")
                sys.stdout.flush()
                # 继续删除其他表，不中断
        
        # 恢复外键检查
        print("[进度] 恢复外键检查...")
        sys.stdout.flush()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        cursor.close()
        conn.close()
        
        print(f"[完成] 数据库清空完成，共删除 {total_deleted} 张表")
        print(f"[信息] 保留了 {len(all_tables) - len(program_tables)} 张其他表")
        sys.stdout.flush()
        return True
        
    except pymysql.Error as e:
        print(f"[错误] 数据库错误: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"[错误] 清空失败: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
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
    
    clear_type = config.get('clearType', 'all')  # all, local, database, fast
    fast_mode = config.get('fastMode', False)  # 是否使用极速模式（DROP DATABASE）
    
    print("="*60)
    print("数据清空工具")
    print("="*60)
    print(f"[配置] 清空类型: {clear_type}")
    print(f"[配置] 极速模式: {'是（DROP DATABASE）' if fast_mode else '否（逐表删除）'}")
    print(f"[配置] 数据库地址: {db_config['host']}:{db_config['port']}")
    print(f"[配置] 数据库名: {db_config['database']}")
    print(f"[配置] 用户名: {db_config['user']}")
    print(f"[配置] 密码: {'*' * len(str(db_config.get('password', '')))}")
    print("="*60)
    sys.stdout.flush()
    
    success = True
    
    try:
        if clear_type in ['all', 'local']:
            local_success = clear_local_data()
            success = success and local_success
        
        if clear_type in ['all', 'database']:
            if fast_mode:
                # 极速模式：DROP DATABASE
                db_success = drop_and_recreate_database(db_config)
            else:
                # 普通模式：逐表删除
                db_success = clear_database_tables(db_config)
            success = success and db_success
        
        print("\n" + "="*60)
        if success:
            print("[完成] ✓ 数据清空完成！")
        else:
            print("[错误] ✗ 数据清空失败")
        print("="*60)
        sys.stdout.flush()
        
        if not success:
            sys.exit(1)
            
    except Exception as e:
        print(f"[错误] 执行失败: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
