"""
执行SQL文件
"""
import pymysql
import sys
import json
import os

def get_db_connection(db_config):
    """获取数据库连接"""
    try:
        return pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def execute_sql_file(conn, sql_file):
    """执行SQL文件"""
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        cursor = conn.cursor()
        
        # 分割SQL语句
        statements = sql_content.split(';')
        
        for statement in statements:
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    cursor.execute(statement)
                    print(f"✓ 执行成功")
                except Exception as e:
                    # 忽略某些错误
                    if 'already exists' not in str(e).lower():
                        print(f"⚠ 警告: {str(e)[:100]}")
        
        conn.commit()
        cursor.close()
        
        print("\n[完成] SQL文件执行完成")
        return True
        
    except Exception as e:
        print(f"[错误] 执行失败: {e}")
        return False

def main():
    """主函数"""
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    db_config = config.get('dbConfig', {
        'host': 'localhost',
        'port': 3306,
        'database': 'datas',
        'user': 'root',
        'password': ''
    })
    
    sql_file = config.get('sqlFile')
    
    if not sql_file or not os.path.exists(sql_file):
        print(f"[错误] SQL文件不存在: {sql_file}")
        sys.exit(1)
    
    print("="*60)
    print("执行SQL文件")
    print("="*60)
    print(f"文件: {sql_file}")
    print(f"数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print("="*60)
    
    conn = get_db_connection(db_config)
    if not conn:
        sys.exit(1)
    
    try:
        success = execute_sql_file(conn, sql_file)
        sys.exit(0 if success else 1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
