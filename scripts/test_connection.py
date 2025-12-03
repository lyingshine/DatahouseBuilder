"""
测试数据库连接
"""
import sys
import json
import pymysql

def test_connection(db_config):
    """测试数据库连接"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            connect_timeout=3,
            charset='utf8mb4'
        )
        conn.close()
        return True
    except Exception as e:
        print(f"连接失败: {e}", file=sys.stderr)
        return False

if __name__ == '__main__':
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    if test_connection(config):
        sys.exit(0)
    else:
        sys.exit(1)
