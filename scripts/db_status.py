"""
数据库状态检测工具
"""
import pymysql
import sys
import json


def check_db_status(db_config):
    """检查数据库状态"""
    conn = None
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4',
            connect_timeout=5,
            read_timeout=10
        )
        cursor = conn.cursor()
        
        # 连接状态
        print("【连接状态】✓ 已连接")
        
        # 当前进程
        cursor.execute("""
            SELECT id, time, LEFT(info, 60) as query
            FROM information_schema.processlist 
            WHERE db = %s AND command = 'Query' AND time > 1
            ORDER BY time DESC LIMIT 5
        """, (db_config['database'],))
        queries = cursor.fetchall()
        
        if queries:
            print(f"\n【慢查询】⚠️ {len(queries)} 个")
            for q in queries:
                print(f"  ID:{q[0]} 耗时:{q[1]}秒")
        else:
            print("\n【慢查询】✓ 无")
        
        # 锁状态
        lock_waits = 0
        try:
            cursor.execute("SELECT COUNT(*) FROM performance_schema.data_lock_waits")
            lock_waits = cursor.fetchone()[0]
        except:
            pass
        
        if lock_waits > 0:
            print(f"\n【锁等待】⚠️ {lock_waits} 个")
        else:
            print("\n【锁等待】✓ 无")
        
        # InnoDB状态
        cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool%'")
        status = dict(cursor.fetchall())
        pool_size = int(status.get('Innodb_buffer_pool_pages_total', 0))
        pool_free = int(status.get('Innodb_buffer_pool_pages_free', 0))
        pool_dirty = int(status.get('Innodb_buffer_pool_pages_dirty', 0))
        
        if pool_size > 0:
            used_pct = (pool_size - pool_free) / pool_size * 100
            dirty_pct = pool_dirty / pool_size * 100
            print(f"\n【缓冲池】使用率:{used_pct:.0f}% 脏页:{dirty_pct:.0f}%")
        
        # 表统计
        cursor.execute("""
            SELECT COUNT(*), SUM(table_rows), ROUND(SUM(data_length)/1024/1024, 1)
            FROM information_schema.tables WHERE table_schema = %s
        """, (db_config['database'],))
        row = cursor.fetchone()
        if row[0]:
            print(f"\n【数据量】{row[0]}张表, ~{int(row[1] or 0):,}行, {row[2] or 0}MB")
        
        # 结论
        if lock_waits > 0:
            print("\n【结论】❌ 存在锁等待")
        elif queries:
            print("\n【结论】⏳ 有查询执行中")
        else:
            print("\n【结论】✓ 状态正常")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"【连接状态】✗ 失败: {e}")
        return False
    finally:
        if conn:
            conn.close()


def kill_all_queries(db_config):
    """停止所有查询"""
    conn = None
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4',
            connect_timeout=5
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT CONNECTION_ID()")
        my_id = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT id, time FROM information_schema.processlist 
            WHERE db = %s AND id != %s AND command != 'Sleep'
        """, (db_config['database'], my_id))
        processes = cursor.fetchall()
        
        if not processes:
            print("✓ 没有活跃查询")
            return True
        
        killed = 0
        for proc in processes:
            try:
                cursor.execute(f"KILL {proc[0]}")
                killed += 1
            except:
                pass
        
        cursor.execute("UNLOCK TABLES")
        print(f"✓ 已终止 {killed} 个查询")
        
        cursor.close()
        return True
    except Exception as e:
        print(f"✗ 失败: {e}")
        return False
    finally:
        if conn:
            conn.close()


def main():
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
    
    action = config.get('action', 'status')
    
    if action == 'kill':
        kill_all_queries(db_config)
    else:
        check_db_status(db_config)


if __name__ == '__main__':
    main()
