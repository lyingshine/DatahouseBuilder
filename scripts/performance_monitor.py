"""
数据仓库性能监控工具
监控查询性能、表大小、索引使用情况
"""
import pymysql
import sys
import json
from datetime import datetime

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

def show_table_sizes(conn):
    """显示表大小统计"""
    print("\n" + "="*80)
    print("表大小统计")
    print("="*80)
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            TABLE_NAME,
            TABLE_ROWS,
            ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
            ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb,
            ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS total_mb,
            ENGINE,
            ROW_FORMAT
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC
        LIMIT 20
    """)
    
    results = cursor.fetchall()
    
    print(f"{'表名':<30} {'行数':>12} {'数据(MB)':>12} {'索引(MB)':>12} {'总计(MB)':>12} {'引擎':<10} {'格式':<12}")
    print("-"*110)
    
    total_data = 0
    total_index = 0
    
    for row in results:
        table_name, rows, data_mb, index_mb, total_mb, engine, row_format = row
        print(f"{table_name:<30} {rows:>12,} {data_mb:>12.2f} {index_mb:>12.2f} {total_mb:>12.2f} {engine:<10} {row_format:<12}")
        total_data += data_mb or 0
        total_index += index_mb or 0
    
    print("-"*110)
    print(f"{'总计':<30} {'':<12} {total_data:>12.2f} {total_index:>12.2f} {total_data + total_index:>12.2f}")
    
    cursor.close()

def show_index_usage(conn):
    """显示索引使用情况"""
    print("\n" + "="*80)
    print("索引使用统计（未使用的索引）")
    print("="*80)
    
    cursor = conn.cursor()
    
    # 检查是否有 sys schema
    cursor.execute("SHOW DATABASES LIKE 'sys'")
    if not cursor.fetchone():
        print("  sys schema 不存在，跳过索引分析")
        cursor.close()
        return
    
    try:
        cursor.execute("""
            SELECT 
                object_schema,
                object_name,
                index_name
            FROM sys.schema_unused_indexes
            WHERE object_schema = DATABASE()
            ORDER BY object_name, index_name
        """)
        
        results = cursor.fetchall()
        
        if results:
            print(f"{'数据库':<20} {'表名':<30} {'索引名':<30}")
            print("-"*80)
            for row in results:
                print(f"{row[0]:<20} {row[1]:<30} {row[2]:<30}")
        else:
            print("  ✓ 所有索引都在使用中")
    except Exception as e:
        print(f"  无法查询索引使用情况: {e}")
    
    cursor.close()

def show_slow_queries(conn):
    """显示慢查询统计"""
    print("\n" + "="*80)
    print("慢查询统计（TOP 10）")
    print("="*80)
    
    cursor = conn.cursor()
    
    # 检查慢查询日志是否开启
    cursor.execute("SHOW VARIABLES LIKE 'slow_query_log'")
    result = cursor.fetchone()
    
    if not result or result[1] != 'ON':
        print("  慢查询日志未开启")
        print("  执行以下命令开启: SET GLOBAL slow_query_log = 1;")
        cursor.close()
        return
    
    # 使用 performance_schema 查询慢查询
    try:
        cursor.execute("""
            SELECT 
                DIGEST_TEXT,
                COUNT_STAR AS exec_count,
                ROUND(AVG_TIMER_WAIT / 1000000000000, 2) AS avg_time_sec,
                ROUND(MAX_TIMER_WAIT / 1000000000000, 2) AS max_time_sec,
                ROUND(SUM_TIMER_WAIT / 1000000000000, 2) AS total_time_sec
            FROM performance_schema.events_statements_summary_by_digest
            WHERE SCHEMA_NAME = DATABASE()
            ORDER BY AVG_TIMER_WAIT DESC
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        
        if results:
            for i, row in enumerate(results, 1):
                digest, count, avg_time, max_time, total_time = row
                print(f"\n{i}. 执行次数: {count}, 平均: {avg_time}s, 最大: {max_time}s, 总计: {total_time}s")
                print(f"   SQL: {digest[:100]}...")
        else:
            print("  暂无慢查询记录")
    except Exception as e:
        print(f"  无法查询慢查询: {e}")
    
    cursor.close()

def show_connection_info(conn):
    """显示连接信息"""
    print("\n" + "="*80)
    print("连接信息")
    print("="*80)
    
    cursor = conn.cursor()
    
    # 当前连接数
    cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
    threads_connected = cursor.fetchone()[1]
    
    # 最大连接数
    cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
    max_connections = cursor.fetchone()[1]
    
    # 历史最大连接数
    cursor.execute("SHOW STATUS LIKE 'Max_used_connections'")
    max_used = cursor.fetchone()[1]
    
    print(f"  当前连接数: {threads_connected}")
    print(f"  最大连接数: {max_connections}")
    print(f"  历史峰值: {max_used}")
    print(f"  连接使用率: {int(threads_connected) / int(max_connections) * 100:.1f}%")
    
    cursor.close()

def show_buffer_pool_info(conn):
    """显示 InnoDB 缓冲池信息"""
    print("\n" + "="*80)
    print("InnoDB 缓冲池")
    print("="*80)
    
    cursor = conn.cursor()
    
    # 缓冲池大小
    cursor.execute("SHOW VARIABLES LIKE 'innodb_buffer_pool_size'")
    pool_size = int(cursor.fetchone()[1])
    
    # 缓冲池使用情况
    cursor.execute("""
        SELECT 
            ROUND(DATA_SIZE / 1024 / 1024 / 1024, 2) AS data_gb,
            ROUND(FREE_BUFFERS * 16384 / 1024 / 1024 / 1024, 2) AS free_gb
        FROM information_schema.INNODB_BUFFER_POOL_STATS
    """)
    
    result = cursor.fetchone()
    if result:
        data_gb, free_gb = result
        total_gb = pool_size / 1024 / 1024 / 1024
        used_gb = total_gb - free_gb
        
        print(f"  缓冲池大小: {total_gb:.2f} GB")
        print(f"  已使用: {used_gb:.2f} GB ({used_gb / total_gb * 100:.1f}%)")
        print(f"  空闲: {free_gb:.2f} GB")
    
    # 缓冲池命中率
    cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_read_requests'")
    read_requests = int(cursor.fetchone()[1])
    
    cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_reads'")
    disk_reads = int(cursor.fetchone()[1])
    
    if read_requests > 0:
        hit_rate = (1 - disk_reads / read_requests) * 100
        print(f"  缓冲池命中率: {hit_rate:.2f}%")
    
    cursor.close()

def show_query_cache_info(conn):
    """显示查询缓存信息（MySQL 5.7及以下）"""
    print("\n" + "="*80)
    print("查询缓存")
    print("="*80)
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("SHOW VARIABLES LIKE 'query_cache_type'")
        result = cursor.fetchone()
        
        if not result or result[1] == 'OFF':
            print("  查询缓存未开启（MySQL 8.0+ 已移除此功能）")
        else:
            cursor.execute("SHOW STATUS LIKE 'Qcache%'")
            results = cursor.fetchall()
            
            for row in results:
                print(f"  {row[0]}: {row[1]}")
    except:
        print("  查询缓存不可用（MySQL 8.0+ 已移除）")
    
    cursor.close()

def show_data_statistics(conn):
    """显示数据统计"""
    print("\n" + "="*80)
    print("数据统计")
    print("="*80)
    
    cursor = conn.cursor()
    
    # ODS层统计
    print("\nODS层:")
    tables = ['ods_orders', 'ods_order_details', 'ods_users', 'ods_products', 'ods_stores']
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table:<25} {count:>12,} 行")
        except:
            pass
    
    # DWD层统计
    print("\nDWD层:")
    tables = ['dwd_fact_order', 'dwd_fact_order_detail']
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table:<25} {count:>12,} 行")
        except:
            pass
    
    # DWS层统计
    print("\nDWS层:")
    tables = ['dws_trade_order_1d', 'dws_trade_product_1d', 'dws_user_total']
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table:<25} {count:>12,} 行")
        except:
            pass
    
    cursor.close()

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
    
    print("="*80)
    print(f"数据仓库性能监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    conn = get_db_connection(db_config)
    if not conn:
        return
    
    try:
        show_connection_info(conn)
        show_buffer_pool_info(conn)
        show_query_cache_info(conn)
        show_data_statistics(conn)
        show_table_sizes(conn)
        show_index_usage(conn)
        show_slow_queries(conn)
        
        print("\n" + "="*80)
        print("监控完成")
        print("="*80)
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()
