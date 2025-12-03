"""
分区管理工具 - 自动添加/删除分区
支持千万级数据的生命周期管理
"""
import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys
import json

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

def add_future_partitions(conn, months_ahead=6):
    """
    为分区表添加未来N个月的分区
    """
    print(f"\n添加未来 {months_ahead} 个月的分区...")
    
    # 需要分区的表
    tables = ['ods_orders', 'ods_order_details', 'dwd_fact_order', 'dwd_fact_order_detail']
    
    cursor = conn.cursor()
    
    for table in tables:
        try:
            # 获取当前最大分区
            cursor.execute(f"""
                SELECT PARTITION_NAME, PARTITION_DESCRIPTION
                FROM information_schema.PARTITIONS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '{table}'
                  AND PARTITION_NAME != 'p_future'
                ORDER BY PARTITION_ORDINAL_POSITION DESC
                LIMIT 1
            """)
            
            result = cursor.fetchone()
            if not result:
                print(f"  {table}: 未找到分区信息")
                continue
            
            last_partition = result[0]
            last_value = int(result[1])
            
            # 解析最后分区的年月
            if last_partition.startswith('p'):
                year_month = int(last_partition[1:])
                year = year_month // 100
                month = year_month % 100
            else:
                continue
            
            # 生成未来分区
            current_date = datetime(year, month, 1)
            partitions_to_add = []
            
            for i in range(1, months_ahead + 1):
                next_date = current_date + relativedelta(months=i)
                partition_name = f"p{next_date.year}{next_date.month:02d}"
                
                # 计算下个月的值
                next_month_date = next_date + relativedelta(months=1)
                partition_value = next_month_date.year * 100 + next_month_date.month
                
                # 检查分区是否已存在
                cursor.execute(f"""
                    SELECT COUNT(*) FROM information_schema.PARTITIONS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = '{table}'
                      AND PARTITION_NAME = '{partition_name}'
                """)
                
                if cursor.fetchone()[0] == 0:
                    partitions_to_add.append((partition_name, partition_value))
            
            # 添加分区
            if partitions_to_add:
                # 先删除 p_future 分区
                cursor.execute(f"ALTER TABLE {table} DROP PARTITION p_future")
                
                # 添加新分区
                for partition_name, partition_value in partitions_to_add:
                    cursor.execute(f"""
                        ALTER TABLE {table} ADD PARTITION (
                            PARTITION {partition_name} VALUES LESS THAN ({partition_value})
                        )
                    """)
                    print(f"  ✓ {table}: 添加分区 {partition_name} (< {partition_value})")
                
                # 重新添加 p_future
                cursor.execute(f"""
                    ALTER TABLE {table} ADD PARTITION (
                        PARTITION p_future VALUES LESS THAN MAXVALUE
                    )
                """)
                
                conn.commit()
            else:
                print(f"  {table}: 分区已是最新")
                
        except Exception as e:
            print(f"  ✗ {table}: 添加分区失败 - {e}")
            conn.rollback()
    
    cursor.close()

def archive_old_partitions(conn, months_to_keep=12):
    """
    归档旧分区（删除N个月之前的数据）
    """
    print(f"\n归档 {months_to_keep} 个月之前的分区...")
    
    tables = ['ods_orders', 'ods_order_details', 'dwd_fact_order', 'dwd_fact_order_detail']
    
    cursor = conn.cursor()
    cutoff_date = datetime.now() - relativedelta(months=months_to_keep)
    cutoff_value = cutoff_date.year * 100 + cutoff_date.month
    
    for table in tables:
        try:
            # 获取需要归档的分区
            cursor.execute(f"""
                SELECT PARTITION_NAME, PARTITION_DESCRIPTION
                FROM information_schema.PARTITIONS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '{table}'
                  AND PARTITION_NAME LIKE 'p20%'
                  AND CAST(PARTITION_DESCRIPTION AS UNSIGNED) < {cutoff_value}
                ORDER BY PARTITION_ORDINAL_POSITION
            """)
            
            partitions_to_drop = cursor.fetchall()
            
            if partitions_to_drop:
                for partition_name, partition_value in partitions_to_drop:
                    # 可选：先备份数据到归档表
                    # archive_table = f"{table}_archive"
                    # cursor.execute(f"INSERT INTO {archive_table} SELECT * FROM {table} PARTITION ({partition_name})")
                    
                    # 删除分区
                    cursor.execute(f"ALTER TABLE {table} DROP PARTITION {partition_name}")
                    print(f"  ✓ {table}: 删除分区 {partition_name} (< {partition_value})")
                
                conn.commit()
            else:
                print(f"  {table}: 无需归档")
                
        except Exception as e:
            print(f"  ✗ {table}: 归档失败 - {e}")
            conn.rollback()
    
    cursor.close()

def optimize_tables(conn):
    """
    优化表（重建索引、回收空间）
    """
    print("\n优化表...")
    
    cursor = conn.cursor()
    
    # 获取所有表
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        try:
            print(f"  优化 {table}...")
            cursor.execute(f"OPTIMIZE TABLE {table}")
            conn.commit()
            print(f"  ✓ {table} 优化完成")
        except Exception as e:
            print(f"  ✗ {table} 优化失败: {e}")
    
    cursor.close()

def analyze_tables(conn):
    """
    分析表统计信息（更新索引统计）
    """
    print("\n分析表统计信息...")
    
    cursor = conn.cursor()
    
    # 重点分析大表
    important_tables = [
        'ods_orders', 'ods_order_details',
        'dwd_fact_order', 'dwd_fact_order_detail',
        'dws_trade_order_1d', 'dws_trade_product_1d'
    ]
    
    for table in important_tables:
        try:
            cursor.execute(f"ANALYZE TABLE {table}")
            result = cursor.fetchone()
            print(f"  ✓ {table}: {result[3]}")
        except Exception as e:
            print(f"  ✗ {table}: {e}")
    
    cursor.close()

def show_partition_info(conn):
    """
    显示分区信息
    """
    print("\n分区信息统计:")
    print("="*80)
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            TABLE_NAME,
            PARTITION_NAME,
            PARTITION_ORDINAL_POSITION,
            TABLE_ROWS,
            ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
            ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = DATABASE()
          AND PARTITION_NAME IS NOT NULL
        ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION
    """)
    
    results = cursor.fetchall()
    
    current_table = None
    for row in results:
        table_name, partition_name, pos, rows, data_mb, index_mb = row
        
        if table_name != current_table:
            if current_table:
                print()
            print(f"\n{table_name}:")
            current_table = table_name
        
        print(f"  {partition_name:15s} | 行数: {rows:>10,} | 数据: {data_mb:>8.2f}MB | 索引: {index_mb:>8.2f}MB")
    
    cursor.close()
    print("="*80)

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
    
    action = config.get('action', 'info')
    
    print("="*60)
    print("分区管理工具")
    print("="*60)
    
    conn = get_db_connection(db_config)
    if not conn:
        return
    
    try:
        if action == 'add':
            # 添加未来分区
            months = config.get('months', 6)
            add_future_partitions(conn, months)
        
        elif action == 'archive':
            # 归档旧分区
            months_to_keep = config.get('monthsToKeep', 12)
            archive_old_partitions(conn, months_to_keep)
        
        elif action == 'optimize':
            # 优化表
            optimize_tables(conn)
        
        elif action == 'analyze':
            # 分析表
            analyze_tables(conn)
        
        elif action == 'info':
            # 显示分区信息
            show_partition_info(conn)
        
        else:
            print(f"未知操作: {action}")
            print("支持的操作: add, archive, optimize, analyze, info")
    
    finally:
        conn.close()
    
    print("\n✓ 完成")

if __name__ == '__main__':
    main()
