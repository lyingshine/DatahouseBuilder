"""
MySQL 性能优化配置脚本
自动检测并优化 MySQL 配置
"""
import pymysql
import sys
import json


def test_and_optimize(db_config):
    """测试并优化 MySQL 配置"""
    print("="*60)
    print("MySQL 性能优化检测")
    print("="*60)
    
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config.get('database', 'mysql'),
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        print("\n1. 检测 local_infile 配置...")
        cursor.execute("SHOW VARIABLES LIKE 'local_infile'")
        result = cursor.fetchone()
        
        if result and result[1].lower() == 'on':
            print("  ✓ local_infile 已启用（极速导入模式）")
        else:
            print("  ✗ local_infile 未启用（使用批量插入模式）")
            print("\n  尝试启用 local_infile...")
            try:
                cursor.execute("SET GLOBAL local_infile = 1")
                conn.commit()
                print("  ✓ local_infile 已启用（需要 SUPER 权限）")
            except Exception as e:
                print(f"  ✗ 启用失败: {e}")
                print("\n  解决方案：")
                print("  1. 授予 SUPER 权限: GRANT SUPER ON *.* TO 'user'@'host';")
                print("  2. 或在 my.ini/my.cnf 中添加: local_infile=1")
                print("  3. 重启 MySQL 服务")
        
        print("\n2. 检测 binlog 配置...")
        try:
            cursor.execute("SHOW VARIABLES LIKE 'sql_log_bin'")
            result = cursor.fetchone()
            if result:
                if result[1].lower() == 'on':
                    print("  ℹ️ binlog 已启用（安全但稍慢）")
                    print("  提示：如不需要主从复制，可临时禁用以提升性能")
                else:
                    print("  ✓ binlog 已禁用（性能优化）")
        except:
            print("  ℹ️ 无法检测 binlog 状态")
        
        print("\n3. 检测刷盘策略...")
        cursor.execute("SHOW VARIABLES LIKE 'innodb_flush_log_at_trx_commit'")
        result = cursor.fetchone()
        if result:
            value = result[1]
            if value == '1':
                print(f"  ℹ️ 刷盘策略: {value} (最安全，默认)")
                print("  提示：可设置为 2 以提升性能（临时丢失风险低）")
            elif value == '2':
                print(f"  ✓ 刷盘策略: {value} (性能优化)")
            else:
                print(f"  ⚠️ 刷盘策略: {value} (最快但有风险)")
        
        print("\n4. 检测缓冲池大小...")
        cursor.execute("SHOW VARIABLES LIKE 'innodb_buffer_pool_size'")
        result = cursor.fetchone()
        if result:
            size_bytes = int(result[1])
            size_gb = size_bytes / (1024**3)
            print(f"  ℹ️ 缓冲池大小: {size_gb:.2f} GB")
            if size_gb < 1:
                print("  ⚠️ 缓冲池较小，建议设置为物理内存的 50-70%")
            else:
                print("  ✓ 缓冲池配置合理")
        
        print("\n5. 检测连接数配置...")
        cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
        result = cursor.fetchone()
        if result:
            max_conn = int(result[1])
            print(f"  ℹ️ 最大连接数: {max_conn}")
            if max_conn < 200:
                print("  ⚠️ 连接数较少，建议增加到 500+")
            else:
                print("  ✓ 连接数配置合理")
        
        print("\n6. 检测数据包大小...")
        cursor.execute("SHOW VARIABLES LIKE 'max_allowed_packet'")
        result = cursor.fetchone()
        if result:
            size_bytes = int(result[1])
            size_mb = size_bytes / (1024**2)
            print(f"  ℹ️ 最大数据包: {size_mb:.0f} MB")
            if size_mb < 64:
                print("  ⚠️ 数据包较小，建议增加到 256MB")
            else:
                print("  ✓ 数据包配置合理")
        
        print("\n" + "="*60)
        print("检测完成")
        print("="*60)
        
        # 生成优化建议
        print("\n📋 优化建议：")
        print("\n在 my.ini (Windows) 或 my.cnf (Linux/Mac) 中添加：")
        print("""
[mysqld]
local_infile = 1
innodb_buffer_pool_size = 4G
innodb_flush_log_at_trx_commit = 2
max_connections = 500
max_allowed_packet = 256M

[mysql]
local_infile = 1
""")
        print("\n修改后重启 MySQL 服务生效")
        print("\n详细说明请查看：MySQL性能优化配置.md")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n✗ 连接失败: {e}")
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
        'user': 'root',
        'password': ''
    })
    
    print(f"数据库: {db_config['host']}:{db_config['port']}")
    print(f"用户: {db_config['user']}")
    print()
    
    success = test_and_optimize(db_config)
    
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()
