"""
数据库连接管理器
统一管理数据库连接、优化配置、清理逻辑
"""
import pymysql
import sys
from datetime import datetime


class DatabaseManager:
    """数据库连接管理器"""
    
    def __init__(self, db_config):
        """
        初始化数据库管理器
        
        Args:
            db_config: 数据库配置字典
                {
                    'host': 'localhost',
                    'port': 3306,
                    'database': 'datas',
                    'user': 'root',
                    'password': ''
                }
        """
        self.config = db_config
        self.connection = None
        self._original_settings = {}
    
    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                charset='utf8mb4'
            )
            return self.connection
        except Exception as e:
            print(f"数据库连接失败: {e}")
            sys.stdout.flush()
            return None
    
    def optimize_for_performance(self, enable_global=True):
        """
        应用性能优化配置
        
        Args:
            enable_global: 是否尝试应用全局优化（需要SUPER权限）
        """
        if not self.connection:
            return False
        
        cursor = self.connection.cursor()
        
        # 全局优化配置（需要SUPER权限）
        if enable_global:
            print("  尝试应用极致性能优化...")
            sys.stdout.flush()
            
            global_optimizations = {
                'innodb_flush_log_at_trx_commit': '0',  # 0=最快（每秒刷盘）
                'sync_binlog': '0',  # 禁用binlog同步刷盘
                'innodb_doublewrite': '0',  # 禁用双写缓冲（SSD可禁用）
                'innodb_flush_neighbors': '0',  # 禁用邻页刷新（SSD优化）
                'innodb_io_capacity': '5000',  # SSD极限
                'innodb_io_capacity_max': '10000',
                'innodb_read_io_threads': '20',  # 充分利用多核
                'innodb_write_io_threads': '20',
                'max_connections': '2000',
                'local_infile': '1',
            }
            
            applied_count = 0
            for var, value in global_optimizations.items():
                try:
                    # 保存原值
                    cursor.execute(f"SELECT @@GLOBAL.{var}")
                    result = cursor.fetchone()
                    if result:
                        self._original_settings[var] = result[0]
                    
                    # 设置新值
                    cursor.execute(f"SET GLOBAL {var} = {value}")
                    applied_count += 1
                except Exception:
                    # 某些变量可能无法动态修改或需要特殊权限
                    pass
            
            if applied_count > 0:
                print(f"  ✓ 已应用 {applied_count} 项全局优化（需SUPER权限）")
            else:
                print("  ℹ️ 无SUPER权限，使用会话级优化")
            sys.stdout.flush()
        
        # 会话级优化（不需要特殊权限）- 极限配置
        try:
            cursor.execute("SET SESSION sql_mode = ''")
            cursor.execute("SET SESSION foreign_key_checks = 0")
            cursor.execute("SET SESSION unique_checks = 0")
            cursor.execute("SET SESSION autocommit = 0")
            cursor.execute("SET SESSION sort_buffer_size = 67108864")        # 64MB
            cursor.execute("SET SESSION join_buffer_size = 67108864")        # 64MB
            cursor.execute("SET SESSION read_buffer_size = 33554432")        # 32MB
            cursor.execute("SET SESSION read_rnd_buffer_size = 33554432")    # 32MB
            cursor.execute("SET SESSION tmp_table_size = 8589934592")        # 8GB
            cursor.execute("SET SESSION max_heap_table_size = 8589934592")   # 8GB
            cursor.execute("SET SESSION bulk_insert_buffer_size = 536870912") # 512MB
            cursor.execute("SET SESSION optimizer_switch = 'block_nested_loop=on,batched_key_access=on'")
            
            try:
                cursor.execute("SET SESSION sql_log_bin = 0")  # 禁用binlog
            except Exception:
                pass
            
            self.connection.commit()
            print("  ✓ 会话级缓冲区已优化（极速模式）")
            sys.stdout.flush()
        except Exception as e:
            print(f"  ⚠️ 会话级优化部分失败: {e}")
            sys.stdout.flush()
        finally:
            cursor.close()
        
        return True
    
    def restore_settings(self):
        """恢复原始全局配置"""
        if not self.connection or not self._original_settings:
            return
        
        try:
            print("\n[清理] 正在恢复配置...")
            sys.stdout.flush()
            
            cursor = self.connection.cursor()
            restored_count = 0
            
            for var, value in self._original_settings.items():
                try:
                    cursor.execute(f"SET GLOBAL {var} = {value}")
                    restored_count += 1
                except Exception:
                    pass
            
            cursor.close()
            
            if restored_count > 0:
                print(f"[清理] ✓ 已恢复 {restored_count} 项全局配置")
                sys.stdout.flush()
        except Exception:
            pass
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            try:
                self.restore_settings()
                self.connection.rollback()
                self.connection.close()
                self.connection = None
                print("[清理] ✓ 连接已关闭")
                sys.stdout.flush()
            except Exception:
                pass
    
    def execute_sql(self, sql, description, skip_commit=False, batch_commit=False):
        """
        执行SQL语句（带计时和日志）
        
        Args:
            sql: SQL语句
            description: 操作描述
            skip_commit: 是否跳过提交
            batch_commit: 是否批量提交模式
        
        Returns:
            bool: 是否执行成功
        """
        if not self.connection:
            print(f"  {description}... ✗ 失败: 数据库未连接")
            sys.stdout.flush()
            return False
        
        cursor = None
        try:
            import time
            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"  [{timestamp}] {description}...", end='', flush=True)
            start_time = time.time()
            
            cursor = self.connection.cursor()
            
            # 对于大批量操作，禁用自动提交
            if batch_commit:
                cursor.execute("SET autocommit = 0")
            
            cursor.execute(sql)
            
            if not skip_commit:
                self.connection.commit()
            
            elapsed = time.time() - start_time
            affected_rows = cursor.rowcount
            
            if affected_rows > 0 and elapsed > 0:
                speed = int(affected_rows / elapsed)
                print(f" ✓ {affected_rows:,}行 ({elapsed:.1f}s, {speed:,}行/s)")
            else:
                print(f" ✓ ({elapsed:.1f}s)")
            sys.stdout.flush()
            return True
        except Exception as e:
            print(f" ✗ 失败: {e}")
            sys.stdout.flush()
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
    
    def get_table_count(self, table_name):
        """获取表行数"""
        if not self.connection:
            return 0
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]
        except Exception:
            return 0
        finally:
            cursor.close()
    
    def __enter__(self):
        """支持 with 语句"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持 with 语句"""
        self.close()


# 全局单例（用于信号处理）
_global_db_manager = None


def set_global_db_manager(manager):
    """设置全局数据库管理器（用于信号处理）"""
    global _global_db_manager
    _global_db_manager = manager


def cleanup_global_db_manager():
    """清理全局数据库管理器"""
    global _global_db_manager
    if _global_db_manager:
        _global_db_manager.close()
        _global_db_manager = None


def get_db_manager(db_config):
    """
    获取数据库管理器（工厂函数）
    
    Args:
        db_config: 数据库配置字典
    
    Returns:
        DatabaseManager: 数据库管理器实例
    """
    manager = DatabaseManager(db_config)
    if manager.connect():
        manager.optimize_for_performance()
        set_global_db_manager(manager)
        return manager
    return None
