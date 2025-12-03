"""
将CSV数据加载到MySQL数据库
支持全量模式（删除重建）和增量模式（追加数据）
"""
import pandas as pd
import pymysql
import os
import sys
import json
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import atexit

# 获取项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# 全局引擎对象，用于信号处理
_global_engine = None

def cleanup_engine():
    """清理数据库引擎和连接"""
    global _global_engine
    if _global_engine:
        try:
            print("\n[清理] 正在恢复数据库设置并释放连接...")
            sys.stdout.flush()
            try:
                with _global_engine.connect() as conn:
                    # 回滚未提交的事务
                    conn.rollback()
                    # 恢复数据库设置
                    conn.execute(text("SET SESSION foreign_key_checks = 1"))
                    conn.execute(text("SET SESSION unique_checks = 1"))
                    conn.commit()
                    print("[清理] ✓ 数据库设置已恢复")
            except:
                pass
            _global_engine.dispose()
            _global_engine = None
            print("[清理] ✓ 数据库连接已关闭")
            sys.stdout.flush()
        except:
            pass

def signal_handler(signum, frame):
    """处理中断信号（Ctrl+C）"""
    print("\n\n[中断] 检测到用户中断，正在清理资源...")
    sys.stdout.flush()
    cleanup_engine()
    print("[中断] 程序已安全退出")
    sys.stdout.flush()
    sys.exit(1)

# 表结构映射（CSV列名 -> 数据库列名）
COLUMN_MAPPING = {
    'ods_stores': {
        '店铺ID': 'store_id',
        '店铺名称': 'store_name',
        '店铺类型': 'store_type',
        '平台': 'platform',
        '开店日期': 'open_date'
    },
    'ods_products': {
        'SKU_ID': 'sku_id',
        '商品ID': 'product_id',
        '产品编码': 'product_code',
        '规格编码': 'spec_code',
        '店铺ID': 'store_id',
        '平台': 'platform',
        '商品名称': 'product_name',
        '规格': 'spec',
        '一级类目': 'category_l1',
        '二级类目': 'category_l2',
        '商品分层': 'product_tier',
        '售价': 'price',
        '成本': 'cost',
        '库存': 'stock',
        '创建时间': 'create_time'
    },
    'ods_users': {
        '用户ID': 'user_id',
        '用户名': 'user_name',
        '性别': 'gender',
        '年龄': 'age',
        '城市': 'city',
        '注册日期': 'register_date'
    },
    'ods_orders': {
        '订单ID': 'order_id',
        '用户ID': 'user_id',
        '店铺ID': 'store_id',
        '平台': 'platform',
        '下单时间': 'order_time',
        '订单状态': 'order_status',
        '商品总额': 'total_amount',
        '优惠金额': 'discount_amount',
        '运费': 'shipping_fee',
        '实付金额': 'final_amount',
        '成本总额': 'total_cost',
        '支付方式': 'payment_method',
        '流量来源': 'traffic_source',
        '创建时间': 'create_time',
        '更新时间': 'update_time'
    },
    'ods_order_details': {
        '订单明细ID': 'order_detail_id',
        '订单ID': 'order_id',
        'SKU_ID': 'sku_id',
        '商品ID': 'product_id',
        '数量': 'quantity',
        '单价': 'price',
        '金额': 'amount'
    },
    'ods_promotion': {
        '推广ID': 'promotion_id',
        '日期': 'date',
        '店铺ID': 'store_id',
        '平台': 'platform',
        'SKU_ID': 'sku_id',
        '商品ID': 'product_id',
        '一级类目': 'category_l1',
        '二级类目': 'category_l2',
        '推广渠道': 'channel',
        '推广花费': 'cost',
        '曝光量': 'impressions',
        '点击量': 'clicks',
        '点击率': 'ctr'
    },
    'ods_traffic': {
        '日期': 'date',
        '店铺ID': 'store_id',
        '平台': 'platform',
        '访客数': 'visitors',
        '浏览量': 'page_views',
        '搜索流量': 'search_traffic',
        '推荐流量': 'recommend_traffic',
        '直接访问': 'direct_traffic',
        '其他流量': 'other_traffic',
        '平均停留时长': 'avg_stay_time',
        '跳失率': 'bounce_rate'
    },
    'ods_inventory': {
        '库存记录ID': 'inventory_id',
        '日期': 'date',
        'SKU_ID': 'sku_id',
        '商品ID': 'product_id',
        '店铺ID': 'store_id',
        '变动类型': 'change_type',
        '变动数量': 'change_quantity',
        '变动后库存': 'stock_quantity',
        '备注': 'remark'
    },
    'ods_product_traffic': {
        '流量ID': 'traffic_id',
        '日期': 'date',
        '店铺ID': 'store_id',
        '平台': 'platform',
        'SKU_ID': 'sku_id',
        '商品ID': 'product_id',
        '一级类目': 'category_l1',
        '二级类目': 'category_l2',
        '流量渠道': 'channel',
        '曝光量': 'impressions',
        '点击量': 'clicks',
        '收藏量': 'favorites',
        '加购量': 'add_to_cart',
        '点击率': 'ctr'
    }
}


def get_db_connection(db_config):
    """获取数据库连接"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None


def create_database_if_not_exists(db_config):
    """创建数据库（如果不存在）"""
    try:
        config = db_config.copy()
        db_name = config.pop('database')
        conn = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.commit()
        cursor.close()
        conn.close()
        print(f"数据库 {db_name} 已就绪")
        return True
    except Exception as e:
        print(f"创建数据库失败: {e}")
        return False


def load_csv_with_load_data_infile(csv_path, table_name, engine, column_mapping):
    """
    使用 LOAD DATA LOCAL INFILE 快速导入（比 to_sql 快 10-20 倍）
    """
    import tempfile
    import shutil
    
    try:
        # 读取 CSV 并重命名列
        df = pd.read_csv(csv_path, encoding='utf-8-sig', low_memory=False)
        
        if table_name in column_mapping:
            df = df.rename(columns=column_mapping[table_name])
        
        # 创建临时文件（确保列名正确）
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp_file:
            tmp_path = tmp_file.name
            df.to_csv(tmp_path, index=False, header=True)
        
        # 获取列名
        columns = ', '.join([f'`{col}`' for col in df.columns])
        
        # 使用 LOAD DATA LOCAL INFILE
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            
            # 转换路径为 Unix 风格（MySQL 要求）
            tmp_path_unix = tmp_path.replace('\\', '/')
            
            load_sql = f"""
            LOAD DATA LOCAL INFILE '{tmp_path_unix}'
            INTO TABLE `{table_name}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            ({columns})
            """
            
            cursor.execute(load_sql)
            conn.commit()
            affected_rows = cursor.rowcount
            cursor.close()
        finally:
            conn.close()
        
        # 删除临时文件
        os.unlink(tmp_path)
        
        return True, affected_rows
        
    except Exception as e:
        print(f"  LOAD DATA INFILE 失败: {str(e)[:100]}")
        print(f"  回退到标准导入方法...")
        return False, 0


def load_dataframes_to_db(dataframes, mode='full', db_config=None):
    """
    直接从 DataFrame 加载到数据库（跳过 CSV，最快）
    dataframes: 字典 {table_name: dataframe}
    """
    print(f"\n{'='*60}")
    print(f"直接从内存加载数据到数据库（高速模式）")
    print(f"{'='*60}")
    
    # 创建数据库（如果不存在）
    if not create_database_if_not_exists(db_config):
        print("✗ 数据库创建失败")
        return False
    
    from sqlalchemy import create_engine, text
    
    engine = create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4",
        pool_size=20,
        max_overflow=40,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
        connect_args={
            'connect_timeout': 300,
            'read_timeout': 300,
            'write_timeout': 300
        }
    )
    
    # 优化 MySQL 参数
    try:
        with engine.connect() as conn:
            try:
                conn.execute(text("SET SESSION foreign_key_checks = 0"))
                conn.execute(text("SET SESSION unique_checks = 0"))
                conn.execute(text("SET SESSION autocommit = 0"))
                conn.commit()
                print("  ✓ MySQL性能参数已优化")
                sys.stdout.flush()
            except Exception as e:
                print(f"  警告: 性能优化设置失败 - {e}")
                sys.stdout.flush()
    except Exception as e:
        print(f"  警告: 无法连接数据库进行优化 - {e}")
        sys.stdout.flush()
    
    # 表名映射
    table_mapping = {
        'stores': 'ods_stores',
        'products': 'ods_products',
        'users': 'ods_users',
        'orders': 'ods_orders',
        'order_details': 'ods_order_details',
        'promotion': 'ods_promotion',
        'traffic': 'ods_traffic',
        'inventory': 'ods_inventory'
    }
    
    success_count = 0
    
    for df_name, df in dataframes.items():
        table_name = table_mapping.get(df_name, f'ods_{df_name}')
        
        try:
            print(f"\n  正在加载: {table_name} ({len(df):,} 行)...")
            sys.stdout.flush()
            
            # 删除旧表
            if mode == 'full':
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    conn.commit()
            
            # 列名映射
            if table_name in COLUMN_MAPPING:
                df = df.rename(columns=COLUMN_MAPPING[table_name])
            
            # 使用原生批量插入（高速模式）
            batch_insert_native(df, table_name, engine)
            
            print(f"  ✓ 加载成功: {table_name} ({len(df):,} 行)")
            sys.stdout.flush()
            success_count += 1
            
        except Exception as e:
            print(f"  ✗ 加载失败: {table_name} - {str(e)}")
            sys.stdout.flush()
    
    # 恢复 MySQL 设置
    try:
        with engine.connect() as conn:
            try:
                conn.execute(text("SET SESSION foreign_key_checks = 1"))
                conn.execute(text("SET SESSION unique_checks = 1"))
                conn.execute(text("COMMIT"))
                print("\n  ✓ MySQL设置已恢复")
                sys.stdout.flush()
            except Exception as e:
                print(f"  警告: 恢复设置失败 - {e}")
                sys.stdout.flush()
    except Exception as e:
        print(f"  警告: 无法连接数据库恢复设置 - {e}")
        sys.stdout.flush()
    finally:
        # 确保引擎资源释放
        try:
            engine.dispose()
        except:
            pass
    
    print(f"\n数据加载完成: {success_count}/{len(dataframes)} 个表成功")
    return success_count == len(dataframes)


def batch_insert_native(df, table_name, engine):
    """
    高性能批量插入 - 行业标准速度
    使用executemany + 大批次 + 禁用索引检查
    目标：10万行/秒
    """
    import numpy as np
    
    df = df.replace({np.nan: None})
    
    columns = df.columns.tolist()
    columns_str = ', '.join([f'`{col}`' for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    
    # 先用pandas创建表结构
    df.iloc[0:0].to_sql(table_name, con=engine, if_exists='replace', index=False)
    
    total_rows = len(df)
    batch_size = 10000  # 1万条/批，平衡内存和速度
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    try:
        # 性能优化设置
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        # 批量插入
        sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            values = [tuple(None if pd.isna(x) else x for x in row) for row in batch.values]
            cursor.executemany(sql, values)
        
        conn.commit()
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def load_with_load_data_infile(df, table_name, engine):
    """
    使用 LOAD DATA LOCAL INFILE 极速导入（需要开启 local_infile）
    性能：比批量插入快 5-10 倍
    
    需要配置：
    1. MySQL配置文件添加：local_infile=1
    2. 或执行：SET GLOBAL local_infile=1; (需要SUPER权限)
    """
    import tempfile
    import os
    
    tmp_path = None
    try:
        # 创建临时CSV文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, 
                                         encoding='utf-8', newline='') as tmp_file:
            tmp_path = tmp_file.name
            # 写入CSV（不包含表头，LOAD DATA会跳过）
            df.to_csv(tmp_path, index=False, header=True, encoding='utf-8')
        
        # 转换路径为Unix风格
        tmp_path_unix = tmp_path.replace('\\', '/')
        
        # 获取列名
        columns = ', '.join([f'`{col}`' for col in df.columns])
        
        # 创建表结构（空表）
        first_row = df.iloc[0:1]
        first_row.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
        # 清空表，准备LOAD DATA
        with engine.connect() as conn_temp:
            conn_temp.execute(text(f"TRUNCATE TABLE `{table_name}`"))
            conn_temp.commit()
        
        # 使用 LOAD DATA LOCAL INFILE
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        try:
            # 性能优化
            cursor.execute("SET unique_checks=0")
            cursor.execute("SET foreign_key_checks=0")
            cursor.execute("SET autocommit=0")
            
            try:
                cursor.execute("SET sql_log_bin=0")
            except:
                pass
            
            # LOAD DATA LOCAL INFILE
            load_sql = f"""
            LOAD DATA LOCAL INFILE '{tmp_path_unix}'
            INTO TABLE `{table_name}`
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' 
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            ({columns})
            """
            
            cursor.execute(load_sql)
            
            # 恢复设置
            cursor.execute("SET unique_checks=1")
            cursor.execute("SET foreign_key_checks=1")
            
            try:
                cursor.execute("SET sql_log_bin=1")
            except:
                pass
            
            conn.commit()
            affected_rows = cursor.rowcount
            
            return True, affected_rows
            
        except Exception as e:
            # 异常时回滚并恢复设置
            try:
                conn.rollback()
                cursor.execute("SET unique_checks=1")
                cursor.execute("SET foreign_key_checks=1")
                try:
                    cursor.execute("SET sql_log_bin=1")
                except:
                    pass
            except:
                pass
            raise e
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
            
    except Exception as e:
        print(f"  LOAD DATA INFILE 失败: {str(e)}")
        print(f"  回退到批量插入模式...")
        sys.stdout.flush()
        return False, 0
    finally:
        # 确保删除临时文件
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except:
                pass


def load_csv_file(csv_path, table_name):
    """多线程读取单个CSV文件"""
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig', low_memory=False)
        if table_name in COLUMN_MAPPING:
            df = df.rename(columns=COLUMN_MAPPING[table_name])
        return table_name, df, None
    except Exception as e:
        return table_name, None, str(e)


def load_layer_to_db(layer, mode='full', db_config=None):
    """
    加载指定层的数据到数据库（优化版：使用 LOAD DATA INFILE）
    layer: 'ods', 'dwd', 'dws'
    mode: 'full' 全量模式（删除重建）, 'incremental' 增量模式（追加）
    db_config: 数据库配置
    """
    print(f"\n{'='*60}")
    print(f"开始加载 {layer.upper()} 层数据 - 模式: {mode}")
    print(f"{'='*60}")
    
    global _global_engine
    
    # 创建数据库引擎（极致性能：无限制连接池）
    engine = create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4&local_infile=1",
        pool_size=100,  # 极大连接池
        max_overflow=200,  # 极大溢出连接数
        pool_pre_ping=False,  # 禁用ping检查，提速
        pool_recycle=7200,
        echo=False,
        connect_args={
            'connect_timeout': 1200,
            'read_timeout': 1200,
            'write_timeout': 1200
        }
    )
    
    _global_engine = engine
    
    # 优化MySQL性能参数
    try:
        with engine.connect() as conn:
            try:
                # 临时禁用索引和约束检查以加速插入
                conn.execute(text("SET SESSION sql_mode = ''"))
                conn.execute(text("SET SESSION foreign_key_checks = 0"))
                conn.execute(text("SET SESSION unique_checks = 0"))
                conn.execute(text("SET SESSION autocommit = 0"))
                conn.commit()
                print("  ✓ MySQL性能参数已优化")
                sys.stdout.flush()
            except Exception as e:
                print(f"  警告: 性能优化设置失败 - {e}")
                sys.stdout.flush()
    except Exception as e:
        print(f"  警告: 无法连接数据库进行优化 - {e}")
        sys.stdout.flush()
    
    # 获取该层的所有CSV文件
    layer_path = os.path.join(DATA_DIR, layer)
    if not os.path.exists(layer_path):
        print(f"错误: 目录 {layer_path} 不存在")
        return False
    
    csv_files = [f for f in os.listdir(layer_path) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"警告: {layer_path} 目录下没有CSV文件")
        return False
    
    # 多线程读取所有CSV文件到内存（极致并发）
    print(f"\n使用多线程读取 {len(csv_files)} 个CSV文件...")
    sys.stdout.flush()
    
    dataframes = {}
    max_read_workers = min(len(csv_files), 16)
    with ThreadPoolExecutor(max_workers=max_read_workers) as executor:
        futures = {}
        for csv_file in csv_files:
            table_name = csv_file.replace('.csv', '')
            csv_path = os.path.join(layer_path, csv_file)
            futures[executor.submit(load_csv_file, csv_path, table_name)] = table_name
        
        for future in as_completed(futures):
            table_name, df, error = future.result()
            if error:
                print(f"  ✗ 读取失败: {table_name} - {error}")
                sys.stdout.flush()
            else:
                dataframes[table_name] = df
                print(f"  ✓ 已读取: {table_name} ({len(df):,} 行)")
                sys.stdout.flush()
    
    if not dataframes:
        print("错误: 没有成功读取任何文件")
        return False
    
    print(f"\n所有文件已读取到内存，开始并行导入...")
    sys.stdout.flush()
    
    # 先删除旧表
    if mode == 'full':
        print("  删除旧表...")
        with engine.connect() as conn:
            for table_name in dataframes.keys():
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
        print("  ✓ 旧表已删除")
        sys.stdout.flush()
    
    # 尝试使用 LOAD DATA INFILE（最快），失败则回退到批量插入
    success_count = 0
    use_load_data = True  # 默认尝试使用 LOAD DATA INFILE
    
    # 先测试是否支持 LOAD DATA LOCAL INFILE
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SHOW VARIABLES LIKE 'local_infile'"))
            row = result.fetchone()
            if row and row[1].lower() != 'on':
                print("  ⚠️ local_infile 未开启，使用批量插入模式")
                print("  提示：执行 SET GLOBAL local_infile=1; 可启用极速导入（需要SUPER权限）")
                sys.stdout.flush()
                use_load_data = False
    except:
        use_load_data = False
    
    # 按数据量排序，小表先导入
    sorted_tables = sorted(dataframes.items(), key=lambda x: len(x[1]))
    
    import time
    total_rows = sum(len(df) for df in dataframes.values())
    start_time = time.time()
    imported_rows = 0
    
    for table_name, df in sorted_tables:
        try:
            table_start = time.time()
            rows = len(df)
            
            # 大表尝试LOAD DATA INFILE
            if use_load_data and rows > 10000:
                success, affected = load_with_load_data_infile(df, table_name, engine)
                if success and affected > 0:
                    elapsed = time.time() - table_start
                    speed = int(rows / elapsed) if elapsed > 0 else rows
                    print(f"  ✓ {table_name}: {rows:,} 行 ({speed:,} 行/秒) [LOAD DATA]")
                    imported_rows += rows
                    success_count += 1
                    continue
            
            # 批量插入
            batch_insert_native(df, table_name, engine)
            elapsed = time.time() - table_start
            speed = int(rows / elapsed) if elapsed > 0 else rows
            print(f"  ✓ {table_name}: {rows:,} 行 ({speed:,} 行/秒)")
            imported_rows += rows
            success_count += 1
            
        except Exception as e:
            print(f"  ✗ {table_name}: 失败 - {str(e)[:50]}")
            sys.stdout.flush()
    
    # 打印总体性能
    total_time = time.time() - start_time
    avg_speed = int(imported_rows / total_time) if total_time > 0 else imported_rows
    print(f"\n  总计: {imported_rows:,} 行, 耗时 {total_time:.1f}秒, 平均 {avg_speed:,} 行/秒")
    
    # 恢复MySQL设置
    try:
        with engine.connect() as conn:
            try:
                conn.execute(text("SET SESSION foreign_key_checks = 1"))
                conn.execute(text("SET SESSION unique_checks = 1"))
                conn.execute(text("COMMIT"))
                print("\n  ✓ MySQL设置已恢复")
                sys.stdout.flush()
            except Exception as e:
                print(f"  警告: 恢复设置失败 - {e}")
                sys.stdout.flush()
    except Exception as e:
        print(f"  警告: 无法连接数据库恢复设置 - {e}")
        sys.stdout.flush()
    finally:
        # 确保引擎资源释放
        try:
            engine.dispose()
        except:
            pass
    
    print(f"\n{layer.upper()} 层加载完成: {success_count}/{len(dataframes)} 个表成功")
    return success_count == len(dataframes)


def main():
    """主函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 终止信号
    
    # 注册退出时的清理函数
    atexit.register(cleanup_engine)
    
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
    
    layer = config.get('layer', 'ods')
    mode = config.get('mode', 'full')
    
    print("="*60)
    print("数据库加载工具")
    print("="*60)
    print(f"数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print(f"层级: {layer.upper()}")
    print(f"模式: {'全量（删除重建）' if mode == 'full' else '增量（追加数据）'}")
    print("="*60)
    
    try:
        # 创建数据库
        if not create_database_if_not_exists(db_config):
            return
        
        # 加载数据
        success = load_layer_to_db(layer, mode, db_config)
        
        if success:
            print("\n✓ 数据加载完成！")
        else:
            print("\n✗ 数据加载失败")
    except KeyboardInterrupt:
        print("\n[中断] 用户取消操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n[错误] 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
