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

# 获取项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# 表结构映射（CSV列名 -> 数据库列名）
COLUMN_MAPPING = {
    'ods_stores': {
        '店铺ID': 'store_id',
        '店铺名称': 'store_name',
        '平台': 'platform',
        '开店日期': 'open_date'
    },
    'ods_products': {
        '商品ID': 'product_id',
        '店铺ID': 'store_id',
        '平台': 'platform',
        '商品名称': 'product_name',
        '一级类目': 'category_l1',
        '二级类目': 'category_l2',
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
        '创建时间': 'create_time',
        '更新时间': 'update_time'
    },
    'ods_order_details': {
        '订单明细ID': 'order_detail_id',
        '订单ID': 'order_id',
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
        '商品ID': 'product_id',
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
        '商品ID': 'product_id',
        '店铺ID': 'store_id',
        '变动类型': 'change_type',
        '变动数量': 'change_quantity',
        '变动后库存': 'after_stock',
        '备注': 'remark'
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
    
    print(f"\n数据加载完成: {success_count}/{len(dataframes)} 个表成功")
    return success_count == len(dataframes)


def batch_insert_native(df, table_name, engine):
    """原生批量插入（极致性能模式）"""
    import numpy as np
    
    # 替换 NaN 为 None
    df = df.replace({np.nan: None})
    
    # 获取列名
    columns = df.columns.tolist()
    columns_str = ', '.join([f'`{col}`' for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    
    # 创建表（如果不存在）
    first_row = df.iloc[0:1]
    first_row.to_sql(table_name, con=engine, if_exists='replace', index=False)
    
    # 极致批次大小
    total_rows = len(df)
    if total_rows > 1000000:
        batch_size = 100000  # 超大表：10万条/批
    elif total_rows > 500000:
        batch_size = 50000   # 大表：5万条/批
    else:
        batch_size = 20000   # 普通表：2万条/批
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    try:
        # 禁用自动提交
        conn.autocommit(False)
        
        # 极致性能优化（移除需要SUPER权限的设置）
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            values = [tuple(row) for row in batch.values]
            
            # 拼接批量插入SQL
            sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            cursor.executemany(sql, values)
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        
        # 最后统一提交
        conn.commit()
    finally:
        cursor.close()
        conn.close()


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
    
    # 优化MySQL性能参数
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
    
    # 多线程并行导入（极致并发）
    success_count = 0
    max_workers = min(len(dataframes), 16)  # 最多16线程
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for table_name, df in dataframes.items():
            print(f"  提交任务: {table_name} ({len(df):,} 行)")
            sys.stdout.flush()
            future = executor.submit(batch_insert_native, df, table_name, engine)
            futures[future] = table_name
        
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                future.result()
                print(f"  ✓ 导入成功: {table_name}")
                sys.stdout.flush()
                success_count += 1
            except Exception as e:
                print(f"  ✗ 导入失败: {table_name} - {str(e)}")
                sys.stdout.flush()
    
    # 恢复MySQL设置
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
    
    print(f"\n{layer.upper()} 层加载完成: {success_count}/{len(dataframes)} 个表成功")
    return success_count == len(dataframes)


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
    
    layer = config.get('layer', 'ods')
    mode = config.get('mode', 'full')
    
    print("="*60)
    print("数据库加载工具")
    print("="*60)
    print(f"数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print(f"层级: {layer.upper()}")
    print(f"模式: {'全量（删除重建）' if mode == 'full' else '增量（追加数据）'}")
    print("="*60)
    
    # 创建数据库
    if not create_database_if_not_exists(db_config):
        return
    
    # 加载数据
    success = load_layer_to_db(layer, mode, db_config)
    
    if success:
        print("\n✓ 数据加载完成！")
    else:
        print("\n✗ 数据加载失败")


if __name__ == '__main__':
    main()
