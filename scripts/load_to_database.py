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
        '跳失率': 'bounce_rate',
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


def load_layer_to_db(layer, mode='full', db_config=None):
    """
    加载指定层的数据到数据库
    layer: 'ods', 'dwd', 'dws'
    mode: 'full' 全量模式（删除重建）, 'incremental' 增量模式（追加）
    db_config: 数据库配置
    """
    print(f"\n{'='*60}")
    print(f"开始加载 {layer.upper()} 层数据 - 模式: {mode}")
    print(f"{'='*60}")
    
    # 创建数据库引擎
    engine = create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4"
    )
    
    # 获取该层的所有CSV文件
    layer_path = os.path.join(DATA_DIR, layer)
    if not os.path.exists(layer_path):
        print(f"错误: 目录 {layer_path} 不存在")
        return False
    
    csv_files = [f for f in os.listdir(layer_path) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"警告: {layer_path} 目录下没有CSV文件")
        return False
    
    success_count = 0
    
    for csv_file in csv_files:
        table_name = csv_file.replace('.csv', '')
        csv_path = os.path.join(layer_path, csv_file)
        
        try:
            # 读取CSV
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            if len(df) == 0:
                print(f"  跳过 {table_name}: 文件为空")
                continue
            
            # 列名映射
            if table_name in COLUMN_MAPPING:
                df = df.rename(columns=COLUMN_MAPPING[table_name])
            
            # 全量模式：删除表并重建
            if mode == 'full':
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    conn.commit()
                print(f"  ✓ 删除旧表: {table_name}")
            
            # 写入数据库
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append' if mode == 'incremental' else 'replace',
                index=False,
                chunksize=1000
            )
            
            print(f"  ✓ 加载成功: {table_name} ({len(df)} 行)")
            success_count += 1
            
        except Exception as e:
            print(f"  ✗ 加载失败: {table_name} - {str(e)}")
    
    print(f"\n{layer.upper()} 层加载完成: {success_count}/{len(csv_files)} 个表成功")
    return success_count == len(csv_files)


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
