"""
生成电商模拟数据
支持多种主营类目和自定义平台店铺配置
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys
import io
import json
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing
import csv
import time

# 设置控制台UTF-8编码（解决Windows中文乱码）
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

fake = Faker('zh_CN')
np.random.seed(42)
random.seed(42)

# 获取项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# 确保数据目录存在
os.makedirs(os.path.join(DATA_DIR, 'ods'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'dwd'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'dws'), exist_ok=True)

# 不同类目的商品配置
CATEGORY_CONFIGS = {
    'bicycle': {
        'name': '自行车',
        'categories': {
            '整车-品牌': ['品牌公路车', '品牌山地车', '品牌折叠车'],  # 千元品牌
            '整车-白牌': ['白牌公路车', '白牌山地车', '白牌折叠车', '白牌通勤车', '白牌儿童车'],  # 百元白牌
            '骑行装备': ['头盔', '手套', '骑行服', '骑行裤', '骑行鞋', '眼镜', '水壶', '车灯', '车锁', '码表']
        },
        'price_ranges': {
            '整车-品牌': (800, 3000),      # 千元品牌：800-3000元
            '整车-白牌': (200, 800),        # 百元白牌：200-800元
            '骑行装备': (30, 300)           # 配件：30-300元
        },
        'category_weights': {
            '整车-品牌': 0.25,   # 25% 品牌整车
            '整车-白牌': 0.50,   # 50% 白牌整车（主营）
            '骑行装备': 0.25     # 25% 配件
        }
    },
    'clothing': {
        'name': '服装',
        'categories': {
            '男装': ['T恤', '衬衫', '裤子', '外套', '卫衣', '牛仔裤'],
            '女装': ['连衣裙', '半身裙', '上衣', '裤子', '外套', '毛衣'],
            '童装': ['T恤', '裤子', '外套', '连衣裙', '套装']
        },
        'price_ranges': {
            '男装': (50, 800),
            '女装': (80, 1200),
            '童装': (40, 500)
        }
    },
    'electronics': {
        'name': '数码',
        'categories': {
            '手机': ['智能手机', '老人机', '游戏手机', '拍照手机'],
            '电脑': ['笔记本', '台式机', '平板电脑', '一体机'],
            '配件': ['耳机', '充电器', '数据线', '保护壳', '移动电源', '键盘', '鼠标']
        },
        'price_ranges': {
            '手机': (1000, 8000),
            '电脑': (3000, 15000),
            '配件': (20, 500)
        }
    },
    'food': {
        'name': '食品',
        'categories': {
            '零食': ['薯片', '饼干', '糖果', '巧克力', '坚果', '果干'],
            '生鲜': ['水果', '蔬菜', '肉类', '海鲜', '蛋类'],
            '饮料': ['茶饮', '咖啡', '果汁', '碳酸饮料', '矿泉水']
        },
        'price_ranges': {
            '零食': (10, 100),
            '生鲜': (15, 200),
            '饮料': (5, 80)
        }
    },
    'beauty': {
        'name': '美妆',
        'categories': {
            '护肤': ['洁面', '水乳', '精华', '面膜', '防晒', '眼霜'],
            '彩妆': ['口红', '粉底', '眼影', '睫毛膏', '腮红', '眉笔'],
            '个护': ['洗发水', '沐浴露', '牙膏', '香水', '身体乳']
        },
        'price_ranges': {
            '护肤': (50, 800),
            '彩妆': (30, 500),
            '个护': (20, 300)
        }
    },
    'home': {
        'name': '家居',
        'categories': {
            '家具': ['沙发', '床', '桌子', '椅子', '柜子', '书架'],
            '家纺': ['床单', '被套', '枕头', '毛巾', '窗帘', '地毯'],
            '装饰': ['挂画', '摆件', '花瓶', '相框', '灯具', '钟表']
        },
        'price_ranges': {
            '家具': (500, 5000),
            '家纺': (50, 800),
            '装饰': (30, 500)
        }
    }
}

# 默认使用自行车类目
PRODUCT_CATEGORIES = CATEGORY_CONFIGS['bicycle']['categories']


"""
编码规则说明（中文编码，一目了然）：
SPU编码 = 品牌类型-商品类型-序号
SKU编码 = SPU编码-具体属性（中文）

示例：
- SPU: 品牌-公路车-01
- SKU: 品牌-公路车-01-铝架-21速-27寸-黑色
"""

# 整车SKU属性选项（中文）
BIKE_FRAMES = ['铁架', '铝架', '钢架']
BIKE_SPEEDS = ['7速', '21速', '24速', '27速']
BIKE_SIZES = ['24寸', '26寸', '27寸', '29寸']
COLORS = ['黑色', '白色', '红色', '蓝色', '绿色']

# 装备SKU属性选项（中文）
EQUIP_SIZES = ['S码', 'M码', 'L码']


def generate_spu_library(category_config):
    """
    生成SPU商品库（约100款产品）- 真实商品分层模型
    商品分层：
    - 畅销品（30%）：销量高，利润率15-20%
    - 利润品（20%）：销量低，利润率30-40%
    - 主推新品（15%）：销量中，利润率15-25%，推广费高
    - 滞销品（20%）：销量低，利润率10-30%
    - 引流品（15%）：销量高，利润率5-10%（微利或亏损）
    """
    spu_library = {'品牌': [], '白牌': []}
    categories = category_config['categories']
    price_ranges = category_config['price_ranges']
    
    # 商品分层配置（调整利润率以确保整体毛利率在30-37%）
    # 商品利润率 = (售价 - 成本) / 售价，扣除运费后达到目标毛利率
    product_tiers = {
        '畅销品': {'ratio': 0.30, 'profit_margin': (0.28, 0.33), 'sales_weight': 3.0},
        '利润品': {'ratio': 0.20, 'profit_margin': (0.40, 0.50), 'sales_weight': 0.5},
        '主推新品': {'ratio': 0.15, 'profit_margin': (0.28, 0.35), 'sales_weight': 1.5},
        '滞销品': {'ratio': 0.20, 'profit_margin': (0.25, 0.40), 'sales_weight': 0.3},
        '引流品': {'ratio': 0.15, 'profit_margin': (0.20, 0.25), 'sales_weight': 4.0},
    }
    
    # 为每个商品分配分层
    def assign_tier():
        rand = random.random()
        cumulative = 0
        for tier, config in product_tiers.items():
            cumulative += config['ratio']
            if rand < cumulative:
                return tier, config
        return '畅销品', product_tiers['畅销品']
    
    # ========== 品牌整车 ==========
    # 品牌公路车、品牌山地车、品牌折叠车，每类5款
    for sub_cat in categories.get('整车-品牌', []):
        for i in range(1, 6):
            spu_code = f'品牌-{sub_cat}-{i:02d}'
            price_min, price_max = price_ranges.get('整车-品牌', (800, 3000))
            base_price = round(random.uniform(price_min, price_max), 2)
            
            # 根据商品分层设置利润率
            tier, tier_config = assign_tier()
            profit_margin = random.uniform(*tier_config['profit_margin'])
            cost_rate = 1 - profit_margin  # 成本率 = 1 - 利润率
            
            # 生成该SPU下的SKU规格列表
            sku_specs = []
            # 每个SPU生成3个SKU（不同车架+颜色组合）
            frame = random.choice(BIKE_FRAMES)
            speed = random.choice(BIKE_SPEEDS)
            size = random.choice(BIKE_SIZES)
            for color in random.sample(COLORS, 3):
                sku_code = f'{spu_code}-{frame}-{speed}-{size}-{color}'
                spec_name = f'{frame}/{speed}/{size}/{color}'
                sku_specs.append({
                    'SKU编码': sku_code,
                    '规格': spec_name,
                    '价格系数': 1.0 if frame == '铁架' else (1.1 if frame == '钢架' else 1.2)
                })
            
            spu_library['品牌'].append({
                'SPU编码': spu_code,
                '商品名称': sub_cat,
                '一级类目': '整车-品牌',
                '二级类目': sub_cat,
                '基础价格': base_price,
                '成本率': cost_rate,
                '商品分层': tier,
                '规格列表': sku_specs
            })
    
    # ========== 品牌装备 ==========
    for sub_cat in categories.get('骑行装备', []):
        for i in range(1, 3):  # 每类2款
            spu_code = f'品牌-{sub_cat}-{i:02d}'
            price_min, price_max = price_ranges.get('骑行装备', (30, 300))
            base_price = round(random.uniform(price_min * 1.5, price_max * 1.5), 2)
            
            # 装备类利润率更高
            tier, tier_config = assign_tier()
            profit_margin = random.uniform(*tier_config['profit_margin']) + 0.10  # 装备额外+10%利润
            profit_margin = min(0.58, profit_margin)  # 最高58%
            cost_rate = 1 - profit_margin
            
            # 生成SKU（尺码+颜色）
            sku_specs = []
            color = random.choice(COLORS)
            for size in EQUIP_SIZES:
                sku_code = f'{spu_code}-{size}-{color}'
                spec_name = f'{size}/{color}'
                sku_specs.append({
                    'SKU编码': sku_code,
                    '规格': spec_name,
                    '价格系数': 1.0
                })
            
            spu_library['品牌'].append({
                'SPU编码': spu_code,
                '商品名称': sub_cat,
                '一级类目': '骑行装备',
                '二级类目': sub_cat,
                '基础价格': base_price,
                '成本率': cost_rate,
                '商品分层': tier,
                '规格列表': sku_specs
            })
    
    # ========== 白牌整车 ==========
    for sub_cat in categories.get('整车-白牌', []):
        for i in range(1, 9):  # 每类8款
            spu_code = f'白牌-{sub_cat}-{i:02d}'
            price_min, price_max = price_ranges.get('整车-白牌', (200, 800))
            base_price = round(random.uniform(price_min, price_max), 2)
            
            # 白牌整车利润率稍高
            tier, tier_config = assign_tier()
            profit_margin = random.uniform(*tier_config['profit_margin']) + 0.05  # 白牌额外+5%利润
            profit_margin = min(0.55, profit_margin)  # 最高55%
            cost_rate = 1 - profit_margin
            
            # 生成SKU
            sku_specs = []
            frame = random.choice(BIKE_FRAMES)
            speed = random.choice(BIKE_SPEEDS[:2])  # 白牌主要7速/21速
            size = random.choice(BIKE_SIZES)
            for color in random.sample(COLORS, 3):
                sku_code = f'{spu_code}-{frame}-{speed}-{size}-{color}'
                spec_name = f'{frame}/{speed}/{size}/{color}'
                sku_specs.append({
                    'SKU编码': sku_code,
                    '规格': spec_name,
                    '价格系数': 1.0 if frame == '铁架' else (1.05 if frame == '钢架' else 1.1)
                })
            
            spu_library['白牌'].append({
                'SPU编码': spu_code,
                '商品名称': sub_cat,
                '一级类目': '整车-白牌',
                '二级类目': sub_cat,
                '基础价格': base_price,
                '成本率': cost_rate,
                '商品分层': tier,
                '规格列表': sku_specs
            })
    
    # ========== 白牌装备 ==========
    for sub_cat in categories.get('骑行装备', []):
        for i in range(1, 4):  # 每类3款
            spu_code = f'白牌-{sub_cat}-{i:02d}'
            price_min, price_max = price_ranges.get('骑行装备', (30, 300))
            base_price = round(random.uniform(price_min, price_max), 2)
            
            # 白牌装备利润率最高
            tier, tier_config = assign_tier()
            profit_margin = random.uniform(*tier_config['profit_margin']) + 0.15  # 白牌装备额外+15%利润
            profit_margin = min(0.65, profit_margin)  # 最高65%
            cost_rate = 1 - profit_margin
            
            # 生成SKU
            sku_specs = []
            color = random.choice(COLORS)
            for size in EQUIP_SIZES:
                sku_code = f'{spu_code}-{size}-{color}'
                spec_name = f'{size}/{color}'
                sku_specs.append({
                    'SKU编码': sku_code,
                    '规格': spec_name,
                    '价格系数': 1.0
                })
            
            spu_library['白牌'].append({
                'SPU编码': spu_code,
                '商品名称': sub_cat,
                '一级类目': '骑行装备',
                '二级类目': sub_cat,
                '基础价格': base_price,
                '成本率': cost_rate,
                '商品分层': tier,
                '规格列表': sku_specs
            })
    
    return spu_library


# 生成店铺信息
def generate_stores(platform_stores):
    """
    根据配置生成店铺信息
    platform_stores: {平台名: {'品牌': [店铺名], '白牌': [店铺名]}}
    店铺名称格式：【平台名】店铺名
    """
    stores = []
    store_id = 1
    
    for platform, store_config in platform_stores.items():
        # 品牌店铺
        for store_name in store_config.get('品牌', []):
            full_store_name = f'【{platform}】{store_name}'
            stores.append({
                '店铺ID': f'S{store_id:04d}',
                '店铺名称': full_store_name,
                '店铺类型': '品牌',
                '平台': platform,
                '开店日期': fake.date_between(start_date='-3y', end_date='-1y')
            })
            store_id += 1
        
        # 白牌店铺
        for store_name in store_config.get('白牌', []):
            full_store_name = f'【{platform}】{store_name}'
            stores.append({
                '店铺ID': f'S{store_id:04d}',
                '店铺名称': full_store_name,
                '店铺类型': '白牌',
                '平台': platform,
                '开店日期': fake.date_between(start_date='-3y', end_date='-1y')
            })
            store_id += 1
    
    return pd.DataFrame(stores)


# 生成商品信息
def generate_products(stores_df, category_config):
    """
    生成商品数据（完善版 - 正确的商品/SKU关系）
    
    逻辑说明：
    - 产品编码（SPU）：公司内部编码，多店铺共享（如：品牌-公路车-01）
    - 商品ID：每个店铺上架该产品时分配唯一ID（如：P00000001）
    - SKU ID：每个商品ID下的不同规格，每个都有唯一ID（如：SK00000001, SK00000002...）
    - 规格编码：公司内部规格编码（如：品牌-公路车-01-铝架-21速-27寸-黑色）
    
    关系：
    - 一个产品编码（SPU）在不同店铺有不同的商品ID
    - 一个商品ID包含多个SKU ID（该产品的所有规格）
    - 产品编码和规格编码是公司内部统一的，跨店铺共享
    """
    # 先生成SPU商品库
    spu_library = generate_spu_library(category_config)
    
    print(f"   SPU商品库: 品牌 {len(spu_library['品牌'])} 款, 白牌 {len(spu_library['白牌'])} 款")
    
    products = []
    
    # 全局商品ID和SKU ID计数器（确保平台唯一）
    global_product_id = 1
    global_sku_id = 1
    
    for _, store in stores_df.iterrows():
        store_id = store['店铺ID']
        store_type = store['店铺类型']
        platform = store['平台']
        open_date = store['开店日期']
        
        # 根据店铺类型选择对应的SPU库
        store_spus = spu_library.get(store_type, [])
        
        # 该店铺上架所有对应类型的产品
        for spu in store_spus:
            spu_code = spu['SPU编码']  # 公司内部产品编码（多店铺共享）
            base_price = spu['基础价格']
            cost_rate = spu['成本率']
            
            # 为该店铺的这个SPU分配唯一的平台商品ID
            # 注意：同一个产品编码在不同店铺会有不同的商品ID
            platform_product_id = f'P{global_product_id:08d}'
            global_product_id += 1
            
            # 为该商品ID下的所有SKU规格生成记录
            for spec in spu['规格列表']:
                internal_sku_code = spec['SKU编码']  # 公司内部规格编码（多店铺共享）
                sku_price = round(base_price * spec['价格系数'], 2)
                
                # 为该SKU分配唯一的平台SKU ID
                platform_sku_id = f'SK{global_sku_id:08d}'
                global_sku_id += 1
                
                products.append({
                    'SKU_ID': platform_sku_id,      # 平台唯一SKU ID
                    '商品ID': platform_product_id,  # 平台唯一商品ID（同一商品的多个SKU共享此ID）
                    '产品编码': spu_code,           # 公司内部产品编码（跨店铺共享）
                    '规格编码': internal_sku_code,  # 公司内部规格编码（跨店铺共享）
                    '店铺ID': store_id,
                    '平台': platform,
                    '商品名称': spu['商品名称'],
                    '规格': spec['规格'],
                    '一级类目': spu['一级类目'],
                    '二级类目': spu['二级类目'],
                    '商品分层': spu['商品分层'],
                    '售价': sku_price,
                    '成本': round(sku_price * cost_rate, 2),
                    '库存': random.randint(50, 300),
                    '创建时间': open_date
                })
    
    df = pd.DataFrame(products)
    
    # 打印统计信息，验证关系正确性
    total_skus = len(df)
    unique_product_ids = df['商品ID'].nunique()
    unique_spu_codes = df['产品编码'].nunique()
    avg_skus_per_product = total_skus / unique_product_ids if unique_product_ids > 0 else 0
    
    print(f"   ✓ 商品关系统计：")
    print(f"     - 总SKU数: {total_skus}")
    print(f"     - 唯一商品ID数: {unique_product_ids}")
    print(f"     - 唯一产品编码数: {unique_spu_codes}")
    print(f"     - 平均每个商品ID包含 {avg_skus_per_product:.1f} 个SKU")
    
    return df

# 生成用户信息（优化版 - 批量生成）
def generate_users(num_users=3000, time_span_days=365):
    """
    生成用户数据
    num_users: 用户数量
    time_span_days: 时间跨度（天）
    """
    print(f"   正在生成 {num_users} 个用户...")
    cities = [fake.city() for _ in range(50)]  # 预生成城市列表
    genders = ['男', '女']
    
    # 计算注册日期范围（在订单时间跨度之前）
    start_date = f'-{time_span_days + 180}d'  # 提前6个月开始注册
    end_date = f'-{max(1, time_span_days // 4)}d'  # 到时间跨度的1/4处
    
    # 批量生成数据
    users = {
        '用户ID': [f'U{i:08d}' for i in range(1, num_users + 1)],
        '用户名': [f'用户{i}' for i in range(1, num_users + 1)],
        '性别': [random.choice(genders) for _ in range(num_users)],
        '年龄': [random.randint(18, 65) for _ in range(num_users)],
        '城市': [random.choice(cities) for _ in range(num_users)],
        '注册日期': [fake.date_between(start_date=start_date, end_date=end_date) for _ in range(num_users)]
    }
    
    return pd.DataFrame(users)

# 生成订单数据的子任务（用于多进程，性能优化版）
def generate_orders_batch(batch_id, batch_size, stores_list, store_products_dict, users_list, 
                          start_order_id, start_detail_id, time_span_days):
    """
    生成一批订单数据（多进程任务，性能优化）
    注意：store_products_dict 是字典格式，便于序列化
    """
    orders = []
    order_details = []
    
    order_statuses = ['已完成', '已取消', '退款']
    status_weights = [0.92, 0.06, 0.02]  # 提高完成率到92%，更贴合实际电商数据
    payment_methods = ['支付宝', '微信', '银行卡']
    payment_weights = [0.50, 0.40, 0.10]  # 支付宝和微信占主导
    
    # 预生成随机索引
    store_indices = np.random.randint(0, len(stores_list), batch_size)
    user_indices = np.random.randint(0, len(users_list), batch_size)
    
    # 【性能优化1】预生成所有订单时间
    now = datetime.now()
    time_deltas = np.random.randint(0, time_span_days * 24 * 3600, batch_size)
    order_times = [now - timedelta(seconds=int(delta)) for delta in time_deltas]
    
    # 【性能优化2】预生成所有随机数
    order_status_choices = np.random.choice(order_statuses, size=batch_size, p=status_weights)
    payment_method_choices = np.random.choice(payment_methods, size=batch_size, p=payment_weights)
    update_days = np.random.randint(0, 8, batch_size)
    
    order_id = start_order_id
    detail_id = start_detail_id
    
    for i in range(batch_size):
        store = stores_list[store_indices[i]]
        user = users_list[user_indices[i]]
        
        # 从字典获取商品列表
        store_products = store_products_dict.get(store['店铺ID'])
        if not store_products or len(store_products) == 0:
            continue
        
        order_time = order_times[i]
        order_status = order_status_choices[i]
        
        num_items = random.randint(1, min(3, len(store_products)))
        product_indices = np.random.choice(len(store_products), size=num_items, replace=False)
        
        total_amount = 0
        total_cost = 0
        shipping_fee = 0
        
        # 直接从列表获取商品
        for idx in product_indices:
            product = store_products[idx]
            quantity = random.randint(1, 3)
            item_amount = product['售价'] * quantity
            item_cost = product['成本'] * quantity
            
            total_amount += item_amount
            total_cost += item_cost
            
            # 计算运费：整车30元/件，配件3元/件
            if product['一级类目'].startswith('整车'):
                shipping_fee += 30 * quantity
            else:
                shipping_fee += 3 * quantity
            
            order_details.append([
                f'OD{detail_id:08d}',
                f'O{order_id:08d}',
                product['SKU_ID'],
                product['商品ID'],
                quantity,
                product['售价'],
                round(item_amount, 2)
            ])
            detail_id += 1
        
        final_amount = round(total_amount, 2)  # 实付金额 = 商品总额（包邮）
        
        # 流量来源分配（基于真实电商数据分布）
        # 付费流量占比约20-30%，自然流量占比70-80%
        traffic_sources = ['搜索', '推荐', '直接访问', '活动页', '店铺首页', '付费推广']
        traffic_weights = [0.35, 0.25, 0.10, 0.05, 0.05, 0.20]  # 付费推广20%
        traffic_source = random.choices(traffic_sources, weights=traffic_weights)[0]
        
        orders.append([
            f'O{order_id:08d}',
            user['用户ID'],
            store['店铺ID'],
            store['平台'],
            order_time,
            order_status,
            round(total_amount, 2),
            0,  # 优惠金额固定为0
            shipping_fee,
            final_amount if order_status == '已完成' else 0,
            round(total_cost, 2) if order_status == '已完成' else 0,
            payment_method_choices[i],
            traffic_source,  # 新增：流量来源
            order_time,
            order_time + timedelta(days=int(update_days[i]))
        ])
        
        order_id += 1
    
    return orders, order_details, order_id, detail_id


# 生成订单数据（优化版 - 多线程支持，支持百万级数据）
def generate_orders(stores_df, products_df, users_df, num_orders=50000, time_span_days=365):
    """
    生成订单数据（支持多线程）
    num_orders: 订单数量
    time_span_days: 时间跨度（天）
    """
    print(f"   正在生成 {num_orders:,} 个订单（时间跨度: {time_span_days}天）...")
    
    # 始终使用多进程模式
    use_multithread = True
    num_threads = multiprocessing.cpu_count()
    print(f"   使用多进程模式（{num_threads} 进程，充分利用 CPU）...")
    sys.stdout.flush()
    
    # 预先计算每个店铺的商品列表
    # 转换为列表
    stores_list = stores_df.to_dict('records')
    users_list = users_df.to_dict('records')
    
    # 多进程模式：将 DataFrame 转换为可序列化的字典列表
    store_products_dict = {}
    for store_id in stores_df['店铺ID'].unique():
        prods = products_df[products_df['店铺ID'] == store_id]
        if len(prods) > 0:
            # 转换为字典列表（可序列化）
            store_products_dict[store_id] = prods.to_dict('records')
    
    if use_multithread:
        # 多进程模式：优化批次大小，确保每个进程有足够的工作量
        # 每个进程至少处理 5000 个订单，最多处理 50000 个订单
        min_batch = 5000
        max_batch = 50000
        ideal_batch = num_orders // num_threads
        batch_size = max(min_batch, min(max_batch, ideal_batch))
        
        # 重新计算实际需要的进程数
        actual_processes = min(num_threads, (num_orders + batch_size - 1) // batch_size)
        
        print(f"   批次大小: {batch_size:,} 订单/进程")
        print(f"   实际进程数: {actual_processes}")
        sys.stdout.flush()
        
        batches = []
        order_id = 1
        detail_id = 1
        
        for i in range(actual_processes):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, num_orders)
            current_batch_size = end_idx - start_idx
            
            if current_batch_size > 0:
                batches.append((i, current_batch_size, stores_list, store_products_dict, 
                               users_list, order_id, detail_id, time_span_days))
                order_id += current_batch_size * 3  # 预留ID空间（增加预留）
                detail_id += current_batch_size * 10  # 预留明细ID空间（增加预留）
        
        # 使用进程池执行（绕过 GIL，真正并行）
        all_orders = []
        all_order_details = []
        
        print(f"   启动 {actual_processes} 个进程并行生成...")
        sys.stdout.flush()
        
        start_time = time.time()
        
        # 使用实际进程数，并行执行
        with ProcessPoolExecutor(max_workers=actual_processes) as executor:
            # 提交所有任务（立即并行启动）
            futures = {executor.submit(generate_orders_batch, *batch): i for i, batch in enumerate(batches)}
            
            completed = 0
            total_orders_generated = 0
            total_details_generated = 0
            
            # 使用 as_completed 获取完成的任务（真正并行）
            for future in as_completed(futures):
                batch_id = futures[future]
                orders, order_details, _, _ = future.result()
                all_orders.extend(orders)
                all_order_details.extend(order_details)
                
                total_orders_generated += len(orders)
                total_details_generated += len(order_details)
                
                completed += 1
                progress = int((completed / len(futures)) * 100)
                elapsed = time.time() - start_time
                orders_per_sec = total_orders_generated / elapsed if elapsed > 0 else 0
                
                print(f"   进度: {progress}% ({completed}/{len(futures)} 进程) - {total_orders_generated:,} 订单 - {orders_per_sec:,.0f} 订单/秒")
                sys.stdout.flush()
        
        elapsed_total = time.time() - start_time
        orders_per_sec = total_orders_generated / elapsed_total if elapsed_total > 0 else 0
        print(f"   ✓ 多进程生成完成: {total_orders_generated:,} 订单, {total_details_generated:,} 明细")
        print(f"   性能: {orders_per_sec:,.0f} 订单/秒, 总耗时 {elapsed_total:.1f}秒")
        sys.stdout.flush()
        
        orders = all_orders
        order_details = all_order_details
    
    print("   正在创建DataFrame...")
    sys.stdout.flush()
    
    # 使用列表创建DataFrame（比字典快很多）
    # 优化：指定数据类型以减少内存占用
    orders_df = pd.DataFrame(orders, columns=[
        '订单ID', '用户ID', '店铺ID', '平台', '下单时间', '订单状态',
        '商品总额', '优惠金额', '运费', '实付金额', '成本总额', 
        '支付方式', '流量来源', '创建时间', '更新时间'
    ])
    
    # 优化数据类型以减少内存
    orders_df['订单ID'] = orders_df['订单ID'].astype('string')
    orders_df['用户ID'] = orders_df['用户ID'].astype('string')
    orders_df['店铺ID'] = orders_df['店铺ID'].astype('string')
    orders_df['平台'] = orders_df['平台'].astype('category')
    orders_df['订单状态'] = orders_df['订单状态'].astype('category')
    orders_df['支付方式'] = orders_df['支付方式'].astype('category')
    orders_df['流量来源'] = orders_df['流量来源'].astype('category')
    
    order_details_df = pd.DataFrame(order_details, columns=[
        '订单明细ID', '订单ID', 'SKU_ID', '商品ID', '数量', '单价', '金额'
    ])
    
    # 优化数据类型
    order_details_df['订单明细ID'] = order_details_df['订单明细ID'].astype('string')
    order_details_df['订单ID'] = order_details_df['订单ID'].astype('string')
    order_details_df['SKU_ID'] = order_details_df['SKU_ID'].astype('string')
    order_details_df['商品ID'] = order_details_df['商品ID'].astype('string')
    
    print(f"   ✓ DataFrame创建完成（内存优化）")
    sys.stdout.flush()
    
    return orders_df, order_details_df

# 生成推广数据（优化版 - 批量生成）
def generate_promotion(stores_df, products_df, time_span_days=365):
    """
    生成推广投放数据
    time_span_days: 时间跨度（天）
    """
    promotions = []
    promo_id = 1
    
    # 不同平台的推广工具
    platform_channels = {
        '京东': ['京东快车', '京东展位', '京准通', '品牌特秀'],
        '天猫': ['直通车', '钻展', '超级推荐', '品牌特秀'],
        '抖音': ['巨量千川', '抖音小店随心推', 'DOU+', '品牌广告'],
        '快手': ['磁力金牛', '快手粉条', '快手小店推广', '品牌广告'],
        '微信': ['朋友圈广告', '公众号广告', '小程序广告', '视频号推广'],
        '小红书': ['信息流广告', '搜索广告', '薯条', '品牌合作'],
        '拼多多': ['多多搜索', '多多场景', '多多进宝', '品牌推广']
    }
    
    # 预计算店铺商品映射
    store_products_map = {}
    for store_id in stores_df['店铺ID'].unique():
        store_products_map[store_id] = products_df[products_df['店铺ID'] == store_id]
    
    # 预生成日期列表（根据时间跨度，与订单保持一致）
    dates = [datetime.now().date() - timedelta(days=i) for i in range(time_span_days)]
    
    # 随机选择50%的店铺进行推广投放（降低推广覆盖率）
    total_stores = len(stores_df)
    promo_store_count = int(total_stores * 0.5)
    promo_stores = stores_df.sample(promo_store_count)
    
    for _, store in promo_stores.iterrows():
        store_id = store['店铺ID']
        platform = store['平台']
        store_type = store['店铺类型']
        store_products = store_products_map.get(store_id)
        
        if store_products is None or len(store_products) == 0:
            continue
        
        # 品牌店推广力度更大
        promo_intensity = 0.3 if store_type == '品牌' else 0.15  # 推广天数占比
        promo_days = random.sample(dates, int(len(dates) * promo_intensity))
        
        for date in promo_days:
            # 每天推广1-2个商品（降低推广商品数）
            num_promo_products = random.randint(1, min(2, len(store_products)))
            promo_products = store_products.sample(num_promo_products)
            
            for _, product in promo_products.iterrows():
                impressions = random.randint(3000, 15000)  # 降低曝光量
                clicks = int(impressions * random.uniform(0.015, 0.03))  # 降低点击率
                
                # 推广花费：按每次点击成本(CPC)计算
                # 大幅降低CPC，让推广费占比在5-8%（合理区间）
                if product['一级类目'].startswith('整车'):
                    cpc = random.uniform(0.8, 1.5)  # 整车类CPC: 0.8-1.5元
                else:
                    cpc = random.uniform(0.5, 1.0)  # 配件类CPC: 0.5-1元
                
                promo_cost = round(clicks * cpc, 2)
                
                # 最低推广费30元（降低最低预算）
                promo_cost = max(30, promo_cost)
                
                promotions.append({
                    '推广ID': f'PM{promo_id:08d}',
                    '日期': date,
                    '店铺ID': store_id,
                    '平台': platform,
                    '商品ID': product['商品ID'],
                    '一级类目': product['一级类目'],
                    '二级类目': product['二级类目'],
                    '推广渠道': random.choice(platform_channels.get(platform, ['通用推广'])),
                    '推广花费': promo_cost,
                    '曝光量': impressions,
                    '点击量': clicks,
                    '点击率': round(clicks / impressions * 100, 2)
                })
                promo_id += 1
    
    return pd.DataFrame(promotions)


# 生成流量数据（优化版 - 批量生成）
def generate_traffic(stores_df, time_span_days=365):
    """
    生成店铺流量数据（批量优化版）
    time_span_days: 时间跨度（天）
    """
    # 预生成日期列表（最多90天）
    dates = [datetime.now().date() - timedelta(days=i) for i in range(min(time_span_days, 90))]
    
    # 平台流量基数映射
    platform_uv_ranges = {
        '天猫': (2000, 8000),
        '淘宝': (2000, 8000),
        '京东': (1500, 6000)
    }
    default_uv_range = (800, 3000)
    
    # 批量生成
    traffic_records = []
    for _, store in stores_df.iterrows():
        store_id = store['店铺ID']
        platform = store['平台']
        uv_min, uv_max = platform_uv_ranges.get(platform, default_uv_range)
        
        # 预生成该店铺所有日期的随机数
        num_days = len(dates)
        base_uvs = np.random.randint(uv_min, uv_max, num_days)
        pv_factors = np.random.uniform(2.5, 4.5, num_days)
        search_factors = np.random.uniform(0.3, 0.5, num_days)
        recommend_factors = np.random.uniform(0.2, 0.3, num_days)
        direct_factors = np.random.uniform(0.1, 0.2, num_days)
        stay_times = np.random.uniform(60, 300, num_days)
        bounce_rates = np.random.uniform(30, 70, num_days)
        
        for i, date in enumerate(dates):
            base_uv = int(base_uvs[i])
            pv = int(base_uv * pv_factors[i])
            search_uv = int(base_uv * search_factors[i])
            recommend_uv = int(base_uv * recommend_factors[i])
            direct_uv = int(base_uv * direct_factors[i])
            other_uv = base_uv - search_uv - recommend_uv - direct_uv
            
            traffic_records.append({
                '日期': date,
                '店铺ID': store_id,
                '平台': platform,
                '访客数': base_uv,
                '浏览量': pv,
                '搜索流量': search_uv,
                '推荐流量': recommend_uv,
                '直接访问': direct_uv,
                '其他流量': other_uv,
                '平均停留时长': round(stay_times[i], 2),
                '跳失率': round(bounce_rates[i], 2)
            })
    
    return pd.DataFrame(traffic_records)


# 生成商品自然流量数据
def generate_product_traffic(stores_df, products_df, time_span_days=365):
    """
    生成商品维度的自然流量数据
    包含：曝光、点击、收藏、加购等指标
    """
    product_traffic = []
    traffic_id = 1
    
    # 预生成日期列表
    dates = [datetime.now().date() - timedelta(days=i) for i in range(time_span_days)]
    
    # 流量渠道
    channels = ['搜索', '推荐', '直接访问', '活动页', '店铺首页']
    
    # 预计算店铺商品映射
    store_products_map = {}
    for store_id in stores_df['店铺ID'].unique():
        store_products_map[store_id] = products_df[products_df['店铺ID'] == store_id]
    
    print(f"   正在生成商品自然流量数据...")
    
    for _, store in stores_df.iterrows():
        store_id = store['店铺ID']
        platform = store['平台']
        store_products = store_products_map.get(store_id)
        
        if store_products is None or len(store_products) == 0:
            continue
        
        # 每个店铺每天随机选择30-50%的商品有流量
        for date in dates:
            num_products = random.randint(int(len(store_products) * 0.3), int(len(store_products) * 0.5))
            daily_products = store_products.sample(min(num_products, len(store_products)))
            
            for _, product in daily_products.iterrows():
                channel = random.choice(channels)
                
                # 根据商品类型设置流量基数
                if product['一级类目'].startswith('整车'):
                    impressions = random.randint(100, 500)  # 整车曝光量
                else:
                    impressions = random.randint(50, 200)   # 配件曝光量
                
                clicks = int(impressions * random.uniform(0.05, 0.15))  # 点击率5-15%
                favorites = int(clicks * random.uniform(0.1, 0.3))      # 收藏率10-30%
                add_to_cart = int(clicks * random.uniform(0.15, 0.35))  # 加购率15-35%
                
                product_traffic.append({
                    '流量ID': f'TF{traffic_id:08d}',
                    '日期': date,
                    '店铺ID': store_id,
                    '平台': platform,
                    '商品ID': product['商品ID'],
                    '一级类目': product['一级类目'],
                    '二级类目': product['二级类目'],
                    '流量渠道': channel,
                    '曝光量': impressions,
                    '点击量': clicks,
                    '收藏量': favorites,
                    '加购量': add_to_cart,
                    '点击率': round(clicks / impressions * 100, 2) if impressions > 0 else 0
                })
                traffic_id += 1
    
    return pd.DataFrame(product_traffic)


# 生成库存变动数据（批量优化版）
def generate_inventory(products_df, time_span_days=365):
    """
    生成库存变动记录（批量优化）
    time_span_days: 时间跨度（天）
    """
    # 只为部分商品生成库存记录
    sample_size = min(len(products_df), 200)
    sample_products = products_df.sample(sample_size)
    
    # 预生成常量（最多30天）
    dates = [datetime.now().date() - timedelta(days=i) for i in range(min(time_span_days, 30))]
    num_days = len(dates)
    remarks = ['正常销售', '补货', '退货', '盘点调整']
    
    inventory_records = []
    inv_id = 1
    
    for _, product in sample_products.iterrows():
        current_stock = product['库存']
        sku_id = product['SKU_ID']
        product_id = product['商品ID']
        store_id = product['店铺ID']
        
        # 批量生成随机数
        change_type_choices = np.random.choice(['入库', '出库', '出库'], num_days)
        in_quantities = np.random.randint(20, 80, num_days)
        out_quantities = np.random.randint(5, 15, num_days)
        remark_choices = np.random.choice(remarks, num_days)
        
        for i, date in enumerate(dates):
            change_type = change_type_choices[i]
            
            if change_type == '入库':
                quantity = int(in_quantities[i])
                current_stock += quantity
            else:
                quantity = int(out_quantities[i])
                current_stock = max(10, current_stock - quantity)
            
            inventory_records.append({
                '库存记录ID': f'INV{inv_id:08d}',
                '日期': date,
                'SKU_ID': sku_id,
                '商品ID': product_id,
                '店铺ID': store_id,
                '变动类型': change_type,
                '变动数量': quantity,
                '变动后库存': current_stock,
                '备注': remark_choices[i]
            })
            inv_id += 1
    
    return pd.DataFrame(inventory_records)


# 主函数
def main():
    # 读取配置参数
    config = {}
    
    # 优先从命令行参数读取
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    # 如果命令行没有配置，尝试从config.json读取
    if not config:
        config_file = os.path.join(BASE_DIR, 'config.json')
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            except:
                pass
    
    # 店铺配置：每个平台分品牌店和白牌店
    platform_stores_raw = config.get('platformStores', {
        '京东': {
            '品牌': ['品牌旗舰店'],
            '白牌': ['白牌专营店1号', '白牌专营店2号']
        },
        '天猫': {
            '品牌': ['品牌旗舰店'],
            '白牌': ['白牌专营店1号', '白牌专营店2号']
        },
        '抖音': {
            '品牌': ['品牌旗舰店'],
            '白牌': ['白牌专营店1号', '白牌专营店2号']
        },
        '快手': {
            '品牌': ['品牌旗舰店'],
            '白牌': ['白牌专营店1号', '白牌专营店2号']
        },
        '拼多多': {
            '品牌': [],
            '白牌': ['白牌专营店1号', '白牌专营店2号', '白牌专营店3号']
        }
    })
    
    # 兼容旧格式：如果传入的是列表格式，转换为新格式
    platform_stores = {}
    for platform, stores in platform_stores_raw.items():
        if isinstance(stores, list):
            # 旧格式：列表，全部作为白牌店
            platform_stores[platform] = {
                '品牌': [],
                '白牌': stores
            }
        else:
            # 新格式：字典
            platform_stores[platform] = stores
    
    # 新逻辑：企业体量驱动
    from business_scale import get_scale_config, calculate_traffic_from_scale, get_scale_summary
    
    business_scale = config.get('businessScale', '小型企业')  # 企业体量
    time_span_days = config.get('timeSpanDays', 365)  # 时间跨度
    
    # 计算店铺数量（根据配置或体量范围）
    scale_config = get_scale_config(business_scale)
    total_stores = sum(len(v.get('品牌', [])) + len(v.get('白牌', [])) for v in platform_stores.values())
    
    # 获取体量摘要
    scale_summary = get_scale_summary(business_scale, total_stores, time_span_days)
    
    # 根据流量估算用户数（假设每个用户平均访问10次）
    num_users = max(100, int(scale_summary['total_clicks'] / 10))
    
    main_category = config.get('mainCategory', 'bicycle')
    
    # 获取类目配置
    category_config = CATEGORY_CONFIGS.get(main_category, CATEGORY_CONFIGS['bicycle'])
    
    print("="*60)
    print("数据生成配置（企业体量驱动）")
    print("="*60)
    print(f"企业体量: {business_scale}")
    print(f"体量描述: {scale_config['description']}")
    print(f"主营类目: {category_config['name']}")
    print(f"平台店铺: {len(platform_stores)} 个平台, {total_stores} 家店铺")
    print(f"时间跨度: {time_span_days} 天")
    print(f"\n【流量预估】")
    print(f"  总曝光量: {scale_summary['total_impressions']:,}")
    print(f"  总点击量: {scale_summary['total_clicks']:,}")
    print(f"  日均流量: {scale_summary['daily_traffic']:,}")
    print(f"  每店日均: {scale_summary['daily_per_store']:,}")
    print(f"\n【订单预估】")
    print(f"  预估订单: {scale_summary['estimated_orders']:,} 单")
    print(f"  预估GMV: {scale_summary['estimated_gmv']/10000:.1f} 万元")
    print(f"  月均GMV: {scale_summary['monthly_gmv']/10000:.1f} 万元")
    print(f"\n【用户预估】")
    print(f"  用户数量: {num_users:,} 个（自动计算）")
    print("="*60)
    sys.stdout.flush()
    
    # 生成各类数据
    print("\n1. 生成店铺数据...")
    stores_df = generate_stores(platform_stores)
    print(f"   ✓ 生成 {len(stores_df)} 条店铺数据")
    sys.stdout.flush()
    
    print("\n2. 生成商品数据...")
    products_df = generate_products(stores_df, category_config)
    print(f"   ✓ 生成 {len(products_df)} 条商品数据")
    sys.stdout.flush()
    
    # ========== 新逻辑：统一流量分发 → 转化 → 订单 ==========
    print("\n3. 【统一流量分发】根据企业体量分配流量（自然+付费）...")
    from traffic_distribution import TrafficDistributor
    
    # 使用企业体量的流量基数
    traffic_base = scale_summary['daily_per_store']
    distributor = TrafficDistributor(products_df, time_span_days, traffic_base=traffic_base)
    
    # 启用多进程模式
    all_traffic_df = distributor.distribute_traffic(use_multiprocess=True)
    
    actual_impressions = all_traffic_df['曝光量'].sum()
    actual_clicks = all_traffic_df['点击量'].sum()
    print(f"   ✓ 生成 {len(all_traffic_df):,} 条流量记录")
    print(f"   ✓ 总曝光: {actual_impressions:,}, 总点击: {actual_clicks:,}")
    sys.stdout.flush()
    
    # 先生成临时用户数据用于订单生成
    print(f"\n4. 生成用户数据（{num_users:,} 个）...")
    users_df = generate_users(num_users, time_span_days)
    print(f"   ✓ 生成 {len(users_df):,} 条用户数据")
    sys.stdout.flush()
    
    print("\n5. 【流量转化】根据流量数据生成订单...")
    from conversion_engine import ConversionEngine
    engine = ConversionEngine(all_traffic_df, products_df, users_df, stores_df)
    
    # 使用预估订单数，启用多进程模式
    target_orders = scale_summary['estimated_orders']
    orders_df, order_details_df = engine.generate_orders_from_traffic(target_orders, use_multiprocess=True)
    
    actual_orders = len(orders_df)
    actual_cvr = actual_orders / actual_clicks * 100 if actual_clicks > 0 else 0
    print(f"   ✓ 生成 {actual_orders:,} 条订单数据（转化率: {actual_cvr:.2f}%）")
    print(f"   ✓ 生成 {len(order_details_df):,} 条订单明细数据")
    sys.stdout.flush()
    
    # 根据实际订单数重新生成用户数据
    actual_user_count = orders_df['用户ID'].nunique()
    print(f"\n6. 根据实际订单重新生成用户数据（{actual_user_count:,} 个）...")
    users_df = generate_users(actual_user_count, time_span_days)
    # 使用订单中的实际用户ID
    actual_user_ids = sorted(orders_df['用户ID'].unique())
    users_df['用户ID'] = actual_user_ids[:len(users_df)]
    print(f"   ✓ 生成 {len(users_df):,} 条用户数据（基于实际订单）")
    sys.stdout.flush()
    
    # 拆分流量数据为：付费推广表 + 商品自然流量表
    print("\n7. 拆分流量数据...")
    promotion_df = all_traffic_df[all_traffic_df['流量类型'] == '付费'].copy()
    promotion_df = promotion_df.rename(columns={
        '推广费用': '推广花费',
        '流量渠道': '推广渠道'
    })
    promotion_df['推广ID'] = [f'PM{i:08d}' for i in range(1, len(promotion_df) + 1)]
    promotion_df = promotion_df[['推广ID', '日期', '店铺ID', '平台', 'SKU_ID', '商品ID', '一级类目', '二级类目', '推广渠道', '推广花费', '曝光量', '点击量', '点击率']]
    
    product_traffic_df = all_traffic_df[all_traffic_df['流量类型'] == '自然'].copy()
    product_traffic_df['流量ID'] = [f'TF{i:08d}' for i in range(1, len(product_traffic_df) + 1)]
    product_traffic_df['收藏量'] = (product_traffic_df['点击量'] * np.random.uniform(0.1, 0.3, len(product_traffic_df))).astype(int)
    product_traffic_df['加购量'] = (product_traffic_df['点击量'] * np.random.uniform(0.15, 0.35, len(product_traffic_df))).astype(int)
    product_traffic_df = product_traffic_df[['流量ID', '日期', '店铺ID', '平台', 'SKU_ID', '商品ID', '一级类目', '二级类目', '流量渠道', '曝光量', '点击量', '收藏量', '加购量', '点击率']]
    
    print(f"   ✓ 付费推广: {len(promotion_df):,} 条")
    print(f"   ✓ 自然流量: {len(product_traffic_df):,} 条")
    sys.stdout.flush()
    
    print("\n8. 生成店铺流量数据...")
    traffic_df = generate_traffic(stores_df, time_span_days)
    print(f"   ✓ 生成 {len(traffic_df):,} 条店铺流量数据")
    sys.stdout.flush()
    
    print("\n9. 生成库存数据...")
    inventory_df = generate_inventory(products_df, time_span_days)
    print(f"   ✓ 生成 {len(inventory_df):,} 条库存数据")
    sys.stdout.flush()
    
    # 计算实际GMV
    actual_gmv = orders_df[orders_df['订单状态'] == '已完成']['实付金额'].sum()
    actual_monthly_gmv = actual_gmv / (time_span_days / 30)
    
    print("\n" + "="*60)
    print("✓ 数据生成完成（企业体量驱动模型）")
    print("="*60)
    print(f"【流量数据】")
    print(f"  流量记录: {len(all_traffic_df):,} 条")
    print(f"  - 付费推广: {len(promotion_df):,} 条")
    print(f"  - 自然流量: {len(product_traffic_df):,} 条")
    print(f"  总曝光: {actual_impressions:,}, 总点击: {actual_clicks:,}")
    print(f"\n【订单数据】")
    print(f"  订单数: {actual_orders:,} 条（从流量转化）")
    print(f"  订单明细: {len(order_details_df):,} 条")
    print(f"  转化率: {actual_cvr:.2f}%")
    print(f"\n【GMV数据】")
    print(f"  总GMV: {actual_gmv/10000:.1f} 万元")
    print(f"  月均GMV: {actual_monthly_gmv/10000:.1f} 万元")
    print(f"  客单价: {actual_gmv/actual_orders:.0f} 元" if actual_orders > 0 else "  客单价: 0 元")
    print("="*60)
    sys.stdout.flush()
    
    print("\n" + "="*60)
    print("✓ 数据生成完成，开始导入数据库...")
    print("="*60)
    sys.stdout.flush()
    
    # 直接导入数据库
    from load_to_database import load_dataframes_to_db
    
    # 获取数据库配置
    db_config = config.get('dbConfig', {
        'host': 'localhost',
        'port': 3306,
        'database': 'datas',
        'user': 'root',
        'password': ''
    })
    
    # 可选：保存CSV文件（用于备份或调试）
    save_csv = config.get('saveCsv', True)  # 默认保存
    if save_csv:
        print("\n保存CSV文件...")
        users_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_users.csv'), index=False, encoding='utf-8-sig')
        orders_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_orders.csv'), index=False, encoding='utf-8-sig')
        order_details_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_order_details.csv'), index=False, encoding='utf-8-sig')
        promotion_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_promotion.csv'), index=False, encoding='utf-8-sig')
        traffic_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_traffic.csv'), index=False, encoding='utf-8-sig')
        product_traffic_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_product_traffic.csv'), index=False, encoding='utf-8-sig')
        inventory_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_inventory.csv'), index=False, encoding='utf-8-sig')
        print("   ✓ CSV文件已保存")
        sys.stdout.flush()
    
    # 准备数据字典
    dataframes = {
        'stores': stores_df,
        'products': products_df,
        'users': users_df,
        'orders': orders_df,
        'order_details': order_details_df,
        'promotion': promotion_df,
        'traffic': traffic_df,
        'product_traffic': product_traffic_df,
        'inventory': inventory_df
    }
    
    # 导入数据库
    success = load_dataframes_to_db(dataframes, mode='full', db_config=db_config)
    
    if success:
        print("\n" + "="*60)
        print("✓ 数据生成并导入完成！")
        print("="*60)
        print(f"总订单数: {actual_orders:,} 条")
        print(f"总用户数: {num_users:,} 个")
        print(f"时间跨度: {time_span_days} 天")
        print("="*60)
    else:
        print("\n✗ 数据导入失败")
    sys.stdout.flush()
    


if __name__ == '__main__':
    main()
