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
import json
from faker import Faker

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
            '整车': ['公路车', '山地车', '折叠车', '电动车', '儿童车'],
            '骑行装备': ['头盔', '手套', '骑行服', '骑行裤', '骑行鞋', '眼镜', '水壶', '车灯', '车锁', '码表']
        },
        'price_ranges': {
            '整车': (800, 15000),
            '骑行装备': (50, 800)
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

# 生成店铺信息
def generate_stores(platform_stores):
    """
    根据配置生成店铺信息
    platform_stores: {平台名: [店铺名称列表]}
    """
    stores = []
    store_id = 1
    for platform, store_names in platform_stores.items():
        for store_name in store_names:
            stores.append({
                '店铺ID': f'S{store_id:04d}',
                '店铺名称': store_name,
                '平台': platform,
                '开店日期': fake.date_between(start_date='-3y', end_date='-1y')
            })
            store_id += 1
    return pd.DataFrame(stores)

# 生成商品信息（支持多类目）
def generate_products(stores_df, category_config):
    products = []
    product_id = 1
    categories = category_config['categories']
    price_ranges = category_config['price_ranges']
    
    for _, store in stores_df.iterrows():
        # 每个店铺15-20个商品
        num_products = random.randint(15, 20)
        
        for _ in range(num_products):
            main_cat = random.choice(list(categories.keys()))
            sub_cat = random.choice(categories[main_cat])
            
            # 根据类目设置价格
            price_min, price_max = price_ranges.get(main_cat, (50, 500))
            price = round(random.uniform(price_min, price_max), 2)
            
            # 成本率根据类目不同
            if main_cat in ['整车', '手机', '电脑', '家具']:
                cost_rate = random.uniform(0.6, 0.8)  # 高价商品成本率高
            else:
                cost_rate = random.uniform(0.3, 0.6)  # 低价商品成本率低
            
            products.append({
                '商品ID': f'P{product_id:06d}',
                '店铺ID': store['店铺ID'],
                '平台': store['平台'],
                '商品名称': f'{sub_cat}-{product_id}',
                '一级类目': main_cat,
                '二级类目': sub_cat,
                '售价': price,
                '成本': round(price * cost_rate, 2),
                '库存': random.randint(50, 300),
                '创建时间': store['开店日期']
            })
            product_id += 1
    
    return pd.DataFrame(products)

# 生成用户信息（优化版）
def generate_users(num_users=3000):
    users = []
    cities = [fake.city() for _ in range(50)]  # 预生成城市列表
    
    for i in range(1, num_users + 1):
        users.append({
            '用户ID': f'U{i:08d}',
            '用户名': f'用户{i}',
            '性别': random.choice(['男', '女']),
            '年龄': random.randint(18, 65),
            '城市': random.choice(cities),
            '注册日期': fake.date_between(start_date='-2y', end_date='-1m')
        })
    return pd.DataFrame(users)

# 生成订单数据
def generate_orders(stores_df, products_df, users_df, num_orders=50000):
    orders = []
    order_details = []
    
    # 获取实际存在的平台
    platforms = stores_df['平台'].unique()
    
    # 按店铺数量分配订单权重
    platform_store_counts = stores_df.groupby('平台').size()
    total_stores = platform_store_counts.sum()
    
    # 为每个平台分配订单
    platform_orders = {}
    for platform in platforms:
        weight = platform_store_counts[platform] / total_stores
        platform_orders[platform] = int(num_orders * weight)
    
    # 调整总数确保等于num_orders
    total_allocated = sum(platform_orders.values())
    if total_allocated < num_orders:
        # 将剩余订单分配给第一个平台
        first_platform = list(platform_orders.keys())[0]
        platform_orders[first_platform] += num_orders - total_allocated
    
    order_id = 1
    
    for platform, order_count in platform_orders.items():
        # 获取该平台的店铺
        platform_stores = stores_df[stores_df['平台'] == platform]
        
        if len(platform_stores) == 0:
            continue
        
        for _ in range(order_count):
            # 从该平台随机选择店铺
            store = platform_stores.sample(1).iloc[0]
            user = users_df.sample(1).iloc[0]
            
            # 该店铺的商品
            store_products = products_df[products_df['店铺ID'] == store['店铺ID']]
            
            if len(store_products) == 0:
                continue
            
            # 订单基本信息
            order_time = fake.date_time_between(start_date='-1y', end_date='now')
            order_status = random.choices(
                ['已完成', '已取消', '退款'],
                weights=[0.85, 0.10, 0.05]
            )[0]
            
            # 订单包含1-3个商品
            num_items = random.randint(1, 3)
            selected_products = store_products.sample(min(num_items, len(store_products)))
            
            total_amount = 0
            total_cost = 0
            
            for _, product in selected_products.iterrows():
                quantity = random.randint(1, 3)
                item_amount = product['售价'] * quantity
                item_cost = product['成本'] * quantity
                
                total_amount += item_amount
                total_cost += item_cost
                
                order_details.append({
                    '订单明细ID': f'OD{len(order_details) + 1:08d}',
                    '订单ID': f'O{order_id:08d}',
                    '商品ID': product['商品ID'],
                    '数量': quantity,
                    '单价': product['售价'],
                    '金额': round(item_amount, 2)
                })
            
            # 优惠和运费
            discount = round(total_amount * random.uniform(0, 0.15), 2) if random.random() < 0.3 else 0
            shipping_fee = 0 if total_amount > 99 else round(random.uniform(5, 15), 2)
            final_amount = round(total_amount - discount + shipping_fee, 2)
            
            orders.append({
                '订单ID': f'O{order_id:08d}',
                '用户ID': user['用户ID'],
                '店铺ID': store['店铺ID'],
                '平台': store['平台'],
                '下单时间': order_time,
                '订单状态': order_status,
                '商品总额': round(total_amount, 2),
                '优惠金额': discount,
                '运费': shipping_fee,
                '实付金额': final_amount if order_status == '已完成' else 0,
                '成本总额': round(total_cost, 2) if order_status == '已完成' else 0,
                '支付方式': random.choice(['支付宝', '微信', '银行卡']),
                '创建时间': order_time,
                '更新时间': order_time + timedelta(days=random.randint(0, 7))
            })
            
            order_id += 1
    
    return pd.DataFrame(orders), pd.DataFrame(order_details)

# 生成推广数据（优化版）
def generate_promotion(stores_df, products_df):
    """生成推广投放数据"""
    promotions = []
    promo_id = 1
    channels = ['直通车', '钻展', '超级推荐', '品牌广告']
    
    # 减少到30天
    for _, store in stores_df.iterrows():
        store_products = products_df[products_df['店铺ID'] == store['店铺ID']]
        
        if len(store_products) == 0:
            continue
        
        for days_ago in range(30):  # 从90改为30天
            date = datetime.now().date() - timedelta(days=days_ago)
            
            # 每天推广1-3个商品
            num_promo_products = random.randint(1, 3)
            promo_products = store_products.sample(min(num_promo_products, len(store_products)))
            
            for _, product in promo_products.iterrows():
                impressions = random.randint(5000, 30000)
                clicks = int(impressions * random.uniform(0.02, 0.04))
                
                promotions.append({
                    '推广ID': f'PM{promo_id:08d}',
                    '日期': date,
                    '店铺ID': store['店铺ID'],
                    '平台': store['平台'],
                    '商品ID': product['商品ID'],
                    '推广渠道': random.choice(channels),
                    '推广花费': round(random.uniform(100, 400), 2),
                    '曝光量': impressions,
                    '点击量': clicks,
                    '点击率': round(clicks / impressions * 100, 2)
                })
                promo_id += 1
    
    return pd.DataFrame(promotions)


# 生成流量数据（优化版）
def generate_traffic(stores_df):
    """生成店铺流量数据"""
    traffic = []
    
    for _, store in stores_df.iterrows():
        for days_ago in range(30):  # 从90改为30天
            date = datetime.now().date() - timedelta(days=days_ago)
            
            if store['平台'] in ['天猫', '淘宝']:
                base_uv = random.randint(2000, 8000)
            elif store['平台'] == '京东':
                base_uv = random.randint(1500, 6000)
            else:
                base_uv = random.randint(800, 3000)
            
            pv = int(base_uv * random.uniform(2.5, 4.5))
            search_uv = int(base_uv * random.uniform(0.3, 0.5))
            recommend_uv = int(base_uv * random.uniform(0.2, 0.3))
            direct_uv = int(base_uv * random.uniform(0.1, 0.2))
            other_uv = base_uv - search_uv - recommend_uv - direct_uv
            
            traffic.append({
                '日期': date,
                '店铺ID': store['店铺ID'],
                '平台': store['平台'],
                '访客数': base_uv,
                '浏览量': pv,
                '搜索流量': search_uv,
                '推荐流量': recommend_uv,
                '直接访问': direct_uv,
                '其他流量': other_uv,
                '平均停留时长': round(random.uniform(60, 300), 2),
                '跳失率': round(random.uniform(30, 70), 2)
            })
    
    return pd.DataFrame(traffic)


# 生成库存变动数据（优化版）
def generate_inventory(products_df):
    """生成库存变动记录"""
    inventory = []
    inv_id = 1
    
    # 只为部分商品生成库存记录（减少数据量）
    sample_products = products_df.sample(min(len(products_df), 200))
    
    for _, product in sample_products.iterrows():
        current_stock = product['库存']
        
        # 最近15天的库存变动（从30改为15）
        for days_ago in range(15):
            date = datetime.now().date() - timedelta(days=days_ago)
            
            # 入库/出库
            change_type = random.choice(['入库', '出库', '出库'])
            
            if change_type == '入库':
                quantity = random.randint(20, 80)
                current_stock += quantity
            else:
                quantity = random.randint(5, 15)
                current_stock = max(10, current_stock - quantity)
            
            inventory.append({
                '库存记录ID': f'INV{inv_id:08d}',
                '日期': date,
                '商品ID': product['商品ID'],
                '店铺ID': product['店铺ID'],
                '变动类型': change_type,
                '变动数量': quantity,
                '变动后库存': current_stock,
                '备注': random.choice(['正常销售', '补货', '退货', '盘点调整'])
            })
            inv_id += 1
    
    return pd.DataFrame(inventory)


# 主函数
def main():
    # 读取配置参数
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    platform_stores = config.get('platformStores', {
        '京东': ['京东旗舰店1号', '京东旗舰店2号', '京东旗舰店3号', '京东旗舰店4号'],
        '天猫': ['天猫旗舰店1号', '天猫旗舰店2号', '天猫旗舰店3号', '天猫旗舰店4号'],
        '抖音': ['抖音旗舰店1号', '抖音旗舰店2号', '抖音旗舰店3号', '抖音旗舰店4号'],
        '拼多多': ['拼多多旗舰店1号', '拼多多旗舰店2号', '拼多多旗舰店3号', '拼多多旗舰店4号']
    })
    num_users = config.get('numUsers', 3000)
    num_orders = config.get('numOrders', 20000)
    main_category = config.get('mainCategory', 'bicycle')
    
    # 获取类目配置
    category_config = CATEGORY_CONFIGS.get(main_category, CATEGORY_CONFIGS['bicycle'])
    
    print("开始生成数据...")
    print(f"主营类目: {category_config['name']}")
    print(f"配置: {len(platform_stores)} 个平台, {sum(len(v) for v in platform_stores.values())} 家店铺")
    
    # 生成各类数据
    print("1. 生成店铺数据...")
    stores_df = generate_stores(platform_stores)
    stores_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_stores.csv'), index=False, encoding='utf-8-sig')
    print(f"   生成 {len(stores_df)} 条店铺数据")
    
    print("2. 生成商品数据...")
    products_df = generate_products(stores_df, category_config)
    products_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_products.csv'), index=False, encoding='utf-8-sig')
    print(f"   生成 {len(products_df)} 条商品数据")
    
    print("3. 生成用户数据...")
    users_df = generate_users(num_users)
    users_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_users.csv'), index=False, encoding='utf-8-sig')
    print(f"   生成 {len(users_df)} 条用户数据")
    
    print("4. 生成订单数据...")
    orders_df, order_details_df = generate_orders(stores_df, products_df, users_df, num_orders)
    orders_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_orders.csv'), index=False, encoding='utf-8-sig')
    order_details_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_order_details.csv'), index=False, encoding='utf-8-sig')
    print(f"   生成 {len(orders_df)} 条订单数据")
    print(f"   生成 {len(order_details_df)} 条订单明细数据")
    
    print("5. 生成推广数据...")
    promotion_df = generate_promotion(stores_df, products_df)
    promotion_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_promotion.csv'), index=False, encoding='utf-8-sig')
    print(f"   生成 {len(promotion_df)} 条推广数据")
    
    print("6. 生成流量数据...")
    traffic_df = generate_traffic(stores_df)
    traffic_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_traffic.csv'), index=False, encoding='utf-8-sig')
    print(f"   生成 {len(traffic_df)} 条流量数据")
    
    print("7. 生成库存数据...")
    inventory_df = generate_inventory(products_df)
    inventory_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_inventory.csv'), index=False, encoding='utf-8-sig')
    print(f"   生成 {len(inventory_df)} 条库存数据")
    
    print("\n数据生成完成！")
    print(f"数据保存在: {os.path.join(DATA_DIR, 'ods/')}")

if __name__ == '__main__':
    main()
