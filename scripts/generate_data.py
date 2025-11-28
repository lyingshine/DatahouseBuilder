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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing
import csv
import time

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

# 生成商品信息（优化版 - 批量生成）
def generate_products(stores_df, category_config):
    """
    生成商品数据
    商品ID即为SKU编码，全局唯一
    """
    products = []
    product_id = 1
    categories = category_config['categories']
    price_ranges = category_config['price_ranges']
    
    # 预生成类目列表
    main_cats = list(categories.keys())
    high_value_cats = {'整车', '手机', '电脑', '家具'}
    
    for _, store in stores_df.iterrows():
        # 每个店铺15-20个商品
        num_products = random.randint(15, 20)
        store_id = store['店铺ID']
        platform = store['平台']
        open_date = store['开店日期']
        
        for _ in range(num_products):
            main_cat = random.choice(main_cats)
            sub_cat = random.choice(categories[main_cat])
            
            # 根据类目设置价格
            price_min, price_max = price_ranges.get(main_cat, (50, 500))
            price = round(random.uniform(price_min, price_max), 2)
            
            # 成本率根据类目不同
            cost_rate = random.uniform(0.6, 0.8) if main_cat in high_value_cats else random.uniform(0.3, 0.6)
            
            # 商品ID作为唯一SKU编码（格式：P + 6位数字）
            sku_code = f'P{product_id:06d}'
            
            products.append({
                '商品ID': sku_code,  # SKU编码，全局唯一
                '店铺ID': store_id,
                '平台': platform,
                '商品名称': f'{sub_cat}-{product_id}',
                '一级类目': main_cat,
                '二级类目': sub_cat,
                '售价': price,
                '成本': round(price * cost_rate, 2),
                '库存': random.randint(50, 300),
                '创建时间': open_date
            })
            product_id += 1
    
    return pd.DataFrame(products)

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
    status_weights = [0.85, 0.10, 0.05]
    payment_methods = ['支付宝', '微信', '银行卡']
    
    # 预生成随机索引
    store_indices = np.random.randint(0, len(stores_list), batch_size)
    user_indices = np.random.randint(0, len(users_list), batch_size)
    
    # 【性能优化1】预生成所有订单时间
    now = datetime.now()
    time_deltas = np.random.randint(0, time_span_days * 24 * 3600, batch_size)
    order_times = [now - timedelta(seconds=int(delta)) for delta in time_deltas]
    
    # 【性能优化2】预生成所有随机数
    order_status_choices = np.random.choice(order_statuses, size=batch_size, p=status_weights)
    payment_method_choices = np.random.choice(payment_methods, size=batch_size)
    discount_flags = np.random.random(batch_size) < 0.3
    discount_rates = np.random.uniform(0, 0.15, batch_size)
    shipping_fees = np.random.uniform(5, 15, batch_size)
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
        
        # 直接从列表获取商品
        for idx in product_indices:
            product = store_products[idx]
            quantity = random.randint(1, 3)
            item_amount = product['售价'] * quantity
            item_cost = product['成本'] * quantity
            
            total_amount += item_amount
            total_cost += item_cost
            
            order_details.append([
                f'OD{detail_id:08d}',
                f'O{order_id:08d}',
                product['商品ID'],
                quantity,
                product['售价'],
                round(item_amount, 2)
            ])
            detail_id += 1
        
        discount = round(total_amount * discount_rates[i], 2) if discount_flags[i] else 0
        shipping_fee = 0 if total_amount > 99 else round(shipping_fees[i], 2)
        final_amount = round(total_amount - discount + shipping_fee, 2)
        
        orders.append([
            f'O{order_id:08d}',
            user['用户ID'],
            store['店铺ID'],
            store['平台'],
            order_time,
            order_status,
            round(total_amount, 2),
            discount,
            shipping_fee,
            final_amount if order_status == '已完成' else 0,
            round(total_cost, 2) if order_status == '已完成' else 0,
            payment_method_choices[i],
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
    
    # 判断是否使用多进程（订单数超过10万时启用）
    use_multithread = num_orders > 100000
    
    if use_multithread:
        # 使用所有可用 CPU 核心（不限制）
        num_threads = multiprocessing.cpu_count()
        print(f"   使用多进程模式（{num_threads} 进程，充分利用 CPU）...")
    
    sys.stdout.flush()
    
    # 预先计算每个店铺的商品列表
    # 转换为列表
    stores_list = stores_df.to_dict('records')
    users_list = users_df.to_dict('records')
    
    if use_multithread:
        # 多进程模式：将 DataFrame 转换为可序列化的字典列表
        store_products_dict = {}
        for store_id in stores_df['店铺ID'].unique():
            prods = products_df[products_df['店铺ID'] == store_id]
            if len(prods) > 0:
                # 转换为字典列表（可序列化）
                store_products_dict[store_id] = prods.to_dict('records')
    else:
        # 单进程模式：使用 DataFrame（更快）
        store_products_map = {}
        for store_id in stores_df['店铺ID'].unique():
            prods = products_df[products_df['店铺ID'] == store_id]
            if len(prods) > 0:
                store_products_map[store_id] = prods
    
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
        
    else:
        # 单线程模式（订单数较少时）
        orders = []
        order_details = []
        
        order_statuses = ['已完成', '已取消', '退款']
        status_weights = [0.85, 0.10, 0.05]
        payment_methods = ['支付宝', '微信', '银行卡']
        
        store_indices = np.random.randint(0, len(stores_list), num_orders)
        user_indices = np.random.randint(0, len(users_list), num_orders)
        
        order_id = 1
        detail_id = 1
        progress_step = max(1, num_orders // 20)
        
        start_date = f'-{time_span_days}d'
        end_date = 'now'
        
        # 【性能优化】预生成所有订单时间和随机数
        now = datetime.now()
        time_deltas = np.random.randint(0, time_span_days * 24 * 3600, num_orders)
        order_times = [now - timedelta(seconds=int(delta)) for delta in time_deltas]
        
        order_status_choices = np.random.choice(order_statuses, size=num_orders, p=status_weights)
        payment_method_choices = np.random.choice(payment_methods, size=num_orders)
        discount_flags = np.random.random(num_orders) < 0.3
        discount_rates = np.random.uniform(0, 0.15, num_orders)
        shipping_fees = np.random.uniform(5, 15, num_orders)
        update_days = np.random.randint(0, 8, num_orders)
        
        for i in range(num_orders):
            store = stores_list[store_indices[i]]
            user = users_list[user_indices[i]]
            
            store_products = store_products_map.get(store['店铺ID'])
            if store_products is None or len(store_products) == 0:
                continue
            
            order_time = order_times[i]
            order_status = order_status_choices[i]
            
            num_items = random.randint(1, min(3, len(store_products)))
            product_indices = np.random.choice(len(store_products), size=num_items, replace=False)
            selected_products = store_products.iloc[product_indices]
            
            total_amount = 0
            total_cost = 0
            
            # 使用 itertuples 代替 iterrows（快 10 倍）
            for product in selected_products.itertuples():
                quantity = random.randint(1, 3)
                item_amount = product.售价 * quantity
                item_cost = product.成本 * quantity
                
                total_amount += item_amount
                total_cost += item_cost
                
                order_details.append([
                    f'OD{detail_id:08d}',
                    f'O{order_id:08d}',
                    product.商品ID,
                    quantity,
                    product.售价,
                    round(item_amount, 2)
                ])
                detail_id += 1
            
            discount = round(total_amount * discount_rates[i], 2) if discount_flags[i] else 0
            shipping_fee = 0 if total_amount > 99 else round(shipping_fees[i], 2)
            final_amount = round(total_amount - discount + shipping_fee, 2)
            
            orders.append([
                f'O{order_id:08d}',
                user['用户ID'],
                store['店铺ID'],
                store['平台'],
                order_time,
                order_status,
                round(total_amount, 2),
                discount,
                shipping_fee,
                final_amount if order_status == '已完成' else 0,
                round(total_cost, 2) if order_status == '已完成' else 0,
                payment_method_choices[i],
                order_time,
                order_time + timedelta(days=int(update_days[i]))
            ])
            
            order_id += 1
            
            if i > 0 and i % progress_step == 0:
                progress = int((i / num_orders) * 100)
                print(f"   进度: {progress}% ({i:,}/{num_orders:,})")
                sys.stdout.flush()
        
        print(f"   进度: 100% ({num_orders:,}/{num_orders:,})")
        sys.stdout.flush()
    
    print("   正在创建DataFrame...")
    sys.stdout.flush()
    
    # 使用列表创建DataFrame（比字典快很多）
    # 优化：指定数据类型以减少内存占用
    orders_df = pd.DataFrame(orders, columns=[
        '订单ID', '用户ID', '店铺ID', '平台', '下单时间', '订单状态',
        '商品总额', '优惠金额', '运费', '实付金额', '成本总额', 
        '支付方式', '创建时间', '更新时间'
    ])
    
    # 优化数据类型以减少内存
    orders_df['订单ID'] = orders_df['订单ID'].astype('string')
    orders_df['用户ID'] = orders_df['用户ID'].astype('string')
    orders_df['店铺ID'] = orders_df['店铺ID'].astype('string')
    orders_df['平台'] = orders_df['平台'].astype('category')
    orders_df['订单状态'] = orders_df['订单状态'].astype('category')
    orders_df['支付方式'] = orders_df['支付方式'].astype('category')
    
    order_details_df = pd.DataFrame(order_details, columns=[
        '订单明细ID', '订单ID', '商品ID', '数量', '单价', '金额'
    ])
    
    # 优化数据类型
    order_details_df['订单明细ID'] = order_details_df['订单明细ID'].astype('string')
    order_details_df['订单ID'] = order_details_df['订单ID'].astype('string')
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
    channels = ['直通车', '钻展', '超级推荐', '品牌广告']
    
    # 预计算店铺商品映射
    store_products_map = {}
    for store_id in stores_df['店铺ID'].unique():
        store_products_map[store_id] = products_df[products_df['店铺ID'] == store_id]
    
    # 预生成日期列表（根据时间跨度）
    dates = [datetime.now().date() - timedelta(days=i) for i in range(min(time_span_days, 90))]  # 最多90天推广数据
    
    for _, store in stores_df.iterrows():
        store_id = store['店铺ID']
        platform = store['平台']
        store_products = store_products_map.get(store_id)
        
        if store_products is None or len(store_products) == 0:
            continue
        
        for date in dates:
            # 每天推广1-3个商品
            num_promo_products = random.randint(1, min(3, len(store_products)))
            promo_products = store_products.sample(num_promo_products)
            
            for _, product in promo_products.iterrows():
                impressions = random.randint(5000, 30000)
                clicks = int(impressions * random.uniform(0.02, 0.04))
                
                promotions.append({
                    '推广ID': f'PM{promo_id:08d}',
                    '日期': date,
                    '店铺ID': store_id,
                    '平台': platform,
                    '商品ID': product['商品ID'],
                    '推广渠道': random.choice(channels),
                    '推广花费': round(random.uniform(100, 400), 2),
                    '曝光量': impressions,
                    '点击量': clicks,
                    '点击率': round(clicks / impressions * 100, 2)
                })
                promo_id += 1
    
    return pd.DataFrame(promotions)


# 生成流量数据（优化版 - 批量生成）
def generate_traffic(stores_df, time_span_days=365):
    """
    生成店铺流量数据
    time_span_days: 时间跨度（天）
    """
    traffic = []
    
    # 预生成日期列表（根据时间跨度，最多90天）
    dates = [datetime.now().date() - timedelta(days=i) for i in range(min(time_span_days, 90))]
    
    # 平台流量基数映射
    platform_uv_ranges = {
        '天猫': (2000, 8000),
        '淘宝': (2000, 8000),
        '京东': (1500, 6000)
    }
    default_uv_range = (800, 3000)
    
    for _, store in stores_df.iterrows():
        store_id = store['店铺ID']
        platform = store['平台']
        uv_min, uv_max = platform_uv_ranges.get(platform, default_uv_range)
        
        for date in dates:
            base_uv = random.randint(uv_min, uv_max)
            pv = int(base_uv * random.uniform(2.5, 4.5))
            search_uv = int(base_uv * random.uniform(0.3, 0.5))
            recommend_uv = int(base_uv * random.uniform(0.2, 0.3))
            direct_uv = int(base_uv * random.uniform(0.1, 0.2))
            other_uv = base_uv - search_uv - recommend_uv - direct_uv
            
            traffic.append({
                '日期': date,
                '店铺ID': store_id,
                '平台': platform,
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


# 生成库存变动数据（优化版 - 批量生成）
def generate_inventory(products_df, time_span_days=365):
    """
    生成库存变动记录
    time_span_days: 时间跨度（天）
    """
    inventory = []
    inv_id = 1
    
    # 只为部分商品生成库存记录
    sample_size = min(len(products_df), 200)
    sample_products = products_df.sample(sample_size)
    
    # 预生成常量（根据时间跨度，最多30天）
    dates = [datetime.now().date() - timedelta(days=i) for i in range(min(time_span_days, 30))]
    change_types = ['入库', '出库', '出库']
    remarks = ['正常销售', '补货', '退货', '盘点调整']
    
    for _, product in sample_products.iterrows():
        current_stock = product['库存']
        product_id = product['商品ID']
        store_id = product['店铺ID']
        
        for date in dates:
            # 入库/出库
            change_type = random.choice(change_types)
            
            if change_type == '入库':
                quantity = random.randint(20, 80)
                current_stock += quantity
            else:
                quantity = random.randint(5, 15)
                current_stock = max(10, current_stock - quantity)
            
            inventory.append({
                '库存记录ID': f'INV{inv_id:08d}',
                '日期': date,
                '商品ID': product_id,
                '店铺ID': store_id,
                '变动类型': change_type,
                '变动数量': quantity,
                '变动后库存': current_stock,
                '备注': random.choice(remarks)
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
        '快手': ['快手旗舰店1号', '快手旗舰店2号', '快手旗舰店3号', '快手旗舰店4号'],
        '微信': ['微信旗舰店1号', '微信旗舰店2号', '微信旗舰店3号', '微信旗舰店4号'],
        '小红书': ['小红书旗舰店1号', '小红书旗舰店2号', '小红书旗舰店3号', '小红书旗舰店4号'],
        '拼多多': ['拼多多旗舰店1号', '拼多多旗舰店2号', '拼多多旗舰店3号', '拼多多旗舰店4号']
    })
    
    # 新逻辑：只需要订单数和时间跨度
    num_orders = config.get('numOrders', 20000)
    time_span_days = config.get('timeSpanDays', 365)  # 默认1年
    
    # 根据订单数自动计算用户数（平均每个用户下单3-5次）
    avg_orders_per_user = 4
    num_users = max(100, int(num_orders / avg_orders_per_user))
    
    main_category = config.get('mainCategory', 'bicycle')
    
    # 获取类目配置
    category_config = CATEGORY_CONFIGS.get(main_category, CATEGORY_CONFIGS['bicycle'])
    
    print("="*60)
    print("数据生成配置")
    print("="*60)
    print(f"主营类目: {category_config['name']}")
    print(f"平台店铺: {len(platform_stores)} 个平台, {sum(len(v) for v in platform_stores.values())} 家店铺")
    print(f"订单数量: {num_orders:,} 条")
    print(f"时间跨度: {time_span_days} 天")
    print(f"用户数量: {num_users:,} 个（自动计算）")
    print("="*60)
    sys.stdout.flush()
    
    # 生成各类数据
    print("\n1. 生成店铺数据...")
    stores_df = generate_stores(platform_stores)
    stores_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_stores.csv'), index=False, encoding='utf-8-sig')
    print(f"   ✓ 生成 {len(stores_df)} 条店铺数据")
    sys.stdout.flush()
    
    print("\n2. 生成商品数据...")
    products_df = generate_products(stores_df, category_config)
    products_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_products.csv'), index=False, encoding='utf-8-sig')
    print(f"   ✓ 生成 {len(products_df)} 条商品数据")
    sys.stdout.flush()
    
    print(f"\n3. 生成用户数据（{num_users:,} 个）...")
    users_df = generate_users(num_users, time_span_days)
    print("   保存用户数据到CSV...")
    # 优化：使用压缩和更快的写入方式
    users_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_users.csv'), index=False, encoding='utf-8-sig', chunksize=50000)
    print(f"   ✓ 生成 {len(users_df):,} 条用户数据")
    sys.stdout.flush()
    
    print(f"\n4. 生成订单数据（{num_orders:,} 条）...")
    orders_df, order_details_df = generate_orders(stores_df, products_df, users_df, num_orders, time_span_days)
    
    # 暂时禁用直接写入模式，使用 CSV 方式（更稳定）
    use_direct_db = False
    
    if False:  # 预留代码，暂不启用
        print("   检测到大数据量，将跳过 CSV 直接写入数据库...")
        print(f"   ✓ 生成 {len(orders_df):,} 条订单数据（内存中）")
        print(f"   ✓ 生成 {len(order_details_df):,} 条订单明细数据（内存中）")
        sys.stdout.flush()
    else:
        print("   保存订单数据到CSV...")
        orders_path = os.path.join(DATA_DIR, 'ods/ods_orders.csv')
        
        # 使用原生 CSV writer
        with open(orders_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(orders_df.columns)
            writer.writerows(orders_df.values)
        
        print("   保存订单明细数据到CSV...")
        order_details_path = os.path.join(DATA_DIR, 'ods/ods_order_details.csv')
        
        with open(order_details_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(order_details_df.columns)
            writer.writerows(order_details_df.values)
        
        print(f"   ✓ 生成 {len(orders_df):,} 条订单数据")
        print(f"   ✓ 生成 {len(order_details_df):,} 条订单明细数据")
        sys.stdout.flush()
    
    print("\n5. 生成推广数据...")
    promotion_df = generate_promotion(stores_df, products_df, time_span_days)
    promotion_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_promotion.csv'), index=False, encoding='utf-8-sig')
    print(f"   ✓ 生成 {len(promotion_df):,} 条推广数据")
    sys.stdout.flush()
    
    print("\n6. 生成流量数据...")
    traffic_df = generate_traffic(stores_df, time_span_days)
    traffic_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_traffic.csv'), index=False, encoding='utf-8-sig')
    print(f"   ✓ 生成 {len(traffic_df):,} 条流量数据")
    sys.stdout.flush()
    
    print("\n7. 生成库存数据...")
    inventory_df = generate_inventory(products_df, time_span_days)
    inventory_df.to_csv(os.path.join(DATA_DIR, 'ods/ods_inventory.csv'), index=False, encoding='utf-8-sig')
    print(f"   ✓ 生成 {len(inventory_df):,} 条库存数据")
    sys.stdout.flush()
    
    print("\n" + "="*60)
    print("✓ 数据生成完成！")
    print("="*60)
    print(f"数据保存位置: {os.path.join(DATA_DIR, 'ods/')}")
    print(f"总订单数: {num_orders:,} 条")
    print(f"总用户数: {num_users:,} 个")
    print(f"时间跨度: {time_span_days} 天")
    print("="*60)
    sys.stdout.flush()
    
    # 数据生成完成，不自动导入数据库
    


if __name__ == '__main__':
    main()
