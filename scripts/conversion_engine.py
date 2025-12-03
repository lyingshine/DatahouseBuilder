"""
转化引擎 - 将流量转化为订单（多进程优化版）
"""
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import time


# 全局配置
TIER_CONVERSION_RATES = {
    '畅销品': (0.03, 0.08),
    '利润品': (0.01, 0.03),
    '主推新品': (0.02, 0.05),
    '滞销品': (0.005, 0.015),
    '引流品': (0.04, 0.10),
}


def generate_daily_orders_batch(batch_data):
    """
    多进程任务：生成一批日期的订单
    batch_data: (daily_traffic_list, product_dict, users_list, orders_per_day, start_order_id, start_detail_id)
    """
    daily_traffic_list, product_dict, users_list, orders_per_day, start_order_id, start_detail_id = batch_data
    
    all_orders = []
    all_details = []
    order_id = start_order_id
    detail_id = start_detail_id
    
    for date, daily_traffic_df in daily_traffic_list:
        # 按SKU+店铺聚合流量
        product_traffic = daily_traffic_df.groupby(['SKU_ID', '店铺ID']).agg({
            '点击量': 'sum',
            '流量类型': lambda x: '付费' if '付费' in x.values else '自然',
            '平台': 'first',
            '商品ID': 'first',
            '商品分层': 'first'
        }).reset_index()
        
        # 计算转化权重（不是直接转化数）
        conversions = []
        total_weight = 0
        
        for _, row in product_traffic.iterrows():
            tier = row['商品分层']
            cvr_min, cvr_max = TIER_CONVERSION_RATES.get(tier, (0.02, 0.05))
            cvr = random.uniform(cvr_min, cvr_max)
            
            # 权重 = 点击量 × 转化率
            weight = row['点击量'] * cvr
            total_weight += weight
            
            if weight > 0:
                conversions.append({
                    'SKU_ID': row['SKU_ID'],
                    '商品ID': row['商品ID'],
                    '店铺ID': row['店铺ID'],
                    '平台': row['平台'],
                    '流量来源': '付费推广' if row['流量类型'] == '付费' else random.choice(['搜索', '推荐', '直接访问']),
                    'weight': weight
                })
        
        # 按权重分配订单数（确保总数不超过orders_per_day）
        if len(conversions) > 0 and total_weight > 0:
            for conv in conversions:
                # 按权重比例分配订单
                conv['转化数'] = int(orders_per_day * conv['weight'] / total_weight)
            
            # 补齐剩余订单
            actual_total = sum(c['转化数'] for c in conversions)
            if actual_total < orders_per_day:
                shortage = orders_per_day - actual_total
                # 随机分配剩余订单
                for _ in range(shortage):
                    conv = random.choice(conversions)
                    conv['转化数'] += 1
        
        # 生成订单（严格控制数量）
        daily_order_count = 0
        for conv in conversions:
            for _ in range(conv['转化数']):
                if daily_order_count >= orders_per_day:
                    break
                
                order, detail = _create_order_static(
                    conv, date, order_id, detail_id, product_dict, users_list
                )
                if order:
                    all_orders.append(order)
                    all_details.extend(detail)
                    order_id += 1
                    detail_id += len(detail)
                    daily_order_count += 1
            
            if daily_order_count >= orders_per_day:
                break
    
    return all_orders, all_details, order_id, detail_id


def _create_order_static(conversion, date, order_id, detail_id, product_dict, users_list):
    """创建单个订单（静态方法）"""
    sku_id = conversion['SKU_ID']
    product_id = conversion['商品ID']
    store_id = conversion['店铺ID']
    platform = conversion['平台']
    traffic_source = conversion['流量来源']
    
    key = f"{sku_id}_{store_id}"
    product = product_dict.get(key)
    if not product:
        return None, []
    
    user = random.choice(users_list)
    
    order_status = random.choices(
        ['已完成', '已取消', '退款'],
        weights=[0.92, 0.06, 0.02]
    )[0]
    
    payment_method = random.choices(
        ['支付宝', '微信', '银行卡'],
        weights=[0.50, 0.40, 0.10]
    )[0]
    
    order_time = datetime.combine(date, datetime.min.time()) + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    quantity = random.randint(1, 3)
    price = product['售价']
    cost = product['成本']
    amount = round(price * quantity, 2)
    cost_amount = round(cost * quantity, 2)
    
    if product['一级类目'].startswith('整车'):
        shipping_fee = 30 * quantity
    else:
        shipping_fee = 3 * quantity
    
    order = {
        '订单ID': f'O{order_id:08d}',
        '用户ID': user['用户ID'],
        '店铺ID': store_id,
        '平台': platform,
        '下单时间': order_time,
        '订单状态': order_status,
        '商品总额': amount,
        '优惠金额': 0,
        '运费': shipping_fee,
        '实付金额': amount if order_status == '已完成' else 0,
        '成本总额': cost_amount if order_status == '已完成' else 0,
        '支付方式': payment_method,
        '流量来源': traffic_source,
        '创建时间': order_time,
        '更新时间': order_time + timedelta(days=random.randint(0, 7))
    }
    
    detail = {
        '订单明细ID': f'OD{detail_id:08d}',
        '订单ID': f'O{order_id:08d}',
        'SKU_ID': sku_id,
        '商品ID': product_id,
        '数量': quantity,
        '单价': price,
        '金额': amount
    }
    
    return order, [detail]


class ConversionEngine:
    """转化引擎 - 根据流量数据生成订单（多进程优化）"""
    
    def __init__(self, traffic_df, products_df, users_df, stores_df):
        self.traffic_df = traffic_df
        self.products_df = products_df
        self.users_df = users_df
        self.stores_df = stores_df
        self.tier_conversion_rates = TIER_CONVERSION_RATES
        
        # 创建商品字典
        self.product_dict = {}
        for _, row in products_df.iterrows():
            key = f"{row['SKU_ID']}_{row['店铺ID']}"
            self.product_dict[key] = row.to_dict()
        self.users_list = users_df.to_dict('records')
    
    def generate_orders_from_traffic(self, target_order_count, use_multiprocess=True):
        """
        从流量数据生成订单
        use_multiprocess: 是否使用多进程
        """
        total_days = len(self.traffic_df['日期'].unique())
        
        if not use_multiprocess or total_days < 30:
            return self._generate_orders_single(target_order_count)
        
        return self._generate_orders_multi(target_order_count)

    
    def _generate_orders_single(self, target_order_count):
        """单进程模式"""
        orders = []
        order_details = []
        order_id = 1
        detail_id = 1
        
        traffic_by_date = self.traffic_df.groupby('日期')
        total_days = len(self.traffic_df['日期'].unique())
        orders_per_day = target_order_count // total_days
        
        for date, daily_traffic in traffic_by_date:
            daily_orders = self._generate_daily_orders(
                daily_traffic, orders_per_day, order_id, detail_id, date
            )
            
            orders.extend(daily_orders['orders'])
            order_details.extend(daily_orders['details'])
            order_id = daily_orders['next_order_id']
            detail_id = daily_orders['next_detail_id']
        
        return pd.DataFrame(orders), pd.DataFrame(order_details)
    
    def _generate_orders_multi(self, target_order_count):
        """多进程模式"""
        num_processes = multiprocessing.cpu_count()
        traffic_by_date = list(self.traffic_df.groupby('日期'))
        total_days = len(traffic_by_date)
        
        # 计算每天目标订单数
        orders_per_day = max(1, target_order_count // total_days)
        
        # 分批：每个进程处理一部分日期
        batch_size = max(5, total_days // num_processes)
        batches = []
        order_id = 1
        detail_id = 1
        
        for i in range(0, total_days, batch_size):
            batch_dates = traffic_by_date[i:i+batch_size]
            batches.append((
                batch_dates,
                self.product_dict,
                self.users_list,
                orders_per_day,
                order_id,
                detail_id
            ))
            # 预留ID空间（精确计算）
            order_id += batch_size * orders_per_day + 100
            detail_id += batch_size * orders_per_day + 100
        
        print(f"   使用多进程模式（{num_processes} 进程，{len(batches)} 批次）...")
        
        all_orders = []
        all_details = []
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            futures = {executor.submit(generate_daily_orders_batch, batch): i 
                      for i, batch in enumerate(batches)}
            
            completed = 0
            for future in as_completed(futures):
                orders, details, _, _ = future.result()
                all_orders.extend(orders)
                all_details.extend(details)
                
                completed += 1
                progress = int((completed / len(futures)) * 100)
                elapsed = time.time() - start_time
                orders_per_sec = len(all_orders) / elapsed if elapsed > 0 else 0
                
                print(f"   进度: {progress}% ({completed}/{len(futures)}) - "
                      f"{len(all_orders):,} 订单 - {orders_per_sec:,.0f} 订单/秒")
        
        elapsed_total = time.time() - start_time
        print(f"   ✓ 多进程生成完成: {len(all_orders):,} 订单, 耗时 {elapsed_total:.1f}秒")
        
        return pd.DataFrame(all_orders), pd.DataFrame(all_details)
    
    def _generate_daily_orders(self, daily_traffic, target_count, start_order_id, start_detail_id, date):
        """生成当天的订单（单进程模式使用）"""
        orders = []
        details = []
        order_id = start_order_id
        detail_id = start_detail_id
        
        product_traffic = daily_traffic.groupby(['SKU_ID', '店铺ID']).agg({
            '点击量': 'sum',
            '流量类型': lambda x: '付费' if '付费' in x.values else '自然',
            '平台': 'first',
            '商品ID': 'first',
            '商品分层': 'first'
        }).reset_index()
        
        # 计算转化权重
        conversions = []
        total_weight = 0
        
        for _, row in product_traffic.iterrows():
            tier = row['商品分层']
            cvr_min, cvr_max = self.tier_conversion_rates.get(tier, (0.02, 0.05))
            cvr = random.uniform(cvr_min, cvr_max)
            
            weight = row['点击量'] * cvr
            total_weight += weight
            
            if weight > 0:
                conversions.append({
                    'SKU_ID': row['SKU_ID'],
                    '商品ID': row['商品ID'],
                    '店铺ID': row['店铺ID'],
                    '平台': row['平台'],
                    '流量来源': '付费推广' if row['流量类型'] == '付费' else random.choice(['搜索', '推荐', '直接访问']),
                    'weight': weight
                })
        
        # 按权重分配订单数
        if len(conversions) > 0 and total_weight > 0:
            for conv in conversions:
                conv['转化数'] = int(target_count * conv['weight'] / total_weight)
            
            actual_total = sum(c['转化数'] for c in conversions)
            if actual_total < target_count:
                shortage = target_count - actual_total
                for _ in range(shortage):
                    conv = random.choice(conversions)
                    conv['转化数'] += 1
        
        # 生成订单（严格控制数量）
        daily_order_count = 0
        for conv in conversions:
            for _ in range(conv['转化数']):
                if daily_order_count >= target_count:
                    break
                
                order, order_detail = _create_order_static(
                    conv, date, order_id, detail_id, self.product_dict, self.users_list
                )
                
                if order:
                    orders.append(order)
                    details.extend(order_detail)
                    order_id += 1
                    detail_id += len(order_detail)
                    daily_order_count += 1
            
            if daily_order_count >= target_count:
                break
        
        return {
            'orders': orders,
            'details': details,
            'next_order_id': order_id,
            'next_detail_id': detail_id
        }
