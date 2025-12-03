"""
订单生成器
负责生成订单和订单明细数据（支持多进程）
"""
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
from .base_generator import BaseGenerator


class OrderGenerator(BaseGenerator):
    """订单数据生成器（支持多进程）"""
    
    def __init__(self, stores_df, products_df, users_df, 
                 num_orders=50000, time_span_days=365, config=None):
        """
        初始化订单生成器
        
        Args:
            stores_df: 店铺数据
            products_df: 商品数据
            users_df: 用户数据
            num_orders: 订单数量
            time_span_days: 时间跨度（天）
            config: 额外配置
        """
        super().__init__(config)
        self.stores_df = stores_df
        self.products_df = products_df
        self.users_df = users_df
        self.num_orders = num_orders
        self.time_span_days = time_span_days
    
    def generate(self):
        """
        生成订单数据（多进程模式）
        
        Returns:
            tuple: (orders_df, order_details_df)
        """
        print(f"   正在生成 {self.num_orders:,} 个订单（时间跨度: {self.time_span_days}天）...")
        
        # 使用多进程模式
        num_processes = multiprocessing.cpu_count()
        print(f"   使用多进程模式（{num_processes} 进程）...")
        
        # 转换为可序列化的格式
        stores_list = self.stores_df.to_dict('records')
        users_list = self.users_df.to_dict('records')
        
        # 预先计算每个店铺的商品列表
        store_products_dict = {}
        for store_id in self.stores_df['店铺ID'].unique():
            prods = self.products_df[self.products_df['店铺ID'] == store_id]
            if len(prods) > 0:
                store_products_dict[store_id] = prods.to_dict('records')
        
        # 计算批次大小
        min_batch = 5000
        max_batch = 50000
        ideal_batch = self.num_orders // num_processes
        batch_size = max(min_batch, min(max_batch, ideal_batch))
        
        actual_processes = min(num_processes, (self.num_orders + batch_size - 1) // batch_size)
        
        print(f"   批次大小: {batch_size:,} 订单/进程")
        print(f"   实际进程数: {actual_processes}")
        
        # 准备批次
        batches = []
        order_id = 1
        detail_id = 1
        
        for i in range(actual_processes):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, self.num_orders)
            current_batch_size = end_idx - start_idx
            
            if current_batch_size > 0:
                batches.append((
                    i, current_batch_size, stores_list, store_products_dict,
                    users_list, order_id, detail_id, self.time_span_days
                ))
                order_id += current_batch_size * 3
                detail_id += current_batch_size * 10
        
        # 执行多进程生成
        all_orders = []
        all_order_details = []
        
        print(f"   启动 {actual_processes} 个进程并行生成...")
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=actual_processes) as executor:
            futures = {
                executor.submit(self._generate_orders_batch, *batch): i
                for i, batch in enumerate(batches)
            }
            
            completed = 0
            total_orders_generated = 0
            total_details_generated = 0
            
            for future in as_completed(futures):
                orders, order_details, _, _ = future.result()
                all_orders.extend(orders)
                all_order_details.extend(order_details)
                
                total_orders_generated += len(orders)
                total_details_generated += len(order_details)
                
                completed += 1
                progress = int((completed / len(futures)) * 100)
                elapsed = time.time() - start_time
                orders_per_sec = total_orders_generated / elapsed if elapsed > 0 else 0
                
                print(f"   进度: {progress}% ({completed}/{len(futures)} 进程) - "
                      f"{total_orders_generated:,} 订单 - {orders_per_sec:,.0f} 订单/秒")
        
        elapsed_total = time.time() - start_time
        orders_per_sec = total_orders_generated / elapsed_total if elapsed_total > 0 else 0
        print(f"   ✓ 多进程生成完成: {total_orders_generated:,} 订单, "
              f"{total_details_generated:,} 明细")
        print(f"   性能: {orders_per_sec:,.0f} 订单/秒, 总耗时 {elapsed_total:.1f}秒")
        
        # 创建DataFrame
        print("   正在创建DataFrame...")
        
        orders_df = pd.DataFrame(all_orders, columns=[
            '订单ID', '用户ID', '店铺ID', '平台', '下单时间', '订单状态',
            '商品总额', '优惠金额', '运费', '实付金额', '成本总额',
            '支付方式', '流量来源', '创建时间', '更新时间'
        ])
        
        # 优化数据类型
        orders_df['订单ID'] = orders_df['订单ID'].astype('string')
        orders_df['用户ID'] = orders_df['用户ID'].astype('string')
        orders_df['店铺ID'] = orders_df['店铺ID'].astype('string')
        orders_df['平台'] = orders_df['平台'].astype('category')
        orders_df['订单状态'] = orders_df['订单状态'].astype('category')
        orders_df['支付方式'] = orders_df['支付方式'].astype('category')
        orders_df['流量来源'] = orders_df['流量来源'].astype('category')
        
        order_details_df = pd.DataFrame(all_order_details, columns=[
            '订单明细ID', '订单ID', 'SKU_ID', '商品ID', '数量', '单价', '金额'
        ])
        
        # 优化数据类型
        order_details_df['订单明细ID'] = order_details_df['订单明细ID'].astype('string')
        order_details_df['订单ID'] = order_details_df['订单ID'].astype('string')
        order_details_df['SKU_ID'] = order_details_df['SKU_ID'].astype('string')
        order_details_df['商品ID'] = order_details_df['商品ID'].astype('string')
        
        print(f"   ✓ DataFrame创建完成（内存优化）")
        
        return orders_df, order_details_df
    
    @staticmethod
    def _generate_orders_batch(batch_id, batch_size, stores_list, store_products_dict,
                               users_list, start_order_id, start_detail_id, time_span_days):
        """
        生成一批订单数据（多进程任务）
        
        Args:
            batch_id: 批次ID
            batch_size: 批次大小
            stores_list: 店铺列表
            store_products_dict: 店铺商品字典
            users_list: 用户列表
            start_order_id: 起始订单ID
            start_detail_id: 起始明细ID
            time_span_days: 时间跨度
        
        Returns:
            tuple: (orders, order_details, next_order_id, next_detail_id)
        """
        orders = []
        order_details = []
        
        order_statuses = ['已完成', '已取消', '退款']
        status_weights = [0.92, 0.06, 0.02]
        payment_methods = ['支付宝', '微信', '银行卡']
        payment_weights = [0.50, 0.40, 0.10]
        
        # 预生成随机索引
        store_indices = np.random.randint(0, len(stores_list), batch_size)
        user_indices = np.random.randint(0, len(users_list), batch_size)
        
        # 预生成订单时间
        now = datetime.now()
        time_deltas = np.random.randint(0, time_span_days * 24 * 3600, batch_size)
        order_times = [now - timedelta(seconds=int(delta)) for delta in time_deltas]
        
        # 预生成随机数
        order_status_choices = np.random.choice(order_statuses, size=batch_size, p=status_weights)
        payment_method_choices = np.random.choice(payment_methods, size=batch_size, p=payment_weights)
        update_days = np.random.randint(0, 8, batch_size)
        
        order_id = start_order_id
        detail_id = start_detail_id
        
        for i in range(batch_size):
            store = stores_list[store_indices[i]]
            user = users_list[user_indices[i]]
            
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
            
            for idx in product_indices:
                product = store_products[idx]
                quantity = random.randint(1, 3)
                item_amount = product['售价'] * quantity
                item_cost = product['成本'] * quantity
                
                total_amount += item_amount
                total_cost += item_cost
                
                # 计算运费
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
            
            final_amount = round(total_amount, 2)
            
            # 流量来源分配
            traffic_sources = ['搜索', '推荐', '直接访问', '活动页', '店铺首页', '付费推广']
            traffic_weights = [0.35, 0.25, 0.10, 0.05, 0.05, 0.20]
            traffic_source = random.choices(traffic_sources, weights=traffic_weights)[0]
            
            orders.append([
                f'O{order_id:08d}',
                user['用户ID'],
                store['店铺ID'],
                store['平台'],
                order_time,
                order_status,
                round(total_amount, 2),
                0,  # 优惠金额
                shipping_fee,
                final_amount if order_status == '已完成' else 0,
                round(total_cost, 2) if order_status == '已完成' else 0,
                payment_method_choices[i],
                traffic_source,
                order_time,
                order_time + timedelta(days=int(update_days[i]))
            ])
            
            order_id += 1
        
        return orders, order_details, order_id, detail_id
