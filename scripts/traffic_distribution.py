"""
统一流量分发模块（多进程优化版）
从流量模拟开始 → 曝光/点击 → 转化 → 订单生成
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import time


# 全局配置（用于多进程）
TIER_TRAFFIC_WEIGHTS = {
    '畅销品': 3.0,
    '利润品': 0.5,
    '主推新品': 1.5,
    '滞销品': 0.3,
    '引流品': 4.0,
}

NATURAL_CHANNELS = ['搜索', '推荐', '直接访问', '活动页', '店铺首页']

PAID_CHANNELS = {
    '京东': ['京东快车', '京东展位', '京准通'],
    '天猫': ['直通车', '钻展', '超级推荐'],
    '抖音': ['巨量千川', '抖音小店随心推', 'DOU+'],
    '快手': ['磁力金牛', '快手粉条'],
    '微信': ['朋友圈广告', '公众号广告'],
    '小红书': ['信息流广告', '搜索广告', '薯条'],
    '拼多多': ['多多搜索', '多多场景']
}


def generate_product_traffic_batch(batch_data):
    """
    多进程任务：为一批商品生成流量
    batch_data: (products_list, dates, traffic_base, batch_id)
    """
    products_list, dates, traffic_base, batch_id = batch_data
    traffic_records = []
    
    for product in products_list:
        tier = product['商品分层']
        weight = TIER_TRAFFIC_WEIGHTS.get(tier, 1.0)
        
        for date in dates:
            # 自然流量（每天都有）
            natural_traffic = _generate_natural_traffic_static(product, date, weight, traffic_base)
            traffic_records.extend(natural_traffic)
            
            # 付费流量（精确调整至5-8%推广费率，目标6.5%）
            # 主推新品和引流品：5%概率投放
            # 其他商品：2%概率投放
            if tier in ['主推新品', '引流品']:
                if random.random() < 0.05:
                    paid_traffic = _generate_paid_traffic_static(product, date, weight, traffic_base)
                    traffic_records.extend(paid_traffic)
            else:
                if random.random() < 0.02:
                    paid_traffic = _generate_paid_traffic_static(product, date, weight, traffic_base)
                    traffic_records.extend(paid_traffic)
    
    return traffic_records


def _generate_natural_traffic_static(product, date, weight, traffic_base):
    """生成自然流量（静态方法，用于多进程）"""
    records = []
    base_factor = traffic_base / 1000
    
    if product['一级类目'].startswith('整车'):
        base_impressions = int(random.uniform(100, 500) * weight * base_factor)
    else:
        base_impressions = int(random.uniform(50, 200) * weight * base_factor)
    
    num_channels = random.randint(1, 2)
    selected_channels = random.sample(NATURAL_CHANNELS, num_channels)
    
    for channel in selected_channels:
        impressions = int(base_impressions / num_channels)
        ctr = random.uniform(0.05, 0.15)
        clicks = int(impressions * ctr)
        
        records.append({
            '日期': date,
            '店铺ID': product['店铺ID'],
            '平台': product['平台'],
            'SKU_ID': product['SKU_ID'],
            '商品ID': product['商品ID'],
            '一级类目': product['一级类目'],
            '二级类目': product['二级类目'],
            '商品分层': product['商品分层'],
            '流量类型': '自然',
            '流量渠道': channel,
            '曝光量': impressions,
            '点击量': clicks,
            '点击率': round(ctr * 100, 2),
            '推广费用': 0,
            'CPC': 0
        })
    
    return records


def _generate_paid_traffic_static(product, date, weight, traffic_base):
    """生成付费流量（静态方法，用于多进程）- 精确调整至5-8%推广费率，目标6.5%"""
    records = []
    platform = product['平台']
    channels = PAID_CHANNELS.get(platform, ['通用推广'])
    channel = random.choice(channels)
    
    base_factor = traffic_base / 1000
    
    # 精确调整曝光量以达到目标推广费率6.5%
    if product['一级类目'].startswith('整车'):
        base_impressions = int(random.uniform(80, 180) * weight * base_factor)
    else:
        base_impressions = int(random.uniform(40, 90) * weight * base_factor)
    
    ctr = random.uniform(0.02, 0.04)
    clicks = int(base_impressions * ctr)
    
    # 精确调整CPC成本以达到目标推广费率6.5%
    if product['一级类目'].startswith('整车'):
        cpc = random.uniform(0.45, 0.75)  # 整车类CPC: 0.45-0.75元
    else:
        cpc = random.uniform(0.28, 0.52)  # 配件类CPC: 0.28-0.52元
    
    cost = round(clicks * cpc, 2)
    cost = max(12, cost)  # 最低预算12元
    
    records.append({
        '日期': date,
        '店铺ID': product['店铺ID'],
        '平台': product['平台'],
        'SKU_ID': product['SKU_ID'],
        '商品ID': product['商品ID'],
        '一级类目': product['一级类目'],
        '二级类目': product['二级类目'],
        '商品分层': product['商品分层'],
        '流量类型': '付费',
        '流量渠道': channel,
        '曝光量': base_impressions,
        '点击量': clicks,
        '点击率': round(ctr * 100, 2),
        '推广费用': cost,
        'CPC': round(cpc, 2)
    })
    
    return records


class TrafficDistributor:
    """流量分发器 - 根据商品分层分配流量权重（多进程优化）"""
    
    def __init__(self, products_df, time_span_days=365, traffic_base=1000):
        self.products_df = products_df
        self.time_span_days = time_span_days
        self.dates = [datetime.now().date() - timedelta(days=i) for i in range(time_span_days)]
        self.traffic_base = traffic_base
        self.tier_traffic_weights = TIER_TRAFFIC_WEIGHTS
        self.natural_channels = NATURAL_CHANNELS
        self.paid_channels = PAID_CHANNELS
    
    def distribute_traffic(self, use_multiprocess=True):
        """
        为所有商品分配流量（自然+付费）
        use_multiprocess: 是否使用多进程（默认True）
        """
        if not use_multiprocess or len(self.products_df) < 100:
            return self._distribute_traffic_single()
        
        return self._distribute_traffic_multi()
    
    def _distribute_traffic_single(self):
        """单进程模式"""
        traffic_records = []
        
        for _, product in self.products_df.iterrows():
            tier = product['商品分层']
            weight = self.tier_traffic_weights.get(tier, 1.0)
            
            for date in self.dates:
                # 自然流量（每天都有）
                natural_traffic = self._generate_natural_traffic(product, date, weight)
                traffic_records.extend(natural_traffic)
                
                # 付费流量（极低投放概率）
                if tier in ['主推新品', '引流品']:
                    if random.random() < 0.10:
                        paid_traffic = self._generate_paid_traffic(product, date, weight)
                        traffic_records.extend(paid_traffic)
                else:
                    if random.random() < 0.02:
                        paid_traffic = self._generate_paid_traffic(product, date, weight)
                        traffic_records.extend(paid_traffic)
        
        return pd.DataFrame(traffic_records)
    
    def _distribute_traffic_multi(self):
        """多进程模式"""
        num_processes = multiprocessing.cpu_count()
        products_list = self.products_df.to_dict('records')
        
        # 分批：每个进程处理一部分商品
        batch_size = max(10, len(products_list) // num_processes)
        batches = []
        
        for i in range(0, len(products_list), batch_size):
            batch_products = products_list[i:i+batch_size]
            batches.append((batch_products, self.dates, self.traffic_base, i // batch_size))
        
        print(f"   使用多进程模式（{num_processes} 进程，{len(batches)} 批次）...")
        
        all_traffic = []
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            futures = {executor.submit(generate_product_traffic_batch, batch): i 
                      for i, batch in enumerate(batches)}
            
            completed = 0
            for future in as_completed(futures):
                batch_traffic = future.result()
                all_traffic.extend(batch_traffic)
                
                completed += 1
                progress = int((completed / len(futures)) * 100)
                elapsed = time.time() - start_time
                records_per_sec = len(all_traffic) / elapsed if elapsed > 0 else 0
                
                print(f"   进度: {progress}% ({completed}/{len(futures)}) - "
                      f"{len(all_traffic):,} 条记录 - {records_per_sec:,.0f} 条/秒")
        
        elapsed_total = time.time() - start_time
        print(f"   ✓ 多进程生成完成: {len(all_traffic):,} 条记录, 耗时 {elapsed_total:.1f}秒")
        
        return pd.DataFrame(all_traffic)

    
    def _generate_natural_traffic(self, product, date, weight):
        """生成自然流量（单进程模式使用）"""
        return _generate_natural_traffic_static(product, date, weight, self.traffic_base)
    
    def _generate_paid_traffic(self, product, date, weight):
        """生成付费流量（单进程模式使用）"""
        return _generate_paid_traffic_static(product, date, weight, self.traffic_base)
