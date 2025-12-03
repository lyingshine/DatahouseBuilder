"""
企业体量配置
根据企业规模决定流量基数
"""

# 企业体量配置
BUSINESS_SCALES = {
    '微型企业': {
        'name': '微型企业',
        'description': '3-5家店铺，月GMV 10-50万',
        'daily_traffic_base': 500,      # 每店每日基础流量
        'store_count_range': (3, 5),    # 店铺数量范围
        'monthly_gmv_range': (100000, 500000),  # 月GMV范围
        'traffic_multiplier': 0.5       # 流量系数
    },
    '小型企业': {
        'name': '小型企业',
        'description': '6-10家店铺，月GMV 50-200万',
        'daily_traffic_base': 1500,
        'store_count_range': (6, 10),
        'monthly_gmv_range': (500000, 2000000),
        'traffic_multiplier': 1.0
    },
    '中型企业': {
        'name': '中型企业',
        'description': '10-20家店铺，月GMV 200-1000万',
        'daily_traffic_base': 3000,
        'store_count_range': (10, 20),
        'monthly_gmv_range': (2000000, 10000000),
        'traffic_multiplier': 2.0
    },
    '大型企业': {
        'name': '大型企业',
        'description': '20-50家店铺，月GMV 1000-5000万',
        'daily_traffic_base': 8000,
        'store_count_range': (20, 50),
        'monthly_gmv_range': (10000000, 50000000),
        'traffic_multiplier': 5.0
    },
    '超大型企业': {
        'name': '超大型企业',
        'description': '50+家店铺，月GMV 5000万+',
        'daily_traffic_base': 20000,
        'store_count_range': (50, 100),
        'monthly_gmv_range': (50000000, 200000000),
        'traffic_multiplier': 10.0
    }
}


def get_scale_config(scale_name):
    """获取企业体量配置"""
    return BUSINESS_SCALES.get(scale_name, BUSINESS_SCALES['小型企业'])


def calculate_traffic_from_scale(scale_name, store_count, time_span_days):
    """
    根据企业体量计算总流量
    
    返回：
    - total_traffic: 总流量（曝光量）
    - daily_traffic: 日均流量
    """
    config = get_scale_config(scale_name)
    
    # 每店每日流量
    daily_traffic_per_store = config['daily_traffic_base'] * config['traffic_multiplier']
    
    # 总流量 = 店铺数 × 每店每日流量 × 天数
    total_traffic = int(store_count * daily_traffic_per_store * time_span_days)
    daily_traffic = int(store_count * daily_traffic_per_store)
    
    return {
        'total_traffic': total_traffic,
        'daily_traffic': daily_traffic,
        'daily_per_store': int(daily_traffic_per_store)
    }


def estimate_orders_from_traffic(total_clicks, avg_cvr=0.05):
    """
    根据总点击量估算订单数
    
    参数：
    - total_clicks: 总点击量
    - avg_cvr: 平均转化率（默认5%）
    
    返回：预估订单数
    """
    return int(total_clicks * avg_cvr)


def get_scale_summary(scale_name, store_count, time_span_days):
    """
    获取企业体量摘要信息
    """
    config = get_scale_config(scale_name)
    traffic_info = calculate_traffic_from_scale(scale_name, store_count, time_span_days)
    
    # 假设点击率3%，转化率5%
    ctr = 0.03
    cvr = 0.05
    
    total_impressions = traffic_info['total_traffic']
    total_clicks = int(total_impressions * ctr)
    estimated_orders = estimate_orders_from_traffic(total_clicks, cvr)
    
    # 估算GMV（假设客单价500元）
    avg_order_value = 500
    estimated_gmv = estimated_orders * avg_order_value
    monthly_gmv = estimated_gmv / (time_span_days / 30)
    
    return {
        'scale_name': scale_name,
        'description': config['description'],
        'store_count': store_count,
        'time_span_days': time_span_days,
        'total_impressions': total_impressions,
        'total_clicks': total_clicks,
        'estimated_orders': estimated_orders,
        'estimated_gmv': estimated_gmv,
        'monthly_gmv': monthly_gmv,
        'daily_traffic': traffic_info['daily_traffic'],
        'daily_per_store': traffic_info['daily_per_store']
    }
