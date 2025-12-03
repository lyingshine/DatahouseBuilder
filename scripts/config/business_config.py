"""
商品分层和业务配置
定义商品分层策略、利润率、销售权重等
"""

# 商品分层配置（真实电商模型）
PRODUCT_TIERS = {
    '畅销品': {
        'ratio': 0.30,                    # 占比30%
        'profit_margin': (0.28, 0.33),    # 利润率28-33%
        'sales_weight': 3.0,              # 销量权重3倍
        'conversion_rate': (0.03, 0.08),  # 转化率3-8%
        'description': '走量商品，中等利润'
    },
    '利润品': {
        'ratio': 0.20,
        'profit_margin': (0.40, 0.50),
        'sales_weight': 0.5,
        'conversion_rate': (0.01, 0.03),
        'description': '高毛利，销量少'
    },
    '主推新品': {
        'ratio': 0.15,
        'profit_margin': (0.28, 0.35),
        'sales_weight': 1.5,
        'conversion_rate': (0.02, 0.05),
        'description': '中等利润，推广费高'
    },
    '滞销品': {
        'ratio': 0.20,
        'profit_margin': (0.25, 0.40),
        'sales_weight': 0.3,
        'conversion_rate': (0.005, 0.015),
        'description': '销量低，利润不稳定'
    },
    '引流品': {
        'ratio': 0.15,
        'profit_margin': (0.20, 0.25),
        'sales_weight': 4.0,
        'conversion_rate': (0.04, 0.10),
        'description': '高销量，低利润'
    },
}

# 不同类别的利润率加成
CATEGORY_PROFIT_BONUS = {
    '整车-品牌': 0.00,   # 品牌整车：无加成
    '整车-白牌': 0.05,   # 白牌整车：+5%
    '骑行装备-品牌': 0.10,  # 品牌装备：+10%
    '骑行装备-白牌': 0.15,  # 白牌装备：+15%
}

# 运费配置
SHIPPING_FEE = {
    '整车': 30,  # 整车30元/件
    '配件': 3,   # 配件3元/件
}

# 费用率配置
FEE_RATES = {
    'after_sale': 0.02,   # 售后费：2%
    'platform': 0.05,     # 平台费：5%
    'management': 0.10,   # 管理费：10%（原8%，调整为10%）
}


def get_tier_config(tier_name):
    """
    获取商品分层配置
    
    Args:
        tier_name: 分层名称
    
    Returns:
        dict: 分层配置
    """
    return PRODUCT_TIERS.get(tier_name, PRODUCT_TIERS['畅销品'])


def get_profit_margin(tier_name, category_type=None):
    """
    获取利润率范围（含类别加成）
    
    Args:
        tier_name: 分层名称
        category_type: 类别类型（如 '整车-品牌'）
    
    Returns:
        tuple: (最小利润率, 最大利润率)
    """
    tier = get_tier_config(tier_name)
    base_min, base_max = tier['profit_margin']
    
    # 应用类别加成
    if category_type:
        bonus = CATEGORY_PROFIT_BONUS.get(category_type, 0)
        return (
            min(0.65, base_min + bonus),  # 最高65%
            min(0.65, base_max + bonus)
        )
    
    return (base_min, base_max)


def get_sales_weight(tier_name):
    """
    获取销量权重
    
    Args:
        tier_name: 分层名称
    
    Returns:
        float: 销量权重
    """
    tier = get_tier_config(tier_name)
    return tier['sales_weight']


def get_conversion_rate(tier_name):
    """
    获取转化率范围
    
    Args:
        tier_name: 分层名称
    
    Returns:
        tuple: (最小转化率, 最大转化率)
    """
    tier = get_tier_config(tier_name)
    return tier['conversion_rate']


def get_shipping_fee(category):
    """
    获取运费
    
    Args:
        category: 类目（如 '整车-品牌' 或 '骑行装备'）
    
    Returns:
        float: 运费
    """
    if '整车' in category:
        return SHIPPING_FEE['整车']
    return SHIPPING_FEE['配件']


def get_fee_rate(fee_type):
    """
    获取费用率
    
    Args:
        fee_type: 费用类型 ('after_sale', 'platform', 'management')
    
    Returns:
        float: 费用率
    """
    return FEE_RATES.get(fee_type, 0)
