"""
配置管理模块
统一管理所有配置信息
"""
from .business_config import (
    PRODUCT_TIERS,
    get_tier_config,
    get_profit_margin,
    get_sales_weight
)

from .platform_config import (
    PLATFORM_CHANNELS,
    get_platform_channels,
    get_all_platforms
)

from .category_config import (
    CATEGORY_CONFIGS,
    get_category_config,
    get_all_categories
)

__all__ = [
    # 商品分层配置
    'PRODUCT_TIERS',
    'get_tier_config',
    'get_profit_margin',
    'get_sales_weight',
    
    # 平台配置
    'PLATFORM_CHANNELS',
    'get_platform_channels',
    'get_all_platforms',
    
    # 类目配置
    'CATEGORY_CONFIGS',
    'get_category_config',
    'get_all_categories',
]
