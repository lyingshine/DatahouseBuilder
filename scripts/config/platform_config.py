"""
平台配置
定义各平台的推广渠道、特性等
"""

# 平台推广渠道配置
PLATFORM_CHANNELS = {
    '京东': ['京东快车', '京东展位', '京准通', '品牌特秀'],
    '天猫': ['直通车', '钻展', '超级推荐', '品牌特秀'],
    '抖音': ['巨量千川', '抖音小店随心推', 'DOU+', '品牌广告'],
    '快手': ['磁力金牛', '快手粉条', '快手小店推广', '品牌广告'],
    '微信': ['朋友圈广告', '公众号广告', '小程序广告', '视频号推广'],
    '小红书': ['信息流广告', '搜索广告', '薯条', '品牌合作'],
    '拼多多': ['多多搜索', '多多场景', '多多进宝', '品牌推广']
}

# 平台特性配置
PLATFORM_FEATURES = {
    '京东': {
        'type': '综合电商',
        'user_group': '中高端用户',
        'avg_order_value': 500,
        'conversion_rate': 0.05,
    },
    '天猫': {
        'type': '综合电商',
        'user_group': '品质用户',
        'avg_order_value': 450,
        'conversion_rate': 0.048,
    },
    '抖音': {
        'type': '内容电商',
        'user_group': '年轻用户',
        'avg_order_value': 300,
        'conversion_rate': 0.06,
    },
    '快手': {
        'type': '内容电商',
        'user_group': '下沉市场',
        'avg_order_value': 250,
        'conversion_rate': 0.055,
    },
    '微信': {
        'type': '社交电商',
        'user_group': '熟人社交',
        'avg_order_value': 350,
        'conversion_rate': 0.07,
    },
    '小红书': {
        'type': '种草电商',
        'user_group': '女性用户',
        'avg_order_value': 400,
        'conversion_rate': 0.045,
    },
    '拼多多': {
        'type': '社交电商',
        'user_group': '价格敏感',
        'avg_order_value': 200,
        'conversion_rate': 0.08,
    }
}


def get_platform_channels(platform):
    """
    获取平台的推广渠道列表
    
    Args:
        platform: 平台名称
    
    Returns:
        list: 推广渠道列表
    """
    return PLATFORM_CHANNELS.get(platform, ['通用推广'])


def get_platform_features(platform):
    """
    获取平台特性
    
    Args:
        platform: 平台名称
    
    Returns:
        dict: 平台特性配置
    """
    return PLATFORM_FEATURES.get(platform, {
        'type': '综合电商',
        'user_group': '通用用户',
        'avg_order_value': 350,
        'conversion_rate': 0.05,
    })


def get_all_platforms():
    """
    获取所有平台列表
    
    Returns:
        list: 平台名称列表
    """
    return list(PLATFORM_CHANNELS.keys())
