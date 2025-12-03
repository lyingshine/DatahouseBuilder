"""
店铺生成器
负责生成店铺数据
"""
import pandas as pd
from faker import Faker
from .base_generator import BaseGenerator

fake = Faker('zh_CN')


class StoreGenerator(BaseGenerator):
    """店铺数据生成器"""
    
    def __init__(self, platform_stores, config=None):
        """
        初始化店铺生成器
        
        Args:
            platform_stores: 平台店铺配置
                {
                    '京东': ['店铺1', '店铺2'],
                    '天猫': ['店铺1', '店铺2']
                }
            config: 额外配置
        """
        super().__init__(config)
        self.platform_stores = platform_stores
    
    def generate(self):
        """
        生成店铺数据
        
        Returns:
            pd.DataFrame: 店铺数据
        """
        stores = []
        store_id = 1
        
        for platform, store_list in self.platform_stores.items():
            for store_name in store_list:
                # 店铺名称格式：【平台名】店铺名
                full_store_name = f'【{platform}】{store_name}'
                
                # 判断店铺类型
                store_type = self._get_store_type(store_name)
                
                stores.append({
                    '店铺ID': f'S{store_id:04d}',
                    '店铺名称': full_store_name,
                    '店铺类型': store_type,
                    '平台': platform,
                    '开店日期': fake.date_between(start_date='-3y', end_date='-1y')
                })
                store_id += 1
        
        df = pd.DataFrame(stores)
        print(f"   ✓ 生成店铺: {len(df)} 家")
        return df
    
    def _get_store_type(self, store_name):
        """
        根据店铺名称判断店铺类型
        
        Args:
            store_name: 店铺名称
        
        Returns:
            str: 店铺类型（'品牌' 或 '白牌'）
        """
        # 简单规则：包含"品牌"、"旗舰"、"官方"的为品牌店
        brand_keywords = ['品牌', '旗舰', '官方', '直营']
        for keyword in brand_keywords:
            if keyword in store_name:
                return '品牌'
        return '白牌'
