"""
用户生成器
负责生成用户数据
"""
import pandas as pd
import random
from faker import Faker
from .base_generator import BaseGenerator

fake = Faker('zh_CN')


class UserGenerator(BaseGenerator):
    """用户数据生成器"""
    
    def __init__(self, num_users=3000, time_span_days=365, config=None):
        """
        初始化用户生成器
        
        Args:
            num_users: 用户数量
            time_span_days: 时间跨度（天）
            config: 额外配置
        """
        super().__init__(config)
        self.num_users = num_users
        self.time_span_days = time_span_days
    
    def generate(self):
        """
        生成用户数据（批量优化）
        
        Returns:
            pd.DataFrame: 用户数据
        """
        print(f"   正在生成 {self.num_users:,} 个用户...")
        
        # 预生成数据
        cities = [fake.city() for _ in range(50)]
        genders = ['男', '女']
        
        # 计算注册日期范围（在订单时间跨度之前）
        start_date = f'-{self.time_span_days + 180}d'  # 提前6个月
        end_date = f'-{max(1, self.time_span_days // 4)}d'  # 到时间跨度的1/4处
        
        # 批量生成
        users = {
            '用户ID': [f'U{i:08d}' for i in range(1, self.num_users + 1)],
            '用户名': [f'用户{i}' for i in range(1, self.num_users + 1)],
            '性别': [random.choice(genders) for _ in range(self.num_users)],
            '年龄': [random.randint(18, 65) for _ in range(self.num_users)],
            '城市': [random.choice(cities) for _ in range(self.num_users)],
            '注册日期': [
                fake.date_between(start_date=start_date, end_date=end_date)
                for _ in range(self.num_users)
            ]
        }
        
        df = pd.DataFrame(users)
        print(f"   ✓ 生成用户: {len(df):,} 个")
        return df
