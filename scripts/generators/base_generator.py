"""
基础生成器类
定义生成器的通用接口和方法
"""
import pandas as pd
from abc import ABC, abstractmethod


class BaseGenerator(ABC):
    """数据生成器基类"""
    
    def __init__(self, config=None):
        """
        初始化生成器
        
        Args:
            config: 配置字典
        """
        self.config = config or {}
    
    @abstractmethod
    def generate(self):
        """
        生成数据（抽象方法，子类必须实现）
        
        Returns:
            pd.DataFrame: 生成的数据
        """
        pass
    
    def validate(self, df):
        """
        验证生成的数据
        
        Args:
            df: 待验证的数据框
        
        Returns:
            bool: 是否验证通过
        """
        if df is None or df.empty:
            return False
        return True
    
    def save_to_csv(self, df, filepath):
        """
        保存数据到CSV文件
        
        Args:
            df: 数据框
            filepath: 文件路径
        """
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"   ✓ 已保存: {filepath} ({len(df):,} 行)")
