"""
商品生成器
负责生成商品数据（SPU/SKU）
"""
import pandas as pd
import random
from .base_generator import BaseGenerator
from config import get_tier_config, get_profit_margin, PRODUCT_TIERS


# 整车SKU属性选项（中文）
BIKE_FRAMES = ['铁架', '铝架', '钢架']
BIKE_SPEEDS = ['7速', '21速', '24速', '27速']
BIKE_SIZES = ['24寸', '26寸', '27寸', '29寸']
COLORS = ['黑色', '白色', '红色', '蓝色', '绿色']

# 装备SKU属性选项（中文）
EQUIP_SIZES = ['S码', 'M码', 'L码']


class ProductGenerator(BaseGenerator):
    """商品数据生成器"""
    
    def __init__(self, stores_df, category_config, config=None):
        """
        初始化商品生成器
        
        Args:
            stores_df: 店铺数据
            category_config: 类目配置
            config: 额外配置
        """
        super().__init__(config)
        self.stores_df = stores_df
        self.category_config = category_config
    
    def generate(self):
        """
        生成商品数据
        
        Returns:
            pd.DataFrame: 商品数据
        """
        # 先生成SPU商品库
        spu_library = self._generate_spu_library()
        
        print(f"   SPU商品库: 品牌 {len(spu_library['品牌'])} 款, "
              f"白牌 {len(spu_library['白牌'])} 款")
        
        products = []
        global_product_id = 1
        global_sku_id = 1
        
        for _, store in self.stores_df.iterrows():
            store_id = store['店铺ID']
            store_type = store['店铺类型']
            platform = store['平台']
            open_date = store['开店日期']
            
            # 根据店铺类型选择对应的SPU库
            store_spus = spu_library.get(store_type, [])
            
            # 该店铺上架所有对应类型的产品
            for spu in store_spus:
                spu_code = spu['SPU编码']
                base_price = spu['基础价格']
                cost_rate = spu['成本率']
                
                # 为该店铺的这个SPU分配唯一的平台商品ID
                platform_product_id = f'P{global_product_id:08d}'
                global_product_id += 1
                
                # 为该商品ID下的所有SKU规格生成记录
                for spec in spu['规格列表']:
                    internal_sku_code = spec['SKU编码']
                    sku_price = round(base_price * spec['价格系数'], 2)
                    
                    # 为该SKU分配唯一的平台SKU ID
                    platform_sku_id = f'SK{global_sku_id:08d}'
                    global_sku_id += 1
                    
                    products.append({
                        'SKU_ID': platform_sku_id,
                        '商品ID': platform_product_id,
                        '产品编码': spu_code,
                        '规格编码': internal_sku_code,
                        '店铺ID': store_id,
                        '平台': platform,
                        '商品名称': spu['商品名称'],
                        '规格': spec['规格'],
                        '一级类目': spu['一级类目'],
                        '二级类目': spu['二级类目'],
                        '商品分层': spu['商品分层'],
                        '售价': sku_price,
                        '成本': round(sku_price * cost_rate, 2),
                        '库存': random.randint(50, 300),
                        '创建时间': open_date
                    })
        
        df = pd.DataFrame(products)
        
        # 打印统计信息
        total_skus = len(df)
        unique_product_ids = df['商品ID'].nunique()
        unique_spu_codes = df['产品编码'].nunique()
        avg_skus_per_product = total_skus / unique_product_ids if unique_product_ids > 0 else 0
        
        print(f"   ✓ 商品关系统计：")
        print(f"     - 总SKU数: {total_skus:,}")
        print(f"     - 唯一商品ID数: {unique_product_ids:,}")
        print(f"     - 唯一产品编码数: {unique_spu_codes:,}")
        print(f"     - 平均每个商品ID包含 {avg_skus_per_product:.1f} 个SKU")
        
        return df
    
    def _generate_spu_library(self):
        """生成SPU商品库"""
        spu_library = {'品牌': [], '白牌': []}
        categories = self.category_config['categories']
        price_ranges = self.category_config['price_ranges']
        
        # 为每个商品分配分层
        def assign_tier():
            rand = random.random()
            cumulative = 0
            for tier, config in PRODUCT_TIERS.items():
                cumulative += config['ratio']
                if rand < cumulative:
                    return tier, config
            return '畅销品', PRODUCT_TIERS['畅销品']
        
        # 品牌整车
        for sub_cat in categories.get('整车-品牌', []):
            for i in range(1, 6):
                spu = self._create_spu(
                    '品牌', sub_cat, i, '整车-品牌',
                    price_ranges.get('整车-品牌', (800, 3000)),
                    assign_tier, is_bike=True
                )
                spu_library['品牌'].append(spu)
        
        # 品牌装备
        for sub_cat in categories.get('骑行装备', []):
            for i in range(1, 3):
                spu = self._create_spu(
                    '品牌', sub_cat, i, '骑行装备',
                    price_ranges.get('骑行装备', (30, 300)),
                    assign_tier, is_bike=False, profit_bonus=0.10
                )
                spu_library['品牌'].append(spu)
        
        # 白牌整车
        for sub_cat in categories.get('整车-白牌', []):
            for i in range(1, 9):
                spu = self._create_spu(
                    '白牌', sub_cat, i, '整车-白牌',
                    price_ranges.get('整车-白牌', (200, 800)),
                    assign_tier, is_bike=True, profit_bonus=0.05
                )
                spu_library['白牌'].append(spu)
        
        # 白牌装备
        for sub_cat in categories.get('骑行装备', []):
            for i in range(1, 4):
                spu = self._create_spu(
                    '白牌', sub_cat, i, '骑行装备',
                    price_ranges.get('骑行装备', (30, 300)),
                    assign_tier, is_bike=False, profit_bonus=0.15
                )
                spu_library['白牌'].append(spu)
        
        return spu_library
    
    def _create_spu(self, brand_type, sub_cat, index, category_l1,
                    price_range, assign_tier_func, is_bike=True, profit_bonus=0):
        """创建单个SPU"""
        spu_code = f'{brand_type}-{sub_cat}-{index:02d}'
        price_min, price_max = price_range
        base_price = round(random.uniform(price_min, price_max), 2)
        
        # 根据商品分层设置利润率
        tier, tier_config = assign_tier_func()
        profit_margin = random.uniform(*tier_config['profit_margin']) + profit_bonus
        profit_margin = min(0.65, profit_margin)  # 最高65%
        cost_rate = 1 - profit_margin
        
        # 生成SKU规格列表
        if is_bike:
            sku_specs = self._generate_bike_skus(spu_code)
        else:
            sku_specs = self._generate_equip_skus(spu_code)
        
        return {
            'SPU编码': spu_code,
            '商品名称': sub_cat,
            '一级类目': category_l1,
            '二级类目': sub_cat,
            '基础价格': base_price,
            '成本率': cost_rate,
            '商品分层': tier,
            '规格列表': sku_specs
        }
    
    def _generate_bike_skus(self, spu_code):
        """生成整车SKU"""
        sku_specs = []
        frame = random.choice(BIKE_FRAMES)
        speed = random.choice(BIKE_SPEEDS)
        size = random.choice(BIKE_SIZES)
        
        for color in random.sample(COLORS, 3):
            sku_code = f'{spu_code}-{frame}-{speed}-{size}-{color}'
            spec_name = f'{frame}/{speed}/{size}/{color}'
            price_factor = 1.0 if frame == '铁架' else (1.1 if frame == '钢架' else 1.2)
            
            sku_specs.append({
                'SKU编码': sku_code,
                '规格': spec_name,
                '价格系数': price_factor
            })
        
        return sku_specs
    
    def _generate_equip_skus(self, spu_code):
        """生成装备SKU"""
        sku_specs = []
        color = random.choice(COLORS)
        
        for size in EQUIP_SIZES:
            sku_code = f'{spu_code}-{size}-{color}'
            spec_name = f'{size}/{color}'
            
            sku_specs.append({
                'SKU编码': sku_code,
                '规格': spec_name,
                '价格系数': 1.0
            })
        
        return sku_specs
