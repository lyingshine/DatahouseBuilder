"""
ODS层数据生成入口脚本
使用新的生成器模块
"""
import sys
import json
import os
from pathlib import Path

# 添加脚本目录到路径
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / 'scripts'))

from generators import StoreGenerator, UserGenerator, ProductGenerator, OrderGenerator
from config import get_category_config
from business_scale import get_scale_summary
from traffic_distribution import TrafficDistributor
from conversion_engine import ConversionEngine


def main():
    """主函数"""
    # 读取配置
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except Exception as e:
            print(f"配置解析失败: {e}")
            sys.exit(1)
    
    # 提取配置参数
    platform_stores_raw = config.get('platformStores', {})
    business_scale = config.get('businessScale', '小型企业')
    time_span_days = config.get('timeSpanDays', 365)
    main_category = config.get('mainCategory', 'bicycle')
    
    # 转换平台店铺格式（兼容新旧格式）
    platform_stores = {}
    for platform, stores in platform_stores_raw.items():
        if isinstance(stores, list):
            platform_stores[platform] = stores
        elif isinstance(stores, dict):
            # 合并品牌店和白牌店
            platform_stores[platform] = stores.get('品牌', []) + stores.get('白牌', [])
        else:
            platform_stores[platform] = []
    
    # 计算店铺总数
    total_stores = sum(len(stores) for stores in platform_stores.values())
    
    # 获取体量摘要
    scale_summary = get_scale_summary(business_scale, total_stores, time_span_days)
    num_users = max(100, int(scale_summary['total_clicks'] / 10))
    
    # 获取类目配置
    category_config = get_category_config(main_category)
    
    # 打印配置信息
    print("="*60)
    print("ODS层数据生成（企业体量驱动）")
    print("="*60)
    print(f"企业体量: {business_scale}")
    print(f"主营类目: {category_config['name']}")
    print(f"平台店铺: {len(platform_stores)} 个平台, {total_stores} 家店铺")
    print(f"时间跨度: {time_span_days} 天")
    print(f"预估订单: {scale_summary['estimated_orders']:,} 单")
    print(f"预估用户: {num_users:,} 个")
    print("="*60)
    
    # 数据目录
    data_dir = BASE_DIR / 'data' / 'ods'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. 生成店铺数据
        print("\n【步骤 1/8】生成店铺数据")
        store_gen = StoreGenerator(platform_stores)
        stores_df = store_gen.generate()
        store_gen.save_to_csv(stores_df, data_dir / 'ods_stores.csv')
        
        # 2. 生成商品数据
        print("\n【步骤 2/8】生成商品数据")
        product_gen = ProductGenerator(stores_df, category_config)
        products_df = product_gen.generate()
        product_gen.save_to_csv(products_df, data_dir / 'ods_products.csv')
        
        # 3. 生成用户数据
        print("\n【步骤 3/8】生成用户数据")
        user_gen = UserGenerator(num_users, time_span_days)
        users_df = user_gen.generate()
        user_gen.save_to_csv(users_df, data_dir / 'ods_users.csv')
        
        # 4. 生成流量数据（使用流量分发器）
        print("\n【步骤 4/8】生成流量数据")
        distributor = TrafficDistributor(products_df, time_span_days)
        traffic_df = distributor.distribute_traffic()
        print(f"   ✓ 生成流量: {len(traffic_df):,} 条记录")
        
        # 5. 从流量转化为订单（使用转化引擎）
        print("\n【步骤 5/8】从流量生成订单")
        engine = ConversionEngine(traffic_df, products_df, users_df, stores_df)
        orders_df, order_details_df = engine.generate_orders_from_traffic(
            target_order_count=scale_summary['estimated_orders']
        )
        
        # 保存订单数据
        orders_df.to_csv(data_dir / 'ods_orders.csv', index=False, encoding='utf-8-sig')
        print(f"   ✓ 已保存: ods_orders.csv ({len(orders_df):,} 行)")
        
        order_details_df.to_csv(data_dir / 'ods_order_details.csv', index=False, encoding='utf-8-sig')
        print(f"   ✓ 已保存: ods_order_details.csv ({len(order_details_df):,} 行)")
        
        # 6. 拆分流量数据为推广表和商品流量表
        print("\n【步骤 6/8】拆分流量数据")
        
        # 付费推广表
        promotion_df = traffic_df[traffic_df['流量类型'] == '付费'].copy()
        promotion_df['推广ID'] = [f'PM{i:08d}' for i in range(1, len(promotion_df) + 1)]
        promotion_df = promotion_df[[
            '推广ID', '日期', '店铺ID', '平台', '商品ID', 
            '一级类目', '二级类目', '流量渠道', '推广费用', 
            '曝光量', '点击量', '点击率'
        ]]
        promotion_df.columns = [
            'promotion_id', 'date', 'store_id', 'platform', 'product_id',
            'category_l1', 'category_l2', 'channel', 'cost',
            'impressions', 'clicks', 'ctr'
        ]
        promotion_df.to_csv(data_dir / 'ods_promotion.csv', index=False, encoding='utf-8-sig')
        print(f"   ✓ 已保存: ods_promotion.csv ({len(promotion_df):,} 行)")
        
        # 商品自然流量表
        product_traffic_df = traffic_df[traffic_df['流量类型'] == '自然'].copy()
        product_traffic_df = product_traffic_df[[
            '日期', '店铺ID', '平台', 'SKU_ID', '商品ID',
            '一级类目', '二级类目', '流量渠道', '曝光量', '点击量'
        ]]
        product_traffic_df.columns = [
            'date', 'store_id', 'platform', 'sku_id', 'product_id',
            'category_l1', 'category_l2', 'channel', 'impressions', 'clicks'
        ]
        # 添加收藏量和加购量（模拟）
        import random
        product_traffic_df['favorites'] = product_traffic_df['clicks'].apply(
            lambda x: int(x * random.uniform(0.1, 0.3))
        )
        product_traffic_df['add_to_cart'] = product_traffic_df['clicks'].apply(
            lambda x: int(x * random.uniform(0.2, 0.5))
        )
        product_traffic_df.to_csv(data_dir / 'ods_product_traffic.csv', index=False, encoding='utf-8-sig')
        print(f"   ✓ 已保存: ods_product_traffic.csv ({len(product_traffic_df):,} 行)")
        
        # 7. 生成店铺流量汇总表
        print("\n【步骤 7/8】生成店铺流量汇总")
        store_traffic = traffic_df.groupby(['日期', '店铺ID', '平台']).agg({
            '曝光量': 'sum',
            '点击量': 'sum'
        }).reset_index()
        
        # 添加其他流量指标
        store_traffic['visitors'] = (store_traffic['点击量'] * 0.8).astype(int)
        store_traffic['page_views'] = (store_traffic['点击量'] * 1.5).astype(int)
        store_traffic['search_traffic'] = (store_traffic['visitors'] * 0.4).astype(int)
        store_traffic['recommend_traffic'] = (store_traffic['visitors'] * 0.3).astype(int)
        store_traffic['direct_traffic'] = (store_traffic['visitors'] * 0.2).astype(int)
        store_traffic['other_traffic'] = (store_traffic['visitors'] * 0.1).astype(int)
        store_traffic['avg_stay_time'] = store_traffic['visitors'].apply(
            lambda x: round(random.uniform(60, 300), 2)
        )
        store_traffic['bounce_rate'] = store_traffic['visitors'].apply(
            lambda x: round(random.uniform(30, 70), 2)
        )
        
        store_traffic = store_traffic[[
            '日期', '店铺ID', '平台', 'visitors', 'page_views',
            'search_traffic', 'recommend_traffic', 'direct_traffic', 'other_traffic',
            'avg_stay_time', 'bounce_rate'
        ]]
        store_traffic.columns = [
            'date', 'store_id', 'platform', 'visitors', 'page_views',
            'search_traffic', 'recommend_traffic', 'direct_traffic', 'other_traffic',
            'avg_stay_time', 'bounce_rate'
        ]
        store_traffic.to_csv(data_dir / 'ods_traffic.csv', index=False, encoding='utf-8-sig')
        print(f"   ✓ 已保存: ods_traffic.csv ({len(store_traffic):,} 行)")
        
        # 8. 生成库存数据（简化版）
        print("\n【步骤 8/8】生成库存数据")
        inventory_records = []
        inventory_id = 1
        
        # 为每个商品生成初始库存记录
        for _, product in products_df.iterrows():
            inventory_records.append({
                'inventory_id': f'INV{inventory_id:08d}',
                'date': users_df['注册日期'].min(),
                'product_id': product['SKU_ID'],
                'store_id': product['店铺ID'],
                'change_type': '入库',
                'change_quantity': product['库存'],
                'stock_quantity': product['库存'],
                'remark': '初始库存'
            })
            inventory_id += 1
        
        import pandas as pd
        inventory_df = pd.DataFrame(inventory_records)
        inventory_df.to_csv(data_dir / 'ods_inventory.csv', index=False, encoding='utf-8-sig')
        print(f"   ✓ 已保存: ods_inventory.csv ({len(inventory_df):,} 行)")
        
        print("\n" + "="*60)
        print("✓ ODS层数据生成完成！")
        print("="*60)
        
        return 0
        
    except Exception as e:
        print(f"\n✗ 数据生成失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
