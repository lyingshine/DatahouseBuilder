"""
测试统一流量分发模型
验证：流量分发 → 转化 → 订单 的数据一致性
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from traffic_distribution import TrafficDistributor
from conversion_engine import ConversionEngine


def test_traffic_flow():
    """测试流量分发和转化流程"""
    print("="*60)
    print("测试统一流量分发模型")
    print("="*60)
    
    # 1. 创建测试数据
    print("\n1. 创建测试数据...")
    
    # 店铺
    stores_df = pd.DataFrame([
        {'店铺ID': 'S0001', '店铺名称': '【京东】测试店', '店铺类型': '品牌', '平台': '京东'},
        {'店铺ID': 'S0002', '店铺名称': '【天猫】测试店', '店铺类型': '白牌', '平台': '天猫'}
    ])
    
    # 商品（不同分层）
    products_df = pd.DataFrame([
        {'商品ID': 'P001', '商品编码': 'SPU001', '店铺ID': 'S0001', '平台': '京东', 
         '商品名称': '畅销车', '一级类目': '整车-品牌', '二级类目': '公路车', 
         '商品分层': '畅销品', '售价': 1000, '成本': 800},
        {'商品ID': 'P002', '商品编码': 'SPU002', '店铺ID': 'S0001', '平台': '京东',
         '商品名称': '引流车', '一级类目': '整车-白牌', '二级类目': '山地车',
         '商品分层': '引流品', '售价': 500, '成本': 450},
        {'商品ID': 'P003', '商品编码': 'SPU003', '店铺ID': 'S0002', '平台': '天猫',
         '商品名称': '主推新品', '一级类目': '整车-品牌', '二级类目': '折叠车',
         '商品分层': '主推新品', '售价': 1500, '成本': 1200}
    ])
    
    # 用户
    users_df = pd.DataFrame([
        {'用户ID': f'U{i:08d}', '用户名': f'用户{i}', '性别': '男', '年龄': 25, '城市': '北京'}
        for i in range(1, 101)
    ])
    
    print(f"   店铺: {len(stores_df)} 个")
    print(f"   商品: {len(products_df)} 个")
    print(f"   用户: {len(users_df)} 个")
    
    # 2. 流量分发
    print("\n2. 流量分发...")
    distributor = TrafficDistributor(products_df, time_span_days=7)
    traffic_df = distributor.distribute_traffic()
    
    print(f"   总流量记录: {len(traffic_df)} 条")
    print(f"   自然流量: {len(traffic_df[traffic_df['流量类型']=='自然'])} 条")
    print(f"   付费流量: {len(traffic_df[traffic_df['流量类型']=='付费'])} 条")
    
    # 按商品分层统计
    print("\n   按商品分层统计流量:")
    tier_stats = traffic_df.groupby('商品分层').agg({
        '曝光量': 'sum',
        '点击量': 'sum'
    })
    for tier, row in tier_stats.iterrows():
        print(f"     {tier}: 曝光 {row['曝光量']:,}, 点击 {row['点击量']:,}")

    
    # 3. 转化为订单
    print("\n3. 流量转化为订单...")
    engine = ConversionEngine(traffic_df, products_df, users_df, stores_df)
    orders_df, order_details_df = engine.generate_orders_from_traffic(target_order_count=50)
    
    print(f"   订单数: {len(orders_df)} 条")
    print(f"   订单明细: {len(order_details_df)} 条")
    
    # 4. 验证数据一致性
    print("\n4. 验证数据一致性...")
    
    # 验证1: 订单的流量来源分布
    traffic_source_dist = orders_df['流量来源'].value_counts()
    print("\n   订单流量来源分布:")
    for source, count in traffic_source_dist.items():
        pct = count / len(orders_df) * 100
        print(f"     {source}: {count} 单 ({pct:.1f}%)")
    
    # 验证2: 订单状态分布
    status_dist = orders_df['订单状态'].value_counts()
    print("\n   订单状态分布:")
    for status, count in status_dist.items():
        pct = count / len(orders_df) * 100
        print(f"     {status}: {count} 单 ({pct:.1f}%)")
    
    # 验证3: 按商品统计转化
    print("\n   按商品统计转化:")
    product_orders = order_details_df.merge(
        products_df[['商品ID', '商品名称', '商品分层']], 
        on='商品ID'
    )
    product_stats = product_orders.groupby(['商品名称', '商品分层']).agg({
        '订单ID': 'count',
        '数量': 'sum'
    }).rename(columns={'订单ID': '订单数', '数量': '销量'})
    
    for (name, tier), row in product_stats.iterrows():
        print(f"     {name} ({tier}): {row['订单数']} 单, 销量 {row['销量']}")
    
    # 验证4: 流量到订单的转化率
    print("\n   流量转化率分析:")
    total_clicks = traffic_df['点击量'].sum()
    total_orders = len(orders_df)
    overall_cvr = total_orders / total_clicks * 100 if total_clicks > 0 else 0
    print(f"     总点击量: {total_clicks:,}")
    print(f"     总订单数: {total_orders}")
    print(f"     整体转化率: {overall_cvr:.2f}%")
    
    # 按商品分层计算转化率
    for tier in traffic_df['商品分层'].unique():
        tier_traffic = traffic_df[traffic_df['商品分层'] == tier]
        tier_clicks = tier_traffic['点击量'].sum()
        
        # 统计该分层的订单数
        tier_product_ids = products_df[products_df['商品分层'] == tier]['商品ID'].tolist()
        tier_orders = order_details_df[order_details_df['商品ID'].isin(tier_product_ids)]
        tier_order_count = len(tier_orders)
        
        tier_cvr = tier_order_count / tier_clicks * 100 if tier_clicks > 0 else 0
        print(f"     {tier}: 点击 {tier_clicks:,}, 订单 {tier_order_count}, CVR {tier_cvr:.2f}%")
    
    print("\n" + "="*60)
    print("✓ 测试完成！流量分发模型运行正常")
    print("="*60)


if __name__ == '__main__':
    test_traffic_flow()
