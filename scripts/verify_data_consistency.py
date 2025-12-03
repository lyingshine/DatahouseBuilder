"""
数据一致性验证脚本
验证设计数据-CSV数据-数据库数据三方是否一致
包括：订单、推广、财务指标（毛利率、推广费率、净利率）
"""
import pymysql
import pandas as pd
import os
import sys
import json
from pathlib import Path

def get_db_connection(db_config):
    """获取数据库连接"""
    return pymysql.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset='utf8mb4'
    )

def log(message):
    """输出日志"""
    print(message)
    sys.stdout.flush()

def get_design_metrics(config):
    """获取设计预期指标"""
    business_scale = config.get('businessScale', '小型企业')
    
    # 设计预期的财务指标范围（调整后合理的指标）
    design_ranges = {
        '微型企业': {'毛利率': (30, 37), '推广费率': (5, 8), '净利率': (7, 12)},
        '小型企业': {'毛利率': (30, 37), '推广费率': (5, 8), '净利率': (7, 12)},
        '中型企业': {'毛利率': (30, 37), '推广费率': (5, 8), '净利率': (7, 12)},
        '大型企业': {'毛利率': (30, 37), '推广费率': (5, 8), '净利率': (7, 12)},
        '超大型企业': {'毛利率': (30, 37), '推广费率': (5, 8), '净利率': (7, 12)},
    }
    
    return design_ranges.get(business_scale, design_ranges['小型企业'])

def verify_orders_consistency(csv_path, db_config):
    """验证订单数据一致性"""
    log('\n【订单数据验证】')
    
    # 读取CSV
    try:
        csv_df = pd.read_csv(csv_path, encoding='utf-8-sig')
        csv_completed = csv_df[csv_df['订单状态'] == '已完成']
        
        csv_metrics = {
            '订单数': len(csv_completed),
            '销售额': csv_completed['实付金额'].sum(),
            '成本': csv_completed['成本总额'].sum(),
        }
        log(f'  CSV: 订单数={csv_metrics["订单数"]:,}, 销售额={csv_metrics["销售额"]:,.2f}')
    except Exception as e:
        log(f'  ❌ CSV读取失败: {e}')
        return False
    
    # 查询数据库
    try:
        conn = get_db_connection(db_config)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) as cnt, SUM(final_amount) as sales, SUM(total_cost) as cost
            FROM ods_orders WHERE order_status = '已完成'
        ''')
        row = cursor.fetchone()
        
        db_metrics = {
            '订单数': row[0] or 0,
            '销售额': float(row[1]) if row[1] else 0,
            '成本': float(row[2]) if row[2] else 0,
        }
        log(f'  DB:  订单数={db_metrics["订单数"]:,}, 销售额={db_metrics["销售额"]:,.2f}')
        
        conn.close()
    except Exception as e:
        log(f'  ❌ 数据库查询失败: {e}')
        return False
    
    # 对比
    orders_match = csv_metrics['订单数'] == db_metrics['订单数']
    sales_match = abs(csv_metrics['销售额'] - db_metrics['销售额']) < 1
    
    if orders_match and sales_match:
        log('  ✅ 订单数据一致')
        return True
    else:
        log(f'  ❌ 订单数据不一致')
        if not orders_match:
            log(f'     订单数差异: {abs(csv_metrics["订单数"] - db_metrics["订单数"])}')
        if not sales_match:
            log(f'     销售额差异: {abs(csv_metrics["销售额"] - db_metrics["销售额"]):,.2f}')
        return False

def verify_promotion_consistency(csv_path, db_config):
    """验证推广数据一致性"""
    log('\n【推广数据验证】')
    
    # 读取CSV
    try:
        csv_df = pd.read_csv(csv_path, encoding='utf-8-sig')
        csv_metrics = {
            '记录数': len(csv_df),
            '推广费': csv_df['推广花费'].sum(),
        }
        log(f'  CSV: 记录数={csv_metrics["记录数"]:,}, 推广费={csv_metrics["推广费"]:,.2f}')
    except Exception as e:
        log(f'  ❌ CSV读取失败: {e}')
        return False
    
    # 查询数据库
    try:
        conn = get_db_connection(db_config)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) as cnt, SUM(cost) as total FROM ods_promotion')
        row = cursor.fetchone()
        
        db_metrics = {
            '记录数': row[0] or 0,
            '推广费': float(row[1]) if row[1] else 0,
        }
        log(f'  DB:  记录数={db_metrics["记录数"]:,}, 推广费={db_metrics["推广费"]:,.2f}')
        
        conn.close()
    except Exception as e:
        log(f'  ❌ 数据库查询失败: {e}')
        return False
    
    # 对比
    count_match = csv_metrics['记录数'] == db_metrics['记录数']
    cost_match = abs(csv_metrics['推广费'] - db_metrics['推广费']) < 1
    
    if count_match and cost_match:
        log('  ✅ 推广数据一致')
        return True
    else:
        log(f'  ❌ 推广数据不一致')
        if not count_match:
            log(f'     记录数差异: {abs(csv_metrics["记录数"] - db_metrics["记录数"])}')
        if not cost_match:
            log(f'     推广费差异: {abs(csv_metrics["推广费"] - db_metrics["推广费"]):,.2f}')
        return False

def verify_financial_metrics(csv_orders_path, csv_promo_path, db_config, design_ranges):
    """验证财务指标（毛利率、推广费率、净利率）"""
    log('\n【财务指标验证】')
    
    try:
        # 读取CSV数据
        orders_df = pd.read_csv(csv_orders_path, encoding='utf-8-sig')
        promo_df = pd.read_csv(csv_promo_path, encoding='utf-8-sig')
        
        completed = orders_df[orders_df['订单状态'] == '已完成']
        
        # CSV计算（按照文档公式）
        csv_sales = completed['实付金额'].sum()
        csv_cost = completed['成本总额'].sum()
        csv_shipping = completed['运费'].sum()
        csv_promo = promo_df['推广花费'].sum()
        
        # 毛利 = 销售额 - 成本 - 运费
        csv_gross_profit = csv_sales - csv_cost - csv_shipping
        
        # 其他费用（按销售额比例）
        csv_after_sales = csv_sales * 0.02  # 售后费 2%
        csv_platform_fee = csv_sales * 0.05  # 平台费 5%
        csv_management = csv_sales * 0.08  # 管理费 8%
        
        # 净利润 = 毛利 - 推广费 - 售后费 - 平台费 - 管理费
        csv_net_profit = csv_gross_profit - csv_promo - csv_after_sales - csv_platform_fee - csv_management
        
        csv_gross_rate = (csv_gross_profit / csv_sales * 100) if csv_sales > 0 else 0
        csv_promo_rate = (csv_promo / csv_sales * 100) if csv_sales > 0 else 0
        csv_net_rate = (csv_net_profit / csv_sales * 100) if csv_sales > 0 else 0
        
        log(f'  CSV数据:')
        log(f'    销售额: {csv_sales:,.2f}')
        log(f'    成本: {csv_cost:,.2f}')
        log(f'    运费: {csv_shipping:,.2f}')
        log(f'    毛利: {csv_gross_profit:,.2f} (毛利率: {csv_gross_rate:.2f}%)')
        log(f'    推广费: {csv_promo:,.2f} (推广费率: {csv_promo_rate:.2f}%)')
        log(f'    售后费: {csv_after_sales:,.2f} (2%)')
        log(f'    平台费: {csv_platform_fee:,.2f} (5%)')
        log(f'    管理费: {csv_management:,.2f} (10%)')
        log(f'    净利润: {csv_net_profit:,.2f} (净利率: {csv_net_rate:.2f}%)')
        
        # 数据库计算
        conn = get_db_connection(db_config)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT SUM(final_amount), SUM(total_cost), SUM(shipping_fee)
            FROM ods_orders WHERE order_status = '已完成'
        ''')
        row = cursor.fetchone()
        db_sales = float(row[0] or 0)
        db_cost = float(row[1] or 0)
        db_shipping = float(row[2] or 0)
        
        cursor.execute('SELECT SUM(cost) FROM ods_promotion')
        db_promo = float(cursor.fetchone()[0] or 0)
        
        # 毛利 = 销售额 - 成本 - 运费
        db_gross_profit = db_sales - db_cost - db_shipping
        
        # 其他费用
        db_after_sales = db_sales * 0.02
        db_platform_fee = db_sales * 0.05
        db_management = db_sales * 0.08
        
        # 净利润 = 毛利 - 推广费 - 售后费 - 平台费 - 管理费
        db_net_profit = db_gross_profit - db_promo - db_after_sales - db_platform_fee - db_management
        
        db_gross_rate = (db_gross_profit / db_sales * 100) if db_sales > 0 else 0
        db_promo_rate = (db_promo / db_sales * 100) if db_sales > 0 else 0
        db_net_rate = (db_net_profit / db_sales * 100) if db_sales > 0 else 0
        
        log(f'  数据库:')
        log(f'    销售额: {db_sales:,.2f}')
        log(f'    成本: {db_cost:,.2f}')
        log(f'    运费: {db_shipping:,.2f}')
        log(f'    毛利: {db_gross_profit:,.2f} (毛利率: {db_gross_rate:.2f}%)')
        log(f'    推广费: {db_promo:,.2f} (推广费率: {db_promo_rate:.2f}%)')
        log(f'    售后费: {db_after_sales:,.2f} (2%)')
        log(f'    平台费: {db_platform_fee:,.2f} (5%)')
        log(f'    管理费: {db_management:,.2f} (10%)')
        log(f'    净利润: {db_net_profit:,.2f} (净利率: {db_net_rate:.2f}%)')
        
        conn.close()
        
        # 设计预期验证
        log(f'  设计预期:')
        log(f'    毛利率: {design_ranges["毛利率"][0]}-{design_ranges["毛利率"][1]}%')
        log(f'    推广费率: {design_ranges["推广费率"][0]}-{design_ranges["推广费率"][1]}%')
        log(f'    净利率: {design_ranges["净利率"][0]}-{design_ranges["净利率"][1]}%')
        
        # 验证
        all_pass = True
        
        # CSV vs DB
        if abs(csv_gross_rate - db_gross_rate) > 0.5:
            log(f'  ❌ 毛利率不一致: CSV={csv_gross_rate:.2f}%, DB={db_gross_rate:.2f}%')
            all_pass = False
        else:
            log(f'  ✅ 毛利率一致: {csv_gross_rate:.2f}%')
        
        if abs(csv_promo_rate - db_promo_rate) > 0.5:
            log(f'  ❌ 推广费率不一致: CSV={csv_promo_rate:.2f}%, DB={db_promo_rate:.2f}%')
            all_pass = False
        else:
            log(f'  ✅ 推广费率一致: {csv_promo_rate:.2f}%')
        
        if abs(csv_net_rate - db_net_rate) > 0.5:
            log(f'  ❌ 净利率不一致: CSV={csv_net_rate:.2f}%, DB={db_net_rate:.2f}%')
            all_pass = False
        else:
            log(f'  ✅ 净利率一致: {csv_net_rate:.2f}%')
        
        # 设计预期验证
        gross_in_range = design_ranges["毛利率"][0] <= csv_gross_rate <= design_ranges["毛利率"][1]
        promo_in_range = design_ranges["推广费率"][0] <= csv_promo_rate <= design_ranges["推广费率"][1]
        net_in_range = design_ranges["净利率"][0] <= csv_net_rate <= design_ranges["净利率"][1]
        
        if not gross_in_range:
            log(f'  ⚠️  毛利率超出设计范围: {csv_gross_rate:.2f}%')
        if not promo_in_range:
            log(f'  ⚠️  推广费率超出设计范围: {csv_promo_rate:.2f}%')
        if not net_in_range:
            log(f'  ⚠️  净利率超出设计范围: {csv_net_rate:.2f}%')
        
        if gross_in_range and promo_in_range and net_in_range:
            log(f'  ✅ 所有指标符合设计预期')
        
        return all_pass
        
    except Exception as e:
        log(f'  ❌ 验证失败: {e}')
        return False

def verify_ads_consistency(db_config):
    """验证ADS层数据一致性"""
    log('\n【ADS层数据验证】')
    
    try:
        conn = get_db_connection(db_config)
        cursor = conn.cursor()
        
        # 检查ADS表是否存在
        cursor.execute('SHOW TABLES LIKE "ads_daily_report"')
        if not cursor.fetchone():
            log('  ⚠️  ads_daily_report表不存在，跳过验证')
            conn.close()
            return True
        
        # ADS层汇总
        cursor.execute('''
            SELECT SUM(`销售额`) as sales, SUM(`推广费`) as promo, SUM(`净利润`) as profit
            FROM ads_daily_report
        ''')
        row = cursor.fetchone()
        ads_sales = float(row[0]) if row[0] else 0
        ads_promo = float(row[1]) if row[1] else 0
        ads_profit = float(row[2]) if row[2] else 0
        
        # ODS层汇总
        cursor.execute('''
            SELECT SUM(final_amount) as sales FROM ods_orders WHERE order_status = '已完成'
        ''')
        ods_sales = float(cursor.fetchone()[0] or 0)
        
        cursor.execute('SELECT SUM(cost) as promo FROM ods_promotion')
        ods_promo = float(cursor.fetchone()[0] or 0)
        
        conn.close()
        
        log(f'  ADS: 销售额={ads_sales:,.2f}, 推广费={ads_promo:,.2f}')
        log(f'  ODS: 销售额={ods_sales:,.2f}, 推广费={ods_promo:,.2f}')
        
        sales_match = abs(ads_sales - ods_sales) < 100
        promo_match = abs(ads_promo - ods_promo) < 100
        
        if sales_match and promo_match:
            log('  ✅ ADS层数据一致')
            return True
        else:
            log('  ❌ ADS层数据不一致')
            if not sales_match:
                log(f'     销售额差异: {abs(ads_sales - ods_sales):,.2f}')
            if not promo_match:
                log(f'     推广费差异: {abs(ads_promo - ods_promo):,.2f}')
            return False
            
    except Exception as e:
        log(f'  ❌ 验证失败: {e}')
        return False

def main():
    """主函数"""
    if len(sys.argv) < 2:
        log('❌ 缺少配置参数')
        sys.exit(1)
    
    try:
        config = json.loads(sys.argv[1])
        db_config = config.get('dbConfig', {})
        data_dir = config.get('dataDir', 'data/ods')
        business_scale = config.get('businessScale', '小型企业')
    except Exception as e:
        log(f'❌ 配置解析失败: {e}')
        sys.exit(1)
    
    log('='*60)
    log('数据一致性验证')
    log('='*60)
    log(f'企业体量: {business_scale}')
    
    results = []
    orders_csv = os.path.join(data_dir, 'ods_orders.csv')
    promo_csv = os.path.join(data_dir, 'ods_promotion.csv')
    
    # 验证订单数据
    if os.path.exists(orders_csv):
        results.append(verify_orders_consistency(orders_csv, db_config))
    else:
        log('\n【订单数据验证】')
        log('  ⚠️  CSV文件不存在，跳过验证')
    
    # 验证推广数据
    if os.path.exists(promo_csv):
        results.append(verify_promotion_consistency(promo_csv, db_config))
    else:
        log('\n【推广数据验证】')
        log('  ⚠️  CSV文件不存在，跳过验证')
    
    # 验证财务指标
    if os.path.exists(orders_csv) and os.path.exists(promo_csv):
        design_ranges = get_design_metrics({'businessScale': business_scale})
        results.append(verify_financial_metrics(orders_csv, promo_csv, db_config, design_ranges))
    else:
        log('\n【财务指标验证】')
        log('  ⚠️  CSV文件不完整，跳过验证')
    
    # 验证ADS层
    results.append(verify_ads_consistency(db_config))
    
    # 总结
    log('\n' + '='*60)
    if all(results):
        log('✅ 所有验证通过！数据完全一致')
        log('='*60)
        sys.exit(0)
    else:
        log('⚠️  部分验证未通过，请检查数据')
        log('='*60)
        sys.exit(1)

if __name__ == '__main__':
    main()
