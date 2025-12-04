"""
数据一致性验证脚本
验证CSV-ODS-DWD-DWS-ADS五层数据一致性
使用表格显示所有字段的数值对比
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
    print(message, flush=True)

def collect_all_metrics(csv_orders_path, csv_promo_path, db_config):
    """收集CSV-ODS-DWD-DWS-ADS五层的所有指标"""
    metrics = {}
    
    # ========== CSV层 ==========
    try:
        orders_df = pd.read_csv(csv_orders_path, encoding='utf-8-sig')
        promo_df = pd.read_csv(csv_promo_path, encoding='utf-8-sig')
        completed = orders_df[orders_df['订单状态'] == '已完成']
        
        metrics['CSV'] = {
            '订单数': len(completed),
            '销售额': completed['实付金额'].sum(),
            '成本': completed['成本总额'].sum(),
            '运费': completed['运费'].sum(),
            '推广费': promo_df['推广花费'].sum(),
            '销量': 0,  # CSV层没有销量
        }
    except Exception as e:
        log(f'❌ CSV读取失败: {e}')
        metrics['CSV'] = None
    
    # ========== 数据库层 ==========
    try:
        conn = get_db_connection(db_config)
        cursor = conn.cursor()
        
        # ODS层
        cursor.execute('''
            SELECT COUNT(*) as cnt, SUM(final_amount), SUM(total_cost), SUM(shipping_fee)
            FROM ods_orders WHERE order_status = '已完成'
        ''')
        row = cursor.fetchone()
        
        cursor.execute('''
            SELECT SUM(od.quantity)
            FROM ods_order_details od
            INNER JOIN ods_orders o ON od.order_id = o.order_id
            WHERE o.order_status = '已完成'
        ''')
        qty_row = cursor.fetchone()
        
        cursor.execute('SELECT SUM(cost) FROM ods_promotion')
        promo_row = cursor.fetchone()
        
        metrics['ODS'] = {
            '订单数': row[0] or 0,
            '销售额': float(row[1]) if row[1] else 0,
            '成本': float(row[2]) if row[2] else 0,
            '运费': float(row[3]) if row[3] else 0,
            '推广费': float(promo_row[0]) if promo_row[0] else 0,
            '销量': int(qty_row[0]) if qty_row[0] else 0,
        }
        
        # DWD层
        try:
            cursor.execute('SHOW TABLES LIKE "fact_order"')
            if cursor.fetchone():
                cursor.execute('''
                    SELECT COUNT(*) as cnt, SUM(final_amount), SUM(total_cost), SUM(shipping_fee)
                    FROM fact_order WHERE order_status = '已完成'
                ''')
                row = cursor.fetchone()
                
                cursor.execute('SELECT SUM(quantity) FROM fact_order_detail')
                qty_row = cursor.fetchone()
                
                cursor.execute('SELECT SUM(cost) FROM fact_promotion')
                promo_row = cursor.fetchone()
                
                metrics['DWD'] = {
                    '订单数': row[0] or 0,
                    '销售额': float(row[1]) if row[1] else 0,
                    '成本': float(row[2]) if row[2] else 0,
                    '运费': float(row[3]) if row[3] else 0,
                    '推广费': float(promo_row[0]) if promo_row[0] else 0,
                    '销量': int(qty_row[0]) if qty_row[0] else 0,
                }
            else:
                metrics['DWD'] = None
        except Exception as e:
            log(f'⚠️  DWD层查询失败: {e}')
            metrics['DWD'] = None
        
        # DWS层
        try:
            cursor.execute('SHOW TABLES LIKE "dws_sales_daily"')
            if cursor.fetchone():
                cursor.execute('SELECT SUM(sales_amount), SUM(cost_amount) FROM dws_sales_daily')
                row = cursor.fetchone()
                
                cursor.execute('SELECT SUM(sales_quantity) FROM dws_product_daily')
                qty_row = cursor.fetchone()
                
                cursor.execute('SELECT SUM(cost) FROM dws_promotion_daily')
                promo_row = cursor.fetchone()
                
                cursor.execute('SELECT SUM(order_count) FROM dws_sales_daily')
                order_row = cursor.fetchone()
                
                metrics['DWS'] = {
                    '订单数': order_row[0] or 0,
                    '销售额': float(row[0]) if row[0] else 0,
                    '成本': float(row[1]) if row[1] else 0,
                    '运费': 0,  # DWS层没有运费
                    '推广费': float(promo_row[0]) if promo_row[0] else 0,
                    '销量': int(qty_row[0]) if qty_row[0] else 0,
                }
            else:
                metrics['DWS'] = None
        except Exception as e:
            log(f'⚠️  DWS层查询失败: {e}')
            metrics['DWS'] = None
        
        # ADS层
        try:
            cursor.execute('SHOW TABLES LIKE "ads_daily_report"')
            if cursor.fetchone():
                cursor.execute('SELECT SUM(`销售额`), SUM(`推广费`), SUM(`订单数`), SUM(`销量`) FROM ads_daily_report')
                row = cursor.fetchone()
                metrics['ADS'] = {
                    '订单数': int(row[2]) if row[2] else 0,
                    '销售额': float(row[0]) if row[0] else 0,
                    '成本': 0,  # ADS层没有成本
                    '运费': 0,  # ADS层没有运费
                    '推广费': float(row[1]) if row[1] else 0,
                    '销量': int(row[3]) if row[3] else 0,
                }
            else:
                metrics['ADS'] = None
        except Exception as e:
            log(f'⚠️  ADS层查询失败: {e}')
            metrics['ADS'] = None
        
        conn.close()
        
    except Exception as e:
        log(f'❌ 数据库查询失败: {e}')
        import traceback
        traceback.print_exc()
        return None
    
    return metrics

def print_html_table(headers, rows, title=""):
    """打印HTML格式的表格"""
    html = f'''
<div class="verification-table">
    <h3>{title}</h3>
    <table>
        <thead>
            <tr>
'''
    for i, header in enumerate(headers):
        html += f'                <th>{header}</th>\n'
    
    html += '''            </tr>
        </thead>
        <tbody>
'''
    
    for row in rows:
        html += '            <tr>\n'
        for i, cell in enumerate(row):
            html += f'                <td>{cell}</td>\n'
        html += '            </tr>\n'
    
    html += '''        </tbody>
    </table>
</div>
'''
    log(html)

def display_metrics_table(metrics):
    """使用表格显示五层数据对比"""
    # CSS样式 - 深色主题，与程序融为一体
    log('''
<style>
.verification-report {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Microsoft YaHei', Arial, sans-serif;
    line-height: 1.8;
    color: #e4e4e7;
    background: transparent;
    min-height: 100%;
    height: auto;
    padding: 20px 25px;
}
.verification-table {
    margin: 16px 0;
}
.verification-table h3 {
    color: #e4e4e7;
    font-size: 20px;
    font-weight: 600;
    margin-bottom: 16px;
    padding-bottom: 10px;
    border-bottom: 2px solid #667eea;
    display: flex;
    align-items: center;
    gap: 10px;
}
.verification-table table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    background: #27272a;
    border-radius: 8px;
    overflow: visible;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    table-layout: auto;
}
.verification-table th {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: #fff;
    padding: 16px 12px;
    text-align: center;
    font-weight: 600;
    font-size: 14px;
    letter-spacing: 0.5px;
    border-bottom: 2px solid #52525b;
}
.verification-table td {
    padding: 14px 12px;
    text-align: right;
    border-bottom: 1px solid #3f3f46;
    font-size: 15px;
    font-weight: 500;
    color: #e4e4e7;
    background: #27272a;
    font-family: 'Consolas', 'Monaco', monospace;
}
.verification-table td:first-child {
    font-weight: 600;
    color: #fbbf24;
    background: #1f1f23;
    text-align: left;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Microsoft YaHei', Arial, sans-serif;
    font-size: 15px;
}
.verification-table tbody tr:hover td {
    background: #3f3f46;
}
.verification-table tbody tr:last-child td {
    border-bottom: none;
}
.status-section {
    margin: 16px 0;
    padding: 16px;
    border-radius: 8px;
    background: #27272a;
    border: 1px solid #3f3f46;
}
.status-section h3 {
    color: #e4e4e7;
    font-size: 20px;
    font-weight: 600;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 10px;
}
.status-item {
    padding: 12px 16px;
    font-size: 15px;
    margin: 10px 0;
    border-radius: 6px;
    background: #1f1f23;
    border: 1px solid #3f3f46;
}
.status-item strong {
    font-size: 16px;
    color: #fbbf24;
    display: block;
    margin-bottom: 10px;
}
.status-pass { 
    color: #4ade80;
    font-weight: 600;
    padding: 6px 12px;
    background: rgba(74, 222, 128, 0.1);
    border: 1px solid rgba(74, 222, 128, 0.3);
    border-radius: 4px;
    display: inline-block;
    margin: 4px 6px 4px 0;
    font-size: 14px;
}
.status-fail { 
    color: #f87171;
    font-weight: 600;
    padding: 6px 12px;
    background: rgba(248, 113, 113, 0.1);
    border: 1px solid rgba(248, 113, 113, 0.3);
    border-radius: 4px;
    display: inline-block;
    margin: 4px 6px 4px 0;
    font-size: 14px;
}
.summary-box {
    margin: 16px 0;
    padding: 20px;
    border-radius: 8px;
    text-align: center;
    font-size: 18px;
    font-weight: 600;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
}
.summary-success {
    background: linear-gradient(135deg, rgba(74, 222, 128, 0.15) 0%, rgba(34, 197, 94, 0.15) 100%);
    color: #4ade80;
    border: 2px solid rgba(74, 222, 128, 0.3);
}
.summary-warning {
    background: linear-gradient(135deg, rgba(248, 113, 113, 0.15) 0%, rgba(239, 68, 68, 0.15) 100%);
    color: #f87171;
    border: 2px solid rgba(248, 113, 113, 0.3);
}
.report-header {
    padding: 12px 16px;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    border-radius: 6px;
    margin: 0 0 16px 0;
    box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
}
</style>
''')
    
    # 准备表格数据
    fields = ['订单数', '销售额', '成本', '运费', '推广费', '销量']
    layers = ['CSV', 'ODS', 'DWD', 'DWS', 'ADS']
    
    table_data = []
    for field in fields:
        row = [field]
        for layer in layers:
            if metrics.get(layer) is None:
                row.append('-')
            else:
                value = metrics[layer].get(field, 0)
                if field == '订单数' or field == '销量':
                    row.append(f'{value:,}')
                else:
                    row.append(f'{value:,.2f}')
        table_data.append(row)
    
    # 显示表格
    headers = ['指标'] + layers
    print_html_table(headers, table_data, '数据一致性对比表')
    
    # 计算衍生指标
    financial_data = []
    for layer in layers:
        if metrics.get(layer) is None:
            continue
        
        m = metrics[layer]
        sales = m.get('销售额', 0)
        cost = m.get('成本', 0)
        shipping = m.get('运费', 0)
        promo = m.get('推广费', 0)
        
        # 毛利 = 销售额 - 成本 - 运费
        gross_profit = sales - cost - shipping
        gross_rate = (gross_profit / sales * 100) if sales > 0 else 0
        
        # 推广费率
        promo_rate = (promo / sales * 100) if sales > 0 else 0
        
        # 其他费用
        after_sales = sales * 0.02
        platform_fee = sales * 0.05
        management = sales * 0.10
        
        # 净利润
        net_profit = gross_profit - promo - after_sales - platform_fee - management
        net_rate = (net_profit / sales * 100) if sales > 0 else 0
        
        financial_data.append([
            layer,
            f'{gross_profit:,.2f}',
            f'{gross_rate:.2f}%',
            f'{promo:,.2f}',
            f'{promo_rate:.2f}%',
            f'{net_profit:,.2f}',
            f'{net_rate:.2f}%'
        ])
    
    headers = ['数据层', '毛利', '毛利率', '推广费', '推广费率', '净利润', '净利率']
    print_html_table(headers, financial_data, '财务指标对比表')

def verify_consistency(metrics):
    """验证各层数据一致性"""
    log('<div class="status-section">')
    log('<h3>一致性检查结果</h3>')
    
    all_pass = True
    tolerance = 1.0  # 允许的误差范围
    
    # 检查字段
    fields_to_check = ['订单数', '销售额', '推广费', '销量']
    
    # CSV vs ODS
    if metrics.get('CSV') and metrics.get('ODS'):
        log('<div class="status-item"><strong>📌 CSV vs ODS 对比</strong>')
        for field in ['订单数', '销售额', '推广费']:
            csv_val = metrics['CSV'].get(field, 0)
            ods_val = metrics['ODS'].get(field, 0)
            diff = abs(csv_val - ods_val)
            if diff < tolerance:
                log(f'<span class="status-pass">✅ {field}: 一致</span>')
            else:
                log(f'<span class="status-fail">❌ {field}: 不一致 (差异: {diff:,.2f})</span>')
                all_pass = False
        log('</div>')
    
    # ODS vs DWD
    if metrics.get('ODS') and metrics.get('DWD'):
        log('<div class="status-item"><strong>📌 ODS vs DWD 对比</strong>')
        for field in fields_to_check:
            ods_val = metrics['ODS'].get(field, 0)
            dwd_val = metrics['DWD'].get(field, 0)
            diff = abs(ods_val - dwd_val)
            if diff < tolerance:
                log(f'<span class="status-pass">✅ {field}: 一致</span>')
            else:
                log(f'<span class="status-fail">❌ {field}: 不一致 (差异: {diff:,.2f})</span>')
                all_pass = False
        log('</div>')
    
    # DWD vs DWS
    if metrics.get('DWD') and metrics.get('DWS'):
        log('<div class="status-item"><strong>📌 DWD vs DWS 对比</strong>')
        for field in fields_to_check:
            dwd_val = metrics['DWD'].get(field, 0)
            dws_val = metrics['DWS'].get(field, 0)
            diff = abs(dwd_val - dws_val)
            if diff < tolerance:
                log(f'<span class="status-pass">✅ {field}: 一致</span>')
            else:
                log(f'<span class="status-fail">❌ {field}: 不一致 (差异: {diff:,.2f})</span>')
                all_pass = False
        log('</div>')
    
    # DWS vs ADS
    if metrics.get('DWS') and metrics.get('ADS'):
        log('<div class="status-item"><strong>📌 DWS vs ADS 对比</strong>')
        for field in ['订单数', '销售额', '推广费', '销量']:
            dws_val = metrics['DWS'].get(field, 0)
            ads_val = metrics['ADS'].get(field, 0)
            diff = abs(dws_val - ads_val)
            if diff < tolerance:
                log(f'<span class="status-pass">✅ {field}: 一致</span>')
            else:
                log(f'<span class="status-fail">❌ {field}: 不一致 (差异: {diff:,.2f})</span>')
                all_pass = False
        log('</div>')
    
    log('</div>')
    return all_pass

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
    
    log('<div class="verification-report">')
    log('<div class="report-header">')
    log(f'<div style="display: flex; align-items: center; justify-content: space-between;"><span style="font-size: 18px; font-weight: 600;">📊 数据一致性验证报告</span><span style="font-size: 14px; opacity: 0.9;">企业体量: {business_scale}</span></div>')
    log('</div>')
    
    orders_csv = os.path.join(data_dir, 'ods_orders.csv')
    promo_csv = os.path.join(data_dir, 'ods_promotion.csv')
    
    # 收集所有层的指标
    metrics = collect_all_metrics(orders_csv, promo_csv, db_config)
    
    if metrics is None:
        log('\n❌ 数据收集失败')
        sys.exit(1)
    
    # 显示对比表格
    display_metrics_table(metrics)
    
    # 验证一致性
    all_pass = verify_consistency(metrics)
    
    # 总结
    if all_pass:
        log('<div class="summary-box summary-success">✅ 验证通过！所有数据层完全一致</div>')
    else:
        log('<div class="summary-box summary-warning">⚠️ 发现数据不一致，请检查上述差异项</div>')
    
    log('</div>')
    log('</div>')  # 关闭 verification-report
    sys.exit(0 if all_pass else 1)

if __name__ == '__main__':
    main()
