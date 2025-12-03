"""
DWS层转换 - 汇总数据层
按主题域组织：交易域、用户域、流量域、营销域
支持千万级数据
"""
import sys
import json
import signal
import atexit

# 导入数据库管理器
from db_manager import get_db_manager, cleanup_global_db_manager


def signal_handler(signum, frame):
    """处理中断信号"""
    print("\n\n[中断] 检测到用户中断...")
    cleanup_global_db_manager()
    sys.exit(1)


def transform_dws(mode='full', db_config=None):
    """转换DWS层数据 - 支持千万级数据"""
    print("="*60)
    print("DWS层数据转换 - 多维度汇总（支持千万级）")
    print("="*60)
    
    # 使用数据库管理器
    db_manager = get_db_manager(db_config)
    if not db_manager:
        return False
    
    try:
        # ========== 1. 交易域汇总 ==========
        print("\n【第一步】交易域汇总")
        
        print("\n1.1 订单日汇总表")
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_trade_order_1d", "删除旧表")
        
        sql_order_1d = """
        CREATE TABLE IF NOT EXISTS dws_trade_order_1d AS
        SELECT 
            f.date_key, f.store_key, f.platform,
            COUNT(DISTINCT f.order_id) AS order_count,
            COUNT(DISTINCT f.user_key) AS order_user_count,
            SUM(f.final_amount) AS order_amount,
            COUNT(DISTINCT CASE WHEN f.order_status IN ('已完成', '已发货') THEN f.order_id END) AS payment_count,
            SUM(CASE WHEN f.order_status IN ('已完成', '已发货') THEN f.final_amount ELSE 0 END) AS payment_amount,
            SUM(CASE WHEN f.order_status IN ('已完成', '已发货') THEN f.total_cost ELSE 0 END) AS cost_amount,
            SUM(CASE WHEN f.order_status IN ('已完成', '已发货') THEN f.profit_amount ELSE 0 END) AS profit_amount,
            ROUND(AVG(CASE WHEN f.order_status IN ('已完成', '已发货') THEN f.final_amount END), 2) AS avg_order_amount,
            CURDATE() AS etl_date
        FROM dwd_fact_order f
        GROUP BY f.date_key, f.store_key, f.platform
        """
        db_manager.execute_sql(sql_order_1d, "创建订单日汇总表")
        
        print("\n1.2 商品日汇总表")
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_trade_product_1d", "删除旧表")
        
        sql_product_1d = """
        CREATE TABLE IF NOT EXISTS dws_trade_product_1d AS
        SELECT 
            fd.date_key, fd.product_key,
            COUNT(DISTINCT fd.order_id) AS order_count,
            SUM(fd.quantity) AS sales_quantity,
            SUM(fd.amount) AS sales_amount,
            SUM(fd.cost_amount) AS cost_amount,
            SUM(fd.profit_amount) AS profit_amount,
            COUNT(DISTINCT fd.user_key) AS buyer_count,
            CURDATE() AS etl_date
        FROM dwd_fact_order_detail fd
        INNER JOIN dwd_fact_order f ON fd.order_id = f.order_id
        WHERE f.order_status IN ('已完成', '已发货')
        GROUP BY fd.date_key, fd.product_key
        """
        db_manager.execute_sql(sql_product_1d, "创建商品日汇总表")
        
        # ========== 2. 店铺维度汇总 ==========
        print("\n【第二步】店铺维度汇总")
        
        print("\n2.1 店铺日汇总表")
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_store_daily", "删除旧表")
        
        sql_store_daily = """
        CREATE TABLE IF NOT EXISTS dws_store_daily AS
        SELECT 
            f.date_key, f.store_key,
            COUNT(DISTINCT f.order_id) AS order_count,
            COUNT(DISTINCT f.user_key) AS user_count,
            SUM(f.final_amount) AS sales_amount,
            SUM(f.total_cost) AS cost_amount,
            SUM(f.profit_amount) AS profit_amount,
            CASE WHEN SUM(f.final_amount) > 0 THEN ROUND(SUM(f.profit_amount) / SUM(f.final_amount) * 100, 2) ELSE 0 END AS profit_rate
        FROM dwd_fact_order f
        WHERE f.order_status IN ('已完成', '已发货')
        GROUP BY f.date_key, f.store_key
        """
        db_manager.execute_sql(sql_store_daily, "创建店铺日汇总表")
        
        print("\n2.2 店铺总汇总表")
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_store_total", "删除旧表")
        
        sql_store_total = """
        CREATE TABLE IF NOT EXISTS dws_store_total AS
        SELECT 
            f.store_key, s.store_id, s.store_name, s.platform,
            COUNT(DISTINCT f.order_id) AS order_count,
            COUNT(DISTINCT f.user_key) AS user_count,
            SUM(f.final_amount) AS sales_amount,
            SUM(f.total_cost) AS cost_amount,
            SUM(f.profit_amount) AS profit_amount,
            CASE WHEN SUM(f.final_amount) > 0 THEN ROUND(SUM(f.profit_amount) / SUM(f.final_amount) * 100, 2) ELSE 0 END AS profit_rate,
            ROUND(AVG(f.final_amount), 2) AS avg_order_amount
        FROM dwd_fact_order f
        LEFT JOIN dim_store s ON f.store_key = s.store_key
        WHERE f.order_status IN ('已完成', '已发货')
        GROUP BY f.store_key, s.store_id, s.store_name, s.platform
        """
        db_manager.execute_sql(sql_store_total, "创建店铺总汇总表")

        
        # ========== 3. 商品维度汇总 ==========
        print("\n【第三步】商品维度汇总")
        
        print("\n3.1 商品总汇总表")
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_product_total", "删除旧表")
        
        sql_product_total = """
        CREATE TABLE IF NOT EXISTS dws_product_total AS
        SELECT 
            fd.product_key, p.product_id, p.product_name, p.category_l1, p.category_l2,
            COUNT(DISTINCT fd.order_id) AS order_count,
            SUM(fd.quantity) AS sales_quantity,
            SUM(fd.amount) AS sales_amount,
            SUM(fd.cost_amount) AS cost_amount,
            SUM(fd.profit_amount) AS profit_amount,
            CASE WHEN SUM(fd.amount) > 0 THEN ROUND(SUM(fd.profit_amount) / SUM(fd.amount) * 100, 2) ELSE 0 END AS profit_rate
        FROM dwd_fact_order_detail fd
        LEFT JOIN dim_product p ON fd.product_key = p.product_key
        INNER JOIN dwd_fact_order f ON fd.order_id = f.order_id
        WHERE f.order_status IN ('已完成', '已发货')
        GROUP BY fd.product_key, p.product_id, p.product_name, p.category_l1, p.category_l2
        """
        db_manager.execute_sql(sql_product_total, "创建商品总汇总表")
        
        # ========== 4. 类目维度汇总 ==========
        print("\n【第四步】类目维度汇总")
        
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_category_total", "删除旧表")
        
        sql_category_total = """
        CREATE TABLE IF NOT EXISTS dws_category_total AS
        SELECT 
            p.category_l1, p.category_l2, f.platform,
            COUNT(DISTINCT fd.order_id) AS order_count,
            SUM(fd.quantity) AS sales_quantity,
            SUM(fd.amount) AS sales_amount,
            SUM(fd.profit_amount) AS profit_amount,
            CASE WHEN SUM(fd.amount) > 0 THEN ROUND(SUM(fd.profit_amount) / SUM(fd.amount) * 100, 2) ELSE 0 END AS profit_rate
        FROM dwd_fact_order_detail fd
        LEFT JOIN dim_product p ON fd.product_key = p.product_key
        INNER JOIN dwd_fact_order f ON fd.order_id = f.order_id
        WHERE f.order_status IN ('已完成', '已发货')
        GROUP BY p.category_l1, p.category_l2, f.platform
        """
        db_manager.execute_sql(sql_category_total, "创建类目总汇总表")
        
        # ========== 5. 用户维度汇总 ==========
        print("\n【第五步】用户维度汇总")
        
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_user_total", "删除旧表")
        
        sql_user_total = """
        CREATE TABLE IF NOT EXISTS dws_user_total AS
        SELECT 
            f.user_key, u.user_id, u.gender, u.age, u.age_group, u.city,
            COUNT(DISTINCT f.order_id) AS order_count,
            SUM(f.final_amount) AS total_amount,
            ROUND(AVG(f.final_amount), 2) AS avg_order_amount,
            MIN(f.order_time) AS first_order_date,
            MAX(f.order_time) AS last_order_date,
            CASE 
                WHEN SUM(f.final_amount) >= 10000 THEN '高价值'
                WHEN SUM(f.final_amount) >= 5000 THEN '中价值'
                ELSE '低价值'
            END AS user_level
        FROM dwd_fact_order f
        LEFT JOIN dim_user u ON f.user_key = u.user_key
        WHERE f.order_status IN ('已完成', '已发货')
        GROUP BY f.user_key, u.user_id, u.gender, u.age, u.age_group, u.city
        """
        db_manager.execute_sql(sql_user_total, "创建用户总汇总表")
        
        # ========== 6. 推广维度汇总 ==========
        print("\n【第六步】推广维度汇总")
        
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_promotion_daily", "删除旧表")
        
        sql_promotion_daily = """
        CREATE TABLE IF NOT EXISTS dws_promotion_daily AS
        SELECT 
            fp.date_key, fp.channel, fp.platform,
            SUM(fp.cost) AS cost,
            SUM(fp.impressions) AS impressions,
            SUM(fp.clicks) AS clicks,
            CASE WHEN SUM(fp.impressions) > 0 THEN ROUND(SUM(fp.clicks) / SUM(fp.impressions) * 100, 2) ELSE 0 END AS click_rate,
            CASE WHEN SUM(fp.clicks) > 0 THEN ROUND(SUM(fp.cost) / SUM(fp.clicks), 2) ELSE 0 END AS avg_click_cost
        FROM dwd_fact_promotion fp
        GROUP BY fp.date_key, fp.channel, fp.platform
        """
        db_manager.execute_sql(sql_promotion_daily, "创建推广日汇总表")
        
        # ========== 7. 流量维度汇总 ==========
        print("\n【第七步】流量维度汇总")
        
        if mode == 'full':
            db_manager.execute_sql("DROP TABLE IF EXISTS dws_traffic_daily", "删除旧表")
        
        sql_traffic_daily = """
        CREATE TABLE IF NOT EXISTS dws_traffic_daily AS
        SELECT 
            ft.date_key, ft.store_key, ft.platform,
            ft.visitors, ft.page_views, ft.avg_stay_time, ft.bounce_rate,
            CASE WHEN ft.visitors > 0 THEN ROUND(COALESCE(o.order_cnt, 0) / ft.visitors * 100, 2) ELSE 0 END AS conversion_rate
        FROM dwd_fact_traffic ft
        LEFT JOIN (
            SELECT date_key, store_key, COUNT(DISTINCT order_id) AS order_cnt
            FROM dwd_fact_order WHERE order_status IN ('已完成', '已发货')
            GROUP BY date_key, store_key
        ) o ON ft.date_key = o.date_key AND ft.store_key = o.store_key
        """
        db_manager.execute_sql(sql_traffic_daily, "创建流量日汇总表")
        
        print("\n" + "="*60)
        print("✓ DWS层转换完成！")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"✗ 转换失败: {e}")
        return False
    finally:
        db_manager.close()

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(cleanup_global_db_manager)
    
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    db_config = config.get('dbConfig', {
        'host': 'localhost', 'port': 3306, 'database': 'datas', 'user': 'root', 'password': ''
    })
    
    mode = config.get('mode', 'full')
    print(f"模式: {'全量' if mode == 'full' else '增量'}")
    
    try:
        success = transform_dws(mode, db_config)
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n[中断] 用户取消操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n[错误] 执行失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
