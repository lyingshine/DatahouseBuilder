"""
ADS层转换 - 应用数据层
面向业务的宽表和报表
"""
import pymysql
import sys
import json
import signal
import atexit

_global_connection = None

def cleanup_connection():
    """清理数据库连接并恢复全局配置"""
    global _global_connection
    if _global_connection:
        try:
            print("\n[清理] 正在恢复配置并释放连接...")
            sys.stdout.flush()
            
            # 恢复全局配置
            if hasattr(_global_connection, '_original_settings'):
                cursor = _global_connection.cursor()
                restored_count = 0
                for var, value in _global_connection._original_settings.items():
                    try:
                        cursor.execute(f"SET GLOBAL {var} = {value}")
                        restored_count += 1
                    except:
                        pass
                cursor.close()
                if restored_count > 0:
                    print(f"[清理] ✓ 已恢复 {restored_count} 项全局配置")
            
            _global_connection.rollback()
            _global_connection.close()
            _global_connection = None
            print("[清理] ✓ 连接已关闭")
        except:
            pass

def signal_handler(signum, frame):
    print("\n\n[中断] 检测到用户中断...")
    cleanup_connection()
    sys.exit(1)

import time

def get_db_connection(db_config):
    """获取数据库连接并应用极限优化"""
    global _global_connection
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        _global_connection = conn
        
        cursor = conn.cursor()
        
        # 保存原始全局配置
        original_settings = {}
        
        # 尝试设置全局变量（需要SUPER权限）
        print("  尝试应用极致性能优化...")
        global_optimizations = {
            'innodb_flush_log_at_trx_commit': '0',
            'sync_binlog': '0',
            'innodb_doublewrite': '0',
            'innodb_flush_neighbors': '0',
            'innodb_io_capacity': '5000',
            'innodb_io_capacity_max': '10000',
            'innodb_read_io_threads': '20',
            'innodb_write_io_threads': '20',
            'max_connections': '2000',
            'local_infile': '1',
        }
        
        applied_count = 0
        for var, value in global_optimizations.items():
            try:
                cursor.execute(f"SELECT @@GLOBAL.{var}")
                result = cursor.fetchone()
                if result:
                    original_settings[var] = result[0]
                cursor.execute(f"SET GLOBAL {var} = {value}")
                applied_count += 1
            except:
                pass
        
        if applied_count > 0:
            print(f"  ✓ 已应用 {applied_count} 项全局优化")
        else:
            print("  ℹ️ 无SUPER权限，使用会话级优化")
        
        # 会话级优化（极限配置）
        cursor.execute("SET SESSION sql_mode = ''")
        cursor.execute("SET SESSION foreign_key_checks = 0")
        cursor.execute("SET SESSION unique_checks = 0")
        cursor.execute("SET SESSION autocommit = 0")
        cursor.execute("SET SESSION sort_buffer_size = 67108864")
        cursor.execute("SET SESSION join_buffer_size = 67108864")
        cursor.execute("SET SESSION read_buffer_size = 33554432")
        cursor.execute("SET SESSION read_rnd_buffer_size = 33554432")
        cursor.execute("SET SESSION tmp_table_size = 8589934592")
        cursor.execute("SET SESSION max_heap_table_size = 8589934592")
        cursor.execute("SET SESSION bulk_insert_buffer_size = 536870912")
        cursor.execute("SET SESSION optimizer_switch = 'block_nested_loop=on,batched_key_access=on'")
        
        try:
            cursor.execute("SET SESSION sql_log_bin = 0")
        except:
            pass
        
        conn.commit()
        cursor.close()
        print("  ✓ 会话级缓冲区已优化（极速模式）")
        
        conn._original_settings = original_settings
        
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def execute_sql(conn, sql, description):
    cursor = None
    try:
        print(f"  {description}...", end='', flush=True)
        start_time = time.time()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        elapsed = time.time() - start_time
        affected_rows = cursor.rowcount
        if affected_rows > 0 and elapsed > 0:
            speed = int(affected_rows / elapsed)
            print(f" ✓ {affected_rows:,} 行 ({elapsed:.1f}秒, {speed:,} 行/秒)")
        else:
            print(f" ✓ {affected_rows:,} 行 ({elapsed:.1f}秒)")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f" ✗ 失败: {e}")
        sys.stdout.flush()
        conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()


def transform_ads(mode='full', db_config=None):
    """转换ADS层数据 - 业务宽表"""
    print("="*60)
    print("ADS层数据转换 - 业务宽表")
    print("="*60)
    
    conn = get_db_connection(db_config)
    if not conn:
        return False
    
    try:
        # ========== 1. 日报宽表 ==========
        print("\n【第一步】日报宽表")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS ads_daily_report", "删除旧表")
        
        # 新逻辑：先汇总推广数据，再关联销售数据，确保所有推广费都体现
        sql_daily_report = """
        CREATE TABLE IF NOT EXISTS ads_daily_report AS
        SELECT 
            base.`日期`,
            base.`平台`,
            base.`店铺`,
            base.`SPU编码`,
            base.`SKU编码`,
            base.`商品名称`,
            base.`规格`,
            base.`一级类目`,
            base.`二级类目`,
            COALESCE(base.`订单数`, 0) AS `订单数`,
            COALESCE(base.`客户数`, 0) AS `客户数`,
            COALESCE(base.`销量`, 0) AS `销量`,
            COALESCE(base.`销售额`, 0) AS `销售额`,
            COALESCE(base.`商品成本`, 0) AS `商品成本`,
            COALESCE(base.`运费`, 0) AS `运费`,
            COALESCE(base.`毛利`, 0) AS `毛利`,
            COALESCE(base.`毛利率`, 0) AS `毛利率`,
            COALESCE(base.`推广费`, 0) AS `推广费`,
            COALESCE(base.`售后费`, 0) AS `售后费`,
            COALESCE(base.`平台费`, 0) AS `平台费`,
            COALESCE(base.`管理费`, 0) AS `管理费`,
            COALESCE(base.`净利润`, 0) AS `净利润`,
            COALESCE(base.`净利率`, 0) AS `净利率`,
            COALESCE(base.`客单价`, 0) AS `客单价`
        FROM (
            -- 销售数据（有订单的商品）
            SELECT 
                d.date_value AS `日期`,
                f.platform AS `平台`,
                s.store_name AS `店铺`,
                p.product_id AS `SPU编码`,
                p.sku_id AS `SKU编码`,
                p.product_name AS `商品名称`,
                p.spec AS `规格`,
                p.category_l1 AS `一级类目`,
                p.category_l2 AS `二级类目`,
                COUNT(DISTINCT f.order_id) AS `订单数`,
                COUNT(DISTINCT f.user_key) AS `客户数`,
                SUM(fd.quantity) AS `销量`,
                ROUND(SUM(fd.amount), 2) AS `销售额`,
                ROUND(SUM(fd.cost_amount), 2) AS `商品成本`,
                ROUND(SUM(fd.quantity * CASE WHEN p.category_l1 LIKE '整车%' THEN 30 ELSE 3 END), 2) AS `运费`,
                ROUND(SUM(fd.amount) - SUM(fd.cost_amount) - SUM(fd.quantity * CASE WHEN p.category_l1 LIKE '整车%' THEN 30 ELSE 3 END), 2) AS `毛利`,
                ROUND(CASE WHEN SUM(fd.amount) > 0 
                    THEN (SUM(fd.amount) - SUM(fd.cost_amount) - SUM(fd.quantity * CASE WHEN p.category_l1 LIKE '整车%' THEN 30 ELSE 3 END)) / SUM(fd.amount) * 100 
                    ELSE 0 END, 2) AS `毛利率`,
                COALESCE(pm.promo_cost, 0) AS `推广费`,
                ROUND(SUM(fd.amount) * 0.02, 2) AS `售后费`,
                ROUND(SUM(fd.amount) * 0.05, 2) AS `平台费`,
                ROUND(SUM(fd.amount) * 0.10, 2) AS `管理费`,
                ROUND((SUM(fd.amount) - SUM(fd.cost_amount) - SUM(fd.quantity * CASE WHEN p.category_l1 LIKE '整车%' THEN 30 ELSE 3 END)) 
                    - COALESCE(pm.promo_cost, 0) 
                    - SUM(fd.amount) * 0.02 - SUM(fd.amount) * 0.05 - SUM(fd.amount) * 0.10, 2) AS `净利润`,
                ROUND(CASE WHEN SUM(fd.amount) > 0 
                    THEN ((SUM(fd.amount) - SUM(fd.cost_amount) - SUM(fd.quantity * CASE WHEN p.category_l1 LIKE '整车%' THEN 30 ELSE 3 END)) 
                        - COALESCE(pm.promo_cost, 0) 
                        - SUM(fd.amount) * 0.02 - SUM(fd.amount) * 0.05 - SUM(fd.amount) * 0.10) / SUM(fd.amount) * 100 
                    ELSE 0 END, 2) AS `净利率`,
                ROUND(CASE WHEN COUNT(DISTINCT f.order_id) > 0 
                    THEN SUM(fd.amount) / COUNT(DISTINCT f.order_id) ELSE 0 END, 2) AS `客单价`
            FROM dwd_fact_order f
            INNER JOIN dwd_fact_order_detail fd ON f.order_id = fd.order_id
            LEFT JOIN dim_date d ON f.date_key = d.date_key
            LEFT JOIN dim_store s ON f.store_key = s.store_key
            LEFT JOIN dim_product p ON fd.product_key = p.product_key
            LEFT JOIN (
                SELECT date_key, store_key, product_key, SUM(cost) AS promo_cost
                FROM dwd_fact_promotion 
                GROUP BY date_key, store_key, product_key
            ) pm ON f.date_key = pm.date_key AND f.store_key = pm.store_key AND fd.product_key = pm.product_key
            WHERE f.order_status IN ('已完成', '已发货')
            GROUP BY d.date_value, f.platform, f.store_key, s.store_name, fd.product_key, p.product_id, p.sku_id, p.product_name, p.spec, p.category_l1, p.category_l2, pm.promo_cost
            
            UNION ALL
            
            -- 推广数据（只有推广没有销售的商品）
            SELECT 
                d.date_value AS `日期`,
                fp.platform AS `平台`,
                s.store_name AS `店铺`,
                p.product_id AS `SPU编码`,
                p.sku_id AS `SKU编码`,
                p.product_name AS `商品名称`,
                p.spec AS `规格`,
                p.category_l1 AS `一级类目`,
                p.category_l2 AS `二级类目`,
                0 AS `订单数`,
                0 AS `客户数`,
                0 AS `销量`,
                0 AS `销售额`,
                0 AS `商品成本`,
                0 AS `运费`,
                0 AS `毛利`,
                0 AS `毛利率`,
                SUM(fp.cost) AS `推广费`,
                0 AS `售后费`,
                0 AS `平台费`,
                0 AS `管理费`,
                -SUM(fp.cost) AS `净利润`,
                0 AS `净利率`,
                0 AS `客单价`
            FROM dwd_fact_promotion fp
            LEFT JOIN dim_date d ON fp.date_key = d.date_key
            LEFT JOIN dim_store s ON fp.store_key = s.store_key
            LEFT JOIN dim_product p ON fp.product_key = p.product_key
            -- 排除已经在销售数据中的记录
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dwd_fact_order f
                INNER JOIN dwd_fact_order_detail fd ON f.order_id = fd.order_id
                WHERE f.order_status IN ('已完成', '已发货')
                  AND f.date_key = fp.date_key
                  AND f.store_key = fp.store_key
                  AND fd.product_key = fp.product_key
            )
            GROUP BY d.date_value, fp.platform, fp.store_key, s.store_name, fp.product_key, p.product_id, p.sku_id, p.product_name, p.spec, p.category_l1, p.category_l2
        ) base
        ORDER BY base.`日期` DESC, base.`平台`, base.`店铺`
        """
        if not execute_sql(conn, sql_daily_report, "创建日报宽表（包含所有推广费）"):
            return False
        
        execute_sql(conn, "ALTER TABLE ads_daily_report ADD INDEX idx_date (`日期`)", "创建日期索引")
        execute_sql(conn, "ALTER TABLE ads_daily_report ADD INDEX idx_platform (`平台`)", "创建平台索引")
        
        # ========== 2. 平台汇总表 ==========
        print("\n【第二步】平台汇总表")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS ads_platform_summary", "删除旧表")
        
        sql_platform = """
        CREATE TABLE IF NOT EXISTS ads_platform_summary AS
        SELECT 
            `平台`,
            COUNT(DISTINCT `店铺`) AS `店铺数`,
            SUM(`订单数`) AS `总订单数`,
            SUM(`客户数`) AS `总客户数`,
            ROUND(SUM(`销售额`), 2) AS `总销售额`,
            ROUND(SUM(`商品成本`), 2) AS `总成本`,
            ROUND(SUM(`毛利`), 2) AS `总毛利`,
            ROUND(SUM(`推广费`), 2) AS `总推广费`,
            ROUND(SUM(`净利润`), 2) AS `总净利润`,
            ROUND(CASE WHEN SUM(`销售额`) > 0 THEN SUM(`净利润`) / SUM(`销售额`) * 100 ELSE 0 END, 2) AS `净利率`,
            ROUND(SUM(`销售额`) / SUM(`订单数`), 2) AS `客单价`
        FROM ads_daily_report
        GROUP BY `平台`
        ORDER BY `总销售额` DESC
        """
        execute_sql(conn, sql_platform, "创建平台汇总表")
        
        # ========== 3. 店铺排行榜 ==========
        print("\n【第三步】店铺排行榜")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS ads_store_ranking", "删除旧表")
        
        sql_store_rank = """
        CREATE TABLE IF NOT EXISTS ads_store_ranking AS
        SELECT 
            `平台`, `店铺`,
            SUM(`订单数`) AS `总订单数`,
            ROUND(SUM(`销售额`), 2) AS `总销售额`,
            ROUND(SUM(`净利润`), 2) AS `总净利润`,
            ROUND(CASE WHEN SUM(`销售额`) > 0 THEN SUM(`净利润`) / SUM(`销售额`) * 100 ELSE 0 END, 2) AS `净利率`,
            ROW_NUMBER() OVER (ORDER BY SUM(`销售额`) DESC) AS `销售排名`,
            ROW_NUMBER() OVER (ORDER BY SUM(`净利润`) DESC) AS `利润排名`
        FROM ads_daily_report
        GROUP BY `平台`, `店铺`
        ORDER BY `总销售额` DESC
        """
        execute_sql(conn, sql_store_rank, "创建店铺排行榜")
        
        # ========== 4. 流量宽表（完整版：SPU维度 + 所有渠道）==========
        print("\n【第四步】流量宽表（SPU维度 + 所有渠道）")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS ads_traffic_report", "删除旧表")
        
        # 先创建付费流量汇总（按SPU聚合）
        sql_paid_traffic = """
        CREATE TEMPORARY TABLE tmp_paid_traffic AS
        SELECT 
            d.date_value AS `日期`,
            fp.platform AS `平台`,
            s.store_name AS `店铺`,
            p.product_id AS `SPU编码`,
            p.category_l1 AS `一级类目`,
            p.category_l2 AS `二级类目`,
            fp.channel AS `流量渠道`,
            SUM(fp.impressions) AS `曝光量`,
            SUM(fp.clicks) AS `点击量`,
            ROUND(SUM(fp.cost), 2) AS `推广费用`,
            ROUND(CASE WHEN SUM(fp.clicks) > 0 THEN SUM(fp.cost) / SUM(fp.clicks) ELSE 0 END, 2) AS `平均点击成本`
        FROM dwd_fact_promotion fp
        INNER JOIN dim_date d ON fp.date_key = d.date_key
        INNER JOIN dim_store s ON fp.store_key = s.store_key
        INNER JOIN dim_product p ON fp.product_key = p.product_key
        GROUP BY d.date_value, fp.platform, s.store_name, p.product_id, p.category_l1, p.category_l2, fp.channel
        """
        execute_sql(conn, sql_paid_traffic, "创建付费流量临时表")
        
        # 创建自然流量汇总（按SPU聚合）
        sql_natural_traffic = """
        CREATE TEMPORARY TABLE tmp_natural_traffic AS
        SELECT 
            pt.date AS `日期`,
            pt.platform AS `平台`,
            s.store_name AS `店铺`,
            p.product_id AS `SPU编码`,
            p.category_l1 AS `一级类目`,
            p.category_l2 AS `二级类目`,
            pt.channel AS `流量渠道`,
            SUM(pt.impressions) AS `曝光量`,
            SUM(pt.clicks) AS `点击量`,
            SUM(pt.favorites) AS `收藏量`,
            SUM(pt.add_to_cart) AS `加购量`
        FROM ods_product_traffic pt
        INNER JOIN dim_store s ON pt.store_id = s.store_id
        INNER JOIN dim_product p ON pt.sku_id = p.sku_id
        GROUP BY pt.date, pt.platform, s.store_name, p.product_id, p.category_l1, p.category_l2, pt.channel
        """
        execute_sql(conn, sql_natural_traffic, "创建自然流量临时表")
        
        # 创建销售数据汇总（按SPU聚合，直接从DWD层计算）- 用于付费流量
        # 只统计流量来源为"付费推广"的订单
        sql_sales_spu_paid = """
        CREATE TEMPORARY TABLE tmp_sales_spu_paid AS
        SELECT 
            d.date_value AS `日期`,
            f.platform AS `平台`,
            s.store_name AS `店铺`,
            p.product_id AS `SPU编码`,
            SUM(fd.quantity) AS `销量`,
            ROUND(SUM(fd.amount), 2) AS `销售额`
        FROM dwd_fact_order f
        INNER JOIN dwd_fact_order_detail fd ON f.order_id = fd.order_id
        INNER JOIN dim_date d ON f.date_key = d.date_key
        INNER JOIN dim_store s ON f.store_key = s.store_key
        INNER JOIN dim_product p ON fd.product_key = p.product_key
        WHERE f.order_status IN ('已完成', '已发货')
          AND f.traffic_source = '付费推广'
        GROUP BY d.date_value, f.platform, s.store_name, p.product_id
        """
        execute_sql(conn, sql_sales_spu_paid, "创建SPU销售汇总临时表(付费)")
        
        # 创建销售数据汇总（按SPU聚合，直接从DWD层计算）- 用于自然流量
        # 统计流量来源不是"付费推广"的订单
        sql_sales_spu_natural = """
        CREATE TEMPORARY TABLE tmp_sales_spu_natural AS
        SELECT 
            d.date_value AS `日期`,
            f.platform AS `平台`,
            s.store_name AS `店铺`,
            p.product_id AS `SPU编码`,
            SUM(fd.quantity) AS `销量`,
            ROUND(SUM(fd.amount), 2) AS `销售额`
        FROM dwd_fact_order f
        INNER JOIN dwd_fact_order_detail fd ON f.order_id = fd.order_id
        INNER JOIN dim_date d ON f.date_key = d.date_key
        INNER JOIN dim_store s ON f.store_key = s.store_key
        INNER JOIN dim_product p ON fd.product_key = p.product_key
        WHERE f.order_status IN ('已完成', '已发货')
          AND f.traffic_source != '付费推广'
        GROUP BY d.date_value, f.platform, s.store_name, p.product_id
        """
        execute_sql(conn, sql_sales_spu_natural, "创建SPU销售汇总临时表(自然)")
        
        # 先按 SPU 汇总流量，再分别关联对应的销量（避免重复计算）
        sql_traffic_report = """
        CREATE TABLE ads_traffic_report AS
        SELECT 
            paid_agg.`日期`,
            paid_agg.`平台`,
            paid_agg.`店铺`,
            paid_agg.`SPU编码`,
            paid_agg.`一级类目`,
            paid_agg.`二级类目`,
            '付费' AS `流量类型`,
            paid_agg.`曝光量`,
            paid_agg.`点击量`,
            ROUND(CASE WHEN paid_agg.`曝光量` > 0 THEN paid_agg.`点击量` / paid_agg.`曝光量` * 100 ELSE 0 END, 2) AS `点击率`,
            0 AS `收藏量`,
            0 AS `加购量`,
            COALESCE(paid_sales.`销量`, 0) AS `销量`,
            COALESCE(paid_sales.`销售额`, 0) AS `销售额`,
            ROUND(CASE WHEN paid_agg.`点击量` > 0 THEN COALESCE(paid_sales.`销量`, 0) / paid_agg.`点击量` * 100 ELSE 0 END, 2) AS `点击转化率`,
            paid_agg.`推广费用`,
            ROUND(CASE WHEN paid_agg.`点击量` > 0 THEN paid_agg.`推广费用` / paid_agg.`点击量` ELSE 0 END, 2) AS `平均点击成本`,
            ROUND(CASE WHEN paid_agg.`推广费用` > 0 THEN COALESCE(paid_sales.`销售额`, 0) / paid_agg.`推广费用` ELSE 0 END, 2) AS `ROI`
        FROM (
            -- 付费流量汇总（按 SPU 聚合）
            SELECT 
                `日期`, `平台`, `店铺`, `SPU编码`, `一级类目`, `二级类目`,
                SUM(`曝光量`) AS `曝光量`,
                SUM(`点击量`) AS `点击量`,
                SUM(`推广费用`) AS `推广费用`
            FROM tmp_paid_traffic
            GROUP BY `日期`, `平台`, `店铺`, `SPU编码`, `一级类目`, `二级类目`
        ) paid_agg
        LEFT JOIN tmp_sales_spu_paid paid_sales
            ON paid_agg.`日期` = paid_sales.`日期` 
            AND paid_agg.`平台` = paid_sales.`平台` 
            AND paid_agg.`店铺` = paid_sales.`店铺`
            AND paid_agg.`SPU编码` = paid_sales.`SPU编码`
        
        UNION ALL
        
        SELECT 
            nat_agg.`日期`,
            nat_agg.`平台`,
            nat_agg.`店铺`,
            nat_agg.`SPU编码`,
            nat_agg.`一级类目`,
            nat_agg.`二级类目`,
            '自然' AS `流量类型`,
            nat_agg.`曝光量`,
            nat_agg.`点击量`,
            ROUND(CASE WHEN nat_agg.`曝光量` > 0 THEN nat_agg.`点击量` / nat_agg.`曝光量` * 100 ELSE 0 END, 2) AS `点击率`,
            nat_agg.`收藏量`,
            nat_agg.`加购量`,
            COALESCE(nat_sales.`销量`, 0) AS `销量`,
            COALESCE(nat_sales.`销售额`, 0) AS `销售额`,
            ROUND(CASE WHEN nat_agg.`点击量` > 0 THEN COALESCE(nat_sales.`销量`, 0) / nat_agg.`点击量` * 100 ELSE 0 END, 2) AS `点击转化率`,
            0 AS `推广费用`,
            0 AS `平均点击成本`,
            0 AS `ROI`
        FROM (
            -- 自然流量汇总（按 SPU 聚合）
            SELECT 
                `日期`, `平台`, `店铺`, `SPU编码`, `一级类目`, `二级类目`,
                SUM(`曝光量`) AS `曝光量`,
                SUM(`点击量`) AS `点击量`,
                SUM(`收藏量`) AS `收藏量`,
                SUM(`加购量`) AS `加购量`
            FROM tmp_natural_traffic
            GROUP BY `日期`, `平台`, `店铺`, `SPU编码`, `一级类目`, `二级类目`
        ) nat_agg
        LEFT JOIN tmp_sales_spu_natural nat_sales
            ON nat_agg.`日期` = nat_sales.`日期` 
            AND nat_agg.`平台` = nat_sales.`平台` 
            AND nat_agg.`店铺` = nat_sales.`店铺`
            AND nat_agg.`SPU编码` = nat_sales.`SPU编码`
        
        ORDER BY `日期` DESC, `平台`, `店铺`, `SPU编码`, `流量类型`
        """
        execute_sql(conn, sql_traffic_report, "创建流量宽表")
        
        # 添加索引（TEXT类型需要指定前缀长度）
        execute_sql(conn, "ALTER TABLE ads_traffic_report ADD INDEX idx_date (`日期`(10))", "创建日期索引")
        execute_sql(conn, "ALTER TABLE ads_traffic_report ADD INDEX idx_platform (`平台`(20))", "创建平台索引")
        execute_sql(conn, "ALTER TABLE ads_traffic_report ADD INDEX idx_spu (`SPU编码`(50))", "创建SPU索引")
        execute_sql(conn, "ALTER TABLE ads_traffic_report ADD INDEX idx_traffic_type (`流量类型`)", "创建流量类型索引")
        
        print("\n  注意：流量表已按 SPU 汇总，不再按流量渠道明细展示")
        
        # 清理临时表
        execute_sql(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_paid_traffic", "清理临时表")
        execute_sql(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_natural_traffic", "清理临时表")
        execute_sql(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_sales_spu_paid", "清理临时表")
        execute_sql(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_sales_spu_natural", "清理临时表")
        
        print("\n" + "="*60)
        print("✓ ADS层转换完成！")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"✗ 转换失败: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(cleanup_connection)
    
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
        success = transform_ads(mode, db_config)
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
