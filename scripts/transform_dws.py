"""
DWD层到DWS层数据转换
通过SQL在数据库中直接转换
"""
import pymysql
import sys
import json


def get_db_connection(db_config):
    """获取数据库连接"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset='utf8mb4'
        )
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None


def execute_sql(conn, sql, description):
    """执行SQL语句"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        print(f"✓ {description} - 影响 {affected_rows} 行")
        return True
    except Exception as e:
        print(f"✗ {description} 失败: {e}")
        return False


def transform_dws(mode='full', db_config=None):
    """
    转换DWS层数据
    mode: 'full' 全量模式（删除重建）, 'incremental' 增量模式（追加）
    """
    print("="*60)
    print("DWS层数据转换")
    print("="*60)
    
    conn = get_db_connection(db_config)
    if not conn:
        return False
    
    try:
        # 1. 销售汇总表
        print("\n1. 构建销售汇总表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_sales_summary", "删除旧表 dws_sales_summary")
        
        sql_sales_summary = """
        CREATE TABLE IF NOT EXISTS dws_sales_summary AS
        SELECT 
            o.订单日期 AS 日期,
            o.年月,
            o.平台,
            o.店铺ID,
            o.店铺名称,
            COALESCE(od.一级类目, '未分类') AS 一级类目,
            COALESCE(od.二级类目, '未分类') AS 二级类目,
            COUNT(DISTINCT o.订单ID) AS 订单数,
            COUNT(DISTINCT o.用户ID) AS 客户数,
            COALESCE(SUM(o.实付金额), 0) AS 销售额,
            COALESCE(SUM(o.成本总额), 0) AS 成本,
            COALESCE(SUM(o.毛利), 0) AS 毛利,
            CASE 
                WHEN SUM(o.实付金额) > 0 THEN ROUND(SUM(o.毛利) / SUM(o.实付金额) * 100, 2)
                ELSE 0
            END AS 毛利率,
            CASE 
                WHEN COUNT(DISTINCT o.订单ID) > 0 THEN ROUND(SUM(o.实付金额) / COUNT(DISTINCT o.订单ID), 2)
                ELSE 0
            END AS 客单价,
            CASE 
                WHEN COUNT(DISTINCT o.用户ID) > 0 THEN ROUND(SUM(o.实付金额) / COUNT(DISTINCT o.用户ID), 2)
                ELSE 0
            END AS 人均消费,
            CASE 
                WHEN SUM(o.实付金额) > 0 THEN ROUND(SUM(o.成本总额) / SUM(o.实付金额) * 100, 2)
                ELSE 0
            END AS 成本率
        FROM dwd_order_fact o
        LEFT JOIN dwd_order_detail_fact od ON o.订单ID = od.订单ID
        WHERE o.订单状态 = '已完成'
        GROUP BY o.订单日期, o.年月, o.平台, o.店铺ID, o.店铺名称, od.一级类目, od.二级类目
        """
        
        execute_sql(conn, sql_sales_summary, "创建销售汇总表")
        
        # 2. 流量汇总表
        print("\n2. 构建流量汇总表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_traffic_summary", "删除旧表 dws_traffic_summary")
        
        sql_traffic_summary = """
        CREATE TABLE IF NOT EXISTS dws_traffic_summary AS
        SELECT 
            t.date AS 日期,
            t.store_id AS 店铺ID,
            t.platform AS 平台,
            s.店铺名称,
            t.visitors AS 访客数,
            t.page_views AS 浏览量,
            t.search_traffic AS 搜索流量,
            t.recommend_traffic AS 推荐流量,
            t.direct_traffic AS 直接访问,
            t.other_traffic AS 其他流量,
            t.avg_stay_time AS 平均停留时长,
            t.bounce_rate AS 跳失率,
            COUNT(DISTINCT o.订单ID) AS 成交订单数,
            COALESCE(SUM(o.实付金额), 0) AS 成交金额,
            COUNT(DISTINCT o.用户ID) AS 成交客户数,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(COUNT(DISTINCT o.用户ID) / t.visitors * 100, 2)
                ELSE 0
            END AS 访问转化率,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(COUNT(DISTINCT o.订单ID) / t.visitors * 100, 2)
                ELSE 0
            END AS 下单转化率,
            CASE 
                WHEN COUNT(DISTINCT o.订单ID) > 0 THEN ROUND(SUM(o.实付金额) / COUNT(DISTINCT o.订单ID), 2)
                ELSE 0
            END AS 客单价,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.page_views / t.visitors, 2)
                ELSE 0
            END AS 人均浏览页数,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.search_traffic / t.visitors * 100, 2)
                ELSE 0
            END AS 搜索流量占比,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.recommend_traffic / t.visitors * 100, 2)
                ELSE 0
            END AS 推荐流量占比,
            CASE 
                WHEN t.visitors > 0 THEN ROUND(t.direct_traffic / t.visitors * 100, 2)
                ELSE 0
            END AS 直接访问占比
        FROM ods_traffic t
        LEFT JOIN dim_store s ON t.store_id = s.店铺ID
        LEFT JOIN dwd_order_fact o ON t.store_id = o.店铺ID AND t.date = o.订单日期 AND o.订单状态 = '已完成'
        GROUP BY t.date, t.store_id, t.platform, s.店铺名称, t.visitors, t.page_views, 
                 t.search_traffic, t.recommend_traffic, t.direct_traffic, t.other_traffic,
                 t.avg_stay_time, t.bounce_rate
        """
        
        execute_sql(conn, sql_traffic_summary, "创建流量汇总表")
        
        # 3. 库存汇总表
        print("\n3. 构建库存汇总表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_inventory_summary", "删除旧表 dws_inventory_summary")
        
        sql_inventory_summary = """
        CREATE TABLE IF NOT EXISTS dws_inventory_summary AS
        SELECT 
            p.商品ID,
            p.商品名称,
            p.店铺ID,
            p.店铺名称,
            p.平台,
            p.一级类目,
            p.二级类目,
            p.售价,
            p.成本,
            p.库存 AS 当前库存,
            (p.成本 * p.库存) AS 库存金额_成本,
            (p.售价 * p.库存) AS 库存金额_售价,
            COALESCE(SUM(CASE WHEN i.change_type = '入库' THEN i.change_quantity ELSE 0 END), 0) AS 近30天入库量,
            COALESCE(SUM(CASE WHEN i.change_type = '出库' THEN i.change_quantity ELSE 0 END), 0) AS 近30天出库量,
            CASE 
                WHEN p.库存 > 0 THEN ROUND(SUM(CASE WHEN i.change_type = '出库' THEN i.change_quantity ELSE 0 END) / p.库存, 2)
                ELSE 0
            END AS 库存周转率,
            CASE 
                WHEN p.库存 > 100 THEN '充足'
                WHEN p.库存 > 50 THEN '正常'
                WHEN p.库存 > 20 THEN '偏低'
                ELSE '紧急'
            END AS 库存状态
        FROM dim_product p
        LEFT JOIN ods_inventory i ON p.商品ID = i.product_id 
            AND i.date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        GROUP BY p.商品ID, p.商品名称, p.店铺ID, p.店铺名称, p.平台, 
                 p.一级类目, p.二级类目, p.售价, p.成本, p.库存
        """
        
        execute_sql(conn, sql_inventory_summary, "创建库存汇总表")
        
        # 4. 推广汇总表
        print("\n4. 构建推广汇总表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_promotion_summary", "删除旧表 dws_promotion_summary")
        
        sql_promotion_summary = """
        CREATE TABLE IF NOT EXISTS dws_promotion_summary AS
        SELECT 
            pm.date AS 日期,
            pm.platform AS 平台,
            pm.store_id AS 店铺ID,
            s.店铺名称,
            pm.product_id AS 商品ID,
            p.商品名称,
            p.一级类目,
            p.二级类目,
            pm.channel AS 推广渠道,
            COALESCE(SUM(pm.cost), 0) AS 推广花费,
            COALESCE(SUM(pm.impressions), 0) AS 曝光量,
            COALESCE(SUM(pm.clicks), 0) AS 点击量,
            CASE 
                WHEN SUM(pm.impressions) > 0 THEN ROUND(SUM(pm.clicks) / SUM(pm.impressions) * 100, 2)
                ELSE 0
            END AS 点击率,
            CASE 
                WHEN SUM(pm.clicks) > 0 THEN ROUND(SUM(pm.cost) / SUM(pm.clicks), 2)
                ELSE 0
            END AS 平均点击成本,
            COUNT(DISTINCT o.订单ID) AS 成交订单数,
            COALESCE(SUM(o.实付金额), 0) AS 成交金额,
            COALESCE(SUM(od.数量), 0) AS 成交件数,
            CASE 
                WHEN SUM(pm.clicks) > 0 THEN ROUND(COUNT(DISTINCT o.订单ID) / SUM(pm.clicks) * 100, 2)
                ELSE 0
            END AS 转化率,
            CASE 
                WHEN SUM(pm.cost) > 0 THEN ROUND(SUM(o.实付金额) / SUM(pm.cost), 2)
                ELSE 0
            END AS ROI
        FROM ods_promotion pm
        LEFT JOIN dim_product p ON pm.product_id = p.商品ID
        LEFT JOIN dim_store s ON pm.store_id = s.店铺ID
        LEFT JOIN dwd_order_detail_fact od ON pm.product_id = od.商品ID 
        LEFT JOIN dwd_order_fact o ON od.订单ID = o.订单ID 
            AND pm.date = o.订单日期 
            AND o.订单状态 = '已完成'
        GROUP BY pm.date, pm.platform, pm.store_id, s.店铺名称, pm.product_id, 
                 p.商品名称, p.一级类目, p.二级类目, pm.channel
        """
        
        execute_sql(conn, sql_promotion_summary, "创建推广汇总表")
        
        print("\n" + "="*60)
        print("DWS层转换完成！")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"转换失败: {e}")
        return False
    finally:
        conn.close()


def main():
    """主函数"""
    # 读取配置参数
    config = {}
    if len(sys.argv) > 1:
        try:
            config = json.loads(sys.argv[1])
        except:
            pass
    
    # 获取数据库配置
    db_config = config.get('dbConfig', {
        'host': 'localhost',
        'port': 3306,
        'database': 'datas',
        'user': 'root',
        'password': ''
    })
    
    mode = config.get('mode', 'full')
    
    print(f"模式: {'全量（删除重建）' if mode == 'full' else '增量（追加数据）'}")
    
    success = transform_dws(mode, db_config)
    
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()
