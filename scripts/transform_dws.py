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
    """执行SQL语句（极速模式）"""
    try:
        print(f"  正在执行: {description}...")
        sys.stdout.flush()
        
        cursor = conn.cursor()
        
        # 极速优化
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET autocommit=0")
        
        cursor.execute(sql)
        
        # 恢复设置
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")
        
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        
        print(f"  ✓ {description} - 影响 {affected_rows:,} 行")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f"  ✗ {description} 失败: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
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
        # 创建索引以加速查询
        print("\n创建索引...")
        sys.stdout.flush()
        cursor = conn.cursor()
        
        # 为 dwd_order_fact 创建索引
        try:
            print("  正在为 dwd_order_fact 创建索引...")
            sys.stdout.flush()
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_date ON dwd_order_fact(订单日期)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_status ON dwd_order_fact(订单状态)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_store ON dwd_order_fact(店铺ID)")
            print("  ✓ dwd_order_fact 索引创建完成")
            sys.stdout.flush()
        except Exception as e:
            print(f"  索引创建警告: {e}")
            sys.stdout.flush()
        
        # 为 dwd_order_detail_fact 创建索引
        try:
            print("  正在为 dwd_order_detail_fact 创建索引...")
            sys.stdout.flush()
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_detail_order ON dwd_order_detail_fact(订单ID)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_detail_product ON dwd_order_detail_fact(商品ID)")
            print("  ✓ dwd_order_detail_fact 索引创建完成")
            sys.stdout.flush()
        except Exception as e:
            print(f"  索引创建警告: {e}")
            sys.stdout.flush()
        
        conn.commit()
        cursor.close()
        
        # 1. 销售汇总大宽表
        print("\n1. 构建销售汇总大宽表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_sales_summary", "删除旧表 dws_sales_summary")
        
        # 优化：简化SQL，只从订单明细表聚合（避免大型JOIN）
        print("  创建表结构...")
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS dws_sales_summary")
        cursor.execute("""
            CREATE TABLE dws_sales_summary (
                日期 DATE,
                年月 VARCHAR(10),
                年份 INT,
                季度 INT,
                月份 INT,
                星期 INT,
                平台 VARCHAR(50),
                店铺ID VARCHAR(50),
                店铺名称 VARCHAR(100),
                商品ID VARCHAR(50),
                商品名称 VARCHAR(200),
                一级类目 VARCHAR(50),
                二级类目 VARCHAR(50),
                订单数 INT,
                客户数 INT,
                销售件数 INT,
                销售额 DECIMAL(15,2),
                成本 DECIMAL(15,2),
                毛利 DECIMAL(15,2),
                毛利率 DECIMAL(10,2),
                客单价 DECIMAL(10,2),
                件单价 DECIMAL(10,2),
                优惠金额 DECIMAL(15,2),
                运费 DECIMAL(15,2),
                INDEX idx_date (日期),
                INDEX idx_product (商品ID)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        cursor.close()
        print("  ✓ 表结构创建完成")
        
        # 分批插入数据（避免大型JOIN）
        print("  分批插入数据...")
        sql_insert = """
        INSERT INTO dws_sales_summary
        SELECT 
            DATE(o.订单日期) AS 日期,
            DATE_FORMAT(o.订单日期, '%Y-%m') AS 年月,
            YEAR(o.订单日期) AS 年份,
            QUARTER(o.订单日期) AS 季度,
            MONTH(o.订单日期) AS 月份,
            DAYOFWEEK(o.订单日期) AS 星期,
            o.平台,
            o.店铺ID,
            o.店铺名称,
            od.商品ID,
            COALESCE(p.商品名称, '') AS 商品名称,
            COALESCE(p.一级类目, '') AS 一级类目,
            COALESCE(p.二级类目, '') AS 二级类目,
            COUNT(DISTINCT od.订单ID) AS 订单数,
            0 AS 客户数,
            SUM(od.数量) AS 销售件数,
            SUM(od.金额) AS 销售额,
            SUM(od.成本金额) AS 成本,
            SUM(od.毛利) AS 毛利,
            ROUND(AVG(od.毛利率), 2) AS 毛利率,
            ROUND(SUM(od.金额) / NULLIF(COUNT(DISTINCT od.订单ID), 0), 2) AS 客单价,
            ROUND(SUM(od.数量) / NULLIF(COUNT(DISTINCT od.订单ID), 0), 2) AS 件单价,
            0 AS 优惠金额,
            0 AS 运费
        FROM dwd_order_detail_fact od
        INNER JOIN dwd_order_fact o ON od.订单ID = o.订单ID
        LEFT JOIN dim_product p ON od.商品ID = p.商品ID
        WHERE o.订单状态 = '已完成'
        GROUP BY DATE(o.订单日期), o.平台, o.店铺ID, o.店铺名称, od.商品ID
        """
        
        if not execute_sql(conn, sql_insert, "插入销售汇总数据"):
            return False
        
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
        
        if not execute_sql(conn, sql_traffic_summary, "创建流量汇总表"):
            return False
        
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
        
        if not execute_sql(conn, sql_inventory_summary, "创建库存汇总表"):
            return False
        
        # 4. 推广汇总表
        print("\n4. 构建推广汇总表...")
        
        if mode == 'full':
            execute_sql(conn, "DROP TABLE IF EXISTS dws_promotion_summary", "删除旧表 dws_promotion_summary")
        
        # 简化版推广汇总表（只包含推广数据，不关联订单以提升性能）
        sql_promotion_summary = """
        CREATE TABLE IF NOT EXISTS dws_promotion_summary AS
        SELECT 
            pm.date AS 日期,
            pm.platform AS 平台,
            pm.store_id AS 店铺ID,
            COALESCE(s.店铺名称, '') AS 店铺名称,
            pm.product_id AS 商品ID,
            COALESCE(p.商品名称, '') AS 商品名称,
            COALESCE(p.一级类目, '') AS 一级类目,
            COALESCE(p.二级类目, '') AS 二级类目,
            pm.channel AS 推广渠道,
            SUM(pm.cost) AS 推广花费,
            SUM(pm.impressions) AS 曝光量,
            SUM(pm.clicks) AS 点击量,
            CASE 
                WHEN SUM(pm.impressions) > 0 THEN ROUND(SUM(pm.clicks) / SUM(pm.impressions) * 100, 2)
                ELSE 0
            END AS 点击率,
            CASE 
                WHEN SUM(pm.clicks) > 0 THEN ROUND(SUM(pm.cost) / SUM(pm.clicks), 2)
                ELSE 0
            END AS 平均点击成本,
            0 AS 成交订单数,
            0 AS 成交金额,
            0 AS 成交件数,
            0 AS 转化率,
            0 AS ROI
        FROM ods_promotion pm
        LEFT JOIN dim_product p ON pm.product_id = p.商品ID
        LEFT JOIN dim_store s ON pm.store_id = s.店铺ID
        GROUP BY pm.date, pm.platform, pm.store_id, s.店铺名称, pm.product_id, 
                 p.商品名称, p.一级类目, p.二级类目, pm.channel
        """
        
        if not execute_sql(conn, sql_promotion_summary, "创建推广汇总表"):
            return False
        
        print("\n" + "="*60)
        print("✓ DWS层转换完成！")
        print("="*60)
        sys.stdout.flush()
        
        return True
        
    except Exception as e:
        print(f"✗ 转换失败: {e}")
        sys.stdout.flush()
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
