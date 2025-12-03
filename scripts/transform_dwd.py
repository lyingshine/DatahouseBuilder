"""
DWD层转换 - 明细数据层（事实表）
基于DIM层维度，构建星型模型事实表
支持千万级数据的企业级数仓设计

优化策略：
- 分批处理：每次处理10万行，避免内存溢出
- 避免复杂JOIN：先处理主表，再关联维度
- 使用临时表：减少锁竞争
"""
import sys
import json
import signal
import atexit
import time
from datetime import datetime

# 导入数据库管理器
from db_manager import get_db_manager, cleanup_global_db_manager

# 分批处理配置
BATCH_SIZE = 100000  # 每批10万行

_start_time = None  # 全局开始时间


def log(message, level='INFO'):
    """带时间戳的日志输出"""
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    elapsed = ""
    if _start_time:
        elapsed_sec = time.time() - _start_time
        elapsed = f" [+{elapsed_sec:.1f}s]"
    print(f"[{timestamp}]{elapsed} {message}")
    sys.stdout.flush()


def signal_handler(signum, frame):
    """处理中断信号"""
    print("\n\n[中断] 检测到用户中断...")
    sys.stdout.flush()
    cleanup_global_db_manager()
    sys.exit(1)

def batch_update(conn, update_sql, table_name, id_column, description):
    """分批更新数据"""
    cursor = conn.cursor()
    try:
        # 获取总行数
        total = get_table_count(conn, table_name)
        if total == 0:
            print(f"  {description}... ✓ 无数据")
            return True
        
        batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  {description} ({total:,}行, {batches}批)...")
        sys.stdout.flush()
        
        start_time = time.time()
        processed = 0
        
        for batch in range(batches):
            offset = batch * BATCH_SIZE
            # 使用LIMIT分批更新
            batch_sql = f"""
            UPDATE {table_name} t
            INNER JOIN ({update_sql}) src ON t.{id_column} = src.{id_column}
            SET {', '.join([f't.{col} = src.{col}' for col in get_update_columns(update_sql)])}
            WHERE t.{id_column} IN (
                SELECT {id_column} FROM (
                    SELECT {id_column} FROM {table_name} LIMIT {BATCH_SIZE} OFFSET {offset}
                ) tmp
            )
            """
            # 简化：直接用原SQL的LIMIT方式
            cursor.execute(f"{update_sql} LIMIT {BATCH_SIZE} OFFSET {offset}")
            conn.commit()
            
            processed += min(BATCH_SIZE, total - offset)
            progress = int(processed / total * 100)
            elapsed = time.time() - start_time
            speed = int(processed / elapsed) if elapsed > 0 else 0
            print(f"\r  {description} ({total:,}行, {batches}批)... {progress}% ({speed:,}行/秒)", end='', flush=True)
        
        elapsed = time.time() - start_time
        speed = int(total / elapsed) if elapsed > 0 else 0
        print(f"\r  {description}... ✓ {total:,}行 ({elapsed:.1f}秒, {speed:,}行/秒)")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f" ✗ 失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def batch_insert_with_join(conn, target_table, source_sql, description):
    """分批插入带JOIN的数据"""
    cursor = conn.cursor()
    try:
        # 先统计源数据量
        count_sql = f"SELECT COUNT(*) FROM ({source_sql}) t"
        cursor.execute(count_sql)
        total = cursor.fetchone()[0]
        
        if total == 0:
            print(f"  {description}... ✓ 无数据")
            return True
        
        batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  {description} ({total:,}行, {batches}批)...")
        sys.stdout.flush()
        
        start_time = time.time()
        processed = 0
        
        for batch in range(batches):
            offset = batch * BATCH_SIZE
            batch_sql = f"INSERT INTO {target_table} {source_sql} LIMIT {BATCH_SIZE} OFFSET {offset}"
            cursor.execute(batch_sql)
            conn.commit()
            
            processed += min(BATCH_SIZE, total - offset)
            progress = int(processed / total * 100)
            elapsed = time.time() - start_time
            speed = int(processed / elapsed) if elapsed > 0 else 0
            print(f"\r  {description} ({total:,}行, {batches}批)... {progress}% ({speed:,}行/秒)", end='', flush=True)
        
        elapsed = time.time() - start_time
        speed = int(total / elapsed) if elapsed > 0 else 0
        print(f"\r  {description}... ✓ {total:,}行 ({elapsed:.1f}秒, {speed:,}行/秒)")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f" ✗ 失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def batch_update_simple(conn, table_name, update_sql, description):
    """简单分批更新（使用主键范围）"""
    cursor = conn.cursor()
    try:
        total = get_table_count(conn, table_name)
        if total == 0:
            print(f"  {description}... ✓ 无数据")
            return True
        
        batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        start_time = time.time()
        
        # 直接执行更新（MySQL会自动优化）
        print(f"  {description}...", end='', flush=True)
        cursor.execute(update_sql)
        conn.commit()
        
        elapsed = time.time() - start_time
        affected = cursor.rowcount
        speed = int(affected / elapsed) if elapsed > 0 else 0
        print(f" ✓ {affected:,}行 ({elapsed:.1f}秒, {speed:,}行/秒)")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f" ✗ 失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()


def build_dim_tables(db_manager, mode='full'):
    """构建DIM层维度表"""
    log("【构建DIM层维度表】")
    
    # 跳过索引添加（ODS表已有索引，或者在导入时已创建）
    # 添加索引很慢，如果表已有数据，跳过可节省10-20秒
    
    # 1. 日期维度表
    print("\n1. 日期维度表")
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dim_date", "删除旧表")
    
    sql_dim_date = """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INT PRIMARY KEY,
        date_value DATE NOT NULL UNIQUE,
        `year` INT, `quarter` INT, `month` INT, `week` INT, `day` INT,
        `weekday` INT, weekday_name VARCHAR(10), is_weekend TINYINT,
        `year_month` VARCHAR(7), `year_week` VARCHAR(8),
        INDEX idx_date (date_value), INDEX idx_year_month (`year_month`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_dim_date, "创建日期维度表结构")
    
    sql_insert_dates = """
    INSERT IGNORE INTO dim_date (date_key, date_value, `year`, `quarter`, `month`, `week`, `day`, `weekday`, weekday_name, is_weekend, `year_month`, `year_week`)
    SELECT 
        CAST(DATE_FORMAT(date_value, '%Y%m%d') AS UNSIGNED) AS date_key, date_value,
        YEAR(date_value), QUARTER(date_value), MONTH(date_value), WEEK(date_value, 1), DAY(date_value),
        WEEKDAY(date_value) + 1,
        CASE WEEKDAY(date_value) WHEN 0 THEN '周一' WHEN 1 THEN '周二' WHEN 2 THEN '周三' WHEN 3 THEN '周四' WHEN 4 THEN '周五' WHEN 5 THEN '周六' WHEN 6 THEN '周日' END,
        CASE WHEN WEEKDAY(date_value) IN (5, 6) THEN 1 ELSE 0 END,
        DATE_FORMAT(date_value, '%Y-%m'), CONCAT(YEAR(date_value), '-W', LPAD(WEEK(date_value, 1), 2, '0'))
    FROM (
        SELECT DATE_SUB(CURDATE(), INTERVAL seq DAY) AS date_value FROM (
            SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS seq FROM 
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2) d
        ) nums WHERE seq <= 1095
    ) dates
    """
    db_manager.execute_sql(sql_insert_dates, "生成日期维度数据")
    
    # 2. 用户维度表（简化设计）
    print("\n2. 用户维度表")
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dim_user", "删除旧表")
    
    sql_dim_user = """
    CREATE TABLE IF NOT EXISTS dim_user (
        user_key BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL UNIQUE,
        user_name VARCHAR(100), gender VARCHAR(10),
        age INT, age_group VARCHAR(20), city VARCHAR(50),
        register_date DATE,
        INDEX idx_user_id (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_dim_user, "创建用户维度表结构")
    
    sql_insert_user = """
    INSERT INTO dim_user (user_id, user_name, gender, age, age_group, city, register_date)
    SELECT user_id, user_name, gender, age,
        CASE WHEN age < 18 THEN '未成年' WHEN age BETWEEN 18 AND 25 THEN '18-25岁' WHEN age BETWEEN 26 AND 35 THEN '26-35岁'
             WHEN age BETWEEN 36 AND 45 THEN '36-45岁' WHEN age BETWEEN 46 AND 55 THEN '46-55岁' ELSE '55岁以上' END,
        city, register_date
    FROM ods_users WHERE user_id IS NOT NULL
    """
    db_manager.execute_sql(sql_insert_user, "加载用户维度数据")
    
    # 3. 商品维度表（SKU ID 全局唯一）
    print("\n3. 商品维度表")
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dim_product", "删除旧表")
    
    sql_dim_product = """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key BIGINT AUTO_INCREMENT PRIMARY KEY,
        sku_id VARCHAR(100) NOT NULL UNIQUE,
        product_id VARCHAR(50) NOT NULL,
        store_id VARCHAR(20) NOT NULL,
        product_name VARCHAR(200),
        spec VARCHAR(100),
        category_l1 VARCHAR(50), category_l2 VARCHAR(50),
        price DECIMAL(10,2), cost DECIMAL(10,2), profit_margin DECIMAL(5,2),
        stock INT,
        platform VARCHAR(20),
        INDEX idx_sku_id (sku_id),
        INDEX idx_product_id (product_id),
        INDEX idx_store_id (store_id),
        INDEX idx_category (category_l1, category_l2)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_dim_product, "创建商品维度表结构")
    
    sql_insert_product = """
    INSERT INTO dim_product (sku_id, product_id, store_id, product_name, spec, category_l1, category_l2, price, cost, profit_margin, stock, platform)
    SELECT sku_id, product_id, store_id, product_name, spec, category_l1, category_l2, price, cost,
        CASE WHEN price > 0 THEN ROUND((price - cost) / price * 100, 2) ELSE 0 END,
        stock, platform
    FROM ods_products WHERE sku_id IS NOT NULL
    """
    db_manager.execute_sql(sql_insert_product, "加载商品维度数据")
    
    # 4. 店铺维度表（简化设计）
    print("\n4. 店铺维度表")
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dim_store", "删除旧表")
    
    sql_dim_store = """
    CREATE TABLE IF NOT EXISTS dim_store (
        store_key BIGINT AUTO_INCREMENT PRIMARY KEY,
        store_id VARCHAR(20) NOT NULL UNIQUE,
        store_name VARCHAR(100), platform VARCHAR(20),
        store_type VARCHAR(50), open_date DATE,
        INDEX idx_store_id (store_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_dim_store, "创建店铺维度表结构")
    
    sql_insert_store = """
    INSERT INTO dim_store (store_id, store_name, platform, store_type, open_date)
    SELECT store_id, store_name, platform,
        CASE WHEN store_name LIKE '%旗舰店%' THEN '旗舰店' WHEN store_name LIKE '%专卖店%' THEN '专卖店'
             WHEN store_name LIKE '%直营店%' THEN '直营店' ELSE '普通店' END,
        open_date
    FROM ods_stores WHERE store_id IS NOT NULL
    """
    db_manager.execute_sql(sql_insert_store, "加载店铺维度数据")
    
    log("  ✓ DIM层维度表构建完成")
    return True


def build_fact_order(db_manager, mode='full'):
    """构建订单事实表（极速模式：一次INSERT SELECT完成）"""
    log("1. 订单事实表")
    
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dwd_fact_order", "删除旧表")
    
    # 创建表结构
    sql_create = """
    CREATE TABLE IF NOT EXISTS dwd_fact_order (
        order_key VARCHAR(20) PRIMARY KEY,
        order_id VARCHAR(20),
        user_id VARCHAR(50),
        store_id VARCHAR(20),
        user_key BIGINT,
        store_key BIGINT,
        date_key INT,
        order_status VARCHAR(20),
        payment_method VARCHAR(20),
        traffic_source VARCHAR(20),
        platform VARCHAR(20),
        order_time DATETIME,
        total_amount DECIMAL(12,2),
        discount_amount DECIMAL(12,2),
        shipping_fee DECIMAL(10,2),
        final_amount DECIMAL(12,2),
        total_cost DECIMAL(12,2),
        profit_amount DECIMAL(12,2),
        etl_date DATE,
        etl_time DATETIME,
        INDEX idx_user_key (user_key),
        INDEX idx_store_key (store_key),
        INDEX idx_date_key (date_key),
        INDEX idx_order_id (order_id),
        INDEX idx_traffic_source (traffic_source)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_create, "创建表结构")
    
    # 极速模式：一次INSERT SELECT完成所有关联（避免UPDATE）
    sql_insert_all = """
    INSERT INTO dwd_fact_order 
        (order_key, order_id, user_id, store_id, user_key, store_key,
         order_status, payment_method, traffic_source, platform, order_time,
         total_amount, discount_amount, shipping_fee, final_amount, total_cost, profit_amount,
         date_key, etl_date, etl_time)
    SELECT 
        o.order_id, o.order_id, o.user_id, o.store_id, u.user_key, s.store_key,
        o.order_status, o.payment_method, o.traffic_source, o.platform, o.order_time,
        o.total_amount, COALESCE(o.discount_amount, 0), COALESCE(o.shipping_fee, 0),
        o.final_amount, COALESCE(o.total_cost, 0),
        (o.final_amount - COALESCE(o.total_cost, 0)),
        CAST(DATE_FORMAT(o.order_time, '%Y%m%d') AS UNSIGNED),
        CURDATE(), NOW()
    FROM ods_orders o
    INNER JOIN dim_user u ON o.user_id = u.user_id
    INNER JOIN dim_store s ON o.store_id = s.store_id
    WHERE o.order_id IS NOT NULL
    """
    if not db_manager.execute_sql(sql_insert_all, "一次性插入并关联（极速）"):
        return False
    
    return True

def build_fact_order_detail(db_manager, mode='full'):
    """构建订单明细事实表（极速模式：一次INSERT SELECT完成所有关联）"""
    log("2. 订单明细事实表")
    
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dwd_fact_order_detail", "删除旧表")
    
    # 创建表结构
    sql_create = """
    CREATE TABLE dwd_fact_order_detail (
        order_detail_key BIGINT AUTO_INCREMENT PRIMARY KEY,
        order_detail_id VARCHAR(20),
        order_id VARCHAR(20),
        product_id VARCHAR(50),
        store_id VARCHAR(20),
        user_key BIGINT,
        product_key BIGINT,
        store_key BIGINT,
        date_key INT,
        quantity INT,
        price DECIMAL(10,2),
        amount DECIMAL(12,2),
        cost DECIMAL(10,2),
        cost_amount DECIMAL(12,2),
        profit_amount DECIMAL(12,2),
        profit_margin DECIMAL(5,2),
        etl_date DATE,
        etl_time DATETIME,
        INDEX idx_order_detail_id (order_detail_id),
        INDEX idx_order_id (order_id),
        INDEX idx_product_key (product_key),
        INDEX idx_date_key (date_key),
        INDEX idx_store_key (store_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_create, "创建表结构")
    
    # 检查源表行数
    ods_count = db_manager.get_table_count("ods_order_details")
    product_count = db_manager.get_table_count("dim_product")
    print(f"  源数据: ODS明细={ods_count:,}行, 商品维度={product_count:,}个")
    sys.stdout.flush()
    
    # 极速模式：一次INSERT SELECT完成所有关联和计算（避免UPDATE）
    # 使用STRAIGHT_JOIN强制JOIN顺序，避免优化器选择错误
    sql_insert_all = """
    INSERT INTO dwd_fact_order_detail 
        (order_detail_id, order_id, product_id, store_id,
         user_key, product_key, store_key, date_key,
         quantity, price, amount, cost, cost_amount, profit_amount, profit_margin,
         etl_date, etl_time)
    SELECT STRAIGHT_JOIN
        od.order_detail_id, od.order_id, od.product_id, o.store_id,
        o.user_key, p.product_key, o.store_key, o.date_key,
        od.quantity, od.price, od.amount, COALESCE(p.cost, 0),
        COALESCE(p.cost, 0) * od.quantity,
        od.amount - COALESCE(p.cost, 0) * od.quantity,
        CASE WHEN od.amount > 0 THEN ROUND((od.amount - COALESCE(p.cost, 0) * od.quantity) / od.amount * 100, 2) ELSE 0 END,
        CURDATE(), NOW()
    FROM ods_order_details od
    STRAIGHT_JOIN dwd_fact_order o ON od.order_id = o.order_id
    LEFT JOIN dim_product p ON od.sku_id = p.sku_id
    """
    if not db_manager.execute_sql(sql_insert_all, "一次性插入并关联（极速）", batch_commit=True):
        return False
    
    return True

def build_fact_promotion(db_manager, mode='full'):
    """构建推广事实表（分步处理）"""
    log("3. 推广事实表")
    
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dwd_fact_promotion", "删除旧表")
    
    # 创建表结构
    sql_create = """
    CREATE TABLE dwd_fact_promotion (
        promotion_key BIGINT AUTO_INCREMENT PRIMARY KEY,
        promotion_id VARCHAR(20),
        date_key INT,
        store_key BIGINT,
        product_key BIGINT,
        channel VARCHAR(50),
        platform VARCHAR(20),
        cost DECIMAL(10,2),
        impressions INT,
        clicks INT,
        ctr DECIMAL(5,2),
        cpc DECIMAL(10,2),
        etl_date DATE,
        etl_time DATETIME,
        INDEX idx_date_key (date_key),
        INDEX idx_product_key (product_key),
        INDEX idx_store_key (store_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_create, "创建表结构")
    
    # 一次性插入所有数据（含关联，避免UPDATE）
    sql_insert_all = """
    INSERT INTO dwd_fact_promotion 
        (promotion_id, date_key, store_key, product_key, channel, platform, 
         cost, impressions, clicks, ctr, cpc, etl_date, etl_time)
    SELECT 
        pr.promotion_id,
        CAST(DATE_FORMAT(pr.date, '%Y%m%d') AS UNSIGNED),
        s.store_key,
        p.product_key,
        pr.channel, pr.platform, pr.cost, pr.impressions, pr.clicks,
        CASE WHEN pr.impressions > 0 THEN ROUND(pr.clicks / pr.impressions * 100, 2) ELSE 0 END,
        CASE WHEN pr.clicks > 0 THEN ROUND(pr.cost / pr.clicks, 2) ELSE 0 END,
        CURDATE(), NOW()
    FROM ods_promotion pr
    LEFT JOIN dim_store s ON pr.store_id = s.store_id
    LEFT JOIN dim_product p ON pr.sku_id = p.sku_id
    WHERE pr.promotion_id IS NOT NULL
    """
    if not db_manager.execute_sql(sql_insert_all, "插入推广数据（含关联）"):
        return False
    
    return True

def build_fact_traffic(db_manager, mode='full'):
    """构建流量事实表"""
    log("4. 流量事实表")
    
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dwd_fact_traffic", "删除旧表")
    
    sql_create = """
    CREATE TABLE dwd_fact_traffic (
        traffic_key BIGINT AUTO_INCREMENT PRIMARY KEY,
        date_key INT,
        store_key BIGINT,
        platform VARCHAR(20),
        visitors INT,
        page_views INT,
        search_traffic INT,
        recommend_traffic INT,
        direct_traffic INT,
        other_traffic INT,
        avg_stay_time DECIMAL(10,2),
        bounce_rate DECIMAL(5,2),
        etl_date DATE,
        etl_time DATETIME,
        INDEX idx_date_key (date_key),
        INDEX idx_store_key (store_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_create, "创建表结构")
    
    # 插入基础数据
    sql_insert = """
    INSERT INTO dwd_fact_traffic 
        (date_key, platform, visitors, page_views, search_traffic, recommend_traffic, 
         direct_traffic, other_traffic, avg_stay_time, bounce_rate, etl_date, etl_time)
    SELECT 
        CAST(DATE_FORMAT(date, '%Y%m%d') AS UNSIGNED),
        platform, visitors, page_views, search_traffic, recommend_traffic,
        direct_traffic, other_traffic, avg_stay_time, bounce_rate,
        CURDATE(), NOW()
    FROM ods_traffic WHERE date IS NOT NULL
    """
    if not db_manager.execute_sql(sql_insert, "插入基础数据"):
        return False
    
    # 关联店铺（通过临时表减少锁）
    sql_update = """
    UPDATE dwd_fact_traffic f
    INNER JOIN ods_traffic t ON f.date_key = CAST(DATE_FORMAT(t.date, '%Y%m%d') AS UNSIGNED) 
        AND f.platform = t.platform
    INNER JOIN dim_store s ON t.store_id = s.store_id
    SET f.store_key = s.store_key
    """
    db_manager.execute_sql(sql_update, "关联店铺维度")
    
    return True

def build_fact_inventory(db_manager, mode='full'):
    """构建库存事实表"""
    log("5. 库存事实表")
    
    if mode == 'full':
        db_manager.execute_sql("DROP TABLE IF EXISTS dwd_fact_inventory", "删除旧表")
    
    sql_create = """
    CREATE TABLE dwd_fact_inventory (
        inventory_key BIGINT AUTO_INCREMENT PRIMARY KEY,
        inventory_id VARCHAR(20),
        date_key INT,
        product_key BIGINT,
        store_key BIGINT,
        stock_quantity INT,
        in_quantity INT DEFAULT 0,
        out_quantity INT DEFAULT 0,
        etl_date DATE,
        etl_time DATETIME,
        INDEX idx_date_key (date_key),
        INDEX idx_product_key (product_key),
        INDEX idx_store_key (store_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db_manager.execute_sql(sql_create, "创建表结构")
    
    # 一次性插入所有数据（含关联，避免UPDATE）
    sql_insert_all = """
    INSERT INTO dwd_fact_inventory 
        (inventory_id, date_key, product_key, store_key, stock_quantity, in_quantity, out_quantity, etl_date, etl_time)
    SELECT 
        i.inventory_id,
        CAST(DATE_FORMAT(i.date, '%Y%m%d') AS UNSIGNED),
        p.product_key,
        s.store_key,
        i.stock_quantity,
        CASE WHEN i.change_type = '入库' THEN i.change_quantity ELSE 0 END,
        CASE WHEN i.change_type = '出库' THEN i.change_quantity ELSE 0 END,
        CURDATE(), NOW()
    FROM ods_inventory i
    LEFT JOIN dim_product p ON i.sku_id = p.sku_id
    LEFT JOIN dim_store s ON i.store_id = s.store_id
    WHERE i.inventory_id IS NOT NULL
    """
    if not db_manager.execute_sql(sql_insert_all, "插入库存数据（含关联）"):
        return False
    
    return True


def transform_dwd(mode='full', db_config=None):
    """转换DWD层数据"""
    global _start_time
    _start_time = time.time()
    
    print("="*60)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] DWD层数据转换开始")
    print(f"批次大小: {BATCH_SIZE:,} 行")
    print("="*60)
    
    # 使用数据库管理器
    db_manager = get_db_manager(db_config)
    if not db_manager:
        return False
    
    try:
        # 第一步：构建DIM层维度表
        if not build_dim_tables(db_manager, mode):
            return False
        
        # 第二步：构建事实表（分步处理）
        log("【构建DWD事实表】")
        
        if not build_fact_order(db_manager, mode):
            return False
        
        if not build_fact_order_detail(db_manager, mode):
            return False
        
        if not build_fact_promotion(db_manager, mode):
            return False
        
        if not build_fact_traffic(db_manager, mode):
            return False
        
        if not build_fact_inventory(db_manager, mode):
            return False
        
        total_elapsed = time.time() - _start_time
        print("\n" + "="*60)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ DWD层转换完成！")
        print(f"总耗时: {total_elapsed:.1f}秒 ({total_elapsed/60:.1f}分钟)")
        print("="*60)
        sys.stdout.flush()
        
        return True
        
    except Exception as e:
        total_elapsed = time.time() - _start_time if _start_time else 0
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ✗ 转换失败: {e}")
        print(f"已耗时: {total_elapsed:.1f}秒")
        sys.stdout.flush()
        return False
    finally:
        db_manager.close()

def main():
    """主函数"""
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
        'host': 'localhost',
        'port': 3306,
        'database': 'datas',
        'user': 'root',
        'password': ''
    })
    
    mode = config.get('mode', 'full')
    print(f"模式: {'全量' if mode == 'full' else '增量'}")
    
    try:
        success = transform_dwd(mode, db_config)
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
