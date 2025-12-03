-- ============================================
-- 自行车电商数据仓库建表SQL
-- 适用于MySQL/PostgreSQL（根据需要调整数据类型）
-- ============================================

-- ============================================
-- ODS层（原始数据层）
-- ============================================

-- 店铺表
CREATE TABLE ods_stores (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    platform VARCHAR(20),
    open_date DATE
);

-- 商品表（完善版 - 支持商品ID/SKU ID/产品编码关系）
CREATE TABLE ods_products (
    sku_id VARCHAR(20) PRIMARY KEY COMMENT 'SKU ID - 平台唯一',
    product_id VARCHAR(20) NOT NULL COMMENT '商品ID - 平台唯一，一个商品包含多个SKU',
    product_code VARCHAR(100) NOT NULL COMMENT '产品编码 - 公司内部编码，跨店铺共享',
    spec_code VARCHAR(200) NOT NULL COMMENT '规格编码 - 公司内部规格编码，跨店铺共享',
    store_id VARCHAR(20) NOT NULL COMMENT '店铺ID',
    platform VARCHAR(20) COMMENT '平台',
    product_name VARCHAR(200) COMMENT '商品名称',
    spec VARCHAR(200) COMMENT '规格描述',
    category_l1 VARCHAR(50) COMMENT '一级类目',
    category_l2 VARCHAR(50) COMMENT '二级类目',
    product_tier VARCHAR(20) COMMENT '商品分层',
    price DECIMAL(10,2) COMMENT '售价',
    cost DECIMAL(10,2) COMMENT '成本',
    stock INT COMMENT '库存',
    create_time DATETIME COMMENT '创建时间',
    INDEX idx_product_id (product_id),
    INDEX idx_product_code (product_code),
    INDEX idx_store (store_id),
    INDEX idx_platform (platform)
) COMMENT='商品表 - SKU维度，一个商品ID包含多个SKU';

-- 用户表
CREATE TABLE ods_users (
    user_id VARCHAR(50) PRIMARY KEY,
    user_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50),
    register_date DATE
);

-- 订单主表
CREATE TABLE ods_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    user_id VARCHAR(50),
    store_id VARCHAR(20),
    platform VARCHAR(20),
    order_time DATETIME,
    order_status VARCHAR(20),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    shipping_fee DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    total_cost DECIMAL(10,2),
    payment_method VARCHAR(20),
    create_time DATETIME,
    update_time DATETIME
);

-- 订单明细表
CREATE TABLE ods_order_details (
    order_detail_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20),
    sku_id VARCHAR(20) COMMENT 'SKU ID - 具体购买的规格',
    product_id VARCHAR(20) COMMENT '商品ID - 对应的商品',
    quantity INT,
    price DECIMAL(10,2),
    amount DECIMAL(10,2),
    INDEX idx_order (order_id),
    INDEX idx_sku (sku_id),
    INDEX idx_product (product_id)
);

-- ============================================
-- DWD层（明细数据层）- 维度表 + 事实表
-- ============================================

-- ========== 维度表 ==========

-- 用户维度表
CREATE TABLE dim_user (
    user_id VARCHAR(50) PRIMARY KEY,
    user_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    age_group VARCHAR(20),
    city VARCHAR(50),
    register_date DATE
);

-- 商品维度表
CREATE TABLE dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit_rate DECIMAL(5,2),
    stock INT,
    store_id VARCHAR(20),
    platform VARCHAR(20),
    create_time DATETIME
);

-- 店铺维度表
CREATE TABLE dim_store (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    platform VARCHAR(20),
    open_date DATE
);

-- 时间维度表
CREATE TABLE dim_date (
    date_id VARCHAR(10) PRIMARY KEY,  -- YYYY-MM-DD
    date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    weekday INT,
    weekday_name VARCHAR(10),
    is_weekend TINYINT,
    year_month VARCHAR(7)  -- YYYY-MM
);

-- 类目维度表
CREATE TABLE dim_category (
    category_id VARCHAR(20) PRIMARY KEY,
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50)
);

-- ========== 事实表 ==========

-- 订单事实表
CREATE TABLE fact_order (
    order_id VARCHAR(20) PRIMARY KEY,
    user_id VARCHAR(50),
    store_id VARCHAR(20),
    date_id VARCHAR(10),
    order_time DATETIME,
    order_status VARCHAR(20),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    shipping_fee DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    total_cost DECIMAL(10,2),
    profit_amount DECIMAL(10,2),
    payment_method VARCHAR(20),
    platform VARCHAR(20),
    create_time DATETIME,
    update_time DATETIME,
    INDEX idx_user (user_id),
    INDEX idx_store (store_id),
    INDEX idx_date (date_id),
    INDEX idx_status (order_status)
);

-- 订单明细事实表
CREATE TABLE fact_order_detail (
    order_detail_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    user_id VARCHAR(50),
    store_id VARCHAR(20),
    date_id VARCHAR(10),
    quantity INT,
    price DECIMAL(10,2),
    amount DECIMAL(10,2),
    cost DECIMAL(10,2),
    cost_amount DECIMAL(10,2),
    profit_amount DECIMAL(10,2),
    INDEX idx_order (order_id),
    INDEX idx_product (product_id),
    INDEX idx_date (date_id)
);

-- 推广事实表
CREATE TABLE fact_promotion (
    promotion_id VARCHAR(20) PRIMARY KEY,
    date_id VARCHAR(10),
    store_id VARCHAR(20),
    product_id VARCHAR(20),
    channel VARCHAR(50),
    cost DECIMAL(10,2),
    impressions INT,
    clicks INT,
    platform VARCHAR(20),
    INDEX idx_date (date_id),
    INDEX idx_product (product_id),
    INDEX idx_store (store_id)
);

-- 流量事实表
CREATE TABLE fact_traffic (
    traffic_id VARCHAR(20) PRIMARY KEY,
    date_id VARCHAR(10),
    store_id VARCHAR(20),
    visitors INT,
    page_views INT,
    search_traffic INT,
    recommend_traffic INT,
    direct_traffic INT,
    other_traffic INT,
    avg_stay_time DECIMAL(10,2),
    bounce_rate DECIMAL(5,2),
    platform VARCHAR(20),
    INDEX idx_date (date_id),
    INDEX idx_store (store_id)
);

-- 库存事实表
CREATE TABLE fact_inventory (
    inventory_id VARCHAR(20) PRIMARY KEY,
    date_id VARCHAR(10),
    product_id VARCHAR(20),
    store_id VARCHAR(20),
    stock_quantity INT,
    in_quantity INT,
    out_quantity INT,
    INDEX idx_date (date_id),
    INDEX idx_product (product_id)
);

-- ============================================
-- DWS层（汇总数据层）- 多维度汇总
-- ============================================

-- ========== 时间维度汇总 ==========

-- 日汇总表
CREATE TABLE dws_sales_daily (
    date_id VARCHAR(10),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    avg_order_amount DECIMAL(10,2),
    PRIMARY KEY (date_id, platform)
);

-- 月汇总表
CREATE TABLE dws_sales_monthly (
    year_month VARCHAR(7),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    avg_order_amount DECIMAL(10,2),
    PRIMARY KEY (year_month, platform)
);

-- ========== 店铺维度汇总 ==========

-- 店铺日汇总
CREATE TABLE dws_store_daily (
    date_id VARCHAR(10),
    store_id VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (date_id, store_id)
);

-- 店铺总汇总
CREATE TABLE dws_store_total (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    avg_order_amount DECIMAL(10,2)
);

-- ========== 商品维度汇总 ==========

-- 商品日汇总
CREATE TABLE dws_product_daily (
    date_id VARCHAR(10),
    product_id VARCHAR(20),
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (date_id, product_id)
);

-- 商品总汇总
CREATE TABLE dws_product_total (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2)
);

-- ========== 类目维度汇总 ==========

-- 类目日汇总
CREATE TABLE dws_category_daily (
    date_id VARCHAR(10),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    platform VARCHAR(20),
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (date_id, category_l1, category_l2, platform)
);

-- 类目总汇总
CREATE TABLE dws_category_total (
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    platform VARCHAR(20),
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (category_l1, category_l2, platform)
);

-- ========== 用户维度汇总 ==========

-- 用户日汇总
CREATE TABLE dws_user_daily (
    date_id VARCHAR(10),
    user_id VARCHAR(50),
    order_count INT,
    sales_amount DECIMAL(12,2),
    PRIMARY KEY (date_id, user_id)
);

-- 用户总汇总
CREATE TABLE dws_user_total (
    user_id VARCHAR(50) PRIMARY KEY,
    gender VARCHAR(10),
    age INT,
    age_group VARCHAR(20),
    city VARCHAR(50),
    order_count INT,
    total_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    first_order_date DATE,
    last_order_date DATE,
    user_level VARCHAR(20)
);

-- ========== 推广维度汇总 ==========

-- 推广日汇总
CREATE TABLE dws_promotion_daily (
    date_id VARCHAR(10),
    channel VARCHAR(50),
    platform VARCHAR(20),
    cost DECIMAL(12,2),
    impressions INT,
    clicks INT,
    click_rate DECIMAL(5,2),
    avg_click_cost DECIMAL(10,2),
    PRIMARY KEY (date_id, channel, platform)
);

-- 推广商品汇总
CREATE TABLE dws_promotion_product (
    product_id VARCHAR(20),
    channel VARCHAR(50),
    cost DECIMAL(12,2),
    impressions INT,
    clicks INT,
    sales_amount DECIMAL(12,2),
    roi DECIMAL(10,2),
    PRIMARY KEY (product_id, channel)
);

-- ========== 流量维度汇总 ==========

-- 流量日汇总
CREATE TABLE dws_traffic_daily (
    date_id VARCHAR(10),
    store_id VARCHAR(20),
    platform VARCHAR(20),
    visitors INT,
    page_views INT,
    avg_stay_time DECIMAL(10,2),
    bounce_rate DECIMAL(5,2),
    conversion_rate DECIMAL(5,2),
    PRIMARY KEY (date_id, store_id)
);

-- ============================================
-- ADS层（应用数据层）
-- ============================================

-- 平台对比分析
CREATE TABLE ads_platform_report (
    platform VARCHAR(20) PRIMARY KEY,
    store_count INT,
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    avg_store_sales DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    avg_order_amount DECIMAL(10,2),
    sales_ratio DECIMAL(5,2)
);

-- TOP商品排行
CREATE TABLE ads_top_products (
    product_id VARCHAR(20),
    product_name VARCHAR(200),
    category_l1 VARCHAR(50),
    category_l2 VARCHAR(50),
    rank_type VARCHAR(20),
    rank INT,
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    PRIMARY KEY (product_id, rank_type)
);

-- 类目分析
CREATE TABLE ads_category_analysis (
    category_l1 VARCHAR(50) PRIMARY KEY,
    order_count INT,
    sales_quantity INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    sales_ratio DECIMAL(5,2),
    profit_rate DECIMAL(5,2)
);

-- 用户等级分析
CREATE TABLE ads_user_level_analysis (
    user_level VARCHAR(20) PRIMARY KEY,
    user_count INT,
    total_amount DECIMAL(12,2),
    order_count INT,
    avg_amount_per_user DECIMAL(10,2),
    user_ratio DECIMAL(5,2),
    sales_ratio DECIMAL(5,2)
);

-- 月度趋势分析
CREATE TABLE ads_monthly_trend (
    year_month VARCHAR(10),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    profit_rate DECIMAL(5,2),
    avg_order_amount DECIMAL(10,2),
    PRIMARY KEY (year_month, platform)
);

-- 店铺排行榜
CREATE TABLE ads_store_ranking (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    platform VARCHAR(20),
    order_count INT,
    user_count INT,
    sales_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    profit_rate DECIMAL(5,2),
    overall_rank INT,
    platform_rank INT
);
