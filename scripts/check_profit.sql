-- 验证净利润计算
SELECT 
    '订单明细汇总' AS 类型,
    COUNT(*) AS 订单数,
    SUM(实收金额) AS 总实收,
    SUM(商品成本金额) AS 总商品成本,
    SUM(分摊运费成本) AS 总运费,
    SUM(平台费) AS 总平台费,
    SUM(售后费) AS 总售后费,
    SUM(管理费) AS 总管理费,
    SUM(推广费) AS 总推广费,
    SUM(毛利) AS 总毛利,
    SUM(净利润) AS 总净利润,
    ROUND(SUM(净利润) / SUM(实收金额) * 100, 2) AS 净利率
FROM dwd_order_detail_wide
WHERE 订单状态 = '已完成';

-- 手工验证计算
SELECT 
    '手工验证' AS 类型,
    SUM(实收金额) AS 总实收,
    SUM(实收金额 * 0.05) AS 应付平台费,
    SUM(实收金额 * 0.02) AS 应付售后费,
    SUM(实收金额 * 0.10) AS 应付管理费,
    SUM(实收金额) - SUM(实收金额 * 0.17) - SUM(商品成本金额) - SUM(分摊运费成本) - SUM(推广费) AS 计算净利润
FROM dwd_order_detail_wide
WHERE 订单状态 = '已完成';
