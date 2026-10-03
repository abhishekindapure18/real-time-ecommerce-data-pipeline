-- 1. Total revenue by category
SELECT
    category,
    ROUND(total_revenue::numeric, 2) AS revenue,
    purchase_count
FROM revenue_by_category
ORDER BY revenue DESC;


-- 2. Top 5 products by units sold
SELECT
    product_id,
    product_name,
    units_sold,
    ROUND(revenue::numeric, 2) AS revenue
FROM top_products
ORDER BY units_sold DESC
LIMIT 5;


-- 3. Top 5 products by revenue
SELECT
    product_id,
    product_name,
    units_sold,
    ROUND(revenue::numeric, 2) AS revenue
FROM top_products
ORDER BY revenue DESC
LIMIT 5;


-- 4. Total units sold
SELECT
    SUM(units_sold) AS total_units_sold
FROM top_products;


-- 5. Total revenue
SELECT
    ROUND(SUM(revenue)::numeric, 2) AS total_revenue
FROM top_products;


-- 6. Number of products sold
SELECT
    COUNT(*) AS number_of_products
FROM top_products;