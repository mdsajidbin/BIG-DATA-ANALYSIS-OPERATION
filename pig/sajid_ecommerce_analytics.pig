-- Base Data Load (Apnar path ebong schema onujayi)
sales = LOAD '/ecommerce/inputs/sales_data.csv' USING PigStorage(',') 
AS (order_id:chararray, order_date:chararray, customer_id:chararray, customer_name:chararray, city:chararray, product_id:chararray, product_name:chararray, category:chararray, quantity:int, unit_price:double, revenue:double, payment_method:chararray);

clean_sales = FILTER sales BY order_id != 'order_id' AND quantity IS NOT NULL;

-- =====================================================================
-- Operation 1: category_avg_price (Category wise average unit price)
-- =====================================================================
cat_group1 = GROUP clean_sales BY category;
category_avg_price = FOREACH cat_group1 GENERATE group AS category, AVG(clean_sales.unit_price) AS avg_price;
STORE category_avg_price INTO '/ecommerce/pig_output/category_avg_price' USING PigStorage(',');

-- =====================================================================
-- Operation 2: category_quantity (Category wise total quantity sold)
-- =====================================================================
cat_group2 = GROUP clean_sales BY category;
category_quantity = FOREACH cat_group2 GENERATE group AS category, SUM(clean_sales.quantity) AS total_quantity;
STORE category_quantity INTO '/ecommerce/pig_output/category_quantity' USING PigStorage(',');

-- =====================================================================
-- Operation 3: category_revenue (Category wise total revenue)
-- =====================================================================
cat_group3 = GROUP clean_sales BY category;
category_revenue = FOREACH cat_group3 GENERATE group AS category, SUM(clean_sales.revenue) AS total_revenue;
STORE category_revenue INTO '/ecommerce/pig_output/category_revenue' USING PigStorage(',');

-- =====================================================================
-- Operation 4: city_avg_revenue (City wise average order revenue)
-- =====================================================================
city_group1 = GROUP clean_sales BY city;
city_avg_revenue = FOREACH city_group1 GENERATE group AS city, AVG(clean_sales.revenue) AS avg_revenue;
STORE city_avg_revenue INTO '/ecommerce/pig_output/city_avg_revenue' USING PigStorage(',');

-- =====================================================================
-- Operation 5: payment_revenue (Payment method wise total revenue)
-- =====================================================================
pay_group = GROUP clean_sales BY payment_method;
payment_revenue = FOREACH pay_group GENERATE group AS payment_method, SUM(clean_sales.revenue) AS total_revenue;
STORE payment_revenue INTO '/ecommerce/pig_output/payment_revenue' USING PigStorage(',');