use texture_tales;
select * from product_details;
select * from sales;
select * from product_hierarchy;
select * from product_prices;

-- What was the total quantity sold for all products?
select product_name,sum(qty) as total from sales join product_details on prod_id=product_id
group by product_name
order by 2 desc;

-- What is the total generated revenue for all products before discounts?
select sum(qty*price) as total_revenue from sales;
-- What was the total discount amount for all products?
select sum(qty*price*discount)/100 as total_discount_amount from sales;
-- How many unique transactions were there?
select count(distinct txn_id) as Unique_transactions from sales;

-- What are the average unique products purchased in each transaction?
select avg(product_count) from (select txn_id, count(distinct prod_id)as product_count from sales
group by txn_id)a
-- What is the average discount value per transaction?
select avg(total_discount_amount) from (select txn_id,sum(qty*price*discount)/100 as total_discount_amount from sales
group by txn_id)a
-- What is the average revenue for member transactions and non- member transactions?
select case when member = 'true' then 'member' else 'non-member' end as member_status, avg(qty * price * (1 - discount / 100.0)) as avg_revenue
from sales
group by case when member = 'true' then 'member' else 'non-member' end;


-- What are the top 3 products by total revenue before discount?
select top 3 prod_id,sum(qty * price) as total_revenue_before_discount from sales
group by prod_id
order by total_revenue_before_discount desc;

-- What are the total quantity, revenue and discount for each segment?
select pd.segment_name, sum(s.qty) as total_quantity, sum(s.qty * s.price * (1 - s.discount / 100.0)) as total_revenue,sum(s.qty * s.price * s.discount / 100.0) as total_discount
from sales s join product_details pd on s.prod_id = pd.product_id
group by pd.segment_name;

-- What is the top selling product for each segment?
select b.segment_id,b.segment_name,b.product_id,b.product_name,sum(a.qty) as Product_Qty
from sales as a join product_details as b on a.prod_id = b.product_id
group by b.segment_id,b.segment_name,b.product_id,b.product_name
order by Product_Qty desc;


-- What are the total quantity, revenue and discount for each category?
select pd.category_name, sum(s.qty) as total_quantity, sum(s.qty * s.price * (1 - s.discount / 100.0)) as total_revenue,sum(s.qty * s.price * s.discount / 100.0) as total_discount
from sales s join product_details pd on s.prod_id = pd.product_id
group by pd.category_name;

-- What is the top selling product for each category?
select category_id,category_name,product_id,product_name,sum(qty) from product_details join sales on product_id=prod_id
group by category_id,category_name,product_id,product_name
order by 5 desc; 


