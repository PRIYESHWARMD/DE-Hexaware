use burger_bash;
select * from burger_names;
select * from burger_runner;
select * from customer_orders;
select * from runner_orders;




-- 1. How many burgers were ordered?
select count(burger_id) burger from customer_orders;
-- 2. How many unique customer orders were made?
select count(distinct customer_id) Unique_customer from customer_orders;
-- 3. How many successful orders were delivered by each runner?
select runner_id,count(*) as orders from runner_orders
where cancellation is NULL
group by runner_id;
-- 4. How many of each type of burger was delivered?
select c.burger_id,burger_name,count(*) Burger_count
from customer_orders c join burger_names b on c.burger_id =b.burger_id join runner_orders r on c.order_id=r.order_id
where r.cancellation is NULL
group by c.burger_id,b.burger_name;

-- 5. How many Vegetarian and Meatlovers were ordered by each customer?
select customer_id,burger_name, count(c.burger_id) as No_of_burgers from customer_orders c join burger_names b on c.burger_id = b.burger_id
group by c.burger_id,customer_id,burger_name;
-- 6. What was the maximum number of burgers delivered in a single order?
select max(burger_count) Max_count from (select order_id,count(*) as burger_count from customer_orders
group by order_id)a
-- 7. For each customer, how many delivered burgers had at least 1 change and
-- how many had no changes?
select customer_id,sum(case when (exclusions is NULL and extras is null )then 1 else 0 end) as without_Changes,
sum(case when (exclusions is not NULL or extras is not null )then 1 else 0 end) as with_changes
from customer_orders c join runner_orders r on c.order_id=r.order_id
where cancellation is null
group by customer_id;
-- 8. What was the total volume of burgers ordered for each hour of the day?
select DATEPART(HOUR,order_time) as Order_hour, count(*) as Burger from customer_orders
group by DATEPART(HOUR,order_time) 
order by 1;
-- 9. How many runners signed up for each 1 week period?
select  datepart(week, registration_date) as reg_week,count(*) as runners_signed_up
from burger_runner
group by datepart(year, registration_date), datepart(week, registration_date)
order by  reg_week;

-- 10.What was the average distance travelled for each customer?
select c.customer_id,avg(try_cast(replace(replace(r.distance, 'km', ''), ' ', '') as float)) as avg_distance_km
from customer_orders c join runner_orders r on c.order_id = r.order_id
where r.cancellation is null
group by c.customer_id;
