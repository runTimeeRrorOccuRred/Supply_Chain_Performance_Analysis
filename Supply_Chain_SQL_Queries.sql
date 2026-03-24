select * from dim_customers
select * from dim_date
select * from dim_products
select * from dim_targets_orders
select * from fact_order_lines


--drop table fact_order_lines

--checking data_types

select column_name, data_type from INFORMATION_SCHEMA.columns
where table_name='fact_order_lines'

alter table fact_order_lines
alter column in_full tinyint


--data Cleaning--

-- There is no null value present in the dataset

-- duplicate checking

select order_id, customer_id, product_id, count(*) from fact_order_lines
group by order_id, customer_id, product_id
having count(*)>1

-- there is no duplicate value present in the dataset.

-- fixing dim_date table.

go
create view cleaned_dim_date_table as
(
select date, mmm_yy, cast(substring(week_no,2,len(week_no)) as smallint) as 'week_number' from dim_date
)
select * from cleaned_dim_date_table

-- Querying

-- order_level (OT%, IF% AND OTIF%)

with order_level as
(
    select 
        order_id,
        min(on_time) as 'OT',
        min(in_full) as 'IF',
        min(on_time_in_full) as 'OTIF'
    from fact_order_lines
    group by order_id
)
select
    count(*) as Total_Orders,
    cast(sum([OT]) * 100.0 / count(*) as decimal(10,2)) as OT_Percentage,
    cast(sum([IF]) * 100.0 / count(*) as decimal(10,2)) as IF_Percentage,
    cast(sum([OTIF]) * 100.0 / count(*) as decimal(10,2)) as OTIF_Percentage
from order_level;


-- monthly OT% IF% and OTIF%

with monthly_trend as
(
select order_id,
min(order_placement_date) as 'order_date',
min(on_time) as 'OT',
min(in_full) as 'IF',
min(on_time_in_full) as 'OTIF' from fact_order_lines
group by order_id
)

select format(order_date,'yyyy-MM') as 'Order_Month',
cast((sum([OT])*1.0/count(*))*100 as decimal(10,2)) as 'OT%',
cast((sum([IF])*1.0/count(*))*100 as decimal(10,2)) as 'IF%',
cast((sum([OTIF])*1.0/count(*))*100 as decimal(10,2)) as 'OTIF%'
from monthly_trend
group by format(order_date,'yyyy-MM')
order by 'Order_Month'

-- weekly  OT% IF% and OTIF%

with weekly_trend as
(
select fol.order_id,
min(fol.order_placement_date) as 'order_date',
min(cdt.week_number) as 'Order_week',
min(fol.on_time) as 'OT',
min(fol.in_full) as 'IF',
min(fol.on_time_in_full) as 'OTIF' from fact_order_lines as fol
join cleaned_dim_date_table as cdt
on fol.order_placement_date=cdt.date
group by fol.order_id
)

select order_week,
cast((sum([OT])*1.0/count(*))*100 as decimal(10,2)) as 'OT%',
cast((sum([IF])*1.0/count(*))*100 as decimal(10,2)) as 'IF%',
cast((sum([OTIF])*1.0/count(*))*100 as decimal(10,2)) as 'OTIF%'
from weekly_trend
group by order_week
order by order_week


-- calculating LIFR% and VOFR%

select count(*) as 'Total Order Lines',
cast(sum(in_full)*1.0*100/ count(*) as decimal (10,2)) as 'LIFR_Percentage',
cast(sum(delivery_qty)*1.0*100/sum(order_qty) as decimal(10,2)) as 'VOFR_Percentage'
from fact_order_lines


--customer wise OT% IF% and OTIF%

with customer_level_trend as 
(
select
order_id,
customer_name,
min(on_time) as 'OT',
min(in_full) as 'IF',
min(on_time_in_full) as 'OTIF'
from fact_order_lines fol
left join dim_customers dc
on fol.customer_id=dc.customer_id
group by order_id,customer_name
)

select customer_name,
cast((sum([OT])*1.0/count(*))*100 as decimal(10,2)) as 'OT%',
cast((sum([IF])*1.0/count(*))*100 as decimal(10,2))as 'IF%',
cast((sum([OTIF])*1.0/count(*))*100 as decimal(10,2)) as 'OTIF%'
from customer_level_trend
group by customer_name

--city wise OT% IF% and OTIF%

with customer_level_trend as 
(
select
order_id,
city,
min(on_time) as 'OT',
min(in_full) as 'IF',
min(on_time_in_full) as 'OTIF'
from fact_order_lines fol
left join dim_customers dc
on fol.customer_id=dc.customer_id
group by order_id,city
)

select city,
cast((sum([OT])*1.0/count(*))*100 as decimal(10,2)) as 'OT%',
cast((sum([IF])*1.0/count(*))*100 as decimal(10,2))as 'IF%',
cast((sum([OTIF])*1.0/count(*))*100 as decimal(10,2)) as 'OTIF%'
from customer_level_trend
group by city

--product wise OT% IF% and OTIF%

select product_name,
cast(sum(On_Time)*1.0*100/count(*) as decimal(10,2)) as 'OT%',
cast(sum(in_full)*1.0*100/count(*) as decimal(10,2)) as 'IF%',
cast(sum(On_Time_in_full)*1.0*100/count(*) as decimal(10,2))  as 'OTIF%'
from fact_order_lines fol
left join dim_products dp
on fol.product_id=dp.product_id
group by product_name

select * from fact_order_lines
where DATEDIFF(day,agreed_delivery_date,actual_delivery_date)=-1

--orders getting delayed proportion

with delay_proportion as
(
select DATEDIFF(day,agreed_delivery_date,actual_delivery_date) as 'delay',
count(distinct order_id) as 'Number of Orders'
from fact_order_lines
group by DATEDIFF(day,agreed_delivery_date,actual_delivery_date)
)

select case when delay=-1 or delay=0 then 'Right Time Orders' else 'Delay Orders' end as 'Order Type',
sum([Number of orders]) as 'Total Orders',
cast(sum([Number of orders])*1.0*100/(select count(distinct order_id) from fact_order_lines) as decimal(10,2))
as 'Order Proportion'
from delay_proportion
group by
case when delay=-1 or delay=0 then 'Right Time Orders' else 'Delay Orders' end

--creating aggregate table for PowerBI report.

select order_id,customer_id,order_placement_date,
min(on_time) as 'OT',
min(in_full) as 'IF',
min(on_time_in_full) as 'OTIF'
into fact_aggregate
from fact_order_lines
group by order_id,customer_id,order_placement_date

/*
select category,count(distinct order_id)*1.0*100/(SELECT COUNT(*)
FROM (
    SELECT DISTINCT order_id, category
    FROM fact_order_lines foc
    JOIN dim_products dp
    ON foc.product_id = dp.product_id
	) as t)
as 'Total Orders' from fact_order_lines foc
join dim_products dp
on foc.product_id=dp.product_id
group by category



select order_id,category,count(*) from fact_order_lines foc
join dim_products dp
on foc.product_id=dp.product_id
group by order_id,category


select count(*) from 
(select category,count(distinct order_id)
as 'Total Orders' from fact_order_lines foc
join dim_products dp
on foc.product_id=dp.product_id
group by category)as t
*/

