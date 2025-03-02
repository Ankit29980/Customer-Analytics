-- cleaning data
-- total record = 541909

with new_table as(
select * from online_retail
where CustomerID!=0 and year(InvoiceDate)>2010)
,
quantity_unit_price as(
select * from new_table
where quantity>0 and unitprice>0
),
dup_check as(
select *, ROW_NUMBER() over(partition by lnvoiceno,stockcode,quantity order by invoicedate) as dup_flag
 from quantity_unit_price)
select lnvoiceno,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,country  into retail_main_data  from dup_check
where dup_flag=1;


-- Begin cohort Analyis
select * from retail_main_data 
-- unique identifier (customerid)
-- intital start date( first invoice date)
-- revenue data 


/*Q ...
The company Wants to understand customer Retention behavior over time. Are customers staying Loyal 
after the first purchase ,or is there a high churn rate? ? */

-- step 1: Creating a cohort table 
-- this query groups customers based on their first purchase month

with cohort as(
select CustomerID,min(InvoiceDate) as first_purchase,
DATEFROMPARTS(year(min(invoiceDate)),month(min(InvoiceDate)),1) as cohort_date
from retail_main_data
group by CustomerID), 

-- Step 2: assigning an index to each month
-- this help in calculating how long  customer stay active their first purchase

cohort_ind as(
select cohort_date,datediff(month,cohort_date,InvoiceDate) as cohort_index 
,count(distinct m.CustomerID) as active_user from cohort c
inner join retail_main_data m on c.CustomerID=m.CustomerID
group by cohort_date,datediff(month,cohort_date,InvoiceDate)),

-- step 3 : calulating retention rate 
-- counting customers who mase repeat purchases in each month 

retenation as(
select cohort_date,
       cohort_index,
	   active_user,
	   case when cohort_index=0 then active_user else null end  as initial_user
	   from cohort_ind),

-- calculating Rentention percentage

retention_rate as(

select cohort_date ,
        cohort_index,
	   active_user,
	  cast(round(coalesce( active_user*100.0 /
	   nullif (max(initial_user) over(partition by cohort_date ),0),0),2) as decimal(10,0)) as rate
	   from retenation)

-- pivoting Data-converting row values into column for each month 
select cohort_date,
sum(case when cohort_index=0 then rate else 0 end) as '0',
sum(case when cohort_index=1 then rate else 0 end) as '1',
sum(case when cohort_index=2 then rate else 0 end) as '2',
sum(case when cohort_index=3 then rate else 0 end) as '3',
sum(case when cohort_index=4 then rate else 0 end) as '4',
sum(case when cohort_index=5 then rate else 0 end) as '5',
sum(case when cohort_index=6 then rate else 0 end) as '6',
sum(case when cohort_index=7 then rate  else 0 end) as '7',
sum(case when cohort_index=8 then rate  else 0 end) as '8',
sum(case when cohort_index=9 then rate  else 0 end) as '9',
sum(case when cohort_index=10 then rate  else 0 end) as '10',
sum(case when cohort_index=11 then rate  else 0 end) as '11'
into   #cohort_retention_rate_analyis
from   retention_rate
group by cohort_date
order by cohort_date 

--insight 
--Retention rate decreases over time ,indicating a challenge in retaining customers
-- The biggest Drop within the first 3-4 months, suggesting weak onboarding or engagement 
-- Some cohort show better retention, which could be due to special marketing campaigns or discounts
select * from  #cohort_retention_rate_analyis



/* The company wanted to track how revenue is retained over time from different customer cohorts.*/

---- Step 1: Identify the first purchase date for each customer and create a cohort date  '

with cohort as (
select CustomerID,min(InvoiceDate) as first_purchase,
DATEFROMPARTS(year(min(InvoiceDate)),month(min(invoicedate)),1) as cohort_date
from retail_main_data 
group by CustomerID),

-- cohort_index calculates the number of months since the first purchase
cohort_rev as(
select cohort_date,datediff(month,cohort_date,InvoiceDate) as cohort_index,
round(sum(Quantity*unitprice),2) as rev   from cohort c
inner join online_retail_main m on c.CustomerID=m.CustomerID
group by cohort_date,datediff(month,cohort_date,InvoiceDate)),

-- Step 3: Assign revenue values for cohort's first purchase month  
retention_rev as(
select cohort_date,
       cohort_index,
	   rev,
	   case when cohort_index=0 then rev else null end  as initial_rev
	   from cohort_rev),

-- Step 4: Calculate revenue retention rate (percentage of revenue retained each month)  
retention_rev_rate as (

select cohort_date ,
        cohort_index,
	   rev,
	  cast(round(coalesce( rev*100.0 /
	   nullif (max(initial_rev) over(partition by cohort_date ),0),0),2) as decimal(10,0)) as rate
	   from retention_rev )


-- Step 5: Pivot the revenue retention rate for better visualization  
select cohort_date,
sum(case when cohort_index=0 then rate else 0 end) as '0',
sum(case when cohort_index=1 then rate else 0 end) as '1',
sum(case when cohort_index=2 then rate else 0 end) as '2',
sum(case when cohort_index=3 then rate else 0 end) as '3',
sum(case when cohort_index=4 then rate else 0 end) as '4',
sum(case when cohort_index=5 then rate else 0 end) as '5',
sum(case when cohort_index=6 then rate else 0 end) as '6',
sum(case when cohort_index=7 then rate else 0 end) as '7',
sum(case when cohort_index=8 then rate else 0 end) as '8',
sum(case when cohort_index=9 then rate else 0 end) as '9',
sum(case when cohort_index=10 then rate else 0 end) as '10',
sum(case when cohort_index=11 then rate else 0 end) as '11'
into #cohort_revenue_rate_analyis
from retention_rev_rate
group by cohort_date
order by cohort_date;

-- insights
-- Revenue retention Declines over time, indicating fewer rpeat purchases
-- some cohort shows higher revenue retention, suggesting high-value customers
-- The january cohort had a customer retention rate of 19.96% in the 11 month 
-- but the revenue retention rate for the same cohort was 50.00% showing 
-- that the remaing customer were spendin more
select * from #cohort_revenue_rate_analyis


-- pareto analysis on product
/* the company wanted to determine which products contribute most to revenue */ 

with product_wise_sales as(
select Description,round(sum(quantity*unitprice),2) as product_sale from retail_main_data
group by Description)
select * from (
select *,
sum(product_sale) over(order by product_sale desc) as cumulative_sale,
0.8*sum(product_sale) over() as overall_sale
from product_wise_sales ) a
where cumulative_sale<=overall_sale -- 744

select count(distinct description) from retail_main_data -- 3571
--- total product 3571 and 744 product are generating 80% sale
select (1.0*744)/(3571)*100 as prcnt; -- 20 percent 
-- insight 
--Out of 3571 total products only 744 products contribute to 80% of total sales.- This
-- means that a small percentage of products are responsible for most of the sale

-- pareto analysis on country
/* The company wants to Analyze which countries contribute the most to the total sales */

with country_wise_sales as(
select country,round(sum(quantity*unitprice),2) as country_sale from retail_main_data
group by country)
select * from (
select *,
sum(country_sale) over(order by country_sale desc) as cumulative_sale,
0.8*sum(country_sale) over() as overall_sale
from country_wise_sales ) a

-- insight 
/* More than 80% of total sale come from the united kingdom alone - other countries
 contribute very little to the total sales- this means the business heavily relies
 one country*/


 -- pareto analysis on country
 --The company wants to Analyze which countries contribute the most to the total sales

with customer_wise_sales as(
select CustomerID,round(sum(quantity*unitprice),2) as customer_sale from retail_main_data
group by CustomerID)
select * from (
select *,
sum(customer_sale) over(order by customer_sale desc) as cumulative_sale,
0.8*sum(customer_sale) over() as overall_sale
from customer_wise_sales) a
where cumulative_sale<=overall_sale
--insight
/* Only 30 % of customer contribute to 80% of total sales
The company highly dependent on a small group og high*/ 


--- rfm analysis
/* The company wanted to find out if a small percentage of customer drive
most of then sales */

-- select max(invoicedate) from retail_main_data -- 2011-12-10

with rfm_data as(
select CustomerID,round(sum(quantity*unitprice),2) as monetary_value,
count( distinct lnvoiceNo) as frequency,
max(InvoiceDate) as last_order,
DATEDIFF(DD,max(invoicedate),(select max(invoicedate) from online_retail_main)) Recency
from retail_main_data
group by CustomerID),
rfm_value as(
select *,
ntile(5) over(order by Recency desc) as r_value,
ntile(5) over(order by frequency asc) as f_value,
ntile(5) over(order by monetary_value asc ) as m_value
from rfm_data),
rfm_cell as (
select *,
cast(r_value as varchar)+cast(f_value as varchar)+ cast(m_value as varchar) as rfm_string
from rfm_value)
select *  into #rfm_data from rfm_cell r
inner join Segment_Scores s on r.rfm_string=s.Scores

select * from #rfm_data

-- total customer by each segment 
 select segment,count(*) as total_customer from #rfm_data
group by Segment

-- country wise segment customer 
select country,segment, count(distinct r.CustomerID) as total_customer  from #rfm_data r
inner join 
online_retail_main m on m.customerid=r.customerid
group by country,Segment
order by total_customer desc;


select country,segment, count(distinct m.CustomerID) as total_customer  from #rfm_data r
inner  join 
online_retail_main m on m.customerid=r.customerid
where Segment='champions'
group by country,Segment;
-- united kingdom has highest champions customer which are 392


select country, count(distinct m.CustomerID) as total_customer  from #rfm_data r
inner  join 
online_retail_main m on m.customerid=r.customerid
where Segment in ( 'at risk','lost customers')
group by country
order by total_customer desc;
-- most at risk and lost customer are also from united kingdom which are 552 

select description ,count(*) as top_product from #rfm_data r
inner  join 
online_retail_main m on m.customerid=r.customerid
where Segment='champions' and country='united kingdom'
group by Description
order by top_product desc
-- White hanging heart t-light holder and jumbo bag red retrospot are 2 products most sold in united kingdom for champions customer ;


/* The company wanted to analyze trends in new and repeat customer
over time */

with first_purchase as(
select customerid,min(invoicedate) as first_visit  from online_retail_main
group by CustomerID),
calc_cust as(
select m.*,first_visit ,
case when invoicedate=first_visit then 1 else 0 end as first_visit_flag,
case when InvoiceDate != first_visit then 1 else 0 end as repeat_visit_flag
from first_purchase f
inner join online_retail_main m on f.CustomerID=m.CustomerID )
--select * from calc_cust
select DATENAME(MONTH,InvoiceDate),sum(first_visit_flag) as new_customer,sum(repeat_visit_flag) as repeat_customer from (
select customerid,first_visit,InvoiceDate,first_visit_flag,repeat_visit_flag from calc_cust)a
WHERE year(InvoiceDate)>2010
group by DATENAME(MONTH,InvoiceDate)
order by DATENAME(MONTH,InvoiceDate)

--insight
/* The highest number of new customer joined in january after january, new customer
 acquisition declined steadly
 :- Repeat customer peaked at 10.3k in june ,showing strong customer retention
 also in august 10.1k repeat cutomers were recorded, maintaining a high ret
ention rate */ 