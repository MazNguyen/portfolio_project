
-- Show sale data table & order by date

select * 
from sale_16
order by 1, 2

-- union with Sale 2017

select * 
from sale_16
union (
		select *
		from sale_17
		)
order by 1 desc

-- Looking at number of order by productkey

select sale.ProductKey, sale.TerritoryKey, SUM(sale.OrderQuantity) as order_number
from (
	select * from sale_16
	union
	select * from sale_17) as sale
group by sale.ProductKey, sale.TerritoryKey


--Join return_table to get return quantity

select sa.OrderDate, sa.StockDate, sa.OrderQuantity, sa.ProductKey, sa.TerritoryKey, re.ReturnQuantity, re.ReturnDate
from sale_16 sa
left join return_table re
	on sa.ProductKey = re.ProductKey
	and sa.TerritoryKey = re.TerritoryKey


-- Calculate return rate by productkey
With product_summary (ProductKey, TerritoryKey, order_number, return_number, return_qty) 
as
(
select 
	order_groupby.*,
	r.return_number,
	case  when r.return_number > 0 then r.return_number else 0 end as return_qty -- Replace null value by 0
from (	
	select sale.ProductKey, sale.TerritoryKey, SUM(sale.OrderQuantity) as order_number -- Get number of order by productkey
	from (
		select * from sale_16
		union
		select * from sale_17) as sale
	group by sale.ProductKey, sale.TerritoryKey
	) as order_groupby
left join (
			select re.ProductKey, re.TerritoryKey, SUM(re.ReturnQuantity) as return_number -- Get number of return by productkey
			from return_table re
			group by re.ProductKey, re.TerritoryKey ) as r
		on order_groupby.ProductKey = r.ProductKey
		and order_groupby.TerritoryKey = r.TerritoryKey
)
select ps.ProductKey, ps.order_number, ps.return_qty,  
		(return_qty/order_number)*100 as return_rate, -- calculate return rate
		p.ProductSKU, p.ProductName, p.ModelName, p.ProductCost, p.ProductPrice
	from product_summary ps
	left join product p
		on ps.ProductKey = p.ProductKey
	order by 4 desc


-- CREATE TEMP TABLE

Drop table if exists #Summary_order_return_of_product
Create Table #Summary_order_return_of_product
(
product_key numeric,
terri_key numeric,
order_qty numeric,
return_number numeric,
return_qty numeric
)
insert into #Summary_order_return_of_product
select 
	order_groupby.*,
	r.return_number,
	case  when r.return_number > 0 then r.return_number else 0 end as return_qty -- Replace null value by 0
from (	
	select sale.ProductKey, sale.TerritoryKey, SUM(sale.OrderQuantity) as order_number -- Get number of order by productkey
	from (
		select * from sale_16
		union
		select * from sale_17) as sale
	group by sale.ProductKey, sale.TerritoryKey
	) as order_groupby
left join (
			select re.ProductKey, re.TerritoryKey, SUM(re.ReturnQuantity) as return_number -- Get number of return by productkey
			from return_table re
			group by re.ProductKey, re.TerritoryKey ) as r
		on order_groupby.ProductKey = r.ProductKey
		and order_groupby.TerritoryKey = r.TerritoryKey
select * from #Summary_order_return_of_product


-- CREATE VIEW

create view product_summary as
With product_summary (ProductKey, TerritoryKey, order_number, return_number, return_qty) 
as
(
select 
	order_groupby.*,
	r.return_number,
	case  when r.return_number > 0 then r.return_number else 0 end as return_qty -- Replace null value by 0
from (	
	select sale.ProductKey, sale.TerritoryKey, SUM(sale.OrderQuantity) as order_number -- Get number of order by productkey
	from (
		select * from sale_16
		union
		select * from sale_17) as sale
	group by sale.ProductKey, sale.TerritoryKey
	) as order_groupby
left join (
			select re.ProductKey, re.TerritoryKey, SUM(re.ReturnQuantity) as return_number -- Get number of return by productkey
			from return_table re
			group by re.ProductKey, re.TerritoryKey ) as r
		on order_groupby.ProductKey = r.ProductKey
		and order_groupby.TerritoryKey = r.TerritoryKey
)
select ps.ProductKey, ps.order_number, ps.return_qty,  
		(return_qty/order_number)*100 as return_rate, -- calculate return rate
		p.ProductSKU, p.ProductName, p.ModelName, p.ProductCost, p.ProductPrice
	from product_summary ps
	left join product p
		on ps.ProductKey = p.ProductKey
