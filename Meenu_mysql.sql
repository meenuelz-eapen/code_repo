/*Query 1*/
select distinct city as CUSTOMER_CITY from classicmodels.customers;



/*Query 2*/
select * from classicmodels.employees where trim(firstname) not in ("Mary","Gerard");

/*Query 3*/
select concat(firstname,' ',lastname) as fullname from classicmodels.employees where lastname like "%n";

/*Query 4*/
select emp.* from classicmodels.employees emp
inner join 
classicmodels.offices ofc
on emp.officeCode=ofc.officeCode
where ofc.territory="EMEA";

/*Query 5*/
select territory,count(distinct employeenumber) as employeecount from classicmodels.employees emp
inner join 
classicmodels.offices ofc
on emp.officeCode=ofc.officeCode
group by territory
order by employeecount desc;

/*Query 6*/
select  prdlin.textDescription as productLine, prd.productName
from classicmodels.products as prd
inner join 
classicmodels.productlines as prdlin
on prd.productline=prdlin.productline
order by prdlin.textDescription ASC, prd.productName DESC;

/*Query 7*/
select prd.productCode,sum(quantityOrdered) as totalQuantitySold
from
classicmodels.products prd
inner join 
classicmodels.orderdetails as orddet
on prd.productCode=orddet.productCode
inner Join
classicmodels.orders ordr
on ordr.orderNumber=orddet.orderNumber
where orderDate between '2005-04-01' and '2005-04-30' 
group by prd.productCode
order by totalQuantitySold desc limit 5;

/*Query 8*/
select prd.productName, 
sum(case when (quantityOrdered is null) or (priceEach is null) then 0 else quantityOrdered*priceEach end) as totalRevenue
from 
classicmodels.products prd
left join 
classicmodels.orderdetails as orddet
on prd.productCode=orddet.productCode
group by productName
order by totalRevenue desc;
