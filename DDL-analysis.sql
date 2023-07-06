DROP TABLE IF EXISTS dim_Category;
CREATE TABLE dim_Category(Category_ID  SERIAL,
					 Category VARCHAR(50),
					 PRIMARY KEY (Category_ID)
					 );
					 

DROP TABLE IF EXISTS dim_Sub_Category;					 
CREATE TABLE dim_Sub_Category(Subcategory_ID SERIAL,
						 Category_ID INT,
						 Subcategory VARCHAR(50),
						 PRIMARY KEY (Subcategory_ID),
						 FOREIGN KEY (Category_ID) REFERENCES dim_Category(Category_ID)
						 );
DROP TABLE IF EXISTS dim_Date_time;						 
CREATE TABLE dim_Date_time(Date_ID SERIAL,
					  Full_Date DATE,
					  PRIMARY KEY (Date_ID)
					  );
DROP TABLE IF EXISTS dim_Month_time CASCADE;

CREATE TABLE dim_Month_time(Month_ID SERIAL,
					   Month_year VARCHAR(50),
					   PRIMARY KEY (Month_ID)
					   );
					   

					   
DROP TABLE IF EXISTS fact_Target;					   
CREATE TABLE fact_Target(Target_ID SERIAL,
				   Month_ID INT,
				   Category_ID INT,
				   Target INT,
				   PRIMARY KEY (Target_ID),
				   FOREIGN KEY (Month_ID) REFERENCES dim_Month_time(Month_ID),
				   FOREIGN KEY (Category_ID) REFERENCES dim_Category(Category_ID)
				   );
DROP TABLE 	IF EXISTS dim_Customer;			   
CREATE TABLE dim_Customer(Customer_ID SERIAL,
					 Customer_Name VARCHAR(100),
					 State VARCHAR(50),
					 City VARCHAR(100),
					 PRIMARY KEY (Customer_ID)
					 );

DROP TABLE IF EXISTS dim_Ordered_In;
CREATE TABLE dim_Ordered_In(Order_ID VARCHAR(20),
					   Customer_ID INT,
					   Date_ID INT,
					   PRIMARY KEY (Order_ID),
					   FOREIGN KEY (Customer_ID) REFERENCES Customer(Customer_ID),
					   FOREIGN KEY (Date_ID) REFERENCES Date_Time(Date_ID)
					   );
					   
DROP TABLE IF EXISTS fact_Order_Table;					   
CREATE TABLE fact_Order_Table(Orderline_ID SERIAL,
						Order_ID VARCHAR (20),
						Category_ID INT,
						Subcategory_ID INT,
						Profit INT,
						Amount INT,
						Quantity INT,
						PRIMARY KEY (Orderline_ID),
						FOREIGN KEY (Order_ID) REFERENCES dim_Ordered_In(Order_ID),
						FOREIGN KEY (Category_ID) REFERENCES dim_Category(Category_ID),
						FOREIGN KEY (Subcategory_ID) REFERENCES dim_Sub_category(Subcategory_ID)
						);
						
				
CREATE TABLE list_of_orders(Order_ID VARCHAR(50),
						   Order_Date VARCHAR(50),
						   Customer_Name VARCHAR(100),
						   State VARCHAR(50),
						   City VARCHAR(100));
DROP TABLE 	list_of_orders;	

COPY list_of_orders FROM '/Users/shriyareddypulagam/Downloads/List_of_Orders.csv' DELIMITER ',' CSV HEADER;
		

select to_date(order_date, 'dd-mm-yyyy') from list_of_orders;

select * from list_of_orders;


create table list_of_orderss as select Order_ID, to_date(order_date, 'dd-mm-yyyy') order_date, Customer_Name,
State, City from list_of_orders;


select * from list_of_orderss;

create table list_of_orderses as select Order_ID, order_date as full_order_date, to_char(order_date, 'Mon-YY') order_date, Customer_Name,
State, City from list_of_orderss;

select * from list_of_orderses;

CREATE TABLE Order_Table(Order_ID VARCHAR(20),
						Amount FLOAT(3),
						Profit FLOAT(3),
						Quantity INT,
						Category VARCHAR(50),
						Subcategory VARCHAR(50));
						
COPY Order_Table FROM '/Users/shriyareddypulagam/Downloads/Order_Details.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE Sales_target(Month_year VARCHAR(50),
						 Category VARCHAR(50),
						 Target float(2));
						 
COPY Sales_target FROM '/Users/shriyareddypulagam/Downloads/Sales_target.csv' DELIMITER ',' CSV HEADER;
	
insert into dim_Category(Category) (select distinct Category from Order_Table);

select * from dim_Category;

create or replace view order_details_view as
(select ot.Category, ot.Subcategory, (select dc.Category_ID from dim_Category dc where dc.Category = ot.Category)
from Order_Table ot);

select distinct  category, subcategory, category_id from order_details_view;

INSERT INTO dim_Sub_Category(category_ID, subcategory) (select distinct category_ID, subcategory from order_details_view )
select * from dim_Sub_Category;

insert into dim_Date_time(Full_Date) (select distinct Order_Date from list_of_orderss order by Order_Date);

select * from dim_Date_time;

delete from dim_Date_time where full_date is null;


CREATE OR REPLACE VIEW month_date as 
(SELECT ddt.Date_Id, 
 to_char((select ddt1.full_date from dim_Date_time ddt1 where ddt.Date_ID = ddt1.Date_ID), 'Mon-YY') month_year, ddt.full_date
from dim_Date_time ddt );

select * from month_date;

insert into dim_Month_time(Month_Year)
(select DISTINCT month_year from  month_date order by month_year);

select * from dim_Month_time;

insert into fact_target(target, MONTH_ID, Category_ID)
select st.target,
(select dmt.Month_ID FROM dim_Month_time dmt where dmt.Month_Year = st.Month_Year),
(select dc.Category_ID FROM dim_Category dc where dc.category = st.category)
from sales_target st;

select * from fact_target;

INSERT INTO dim_Customer(customer_name,state,city)
select distinct customer_name, state, city from list_of_orders; 

delete from dim_Customer;
select * from dim_Customer;

delete from dim_customer where customer_name is null;

insert into dim_ordered_in (order_ID, Customer_ID, Date_ID)
select lo.order_ID, (select dc.customer_ID from dim_customer dc where dc.customer_name = lo.customer_name
					and dc.state = lo.state and dc.city = lo.city),
(select ddt.Date_ID from dim_date_time ddt where ddt.full_date = lo.order_date)
from list_of_orderss lo;

select * from list_of_orderss;

delete from list_of_orderss where order_id is null;

select * from dim_ordered_in;

insert into fact_Order_Table(profit, amount, quantity,Order_ID, Category_ID, Subcategory_ID)
select ot.profit, ot.amount, ot.quantity,
(select doi.order_ID from dim_ordered_in doi where doi.Order_ID = ot.Order_ID),
(select dc.category_ID FROM dim_category dc where dc.category = ot.category),
(select dsc.subcategory_ID FROM dim_sub_category dsc where dsc.subcategory = ot.subcategory)
from order_table ot;


select * from fact_Order_Table;


-- order of states according to its business business
select dc.state, sum(fot.amount)as state_rank
from dim_customer dc join dim_ordered_in doi on dc.customer_ID = doi.customer_ID
join fact_order_table fot on fot.order_ID = doi.order_ID
group by dc.state
order by state_rank desc;

-- month which met the target
create or replace view dtdmt
as 
(select month_year, sum(target) targetsum
from fact_target ft join dim_month_time dmt on ft.month_ID = dmt.month_ID
group by month_year
order by targetsum desc); 


create or replace view monthamount
as
(select  to_char(ddt.full_date, 'Mon-YY') groupedmonth, sum(amount) amountsum
from dim_ordered_in doi join dim_date_time ddt on doi.date_ID = ddt.date_ID join
fact_order_table fot on fot.order_id = doi.order_id
group by to_char(ddt.full_date, 'Mon-YY')
order by amountsum desc);



select groupedmonth
from monthamount ma join dtdmt dt
on ma.groupedmonth = dt.month_year
where ma.amountsum > dt.targetsum;

-- subcategory was mostly ordered among each category. 
CREATE OR REPLACE VIEW category as
(select dc.category, dsc.subcategory, count(fot.order_id) as countorders 
from fact_order_table fot join dim_category dc on fot.category_ID = dc.category_ID 
join dim_sub_category dsc on fot.subcategory_ID = dsc.subcategory_ID
group by dc.category, dsc.subcategory);

select c.category, c.subcategory
from category c
where countorders IN
(select MAX(countorders) OVER(PARTITION BY c1.category) 
 from category c1 where c1.category = c.category );

-- category with most profit.

select dc.category, dsc.subcategory, sum(profit)/count(quantity) as profitperquantity
from fact_order_table fot join dim_category dc on fot.category_ID = dc.category_ID
join dim_sub_category dsc on fot.subcategory_ID = dsc.subcategory_ID
group by dc.category,dsc.subcategory;







