
-- create schema dim;
-- create schema fact;

--drop table dim_store
create table dim.store(
    store_id integer,
    address varchar(100),
    district varchar(100),
    city varchar(100)
)

--drop table dim.date
create table dim.date(
[date]  date,
date_key int,
period varchar(6),
year varchar(4),
month varchar(2),
day  varchar(2),
quarter varchar(2)
)

create table dim.film(
film_id integer,
title varchar(50),
description varchar(300),
release_year integer,
rental_rate decimal(4,2),
length smallint ,
replacement_cost decimal(5,2),
rating varchar(20)
)

create table dim.customer(
customer_id integer,
first_name varchar(50),
last_name varchar(50),
email varchar(100)
)

create table fact.rental(
    date_id varchar(8),
    hour varchar(7),
    customer_id smallint,
    store_id integer,
    film_id smallint ,
    amount decimal(15,2)
)


select * from dim.store
--delete from dim.store


select * from dim.date
--delete from dim.date


select * from dim.film
--delete from dim.film


select * from dim.customer
--delete from dim.customer


select * from fact.rental
--delete * from fact.rental
