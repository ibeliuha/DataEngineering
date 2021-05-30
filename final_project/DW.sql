--CREATE STAR SCHEMA
--primary keys can not be defined in append-only mode
drop table if exists orders;
drop table if exists out_of_stock;
drop table if exists dim_clients;
drop table if exists dim_clients;
drop table if exists dim_products;
drop table if exists dim_stores;

create table orders (
	order_date date not null,
	client_id int not null,
	product_id int not null,
	store_id int not null,
	quantity int
)
with (orientation=column, appendonly=true,compresslevel=4)
distributed by (store_id);

create table out_of_stock (
	"date" date not null,
	product_id int not null
)
with (orientation=column, appendonly=true,compresslevel=4)
distributed by (product_id);

create table dim_clients (
	"id" int not null,
	fullname varchar(63),
	"area" varchar(64),
	primary key("id")
);

create table dim_products (
	product_id int not null,
	product_name varchar(255),
	deparment varchar(127),
	aisle varchar(127),
	primary key(product_id)
);

create table dim_stores (
	store_id int not null,
	"type" varchar(64),
	"area" varchar(64),
	primary key(store_id)
);