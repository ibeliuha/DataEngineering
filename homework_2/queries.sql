--вывести количество фильмов в каждой категории, отсортировать по убыванию.
select 	c."name" as category,
		count(*) as film_cnt
from film_category as f_c
inner join category as c
on f_c.category_id = c.category_id
group by c."name"
order by count(*) desc;

--вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select 	a.first_name,
		a.last_name,
		count(*) as total_rental
from actor as a
inner join film_actor as fa
on a.actor_id = fa.actor_id
inner join inventory as i
on i.film_id = fa.film_id
inner join rental as r
on r.inventory_id = i.inventory_id
group by a.actor_id, a.first_name, a.last_name 
order by count(*) desc
limit 10;

--вывести категорию фильмов, на которую потратили больше всего денег.
select 	c."name", 
		sum(p.amount) as total_amount_spent
from payment as p
inner join rental as r
on p.rental_id = r.rental_id
inner join inventory as i
on r.inventory_id = i.inventory_id
inner join film_category as fc 
on i.film_id = fc.film_id
inner join category as c
on fc.category_id = c.category_id
group by c."name"
order by sum(p.amount) desc
limit 1;

--вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select f.title
from film as f
left join inventory as i
on f.film_id = i.film_id
where i.film_id is null

--вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
select 	a.first_name,
		a.last_name,
		t.film_cnt
from (
	select	fa.actor_id,
			count(*) as film_cnt,
			dense_rank() over (order by count(*) desc) as r
	from film_actor as fa
	inner join film_category as fc
	on fa.film_id = fc.film_id
	inner join category as c
	on fc.category_id = c.category_id
	where c."name" = 'Children'
	group by fa.actor_id
) as t
inner join actor as a
on t.actor_id = a.actor_id
where t.r < 4
order by t.film_cnt desc

--вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
select 	ct.city,
		sum(c.active) as active_customers,
		sum(1-c.active) as not_active_customers
from customer as c
inner join address as a
on c.address_id = a.address_id
inner join city as ct
on ct.city_id = a.city_id
group by ct.city
order by sum(c.active) desc

--вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
	select 	distinct on(city_type.city_type) city_type.city_type,
			c."name" as category_name,
			sum(extract(day from return_date-rental_date)*24+extract(hour from return_date-rental_date)+extract(minute from return_date-rental_date)/60) as total_rental_hours
	from category as c
	inner join film_category as fc
	on c.category_id = fc.category_id
	inner join inventory as i
	on i.film_id = fc.film_id
	inner join rental as r
	on i.inventory_id = r.inventory_id
	inner join customer as cus
	on r.customer_id = cus.customer_id
	inner join address as a
	on cus.address_id = a.address_id
	inner join (
		select	city_id,
				case
					when city ilike 'a%' then 'starts_with_a'
					when city ilike '%-%' then 'contains_hyphen'
				end as city_type
		from city) as city_type
	on a.city_id = city_type.city_id
		and city_type.city_type is not null
	group by city_type.city_type, c."name"
	order by 1,3 desc
