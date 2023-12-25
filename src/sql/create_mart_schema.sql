CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.d_calendar (
	date_id int not null,
	date_actual date not null,
	epoch bigint not null,
	day_suffix varchar(4) not null,
	day_name varchar(9) not null,
	day_of_week int not null,
	day_of_month int not null,
	day_of_quarter int not null,
	day_of_year int not null,
	week_of_month int not null,
	week_of_year int not null,
	week_of_year_iso char(10) not null,
	month_actual int not null,
	month_name varchar(9) not null,
	month_name_abbreviated char(3) not null,
	quarter_actual int not null,
	quarter_name varchar(9) not null,
	year_actual int not null,
	first_day_of_week date not null,
 	last_day_of_week date not null,
 	first_day_of_month date not null,
 	last_day_of_month date not null,
 	first_day_of_quarter date not null,
 	last_day_of_quarter date not null,
 	first_day_of_year date not null,
 	last_day_of_year date not null,
 	mmyyyy char(6) not null,
 	mmddyyyy char(10) not null,
 	weekend_indr bool not null,
 	CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY(date_id)
);

CREATE INDEX IF NOT EXISTS d_date_date_actual_idx 
    ON mart.d_calendar(date_actual);

CREATE TABLE IF NOT EXISTS mart.d_city (
	id serial not null primary key,
	city_id int UNIQUE,
	city_name varchar(50)
);

CREATE TABLE IF NOT EXISTS mart.d_customer (
	id serial not null primary key,
	customer_id int not null UNIQUE,
	first_name varchar(15),
	last_name varchar(15),
	city_id int,
	CONSTRAINT d_customer_city_id_fkey FOREIGN KEY (city_id) REFERENCES mart.d_city(id)
);

CREATE INDEX IF NOT EXISTS d_customer_city_id_ix
    ON mart.d_customer(city_id);

CREATE TABLE IF NOT EXISTS mart.d_item (
	id serial not null primary key,
	item_id not null unique,
	item_name varchar(50)
);

CREATE TABLE IF NOT EXISTS mart.f_activity (
	id serial primary key,
	activity_id int,
	date_id int,
	click_number bigint,
	CONSTRAINT f_activity_date_id_fk FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id)
);

CREATE INDEX IF NOT EXISTS f_activity_date_id_ix
    ON mart.f_activity(date_id);

CREATE TABLE IF NOT EXISTS mart.f_sales (
	id serial primary key,
	date_id int not null,
	item_id int not null,
	customer_id int not null,
	city_id int not null,
	quantity bigint,
	payment_amount numeric(10,2),
	CONSTRAINT f_sales_date_id_fk FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id),
	CONSTRAINT f_sales_item_id_fk FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id),
	CONSTRAINT f_sales_customer_id_fk FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id),
	CONSTRAINT f_sales_d_city_id_fk FOREIGN KEY (city_id) REFERENCES mart.d_city(city_id)
);

CREATE INDEX IF NOT EXISTS f_sales_date_id_ix 
    ON mart.f_sales(date_id);

CREATE INDEX IF NOT EXISTS f_sales_item_id_ix 
    ON mart.f_sales(item_id);

CREATE INDEX IF NOT EXISTS f_sales_customer_id_ix 
    ON mart.f_sales(customer_id);

CREATE INDEX IF NOT EXISTS f_sales_city_id_ix 
    ON mart.f_sales(city_id);

CREATE OR REPLACE PROCEDURE mart.update_d_city(p_date timestamp DEFAULT NULL)
AS
$$
BEGIN
  /*
   * If p_date is null then grab all data from staging.user_order_log.
   * It is required for full load, but for incremental load we can use date_time.
   */
  INSERT INTO mart.d_city(city_id, city_name)
  SELECT DISTINCT uol.city_id
       , uol.city_name
    FROM staging.user_order_log uol
   WHERE (p_date IS NULL OR uol.date_time = p_date)
      ON CONFLICT (city_id) DO NOTHING;

  ANALYZE mart.d_city;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_d_customer(p_date timestamp DEFAULT NULL)
AS
$$
BEGIN
  /*
   * If p_date is null then grab all data from staging.user_order_log.
   * It is required for full load, but for incremental load we can use date_time.
   */
  WITH ds AS (
  SELECT uol.customer_id
       , uol.first_name
       , uol.last_name
       , MAX(uol.city_id) AS city_id
    FROM staging.user_order_log uol
   WHERE (p_date IS NULL OR uol.date_time = p_date)
   GROUP BY uol.customer_id, uol.first_name, uol.last_name
  )
  INSERT INTO mart.d_customer(customer_id, first_name, last_name, city_id)
  SELECT d.*
    FROM ds d
      ON CONFLICT (customer_id) DO NOTHING;

  ANALYZE mart.d_customer;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_d_item(p_date timestamp DEFAULT NULL)
AS
$$
BEGIN
  /*
   * If p_date is null then grab all data from staging.user_order_log.
   * It is required for full load, but for incremental load we can use date_time.
   */
  INSERT INTO mart.d_item(item_id, item_name)
  SELECT DISTINCT uol.item_id
       , uol.item_name
    FROM staging.user_order_log uol
   WHERE (p_date IS NULL OR uol.date_time = p_date)
      ON CONFLICT (item_id) DO NOTHING;

  ANALYZE mart.d_item;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_d_calendar(p_date timestamp DEFAULT NULL)
AS
$$
BEGIN
  /*
   * If p_date is null then grab all data from staging.user_order_log, 
        staging.user_activity_log and staging.customer_research.
   * It is required for full load, but for incremental load we can use date_time.
   */
  WITH ds AS (
  SELECT cr.date_id::date AS dt
    FROM staging.customer_research cr
   WHERE (p_date IS NULL OR cr.date_id = p_date)

   UNION

  SELECT uol.date_time::date
    FROM staging.user_order_log uol
   WHERE (p_date IS NULL OR uol.date_time = p_date)

   UNION

  SELECT ual.date_time::date
    FROM staging.user_activity_log ual
   WHERE (p_date IS NULL OR ual.date_time = p_date)  
  )
  INSERT INTO mart.d_calendar
  SELECT REPLACE(c.dt::text, '-', '')::int AS date_id
       , c.dt AS date_actual
       , extract(epoch from c.dt) AS epoch
       , LTRIM(to_char(c.dt, 'DDth'), '0') AS day_suffix
       , TRIM(to_char(c.dt, 'Day')) AS day_name
       , to_char(c.dt, 'ID')::int AS day_of_week
       , LTRIM(to_char(c.dt, 'DD'), '0')::int AS day_of_month
       , c.dt - date_trunc('quarter', c.dt)::date + 1 AS day_of_quarter
       , LTRIM(to_char(c.dt, 'DDD'), '0')::int AS day_of_year
       , to_char(c.dt, 'W')::int AS week_of_month
       , LTRIM(to_char(c.dt, 'IW'), '0')::int AS week_of_year
       , FORMAT('%s-W%s-%s', to_char(c.dt, 'IYYY'), to_char(c.dt, 'IW'), to_char(c.dt, 'ID')) AS week_of_year_iso
       , LTRIM(to_char(c.dt, 'MM'), '0')::int AS month_actual
       , TRIM(to_char(c.dt, 'Month')) AS month_name
       , to_char(c.dt, 'Mon') AS month_name_abbreviated
       , to_char(c.dt, 'Q')::int AS quarter_actual

       , CASE to_char(c.dt, 'Q')
           WHEN '1' THEN 'First'
           WHEN '2' THEN 'Second'
           WHEN '3' THEN 'Third'
           WHEN '4' THEN 'Fourth'
         END AS quarter_name

       , to_char(c.dt, 'YYYY')::int AS year_actual
       , date_trunc('week', c.dt)::date AS first_day_of_week
       , (date_trunc('week', c.dt)::date + '6 days'::interval)::date AS last_day_of_week
       , date_trunc('month', c.dt)::date AS first_day_of_month
       , (date_trunc('month', c.dt)::date + '1 month'::interval - '1 day'::interval)::date AS last_day_of_month
       , date_trunc('quarter', c.dt)::date AS first_day_of_quarter
       , (date_trunc('quarter', c.dt)::date + '3 month'::interval - '1 day'::interval)::date AS last_day_of_quarter
       , date_trunc('year', c.dt)::date AS first_day_of_year
       , (date_trunc('year', c.dt)::date + '1 year'::interval - '1 day':: interval)::date AS last_day_of_year
       , to_char(c.dt, 'mmyyyy') AS mmyyyy
       , to_char(c.dt, 'mmddyyyy') AS mmddyyyy
       , to_char(c.dt, 'ID') IN ('6', '7') AS weekend_indr
    FROM ds c
      ON CONFLICT (date_id) DO NOTHING;

  ANALYZE mart.d_calendar;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_f_activity(p_date timestamp DEFAULT NULL)
AS
$$
BEGIN
  WITH ual AS (
  /*
   * If p_date is null then grab all data from staging.user_activity_log.
   * It is required for full load, but for incremental load we can use date_time.
   */
  SELECT ual.action_id
       , ual.date_time
       , SUM(ual.quantity) AS click_number
    FROM staging.user_activity_log ual
   WHERE (p_date IS NULL OR ual.date_time = p_date)
   GROUP BY ual.action_id, ual.date_time
  )
  INSERT INTO mart.f_activity (activity_id, date_id, click_number)
  SELECT ual.action_id
       , cl.date_id
       , ual.click_number
    FROM ual
    LEFT JOIN mart.d_calendar cl
      ON cl.date_actual = ual.date_time;

  ANALYZE mart.f_activity;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_f_daily_sales(p_date timestamp DEFAULT NULL)
AS
$$
BEGIN
  /*
   * If p_date is null then grab all data from staging.user_order_log.
   * It is required for full load, but for incremental load we can use date_time.
   */
  WITH uol AS ( 
  SELECT uol.date_time
       , uol.item_id
       , uol.customer_id
       , uol.city_id
       , SUM(uol.quantity) AS quantity
       
       , SUM(CASE 
       	       WHEN COALESCE(uol.status, 'shipped') = 'shipped' THEN 1 
       	       ELSE -1 
       	     END * uol.payment_amount) AS payment_amount
    FROM staging.user_order_log uol
   WHERE (p_date IS NULL OR uol.date_time = p_date) 
   GROUP BY uol.date_time, uol.item_id, uol.customer_id, uol.city_id
  )
  INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
  SELECT cl.date_id
       , uol.item_id
       , uol.customer_id
       , uol.city_id
       , uol.quantity
       , uol.payment_amount
    FROM uol
    LEFT JOIN mart.d_calendar cl
      ON cl.date_actual = uol.date_time;

  ANALYZE mart.f_daily_sales;
END
$$
LANGUAGE plpgsql;