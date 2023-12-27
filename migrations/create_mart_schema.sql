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
	city_name varchar(50),
    	created_at timestamp,
    	updated_at timestamp
);

CREATE TABLE IF NOT EXISTS mart.d_customer (
	id serial not null primary key,
	customer_id int not null UNIQUE,
	first_name varchar(15),
	last_name varchar(15),
    	city_id int,
    	created_at timestamp,
    	updated_at timestamp,
	CONSTRAINT d_customer_city_id_fkey FOREIGN KEY (city_id) REFERENCES mart.d_city(id)
);

CREATE INDEX IF NOT EXISTS d_customer_city_id_ix
    ON mart.d_customer(city_id);

CREATE TABLE IF NOT EXISTS mart.d_item (
	id serial not null primary key,
	item_id int not null unique,
	item_name varchar(50),
    	created_at timestamp,
    	updated_at timestamp
);

CREATE TABLE IF NOT EXISTS mart.f_activity (
	id serial primary key,
	action_id int,
	date_id int,
	customer_id int,
	quantity bigint,
	CONSTRAINT f_activity_date_id_fk FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id),
	CONSTRAINT f_activity_customer_id_fk FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id)
);

CREATE INDEX IF NOT EXISTS f_activity_date_id_ix
    ON mart.f_activity(date_id);

CREATE INDEX IF NOT EXISTS f_activity_customer_id_ix
    ON mart.f_activity(customer_id);

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

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    new_customers_count bigint,
    returning_customers_count bigint,
    refunded_customer_count bigint,
    period_name text,
    period_id int,
    item_id int,
    new_customers_revenue numeric(10,2),
    returning_customers_revenue numeric(10,2),
    customers_refunded bigint,
    CONSTRAINT f_customer_retention_pk PRIMARY KEY(period_id, item_id),
    CONSTRAINT f_customer_retention_item_id_fk FOREIGN KEY(item_id) REFERENCES mart.d_item(item_id)
);

CREATE INDEX IF NOT EXISTS f_customer_retention_item_id_ix
    ON mart.f_customer_retention(item_id);

CREATE OR REPLACE FUNCTION mart.get_staging_load_dates_status(p_file_dt VARCHAR(10),
                                                              OUT p_dt1 text, 
                                  OUT p_dt2 text, 
                                  OUT p_is_success int)
AS
$$
DECLARE
  v_dt timestamp := COALESCE(NULLIF(TRIM(p_file_dt), '')::timestamp, '0001-01-01'::timestamp);
  v_non_success_cnt int;
BEGIN
  SELECT MIN(lsh.min_date) AS min_date
       , MAX(lsh.max_date) AS max_date
       , COUNT(1) FILTER(WHERE lsh.status != 'success') AS non_success_cnt
    INTO p_dt1, p_dt2, v_non_success_cnt   
    FROM staging.load_staging_history lsh
   WHERE lsh.file_dt = v_dt;

   IF p_dt1 IS NULL OR v_non_success_cnt > 0 THEN
     p_is_success := 0;
     RETURN;
   END IF;

   p_is_success := 1;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_d_city(p_dt1 text, p_dt2 text)
AS
$$
BEGIN
  /* Grab data by latest date_time. If it is not present in table insert it.
   * Or we must update it but only in case of difference in data and date_time > updated_at or
   * date_time > created_at in case of updated_at IS NULL   
   */  
  WITH ds AS (
  SELECT uol.city_id
       , uol.city_name
       , uol.date_time
       , ROW_NUMBER() OVER(PARTITION BY uol.city_id ORDER BY uol.date_time DESC) AS rn
    FROM staging.user_order_log uol
   WHERE uol.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp
  )
  INSERT INTO mart.d_city AS c
  SELECT nextval('mart.d_city_id_seq') AS id
       , d.city_id
       , d.city_name
       , d.date_time AS created_at
       , NULL AS updated_at
    FROM ds d
   WHERE d.rn = 1
      ON CONFLICT (city_id)
      DO UPDATE
            SET city_name = EXCLUDED.city_name
              , updated_at = EXCLUDED.created_at
          WHERE c.city_id = EXCLUDED.city_id
            AND COALESCE(c.city_name, '-1') != COALESCE(EXCLUDED.city_name, '-1')
            AND ((c.updated_at IS NULL AND c.created_at < EXCLUDED.created_at) OR 
                  c.updated_at < EXCLUDED.created_at
                );

   ANALYZE mart.d_city;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_d_item(p_dt1 text, p_dt2 text)
AS
$$
BEGIN
  /* Grab data by latest date_time. If it is not present in table insert it.
   * Or we must update it but only in case of difference in data and date_time > updated_at or
   * date_time > created_at in case of updated_at IS NULL   
   */ 
  WITH ds AS (
  SELECT uol.item_id
       , uol.item_name
       , uol.date_time
       , ROW_NUMBER() OVER(PARTITION BY uol.item_id ORDER BY uol.date_time DESC) AS rn
    FROM staging.user_order_log uol
   WHERE uol.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp
  )
  INSERT INTO mart.d_item AS c
  SELECT nextval('mart.d_item_id_seq') AS id
       , d.item_id
       , d.item_name
       , d.date_time AS created_at
       , NULL AS updated_at
    FROM ds d
   WHERE d.rn = 1
      ON CONFLICT (item_id)
      DO UPDATE
            SET item_name = EXCLUDED.item_name
              , updated_at = EXCLUDED.created_at
          WHERE c.item_id = EXCLUDED.item_id
            AND COALESCE(c.item_name, '-1') != COALESCE(EXCLUDED.item_name, '-1')
            AND ((c.updated_at IS NULL AND c.created_at < EXCLUDED.created_at) OR 
                  c.updated_at < EXCLUDED.created_at
                );

  ANALYZE mart.d_item;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_d_customer(p_dt1 text, p_dt2 text)
AS
$$
BEGIN
  /* Grab data by latest date_time. If it is not present in table insert it.
   * Or we must update it but only in case of difference in data and date_time > updated_at or
   * date_time > created_at in case of updated_at IS NULL   
   */ 
  WITH ds AS (
  SELECT uol.customer_id
       , uol.first_name
       , uol.last_name
       , uol.city_id
       , uol.date_time
       , ROW_NUMBER() OVER(PARTITION BY uol.customer_id ORDER BY uol.date_time DESC) AS rn
    FROM staging.user_order_log uol
   WHERE uol.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp
  )
  INSERT INTO mart.d_customer AS c
  SELECT nextval('mart.d_customer_id_seq') AS id
       , d.customer_id
       , d.first_name
       , d.last_name
       , c.id AS city_id
       , d.date_time AS created_at
       , NULL AS updated_at
    FROM ds d
    LEFT JOIN mart.d_city c
      ON c.city_id = d.city_id 
   WHERE d.rn = 1
      ON CONFLICT (customer_id)
      DO UPDATE
            SET first_name = EXCLUDED.first_name
              , last_name = EXCLUDED.last_name
              , city_id = EXCLUDED.city_id
              , updated_at = EXCLUDED.created_at
          WHERE c.customer_id = EXCLUDED.customer_id
            AND (COALESCE(c.first_name, '-1') != COALESCE(EXCLUDED.first_name, '-1') OR
                 COALESCE(c.last_name, '-1') != COALESCE(EXCLUDED.last_name, '-1') OR
                 COALESCE(c.city_id, -1) != COALESCE(EXCLUDED.city_id, -1)
                )
            AND ((c.updated_at IS NULL AND c.created_at < EXCLUDED.created_at) OR 
                  c.updated_at < EXCLUDED.created_at
                );

   INSERT INTO mart.d_customer(customer_id)
   SELECT DISTINCT ual.customer_id
     FROM staging.user_activity_log ual
    WHERE ual.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp
       ON CONFLICT (customer_id) DO NOTHING;

   ANALYZE mart.d_customer;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_d_calendar(p_dt1 text, p_dt2 text)
AS
$$
BEGIN
  /*Dates can not be changed so do nothing if we have primary key conflict*/
  WITH ds AS (
  SELECT cr.date_id::date AS dt
    FROM staging.customer_research cr
   WHERE cr.date_id BETWEEN p_dt1::timestamp AND p_dt2::timestamp

   UNION

  SELECT uol.date_time::date
    FROM staging.user_order_log uol
   WHERE uol.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp

   UNION

  SELECT ual.date_time::date
    FROM staging.user_activity_log ual
   WHERE ual.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp
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

CREATE OR REPLACE PROCEDURE mart.update_f_activity(p_dt1 text, p_dt2 text)
AS
$$
DECLARE
  v_dt1 int := TO_CHAR(p_dt1::timestamp, 'YYYYMMDD')::int;
  v_dt2 int := TO_CHAR(p_dt2::timestamp, 'YYYYMMDD')::int;
BEGIN 
  /*
   * Clear data before recalculation
   */
  DELETE
    FROM mart.f_activity f
   WHERE f.date_id BETWEEN v_dt1 AND v_dt2;

  INSERT INTO mart.f_activity(action_id, date_id, customer_id, quantity)
  SELECT ual.action_id
       , cl.date_id
       , cs.customer_id AS customer_id
       , MAX(ual.quantity) AS quantity
    FROM staging.user_activity_log ual
    LEFT JOIN mart.d_calendar cl
      ON cl.date_actual = ual.date_time::date
    LEFT JOIN mart.d_customer cs
      ON cs.customer_id = ual.customer_id  
   WHERE ual.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp
   GROUP BY ual.action_id, cl.date_id, cs.customer_id;

  ANALYZE mart.f_activity;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_f_sales(p_dt1 text, p_dt2 text)
AS
$$
DECLARE
  v_dt1 int := TO_CHAR(p_dt1::timestamp, 'YYYYMMDD')::int;
  v_dt2 int := TO_CHAR(p_dt2::timestamp, 'YYYYMMDD')::int;
BEGIN
  /*
   * Clear data before recalculation
   */
  DELETE
    FROM mart.f_sales f
   WHERE f.date_id BETWEEN v_dt1 AND v_dt2;

  INSERT INTO mart.f_sales(date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
  SELECT cl.date_id
       , i.item_id
       , cs.customer_id
       , ct.city_id
       , uol.quantity

       , CASE 
       	   WHEN COALESCE(uol.status, 'shipped') = 'shipped' THEN 1 
       	   ELSE -1 
       	 END * uol.payment_amount AS payment_amount

       , COALESCE(uol.status, 'shipped') AS status
    FROM staging.user_order_log uol
    LEFT JOIN mart.d_calendar cl
      ON cl.date_actual = uol.date_time::date
    LEFT JOIN mart.d_item i
      ON i.item_id = uol.item_id  
    LEFT JOIN mart.d_customer cs
      ON cs.customer_id = uol.customer_id
    LEFT JOIN mart.d_city ct
      ON ct.city_id = uol.city_id  
   WHERE uol.date_time BETWEEN p_dt1::timestamp AND p_dt2::timestamp;

  ANALYZE mart.f_sales;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE mart.update_f_customer_retention(p_dt1 text, p_dt2 text)
AS
$$
DECLARE
  v_week1 int := TO_CHAR(p_dt1::timestamp, 'WW')::int;
  v_week2 int := TO_CHAR(p_dt2::timestamp, 'WW')::int;
BEGIN
  /*
   * Clear data before recalculation
   */
  DELETE
    FROM mart.f_customer_retention f
   WHERE f.period_id BETWEEN v_week1 AND v_week2;

  WITH sales AS (
  SELECT d.week_of_year AS period_id
       , f.customer_id
       , f.item_id
       , f.payment_amount
       , f.status
    FROM mart.f_sales f
    JOIN mart.d_calendar d
      ON d.date_id = f.date_id
   WHERE d.week_of_year BETWEEN v_week1 AND v_week2    
  ),
  customers_sales AS (
  SELECT s.period_id
       , s.customer_id
       , COUNT(1) AS sales_cnt
    FROM sales s
   WHERE s.status = 'shipped'
   GROUP BY s.period_id, s.customer_id
  )
  INSERT INTO mart.f_customer_retention
  SELECT COUNT(DISTINCT s.customer_id) FILTER(WHERE s.status = 'shipped' 
                                                AND nc.sales_cnt = 1) AS new_customers_count
       , COUNT(DISTINCT s.customer_id) FILTER(WHERE s.status = 'shipped' 
                                                AND nc.sales_cnt > 1) AS returning_customers_count
       , COUNT(DISTINCT s.customer_id) FILTER(WHERE s.status = 'refunded') AS refunded_customer_count
  
       , 'weekly' AS period_name
       , s.period_id
       , s.item_id

       , SUM(s.payment_amount) FILTER(WHERE s.status = 'shipped' 
                                        AND nc.sales_cnt = 1) AS new_customers_revenue
       
       , SUM(s.payment_amount) FILTER(WHERE s.status = 'shipped' 
                                        AND nc.sales_cnt > 1) AS returning_customers_revenue

       , COUNT(1) FILTER(WHERE s.status = 'refunded') AS customers_refunded
    FROM sales s
    LEFT JOIN customers_sales nc
      ON nc.customer_id = s.customer_id
     AND nc.period_id = s.period_id 
   GROUP BY s.period_id, s.item_id;

   ANALYZE mart.f_customer_retention;
END
$$
LANGUAGE plpgsql;