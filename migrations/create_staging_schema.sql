CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.load_staging_history (
    id BIGSERIAL PRIMARY KEY,
    start_load TIMESTAMP,
    finish_load TIMESTAMP,
    file_name VARCHAR(32),
    status VARCHAR(12) CHECK (status IN ('success', 'in_progress', 'not_found', 'failed')),
    CONSTRAINT load_staging_history_file_name_ukey UNIQUE (file_name)
);

CREATE TABLE IF NOT EXISTS staging.load_mart_history (
    id BIGSERIAL PRIMARY KEY,
    start_load TIMESTAMP,
    finish_load TIMESTAMP,
    file_name VARCHAR(30),
    status VARCHAR(12) CHECK (status IN ('success', 'in_progress', 'failed')),
    CONSTRAINT load_mart_history_file_name_ukey UNIQUE (file_name)
);

CREATE TABLE IF NOT EXISTS staging.customer_research (
    date_id timestamp,
    category_id int,
    geo_id int,
    sales_qty int,
    sales_amt numeric(14,2),
    CONSTRAINT customer_research_pk PRIMARY KEY(date_id, category_id, geo_id)
);

CREATE TABLE IF NOT EXISTS staging.user_activity_log (
	uniq_id VARCHAR(32) NOT NULL,
	date_time timestamp NOT NULL,
	action_id int NOT NULL,
	customer_id int NOT NULL,
	quantity bigint NOT NULL,
	CONSTRAINT user_activity_log_pk PRIMARY KEY(uniq_id)
);

CREATE INDEX IF NOT EXISTS ual_date_time_ix
    ON staging.user_activity_log(date_time);

CREATE TABLE IF NOT EXISTS staging.user_order_log (
	uniq_id VARCHAR(32) NOT NULL,
	date_time timestamp NOT NULL,
	city_id int NOT NULL,
	city_name varchar(100),
	customer_id int NOT NULL,
	first_name varchar(100),
	last_name varchar(100),
	item_id int NOT NULL,
	item_name varchar(100),
	quantity bigint,
	payment_amount numeric(10,2),
	CONSTRAINT user_order_log_pk PRIMARY KEY(uniq_id)
);

CREATE INDEX IF NOT EXISTS uol_date_time_ix
    ON staging.user_order_log(date_time);

CREATE OR REPLACE FUNCTION staging.start_staging_load(p_obj_name VARCHAR(30),
						      OUT batch_id bigint, 
						      OUT load_status VARCHAR(8)) AS
$$
BEGIN
  SELECT lh.id
       , lh.status
    INTO batch_id, load_status
    FROM staging.load_staging_history lh
   WHERE lh.file_name = p_obj_name;

  IF batch_id IS NOT NULL AND load_status = 'success' THEN
     RETURN;
  END IF;

  IF batch_id IS NULL THEN
    INSERT INTO staging.load_staging_history(start_load, file_name, status)
    VALUES (current_timestamp, p_obj_name, 'in_progress') 
    RETURNING id, status INTO batch_id, load_status;

    RETURN;
  END IF;

  UPDATE staging.load_staging_history lh
     SET start_load = current_timestamp
       , finish_load = NULL
       , status = 'in_progress'
   WHERE lh.id = batch_id;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE staging.finish_staging_load(p_id bigint,
							p_status VARCHAR(12)) AS
$$
UPDATE staging.load_staging_history lh
   SET finish_load = current_timestamp
     , status = p_status
 WHERE lh.id = p_id;
$$
LANGUAGE sql;
