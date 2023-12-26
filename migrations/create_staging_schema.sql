CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.load_staging_history (
    id BIGSERIAL PRIMARY KEY,
    start_load TIMESTAMP,
    finish_load TIMESTAMP,
    min_date TIMESTAMP,
    max_date TIMESTAMP,
    file_dt TIMESTAMP,
    file_name VARCHAR(32),
    status VARCHAR(12) CHECK (status IN ('success', 'in_progress', 'not_found', 'failed')),
    CONSTRAINT load_staging_history_file_name_ukey UNIQUE (file_name)
);

CREATE INDEX IF NOT EXISTS load_staging_history_file_dt_ix
    ON staging.load_staging_history(file_dt);

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

CREATE OR REPLACE PROCEDURE staging.start_staging_load(p_file_dt VARCHAR(10), 
                            p_file_name VARCHAR(32)) AS
$$
INSERT INTO staging.load_staging_history AS lsh
VALUES (nextval('staging.load_staging_history_id_seq'), current_timestamp, NULL, 
        NULL, NULL, COALESCE(NULLIF(TRIM(p_file_dt), '')::timestamp, '0001-01-01'::timestamp), 
        p_file_name, 'in_progress')
    ON CONFLICT (file_name)
    DO UPDATE
          SET start_load = EXCLUDED.start_load
            , finish_load = EXCLUDED.finish_load
            , status = EXCLUDED.status
        WHERE lsh.file_name = EXCLUDED.file_name;
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE staging.drop_staging_file_data(p_file_name VARCHAR(32),
                              p_obj_name VARCHAR(32), 
                              p_col_name VARCHAR(64)) AS
$$
DECLARE
  v_min_date TIMESTAMP;
  v_max_date TIMESTAMP;
BEGIN
  SELECT lsh.min_date, lsh.max_date
    INTO v_min_date, v_max_date
    FROM staging.load_staging_history lsh
   WHERE lsh.file_name = p_file_name;

  EXECUTE FORMAT('DELETE FROM staging.%s WHERE %s BETWEEN $1 AND $2', 
                 p_obj_name, p_col_name) USING v_min_date, v_max_date;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE staging.finish_staging_load(p_file_name VARCHAR(32),
							p_status VARCHAR(12),
                            p_min_date timestamp DEFAULT NULL,
                            p_max_date timestamp DEFAULT NULL) AS
$$
UPDATE staging.load_staging_history lh
   SET finish_load = current_timestamp
     , status = p_status
     , min_date = COALESCE(p_min_date, min_date)
     , max_date = COALESCE(p_max_date, max_date)
 WHERE lh.file_name = p_file_name;
$$
LANGUAGE sql;