CREATE SCHEMA IF NOT EXISTS utils;

CREATE OR REPLACE PROCEDURE utils.add_column(p_schema text, 
                                             p_table text,
                                             p_col_name text, 
                                             p_col_type text) AS
$$
DECLARE
  v_rec RECORD;
BEGIN
  /* Before adding some column we need to check whether it exists in p_table */
  FOR v_rec IN (SELECT FORMAT('ALTER TABLE %s.%s ADD COLUMN %s %s', nsp.nspname, c.relname, p_col_name, p_col_type) AS cmd
                  FROM pg_namespace nsp
                  JOIN pg_class c
                    ON c.relnamespace = nsp.oid
                   AND c.relkind = 'r'
                   AND c.relname = p_table
                 WHERE nsp.nspname = p_schema
                   AND NOT EXISTS (SELECT 1
                                     FROM pg_attribute a
                                    WHERE a.attrelid = c.oid
                                      AND a.attname = p_col_name
                                      AND NOT a.attisdropped
                                      AND a.attnum > 0
                                  )
               )
  LOOP
    EXECUTE v_rec.cmd;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

BEGIN;
  CALL utils.add_column('staging', 'user_order_log', 'status', 'varchar(10)');
  CALL utils.add_column('mart', 'user_order_log', 'status', 'varchar(10)');
COMMIT;