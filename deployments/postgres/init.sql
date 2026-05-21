-- psql meta-commands read app credentials from container environment at init time.
\set ON_ERROR_STOP on
\getenv app_db_user APP_DB_USER
\getenv app_db_password APP_DB_PASSWORD

-- Enable TimescaleDB in the target database.
CREATE EXTENSION IF NOT EXISTS timescaledb;

DO
$$
DECLARE
  app_user text := :'app_db_user';
  app_password text := :'app_db_password';
  db_name text := current_database();
BEGIN
  IF app_user IS NULL OR btrim(app_user) = '' THEN
    RAISE EXCEPTION 'APP_DB_USER must be set';
  END IF;

  IF app_password IS NULL OR btrim(app_password) = '' THEN
    RAISE EXCEPTION 'APP_DB_PASSWORD must be set';
  END IF;

  -- Create or update the dedicated application role.
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = app_user) THEN
    EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', app_user, app_password);
  ELSE
    EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L', app_user, app_password);
  END IF;

  -- Grant full database-level privileges to the application role.
  EXECUTE format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', db_name, app_user);

  -- Ensure required schemas exist and are accessible to the app role.
  EXECUTE 'CREATE SCHEMA IF NOT EXISTS public';
  EXECUTE 'CREATE SCHEMA IF NOT EXISTS timeseries';
  EXECUTE format('GRANT USAGE, CREATE ON SCHEMA public TO %I', app_user);
  EXECUTE format('GRANT USAGE, CREATE ON SCHEMA timeseries TO %I', app_user);
END
$$;
