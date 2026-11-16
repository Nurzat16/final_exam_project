-- Apply with psql -v login=korgonbaeva -f 01_schema.sql.
\set stg :login'_stg'
\set ods :login'_ods'
\set dm :login'_datamart'

CREATE SCHEMA IF NOT EXISTS :"stg";
CREATE SCHEMA IF NOT EXISTS :"ods";
CREATE SCHEMA IF NOT EXISTS :"dm";
CREATE SCHEMA IF NOT EXISTS :"login";

CREATE TABLE IF NOT EXISTS :"stg".stg_sensors_history (
    event_id text, location_id text, sensor_id text, parameter text,
    measured_at text, value text, unit text,
    _ds text NOT NULL, _source_file text NOT NULL, _loaded_at text NOT NULL
);
CREATE TABLE IF NOT EXISTS :"stg".stg_locations_dictionary (
    location_id text, location_name text, city text, latitude text, longitude text,
    _ds text NOT NULL, _source_file text NOT NULL, _loaded_at text NOT NULL
);
CREATE TABLE IF NOT EXISTS :"stg".stg_api_hourly (
    sensor_id text, location_id text, parameter text, measured_at text, value text, unit text,
    _ds text NOT NULL, _source_file text NOT NULL, _loaded_at text NOT NULL
);
CREATE TABLE IF NOT EXISTS :"stg".etl_load_log (
    id bigserial PRIMARY KEY,
    dag_id text NOT NULL, task_id text NOT NULL, source text NOT NULL, ds date NOT NULL,
    started_at timestamptz NOT NULL, finished_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('success', 'failed')),
    row_count integer NOT NULL DEFAULT 0, error_text text
);

CREATE TABLE IF NOT EXISTS :"ods".ods_sensors_history (
    event_id text PRIMARY KEY, location_id integer NOT NULL, sensor_id integer NOT NULL,
    parameter text NOT NULL, measured_at timestamptz NOT NULL,
    value numeric(14,4) NOT NULL, unit text NOT NULL
);
CREATE TABLE IF NOT EXISTS :"ods".ods_locations_dictionary (
    location_id integer PRIMARY KEY, location_name text NOT NULL, city text NOT NULL,
    latitude numeric(9,6), longitude numeric(9,6)
);
CREATE TABLE IF NOT EXISTS :"ods".ods_api_hourly (
    sensor_id integer NOT NULL, location_id integer, parameter text NOT NULL,
    measured_at timestamptz NOT NULL, value numeric(14,4) NOT NULL, unit text NOT NULL,
    PRIMARY KEY (sensor_id, measured_at, parameter)
);

CREATE TABLE IF NOT EXISTS :"dm".dm_daily_air_quality (
    ds date PRIMARY KEY, measurement_count bigint NOT NULL, value_sum numeric(20,4) NOT NULL,
    mean_value numeric(14,4), api_measurement_count bigint NOT NULL, source_count integer NOT NULL
);

CREATE TABLE IF NOT EXISTS :"login".sensor_status (
    sensor_id integer PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('active', 'inactive', 'maintenance')),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS :"ods".ods_sensor_status (
    sensor_id integer PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('active', 'inactive', 'maintenance')),
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS :"dm".dm_monthly_by_station (
    month date NOT NULL,
    location_id integer NOT NULL,
    location_name text NOT NULL,
    city text NOT NULL,
    measurement_count bigint NOT NULL,
    value_sum numeric(20,4) NOT NULL,
    mean_value numeric(14,4),
    api_measurement_count bigint NOT NULL,
    PRIMARY KEY (month, location_id)
);
