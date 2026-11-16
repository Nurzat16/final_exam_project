import csv
import json
import os
import subprocess
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import clickhouse_connect
import pendulum
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

ROOT = Path(__file__).resolve().parents[1]
ZONE = ZoneInfo("Asia/Bishkek")
CONNECTION = "aiacademy_pg_korgonbaeva"
MAIN = "final_korgonbaeva_pipeline"
MART = "final_korgonbaeva_datamart"
STG, ODS, DM = "korgonbaeva_stg", "korgonbaeva_ods", "korgonbaeva_datamart"
FIELDS = {
    "sensors_history": ("event_id", "location_id", "sensor_id", "parameter", "measured_at", "value", "unit"),
    "locations_dictionary": ("location_id", "location_name", "city", "latitude", "longitude"),
    "api_hourly": ("sensor_id", "location_id", "parameter", "measured_at", "value", "unit"),
}

def connection():
    return BaseHook.get_connection(CONNECTION)

@contextmanager
def postgres():
    c = connection()
    db = psycopg2.connect(host=c.host, port=c.port or 5432, dbname=c.schema, user=c.login, password=c.password, connect_timeout=8)
    try:
        with db, db.cursor() as cursor:
            yield cursor
    finally:
        db.close()

@contextmanager
def logged(context, source):
    start = datetime.now(timezone.utc)
    stats = {"rows": 0}
    status, error = "success", None
    try:
        yield stats
    except Exception as exc:
        status, error = "failed", str(exc)[:1500]
        raise
    finally:
        with postgres() as cur:
            cur.execute("""INSERT INTO korgonbaeva_stg.etl_load_log
                (dag_id,task_id,source,ds,started_at,finished_at,status,row_count,error_text)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (context["dag"].dag_id, context["task"].task_id, source, context["ds"], start,
                 datetime.now(timezone.utc), status, stats["rows"], error))

def local_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(ZONE)

def fetch_hours(ds):
    config = json.loads((ROOT / "data/openaq_sources.json").read_text())
    day = date.fromisoformat(ds)
    start = datetime.combine(day, datetime.min.time(), ZONE)
    stop = start + timedelta(days=1)
    rows = []
    base_url = os.environ.get("OPENAQ_API_BASE_URL", "https://api.openaq.org/v3").rstrip("/")
    retry = Retry(total=2, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504), allowed_methods={"GET"})
    with requests.Session() as session:
        session.headers["X-API-Key"] = os.environ["OPENAQ_API_KEY"]
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        for station in config["locations"]:
            if station["source"] != "api_hours":
                continue
            sensor = station["sensor_id"]
            url = f"{base_url}/sensors/{sensor}/hours"
            response = session.get(url, params={"datetime_from": start.isoformat(), "datetime_to": stop.isoformat(), "limit": 1000}, timeout=(8, 30))
            response.raise_for_status()
            items = response.json()["results"]
            if len(items) >= 1000:
                raise ValueError("Unexpected OpenAQ pagination for a one-day hourly query")
            for item in items:
                when = local_time(item["period"]["datetimeFrom"]["utc"])
                parameter = item["parameter"]
                if not start <= when < stop or item["value"] is None:
                    continue
                if parameter["name"] != "pm25" or parameter["units"] != "µg/m³":
                    raise ValueError("Unexpected OpenAQ parameter or units")
                rows.append((sensor, station["id"], "pm25", when.isoformat(), item["value"], parameter["units"]))
    return rows

def load_stage(source):
    context = get_current_context()
    ds = context["ds"]
    with logged(context, source) as stats:
        if source == "api_hourly":
            rows, origin = fetch_hours(ds), "OpenAQ_v3_hours"
        else:
            path = ROOT / "data" / f"{source}.csv"
            origin = path.name
            with path.open(encoding="utf-8-sig", newline="") as stream:
                rows = [tuple(row[key] for key in FIELDS[source]) for row in csv.DictReader(stream)
                        if source != "sensors_history" or local_time(row["measured_at"]).date().isoformat() == ds]
        relation = sql.Identifier(STG, f"stg_{source}")
        columns = sql.SQL(",").join(map(sql.Identifier, FIELDS[source] + ("_ds", "_source_file", "_loaded_at")))
        timestamp = datetime.now(timezone.utc).isoformat()
        with postgres() as cur:
            cur.execute(sql.SQL("DELETE FROM {} WHERE _ds=%s").format(relation), (ds,))
            if rows:
                values = [tuple(map(str, row)) + (ds, origin, timestamp) for row in rows]
                execute_values(cur, sql.SQL("INSERT INTO {} ({}) VALUES %s").format(relation, columns), values)
        stats["rows"] = len(rows)

ODS_SQL = {
    "sensors_history": ("event_id,location_id::int,sensor_id::int,lower(parameter),measured_at::timestamptz,value::numeric,unit", "event_id"),
    "locations_dictionary": ("location_id::int,location_name,city,latitude::numeric,longitude::numeric", "location_id"),
    "api_hourly": ("sensor_id::int,location_id::int,lower(parameter),measured_at::timestamptz,value::numeric,unit", "sensor_id,measured_at,parameter"),
}

def load_ods(source):
    context = get_current_context()
    ds = context["ds"]
    fields, keys = ODS_SQL[source]
    stage = sql.Identifier(STG, f"stg_{source}")
    target = sql.Identifier(ODS, f"ods_{source}")
    with logged(context, f"stg_{source}") as stats, postgres() as cur:
        if source != "locations_dictionary":
            cur.execute(sql.SQL("DELETE FROM {} WHERE (measured_at AT TIME ZONE 'Asia/Bishkek')::date=%s").format(target), (ds,))
        key_names = keys.split(",")
        updates = sql.SQL(",").join(sql.SQL("{0}=EXCLUDED.{0}").format(sql.Identifier(field))
                                     for field in FIELDS[source] if field not in key_names)
        columns = sql.SQL(",").join(map(sql.Identifier, FIELDS[source]))
        key_columns = sql.SQL(",").join(map(sql.Identifier, key_names))
        cur.execute(sql.SQL("INSERT INTO {} ({}) SELECT DISTINCT ON ({}) {} FROM {} WHERE _ds=%s ORDER BY {}, _loaded_at DESC ON CONFLICT ({}) DO UPDATE SET {}").format(
            target, columns, key_columns, sql.SQL(fields), stage, key_columns, key_columns, updates), (ds,))
        stats["rows"] = cur.rowcount

class LoggedTrigger(TriggerDagRunOperator):
    def execute(self, context):
        with logged(context, MART):
            return super().execute(context)

def build_daily():
    context = get_current_context()
    with logged(context, "dbt") as stats:
        c = connection()
        env = dict(os.environ, PGHOST=c.host, PGPORT=str(c.port or 5432), PGDATABASE=c.schema, PGUSER=c.login, PGPASSWORD=c.password or "")
        dbt = str(ROOT / "dbt")
        subprocess.run(["dbt", "build", "--project-dir", dbt, "--profiles-dir", dbt, "--select", "+dm_daily_air_quality"], env=env, check=True, timeout=240)
        with postgres() as cur:
            cur.execute("SELECT count(*) FROM korgonbaeva_datamart.dm_daily_air_quality WHERE ds=%s", (context["ds"],))
            stats["rows"] = cur.fetchone()[0]

def build_monthly():
    context = get_current_context()
    month = date.fromisoformat(context["ds"]).replace(day=1)
    with logged(context, "ods_hours") as stats, postgres() as cur:
        cur.execute("DELETE FROM korgonbaeva_datamart.dm_monthly_by_station WHERE month=%s", (month,))
        cur.execute("""INSERT INTO korgonbaeva_datamart.dm_monthly_by_station (
                month, location_id, location_name, city, measurement_count,
                value_sum, mean_value, api_measurement_count
            )
            SELECT date_trunc('month', h.ds)::date, s.location_id, s.location_name, s.city,
                   count(*), sum(h.value), round(avg(h.value),4), count(*) FILTER (WHERE h.is_api)
            FROM korgonbaeva_ods.v_measurements_hourly AS h
            JOIN korgonbaeva_ods.ods_locations_dictionary AS s USING (location_id)
            WHERE h.ds >= %s AND h.ds < %s + INTERVAL '1 month'
            GROUP BY 1,2,3,4""", (month, month))
        stats["rows"] = cur.rowcount

def copy_clickhouse():
    context = get_current_context()
    day = date.fromisoformat(context["ds"])
    with logged(context, "ClickHouse") as stats:
        with postgres() as cur:
            cur.execute("SELECT ds,measurement_count,value_sum,mean_value,api_measurement_count,source_count FROM korgonbaeva_datamart.dm_daily_air_quality WHERE ds=%s", (day,))
            rows = cur.fetchall()
        ch = clickhouse_connect.get_client(host=os.environ.get("CLICKHOUSE_HOST") or os.environ["CH_HOST"], port=int(os.environ.get("CLICKHOUSE_PORT") or os.environ["CH_PORT"]), username=os.environ.get("CLICKHOUSE_USER") or os.environ["CH_USER"], password=os.environ.get("CLICKHOUSE_PASSWORD") or os.environ["CH_PASSWORD"])
        try:
            ch.command("CREATE DATABASE IF NOT EXISTS korgonbaeva")
            for table in ("dm_daily_air_quality", "dm_daily_air_quality_stage"):
                ch.command(f"""CREATE TABLE IF NOT EXISTS korgonbaeva.{table} (ds Date, measurement_count Int64,
                    value_sum Decimal(20,4), mean_value Nullable(Decimal(14,4)), api_measurement_count Int64, source_count Int32)
                    ENGINE=MergeTree PARTITION BY ds ORDER BY ds""")
            dest = "korgonbaeva.dm_daily_air_quality"
            stage = f"{dest}_stage"
            partition = f"'{day.isoformat()}'"
            ch.command(f"ALTER TABLE {stage} DROP PARTITION {partition}")
            if rows:
                ch.insert(stage, rows)
                ch.command(f"ALTER TABLE {dest} REPLACE PARTITION {partition} FROM {stage}")
            else:
                ch.command(f"ALTER TABLE {dest} DROP PARTITION {partition}")
            actual = ch.query(f"SELECT ds,measurement_count,value_sum,mean_value,api_measurement_count,source_count FROM {dest} WHERE ds=toDate({partition})").result_rows
            if [tuple(row) for row in actual] != rows:
                raise ValueError("PostgreSQL and ClickHouse daily rows differ")
            stats["rows"] = len(rows)
        finally:
            ch.close()

def check_marts():
    context = get_current_context()
    day = date.fromisoformat(context["ds"])
    month = day.replace(day=1)
    with logged(context, "datamart") as stats, postgres() as cur:
        cur.execute("""SELECT
            (SELECT coalesce(sum(measurement_count),0) FROM korgonbaeva_datamart.dm_daily_air_quality WHERE ds >= %s AND ds < %s + INTERVAL '1 month'),
            (SELECT coalesce(sum(measurement_count),0) FROM korgonbaeva_datamart.dm_monthly_by_station WHERE month=%s)""", (month, month, month))
        daily, monthly = cur.fetchone()
        if daily != monthly:
            raise ValueError(f"Daily and monthly counts differ: {daily} vs {monthly}")
        stats["rows"] = daily

start_date = pendulum.datetime(2026, 9, 1, tz="UTC")
schedule = None if os.getenv("OPENAQ_MANUAL_RUN") == "1" else "@daily"
with DAG(MAIN, start_date=start_date, schedule=schedule, catchup=False, max_active_runs=1, tags=["openaq"]) as pipeline:
    trigger = LoggedTrigger(task_id="trigger_datamart", trigger_dag_id=MART, logical_date="{{ logical_date.isoformat() }}", reset_dag_run=True)
    for source in FIELDS:
        stg = PythonOperator(task_id=f"stg_{source}", python_callable=load_stage, op_kwargs={"source": source}, retries=1)
        ods = PythonOperator(task_id=f"ods_{source}", python_callable=load_ods, op_kwargs={"source": source})
        stg >> ods >> trigger

with DAG(MART, start_date=start_date, schedule=None, catchup=False, max_active_runs=1, tags=["openaq"]) as marts:
    daily = PythonOperator(task_id="dbt_daily", python_callable=build_daily)
    monthly = PythonOperator(task_id="monthly_mart", python_callable=build_monthly)
    copy = PythonOperator(task_id="clickhouse_copy", python_callable=copy_clickhouse)
    check = PythonOperator(task_id="quality_checks", python_callable=check_marts)
    daily >> monthly >> check >> copy

