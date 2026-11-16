import json
import os
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time
from collections import Counter
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import urlsplit
from uuid import uuid4

import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
DS = '2026-09-10'


def command(*args, env=None, expected=0):
    result = subprocess.run(args, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=70)
    if result.returncode != expected:
        raise RuntimeError(f'{args[:3]} returned {result.returncode}, expected {expected}\n{result.stdout[-4000:]}')
    return result.stdout


def check_bad_key(tmp, pg):
    schema = 'korgonbaeva_ftt_' + uuid4().hex[:8]
    namespace = sql.Identifier(schema)
    table = sql.Identifier(schema, 'ods_sensors_history')
    clone = tmp / 'dbt'
    shutil.copytree(ROOT / 'dbt', clone, ignore=shutil.ignore_patterns('target', 'logs', 'dbt_packages'))
    source_file = clone / 'models/schema.yml'
    content = source_file.read_text()
    assert content.count('schema: korgonbaeva_ods') == 1
    source_file.write_text(content.replace('schema: korgonbaeva_ods', f'schema: {schema}'))
    env = dict(os.environ, PGDATABASE='aiacademy', DBT_USE_COLORS='false')
    args = ('dbt', 'test', '--project-dir', str(clone), '--profiles-dir', str(clone),
            '--select', 'source_unique_ods_ods_sensors_history_event_id')
    with pg.cursor() as cur:
        cur.execute(sql.SQL('CREATE SCHEMA {}').format(namespace))
        try:
            cur.execute(sql.SQL('CREATE TABLE {} (event_id text)').format(table))
            cur.execute('SELECT event_id FROM korgonbaeva_ods.ods_sensors_history ORDER BY event_id LIMIT 1')
            event_id, = cur.fetchone()
            cur.execute(sql.SQL('INSERT INTO {} (event_id) VALUES (%s), (%s)').format(table), (event_id, event_id))
            red = command(*args, env=env, expected=1)
            assert 'FAIL 1' in red and 'source_unique_ods_ods_sensors_history_event_id' in red, red
            print('FT-10 RED: unique(event_id) failed on exactly one duplicate key', flush=True)
            cur.execute(sql.SQL('DELETE FROM {} WHERE ctid IN (SELECT ctid FROM {} LIMIT 1)').format(table, table))
            green = command(*args, env=env)
            assert 'PASS' in green and 'PASS=1' in green, green
            print('FT-10 GREEN: the same dbt test passed after removing the duplicate', flush=True)
        finally:
            cur.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(table))
            cur.execute(sql.SQL('DROP SCHEMA {}').format(namespace))


def wait_until(callback, seconds, error):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = callback()
        if result:
            return result
        time.sleep(1)
    raise TimeoutError(error)


def check_api_failure(tmp, pg):
    calls = Counter()
    dag_id = 'final_korgonbaeva_api_failure_check_' + uuid4().hex[:8]
    sensors = [str(station['sensor_id']) for station in json.loads((ROOT / 'data/openaq_sources.json').read_text())['locations']
               if station['source'] == 'api_hours']
    assert len(sensors) >= 2

    class ApiFixture(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            path = urlsplit(self.path).path
            calls[path] += 1
            if path != f'/v3/sensors/{sensors[0]}/hours':
                self.send_error(503, 'Acceptance test: OpenAQ unavailable for second sensor')
                return
            body = json.dumps({'results': [{'value': 999.0,
                'parameter': {'name': 'pm25', 'units': 'µg/m³'},
                'period': {'datetimeFrom': {'utc': DS + 'T03:00:00Z'}}}]}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def snapshot(cur):
        cur.execute('SELECT to_jsonb(r)::text FROM korgonbaeva_stg.stg_api_hourly AS r WHERE _ds=%s ORDER BY 1', (DS,))
        return cur.fetchall()

    with pg.cursor() as cur, ThreadingHTTPServer(('127.0.0.1', 0), ApiFixture) as server:
        before = snapshot(cur)
        assert before, 'Run the successful 10 September baseline first'
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        airflow = tmp / 'airflow'
        dags = airflow / 'dags'
        dags.mkdir(parents=True)
        (dags / 'api_failure.py').write_text(f'''from datetime import timedelta
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.insert(0, {str(ROOT / 'dag')!r})
from final_pipeline import load_stage
with DAG({dag_id!r}, start_date=pendulum.datetime(2026,9,1,tz='UTC'), schedule=None, catchup=False):
    PythonOperator(task_id='stg_api_hourly', python_callable=load_stage,
                   op_kwargs={{'source': 'api_hourly'}}, retries=1, retry_delay=timedelta(seconds=1))
''')
        database = airflow / 'airflow.db'
        env = dict(os.environ, AIRFLOW_HOME=str(airflow), AIRFLOW_CONFIG=str(airflow / 'airflow.cfg'),
                   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f'sqlite:///{database}', AIRFLOW__CORE__DAGS_FOLDER=str(dags),
                   AIRFLOW__CORE__LOAD_EXAMPLES='False', AIRFLOW__CORE__EXECUTOR='SequentialExecutor',
                   AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='False', AIRFLOW__LOGGING__BASE_LOG_FOLDER=str(airflow / 'logs'),
                   OPENAQ_MANUAL_RUN='1', OPENAQ_API_BASE_URL=f'http://127.0.0.1:{server.server_port}/v3',
                   OPENAQ_API_KEY='acceptance-fixture-not-a-real-key',
                   AIRFLOW_CONN_AIACADEMY_PG_KORGONBAEVA=json.dumps(dict(conn_type='postgres', host=os.environ['PGHOST'],
                       port=int(os.environ.get('PGPORT', '5432')), schema='aiacademy', login=os.environ['PGUSER'],
                       password=os.environ['PGPASSWORD'])))
        try:
            command('airflow', 'db', 'migrate', env=env)
            with (airflow / 'scheduler.log').open('w') as log:
                process = subprocess.Popen(['airflow', 'scheduler'], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    with closing(sqlite3.connect(database, timeout=8)) as metadata:
                        wait_until(lambda: metadata.execute('SELECT 1 FROM serialized_dag WHERE dag_id=?', (dag_id,)).fetchone(),
                                   40, 'Acceptance DAG did not register')
                        command('airflow', 'dags', 'trigger', '--exec-date', DS + 'T00:00:00+00:00', dag_id, env=env)
                        def finished():
                            state = metadata.execute('SELECT state FROM dag_run WHERE dag_id=?', (dag_id,)).fetchone()
                            return state if state and state[0] in ('success', 'failed') else None
                        assert wait_until(finished, 100, 'Acceptance DAG did not finish') == ('failed',)
                        task = metadata.execute('SELECT state FROM task_instance WHERE dag_id=? AND task_id=?', (dag_id, 'stg_api_hourly')).fetchone()
                        assert task == ('failed',), task
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=12)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait(timeout=5)
        finally:
            server.shutdown()
            thread.join(timeout=5)
        cur.execute('SELECT status,row_count,error_text FROM korgonbaeva_stg.etl_load_log WHERE dag_id=%s ORDER BY id', (dag_id,))
        attempts = cur.fetchall()
        assert len(attempts) == 2 and all(status == 'failed' and count == 0 and '503' in error for status,count,error in attempts), attempts
        assert calls[f'/v3/sensors/{sensors[0]}/hours'] == 2 and calls[f'/v3/sensors/{sensors[1]}/hours'] == 6, calls
        after = snapshot(cur)
        assert before == after, 'STG changed after a partially successful API fetch'
        print(f'NFT-4 PASS: Airflow task failed; 2 task attempts, 6 HTTP 503 responses; all {len(after)} STG rows identical', flush=True)
        print('NFT-4 EVIDENCE DAG', dag_id, flush=True)


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='openaq-acceptance-') as temp, closing(psycopg2.connect(dbname='aiacademy')) as pg:
        pg.autocommit = True
        check_bad_key(Path(temp), pg)
        check_api_failure(Path(temp), pg)
