from getpass import getpass
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time

project = Path(__file__).resolve().parent
runtime = Path.home() / 'airflow-openaq'
runtime.mkdir(mode=0o700, exist_ok=True)
metadata = runtime / 'airflow.db'
main, mart = 'final_korgonbaeva_pipeline', 'final_korgonbaeva_datamart'
logical_date = sys.argv[1] if len(sys.argv) > 1 else '2026-09-10T00:00:00+00:00'
key = os.environ.get('OPENAQ_API_KEY') or getpass('OpenAQ API key (hidden): ')
env = dict(os.environ, AIRFLOW_HOME=str(runtime), AIRFLOW_CONFIG=str(runtime / 'airflow.cfg'),
           AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f'sqlite:///{metadata}',
           AIRFLOW__CORE__DAGS_FOLDER=str(project / 'dag'), AIRFLOW__CORE__LOAD_EXAMPLES='False',
           AIRFLOW__CORE__EXECUTOR='SequentialExecutor', AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='False',
           AIRFLOW__LOGGING__BASE_LOG_FOLDER=str(runtime / 'logs'), OPENAQ_MANUAL_RUN='1', OPENAQ_API_KEY=key,
           AIRFLOW_CONN_AIACADEMY_PG_KORGONBAEVA=json.dumps(dict(conn_type='postgres', host=os.environ['PGHOST'],
               port=int(os.environ.get('PGPORT', '5432')), schema='aiacademy', login=os.environ['PGUSER'],
               password=os.environ['PGPASSWORD'])))

def call(*args):
    return subprocess.check_output(['airflow', *args], env=env, text=True, stderr=subprocess.STDOUT, timeout=60)

def until(label, query, timeout):
    expires = time.monotonic() + timeout
    while time.monotonic() < expires:
        value = query()
        if value:
            return value
        time.sleep(2)
    raise TimeoutError(label)

print('Initializing isolated Airflow metadata', flush=True)
call('db', 'migrate')
call('db', 'check')
with (runtime / 'scheduler.log').open('a') as log:
    scheduler = subprocess.Popen(['airflow', 'scheduler'], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        with sqlite3.connect(metadata, timeout=8) as db:
            until('Both DAGs did not register', lambda: db.execute('SELECT count(*) FROM serialized_dag WHERE dag_id IN (?,?)', (main, mart)).fetchone()[0] == 2, 40)
            print('Both DAGs registered', flush=True)
            errors = db.execute('SELECT filename FROM import_error').fetchall()
            if errors:
                raise RuntimeError(f'DAG import errors: {errors}')
            print(call('dags', 'list', '-o', 'plain').strip(), flush=True)
            existing = db.execute('SELECT run_id,state FROM dag_run WHERE dag_id=? AND substr(execution_date,1,10)=?', (main,logical_date[:10])).fetchone()
            if existing:
                print('Retrying existing logical date:', existing, flush=True)
                call('tasks', 'clear', '-y', '-s', logical_date, '-e', logical_date, main)
            else:
                call('dags', 'trigger', '--exec-date', logical_date, '--run-id', f'acceptance_{logical_date[:10]}', main)
            print('Main DAG submitted for', logical_date, flush=True)
            def state(dag):
                row = db.execute('SELECT state,run_id FROM dag_run WHERE dag_id=? AND substr(execution_date,1,10)=?', (dag,logical_date[:10])).fetchone()
                return row if row and row[0] in ('success','failed') else None
            first = until('Main DAG did not finish', lambda: state(main), 180)
            print('PIPELINE', first, flush=True)
            if first[0] == 'success':
                second = until('Datamart DAG did not finish', lambda: state(mart), 240)
                print('DATAMART', second, flush=True)
            for row in db.execute('SELECT dag_id,task_id,state FROM task_instance WHERE dag_id IN (?,?) ORDER BY dag_id,task_id', (main,mart)):
                print('TASK', *row, flush=True)
    except subprocess.CalledProcessError as exc:
        print(exc.output, flush=True)
        raise
    finally:
        if scheduler.poll() is None:
            os.killpg(scheduler.pid, signal.SIGTERM)
            try:
                scheduler.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(scheduler.pid, signal.SIGKILL)
                scheduler.wait(timeout=5)

