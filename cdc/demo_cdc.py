from contextlib import closing
import os
import time

import psycopg2
import requests

from register_connectors import BASE, SINK, SOURCE

SENSOR_ID = 15138127


def check_connectors():
    healthy = True
    for name in (SOURCE, SINK):
        response = requests.get(f'{BASE}/connectors/{name}/status', timeout=5)
        response.raise_for_status()
        status = response.json()
        tasks = status['tasks']
        print(name, status['connector']['state'], [task['state'] for task in tasks], flush=True)
        healthy &= status['connector']['state'] == 'RUNNING' and bool(tasks) and all(t['state'] == 'RUNNING' for t in tasks)
        for task in tasks:
            if task.get('trace'):
                causes = [line.strip() for line in task['trace'].splitlines() if 'Caused by' in line]
                message = (causes[-1] if causes else task['trace'].splitlines()[0]).replace(os.environ['PGPASSWORD'], '<redacted>')
                print(message[:350], flush=True)
    return healthy


def wait_for_ods(cur, status, minimum_timestamp=None):
    started = time.monotonic()
    while time.monotonic() - started < 30:
        cur.execute('SELECT status, updated_at FROM korgonbaeva_ods.ods_sensor_status WHERE sensor_id=%s', (SENSOR_ID,))
        row = cur.fetchone()
        if row and row[0] == status and (minimum_timestamp is None or abs((row[1] - minimum_timestamp).total_seconds()) < 0.002):
            return time.monotonic() - started, row[1]
        time.sleep(0.2)
    check_connectors()
    raise TimeoutError(f'ODS did not receive {status!r} within 30 seconds. Last ODS row: {row!r}')


def run():
    if not check_connectors():
        raise RuntimeError('CDC connectors are not ready')
    with closing(psycopg2.connect(dbname='aiacademy')) as pg, pg.cursor() as cur:
        pg.autocommit = True
        cur.execute('SELECT status FROM korgonbaeva.sensor_status WHERE sensor_id=%s', (SENSOR_ID,))
        source = cur.fetchone()
        if source is None:
            cur.execute("INSERT INTO korgonbaeva.sensor_status (sensor_id,status) VALUES (%s,'maintenance')", (SENSOR_ID,))
            original = 'maintenance'
        else:
            original = source[0]
        _, captured = wait_for_ods(cur, original)
        print('INITIAL_ODS', SENSOR_ID, original, captured, flush=True)
        other = 'active' if original != 'active' else 'maintenance'
        for status in (other, original):
            cur.execute('UPDATE korgonbaeva.sensor_status SET status=%s, updated_at=clock_timestamp() WHERE sensor_id=%s RETURNING updated_at', (status, SENSOR_ID))
            timestamp, = cur.fetchone()
            elapsed, captured = wait_for_ods(cur, status, timestamp)
            print('CDC_DELIVERED', SENSOR_ID, status, f'{elapsed:.2f}s', captured, flush=True)


if __name__ == '__main__':
    run()
