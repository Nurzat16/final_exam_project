import os
import sys
import psycopg2
import requests

BASE = os.getenv('KAFKA_CONNECT_URL', 'http://kafka-connect:8083').rstrip('/')
SOURCE = 'sensor_status-source-korgonbaeva'
SINK = 'sink-korgonbaeva'
PUBLICATION = 'korgonbaeva_openaq_status_pub'
TOPIC_PREFIX = 'korgonbaeva_openaq'
TOPIC = f'{TOPIC_PREFIX}.korgonbaeva.sensor_status'


def ensure_publication():
    with psycopg2.connect(dbname='aiacademy') as pg, pg.cursor() as cur:
        cur.execute('SELECT 1 FROM pg_publication WHERE pubname=%s', (PUBLICATION,))
        if cur.fetchone() is None:
            cur.execute('CREATE PUBLICATION korgonbaeva_openaq_status_pub FOR TABLE korgonbaeva.sensor_status')
        cur.execute('SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname=%s', (PUBLICATION,))
        if cur.fetchall() != [('korgonbaeva', 'sensor_status')]:
            raise RuntimeError('Unexpected publication table list')


def register(replace=False):
    ensure_publication()
    host, user, password = (os.environ[k] for k in ('PGHOST', 'PGUSER', 'PGPASSWORD'))
    port = os.environ.get('PGPORT', '5432')
    common = {'tasks.max': '1'}
    for side in ('key', 'value'):
        common[f'{side}.converter'] = 'org.apache.kafka.connect.json.JsonConverter'
        common[f'{side}.converter.schemas.enable'] = 'true'
    source = dict(common, **{
        'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
        'database.hostname': host, 'database.port': port, 'database.user': user,
        'database.password': password, 'database.dbname': 'aiacademy',
        'topic.prefix': TOPIC_PREFIX, 'plugin.name': 'pgoutput',
        'slot.name': 'korgonbaeva_openaq_status', 'publication.name': PUBLICATION,
        'publication.autocreate.mode': 'disabled', 'table.include.list': r'korgonbaeva\.sensor_status',
        'snapshot.mode': 'initial', 'heartbeat.interval.ms': '5000',
    })
    sink = dict(common, **{
        'connector.class': 'io.debezium.connector.jdbc.JdbcSinkConnector', 'topics': TOPIC,
        'connection.url': f'jdbc:postgresql://{host}:{port}/aiacademy?currentSchema=korgonbaeva_ods',
        'connection.username': user, 'connection.password': password,
        'table.name.format': 'ods_sensor_status', 'insert.mode': 'upsert',
        'primary.key.mode': 'record_key', 'primary.key.fields': 'sensor_id',
        'delete.enabled': 'true', 'schema.evolution': 'none',
    })
    with requests.Session() as client:
        response = client.get(f'{BASE}/connectors', timeout=8)
        response.raise_for_status()
        names = set(response.json())
        for name, config in ((SOURCE, source), (SINK, sink)):
            if name in names and not replace:
                print(name, 'already registered', flush=True)
                continue
            if name in names:
                result = client.put(f'{BASE}/connectors/{name}/config', json=config, timeout=15)
            else:
                result = client.post(f'{BASE}/connectors', json={'name': name, 'config': config}, timeout=15)
            if not result.ok:
                message = result.json().get('message', '').replace(password, '<redacted>')
                raise RuntimeError(f'{name}: HTTP {result.status_code}: {message[:400]}')
            print(name, 'registered', flush=True)


if __name__ == '__main__':
    register(replace='--replace' in sys.argv[1:])
