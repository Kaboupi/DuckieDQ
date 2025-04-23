from DuckieDQ.supporting.connection import Connection
from DuckieDQ import DuckieDQ

if __name__ == '__main__':
    db_postgres_params = {
        'host': 'localhost',
        'port': '5445',
        'database': 'postgres_db',
        'schema': 'default',
        'username': 'postgres_user',
        'password': 'postgres_password',
    }
    conn = Connection(db_postgres_params, 'postgres')
    conn.connect()
    conn.query('SELECT * FROM pg_namespace;')
    conn.close()
    
    db_clickhouse_params = {
        'host': 'localhost',
        'port': '8123',
        'database': 'default',
        'username': 'admin',
        'password': 'admin',
    }
    conn_2 = Connection(db_clickhouse_params, 'clickhouse')
    conn_2.query('SELECT * from system.query_log limit 1')
    conn_2.query('SELECT 1;')