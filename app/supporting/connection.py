"""
Class to keep the connection to db
"""
from typing import Optional, Dict, List, Tuple, Any, Type
import logging
from logging_config.logging_config import setup_logging
from connectors import BaseConnector, PostgresConnector, ClickHouseConnector, MySQLConnector

setup_logging()

CONNECTOR_CLASSES: Dict[str, Type[BaseConnector]] = {
    'postgres': PostgresConnector,
    'clickhouse': ClickHouseConnector,
    'mysql': MySQLConnector
}

class Connection:
    __version: str = 'v0.0.1'
    
    def __init__(self, db_params: Dict[str, str], conn_type: str) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f'Version {self.version}')
        
        self.db_params = db_params
        self.conn_type = conn_type.lower()
                
        self._validate_required('host', db_params.get('host'))
        self._validate_required('port', db_params.get('port'))
        self._validate_choice('conn_type', self.conn_type, self.supported_connectors)
        
        self.connector: BaseConnector = self._instantiate_connector()

    def _validate_required(self, field_name: str, value: Optional[str]) -> None:
        if not value:
            raise ValueError(f"Missing required connection parameter: '{field_name}'")
        
    def _validate_choice(self, field_name: str, value: str, allowed: Tuple[str, ...]) -> None:
        if value not in allowed:
            raise ValueError(f"Invalid {field_name}: '{value}', allowed: {allowed}")
                
    def _instantiate_connector(self) -> BaseConnector:
        connector_class = CONNECTOR_CLASSES.get(self.conn_type, None)
        if not connector_class:
            raise ValueError(f"No connector available for '{self.conn_type}' connection.")
        return connector_class(self.db_params)

    @property
    def supported_connectors(self) -> Tuple[str, ...]:
        return tuple(CONNECTOR_CLASSES.keys())
    
    @property
    def version(self) -> str:
        return self.__version
    
    def connect(self):
        return self.connector.connect()
    
    def query(self, sql: str, params: tuple = ()) -> Any:
        self.connect()
        return self.connector.execute_query(sql, params)
    

if __name__ == '__main__':
    db_params = {
        'host': 'db-postgres',
        'port': '5432',
        'database': 'postgres_db',
        'schema': 'default',
        'username': 'postgres_user',
        'password': 'postgres_password',
    }
    conn = Connection(db_params, 'postgres')
    conn.query('SELECT 1;')
    