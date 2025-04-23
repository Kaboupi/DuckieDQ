"""
Class to keep the connection to db
"""
from typing import Optional, Dict, Tuple, Any, Type
import logging
from .setup.logging_config import setup_logging
from .connectors import BaseConnector, PostgresConnector, ClickHouseConnector, MySQLConnector

setup_logging()

CONNECTOR_CLASSES: Dict[str, Type[BaseConnector]] = {
    'postgres'   : PostgresConnector,
    'clickhouse' : ClickHouseConnector,
    'mysql'      : MySQLConnector,
}

class Connection:
    __version: str = 'v0.0.1'
    
    def __init__(self, db_params: Dict[str, str], conn_type: str) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f'Using {self.__class__.__name__} version {self.version}')
        
        self.db_params = db_params
        self.conn_type = conn_type.lower()
                
        self._validate_required('host', db_params.get('host'))
        self._validate_required('port', db_params.get('port'))
        self._validate_required('username', db_params.get('username'))
        self._validate_required('database', db_params.get('database'))
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
    
    def close(self):
        self.connector.close()
    
    def query(self, sql: str, params: tuple = ()) -> Any:
        self.connector.connect()
        self.logger.info(f"Executing query: '{sql}' {'with params: ' + params if params else ''}")
        result = self.connector.execute_query(sql, params)
        self.connector.close()
        return result