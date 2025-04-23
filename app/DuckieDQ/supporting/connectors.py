from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple
import logging
from .setup.logging_config import setup_logging
import psycopg2 as pg
from psycopg2.extensions import string_types
import clickhouse_connect

setup_logging()

class BaseConnector(ABC):
    __version: str = 'v0.0.1'
    
    def __init__(self, config: Dict[str, str]):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f'Using {self.__class__.__name__} version {self.version}')
        self.config = config
        self.connection = None
    
    @property
    def version(self) -> str:
        return self.__version
    
    @abstractmethod
    def connect(self) -> Any:
        ...
    
    @abstractmethod
    def uri(self) -> str:
        ...
        
    @abstractmethod
    def execute_query(self, query: str, params: tuple = ()) -> Any:
        ...
    
    def close(self) -> None:
        if self.connection:
            self.connection.close()
    
class PostgresConnector(BaseConnector):
    __version: str = 'v0.0.1'
        
    def connect(self):
        self.logger.debug(f'Connecting to postgres at {self.uri}')
        
        self.connection = pg.connect(
            host=self.config.get('host'),
            port=self.config.get('port'),
            dbname=self.config.get('database'),
            user=self.config.get('username'),
            password=self.config.get('password')
        )
        return self.connection
    
    def execute_query(self, query: str, params: tuple = ()) -> Tuple[List[str], List[str]]:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            try:
                return (cursor.fetchall(), [col.name for col in cursor.description])
            except pg.ProgrammingError:
                return ()
    
    @property
    def uri(self) -> str:
        return f"postgresql://{self.config.get('username')}:{'*' * 5}@{self.config.get('host')}:{self.config.get('port')}/{self.config.get('database')}"


class ClickHouseConnector(BaseConnector):
    __version: str = 'v0.0.1'
        
    def connect(self):
        self.logger.debug(f'Connecting to clickhouse at {self.uri}')
        
        self.connection = clickhouse_connect.get_client(
            host=self.config.get('host'),
            port=int(self.config.get('port')),
            username=self.config.get('username'),
            password=self.config.get('password'),
            database=self.config.get('database') or 'default',
        )
        return self.connection
            
    def execute_query(self, query: str, params: tuple = ()) -> Tuple[List[str], List[str]]:
        result = self.connection.query(query)
        return (result.result_rows, result.column_names)
        
    @property
    def uri(self) -> str:
        return f'jdbc:clickhouse://{self.config.get('host')}:{self.config.get('port')}@{self.config.get('username')}:{'*' * 5}'


class MySQLConnector(BaseConnector):
    __version: str = 'v0.0.1'
        
    @property
    def uri(self) -> str:
        return ''
    
