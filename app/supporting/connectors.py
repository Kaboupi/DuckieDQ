from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Tuple, Any
import logging
from logging_config.logging_config import setup_logging
import psycopg2 as pg

setup_logging()

class BaseConnector(ABC):
    __version: str = 'v0.0.1'
    
    def __init__(self, config: Dict[str, str]):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f'Version {self.version}')
        self.config = config
        self.connection = None
    
    @property
    def version(self) -> str:
        return self.__version
    
    @abstractmethod
    def connect(self) -> Any:
        ...
    
    @abstractmethod
    def get_uri(self) -> str:
        ...
        
    @abstractmethod
    def execute_query(self, query: str, params: tuple = ()) -> Any:
        ...
    
    def close(self) -> None:
        if self.connection:
            self.connection.close()
    
class PostgresConnector(BaseConnector):
    __version: str = 'v0.0.1'
    
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        
    def connect(self):
        self.logger.info(f'Connecting to postgres at {self.get_uri}')
        
        self.connection = pg.connect(
            host=self.config['host'],
            port=self.config['port'],
            dbname=self.config['database'],
            user=self.config['username'],
            password=self.config['password']
        )
        return self.connection
    
    def execute_query(self, query: str, params: tuple = ()) -> list:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            try:
                return cursor.fetchall()
            except pg.ProgrammingError:
                return []
    
    @property
    def get_uri(self) -> str:
        return f"postgresql://{self.config['username']}:{'*' * 5}@{self.config['host']}:{self.config['port']}/{self.config['database']}"


class ClickHouseConnector(BaseConnector):
    __version: str = 'v0.0.1'
    
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        
    @property
    def get_uri(self) -> str:
        return ''


class MySQLConnector(BaseConnector):
    __version: str = 'v0.0.1'
    
    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        
    @property
    def get_uri(self) -> str:
        return ''
    
