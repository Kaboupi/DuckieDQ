from sqlalchemy import create_engine
from typing import Optional, Dict, List, Tuple, Any
import clickhouse_connect
import logging
import pandas as pd
import psycopg2 as pg
import yaml

from .supporting.setup.logging_config import setup_logging
from .supporting.connection import Connection
from .supporting.parser import DQParser

setup_logging()

class DuckieDQ:
    __version: str = 'v0.0.1'
    
    def __init__(self, config_path: str) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f'Version {self.version}')
        
        self.configs: List[Dict[str, Any]] = DQParser(config_path).parse()
    
    def execute_dq(self):
        for config in self.configs:
            for task_name, task_values in config.items():
                self.logger.info(f"Start to perform '{task_name}' task.")
                conn_dict = task_values['conn_dict']
                conn_type = task_values['conn_type']
                conn_query = task_values['conn_query']
                
                conn = Connection(conn_dict, conn_type)
                conn.query(conn_query)
    
    
    @property
    def version(self) -> str:
        return self.__version