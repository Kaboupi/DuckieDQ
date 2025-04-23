from typing import  Dict, List, Any
import logging

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
                result = conn.query(conn_query)
                self.logger.info(result)
    
    @property
    def version(self) -> str:
        return self.__version