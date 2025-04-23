"""*
*                       DuckieDQ v0.0.1
*------------------------------------------------------------
*  🦆 Data Quality Framework for validating your pipelines
*
*  🔍 List of supported databases:
*     - PostgreSQL
*     - ClickHouse
*     - MySQL
*     - SQLite
*  📄 YAML Configuration files
*  🧪 SQL-queries for validation 
*"""
from typing import  Dict, List, Any
import logging
import pandas as pd

from ..config.logging_config import setup_logging
from .connection import Connection
from .parser import DQParser

setup_logging()

class DuckieDQ:
    __version: str = 'v0.0.1'
    
    def __init__(self, config_path: str) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f'Using {self.__class__.__name__} version {self.version}')
        
        for line in __doc__.split('\n'):
            self.logger.info(line)
        
        self.df_list: list = []
        self.configs: List[Dict[str, Any]] = DQParser(config_path).parse()
        
        for config in self.configs:
            for config_item, config_value in config.items():
                self.logger.debug(f"Start to perform '{config_item}' task.")
                data, columns = self._execute_dq(config_value)
                self.df_list.append(pd.DataFrame(data, columns=columns))
        # self.logger.debug(self.df_list)
            
    def _execute_dq(self, config: Dict[str, Any]):
        conn_dict = config.get('conn_dict')
        conn_type = config.get('conn_type')
        conn_query = config.get('conn_query')
        conn_uuid = config.get('_uniq_uuid')
        
        conn = Connection(conn_dict, conn_type)
        data, cols = conn.query(conn_query)

        self.logger.info(f'Success! Number of rows retrieved: {len(data)}.')
        return (data, cols)
    
    def _print_header(self) -> None:
        for doc_line in __doc__.split('\n'):
            self.logger.info(doc_line)
    
    @property
    def version(self) -> str:
        return self.__version