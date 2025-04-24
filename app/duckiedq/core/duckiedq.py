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
        
        self._print_header()
        
        self.df_list: list = []
        self.configs: List[Dict[str, Any]] = DQParser(config_path).parse()
        
        for config in self.configs:
            for task_name, task_config in config.items():
                self.df_list.append(self._read_and_transform(task_name, task_config))
            
    def _read_and_transform(self, task_name: str, config: Dict[str, Any]) -> pd.DataFrame:
        self.logger.info(f"Executing task: '{task_name}'")
        conn_dict = config.get('conn_dict')
        conn_type = config.get('conn_type')
        conn_query = config.get('conn_query')
        conn_dim_date = config.get('_dim_date')
         
        conn = Connection(conn_dict, conn_type)
        data, columns = conn.query(conn_query)
        columns = [f'{task_name}__{col}' if col != conn_dim_date else col for col in columns]
        
        self.logger.info(f'Retrieved {len(data)} rows for.')

        df = pd.DataFrame(data, columns=columns)
        df.set_index([conn_dim_date], inplace=True)
        return df
    
    def _print_header(self) -> None:
        doc = self.__doc__ or ''
        for line in doc.strip().splitlines():
            self.logger.info(line)
        
    @property
    def get_df(self):
        return self.df_list
    
    @property
    def version(self) -> str:
        return self.__version