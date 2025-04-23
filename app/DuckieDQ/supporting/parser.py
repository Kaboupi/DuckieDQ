from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Tuple, Any
import logging
from .setup.logging_config import setup_logging
import psycopg2 as pg
import clickhouse_connect

setup_logging()

class DQParser:
    def __init__(self, config_path: str):
        self.config_path = config_path
        if self.config_path.endswith('.yaml')

    def _parse_configs(self):
        pass
        
    def _parse_config(self):
        pass
    