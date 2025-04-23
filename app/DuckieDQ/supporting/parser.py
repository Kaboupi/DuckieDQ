from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Tuple, Any, Set
import logging
from .setup.logging_config import setup_logging
import psycopg2 as pg
import clickhouse_connect
import os
import yaml
from pathlib import Path

setup_logging()

class DQParser:
    __version: str = 'v0.0.1'
    
    def __init__(self, config_path: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f'Version {self.version}')
        
        self.configs: List[Dict[str, Any]] = []
        self.config_path = Path(config_path).resolve()
        
        if self.config_path.is_file() and self.config_path.suffix in ('.yaml', '.yml'):
            self.logger.info(f'Start to read config {self.config_path}')
            self.configs.append(self._parse_config(self.config_path))
        elif self.config_path.is_dir():
            self.logger.info(f'Start to scan dir {self.config_path} for YAML/YML files.')
            for config_file in self.config_path.glob('*.y*ml'):
                self.configs.append(self._parse_config(config_file))
        else:
            self.logger.warning(f'Provided path {self.config_path} is invalid or has no YAML/YML files.')
            
    def _parse_config(self, config_path):
        try:
            with config_path.open('r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                self.logger.info(f"Successfully read config '{config_path}'")
        except Exception as e:
            self.logger.error(f'Failed to load config {config_path}: {e}')
        
        task_list = config.get('tasks', None)
        if not task_list:
            raise ValueError(f'File {config_path} has no list of task_list.')
        
        db_config = {}
        for task_name, task_values in task_list.items():
            version, spec = task_values.get('version'), task_values.get('spec')
            asset, params = spec['asset'], spec['params']
            
            db_config[task_name] = {
                'conn_dict': asset.get('connection'),
                'conn_type': asset.get('type'),
                'conn_query': params.get('query'),
                '_uniq_uuid': asset.get('key'),
            }
        
        return db_config
    
    def parse(self):
        return self.configs 
    
    @property
    def version(self) -> str:
        return self.__version