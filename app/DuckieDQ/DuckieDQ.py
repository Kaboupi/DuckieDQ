from .supporting.setup.logging_config import setup_logging
from sqlalchemy import create_engine
from typing import Optional, Dict, List, Tuple, Any
import clickhouse_connect
import logging
import pandas as pd
import psycopg2 as pg
import yaml

from .supporting.connectors import PostgresConnector, ClickHouseConnector


class DuckieDQ:
    def __init__(self, source: dict, target: dict) -> None:
        pass
    
    def run(self):
        pass