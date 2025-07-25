"""
Translator Ingest Globally Shared Code and parameters
"""
from os.path import abspath, sep
from loguru import logger

PRIMARY_DATA_PATH = abspath(f"..{sep}..{sep}data")
logger.info("Primary data path: {}".format(PRIMARY_DATA_PATH))
