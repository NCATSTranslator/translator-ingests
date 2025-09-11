"""
Translator Ingest Globally Shared Code and parameters
"""
from os.path import abspath, dirname, join, sep
from loguru import logger

TI_PACKAGE_PATH = abspath(dirname(__file__))
PRIMARY_DATA_PATH = join(TI_PACKAGE_PATH, f"..{sep}..{sep}data")
logger.info("Primary data path: {}".format(PRIMARY_DATA_PATH))
