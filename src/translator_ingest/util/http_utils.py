# HTTP query wrappers

import requests
import datetime
from ftplib import FTP
from json import JSONDecodeError
from email.utils import parsedate_to_datetime

from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)


def post_query(url: str, query: dict, params=None, server: str = "") -> dict:
    """
    Post a JSON query to the specified URL and return the JSON response.

    :param url, str URL target for HTTP POST
    :param query, JSON query for posting
    :param params, optional parameters
    :param server, str human-readable name of server called (for error message reports)
    :return: dict, JSON content response from the query (empty, posting a logging message, if unsuccessful)
    """
    try:
        if params is None:
            response = requests.post(url, json=query)
        else:
            response = requests.post(url, json=query, params=params)
    except Exception as ce:
        logger.error(f"URL {url} could not be accessed: {str(ce)}?")
        return dict()

    result: dict = dict()
    err_msg_prefix: str = (
        f"post_query(): Server {server} at '\nUrl: '{url}', Query: '{query}' with parameters '{params}' -"
    )
    if response.status_code == 200:
        try:
            result = response.json()
        except (JSONDecodeError, UnicodeDecodeError) as je:
            logger.error(f"{err_msg_prefix} response JSON could not be decoded: {str(je)}?")
    else:
        logger.error(f"{err_msg_prefix} returned HTTP error code: '{response.status_code}'")

    return result


def get_modify_date(file_url, str_format: str = "%Y_%m_%d") -> str:
    r = requests.head(file_url)
    r.raise_for_status()
    url_time = r.headers['last-modified']
    # using parsedate_to_datetime from email.utils instead of datetime.strptime because it is designed to parse
    # this specific format and apparently handles timezones better
    modified_datetime = parsedate_to_datetime(url_time)
    return modified_datetime.strftime(str_format)

def get_ftp_modify_date(ftp_url: str, ftp_dir: str, ftp_file: str, str_format: str = "%Y_%m_%d") -> str:
    """
    Get the modification date of a file on an FTP server.

    :param ftp_url: FTP server hostname
    :param ftp_dir: Directory path on the FTP server
    :param ftp_file: Filename to check
    :param str_format: Output date format string
    :return: Formatted modification date string
    """
    with FTP(ftp_url) as ftp:
        ftp.login()
        ftp.cwd(ftp_dir)
        mdtm_response = ftp.voidcmd(f'MDTM {ftp_file}')
        response_code, modification_timestamp = mdtm_response.split()
        if response_code != "213":
            raise RuntimeError(f'Non-213 response from ftp server: {response_code}')
        modification_datetime = datetime.datetime.strptime(modification_timestamp, '%Y%m%d%H%M%S')
        return modification_datetime.strftime(str_format)