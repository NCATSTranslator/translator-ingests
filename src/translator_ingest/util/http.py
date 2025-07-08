# HTTP query wrappers

from typing import Dict
import requests
from json import JSONDecodeError
import logging
logger = logging.getLogger(__name__)


def post_query(url: str, query: Dict, params=None, server: str = "") -> Dict:
    """
    Post a JSON query to the specified URL and return the JSON response.

    :param url, str URL target for HTTP POST
    :param query, JSON query for posting
    :param params, optional parameters
    :param server, str human-readable name of server called (for error message reports)
    :return: Dict, JSON content response from the query (empty, posting a logging message, if unsuccessful)
    """
    try:
        if params is None:
            response = requests.post(url, json=query)
        else:
            response = requests.post(url, json=query, params=params)
    except Exception as ce:
        logging.error(f"URL {url} could not be accessed: {str(ce)}?")
        return dict()

    result: Dict = dict()
    err_msg_prefix: str = \
        f"post_query(): Server {server} at '\nUrl: '{url}', Query: '{query}' with parameters '{params}' -"
    if response.status_code == 200:
        try:
            result = response.json()
        except (JSONDecodeError, UnicodeDecodeError) as je:
            logging.error(f"{err_msg_prefix} response JSON could not be decoded: {str(je)}?")
    else:
        logger.error(f"{err_msg_prefix} returned HTTP error code: '{response.status_code}'")

    return result