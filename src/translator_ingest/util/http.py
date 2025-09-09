# HTTP query wrappers

from urllib3 import request, Retry, BaseHTTPResponse

from json import JSONDecodeError
import logging
logger = logging.getLogger(__name__)

NUMBER_OF_RETRIES = 5
RETRIES = Retry(
    total=NUMBER_OF_RETRIES,
    backoff_factor=0.1,
    status_forcelist=[502, 503, 504],
    allowed_methods={'POST'},
)

def post_query(url: str, query: dict, server: str = "") -> dict:
    """
    Post a JSON query to the specified URL and return the JSON response.

    :param url, str URL target for HTTP POST
    :param query, JSON query for posting
    :param server, str human-readable name of server called (for error message reports)
    :return: dict, JSON content response from the query (empty, posting a logging message, if unsuccessful)
    """
    try:
        response: BaseHTTPResponse = request(
            method="POST",
            url=url,
            json=query,
            retries=RETRIES
        )
    except Exception as ce:
        logging.error(f"URL {url} could not be accessed: {str(ce)}?")
        return dict()

    result: dict = dict()
    err_msg_prefix: str = \
        f"post_query(): Server {server} at '\nUrl: '{url}', Query: '{query}' -"
    if response.status == 200:
        try:
            result = response.json()
        except (JSONDecodeError, UnicodeDecodeError) as je:
            logging.error(f"{err_msg_prefix} response JSON could not be decoded: {str(je)}?")
    else:
        logger.error(f"{err_msg_prefix} returned HTTP error code: '{response.status}'")

    return result
