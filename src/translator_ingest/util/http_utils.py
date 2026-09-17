# HTTP query wrappers

import re
import requests
import datetime
from ftplib import FTP
from json import JSONDecodeError
from email.utils import parsedate_to_datetime

from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)

# Public Gene Ontology release metadata endpoint, shared by the GO-CAM and GOA ingests.
GENEONTOLOGY_RELEASE_METADATA_URL = "https://current.geneontology.org/metadata/release-date.json"
_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _extract_iso_date(text: str) -> str | None:
    """Extract the first ``YYYY-MM-DD`` date from a string, or ``None`` if there isn't one.

    The GO metadata endpoint returns malformed pseudo-JSON with an unquoted key and value,
    so we can't use ``json.loads``; a plain regex extraction is robust to that.

    >>> _extract_iso_date("{date: 2026-06-19}")
    '2026-06-19'
    >>> _extract_iso_date('{"date": "2026-06-19"}')
    '2026-06-19'
    >>> _extract_iso_date("no date here") is None
    True
    """
    match = _ISO_DATE_RE.search(text)
    return match.group(0) if match else None


def get_geneontology_release_version(url: str = GENEONTOLOGY_RELEASE_METADATA_URL) -> str:
    """Fetch the current Gene Ontology release date (YYYY-MM-DD).

    The GO metadata endpoint returns malformed pseudo-JSON with an unquoted key and value
    (e.g. ``{date: 2026-06-19}``), so ``response.json()`` raises ``JSONDecodeError``. We extract
    the ISO date from the raw response text instead of parsing it as JSON.

    :param url: GO release metadata endpoint (defaults to the public ``release-date.json``)
    :return: the release date as a ``YYYY-MM-DD`` string
    :raises RuntimeError: if the endpoint is unreachable or contains no ISO date
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Unable to retrieve GO release metadata from {url}") from exc

    version = _extract_iso_date(response.text)
    if not version:
        raise RuntimeError(f"GO metadata from {url} did not contain a release date: {response.text!r}")

    return version


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