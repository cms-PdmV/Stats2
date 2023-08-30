"""
Module that holds helper functions
"""
import json
import logging
import time
import os
import datetime
import urllib.parse
from connection_wrapper import ConnectionWrapper


__CONNECTION_WRAPPERS = {}
__ACCESS_TOKENS: dict[str, tuple[datetime.timedelta, datetime.datetime, str]] = {}
__PDMV_API_ACCESS_CREDENTIALS: dict[str, str] | None = None


def make_cmsweb_request(query_url, data=None, timeout=90, keep_open=True):
    """
    Make a request to https://cmsweb.cern.ch
    """
    return make_request("https://cmsweb.cern.ch", query_url, data, timeout, keep_open)


def make_cmsweb_prod_request(query_url, data=None, timeout=90, keep_open=True):
    """
    Make a request to https://cmsweb-prod.cern.ch
    """
    return make_request(
        "https://cmsweb-prod.cern.ch:8443", query_url, data, timeout, keep_open
    )


def make_cmsweb_request(query_url, data=None, timeout=90, keep_open=True):
    """
    Make a request to https://cmsweb.cern.ch
    """
    return make_request("https://cmsweb.cern.ch", query_url, data, timeout, keep_open)


def make_request(
    host: str,
    query_url: str,
    data: dict[str, str] | str | None = None,
    timeout: int = 90,
    keep_open: bool = True,
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-type": "application/json",
    },
) -> dict[str, str]:
    """
    Make a HTTP request. Use connection wrapper to keep connection alive
    and add necessary grid certificates for authentication
    """
    connection_wrapper_key = f"{host}___{timeout}___{keep_open}"
    connection_wrapper = __CONNECTION_WRAPPERS.get(connection_wrapper_key)
    if connection_wrapper is None:
        connection_wrapper = ConnectionWrapper(host, keep_open=keep_open)
        connection_wrapper.timeout = timeout
        __CONNECTION_WRAPPERS[connection_wrapper_key] = connection_wrapper

    method = "GET" if data is None else "POST"
    logger = logging.getLogger("logger")
    request_start_time = time.time()
    response = connection_wrapper.api(
        method=method,
        url=query_url,
        data=data,
        headers=headers,
    )
    request_finish_time = time.time()
    time_taken = request_finish_time - request_start_time
    if not data:
        logger.info(
            "%s request to %s%s took %.3fs", method, host, query_url, time_taken
        )
    else:
        logger.info(
            "%s request to %s%s with data \n%s\n took %.3fs",
            method,
            host,
            query_url,
            json.dumps(data, indent=2, sort_keys=True),
            time_taken,
        )

    return json.loads(response.decode("utf-8"))


def get_client_credentials() -> dict[str, str]:
    """
    This function retrieves the client credentials given
    via environment variables

    Returns:
        dict: Credentials required to request an access token via
            client credential grant

    Raises:
        RuntimeError: If there are environment variables that were not provided
    """
    required_variables = [
        "CALLBACK_CLIENT_ID",
        "CALLBACK_CLIENT_SECRET",
        "APPLICATION_CLIENT_ID",
        "DEV_APPLICATION_CLIENT_ID",
    ]
    credentials = {}
    msg = (
        "Some required environment variables are not available "
        "to send the callback notification. Please set them:\n"
    )
    for var in required_variables:
        value = os.getenv(var)
        if not value:
            msg += "%s\n" % var
            continue
        credentials[var] = value

    if len(credentials) == len(required_variables):
        logging.info("Returning OAuth2 credentials for requesting a token")
        return credentials

    logging.error(msg)
    raise RuntimeError(msg)


def __fetch_access_token(
    credentials: dict[str, str], audience: str
) -> dict[str, str | int]:
    """
    Request an access token to Keycloak (CERN SSO) via a
    client credential grant.

    Args:
        credentials (dict): Credentials required to perform a client credential grant
            Client ID, Client Secret
        audience (str): Target application for requesting the token.

    Returns:
        dict[str, str | int]: Access token to authenticate request to other PdmV services
            with its metadata.

    Raises:
        RuntimeError: If there is an issue requesting the access token
    """
    cern_api_access_host: str = "https://auth.cern.ch"
    cern_api_access_endpoint: str = "/auth/realms/cern/api-access/token"
    client_id = credentials["CALLBACK_CLIENT_ID"]
    client_secret = credentials["CALLBACK_CLIENT_SECRET"]
    url_encoded_data: dict = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
    }
    url_encoded_data: str = urllib.parse.urlencode(query=url_encoded_data)
    headers: dict[str, str] = {
        "Content-type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    response: dict[str, str] = make_request(
        host=cern_api_access_host,
        query_url=cern_api_access_endpoint,
        data=url_encoded_data,
        headers=headers,
    )
    token_response: dict[str, str] = response
    token: str = token_response.get("access_token", "")
    if not token:
        token_error = "Invalid access token request. Details: %s" % token_response
        logging.error(token_error)
        raise RuntimeError(token_error)

    return token_response


def get_access_token(credentials: dict[str, str], production: bool = True) -> str:
    """
    Retrieves an access token to send via the Authorization header
    to authenticate one request to another service.

    Args:
        credentials (dict): Credentials required to perform a client credential grant
            Client ID, Client Secret and target applications (audience) if required
        production (bool): If True, this will request an access token for the production application
            else to the development one.

    Returns:
        str: Authorization header to send into HTTP request.

    Raises:
        RuntimeError: If there is an issue requesting the access token
    """
    # Check if we have already a valid token
    audience = (
        credentials["APPLICATION_CLIENT_ID"]
        if production
        else credentials["DEV_APPLICATION_CLIENT_ID"]
    )
    access_token: tuple[
        datetime.timedelta, datetime.datetime, str
    ] | None = __ACCESS_TOKENS.get(audience)
    if access_token:
        # Check if the token is valid, if so return it
        valid_delta, requested_time, token = access_token
        current_time: datetime.datetime = datetime.datetime.now()
        elapsed_time: datetime.timedelta = current_time - requested_time
        if elapsed_time < valid_delta:
            return f"Bearer {token}"

    # Request a new access token and store it
    requested_time: datetime.datetime = datetime.datetime.now()
    access_token_response: dict[str, str | int] = __fetch_access_token(
        credentials=credentials
    )
    token: str = access_token_response["access_token"]
    valid_delta: datetime.timedelta = datetime.timedelta(
        seconds=int(access_token_response["expires_in"] * 0.75)
    )
    __ACCESS_TOKENS[audience] = (valid_delta, requested_time, token)
    return f"Bearer {token}"


def pick_attributes(old_dict, attributes, skip_non_existing=True):
    """
    Pick requested key value pairs from a dictionary and return a new dictionary
    """
    new_dict = {}
    for attribute in attributes:
        if attribute in old_dict:
            new_dict[attribute] = old_dict[attribute]
        elif not skip_non_existing:
            new_dict[attribute] = None

    return new_dict


def setup_console_logging():
    """
    Set default logging format and level
    """
    log_format = "[%(asctime)s][%(levelname)s] %(message)s"
    logging.basicConfig(format=log_format, level=logging.INFO)


def get_unique_list(input_list):
    """
    Return a new list of unique elemets only while preserving original order
    """
    new_list = []
    for element in input_list:
        if element not in new_list:
            new_list.append(element)

    return new_list


def get_nice_size(size, base=1000.0):
    """
    Convert number of bytes to string with KB, MB, GB, TB suffixes
    """
    if size < base:
        return "%sB" % (size)

    if size < base**2:
        return "%.2fKB" % (size / base)

    if size < base**3:
        return "%.2fMB" % (size / base**2)

    if size < base**4:
        return "%.2fGB" % (size / base**3)

    return "%.2fTB" % (size / base**4)


def comma_separate_thousands(number):
    return "{:,}".format(int(number))
