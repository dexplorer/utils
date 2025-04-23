import requests
import logging


def get_http_response(url: str, connection_timeout=3, read_timeout=5):
    try:
        response = requests.get(url, timeout=(connection_timeout, read_timeout))
        response.raise_for_status()
        # print(response)
    except requests.exceptions.HTTPError as err:
        logging.error(err)
    except requests.exceptions.ConnectionError as err:
        logging.error(err)
    except requests.exceptions.Timeout as err:
        logging.error(err)
    except requests.exceptions.RequestException as err:
        logging.error(err)

    return response


def get_request(url: str, payload: dict):
    headers = headers = {
        "accept": "application/json",
        # "content-type": "text/csv",
        # "x-api-version": "2",
        # "auth": session_id,
    }
    verify = False
    connection_timeout = 3
    read_timeout = 5

    try:
        response = requests.get(
            url=url,
            params=payload,
            headers=headers,
            verify=verify,
            timeout=(connection_timeout, read_timeout),
        )
        response.raise_for_status()
        # print(response)
    except requests.exceptions.HTTPError as err:
        logging.error(err)
    except requests.exceptions.ConnectionError as err:
        logging.error(err)
    except requests.exceptions.Timeout as err:
        logging.error(err)
    except requests.exceptions.RequestException as err:
        logging.error(err)

    return response


def get_request_with_json_resp_as_dict(url: str, payload: dict) -> dict:
    response = get_request(url=url, payload=payload)
    return response.json()


def get_request_with_text_resp_as_dict(url: str, payload: dict) -> dict:
    response = get_request(url=url, payload=payload)
    return response.text()
