import logging

import requests


def get_request(
    url: str, payload: dict = None, connection_timeout=3, read_timeout=5, verify=True
):
    headers = headers = {
        "accept": "application/json",
        # "content-type": "text/csv",
        # "x-api-version": "2",
        # "auth": session_id,
    }

    if url.startswith("http://"):
        verify = False

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


def get_request_with_json_resp_as_dict(
    url: str,
    payload: dict,
    connection_timeout=3,
    read_timeout=5,
) -> dict:
    response = get_request(
        url=url,
        payload=payload,
        connection_timeout=connection_timeout,
        read_timeout=read_timeout,
    )
    return response.json()


def get_request_with_text_resp_as_dict(url: str, payload: dict) -> dict:
    response = get_request(url=url, payload=payload)
    return response.text()
