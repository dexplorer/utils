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
