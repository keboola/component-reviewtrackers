import requests
import base64
import json

BASE_URL = "https://api.reviewtrackers.com/"


def _build_headers(username, token):
    auth_string = "{}:{}".format(username, token)
    encoded = base64.b64encode(auth_string.encode())
    encoded = encoded.decode("utf-8")
    return {
        "Authorization": "Basic {}".format(encoded),
        "Content-Type": "application/json; charset=utf-8",
        'Accept': "application/vnd.rtx.campaign.v2.hal+json;charset=utf-8",
    }


def request_endpoint(username, token, endpoint, params=None):
    headers = _build_headers(username, token)
    res = requests.get(url=BASE_URL + endpoint, headers=headers, params=params)

    res = json.loads(res.text)
    # todo: pagination
    # todo: children objects
    # todo: json to csv
    return res
