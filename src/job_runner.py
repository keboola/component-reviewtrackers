import json
import logging
import os
import sys
import warnings
import requests
from requests.auth import HTTPBasicAuth
from service.api_client import request_endpoint, request_reviews_v2, request_accounts

warnings.simplefilter(action='ignore', category=FutureWarning)


def _auth(username, password):
    """
    Basic Authorization to Server
    """
    url = 'https://api-gateway.reviewtrackers.com/auth'
    headers = {
        'Accept': "application/vnd.rtx.authorization.v2.hal+json;charset=utf-8",
        "Content-Type": "application/json"
    }

    res = requests.post(url=url, headers=headers,
                        auth=HTTPBasicAuth(username, password))

    auth_res = json.loads(res.text)
    # logging.info("Authorization Return: {0}".format(auth_res))
    if "error" in auth_res:
        logging.error("{0}: {1}".format(auth_res["error"], auth_res["status"]))
        sys.exit(1)

    return {
        'token': auth_res.get('token')
    }


def _read_state():
    """
    Return the last page Ex requested
    """

    if os.path.isfile("/data/in/state.json"):
        # Fetching refresh token from state file
        logging.info("Fetched State file...")
        with open("/data/in/state.json", 'r') as f:
            temp = json.load(f)
        logging.info("Extractor State: {0}".format(temp))

    else:
        temp = {}
        logging.info("No State file is found.")

    return temp


def _write_state(data_in):
    """
    Updating state file
    """

    logging.info("Outputting State file...")
    logging.info("Output State: {0}".format(data_in))
    with open("/data/out/state.json", "w") as f:
        json.dump(data_in, f)

    return


def run(ui_username, ui_password, ui_clear_state):
    """
    Main Executor for Job_runner
    """

    # Hardcoding the list of endpoints
    ui_endpoints = ["locations", "reviews", "responses"]

    # Authentication
    auth_res = _auth(username=ui_username, password=ui_password)
    token = auth_res.get('token')

    # last_update_time = _get_last_update_time(tables=ui_tables)

    # State File fetch
    logging.info("Clear State: {0}".format(ui_clear_state))
    if not ui_clear_state:
        ex_state = _read_state()
    else:
        logging.info("Clearing State File...")
        ex_state = {}

    accounts = request_accounts(ui_username, token)

    for endpoint in ui_endpoints:
        if endpoint not in ex_state:
            ex_state[endpoint] = {}

        for account in accounts:
            logging.info(f"fetching endpoint {endpoint} for account {account} ...")
            file_name = endpoint
            if endpoint == 'reviews':
                json_res, ex_state_new = request_reviews_v2(
                    ui_username, token, ex_state, endpoint, file_name, account)
            else:
                json_res, ex_state_new = request_endpoint(
                    ui_username, token, ex_state, endpoint, file_name, account)
            if json_res == 404:
                logging.warning(
                    "Endpoint [{}] not found, 404 Error".format(endpoint))
                continue

            ex_state[endpoint][account] = ex_state_new

        # State File Content after 1 Endpoint extraction
        logging.info("Extractor State: {0}".format(ex_state))

    # State File Out
    _write_state(ex_state)

    return
