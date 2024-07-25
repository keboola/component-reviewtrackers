import requests
import base64
import json
import logging
import copy
import sys

from service.parser import parse

BASE_URL = "https://api.reviewtrackers.com/"
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"


def _build_headers(username, token):
    auth_string = "{}:{}".format(username, token)
    encoded = base64.b64encode(auth_string.encode())
    encoded = encoded.decode("utf-8")
    return {
        "Authorization": "Basic {}".format(encoded),
        "Content-Type": "application/json; charset=utf-8",
        'Accept': "application/vnd.rtx.campaign.v2.hal+json;charset=utf-8",
    }


def request_reviews_v2(username, token, state_file, endpoint, file_name, account):
    """
    Request new review endpoint with provided parameters
    """

    entities = []
    headers = _build_headers(username, token)
    request_url = 'v2/{}'.format(endpoint)
    params = {'account_id': account, 'per_page': 500, 'sort[by]': 'published_at', 'sort[order]': 'ASC'}

    # Fetching last state
    next_cursor = state_file.get(endpoint, {}).get('last_cursor')
    if not next_cursor:
        next_cursor = state_file.get(endpoint, {}).get(account, {}).get('last_cursor')
    logging.info('[reviews] loaded last cursor from state: {}'.format(next_cursor))

    last_cursor = next_cursor

    while_loop = True
    while while_loop:
        if next_cursor:
            params['after'] = next_cursor

        res = requests.get(url=BASE_URL + request_url,
                           headers=headers, params=params)
        res_json = res.json()

        logging.info("Current Cursor: [{0}] @ [{1}] - Parsing".format(next_cursor, endpoint))

        try:
            parse(res_json['data'], file_name)
        except Exception as e:
            logging.error(res_json)
            logging.error("Error while parsing data: {}".format(str(e)))

        try:
            next_cursor = res_json['paging']['cursors']['after']
            if next_cursor is None:
                while_loop = False
            else:
                last_cursor = next_cursor
                logging.debug('[reviews] next paging cursor: {}'.format(last_cursor))
        except Exception:
            next_cursor = None
            while_loop = False

    endpoint_state = {'last_cursor': last_cursor}

    return entities, endpoint_state


def request_endpoint(username, token, state_file, endpoint, file_name, account):
    """
    Request endpoint with the provided pagination paramters
    """

    endpoint_state = {}

    entities = []
    headers = _build_headers(username, token)
    params = {'account_id': account, 'per_page': 500}

    res = requests.get(url=BASE_URL + endpoint, headers=headers, params=params)

    if res.status_code == 404:
        print(res.text)
        return 404
    res = json.loads(res.text)

    if 'metrics' in endpoint:
        entities.append(res)
    else:

        starting_page = state_file.get(endpoint, {}).get('last_page_fetched')
        if not starting_page:
            starting_page = state_file.get(endpoint, {}).get(account, {}).get('last_page_fetched')
        if not starting_page:
            starting_page = 1
            logging.info(f"Starting with page {starting_page} for endpoint {endpoint}")
        else:
            logging.info(f"Last fetched page from state for endpoint {endpoint} is {starting_page}")

        first_request_params = copy.deepcopy(params)
        first_request_params["page"] = starting_page
        first_request_params["sort[by]"] = "created_at"
        first_request_params["sort[order]"] = "ASC"

        # collect first page objects
        res = requests.get(url=BASE_URL + endpoint,
                           headers=headers, params=first_request_params)
        res = json.loads(res.text)

        # Captures error
        if "error" in res:
            logging.error("{0}: {1}".format(res["error"], res["status"]))
            sys.exit(1)

        total_pages = int(res.get('_total_pages'))

        logging.info("Endpoint: [{0}]; Total Pages: [{1}]".format(endpoint, total_pages))

        # First page processing
        logging.info("Current Page: [{0}] @ [{1}] - Parsing".format(starting_page, endpoint))
        entities_curr_page = res.get("_embedded").get(endpoint)
        entities += entities_curr_page
        parse(entities_curr_page, file_name)
        starting_page += 1

        while "next" in res["_links"]:
            next_url = res["_links"]["next"]["href"]
            logging.debug("Next Url: ...{0}".format(next_url[-60:]))

            res = requests.get(url=next_url, headers=headers, params=params)
            res = json.loads(res.text)
            logging.info("Current Page: [{0}] @ [{1}] - Parsing".format(starting_page, endpoint))

            entities_curr_page = res.get("_embedded").get(endpoint)
            entities += entities_curr_page

            # if there are no more records, stop at that page
            if len(entities_curr_page) == 0:
                logging.info("No records found on page [{0}] @ [{1}]".format(
                    starting_page, endpoint))
                logging.info("Stopping [{0}] @ page [{1}]".format(
                    endpoint, starting_page))
                total_pages = starting_page
                break
            else:
                parse(entities_curr_page, file_name)

            starting_page += 1

        # Update State file parameters
        # Prevent weird pagination output from ReviewTrackers
        if starting_page > total_pages:
            starting_page = total_pages
        endpoint_state = {
            "last_page_fetched": starting_page - 1,
            "total_pages": total_pages
        }

    return entities, endpoint_state


def request_accounts(username, token):
    """
    Request accounts
    """
    headers = _build_headers(username, token)
    res = requests.get(url=BASE_URL + "accounts", headers=headers)

    if res.status_code == 404:
        print(res.text)
        return 404
    res = json.loads(res.text)

    accounts = res.get("_embedded").get("accounts")
    accounts_ids = [account['id'] for account in accounts]
    return accounts_ids
