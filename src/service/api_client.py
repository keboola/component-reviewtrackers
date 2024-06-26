import requests
import base64
import json
import logging
import copy
import sys
# from service.flattener import flatten
from service.parser import parse


BASE_URL = "https://api.reviewtrackers.com/"
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
# BASE_URL = "https://api-gateway.reviewtrackers.com/"


def _build_headers(username, token):
    auth_string = "{}:{}".format(username, token)
    encoded = base64.b64encode(auth_string.encode())
    encoded = encoded.decode("utf-8")
    return {
        "Authorization": "Basic {}".format(encoded),
        "Content-Type": "application/json; charset=utf-8",
        'Accept': "application/vnd.rtx.campaign.v2.hal+json;charset=utf-8",
    }


def request_reviews_v2(username, token, state_file, endpoint, file_name, params):
    """
    Request new review endpoint with provided parameters
    """

    entities = []
    headers = _build_headers(username, token)
    request_endpoint = 'v2/{}'.format(endpoint)
    params['per_page'] = 250
    ex_itr = 0  # Keeping track of the number of extractions
    while_loop = True

    # Fetching last state
    try:
        next_cursor = state_file['reviews']['last_cursor']
        logging.info('[reviews] last cursor: {}'.format(next_cursor))
    except Exception:
        next_cursor = None

    params['sort[by]'] = 'published_at'
    params['sort[order]'] = 'ASC'
    last_cursor = next_cursor

    while while_loop and ex_itr <= 100:
        if next_cursor:
            params['after'] = next_cursor

        res = requests.get(url=BASE_URL+request_endpoint,
                           headers=headers, params=params)
        res_json = res.json()
        # Outputting

        try:
            parse(res_json['data'], file_name)
        except Exception as e:
            logging.error(res_json)
            logging.error(
                "Error while parsing data: {}".format(str(e)))


        try:
            next_cursor = res_json['paging']['cursors']['after']
            if next_cursor is None:
                while_loop = False
            else:
                last_cursor = next_cursor
                logging.info(
                    '[reviews] next paging cursor: {}'.format(last_cursor))
        except Exception:
            next_cursor = None
            while_loop = False

        ex_itr += 1

    endpoint_state = {
        'last_cursor': last_cursor
    }
    state_file[endpoint] = endpoint_state

    return entities, state_file


def request_endpoint(username, token, state_file, endpoint, file_name, params):
    """
    Request endpoint with the provided pagination paramters
    """

    entities = []
    headers = _build_headers(username, token)
    params["per_page"] = 250

    res = requests.get(url=BASE_URL + endpoint, headers=headers, params=params)

    if res.status_code == 404:
        print(res.text)
        return 404
    res = json.loads(res.text)

    if 'metrics' in endpoint:
        entities.append(res)
    else:
        starting_page = 1
        # Limiting ex to terminate at 100th request
        ex_itr = 0
        if endpoint in state_file:
            starting_page = state_file[endpoint]["last_page_fetched"]
            logging.info("Last fetched page: [{0}] @ [{1}]".format(
                starting_page, endpoint))
        else:
            logging.info("Starting page: [1] @ [{0}]".format(endpoint))
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

        logging.info("Endpoint: [{0}]; Total Pages: [{1}]".format(
            endpoint, total_pages))

        # First page processing
        logging.info(
            "Current Page: [{0}] @ [{1}] - Parsing".format(starting_page, endpoint))
        entities_curr_page = res.get("_embedded").get(endpoint)
        entities += entities_curr_page
        parse(entities_curr_page, file_name)
        starting_page += 1

        while "next" in res["_links"] and ex_itr < 100:
            next_url = res["_links"]["next"]["href"]
            logging.info("Next Url: ...{0}".format(next_url[-60:]))

            res = requests.get(url=next_url, headers=headers, params=params)
            res = json.loads(res.text)
            logging.info(
                "Current Page: [{0}] @ [{1}] - Parsing".format(starting_page, endpoint))

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

            ex_itr += 1
            starting_page += 1

        # Update State file parameters
        # Prevent weird pagination output from ReviewTrackers
        if starting_page > total_pages:
            starting_page = total_pages
        endpoint_state = {
            "last_page_fetched": starting_page - 1,
            "total_pages": total_pages
        }
        state_file[endpoint] = endpoint_state

    return entities, state_file


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
