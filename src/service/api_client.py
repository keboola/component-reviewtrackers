import requests
import base64
import json
import logging
import copy
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


def request_endpoint(username, token, state_file, endpoint, file_name, params):
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
            logging.info("Last fetched page: [{0}] @ [{1}]".format(starting_page, endpoint))
        else:
            logging.info("Starting page: [1] @ [{0}]".format(endpoint))
        first_request_params = copy.deepcopy(params)
        first_request_params["page"] = starting_page
        first_request_params["sort[by]"] = "created_at"
        first_request_params["sort[order]"] = "ASC"

        # collect first page objects
        res = requests.get(url=BASE_URL + endpoint, headers=headers, params=first_request_params)
        res = json.loads(res.text)
        total_pages = int(res.get('_total_pages'))
        logging.info("Endpoint: [{0}]; Total Pages: [{1}]".format(endpoint, total_pages))

        # First page processing
        logging.info("Current Page: [{0}] @ [{1}] - Parsing".format(starting_page, endpoint))
        entities_curr_page = res.get("_embedded").get(endpoint)
        entities += entities_curr_page
        parse(entities_curr_page, file_name)
        starting_page += 1

        while "next" in res["_links"] and ex_itr < 100:
            next_url = res["_links"]["next"]["href"]
            logging.info("Next Url: ...{0}".format(next_url[-60:]))

            res = requests.get(url=next_url, headers=headers, params=params)
            res = json.loads(res.text)
            logging.info("Current Page: [{0}] @ [{1}] - Parsing".format(starting_page, endpoint))

            entities_curr_page = res.get("_embedded").get(endpoint)
            entities += entities_curr_page
            """
            TEST
            if int(starting_page) == 629:
                logging.info(res)
                raise Exception("Pausing @ PAGE 629")
            """
            # if there are no more records, stop at that page
            if len(entities_curr_page) == 0:
                logging.info("No records found on page [{0}] @ [{1}]".format(starting_page, endpoint))
                logging.info("Stopping [{0}] @ page [{1}]".format(endpoint, starting_page))
                total_pages = starting_page
                break
            else:
                parse(entities_curr_page, file_name)

            ex_itr += 1
            starting_page += 1

        # Update State file parameters
        endpoint_state = {
            "last_page_fetched": starting_page - 1,
            "total_pages": total_pages
        }
        state_file[endpoint] = endpoint_state

    return entities, state_file
