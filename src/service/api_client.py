import requests
import base64
import json
import logging
import os
import copy


BASE_URL = "https://api.reviewtrackers.com/"
# BASE_URL = "https://api-gateway.reviewtrackers.com/"


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

    return temp


def _write_state(data_in):
    """
    Updating state file
    """

    logging.info("Outputting State file...")
    with open("/data/out/state.json", "w") as f:
            json.dump(data_in, f)
    return


def _build_headers(username, token):
    auth_string = "{}:{}".format(username, token)
    encoded = base64.b64encode(auth_string.encode())
    encoded = encoded.decode("utf-8")
    return {
        "Authorization": "Basic {}".format(encoded),
        "Content-Type": "application/json; charset=utf-8",
        'Accept': "application/vnd.rtx.campaign.v2.hal+json;charset=utf-8",
    }


def request_endpoint(username, token, endpoint, params, n_th):
    entities = []
    headers = _build_headers(username, token)
    params["per_page"] = 250
    # State file parameters
    state_file = _read_state()

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
        entities += res.get("_embedded").get(endpoint)
        total_pages = int(res.get('_total_pages'))
        # current_page_num = int(res.get('_page'))
        logging.info("Endpoint: [{0}]; Total Pages: [{1}]".format(endpoint, total_pages))

        # logging.info("Total Pages: [{}]".format(total_pages))
        # logging.info("Current Page: [{0}] @ [{1}]".format(current_page_num, endpoint))

        """
        while current_page_num < total_pages:

            entities_curr_page = res.get("_embedded").get(endpoint)
            entities += entities_curr_page
            current_page_num = int(res.get("_page"))
            logging.info("Current Page: [{0}] @ [{1}]".format(current_page_num, endpoint))
            # logging.info("Entities on the page: [{}]".format(len(entities_curr_page)))

            params["page"] = current_page_num + 1
            res = requests.get(url=BASE_URL + endpoint, headers=headers, params=params)
            res = json.loads(res.text)
        """
        # while current_page_num < total_pages + 1:

        # First page processing
        logging.info("Current Page: [{0}] @ [{1}]".format(starting_page, endpoint))
        entities_curr_page = res.get("_embedded").get(endpoint)
        entities += entities_curr_page
        starting_page += 1

        # next_url = res.get("_links").get("next").get("href")
        # if "next" in res["_links"]:
        while "next" in res["_links"] and ex_itr < 1:
            next_url = res["_links"]["next"]["href"]
            logging.info("Next Url: ...{0}".format(next_url[-60:]))

            res = requests.get(url=next_url, headers=headers, params=params)
            print(params)
            res = json.loads(res.text)
            # current_page_num = int(res.get('_page'))
            # logging.info(res["_page"])
            logging.info("Current Page: [{0}] @ [{1}]".format(starting_page, endpoint))

            entities_curr_page = res.get("_embedded").get(endpoint)
            entities += entities_curr_page

            ex_itr += 1
            starting_page += 1

        # Update State file parameters
        endpoint_state = {
            "last_page_fetched": starting_page - 1
        }
        state_file[endpoint] = endpoint_state
        _write_state(state_file)

        # Number of requests
        n_th = n_th + int(total_pages)
        logging.info("Total Requests Required: [{0}] @ [{1}]".format(n_th, endpoint))

    return entities, n_th
