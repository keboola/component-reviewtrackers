import requests
import base64
import json
import logging


BASE_URL = "https://api.reviewtrackers.com/"
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


def request_endpoint(username, token, endpoint, params, n_th):
    entities = []
    headers = _build_headers(username, token)
    res = requests.get(url=BASE_URL + endpoint, headers=headers, params=params)
    if res.status_code == 404:
        print(res.text)
        return 404
    res = json.loads(res.text)

    if 'metrics' in endpoint:
        entities.append(res)
    else:
        # collect first page objects
        entities += res.get("_embedded").get(endpoint)
        total_pages = res.get('_total_pages')
        current_page_num = int(res.get('_page'))
        logging.info("Endpoint: [{0}]; Total Pages: [{1}]".format(endpoint, total_pages))
        #logging.info("Total Pages: [{}]".format(total_pages))
        #logging.info("Current Page: [{}]".format(current_page_num))

        while current_page_num < total_pages:

            entities_curr_page = res.get("_embedded").get(endpoint)
            entities += entities_curr_page
            current_page_num = int(res.get("_page"))
            logging.info("Current Page: [{}]; Entities per Page: [{}]".format(current_page_num, len(entities_curr_page)))
            #logging.info("Entities on the page: [{}]".format(len(entities_curr_page)))

            params["page"] = current_page_num + 1
            res = requests.get(url=BASE_URL + endpoint, headers=headers, params=params)
            res = json.loads(res.text)

        ### Number of requests
        n_th = n_th + int(total_pages)   
        logging.info("Total Requests: [{0}] @ [{1}]".format(n_th, endpoint)) 

    return entities, n_th
