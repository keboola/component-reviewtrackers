# import dateutil.relativedelta
import datetime
import json
import logging
import os
import sys
import warnings
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from service.api_client import request_endpoint, request_reviews_v2, request_accounts


# Input/Output Parameters
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
warnings.simplefilter(action='ignore', category=FutureWarning)


def _auth(username, password):
    """
    Basic Authorization to Server
    """
    url = 'https://api-gateway.reviewtrackers.com/auth'
    # logging.info("{0}:{1}".format(username, password))
    headers = {
        # 'Authorization': "{}:{}".format(username, password),
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


def _lookup(by, by_val, get):
    """
    Looking up Referenced Table
    """

    if by == "endpoint" and "metrics" in by_val:
        tmp = by_val.split("/")
        by_val = "{}/{{account_id}}/{}".format(tmp[0], tmp[2])
    df_lookup = pd.read_csv('/code/src/lookup.csv')
    df_lookup = df_lookup.loc[df_lookup[by].isin([by_val])]
    s = df_lookup[get]

    return s.tolist()[0]


def _validate_date_format(date_text):
    """
    Validating input date format
    """

    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        logging.warning("Incorrect data format, should be YYYY-MM-DD")
        return False


def _parse_ui_metrics(ui_metrics, account_id):
    """
    Formatting Input UI parameters
    """

    lookup = {
        "Overview": {
            "endpoint": "metrics/{}/overview".format(account_id),
            "file_name": "metrics_overview"
        },
        "Monthly": {
            "endpoint": "metrics/{}/overview/monthly".format(account_id),
            "file_name": "metrics_overview_monthly"
        },
        "Sources": {
            "endpoint": "metrics/{}/sources".format(account_id),
            "file_name": "metrics_sources"
        }
    }
    metrics = []

    for m in ui_metrics:
        if len(m) == 0:
            continue
        else:
            if _validate_date_format(m.get("month_after")) \
                    and _validate_date_format(m.get("month_before")):
                m["endpoint"] = lookup.get(
                    m.get("report_type")).get("endpoint")
                m["file_name"] = lookup.get(
                    m.get("report_type")).get("file_name")
                metrics.append(m)

    return metrics


def _produce_manifest(file_name, primary_key):
    """
    Create manifest file
    """

    file = "/data/out/tables/" + str(file_name)+".csv.manifest"

    manifest = {
        "incremental": True,
        "primary_key": [primary_key]
    }
    logging.debug(manifest)
    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info(
                "Output manifest file [{0}] produced.".format(file_name))
    except Exception as e:
        logging.error("Could not produce output file manifest.")
        logging.error(e)

    return


def _output(filename, data):
    """
    Outputting File
    """

    dest = DEFAULT_TABLE_DESTINATION + filename + ".csv"

    if os.path.isfile(dest):
        with open(dest, 'a') as b:
            data.to_csv(b, index=False, header=False)
        b.close()
    else:
        with open(dest, 'w+') as b:
            data.to_csv(b, index=False, header=True)
        b.close()

    if "reviews_" in filename:
        _produce_manifest(filename, "reviews_id")
    elif filename in ["reviews", "locations", "responses"]:
        # _produce_manifest("reviews", "id")
        _produce_manifest(filename, "id")

    return


def _get_last_update_time(tables):
    """
    Getting the last updated time
    """

    tracking_file_path = "/data/in/tables/metadata_ingestion_records.csv"
    now = datetime.datetime.today()
    # today = now.date()

    df_new_record = pd.DataFrame.from_dict(data={
        "ingest_time": [now],
        "review_published_before": [now]
    })

    try:
        found_metafile = False
        if len(tables) == 0:
            raise FileNotFoundError
        for t in tables:
            if t["full_path"] == tracking_file_path:
                found_metafile = True
                break
        if not found_metafile:
            raise FileNotFoundError
        df = pd.read_csv(tracking_file_path)
        df["ingest_time"] = pd.to_datetime(df["ingest_time"])
        df["review_published_before"] = pd.to_datetime(
            df["review_published_before"])
        num_of_rows = df.shape[0]
        if num_of_rows == 0:
            raise ValueError
        published_after = df["review_published_before"].max()
        df_updated = df.append(df_new_record)
        published_after = str(published_after.date())
    except (FileNotFoundError, ValueError):
        logging.warning(
            "Incorrect metadata_ingestion_records table, creating a new one...")
        # published_after = today - dateutil.relativedelta.relativedelta(months=1)
        published_after = None
        df_updated = df_new_record

    _output("metadata_ingestion_records", df_updated)
    return published_after


def run(ui_username, ui_password, ui_clear_state, ui_tables):
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
    if ui_clear_state == "false":
        ex_state = _read_state()
    else:
        logging.info("Clearing State File...")
        ex_state = {}

    accounts = request_accounts(ui_username, token)

    for endpoint in ui_endpoints:
        accounts_state = {}
        for account in accounts:
            params = {
                'account_id': account
            }
            # if endpoint == "reviews" and last_update_time:
            #     params["published_after"] = last_update_time
            # else:
            #     if "published_after" in params:
            #         del params["published_after"]

            logging.info("fetching endpoint {} ...".format(endpoint))
            file_name = _lookup(by='endpoint', by_val=endpoint, get='file_name')
            if endpoint == 'reviews':
                json_res, ex_state_new = request_reviews_v2(
                    ui_username, token, ex_state, endpoint, file_name, params)
            else:
                json_res, ex_state_new = request_endpoint(
                    ui_username, token, ex_state, endpoint, file_name, params)
            if json_res == 404:
                logging.warning(
                    "Endpoint [{}] not found, 404 Error".format(endpoint))
                continue

            accounts_state[account] = ex_state_new
        ex_state[endpoint] = accounts_state

        # State File Content after 1 Endpoint extraction
        logging.info("Extractor State: {0}".format(ex_state))

    # State File Out
    _write_state(ex_state)

    return
