# import dateutil.relativedelta
import datetime
import json
import logging
import os
import warnings
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from service.api_client import request_endpoint
from service.flattener import flatten

DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
warnings.simplefilter(action='ignore', category=FutureWarning)


def _auth(username, password):
    url = 'https://api-gateway.reviewtrackers.com/auth'

    headers = {
        'Authorization': "{}:{}".format(username, password),
        'Accept': "application/vnd.rtx.authorization.v2.hal+json;charset=utf-8",
        "Content-Type": "application/json"
    }

    res = requests.post(url=url, headers=headers, auth=HTTPBasicAuth(username, password))

    auth_res = json.loads(res.text)

    return {
        'account_id': auth_res.get('account_id'),
        'token': auth_res.get('token')
    }


def _lookup(by, by_val, get):
    if by == "endpoint" and "metrics" in by_val:
        tmp = by_val.split("/")
        by_val = "{}/{{account_id}}/{}".format(tmp[0], tmp[2])
    df_lookup = pd.read_csv('/code/src/lookup.csv')
    df_lookup = df_lookup.loc[df_lookup[by].isin([by_val])]
    s = df_lookup[get]
    return s.tolist()[0]


def _validate_date_format(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        logging.warning("Incorrect data format, should be YYYY-MM-DD")
        return False


def _parse_ui_metrics(ui_metrics, account_id):

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
                m["endpoint"] = lookup.get(m.get("report_type")).get("endpoint")
                m["file_name"] = lookup.get(m.get("report_type")).get("file_name")
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
            logging.info("Output manifest file produced.")
    except Exception as e:
        logging.error("Could not produce output file manifest.")
        logging.error(e)


def _output(filename, data):
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
    elif filename == "reviews":
        _produce_manifest("reviews", "id")


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
        df["review_published_before"] = pd.to_datetime(df["review_published_before"])
        num_of_rows = df.shape[0]
        if num_of_rows == 0:
            raise ValueError
        published_after = df["review_published_before"].max()
        df_updated = df.append(df_new_record)
        published_after = str(published_after.date())
    except (FileNotFoundError, ValueError):
        logging.warning("Incorrect metadata_ingestion_records table, creating a new one...")
        # published_after = today - dateutil.relativedelta.relativedelta(months=1)
        published_after = None
        df_updated = df_new_record

    _output("metadata_ingestion_records", df_updated)
    return published_after


def run(ui_username, ui_password, ui_endpoints, ui_metrics, ui_tables):
    auth_res = _auth(username=ui_username, password=ui_password)
    account_id = auth_res.get('account_id')
    token = auth_res.get('token')

    last_update_time = _get_last_update_time(tables=ui_tables)
    params = {
        'account_id': account_id
    }

    if "All" in ui_endpoints:
        ui_endpoints = [
            "accounts",
            "alert_frequencies",
            "alerts",
            "alert_types",
            "campaigns",
            "competitors",
            "contacts",
            "groups",
            "items",
            "layouts",
            "locations",
            "notes",
            "profiles",
            "permissions",
            "request_pages",
            "requests",
            "request_types",
            "responses",
            "reviews",
            "review_status_labels",
            "single_sign_ons",
            "sources",
            "templates",
            "template_tags",
            "urls",
            "users",
            "user_types",
            "whitelabels"
        ]

    # Capturing total requests
    n_th = 0

    for endpoint in ui_endpoints:

        if endpoint == "reviews" and last_update_time:
            params["published_after"] = last_update_time
        else:
            if "published_after" in params:
                del params["published_after"]

        logging.info("fetching endpoint {} ...".format(endpoint))
        json_res, n_th = request_endpoint(ui_username, token, endpoint, params, n_th)
        if json_res == 404:
            logging.warning("Endpoint [{}] not found, 404 Error".format(endpoint))
            continue
        file_name = _lookup(by='endpoint', by_val=endpoint, get='file_name')
        logging.info("preparing file {} ...".format(file_name))
        result_df_d = flatten(json_res, file_name)
        if result_df_d is None:
            continue
        for k in result_df_d:
            _output(k, result_df_d.get(k))

    metrics = _parse_ui_metrics(ui_metrics, account_id)

    for metric in metrics:

        params["month_before"] = metric.get("month_before")
        params["month_after"] = metric.get("month_after")
        endpoint = metric.get("endpoint")
        file_name = metric.get("file_name")

        logging.info("fetching metrics {} ...".format(endpoint))
        json_res, n_th = request_endpoint(ui_username, token, endpoint, params, n_th)
        if json_res == 404:
            logging.warning("Metrics [{}] not found, 404 Error".format(endpoint))
            continue
        logging.info("preparing file {} ...".format(file_name))

        result_df_d = flatten(json_res, file_name)
        if result_df_d is None:
            continue
        for k in result_df_d:
            _output(k, result_df_d.get(k))
