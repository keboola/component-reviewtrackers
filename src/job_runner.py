import os
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import warnings
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
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD")


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
        "Source": {
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


def run(ui_username, ui_password, ui_endpoints, ui_metrics):
    auth_res = _auth(username=ui_username, password=ui_password)
    account_id = auth_res.get('account_id')
    token = auth_res.get('token')

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

    for endpoint in ui_endpoints:

        print("fetching endpoint {} ...".format(endpoint))
        json_res = request_endpoint(ui_username, token, endpoint, params)
        if json_res == 404:
            print("Endpoint [{}] not found, 404 Error".format(endpoint))
            continue
        file_name = _lookup(by='endpoint', by_val=endpoint, get='file_name')
        print("preparing file {} ...".format(file_name))
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

        print("fetching metrics {} ...".format(endpoint))
        json_res = request_endpoint(ui_username, token, endpoint, params)
        if json_res == 404:
            print("Metrics [{}] not found, 404 Error".format(endpoint))
            continue
        print("preparing file {} ...".format(file_name))
        result_df_d = flatten(json_res, file_name)
        if result_df_d is None:
            continue
        for k in result_df_d:
            _output(k, result_df_d.get(k))
