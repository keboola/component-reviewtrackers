import pandas as pd
import sys


def _lookup(by, by_val, get):
    df_lookup = pd.read_csv('lookup.csv')
    df_lookup = df_lookup.loc[df_lookup[by].isin([by_val])]
    s = df_lookup[get]
    return s.tolist()[0]


def _flatten_metrics_overview(json_obj):
    record = {}
    for k in json_obj:
        if k == '_links':
            continue
        elif k == 'ratings':
            ratings = json_obj.get('ratings')
            for r in ratings:
                record["ratings_{}".format(r)] = ratings.get(r)
        else:
            record[k] = json_obj.get(k)

    df = pd.DataFrame.from_records([record])
    return df


def _flatten_individual_metrics(json_obj, metric_name):
    records = []
    for month in json_obj.get(metric_name):
        records.append(month)

    df = pd.DataFrame.from_records(records)

    return df


def _flatten_metrics(json_obj, file_name):
    json_obj = json_obj[0]
    if file_name == 'metrics_overview':
        result_df = _flatten_metrics_overview(json_obj)
    elif file_name == 'metrics_overview_monthly':
        result_df = _flatten_individual_metrics(json_obj, 'monthly')
    elif file_name == 'metrics_sources':
        result_df = _flatten_individual_metrics(json_obj, 'sources')
    else:
        print("Cannot identify metric type")

    return {file_name: result_df}


# this return a dict of dfs
def _flatten_entity(json_obj, file_name):
    result_d = {}
    meta_fields = [
        '_links'
    ]
    df_d = {}
    for k in json_obj:
        if k in meta_fields:
            continue
        else:
            v = json_obj.get(k)
            if isinstance(v, list):
                parent_id = json_obj.get("id")
                extended_col_name = "{}_{}".format(file_name, k)
                df_child = pd.DataFrame(data=v, columns=[extended_col_name])
                df_child["{}_id".format(file_name)] = parent_id
                df_d[extended_col_name] = df_child
            elif isinstance(v, dict):
                for leave_k in v:
                    extended_col_name = "{}_{}".format(k, leave_k)
                    result_d[extended_col_name] = v.get(leave_k)
            else:
                result_d[k] = v

    df = pd.DataFrame.from_records([result_d])
    df_d[file_name] = df

    return df_d


def _flatten_collection(entities, file_name):

    if len(entities) == 0:
        print("No object from '{}' endpoint".format(file_name))
        return None

    df_collection_d = {}

    for e in entities:
        df_d = _flatten_entity(e, file_name)
        for k in df_d:
            if df_collection_d.get(k) is None:
                df_collection_d[k] = df_d.get(k)
            else:
                df_collection_d[k] = df_collection_d.get(k).append(df_d.get(k))

    return df_collection_d


def flatten(json_obj, file_name):
    if 'metrics' in file_name:
        df_d = _flatten_metrics(json_obj, file_name)
    else:
        df_d = _flatten_collection(json_obj, file_name)

    return df_d
