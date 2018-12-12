import pandas as pd


def simple_1_layer_flattener(json_obj, custom_mapping_d=None):

    if custom_mapping_d is None:
        custom_mapping_d = {}

    result_d = {}
    meta_fields = [
        '_links'
    ]
    for k in json_obj:
        if k in meta_fields:
            continue
        else:
            if k in custom_mapping_d:
                result_d[custom_mapping_d.get(k)] = json_obj.get(k)
            else:
                result_d[k] = json_obj.get(k)

    df = pd.DataFrame.from_records([result_d])
    return df


def collection_flattener(json_obj, endpoint, custom_mapping_d=None):

    df_collection = None

    entities = json_obj.get("_embedded").get(endpoint)
    for e in entities:
        df_e = simple_1_layer_flattener(e, custom_mapping_d)
        if df_collection is None:
            df_collection = df_e
        else:
            df_collection = df_collection.append(df_e)

    return df_collection
