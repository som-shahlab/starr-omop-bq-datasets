import numpy as np
import pandas as pd
import yaml
import os
import shutil
import argparse
import pickle

def str2bool(v):
    """
    Converts strings to booleans (e.g., 't' -> True)
    """
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def yaml_write(x, path):
    """
    Writes yaml to disk
    """
    with open(path, "w") as fp:
        yaml.dump(x, fp)


def yaml_read(path):
    """
    Reads yaml from disk
    """
    with open(path, "r") as fp:
        return yaml.load(fp, Loader=yaml.FullLoader)


def df_dict_concat(df_dict, outer_index_name="outer_index", drop_outer_index=False):
    """
    Concatenate a dictionary of dataframes together and remove the inner index
    """
    if isinstance(outer_index_name, str):
        reset_level = 1
    else:
        reset_level = len(outer_index_name)

    return (
        pd.concat(df_dict, sort=False)
        .reset_index(level=reset_level, drop=True)
        .rename_axis(outer_index_name)
        .reset_index(drop=drop_outer_index)
    )


def overwrite_dir(the_path, overwrite=True):
    """
    Overwrites a directory at a path.
    Will fail if overwrite=False and the_path exists
    """
    if os.path.exists(the_path):
        if not overwrite:
            raise ValueError(
                "Trying to overwrite directory {}, but `overwrite` is False".format(
                    the_path
                )
            )
        shutil.rmtree(the_path)
    os.makedirs(the_path)


def read_file(
    filename, columns=None, load_extension="parquet", mode="pandas", **kwargs
    ):

    if mode == "pandas":
        if load_extension == "parquet":
            return pd.read_parquet(filename, columns=columns, **kwargs)
        elif load_extension == "csv":
            return pd.read_csv(filename, usecols=columns, **kwargs)
    elif mode == "dask":
        if load_extension == "parquet":
            return pd.read_parquet(filename, columns=columns, **kwargs)
        elif load_extension == "csv":
            return pd.read_csv(filename, usecols=columns, **kwargs)
    else:
        raise ValueError('"pandas" and "dask" are the only allowable modes')

        
def bq_extract_flowsheets_from_observations(
    bq_dataset:str,
    target_bq_dataset:str, 
    target_bq_table:str,
    bq_project:str='som-nero-nigam-starr',
    target_bq_project:str='som-nero-nigam-starr',
    flowsheet_concept_id:str='2000006253',
    overwrite:bool=False,
    ):
    """
    Construct a BigQuery SQL that extracts flowsheet rows stored as JSON in the OMOP 
    observation table and loads the extracted rows to a target BigQuery
    table. 
    
    flowsheet_concept_id is the custom concept_id that indicates that a row associated
    with the observation_id contains the flowsheet JSON. 
    
    Important note: the extract is large (>4.5B rows)
    """
    
    if overwrite:
        q_create = f"create or replace table `{target_bq_project}.{target_bq_dataset}.{target_bq_table}` as"
    else:
        q_create = f"create table if not exists `{target_bq_project}.{target_bq_dataset}.{target_bq_table}` as"
    
    return f"""
    {q_create} 
    (
    with meas as (
      select observation_id, 
      json_extract_scalar(v, '$.source') as val_source,
      json_extract_scalar(v, '$.value') as val_value
      FROM `{bq_project}.{bq_dataset}.observation` ob 
      left join unnest(json_extract_array(value_as_string,'$.values')) as v
      where ob.observation_concept_id = {flowsheet_concept_id}
      and JSON_VALUE(v,'$.source') = "ip_flwsht_meas.meas_value"
    ),
    disp as (
      select observation_id, 
      json_extract_scalar(v, '$.source') as val_source,
      json_extract_scalar(v, '$.value') as val_value
      FROM `{bq_project}.{bq_dataset}.observation` ob 
      left join unnest(json_extract_array(value_as_string,'$.values')) as v
      where ob.observation_concept_id = {flowsheet_concept_id}
      and JSON_VALUE(v,'$.source') = "ip_flo_gp_data.disp_name"
    ),
    unit as (
      select observation_id, 
      json_extract_scalar(v, '$.source') as val_source,
      json_extract_scalar(v, '$.value') as val_value
      FROM `{bq_project}.{bq_dataset}.observation` ob 
      left join unnest(json_extract_array(value_as_string,'$.values')) as v
      where ob.observation_concept_id = {flowsheet_concept_id}
      and JSON_VALUE(v,'$.source') = "ip_flo_gp_data.units"
    ),
    src as (
      select observation_id, 
      json_extract_scalar(v, '$.source') as val_source,
      json_extract_scalar(v, '$.value') as val_value
      FROM `{bq_project}.{bq_dataset}.observation` ob 
      left join unnest(json_extract_array(observation_source_value,'$.values')) as v
      where ob.observation_concept_id = {flowsheet_concept_id}
      and JSON_VALUE(v,'$.source') = "ip_flt_data.display_name"
    )
    select ob.observation_id, ob.person_id, ob.observation_datetime,
    case 
      when ob.observation_concept_id = {flowsheet_concept_id}
        then src.val_value 
      else ob.observation_source_value
    END as source_display_name,
    case 
      when ob.observation_concept_id = {flowsheet_concept_id}
        then disp.val_value
      else cpt.concept_name
    END as display_name,
    case 
      when ob.observation_concept_id = {flowsheet_concept_id}
        then meas.val_value
      when ob.observation_concept_id <> {flowsheet_concept_id} and value_as_string is not null
        then value_as_string
      when ob.observation_concept_id <> {flowsheet_concept_id} and value_as_string is null
        then CAST(value_as_number as string)
    END as meas_value,
    case 
      when ob.observation_concept_id = {flowsheet_concept_id}
        then unit.val_value
      else ob.unit_source_value
    END as units,
    from `{bq_project}.{bq_dataset}.observation` ob 
    left join meas on ob.observation_id = meas.observation_id
    left join unit on ob.observation_id = unit.observation_id 
    left join disp on ob.observation_id = disp.observation_id
    left join src on ob.observation_id = src.observation_id
    left join `{bq_project}.{bq_dataset}.concept` cpt on cpt.concept_id = ob.observation_source_concept_id
    );
    """