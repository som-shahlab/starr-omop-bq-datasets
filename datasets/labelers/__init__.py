import os 
import random  
import string  

from ..database import Database
from ..util import bq_extract_flowsheets_from_observations

from .demographics import (
    AgeQuery, SexQuery, RaceQuery
)

from .operational import (
    MortalityQuery, LOS7Query, Readmission30Query,
    ICUAdmissionQuery
)

from .lab_based import (
    HyperkalemiaQuery, HypoglycemiaQuery, NeutropeniaQuery,
    HyponatremiaQuery, AcuteKidneyInjuryQuery, AnemiaQuery, 
    ThrombocytopeniaQuery
)

from .dx_based import (
    HypoglycemiaDxQuery, AKIDxQuery, AnemiaDxQuery, 
    HyperkalemiaDxQuery, HyponatremiaDxQuery, ThrombocytopeniaDxQuery,
    NeutropeniaDxQuery
)

    
class Labeler:
    def __init__(self, *args, **kwargs):
        self.config = self.get_config(**kwargs)
        self.check_config()
        self.db = Database(**self.config)
        self.queries = self.get_queries()
        
    def get_default_config(self):
        return {
            "google_application_credentials": os.path.expanduser(
                "~/.config/gcloud/application_default_credentials.json"
            ),
            "gcloud_project": "som-nero-nigam-starr",
            "dataset_project": None,
            "rs_dataset_project": None,
            "dataset": "starr_omop_cdm5_deid_20210723",
            "rs_dataset": "temp_dataset",
            "cohort_name": "temp_cohort",
            'target_table_name':'temp_cohort_labeled',
            'row_id':'prediction_id',
            'window_start_field':'admit_date',
            'window_end_field':'discharge_date',
            'temp_dataset': "temp",
            'extract_labs_from_flowsheets':False, 
            'flowsheets_extract_name':None,
            'overwrite_flowsheets_extract':False,
            'flowsheet_concept_id':'2000006253',
        }
    
    def override_default_config(self, **kwargs):
        return {**self.get_default_config(), **kwargs}
    
    def get_config(self, **kwargs):
        config = self.override_default_config(**kwargs)
        
        # Handle special parameters
        config["dataset_project"] = (
            config["dataset_project"]
            if (
                (config["dataset_project"] is not None)
                and (config["dataset_project"] != "")
            )
            else config["gcloud_project"]
        )
        
        config["rs_dataset_project"] = (
            config["rs_dataset_project"]
            if (
                (config["rs_dataset_project"] is not None)
                and (config["rs_dataset_project"] != "")
            )
            else config["gcloud_project"]
        )
        
        return config
    
    def check_config(self):
        ...
            
    def configure(self, **kwargs):
        self.config = {**self.config, **kwargs}
        
    def get_queries(self):
        """
        A dictionary of available query classes
        """
        
        query_classes = [
            AgeQuery(),
            SexQuery(),
            RaceQuery(),
            MortalityQuery(),
            LOS7Query(),
            ICUAdmissionQuery(),
            #Readmission30Query(), # not properly implemented
            HyperkalemiaQuery(),
            HypoglycemiaQuery(),
            NeutropeniaQuery(),
            HyponatremiaQuery(),
            AcuteKidneyInjuryQuery(),
            AnemiaQuery(),
            ThrombocytopeniaQuery(),
            HypoglycemiaDxQuery(), 
            AKIDxQuery(), 
            AnemiaDxQuery(), 
            HyperkalemiaDxQuery(), 
            HyponatremiaDxQuery(), 
            ThrombocytopeniaDxQuery(),
            NeutropeniaDxQuery(),
        ]
        
        queries =  {
            query_class.config["labeler_id"]:query_class 
            for query_class in query_classes
        }
        
        for k in queries:
            if 'condition_concept_ids' in queries[k].config:
                queries[k].config['condition_concept_ids'] = ','.join([
                    str(x) for x in queries[k].config['condition_concept_ids']
                ])
        
        return queries
    
    
    def list_queries(self):
        """
        Returns a dictionary of available query classes with a description of the labeling function
        """
        
        queries = self.get_queries() 
        
        return {
            x:queries[x].info
            for x in queries
        }
    
    def get_label_query(self, labeler_ids:list=None, exclude_labeler_ids:list=None):
        """Build label query"""
        
        if labeler_ids is None:
            labeler_ids = self.queries.keys()
            
        if exclude_labeler_ids is not None:
            labeler_ids = [x for x in labeler_ids if x not in exclude_labeler_ids]
            
        for labeler_id in labeler_ids:
            if labeler_id not in self.queries.keys():
                raise ValueError(f"Provided labeler_id {labeler_id} not defined")
                
        queries = {k:self.queries[k] for k in labeler_ids}
        
        q_main = ""
        q_join = ""
        q_cleanup = ""
        
        if self.config['extract_labs_from_flowsheets']:
            q_main+=bq_extract_flowsheets_from_observations(
                bq_project = self.config['dataset_project'],
                bq_dataset = self.config['dataset'],
                target_bq_project = self.config['rs_dataset_project'],
                target_bq_dataset = self.config['rs_dataset'],
                target_bq_table = self.config['flowsheets_extract_name'],
                flowsheet_concept_id = self.config['flowsheet_concept_id'],
                overwrite = self.config['overwrite_flowsheets_extract'],
            )
        
        rs_dataset_project = self.config['rs_dataset_project']
        rs_dataset = self.config['rs_dataset']
        temp_dataset = self.config['temp_dataset']
        window_start_field = self.config['window_start_field']
        window_end_field = self.config['window_end_field']
        target_table_name = self.config['target_table_name']
        cohort_name = self.config['cohort_name']
        
        rnd_suffix = ''.join((random.choice(string.ascii_lowercase) for x in range(5)))
        
        # create temp label table for each task
        for labeler_id, query in queries.items():
            
            if self.config['extract_labs_from_flowsheets']:
                query.config = {**query.config, **self.config}
                query.base_query = query.get_base_query()
                
            i_q = query.base_query.format_map({**self.config, **query.config})
            
            q_main += f"""
            CREATE OR REPLACE TABLE {rs_dataset_project}.{temp_dataset}.temp_{labeler_id}_{rnd_suffix}
            AS {i_q};
            """
            
            q_join += f"""
            LEFT JOIN {rs_dataset_project}.{temp_dataset}.temp_{labeler_id}_{rnd_suffix} USING (person_id, {window_start_field}, {window_end_field})
            """
            
            q_cleanup += f"""
            DROP TABLE {rs_dataset_project}.{temp_dataset}.temp_{labeler_id}_{rnd_suffix};
            """
        
        # join w/ cohort 
        q_main += f"""
        CREATE OR REPLACE TABLE {rs_dataset_project}.{rs_dataset}.{target_table_name} AS
        (
            SELECT * 
            FROM {rs_dataset_project}.{rs_dataset}.{cohort_name} 
            {q_join}
        );
        
        {q_cleanup}
        """
        
        return q_main
            
        
    def create_label_table(self, labeler_ids:list=None, exclude_labeler_ids:list=None):
        """
        Creates the cohort table in the database
        """
        self.db.execute_sql(self.get_label_query(labeler_ids, exclude_labeler_ids))
    