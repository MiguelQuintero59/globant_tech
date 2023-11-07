"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import load_csv_file, ingest_data, fetch_tables_spreadsheet

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # node(
            #     func=load_csv_file,
            #     inputs='raw_hired_employees',
            #     outputs='hired_employees_raw',
            #     name="load_hired_employees"
            # ),
            #
            # node(
            #     func=load_csv_file,
            #     inputs='raw_departments',
            #     outputs='departments_raw',
            #     name="load_departments"
            # ),
            #
            # node(
            #     func=load_csv_file,
            #     inputs='raw_jobs',
            #     outputs='jobs_raw',
            #     name="load_jobs"
            # ),

            node(
                func=ingest_data,
                inputs='hired_employees_raw',
                outputs='hired_employees_db',
                name="fetch_hired_employees"
            ),

            node(
                func=ingest_data,
                inputs='departments_raw',
                outputs='departments_db',
                name="fetch_departments"
            ),

            node(
                func=ingest_data,
                inputs='raw_jobs',
                outputs='jobs_raw',
                name="fetch_jobs"
            ),

            node(
                func=fetch_tables_spreadsheet,
                inputs=['params:general.urls.gdrive'],
                outputs=['hired_employees_bck',
                         'departments_bck',
                         'jobs_bck'],
                name="fetch_end_point"
            ),
        ]
    )
