"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import read_csv_file, fetch_tables_spreadsheet

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=read_csv_file,
                inputs=['raw_hired_employees'],
                outputs='hired_employees_db',
                name="fetch_hired_employees"
            ),

            node(
                func=read_csv_file,
                inputs=['raw_departments'],
                outputs='departments_db',
                name="fetch_departments"
            ),

            node(
                func=read_csv_file,
                inputs=['raw_jobs'],
                outputs='jobs_db',
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
