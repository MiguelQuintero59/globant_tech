from kedro.pipeline import Pipeline, node, pipeline
from .nodes import execute_sql

# def create_pipeline(**kwargs) -> Pipeline:
#     return pipeline(
#         [
#             node(
#                 func=execute_sql,
#                 inputs='jobs_quarter_query',
#                 outputs='department_jobs_number',
#                 name="execute_sql_jobs"
#             ),
#         ]
#     )