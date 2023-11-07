import logging
import requests
import datetime
import pandas as pd

from io import BytesIO
from typing import Dict, Any, List
from .utils import apply_schema
from .schemas import HiredEmployees, Departments, Jobs
logger = logging.getLogger(__name__)

def load_csv_file(df):
     return df

def ingest_data(df):
     df['_timestamp'] = datetime.datetime.now()
     return df

def get_data(url:str, file_name, testing):
     if not testing:
          result = requests.get(url)
          if result.status_code == 200:
               data = BytesIO(result.content)
               data = pd.read_csv(data, chunksize=1000)
          raise RuntimeError("Couldn't download the data.")
     else:
          if file_name == "hired_employees":
               path = f"data/01_raw/{file_name}.xlsx"
               data = pd.read_excel(path, header=None, names=['id','name','datetime','department_id','job_id'])

          elif file_name == "departments":
               path = f"data/01_raw/{file_name}.xlsx"
               data = pd.read_excel(path, header=None, names=['id','department'])

          else:
               path = f"data/01_raw/{file_name}.xlsx"
               data = pd.read_excel(path, header=None, names=['id','job'])
     return data

def fetch_tables_spreadsheet(urls_dict: Dict):
     for key,value in urls_dict.items():
          logger.info(f"Fetching data {key} table")
          table = get_data(value, key, testing=True)
          table['_timestamp'] = datetime.datetime.now()
          if key == "hired_employees":
               hired_employees_df = table
               hired_employees_df = apply_schema(hired_employees_df, HiredEmployees)
               HiredEmployees.validate(hired_employees_df)
          elif key == "departments":
               departments_df = table
               departments_df = apply_schema(departments_df, Departments)
               Departments.validate(departments_df)
          else:
               jobs_df = table
               jobs_df = apply_schema(jobs_df, Jobs)
               Jobs.validate(jobs_df)


     return hired_employees_df, departments_df, jobs_df