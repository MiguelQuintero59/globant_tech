import datetime
import pandas as pd
import pandera as pa
from pandera.typing import Series

class HiredEmployees(pa.SchemaModel):
    id: Series[int] = pa.Field(nullable=True)
    name: Series[pd.StringDtype] = pa.Field(nullable=True)
    datetime: Series[pd.StringDtype] = pa.Field(nullable=True)
    department_id: Series[pa.Float] = pa.Field(nullable=True)
    job_id: Series[pa.Float] = pa.Field(nullable=True)
    _timestamp: Series[pa.Timestamp] = pa.Field(nullable=False)

class Departments(pa.SchemaModel):
    id: Series[int] = pa.Field(nullable=True)
    department: Series[pd.StringDtype] = pa.Field(nullable=True)
    _timestamp: Series[pa.Timestamp] = pa.Field(nullable=False)

class Jobs(pa.SchemaModel):
    id: Series[int] = pa.Field(nullable=True)
    job: Series[pd.StringDtype] = pa.Field(nullable=True)
    _timestamp: Series[pa.Timestamp] = pa.Field(nullable=False)