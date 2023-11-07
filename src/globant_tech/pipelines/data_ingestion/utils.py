import numpy as np
import pandas as pd
import pandera as pa

def apply_schema(df: pd.DataFrame, schema: pa.SchemaModel) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        _schema_col = schema.to_schema().columns.get(col)
        if _schema_col is None:
            continue
        _type = _schema_col.dtype.type
        if _type == np.float64:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
        elif _type == np.int64:
            df[col] = pd.to_numeric(df[col], downcast="integer", errors="coerce")
            df[col] = df[col].astype("Int64")
        elif _type == np.dtype("datetime64[ns]"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif _type == np.dtype("<U") or _type == pd.StringDtype():
            df[col] = df[col].astype(str)
            df[col] = df[col].astype("string").replace("<NA>", pd.NA).replace("nan", pd.NA).replace("None", pd.NA)
        elif _type == np.dtype("bool") or _type == pd.BooleanDtype():
            df[col] = df[col].astype(str)
            df[col] = df[col].str.lower().replace("false", False).replace("true", True).replace("nan", pd.NA)
            df[col] = df[col].astype("boolean")
        else:
            df[col] = df[col].astype(_type)
    # Silence a pandas warning about a fragmented dataframe
    return df.copy()