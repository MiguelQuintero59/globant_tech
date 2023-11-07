from typing import Any, Dict, Union
import logging
import pandas as pd
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from kedro.extras.datasets.pandas import GBQQueryDataSet, GBQTableDataSet

logger = logging.getLogger(__name__)

class GBQTokenTableDataSet(GBQTableDataSet):
    def __init__(
        self,
        dataset: str,
        table_name: str,
        staging_dataset: str = None,
        project: str = None,
        credentials: Union[Dict[str, Any], Credentials] = None,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ):
        if isinstance(credentials, dict):
            credentials = service_account.Credentials.from_service_account_info(
                credentials
            )

        self._staging_dataset = staging_dataset
        super().__init__(
            dataset=dataset,
            table_name=table_name,
            project=project,
            credentials=credentials,
            load_args=load_args,
            save_args=save_args,
        )

    def _save(
        self, data: Union[pd.DataFrame, Dict[str, Union[pd.DataFrame, str]]]
    ) -> None:

        if_exists = self._save_args.pop("if_exists", "fail")
        if data.empty:
            logger.info("No data to save")
            return
        data.to_gbq(
            f"{self._dataset}.{self._table_name}",
            project_id=self._project_id,
            credentials=self._credentials,
            if_exists=if_exists,
            **self._save_args,
        )

class GBQTokenQueryDataSet(GBQQueryDataSet):
    def __init__(
        self,
        sql: str = None,
        project: str = None,
        credentials: Union[Dict[str, Any], Credentials] = None,
        load_args: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
        filepath: str = None,
    ) -> None:
        if isinstance(credentials, dict):
            credentials = service_account.Credentials.from_service_account_info(
                credentials
            )

        super().__init__(
            sql=sql,
            project=project,
            credentials=credentials,
            load_args=load_args,
            fs_args=fs_args,
            filepath=filepath,
        )