import pandas as pd

from pyutils.database.artifact import Artifact
from pyutils.graph_dataframe import GraphDataFrameInterface
from pyutils.wrappers import get_compat_kwargs

class DataFrame (Artifact):
    def save_data_to_path(self, artifact_data: pd.DataFrame, path: str, **kwargs) -> None:
        kwargs = get_compat_kwargs(artifact_data.to_csv, **kwargs)
        artifact_data.to_csv(path, index=True, **kwargs)
    
    def read_data_from_path(self, path: str, **kwargs) -> pd.DataFrame:
        kwargs = get_compat_kwargs(pd.read_csv, **kwargs)
        return pd.read_csv(path, index_col=0, **kwargs)

    def merge_function(self, artifact_data: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([artifact_data, other], axis=0).drop_duplicates(ignore_index=True, keep="last")
    
    def update_data(self, artifact_data: any, **kwargs) -> None:
        artifact_data = self.merge_function(self.read_data(**kwargs), artifact_data)
        self.save_data(artifact_data, **kwargs)

    def __str__(self) -> str:
        return "DATAFRAME"

class GraphDataFrame (DataFrame):
    def save_data_to_path(self, artifact_data: any, path: str, partition_columns: list = list(), **kwargs) -> None:
        GraphDataFrameInterface().save_dataframe(artifact_data, path, partition_columns)

    def read_data_from_path(self, path: str, query_predicates: set = set(), **kwargs):
        return GraphDataFrameInterface().read_dataframe(path, query_predicates)

    def update_data(self, artifact_data: any, **kwargs) -> None:
        return GraphDataFrameInterface().merge_dataframe(artifact_data, self.get_node_path(),
                merge_function=self.merge_function)

if __name__ == "__main__":
    pass
