import pandas as pd

from collections.abc import Iterable
from pyutils.database.data_node import DataNode
from pyutils.database.dataframe import DataFrame

class DataFrameTransformer (DataNode):
    def __init__(self, data_node_id: str, connected_dataframe: DataFrame, connection_dpath: str = None,
        description: str = None, parent_database: any = None, **field_kwargs) -> None:
        super().__init__(data_node_id, connection_dpath, description, parent_database, **field_kwargs)
        self.connected_dataframe = connected_dataframe

    def get_node_path(self) -> str:
        return None
    
    def destroy_node(self, **kwargs) -> None:
        return

    def read_data(self, **kwargs) -> any:
        return self.connected_dataframe.read_data(**kwargs)

    def __str__(self) -> str:
        return "TRANSFORMER"

class MapTransformer (DataFrameTransformer):
    def __init__(self, data_node_id: str, connected_dataframe: DataFrame, mapper: any,
        key_field_name: str, value_field_name: str = None, connection_dpath: str = None,
        description: str = None, parent_database: any = None, **field_kwargs) -> None:
        """
        Parameters:
            key_field_name (str): The column name to use the values as keys for the map.
            value_field_name (str, opt): The column name to apply the mapped values on.
                Defaults to <key_field_name>.
            mapper (any):
                (dict): Dictionary object with signature {field_key_value: field_mapped_value}.
                (Artifact): Artifact object that returns a dictionary with the same requirements
                        as field_map (dict) OR a pandas.DataFrame with columns <key_field_name>
                        and <value_field_name>.
        """
        super().__init__(data_node_id, connected_dataframe, connection_dpath,
                description, parent_database, **field_kwargs)

        self.key_field_name = key_field_name
        self.value_field_name = value_field_name
        self.mapper = mapper

    def get_mapper(self, **kwargs) -> dict:
        if isinstance(self.mapper, dict):
            return self.mapper
        
        data = self.mapper.read_data(**kwargs)

        if isinstance(data, dict):
            return data

        if isinstance(data, pd.DataFrame):
            return data.set_index(self.key_field_name, drop=False)[self.value_field_name].to_dict()
        
        raise Exception()
    
    def read_data(self, **kwargs) -> pd.DataFrame:
        pdf = DataFrameTransformer.read_data(self, **kwargs)
        pdf[:, self.value_field_name] = pdf[self.key_field_name].map(self.get_mapper(**kwargs))
        
        return pdf

class GroupByTransformer (DataFrameTransformer):
    def __init__(self, data_node_id: str, connected_dataframe: DataFrame, groupby_field_names: Iterable = None,
        connection_dpath: str = None, description: str = None, parent_database: any = None, **field_kwargs) -> None:
        """
        """
        self.groupby_field_names = groupby_field_names
        DataFrameTransformer.__init__(self, data_node_id, connected_dataframe, connection_dpath,
                description, parent_database, **field_kwargs)

    def aggregate_query(self, pdf: any) -> pd.DataFrame:
        # Groupby.sum by default
        return pdf.sum()
        
    def read_data(self, **kwargs) -> pd.DataFrame:
        pdf = DataFrameTransformer.read_data(self, **kwargs)
        
        # Group dataframe by non-numeric fields
        groupby_field_names = self.groupby_field_names if self.groupby_field_names \
                else pdf.columns[~pdf.columns.isin(pdf.select_dtypes("number").columns)].to_list()

        return self.aggregate_query(pdf.groupby(groupby_field_names))

if __name__ == "__main__":
    pass
