import pandas as pd

from collections.abc import Iterable
from .artifact import Artifact
from .dataframe import DataFrame

class DataFrameQuery (DataFrame):
    def __init__(self, data_node_id: str, connected_dataframe: DataFrame,
        connection_dpath: str = None, description: str = None,
        parent_database: any = None, **field_kwargs) -> None:

        self.connected_dataframe = connected_dataframe
        super().__init__(data_node_id, connection_dpath, description,
                parent_database, **field_kwargs)

    def get_node_path(self) -> str:
        return None # Queries do not have assigned paths

    def save_data(self, artifact_data: pd.DataFrame, *args, **kwargs) -> None:
        pass # No saving permissions

    def read_data(self, *args, **kwargs) -> pd.DataFrame:
        return self.connected_dataframe.read_data(*args, **kwargs)

    def update_data(self, *args, **kwargs) -> None:
        pass # No update permissions

    def __str__(self) -> str:
        return "QUERY"

class MapQuery (DataFrameQuery):
    def __init__(self, data_node_id: str, connected_dataframe: DataFrame,
        field_maps: dict, connection_dpath: str = None, description: str = None,
        parent_database: any = None, **field_kwargs) -> None:
        """
        Parameters:
            field_maps (dict): The mapping of {field_name: field_map} where
                    field_map can be either:
                    (dict): mapping of {field_value: mapped_field_value}.
                    (Artifact): A dictionary artifact encoding the (dict).
                    (PandasDataFrame): Assumed of the format where the field_values is stored in
                            the first* column and the mapped_field_values in the second* column.
                    * DataFrames are read with an assumption of zeroth Index column.
        """
        self.field_maps = field_maps
        super().__init__(data_node_id, connected_dataframe, connection_dpath,
                description, parent_database, **field_kwargs)

    def get_map_field_to_value(self, field_map: any, *args, **kwargs) -> dict:
        if isinstance(field_map, dict):
            return field_map

        if isinstance(field_map, DataFrame):
            field_groupby_pdf = field_map.read_data(*args, **kwargs)
            field_values_column = field_groupby_pdf.columns[0]
            field_groups_column = field_groupby_pdf.columns[1]

            return field_groupby_pdf.set_index(field_values_column) \
                    [field_groups_column].to_dict()

        if isinstance(field_map, Artifact):
            return field_map.read_data(*args, **kwargs)

        return None
    
    def read_data(self, *args, **kwargs) -> pd.DataFrame:
        query_dataframe = DataFrameQuery.read_data(self, *args, **kwargs)

        for field_name, field_map in self.field_maps.items():
            map_field_to_value = self.get_map_field_to_value(field_map)
            query_dataframe.loc[:, field_name] = query_dataframe[field_name] \
                    .map(map_field_to_value)

        return query_dataframe

class GroupByQuery (DataFrameQuery):
    def __init__(self, data_node_id: str, connected_dataframe: DataFrame,
        groupby_fields: Iterable = None, connection_dpath: str = None,
        description: str = None, parent_database: any = None, **field_kwargs) -> None:
        """
        """
        self.groupby_fields = groupby_fields
        DataFrameQuery.__init__(self, data_node_id, connected_dataframe, connection_dpath,
            description, parent_database, **field_kwargs)

    def aggregate_query(self, query_dataframe: any) -> pd.DataFrame:
        # Groupby.sum by default
        return query_dataframe.sum()
        
    def read_data(self, *args, **kwargs) -> pd.DataFrame:
        query_dataframe = DataFrameQuery.read_data(self, *args, **kwargs)
        
        # Group dataframe by non-numeric fields
        groupby_fields = self.groupby_fields if self.groupby_fields else \
                query_dataframe.columns[~query_dataframe.columns.isin(
                    query_dataframe.select_dtypes("number").columns
                )].to_list()

        return self.aggregate_query(query_dataframe.groupby(groupby_fields))

if __name__ == "__main__":
    pass
