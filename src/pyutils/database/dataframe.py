import glob
import os
import pandas as pd

from pyutils.database.artifact import Artifact
from pyutils.pandas_reduced_dtype.reduced_schema import ReducedDataFrameSchema

class DataFrame (Artifact):
    def __init__(self, data_node_id: str, connection_dpath: str = os.getcwd(),
        description: str = None, parent_database: any = None, **field_kwargs) -> None:
        self.dataframe_schema = None
        super().__init__(data_node_id, connection_dpath, description,
                parent_database, **field_kwargs)

    def save_data(self, artifact_data: pd.DataFrame, *args, **kwargs) -> None:
        self.dataframe_schema = ReducedDataFrameSchema()
        self.dataframe_schema.set_reduced_schema(artifact_data)
        self.dataframe_schema.apply_reduced_schema(artifact_data, inplace=True)

        return Artifact.save_data(self, artifact_data, *args, **kwargs)

    def save_data_to_path(self, artifact_data: pd.DataFrame, path: str, *args, **kwargs) -> None:
        artifact_data.to_csv(path, index=False)
    
    def read_data(self, *args, **kwargs) -> pd.DataFrame:
        artifact_data = Artifact.read_data(self, *args, **kwargs)

        if self.dataframe_schema: # Reset origin dtypes
            self.dataframe_schema.reverse_reduced_schema(
                    artifact_data, inplace=True)

        return artifact_data

    def read_data_from_path(self, path: str, *args, **kwargs) -> pd.DataFrame:
        return pd.read_csv(path, *args, **kwargs)

    def update_data(self, artifact_data: pd.DataFrame, *args, ignore_index: bool = True,
        **kwargs) -> None:
        artifact_data = pd.concat([self.read_data(), artifact_data], axis=0)
        self.drop_duplicates(artifact_data, ignore_index=ignore_index)
        self.save_data(artifact_data, *args, **kwargs)
    
    def drop_duplicates(self, artifact_data: pd.DataFrame, ignore_index: bool = True) -> None:
        artifact_data.drop_duplicates(inplace=True, ignore_index=ignore_index,
                keep="last")

    def destroy_node(self, *args, **kwargs) -> None:
        Artifact.destroy_node(self, *args, **kwargs)
        self.dataframe_schema = None

    def __str__(self) -> str:
        return "DATAFRAME"

class ParquetDataFrame (DataFrame):
    def __init__(self, data_node_id: str, connection_dpath: str = os.getcwd(),
        description: str = None, parent_database: any = None, **field_kwargs) -> None:
        self.partition_columns  = None
        super().__init__(data_node_id, connection_dpath, description,
                parent_database, **field_kwargs)

    def get_partition_path(self, partition_field_values: tuple) -> str:
        print(partition_field_values)
        return os.path.join(self.get_node_path(), *[
            f"{partition_column}={partition_field_value}"
            for partition_column, partition_field_value in zip(self.partition_columns,
                    partition_field_values)
        ])

    def apply_schema_to_filters(self, filters: list) -> list:
        reduced_filters = []

        for field_name, filter_operator, filter_values in filters:
            if field_name in self.dataframe_schema.map_column_to_rdtype:
                reduced_dtype = self.dataframe_schema.map_column_to_rdtype[field_name]
                filter_values = [
                    reduced_dtype.apply_on_value(filter_value) for filter_value
                    in filter_values
                ] if isinstance(filter_values, (tuple, list)) else \
                    reduced_dtype.apply_on_value(filter_values)

            reduced_filters.append((field_name, filter_operator, filter_values))

        return reduced_filters

    def filter_partition_path(self, partition_file_path: str, filters: list) -> bool:
        # Parameter <filters> must be in un-reduced dtype formats
        for field_name, filter_operator, filter_value in filters:
            if field_name not in self.partition_columns:
                continue # Not partitioned by field

            field_value = partition_file_path.split(f"{field_name}=")[1].split('/')[0]

            if field_value.replace('.', '').isdigit(): # Convert numeric field values
                field_value = float(field_value) if '.' in field_value \
                        else int(field_value)

            if self.dataframe_schema and field_name in self.dataframe_schema \
                .map_column_to_rdtype: # Reverse reduced dtype for comparison
                field_value = self.dataframe_schema.map_column_to_rdtype[field_name] \
                        .reverse_on_value(field_value)

            if (filter_operator == "not in" and field_value in filter_value) or \
                (filter_operator == "in" and field_value not in filter_value) or \
                (filter_operator == '='  and field_value != filter_value) or \
                (filter_operator == '==' and field_value != filter_value) or \
                (filter_operator == '!=' and field_value == filter_value) or \
                (filter_operator == '>'  and field_value <= filter_value) or \
                (filter_operator == '>=' and field_value <  filter_value) or \
                (filter_operator == '<'  and field_value >= filter_value) or \
                (filter_operator == '<=' and field_value >  filter_value):
                return False

        return True

    def save_data(self, artifact_data: pd.DataFrame, *args, partition_columns: list = None,
        **kwargs) -> None:
        self.partition_columns = partition_columns
        return DataFrame.save_data(self, artifact_data, *args,
                partition_cols=self.partition_columns, **kwargs)

    def save_data_to_path(self, artifact_data: pd.DataFrame, path: str,
        partition_cols: list = None, *args, **kwargs) -> None:
        artifact_data.to_parquet(path, partition_cols=partition_cols)

        """ to be evaluated if renaming is required.
        if not os.path.isdir(path):
            return
        
        # Partitioned dataframe
        file_paths = list(glob.glob(os.path.join(path, "**"), recursive=True))

        for file_path in file_paths: # Rename partitioned files
            dpath, _ = os.path.split(file_path)
            os.rename(file_path, os.path.join(dpath, f"{self.data_node_id}.parquet"))
        """

    def read_data(self, *args, filters: list = None, **kwargs) -> pd.DataFrame:
        if filters: # Apply reduced dtyping to values
            filters = self.apply_schema_to_filters(filters)

        return DataFrame.read_data(self, *args, filters=filters, **kwargs)

    def read_data_from_path(self, path: str, *args, filters: list = None,
        **kwargs) -> pd.DataFrame:
        return pd.read_parquet(path, *args, filters=filters, **kwargs)

    def update_data(self, artifact_data: pd.DataFrame, *args, **kwargs) -> None:
        if not self.partition_columns:
            return DataFrame.update_data(self, artifact_data, *args, **kwargs)

        self.dataframe_schema.apply_reduced_schema(artifact_data, inplace=True)
        
        # Partitioning update optimization
        for partition_field_values, partition_artifact_data in artifact_data.groupby(
            by=self.partition_columns):
            partition_dpath = self.get_partition_path(partition_field_values)

            if os.path.exists(partition_dpath): # Previous partition data exsists for group
                partition_file_path = glob.glob(os.path.join(partition_dpath, "*.parquet"))[0]
                partition_artifact_data = self.drop_duplicates(
                    pd.concat([partition_artifact_data, self.read_data_from_path(
                            partition_file_path, *args, **kwargs)], axis=0)
                )

                os.remove(partition_file_path)

            self.save_data_to_path(partition_artifact_data, self.get_node_path(),
                    partition_cols=self.partition_columns, *args, **kwargs)

    def destroy_node(self, *args, **kwargs) -> None:
        DataFrame.destroy_node(self, *args, **kwargs)
        self.partition_columns = None

if __name__ == "__main__":
    pass
