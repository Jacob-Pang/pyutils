import cloudpickle
import inspect
import os
import pickle
import shutil
import pandas as pd
import pyutils.graph_dataframe.dtype_encoder as dtype_encoders

DTYPE_ENCODERS = {
    dtype_encoder() for _, dtype_encoder in inspect.getmembers(dtype_encoders, inspect.isclass)
    if isinstance(dtype_encoder(), dtype_encoders.BaseDtypeEncoder)
}

class DtypeSchema:
    class UnrecognizedDtypeException (Exception):
        def __init__(self, data: any) -> None:
            super().__init__(f"Could not find matching dtype encoder for data object {data}.")

    def __init__(self, pdf: pd.DataFrame) -> None:
        self.dtype_encoder_schema = dict()
        self.index_dtype_encoder = None

        for column in pdf.columns:
            for dtype_encoder in DTYPE_ENCODERS:
                if dtype_encoder.has_base_dtype(pdf[column]):
                    self.dtype_encoder_schema[column] = dtype_encoder
                    break
            
            if column not in self.dtype_encoder_schema:
                raise DtypeSchema.UnrecognizedDtypeException(pdf[column])

        for dtype_encoder in DTYPE_ENCODERS:
            if dtype_encoder.has_base_dtype(pdf.index):
                self.index_dtype_encoder = dtype_encoder
                break
        
        if not self.index_dtype_encoder:
            raise DtypeSchema.UnrecognizedDtypeException(pdf.index)

    def get(self, column: str) -> dtype_encoders.BaseDtypeEncoder:
        return self.dtype_encoder_schema.get(column)

    def encode_dtype(self, pdf: pd.DataFrame) -> pd.DataFrame:
        for column in pdf.columns:
            pdf[column] = self.dtype_encoder_schema.get(column).encode_dtype(pdf[column])

        pdf.index = self.index_dtype_encoder.encode_dtype(pdf.index)
        return pdf

    def decode_dtype(self, pdf: pd.DataFrame) -> pd.DataFrame:
        for column in pdf.columns:
            pdf[column] = self.dtype_encoder_schema.get(column).decode_dtype(pdf[column])
        
        pdf.index = self.index_dtype_encoder.decode_dtype(pdf.index)
        return pdf

class GraphDataFrameSchema:
    def __init__(self, dtype_schema: DtypeSchema, partition_columns: list = list()) -> None:
        self.dtype_schema = dtype_schema
        self.partition_columns = partition_columns
        self.partition_paths = set()

    def get_partition_path(self, partition_column_values: tuple = tuple()) -> str:
        return os.path.join(*[
            f"{column}={self.dtype_schema.get(column).to_string(column_value)}"
            for column, column_value in zip(self.partition_columns, partition_column_values)
        ])

    def add_partition_path(self, partition_path: str) -> None:
        self.partition_paths.add(partition_path)

    def get_partition_column_values(self, partition_path: str) -> dict:
        partition_column_values = dict()

        for partition_predicate in partition_path.split(os.path.sep):
            column, column_value = partition_predicate.split('=')
            partition_column_values[column] = self.dtype_schema.get(column).from_string(column_value)

        return partition_column_values

class GraphDataFrame:
    @staticmethod
    def get_schema_file_path(root_file_path: str) -> str:
        return os.path.sep.join([root_file_path, "_schema.cp"])
    
    @staticmethod
    def get_data_file_path(root_file_path: str, partition_path: str = None) -> str:
        if not partition_path:
            return os.path.join(root_file_path, "_data.csv")
        
        return os.path.join(root_file_path, partition_path, "_data.csv")

    @staticmethod
    def concat_drop_duplicates(pdf: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([pdf, other]).drop_duplicates()

    def file_path_exists(self, file_path: str) -> bool:
        return os.path.exists(file_path)

    def remove_file_path(self, file_path: str) -> None:
        shutil.rmtree(file_path)

    def save_data_to_file_path(self, pdf: pd.DataFrame, file_path: str) -> None:
        if not self.file_path_exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        
        pdf.to_csv(file_path, index=True)

    def save_schema_to_file_path(self, graph_schema: GraphDataFrameSchema, file_path: str) -> None:
        if not self.file_path_exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        with open(file_path, "wb") as schema_file:
            cloudpickle.dump(graph_schema, schema_file, protocol=pickle.HIGHEST_PROTOCOL)

    def read_data_from_file_path(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path, index_col=0)

    def read_schema_from_file_path(self, file_path: str) -> GraphDataFrameSchema:
        with open(file_path, "rb") as schema_file:
            return cloudpickle.load(schema_file)

    def save_dataframe(self, pdf: pd.DataFrame, to_file_path: str, partition_columns: list = list()) -> None:
        if self.file_path_exists(to_file_path):
            self.remove_file_path(to_file_path) # Remove existing dataframe graph

        graph_schema = GraphDataFrameSchema(DtypeSchema(pdf), partition_columns)

        if partition_columns:
            for partition_column_values, partition_pdf in pdf.groupby(by=partition_columns):
                if not isinstance(partition_column_values, tuple):
                    partition_column_values = tuple([partition_column_values])

                partition_path = graph_schema.get_partition_path(partition_column_values)
                partition_file_path = self.get_data_file_path(to_file_path, partition_path)
                encoded_partition_pdf = graph_schema.dtype_schema.encode_dtype(partition_pdf)

                graph_schema.add_partition_path(partition_path)
                self.save_data_to_file_path(encoded_partition_pdf, partition_file_path)
        else:
            encoded_pdf = graph_schema.dtype_schema.encode_dtype(pdf)
            dataframe_file_path = self.get_data_file_path(to_file_path)
            self.save_data_to_file_path(encoded_pdf, dataframe_file_path)
        
        schema_file_path = self.get_schema_file_path(to_file_path)
        self.save_schema_to_file_path(graph_schema, schema_file_path)

    def merge_dataframe(self, pdf: pd.DataFrame, to_file_path: str, merge_function: callable = concat_drop_duplicates):
        if not self.file_path_exists(to_file_path):
            return self.save_dataframe(pdf)

        graph_schema = self.read_schema_from_file_path(self.get_schema_file_path(to_file_path))
        
        if graph_schema.partition_columns:
            for partition_column_values, partition_pdf in pdf.groupby(by=graph_schema.partition_columns):
                if not isinstance(partition_column_values, tuple):
                    partition_column_values = tuple([partition_column_values])

                partition_path = graph_schema.get_partition_path(partition_column_values)
                partition_file_path = self.get_data_file_path(to_file_path, partition_path)
                encoded_partition_pdf = graph_schema.dtype_schema.encode_dtype(partition_pdf)

                if partition_path in graph_schema.partition_paths:
                    encoded_partition_pdf = merge_function(
                        self.read_data_from_file_path(partition_file_path),
                        encoded_partition_pdf
                    )

                graph_schema.add_partition_path(partition_path)
                self.save_data_to_file_path(encoded_partition_pdf, partition_file_path)
        else:
            dataframe_file_path = self.get_data_file_path(to_file_path)
            encoded_pdf = merge_function(
                self.read_data_from_file_path(dataframe_file_path),
                graph_schema.dtype_schema.encode_dtype(pdf)
            )
            
            self.save_data_to_file_path(encoded_pdf, dataframe_file_path)
        
        schema_file_path = self.get_schema_file_path(to_file_path)
        self.save_schema_to_file_path(graph_schema, schema_file_path)

    def read_dataframe(self, from_file_path: str, query_predicates: set = set()) -> None:
        graph_schema = self.read_schema_from_file_path(self.get_schema_file_path(from_file_path))

        if not graph_schema.partition_columns:
            dataframe_file_path = self.get_data_file_path(from_file_path)
            return graph_schema.dtype_schema.decode_dtype(self.read_data_from_file_path(dataframe_file_path))
        
        partition_pdfs = list()

        def query_partition(partition_path: str) -> None:
            partition_column_values = graph_schema.get_partition_column_values(partition_path)

            for query_predicate in query_predicates:
                if not query_predicate(partition_column_values):
                    return

            partition_file_path = self.get_data_file_path(from_file_path, partition_path)
            encoded_partition_pdf = self.read_data_from_file_path(partition_file_path)
            decoded_partition_pdf = graph_schema.dtype_schema.decode_dtype(encoded_partition_pdf)

            for column, column_value in partition_column_values:
                decoded_partition_pdf[column] = column_value

            partition_pdfs.append(decoded_partition_pdf)

        for partition_path in graph_schema.partition_paths:
            query_partition(partition_path)
        
        return pd.concat(partition_pdfs, axis=0)

if __name__ == "__main__":
    pass