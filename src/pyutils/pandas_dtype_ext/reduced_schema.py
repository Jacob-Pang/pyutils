import inspect
import pandas as pd

from pyutils.pandas_dtype_ext import reduced_dtype as dtypes
from pyutils.pandas_dtype_ext.reduced_dtype import ReducedDtypeBase

REDUCED_DTYPES = [
    dtype for _, dtype in inspect.getmembers(dtypes)
    if inspect.isclass(dtype) and dtype != ReducedDtypeBase
]

class ReducedDataFrameSchema:
    def __init__(self) -> None:
        self.index_rdtype = None
        self.map_column_to_rdtype = {}

    def set_reduced_schema(self, pdf: pd.DataFrame) -> None:            
        for column in pdf.columns:
            for reduced_dtype in REDUCED_DTYPES:
                if reduced_dtype.has_complex_dtype(pdf[column]):
                    self.map_column_to_rdtype[column] = reduced_dtype(pdf[column])

        pdf_index = pdf.index.to_series()

        for reduced_dtype in REDUCED_DTYPES:
            if reduced_dtype.has_complex_dtype(pdf_index):
                self.index_rdtype = reduced_dtype(pdf_index)

    def apply_reduced_schema(self, pdf: pd.DataFrame, inplace: bool = False) -> any:
        if not inplace: # Create copy to prevent overriding of values
            cloned_pdf = pdf.copy()
            self.apply_reduced_schema(cloned_pdf, inplace=True)
            return cloned_pdf

        # Ensure column names are string values
        pdf.columns = pdf.columns.astype(str)

        # Tag Index name to enable reverse operation to identify Index column
        pdf.index.rename(f"{pdf.index.name}_RdfsIndex" if pdf.index.name
                else "RdfsIndex", inplace=True)

        pdf_index_name = pdf.index.name
        pdf.reset_index(inplace=True)

        if self.index_rdtype:
            pdf.loc[:, pdf_index_name] = self.index_rdtype.apply(pdf[pdf_index_name])

        for column, reduced_dtype in self.map_column_to_rdtype.items():
            if column in pdf.columns:
                pdf[column] = reduced_dtype.apply(pdf[column])

    def reverse_reduced_schema(self, pdf: pd.DataFrame, inplace: bool = False) -> any:
        if not inplace: # Create copy to prevent overriding of values
            cloned_pdf = pdf.copy()
            self.reverse_reduced_schema(cloned_pdf, inplace=True)
            return cloned_pdf

        pdf_index_name = None
        for column in pdf.columns: # Search for Index
            if "RdfsIndex" in column:
                pdf_index_name = column
                break

        if pdf_index_name: # Index identified
            if self.index_rdtype:
                pdf.loc[:, pdf_index_name] = self.index_rdtype.reverse(
                        pdf[pdf_index_name])
            
            pdf.set_index(pdf_index_name, inplace=True)
            pdf.index.rename(pdf_index_name.replace("_RdfsIndex", ''),
                    inplace=True)

        for column, reduced_dtype in self.map_column_to_rdtype.items():
            if column in pdf.columns:
                pdf.loc[:, column] = reduced_dtype.reverse(pdf[column])

if __name__ == "main":
    pass
