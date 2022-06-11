import pandas as pd

from pandas.api.types import is_period_dtype, is_datetime64_any_dtype

class ReducedDtypeBase:
    @staticmethod
    def has_complex_dtype(dtype_object: pd.Series) -> bool:
        raise NotImplementedError()

    def __init__(self, pds: pd.Series) -> None:
        self.origin_dtype = pds.dtype

    def apply(self, complex_dtype_obj: any) -> any:
        if isinstance(complex_dtype_obj, pd.Series):
            return self.apply_on_series(complex_dtype_obj)

        return self.apply_on_value(complex_dtype_obj)

    def reverse(self, reduced_dtype_obj: any) -> any:
        if isinstance(reduced_dtype_obj, pd.Series):
            return self.reverse_on_series(reduced_dtype_obj)

        return self.reverse_on_value(reduced_dtype_obj)

    def apply_on_series(self, complex_dtype_pds: pd.Series) -> pd.Series:
        raise NotImplementedError()

    def reverse_on_series(self, reduced_dtype_pds: pd.Series) -> pd.Series:
        raise NotImplementedError()
    
    def apply_on_value(self, complex_dtype_obj: any) -> any:
        raise NotImplementedError()

    def reverse_on_value(self, reduced_dtype_obj: any) -> any:
        raise NotImplementedError()

class ReducedDatetimeDtype (ReducedDtypeBase):
    REDUCED_DATETIME_FORMAT = r"%Y-%m-%d-%H-%M-%S-%f"

    @staticmethod
    def has_complex_dtype(pds: pd.Series) -> bool:
        return is_datetime64_any_dtype(pds)

    def apply_on_series(self, complex_dtype_pds: pd.Series) -> pd.Series:
        return complex_dtype_pds.dt.strftime(self.REDUCED_DATETIME_FORMAT)

    def reverse_on_series(self, reduced_dtype_pds: pd.Series) -> pd.Series:
        return pd.to_datetime(reduced_dtype_pds, format=self.REDUCED_DATETIME_FORMAT)

    def apply_on_value(self, complex_dtype_obj: pd.Timestamp) -> str:
        return complex_dtype_obj.strftime(self.REDUCED_DATETIME_FORMAT)

    def reverse_on_value(self, reduced_dtype_obj: str) -> pd.Timestamp:
        return pd.to_datetime(reduced_dtype_obj, format=self.REDUCED_DATETIME_FORMAT)

class ReducedPeriodDtype (ReducedDatetimeDtype):
    @staticmethod
    def has_complex_dtype(pds: pd.Series) -> bool:
        return is_period_dtype(pds)

    def __init__(self, pds: pd.Series) -> None:
        ReducedDatetimeDtype.__init__(self, pds)
        self.frequency = pds.dt.freq

    def apply_on_series(self, complex_dtype_pds: pd.Series) -> pd.Series:
        datetime_pds = complex_dtype_pds.dt.to_timestamp()
        return ReducedDatetimeDtype.apply_on_series(self, datetime_pds)

    def reverse_on_series(self, reduced_dtype_pds: pd.Series) -> pd.Series:
        datetime_pds = ReducedDatetimeDtype.reverse_on_series(self, reduced_dtype_pds)
        return datetime_pds.dt.to_period(freq=self.frequency)

    def apply_on_value(self, complex_dtype_obj: pd.Period) -> str:
        return ReducedDatetimeDtype.apply_on_value(self,
                complex_dtype_obj.to_timestamp())

    def reverse_on_value(self, reduced_dtype_obj: str) -> pd.Period:
        return ReducedDatetimeDtype.reverse_on_value(self, reduced_dtype_obj) \
                .to_period(self.frequency)

if __name__ == "main":
    pass
