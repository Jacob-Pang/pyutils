import pandas as pd
from pandas.api.types import is_integer_dtype, is_string_dtype, is_float_dtype, \
        is_datetime64_any_dtype, is_period_dtype

class BaseDtypeEncoder:
    def has_base_dtype(self, data: any) -> bool:
        return False

    def has_encoded_dtype(self, data: any) -> bool:
        return self.has_base_dtype(data)

    def encode_dtype(self, data: any) -> (int | str | float):
        return data
    
    def decode_dtype(self, data: (int | str | float)) -> any:
        return data

    def to_string(self, data: any) -> str:
        return str(data)
    
    def from_string(self, data: str) -> any:
        raise NotImplementedError()

class IntDtypeEncoder (BaseDtypeEncoder):
    def has_base_dtype(self, data: any) -> bool:
        return is_integer_dtype(data)
    
    def from_string(self, data: str) -> any:
        return int(data)

class StringDtypeEncoder (BaseDtypeEncoder):
    def has_base_dtype(self, data: any) -> bool:
        return is_string_dtype(data)

    def from_string(self, data: str) -> any:
        return data

class FloatDtypeEncoder (BaseDtypeEncoder):
    def has_base_dtype(self, data: any) -> bool:
        return is_float_dtype(data)
    
    def from_string(self, data: str) -> any:
        return float(data)

class DateTimeDtypeEncoder (BaseDtypeEncoder):
    ENCODED_DATETIME_FORMAT = r"%Y-%m-%d-%H-%M-%S-%f"

    def has_base_dtype(self, data: any) -> bool:
        return is_datetime64_any_dtype(data)

    def encode_dtype(self, data: any) -> (int | str | float):
        return data.dt.strftime(self.ENCODED_DATETIME_FORMAT) if isinstance(data, pd.Series) \
                else data.strftime(self.ENCODED_DATETIME_FORMAT)
    
    def decode_dtype(self, data: (int | str | float)) -> any:
        return pd.to_datetime(data, format=self.ENCODED_DATETIME_FORMAT)

    def to_string(self, data: any) -> str:
        return self.encode_dtype(data)
    
    def from_string(self, data: str) -> any:
        return self.decode_dtype(data)

class PeriodDtypeEncoder (DateTimeDtypeEncoder):
    def has_base_dtype(self, data: any) -> bool:
        return is_period_dtype(data)
    
    def encode_dtype(self, data: any) -> (int | str | float):
        freqstr = data.dt.freq.freqstr if isinstance(data, pd.Series) else data.freqstr
        datetime_data = data.dt.to_timestamp() if isinstance(data, pd.Series) else data.to_timestamp()
        encoded_data = super().encode_dtype(datetime_data)

        return encoded_data + f"={freqstr}"

    def decode_dtype(self, data: (int | str | float)) -> pd.Period:
        if isinstance(data, str):
            data, freqstr = data.split('=')
            return super().decode_dtype(data).to_period(freqstr)

        freqstr = data.iloc[0].split('=')[1]
        data = data.str.rstrip(f"={freqstr}")
        decoded_data = super().decode_dtype(data)

        return decoded_data.dt.to_period(freqstr) if isinstance(data, pd.Series) \
                else decoded_data.to_period(freqstr)

if __name__ == "__main__":
    pass