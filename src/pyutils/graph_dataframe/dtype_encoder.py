import pandas as pd
from pandas.api.types import is_integer_dtype, is_string_dtype, is_float_dtype

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



if __name__ == "__main__":
    pass