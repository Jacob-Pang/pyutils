import pandas as pd

class BaseQueryPredicate:
    def evaluate_column_values(self, column_values: dict) -> bool:
        return True
    
    def evaluate_dataframe(self, pdf: pd.DataFrame) -> pd.DataFrame:
        return pd.Series([False] * pdf.shape[0])

    def query_dataframe(self, pdf: pd.DataFrame) -> pd.DataFrame:
        return pdf[self.evaluate_dataframe(pdf)]

class In (BaseQueryPredicate):
    def __init__(self, column: str, values: set):
        self.column = column
        self.values = values

    def evaluate_column_values(self, column_values: dict) -> bool:
        if self.column in column_values:
            return column_values.get(self.column) in self.values
        
        return True
    
    def evaluate_dataframe(self, pdf: pd.DataFrame) -> pd.DataFrame:
        if not self.column in pdf.columns:
            return super().evaluate_dataframe(pdf)
        
        return pdf[self.column].isin(self.values)

class Not (BaseQueryPredicate):
    def __init__(self, query: BaseQueryPredicate) -> None:
        self.query = query
    
    def evaluate_column_values(self, column_values: dict) -> bool:
        return not self.query.evaluate_column_values(column_values)
    
    def evaluate_dataframe(self, pdf: pd.DataFrame) -> pd.DataFrame:
        return ~self.query.evaluate_dataframe(pdf)

class GreaterThan (BaseQueryPredicate):
    def __init__(self, column: str, value: any) -> None:
        self.column = column
        self.value = value

    def evaluate_column_values(self, column_values: dict) -> bool:
        if self.column in column_values:
            return column_values.get(self.column) > self.value
        
        return True
    
    def evaluate_dataframe(self, pdf: pd.DataFrame) -> pd.DataFrame:
        if not self.column in pdf.columns:
            return super().evaluate_dataframe(pdf)
        
        return pdf[self.column] > self.value

class LesserThan (BaseQueryPredicate):
    def __init__(self, column: str, value: any) -> None:
        self.column = column
        self.value = value

    def evaluate_column_values(self, column_values: dict) -> bool:
        if self.column in column_values:
            return column_values.get(self.column) < self.value
        
        return True
    
    def evaluate_dataframe(self, pdf: pd.DataFrame) -> pd.DataFrame:
        if not self.column in pdf.columns:
            return super().evaluate_dataframe(pdf)
        
        return pdf[self.column] < self.value

if __name__ == "__main__":
    pass