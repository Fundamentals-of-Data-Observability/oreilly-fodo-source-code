import pandas as pd
import json
import hashlib
import os


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default
pd.options.mode.chained_assignment = None
original_read_csv = pd.read_csv
original_to_csv = pd.DataFrame.to_csv


class DataSource:
    location: str
    format: str
    id: str

    def __init__(self, location: str, format: str = None) -> None:
        self.location = location
        self.format = format
        self.id = hashlib.md5(",".join([self.location, self.format]).encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "location": self.location, "format": self.format}


class Schema:
    fields: list[tuple[str, str]]
    data_source: DataSource
    id: str

    def __init__(self, fields: list[tuple[str, str]], data_source: DataSource) -> None:
        self.fields = fields
        self.data_source = data_source
        linearized_fields = ",".join(list(map(lambda x: x[0] + "-" + x[1], sorted(self.fields))))
        self.id = hashlib.md5(",".join([linearized_fields, self.data_source.id]).encode("utf-8")).hexdigest()

    def to_json(self):
        from functools import reduce
        jfields = reduce(lambda x, y: dict(**x, **y), map(lambda f: {f[0]: f[1]}, self.fields))
        return {"id": self.id, "fields": jfields, "data_source": self.data_source.id}

    @staticmethod
    def extract_fields_from_dataframe(df: pd.DataFrame):
        fs = list(zip(df.columns.values.tolist(), map(lambda x: str(x), df.dtypes.values.tolist())))
        return fs


def read_csv_with_do(*args, **kwargs):
    df = original_read_csv(*args, **kwargs)
    file_name = args[0][0:args[0].rfind('.')]
    file_format = args[0][args[0].rfind('.') + 1:]
    ds = DataSource(file_name, file_format)
    print(json.dumps(ds))
    sc = Schema(Schema.extract_fields_from_dataframe(df), ds)
    print(json.dumps(sc))
    return df


def to_csv_with_do(self, *args, **kwargs):
    r = original_to_csv(self, *args, **kwargs)
    file_name = args[0][0:args[0].rfind('.')]
    file_format = args[0][args[0].rfind('.') + 1:]
    ds = DataSource(file_name, file_format)
    print(json.dumps(ds))
    sc = Schema(Schema.extract_fields_from_dataframe(self), ds)
    print(json.dumps(sc))
    return r


pd.read_csv = read_csv_with_do
pd.DataFrame.to_csv = to_csv_with_do

AppTech = pd.read_csv(
    "data/AppTech.csv",
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
)
Buzzfeed = pd.read_csv(
    "data/Buzzfeed.csv",
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
)

monthly_assets = pd.concat([AppTech, Buzzfeed]) \
    .astype({"Symbol": "category"})
monthly_assets.to_csv(
    "data/monthly_assets.csv", index=False
)
