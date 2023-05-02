import pandas as pd
import json


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default
pd.options.mode.chained_assignment = None
original_read_csv = pd.read_csv
original_to_csv = pd.DataFrame.to_csv


def read_csv_with_do(*args, **kwargs):
    df = original_read_csv(*args, **kwargs)
    print("some code for read_csv")
    return df


def to_csv_with_do(self, *args, **kwargs):
    r = original_to_csv(self, *args, **kwargs)
    print("some code for to_csv")
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
