import pandas as pd
import os
import json
import hashlib


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default
pd.options.mode.chained_assignment = None


class Application:
    # primary key
    name: str
    id: str

    def __init__(self, name: str) -> None:
        self.name = name
        self.id = hashlib.md5(self.name.encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "name": self.name}

    @staticmethod
    def fetch_file_name():
        import os
        application_name = os.path.basename(os.path.realpath(__file__))
        return application_name


class ApplicationRepository:
    location: str
    application: Application
    id: str

    def __init__(self, location: str, application: Application) -> None:
        self.location = location
        self.application = application
        self.id = hashlib.md5(",".join([self.location, self.application.id]).encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "location": self.location, "application": self.application.id}

    # Add static method to fetch the git repo
    @staticmethod
    def fetch_git_location():
        import git
        code_repo = git.Repo(os.getcwd(), search_parent_directories=True).remote().url
        return code_repo


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


class DataMetrics:
    schema: Schema
    metrics: list[tuple[str, float]]
    id: str

    def __init__(self, metrics: list[tuple[str, float]], schema: Schema) -> None:
        self.metrics = metrics
        self.schema = schema
        self.id = hashlib.md5(",".join([self.schema.id]).encode("utf-8")).hexdigest()

    def to_json(self):
        from functools import reduce
        jfields = reduce(lambda x, y: dict(**x, **y), map(lambda f: {f[0]: f[1]}, self.metrics))
        return {"id": self.id, "metrics": jfields, "schema": self.schema.id}

    @staticmethod
    def extract_metrics_from_dataframe(df: pd.DataFrame):
        d = df.describe(include='all', datetime_is_numeric=True)
        import math
        import numbers
        metrics = {}
        for field in d.columns[1:]:
            msd = dict(filter(lambda x: isinstance(x[1], numbers.Number) and math.isnan(x[1]) is False,
                              map(lambda x: (field + "." + x[0], x[1]), d[field].to_dict().items())))
            metrics.update(msd)
        return list(metrics.items())


app = Application(Application.fetch_file_name())
app_repo = ApplicationRepository(ApplicationRepository.fetch_git_location(), app)
print(json.dumps(app))
print(json.dumps(app_repo))

AppTech = pd.read_csv(
    "data/AppTech.csv",
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
)
AppTech_DS = DataSource("data/AppTech", "csv")
AppTech_SC = Schema(Schema.extract_fields_from_dataframe(AppTech), AppTech_DS)
AppTech_M = DataMetrics(DataMetrics.extract_metrics_from_dataframe(AppTech), AppTech_SC)
print(json.dumps(AppTech_DS))
print(json.dumps(AppTech_SC))
print(json.dumps(AppTech_M))

Buzzfeed = pd.read_csv(
    "data/Buzzfeed.csv",
    parse_dates=["Date"],
    dtype={"Symbol": "category"},
)
Buzzfeed_DS = DataSource("data/Buzzfeed", "csv")
Buzzfeed_SC = Schema(Schema.extract_fields_from_dataframe(Buzzfeed), Buzzfeed_DS)
Buzzfeed_M = DataMetrics(DataMetrics.extract_metrics_from_dataframe(Buzzfeed), Buzzfeed_SC)
print(json.dumps(Buzzfeed_DS))
print(json.dumps(Buzzfeed_SC))
print(json.dumps(Buzzfeed_M))

monthly_assets = pd.concat([AppTech, Buzzfeed]) \
    .astype({"Symbol": "category"})
monthly_assets.to_csv(
    "data/monthly_assets.csv", index=False
)
monthly_assets_DS = DataSource("data/monthly_assets", "csv")
monthly_assets_SC = Schema(Schema.extract_fields_from_dataframe(monthly_assets), monthly_assets_DS)
monthly_assets_M = DataMetrics(DataMetrics.extract_metrics_from_dataframe(monthly_assets), monthly_assets_SC)
print(json.dumps(monthly_assets_DS))
print(json.dumps(monthly_assets_SC))
print(json.dumps(monthly_assets_M))
