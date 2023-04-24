import pandas as pd
import os
import json
import hashlib
import datetime


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default
pd.options.mode.chained_assignment = None


class Application:
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


class User:
    name: str
    id: str

    def __init__(self, name: str) -> None:
        self.name = name
        self.id = hashlib.md5(self.name.encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "name": self.name}


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

    @staticmethod
    def fetch_git_location():
        import git
        code_repo = git.Repo(os.getcwd(), search_parent_directories=True).remote().url
        return code_repo


class ApplicationVersion:
    version: str
    user: User
    application_repo: ApplicationRepository
    id: str

    def __init__(self, version: str, user: User, application_repo: ApplicationRepository) -> None:
        self.version = version
        self.user = user
        self.application_repo = application_repo
        self.id = hashlib.md5(
            ",".join([self.version, self.user.id, self.application_repo.id]).encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "version": self.version, "user": self.user.id,
                "application_repository": self.application_repo.id}

    @staticmethod
    def fetch_git_version():
        import git
        repo = git.Repo(os.getcwd(), search_parent_directories=True)
        commit = repo.head.commit
        code_version = commit.hexsha
        return code_version

    @staticmethod
    def fetch_git_author():
        import git
        repo = git.Repo(os.getcwd(), search_parent_directories=True)
        commit = repo.head.commit
        code_author = commit.author.name
        return code_author


class ApplicationExecution:
    application_version: ApplicationVersion
    user: User
    id: str

    def __init__(self, application_version: ApplicationVersion, user: User) -> None:
        self.application_version = application_version
        self.user = user
        self.id = hashlib.md5(",".join([self.application_version.id, self.user.id]).encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "application_version": self.application_version.id, "user": self.user.id}


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


class OutputDataLineage:
    schema: Schema
    input_schemas: list[tuple[Schema, dict]]
    id: str

    def __init__(self, schema: Schema, input_schemas_mapping: list[tuple[Schema, dict]]) -> None:
        self.schema = schema
        self.input_schemas_mapping = input_schemas_mapping
        self.id = hashlib.md5(",".join([self.schema.id]).encode("utf-8") + self.linearize().encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "schema": self.schema.id, "input_schemas_mapping": self.input_schemas_mapping}

    @staticmethod
    def generate_direct_mapping(output_schema: Schema, input_schemas: list[tuple[Schema, dict]]):
        input_schemas_mapping = []
        output_schema_field_names = [f[0] for f in output_schema.fields]
        for (schema, extra_mapping) in input_schemas:
            mapping = {}
            for field in schema.fields:
                if field[0] in output_schema_field_names:
                    mapping[field[0]] = [field[0]]
            mapping.update(extra_mapping)
            if len(mapping):
                input_schemas_mapping.append((schema, mapping))
        return input_schemas_mapping

    def linearize(self):
        linearized = ""
        for schema in self.input_schemas_mapping:
            for k in schema[1]:
                linearized += (k + ":")
                linearized += ("-".join(schema[1][k]) + ",")
        linearized = linearized[:-1]
        return linearized


class DataLineageExecution:
    lineage: OutputDataLineage
    application_execution: ApplicationExecution
    start_time: str
    id: str

    def __init__(self, lineage: OutputDataLineage, application_execution: ApplicationExecution) -> None:
        self.lineage = lineage
        self.application_execution = application_execution
        self.start_time = datetime.datetime.now().isoformat()
        self.id = hashlib.md5(
            ",".join([self.lineage.id, self.application_execution.id, self.start_time]).encode("utf-8")).hexdigest()

    def to_json(self):
        return {"id": self.id, "lineage": self.lineage.id, "application_execution": self.application_execution.id,
                "start_time": self.start_time}


class DataMetrics:
    schema: Schema
    lineage_execution: DataLineageExecution
    metrics: list[tuple[str, float]]
    id: str

    def __init__(self, metrics: list[tuple[str, float]], schema: Schema,
                 lineage_execution: DataLineageExecution) -> None:
        self.metrics = metrics
        self.schema = schema
        self.lineage_execution = lineage_execution
        self.id = hashlib.md5(",".join([self.schema.id, self.lineage_execution.id]).encode("utf-8")).hexdigest()

    def to_json(self):
        from functools import reduce
        jfields = reduce(lambda x, y: dict(**x, **y), map(lambda f: {f[0]: f[1]}, self.metrics))
        return {"id": self.id, "metrics": jfields, "schema": self.schema.id,
                "lineage_execution": self.lineage_execution.id}

    @staticmethod
    def extract_metrics_from_df(df: pd.DataFrame):
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
print(json.dumps(app))
app_repo = ApplicationRepository(ApplicationRepository.fetch_git_location(), app)
print(json.dumps(app_repo))
git_user = User(ApplicationVersion.fetch_git_author())
print(json.dumps(git_user))
app_version = ApplicationVersion(ApplicationVersion.fetch_git_version(), git_user, app_repo)
print(json.dumps(app_version))
current_user = User("Emanuele Lucchini")
print(json.dumps(current_user))
app_exe = ApplicationExecution(app_version, current_user)
print(json.dumps(app_exe))

all_assets = pd.read_csv("data/monthly_assets.csv", parse_dates=['Date'])

apptech = all_assets[all_assets['Symbol'] == 'APCX']
buzzfeed = all_assets[all_assets['Symbol'] == 'BZFD']

buzzfeed['Intraday_Delta'] = buzzfeed['Adj Close'] - buzzfeed['Open']
apptech['Intraday_Delta'] = apptech['Adj Close'] - apptech['Open']

kept_values = ['Open', 'Adj Close', 'Intraday_Delta']

buzzfeed[kept_values].to_csv("data/report_buzzfeed.csv", index=False)
apptech[kept_values].to_csv("data/report_appTech.csv", index=False)

all_assets_ds = DataSource("data/monthly_assets.csv", "csv")
print(json.dumps(all_assets_ds))
all_assets_sc = Schema(Schema.extract_fields_from_dataframe(all_assets), all_assets_ds)
print(json.dumps(all_assets_sc))
buzzfeed_ds = DataSource("data/report_buzzfeed.csv", "csv")
print(json.dumps(buzzfeed_ds))
buzzfeed_sc = Schema(Schema.extract_fields_from_dataframe(buzzfeed), buzzfeed_ds)
print(json.dumps(buzzfeed_sc))
apptech_ds = DataSource("data/report_appTech.csv", "csv")
print(json.dumps(apptech_ds))
apptech_sc = Schema(Schema.extract_fields_from_dataframe(apptech), apptech_ds)
print(json.dumps(apptech_sc))
# First lineage
intraday_delta_mapping = {"Intraday_Delta": ['Adj Close', 'Open']}
a = (all_assets_sc, intraday_delta_mapping)
lineage_buzzfeed = OutputDataLineage(buzzfeed_sc,
                                     OutputDataLineage.generate_direct_mapping(buzzfeed_sc, [(all_assets_sc, intraday_delta_mapping)]))
print(json.dumps(lineage_buzzfeed))
lineage_buzzfeed_exe = DataLineageExecution(lineage_buzzfeed, app_exe)
print(json.dumps(lineage_buzzfeed_exe))
all_assets_ms_1 = DataMetrics(DataMetrics.extract_metrics_from_df(all_assets), all_assets_sc, lineage_buzzfeed_exe)
print(json.dumps(all_assets_ms_1))
buzzfeed_ms = DataMetrics(DataMetrics.extract_metrics_from_df(buzzfeed), buzzfeed_sc, lineage_buzzfeed_exe)
print(json.dumps(buzzfeed_ms))
# Second lineage
lineage_apptech = OutputDataLineage(apptech_sc,
                                    OutputDataLineage.generate_direct_mapping(apptech_sc, [(all_assets_sc, intraday_delta_mapping)]))
print(json.dumps(lineage_apptech))
lineage_apptech_exe = DataLineageExecution(lineage_apptech, app_exe)
print(json.dumps(lineage_apptech_exe))
all_assets_ms_2 = DataMetrics(DataMetrics.extract_metrics_from_df(all_assets), all_assets_sc, lineage_apptech_exe)
print(json.dumps(all_assets_ms_2))
apptech_ms = DataMetrics(DataMetrics.extract_metrics_from_df(apptech), apptech_sc, lineage_apptech_exe)
print(json.dumps(apptech_ms))
