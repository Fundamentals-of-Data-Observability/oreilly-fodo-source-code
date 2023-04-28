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
    def generate_direct_mapping(output_schema: Schema, input_schemas: list[Schema]):
        input_schemas_mapping = []
        output_schema_field_names = [f[0] for f in output_schema.fields]
        for schema in input_schemas:
            mapping = {}
            for field in schema.fields:
                if field[0] in output_schema_field_names:
                    mapping[field[0]] = [field[0]]
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
